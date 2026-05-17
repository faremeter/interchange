import { describe, test, expect } from "bun:test";

import { runInference } from "@interchange/inference";
import type {
  ConversationTurn,
  InferenceEvent,
  ProviderConfig,
} from "@interchange/types/runtime";

import { ClockWallClockOverrunError } from "./clock";
import { UnmatchedFetchError } from "./errors";
import { setupHarness, type Harness } from "./harness";
import * as wire from "./wire";

const ANTHROPIC_PROVIDER_CONFIG: ProviderConfig = {
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "test",
};

const HEAD_USAGE = {
  input: 10,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};
const TAIL_USAGE = {
  input: 0,
  output: 5,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

function userTurn(text: string): ConversationTurn {
  return {
    role: "user",
    content: [{ type: "text", text }],
    timestamp: 0,
  };
}

function scriptToolCallTurn(
  harness: Harness,
  callId: string,
  toolName: string,
  argsJSON: string,
): { close: number } {
  const stream = harness.scenario.createStream();
  const chunks = wire.completeResponse("anthropic", {
    toolCalls: [{ callId, name: toolName, argsJSON }],
    headUsage: HEAD_USAGE,
    tailUsage: TAIL_USAGE,
  });
  stream.enqueueAll(chunks, { startAt: 10 });
  harness.scenario.whenRequestMatches(() => true, stream);
  return { close: 10 + chunks.length };
}

describe("scenario.onTool: sync return", () => {
  test("dispatches the result synchronously within the registering tick", () => {
    const harness = setupHarness();
    try {
      harness.scenario.onTool("echo", (args) => args);

      const seen: unknown[] = [];
      harness.scenario.invokeTool("echo", "foo", (result) => {
        seen.push(result);
      });

      expect(seen).toEqual(["foo"]);
    } finally {
      harness.dispose();
    }
  });

  test("rejects re-registration of the same tool name", () => {
    const harness = setupHarness();
    try {
      harness.scenario.onTool("dup", () => "first");
      expect(() => harness.scenario.onTool("dup", () => "second")).toThrow(
        /already registered/,
      );
    } finally {
      harness.dispose();
    }
  });

  test("invokeTool throws when no handler is registered for the name", () => {
    const harness = setupHarness();
    try {
      expect(() =>
        harness.scenario.invokeTool("missing", null, () => undefined),
      ).toThrow(/no handler registered/);
    } finally {
      harness.dispose();
    }
  });

  test("sync return of undefined is a programmer error and throws", () => {
    const harness = setupHarness();
    try {
      harness.scenario.onTool("forgot-return", () => undefined);
      expect(() =>
        harness.scenario.invokeTool("forgot-return", null, () => undefined),
      ).toThrow(/resolved to `undefined`/);
    } finally {
      harness.dispose();
    }
  });
});

describe("scenario.onTool: delayed envelope", () => {
  test("schedules dispatch at clock.now() + virtualDelayMs", async () => {
    const harness = setupHarness();
    try {
      harness.scenario.onTool("delayed", () => ({
        result: "ready",
        virtualDelayMs: 100,
      }));

      const seen: { at: number; result: unknown }[] = [];
      harness.scenario.invokeTool("delayed", null, (result) => {
        seen.push({ at: harness.clock.now(), result });
      });

      expect(seen).toEqual([]);

      await harness.advanceTo(99);
      expect(seen).toEqual([]);

      await harness.advanceTo(100);
      expect(seen).toEqual([{ at: 100, result: "ready" }]);
    } finally {
      harness.dispose();
    }
  });

  test("virtualDelayMs of 0 dispatches synchronously in the same tick", () => {
    const harness = setupHarness();
    try {
      harness.scenario.onTool("now", () => ({
        result: "immediate",
        virtualDelayMs: 0,
      }));

      const seen: unknown[] = [];
      harness.scenario.invokeTool("now", null, (result) => {
        seen.push(result);
      });

      expect(seen).toEqual(["immediate"]);
    } finally {
      harness.dispose();
    }
  });
});

describe("scenario.onTool: promise return", () => {
  test("harness.run() awaits a microtask-resolved promise before quiescence", async () => {
    const harness = setupHarness();
    try {
      harness.scenario.onTool("async-echo", async (args) => {
        await Promise.resolve();
        return args;
      });

      const seen: unknown[] = [];
      harness.scenario.invokeTool("async-echo", "bar", (result) => {
        seen.push(result);
      });

      expect(seen).toEqual([]);
      await harness.run();
      expect(seen).toEqual(["bar"]);
    } finally {
      harness.dispose();
    }
  });

  test("promise that resolves to a delayed envelope schedules on the clock", async () => {
    const harness = setupHarness();
    try {
      harness.scenario.onTool("async-delayed", async () => {
        await Promise.resolve();
        return { result: "deferred", virtualDelayMs: 50 };
      });

      const seen: { at: number; result: unknown }[] = [];
      harness.scenario.invokeTool("async-delayed", null, (result) => {
        seen.push({ at: harness.clock.now(), result });
      });

      // Drive the clock to +49; the handler's promise resolves on the
      // first microtask drain inside `advanceTo`, which schedules the
      // delayed dispatch at now+50. At +49 the dispatch hasn't fired.
      await harness.advanceTo(49);
      expect(seen).toEqual([]);

      await harness.advanceTo(50);
      expect(seen).toEqual([{ at: 50, result: "deferred" }]);
    } finally {
      harness.dispose();
    }
  });

  test("promise resolving to undefined surfaces an error through run()", async () => {
    const harness = setupHarness();
    try {
      harness.scenario.onTool("async-undef", async () => {
        await Promise.resolve();
        return undefined;
      });

      harness.scenario.invokeTool("async-undef", null, () => undefined);

      let caught: unknown;
      try {
        await harness.run();
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(Error);
      if (!(caught instanceof Error)) throw new Error("unreachable");
      expect(caught.message).toMatch(/resolved to `undefined`/);
    } finally {
      harness.dispose();
    }
  });
});

describe("scenario.onTool: quiescence accounting", () => {
  test("a pending tool handler blocks UnmatchedFetchError until it resolves", async () => {
    const harness = setupHarness();
    try {
      let releaseHandler: (() => void) | null = null;
      const handlerGate = new Promise<void>((resolve) => {
        releaseHandler = resolve;
      });

      harness.scenario.onTool("gated", async () => {
        await handlerGate;
        return "done";
      });

      const seen: unknown[] = [];
      harness.scenario.invokeTool("gated", null, (result) => {
        seen.push(result);
      });

      // Park a fetch with no matcher. If the in-flight handler did NOT
      // block quiescence, `harness.run()` would see only the unmatched
      // fetch and throw UnmatchedFetchError before our handler resolves.
      // We arrange for the handler to release AFTER a tick of waiting so
      // we can observe ordering: run() must await the handler first.
      const fetchPromise = harness.deps.fetch("https://example/unmatched");
      const fetchSettled = fetchPromise.catch((err: unknown) => err);

      // Schedule the handler release on the next macrotask via the clock:
      // we want it to fire only after `run()` has begun awaiting the
      // in-flight handler. Using queueMicrotask would resolve immediately
      // and not exercise the blocking behavior. We can't use real
      // setTimeout (it'd trip the watchdog). Instead, kick a microtask
      // chain that flips a flag, and assert the run-before-resolve
      // ordering via the resolution order of the dispatch callback and
      // the unmatched error.
      //
      // We resolve the gate on a deeper microtask so `run()` definitely
      // enters its quiescence loop at least once with the handler still
      // in-flight.
      void Promise.resolve()
        .then(() => undefined)
        .then(() => {
          if (releaseHandler !== null) releaseHandler();
        });

      let runErr: unknown;
      try {
        await harness.run();
      } catch (err) {
        runErr = err;
      }
      expect(runErr).toBeInstanceOf(UnmatchedFetchError);
      // Handler must have completed before the quiescence check fired.
      expect(seen).toEqual(["done"]);
      const fetchErr = await fetchSettled;
      expect(fetchErr).toBeInstanceOf(UnmatchedFetchError);
    } finally {
      harness.dispose();
    }
  });

  test("microtask-only handler completes within the wall-clock watchdog", async () => {
    const harness = setupHarness();
    try {
      harness.scenario.onTool("microtasks", async () => {
        for (let i = 0; i < 100; i++) {
          await Promise.resolve();
        }
        return "ok";
      });

      const seen: unknown[] = [];
      harness.scenario.invokeTool("microtasks", null, (result) => {
        seen.push(result);
      });

      await harness.run();
      expect(seen).toEqual(["ok"]);
    } finally {
      harness.dispose();
    }
  });

  test("real-timer-in-handler trips the wall-clock watchdog", async () => {
    const harness = setupHarness();
    try {
      harness.scenario.onTool("real-timer", () => {
        return new Promise<string>((resolve) => {
          setTimeout(() => {
            resolve("never");
          }, 500);
        });
      });

      harness.scenario.invokeTool("real-timer", null, () => undefined);

      let caught: unknown;
      try {
        await harness.run({ wallClockBudgetMs: 50 });
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(ClockWallClockOverrunError);
    } finally {
      harness.dispose();
    }
  });

  test("run() throw path clears in-flight state so the harness can be re-used", async () => {
    const harness = setupHarness();
    try {
      harness.scenario.onTool("never", () => {
        return new Promise<string>(() => undefined);
      });
      harness.scenario.invokeTool("never", null, () => undefined);

      let firstErr: unknown;
      try {
        await harness.run({ wallClockBudgetMs: 50 });
      } catch (err) {
        firstErr = err;
      }
      expect(firstErr).toBeInstanceOf(ClockWallClockOverrunError);

      const seen: unknown[] = [];
      harness.scenario.onTool("second", () => "fresh");
      harness.scenario.invokeTool("second", null, (result) => {
        seen.push(result);
      });

      await harness.run({ wallClockBudgetMs: Infinity });
      expect(seen).toEqual(["fresh"]);
    } finally {
      harness.dispose();
    }
  });
});

describe("inFlightErrors aggregation contract", () => {
  // Pins the documented contract at `harness.ts` around `inFlightErrors`:
  // when multiple in-flight tool handlers reject in the same tick, only the
  // first rejection surfaces from harness.run(); subsequent rejections are
  // dropped. The harness's `takeInFlightError` comment explicitly notes
  // this is a deliberate simplification a future slice can extend to
  // AggregateError if real tests require it — DO NOT change this behavior
  // to AggregateError without first revisiting that contract.
  test("two simultaneous tool handler rejections surface exactly one error", async () => {
    const harness = setupHarness();
    try {
      harness.scenario.onTool("first", async () => {
        await Promise.resolve();
        throw new Error("first-failure");
      });
      harness.scenario.onTool("second", async () => {
        await Promise.resolve();
        throw new Error("second-failure");
      });

      harness.scenario.invokeTool("first", null, () => undefined);
      harness.scenario.invokeTool("second", null, () => undefined);

      let caught: unknown;
      try {
        await harness.run();
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(Error);
      if (!(caught instanceof Error)) throw new Error("unreachable");
      // Exactly one of the two messages surfaces; whichever it is, the
      // other is dropped. The contract is "single rejection per drain",
      // not "AggregateError of all rejections".
      const matchedFirst = caught.message.includes("first-failure");
      const matchedSecond = caught.message.includes("second-failure");
      expect(matchedFirst || matchedSecond).toBe(true);
      expect(matchedFirst && matchedSecond).toBe(false);
    } finally {
      harness.dispose();
    }
  });
});

describe("harness.runInference auto-dispatch", () => {
  test("fires the registered handler when tool_call.end is observed", async () => {
    const harness = setupHarness();
    try {
      const handlerCalls: { args: unknown }[] = [];
      harness.scenario.onTool("weather", (args) => {
        handlerCalls.push({ args });
        return { temperatureF: 68 };
      });

      const { close } = scriptToolCallTurn(
        harness,
        "call_w_1",
        "weather",
        '{"location":"SF"}',
      );

      let seq = 0;
      const events: InferenceEvent[] = [];
      const collected = (async () => {
        for await (const ev of harness.runInference({
          turns: [userTurn("weather?")],
          model: "claude-test",
          providerConfig: ANTHROPIC_PROVIDER_CONFIG,
          nextSeq: () => ++seq,
        })) {
          events.push(ev);
        }
      })();
      await harness.advanceTo(close + 10);
      await collected;

      // The handler fired automatically with the parsed args; no
      // `scenario.invokeTool` call appeared in this test.
      expect(handlerCalls).toEqual([{ args: { location: "SF" } }]);
      expect(harness.scenario.lastToolDispatch("weather")).toEqual({
        temperatureF: 68,
      });
      // Sanity: the iterator yielded the tool_call.end the auto-dispatch
      // path observed.
      const toolEnd = events.find((e) => e.type === "inference.tool_call.end");
      expect(toolEnd).toBeDefined();
    } finally {
      harness.dispose();
    }
  });

  test("direct runInference({deps: harness.deps}) does NOT auto-fire (escape hatch)", async () => {
    const harness = setupHarness();
    try {
      const handlerCalls: { args: unknown }[] = [];
      harness.scenario.onTool("weather", (args) => {
        handlerCalls.push({ args });
        return { temperatureF: 68 };
      });

      const { close } = scriptToolCallTurn(
        harness,
        "call_w_2",
        "weather",
        '{"location":"SF"}',
      );

      let seq = 0;
      const collected = (async () => {
        for await (const _ev of runInference({
          turns: [userTurn("weather?")],
          model: "claude-test",
          providerConfig: ANTHROPIC_PROVIDER_CONFIG,
          nextSeq: () => ++seq,
          deps: harness.deps,
        })) {
          // Intentionally swallow events; we only care that the handler
          // does NOT auto-fire on this path.
        }
      })();
      await harness.advanceTo(close + 10);
      await collected;

      // The handler is registered but the production runInference path
      // bypasses the auto-dispatch wrapper. The handler must NOT have
      // fired, and no dispatch record exists.
      expect(handlerCalls).toEqual([]);
      expect(harness.scenario.lastToolDispatch("weather")).toBeUndefined();
    } finally {
      harness.dispose();
    }
  });

  test("throws if tool_call.end names a tool with no registered handler", async () => {
    const harness = setupHarness();
    try {
      // Note: no `onTool("unknown", ...)` registration.
      const { close } = scriptToolCallTurn(
        harness,
        "call_u_1",
        "unknown",
        "{}",
      );

      let seq = 0;
      let caught: unknown;
      const collected = (async () => {
        try {
          for await (const _ev of harness.runInference({
            turns: [userTurn("?")],
            model: "claude-test",
            providerConfig: ANTHROPIC_PROVIDER_CONFIG,
            nextSeq: () => ++seq,
          })) {
            // drain
          }
        } catch (err) {
          caught = err;
        }
      })();
      await harness.advanceTo(close + 10);
      await collected;

      expect(caught).toBeInstanceOf(Error);
      if (!(caught instanceof Error)) throw new Error("unreachable");
      expect(caught.message).toMatch(/no handler was registered/);
      expect(caught.message).toContain('"unknown"');
    } finally {
      harness.dispose();
    }
  });
});

describe("scenario.onTool / invokeTool: disposed-state guards", () => {
  test("onTool throws a disposed-naming error after harness.dispose()", () => {
    const harness = setupHarness();
    harness.dispose();
    expect(() => harness.scenario.onTool("after", () => "nope")).toThrow(
      /disposed/,
    );
  });

  test("invokeTool throws a disposed-naming error after harness.dispose()", () => {
    const harness = setupHarness();
    harness.scenario.onTool("registered", () => "value");
    harness.dispose();
    expect(() =>
      harness.scenario.invokeTool("registered", null, () => undefined),
    ).toThrow(/disposed/);
  });
});
