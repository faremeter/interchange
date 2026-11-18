import { describe, test, expect } from "bun:test";
import {
  createReactor,
  type InferenceHarnessOptions,
  type ReactorEmittedEvent,
} from "@intx/inference";
import { createDefaultDependencies } from "@intx/inference/providers";
import { createInboundMessage } from "@intx/mime";
import type {
  AssistantTurn,
  InferenceEvent,
  ToolCall,
  TurnEndedData,
} from "@intx/types/runtime";
import type { SessionStatus } from "@intx/types";
import { waitUntil } from "@intx/types/testing";
import {
  createEventCollectorRegistry,
  type EventCollectorRegistry,
} from "./event-collector-registry";
import { emptyStore, emptyUsage } from "./event-collector.test-helpers";

let seq = 0;

function event(
  type: string,
  data: Record<string, unknown> = {},
): InferenceEvent {
  seq += 1;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test helper: type string is the correct discriminant
  return { type, seq, data } as InferenceEvent;
}

// A database whose writes all succeed and record nothing. The status is
// derived synchronously in dispatch; persistence does not affect it.
function fakeDb() {
  const db = {
    insert(_table: unknown) {
      return {
        values(_vals: Record<string, unknown>) {
          return Promise.resolve();
        },
      };
    },
    update(_table: unknown) {
      return {
        set(_vals: Record<string, unknown>) {
          return {
            where(_condition: unknown) {
              return Promise.resolve();
            },
          };
        },
      };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- fake DB satisfies the shape the registry uses at runtime
  return db as never;
}

function newRegistry(): EventCollectorRegistry {
  return createEventCollectorRegistry({ db: fakeDb() });
}

// A session that receives events in order, and its status after each one.
// Each call creates the collector for its address, so a second call for the
// same address replaces it.
function session(
  registry = newRegistry(),
  address = "agent://status",
): {
  send(type: string, data?: Record<string, unknown>): SessionStatus | undefined;
} {
  registry.create(address, "tnt_test", "ses_test", "run_test");
  return {
    send(type, data = {}) {
      registry.dispatch(address, event(type, data));
      return registry.getStatus(address);
    },
  };
}

const busy: SessionStatus = { status: "busy" };
const idle: SessionStatus = { status: "idle" };
const waiting: SessionStatus = { status: "waiting_approval" };

function start(turnId: string) {
  return ["inference.start", { model: "test", turnId }] as const;
}

function blocked(turnId: string, gateId: string, reason = "approval") {
  return [
    "reactor.gate.blocked",
    { reason, gateId, correlationId: `corr-${gateId}`, turnId },
  ] as const;
}

function ended(turnId: string, outcome: string, extra = {}) {
  return ["reactor.turn.ended", { turnId, outcome, ...extra }] as const;
}

function reply(turnId: string) {
  return ["connector.reply", { content: "hi", turnId }] as const;
}

describe("session status from the turn lifecycle", () => {
  test("a turn that starts and ends replied leaves the session idle", () => {
    const s = session();
    expect(s.send(...start("a"))).toEqual(busy);
    expect(s.send(...reply("a"))).toEqual(busy);
    expect(s.send(...ended("a", "replied"))).toEqual(idle);
  });

  test("another turn's reply and end do not hide an open approval", () => {
    const s = session();
    s.send(...start("a"));
    expect(s.send(...blocked("a", "g-a"))).toEqual(waiting);
    expect(s.send(...start("b"))).toEqual(busy);
    expect(s.send(...reply("b"))).toEqual(busy);
    expect(s.send(...ended("b", "replied"))).toEqual(waiting);
  });

  test("the end of another turn keeps a running turn busy", () => {
    const s = session();
    s.send(...start("a"));
    expect(s.send(...ended("x", "unanswered"))).toEqual(busy);
  });

  test("a joined end changes nothing", () => {
    const s = session();
    s.send(...start("a"));
    expect(s.send(...ended("d", "joined", { joinedTurnId: "a" }))).toEqual(
      busy,
    );
  });

  test("a gate that clears while another turn runs makes its turn run too", () => {
    const s = session();
    s.send(...start("a"));
    s.send(...blocked("a", "g-a"));
    s.send(...start("b"));
    expect(
      s.send("reactor.gate.cleared", { gateId: "g-a", reason: "resolved" }),
    ).toEqual(busy);
    // A still runs after B ends.
    expect(s.send(...ended("b", "replied"))).toEqual(busy);
    expect(s.send(...ended("a", "replied"))).toEqual(idle);
  });

  test("another turn's inference and end do not hide a resumed turn that still runs", () => {
    const s = session();
    s.send(...start("a"));
    expect(s.send(...blocked("a", "g-a"))).toEqual(waiting);
    expect(s.send(...start("b"))).toEqual(busy);
    expect(
      s.send("reactor.gate.cleared", { gateId: "g-a", reason: "resolved" }),
    ).toEqual(busy);
    expect(s.send(...start("b"))).toEqual(busy);
    expect(s.send(...ended("b", "replied"))).toEqual(busy);
    expect(s.send(...ended("a", "replied"))).toEqual(idle);
  });

  test("an approval that resumes drops its gate on its gate.cleared", () => {
    const s = session();
    s.send(...start("a"));
    expect(s.send(...blocked("a", "g-a"))).toEqual(waiting);
    expect(
      s.send("reactor.gate.cleared", { gateId: "g-a", reason: "resolved" }),
    ).toEqual(busy);
    expect(s.send(...start("a"))).toEqual(busy);
    expect(s.send(...ended("a", "replied"))).toEqual(idle);
  });

  test("an inference of a turn that holds an open approval gate keeps waiting_approval", () => {
    const s = session();
    s.send(...start("a"));
    expect(s.send(...blocked("a", "g-a"))).toEqual(waiting);
    expect(s.send(...start("a"))).toEqual(waiting);
    expect(s.send(...start("b"))).toEqual(busy);
    expect(s.send(...ended("b", "replied"))).toEqual(waiting);
  });

  test("a failed end of the busy turn leaves the session idle", () => {
    const s = session();
    s.send(...start("a"));
    expect(
      s.send(
        ...ended("a", "failed", {
          error: { message: "boom", kind: "reactor_fatal" },
        }),
      ),
    ).toEqual(idle);
  });

  test("a turn parked on a non-approval gate after its reply leaves the session idle", () => {
    const s = session();
    s.send(...start("a"));
    s.send(...reply("a"));
    expect(s.send(...blocked("a", "g-pay", "payment"))).toEqual(idle);
  });

  test("another turn's open non-approval gate leaves the session idle at the end of the busy turn", () => {
    const s = session();
    s.send(...start("a"));
    expect(s.send(...blocked("a", "g-pay", "payment"))).toEqual(idle);
    expect(s.send(...start("b"))).toEqual(busy);
    expect(s.send(...reply("b"))).toEqual(busy);
    expect(s.send(...ended("b", "replied"))).toEqual(idle);
  });

  test("an inference.start with no turn id sets busy but makes no turn run", () => {
    const s = session();
    expect(s.send("inference.start", { model: "test" })).toEqual(busy);
    expect(s.send(...ended("x", "unanswered"))).toEqual(idle);
  });

  test("a non-approval park of a turn while another turn runs keeps busy", () => {
    const s = session();
    s.send(...start("a"));
    s.send(...start("b"));
    expect(s.send(...blocked("a", "g-pay", "payment"))).toEqual(busy);
  });

  test("reactor.done and a fatal reactor.error leave the session idle", () => {
    const s = session();
    s.send(...start("a"));
    expect(s.send("reactor.error", { error: "boom", fatal: true })).toEqual(
      idle,
    );
    const t = session();
    t.send(...start("a"));
    expect(t.send("reactor.done")).toEqual(idle);
  });

  test("a non-fatal reactor.error and an unrelated event change nothing", () => {
    const s = session();
    s.send(...start("a"));
    expect(s.send("reactor.error", { error: "oops", fatal: false })).toEqual(
      busy,
    );
    expect(
      s.send("inference.text.delta", { token: "hi", partial: { text: "" } }),
    ).toEqual(busy);
  });

  test("reactor.start does not set busy", () => {
    const s = session();
    expect(s.send("reactor.start")).toEqual(idle);
  });

  // After a restart the tracker never saw the rehydrated gate's blocked.
  // The approval clears a gate it does not know, and the resumed turn
  // starts and runs its approved call.
  test("a turn resumed after a restart reads busy while its approved call runs", () => {
    const s = session();
    expect(
      s.send("reactor.gate.cleared", { gateId: "g1", reason: "resolved" }),
    ).toEqual(idle);
    expect(
      s.send("reactor.turn.started", { turnId: "t1", resumed: true }),
    ).toEqual(busy);
    expect(
      s.send("tool.start", { call: { id: "c1", name: "x", arguments: {} } }),
    ).toEqual(busy);
  });

  test("a resumed turn that starts before its unknown gate clears reads busy", () => {
    const s = session();
    expect(
      s.send("reactor.turn.started", { turnId: "t1", resumed: true }),
    ).toEqual(busy);
    expect(
      s.send("reactor.gate.cleared", { gateId: "g1", reason: "resolved" }),
    ).toEqual(busy);
  });

  test("a turn.started turn that ends replied leaves the session idle", () => {
    const s = session();
    expect(
      s.send("reactor.turn.started", { turnId: "t1", resumed: false }),
    ).toEqual(busy);
    expect(s.send(...ended("t1", "replied"))).toEqual(idle);
  });

  test("a gate.cleared of an unknown gate leaves the status unchanged", () => {
    const s = session();
    s.send(...start("a"));
    s.send(...blocked("a", "g-a"));
    expect(
      s.send("reactor.gate.cleared", { gateId: "g-x", reason: "resolved" }),
    ).toEqual(waiting);
  });

  test("a failed end of a parked approval turn after another turn ended leaves the session idle", () => {
    const s = session();
    s.send(...start("a"));
    s.send(...blocked("a", "g-a"));
    s.send(...start("b"));
    expect(s.send(...ended("b", "replied"))).toEqual(waiting);
    const failed: TurnEndedData = {
      turnId: "a",
      outcome: "failed",
      error: { message: "boom", kind: "reactor_fatal" },
    };
    expect(s.send("reactor.turn.ended", failed)).toEqual(idle);
  });

  test("sessions at different addresses do not share a tracker", () => {
    const registry = newRegistry();
    const x = session(registry, "agent://x");
    const y = session(registry, "agent://y");
    x.send(...start("t"));
    x.send(...blocked("t", "g-t"));
    y.send(...start("t"));
    expect(y.send(...ended("t", "replied"))).toEqual(idle);
    expect(registry.getStatus("agent://x")).toEqual(waiting);
  });

  test("a replaced collector starts with a fresh tracker", () => {
    const registry = newRegistry();
    const first = session(registry, "agent://z");
    first.send(...start("t"));
    first.send(...blocked("t", "g-t"));
    const second = session(registry, "agent://z");
    second.send(...start("u"));
    expect(second.send(...ended("u", "replied"))).toEqual(idle);
  });

  test("the status is removed after reactor.done drains", async () => {
    const registry = newRegistry();
    const s = session(registry, "agent://d");
    s.send(...start("t"));
    s.send("reactor.done");
    await waitUntil(() => !registry.has("agent://d"));
    expect(registry.getStatus("agent://d")).toBeUndefined();
  });
});

describe("session status from a reactor", () => {
  test("a re-inference while a sibling call is parked on approval keeps waiting_approval", async () => {
    // A custom director re-infers on the sibling's tool.done while the ask
    // call stays parked. The turn keeps its live approval gate.
    const registry = newRegistry();
    const address = "agent://x";
    registry.create(address, "tnt_test", "ses_test", "run_test");
    const events: ReactorEmittedEvent[] = [];
    let inferences = 0;
    const calls: ToolCall[] = [
      { id: "c-ask", name: "ask", arguments: {} },
      { id: "c-ok", name: "ok", arguments: {} },
    ];
    const reactor = createReactor({
      sessionId: "parked-sibling",
      director: {
        async decide(e, _s, caps) {
          if (e.type === "message.received") return caps.infer();
          if (e.type === "inference.done") {
            return inferences === 1 ? caps.executeTools(calls) : caps.wait();
          }
          if (e.type === "tool.done") return caps.infer();
          return caps.wait();
        },
      },
      beforeToolExtensions: [
        {
          async beforeTool(call) {
            if (call.name !== "ask") return { type: "allow" };
            return {
              type: "suspend",
              gate: {
                type: "approval",
                gateId: "g-ask",
                correlationId: "corr-ask",
                timeoutAt: Date.now() + 60_000,
              },
              pendingOp: {
                correlationId: "corr-ask",
                kind: "approval",
                registeredAt: Date.now(),
                gateId: "g-ask",
                suspendedCall: call,
              },
            };
          },
        },
      ],
      source: {
        id: "anthropic:m",
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        credentialId: "c",
        model: "m",
      },
      toolRunner: {
        async run(call) {
          return { callId: call.id, content: "ok" };
        },
      },
      contextStore: emptyStore(),
      deps: createDefaultDependencies(),
      shutdownTimeoutMs: 100,
      onEvent: (e) => {
        events.push(e);
        if (e.type !== "message.received") registry.dispatch(address, e);
      },
      inferenceRunner: async function* (
        opts: InferenceHarnessOptions,
      ): AsyncGenerator<InferenceEvent> {
        inferences += 1;
        yield {
          type: "inference.start",
          seq: opts.nextSeq(),
          data: { model: "m" },
        };
        yield {
          type: "inference.done",
          seq: opts.nextSeq(),
          data: {
            turn: {
              role: "assistant",
              content:
                inferences === 1
                  ? calls.map((c) => ({ type: "tool_call" as const, ...c }))
                  : [{ type: "text", text: "thinking" }],
              model: "m",
              timestamp: Date.now(),
            },
            usage: emptyUsage(),
            source: { sourceId: "s", provider: "anthropic", model: "m" },
          },
        };
      },
    });

    reactor.start();
    reactor.deliver(
      createInboundMessage({ from: "a@local", to: "b@local", content: "go" }),
      { turnId: "turn-a" },
    );
    await waitUntil(
      () => events.filter((e) => e.type === "inference.done").length === 2,
    );

    expect(events.some((e) => e.type === "reactor.gate.blocked")).toBe(true);
    expect(events.some((e) => e.type === "reactor.turn.ended")).toBe(false);
    expect(registry.getStatus(address)).toEqual(waiting);

    reactor.abort("admin_kill");
    await waitUntil(() => events.some((e) => e.type === "reactor.done"));
  });

  test("an approved call of one turn that still runs after another turn ends keeps the session busy", async () => {
    // Turn A parks its ask call on approval. Turn B starts and holds its
    // second inference. The approval resumes A, which re-runs the ask call
    // and holds it. B then finishes and ends while A's call still runs.
    const registry = newRegistry();
    const address = "agent://x";
    registry.create(address, "tnt_test", "ses_test", "run_test");
    const events: ReactorEmittedEvent[] = [];
    const releaseB = Promise.withResolvers<boolean>();
    const releaseAsk = Promise.withResolvers<boolean>();
    let askStarted = false;
    let inferences = 0;
    const askCall: ToolCall = { id: "c-ask", name: "ask", arguments: {} };
    const okCall: ToolCall = { id: "c-ok", name: "ok", arguments: {} };
    const reactor = createReactor({
      sessionId: "resumed-turn-runs",
      director: {
        async decide(e, _s, caps) {
          if (e.type === "message.received") return caps.infer();
          if (e.type === "inference.done") {
            const calls: ToolCall[] = [];
            for (const block of e.turn.content) {
              if (block.type === "tool_call") {
                calls.push({ id: block.id, name: block.name, arguments: {} });
              }
            }
            return calls.length > 0
              ? caps.executeTools(calls)
              : caps.reply("done");
          }
          if (e.type === "tool.done") return caps.infer();
          if (e.type === "resume.execute_tools") {
            return caps.executeTools(e.calls);
          }
          return caps.wait();
        },
      },
      beforeToolExtensions: [
        {
          async beforeTool(call, _state, _signal, ctx) {
            if (call.name !== "ask" || ctx.approved) return { type: "allow" };
            return {
              type: "suspend",
              gate: {
                type: "approval",
                gateId: "g-ask",
                correlationId: "corr-ask",
                timeoutAt: Date.now() + 60_000,
              },
              pendingOp: {
                correlationId: "corr-ask",
                kind: "approval",
                registeredAt: Date.now(),
                gateId: "g-ask",
                suspendedCall: call,
              },
            };
          },
        },
      ],
      source: {
        id: "anthropic:m",
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        credentialId: "c",
        model: "m",
      },
      toolRunner: {
        async run(call) {
          if (call.name === "ask") {
            askStarted = true;
            await releaseAsk.promise;
          }
          return { callId: call.id, content: "ok" };
        },
      },
      contextStore: emptyStore(),
      deps: createDefaultDependencies(),
      shutdownTimeoutMs: 100,
      onEvent: (e) => {
        events.push(e);
        if (e.type !== "message.received") registry.dispatch(address, e);
      },
      inferenceRunner: async function* (
        opts: InferenceHarnessOptions,
      ): AsyncGenerator<InferenceEvent> {
        inferences += 1;
        const current = inferences;
        yield {
          type: "inference.start",
          seq: opts.nextSeq(),
          data: { model: "m" },
        };
        if (current === 2) await releaseB.promise;
        let content: AssistantTurn["content"] = [{ type: "text", text: "t" }];
        if (current === 1) content = [{ type: "tool_call", ...askCall }];
        if (current === 2) content = [{ type: "tool_call", ...okCall }];
        yield {
          type: "inference.done",
          seq: opts.nextSeq(),
          data: {
            turn: {
              role: "assistant",
              content,
              model: "m",
              timestamp: Date.now(),
            },
            usage: emptyUsage(),
            source: { sourceId: "s", provider: "anthropic", model: "m" },
          },
        };
      },
    });
    const message = (content: string, correlationId?: string) =>
      createInboundMessage({
        from: "a@local",
        to: "b@local",
        content,
        ...(correlationId !== undefined ? { correlationId } : {}),
      });
    const ended = (turnId: string) =>
      events.some(
        (e) => e.type === "reactor.turn.ended" && e.data.turnId === turnId,
      );

    reactor.start();
    reactor.deliver(message("go"), { turnId: "A" });
    await waitUntil(() =>
      events.some((e) => e.type === "reactor.gate.blocked"),
    );
    expect(registry.getStatus(address)).toEqual(waiting);
    reactor.deliver(message("B"), { turnId: "B" });
    await waitUntil(() => inferences === 2);
    reactor.deliver(
      message(JSON.stringify({ outcome: "approved" }), "corr-ask"),
      { turnId: "D" },
    );
    await waitUntil(() =>
      events.some((e) => e.type === "reactor.gate.cleared"),
    );
    releaseB.resolve(true);
    await waitUntil(() => askStarted && ended("B"));

    expect(ended("A")).toBe(false);
    expect(registry.getStatus(address)).toEqual(busy);

    releaseAsk.resolve(true);
    reactor.abort("admin_kill");
    await waitUntil(() => events.some((e) => e.type === "reactor.done"));
  });
});
