import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import type { InferenceEvent, InferenceSource } from "@intx/types/runtime";

import type { Invariant } from "./invariants";
import { createRecordingHarness } from "./session-recording";
import {
  createReplayHarness,
  replayResponsesForParsing,
  requestBytesMatch,
  SessionReplayMismatchError,
} from "./session-replay";
import { userTurn } from "./turns";
import * as wire from "./wire";

const ANTHROPIC_SOURCE: InferenceSource = {
  id: "anthropic:claude-test",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "test",
  model: "claude-test",
};

const ZERO_USAGE = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

let tmpDirs: string[] = [];

afterEach(async () => {
  for (const dir of tmpDirs) {
    await fs.rm(dir, { recursive: true, force: true });
  }
  tmpDirs = [];
});

async function makeTmpDir(): Promise<string> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "session-replay-"));
  tmpDirs.push(dir);
  return dir;
}

function mergeChunks(chunks: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const c of chunks) total += c.byteLength;
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    merged.set(c, offset);
    offset += c.byteLength;
  }
  return merged;
}

function sseResponse(chunks: Uint8Array[]): Response {
  return new Response(mergeChunks(chunks), {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  });
}

async function recordSingleTurnTextSession(dir: string): Promise<void> {
  const harness = createRecordingHarness({
    outputDir: dir,
    source: {
      provider: "anthropic",
      model: "claude-test",
      baseURL: "https://api.anthropic.com",
    },
    maxExchanges: 2,
    redactRequestHeaders: ["x-api-key"],
    redactResponseHeaders: [],
    fetch: async () => {
      const chunks = wire.completeResponse("anthropic", {
        text: "Hello, world!",
        headUsage: ZERO_USAGE,
        tailUsage: { ...ZERO_USAGE, output: 3 },
      });
      return sseResponse(chunks);
    },
    bypassCIGuardForTests: true,
    now: () => new Date("2026-05-25T12:00:00Z"),
  });
  let seq = 0;
  for await (const _ev of harness.runInference({
    turns: [userTurn("say hi")],
    source: ANTHROPIC_SOURCE,
    nextSeq: () => ++seq,
  })) {
    // drain
  }
  await harness.finalize();
}

async function recordToolRoundtripSession(dir: string): Promise<void> {
  let exchangeIndex = 0;
  const harness = createRecordingHarness({
    outputDir: dir,
    source: {
      provider: "anthropic",
      model: "claude-test",
      baseURL: "https://api.anthropic.com",
    },
    maxExchanges: 4,
    redactRequestHeaders: ["x-api-key"],
    redactResponseHeaders: [],
    fetch: async () => {
      if (exchangeIndex === 0) {
        exchangeIndex++;
        const chunks = wire.completeResponse("anthropic", {
          toolCalls: [
            {
              callId: "call_w_1",
              name: "weather",
              argsJSON: '{"location":"SF"}',
            },
          ],
          headUsage: ZERO_USAGE,
          tailUsage: { ...ZERO_USAGE, output: 1 },
        });
        return sseResponse(chunks);
      }
      exchangeIndex++;
      const chunks = wire.completeResponse("anthropic", {
        text: "It is 68F and foggy in SF.",
        headUsage: ZERO_USAGE,
        tailUsage: { ...ZERO_USAGE, output: 5 },
      });
      return sseResponse(chunks);
    },
    bypassCIGuardForTests: true,
    now: () => new Date("2026-05-25T12:00:00Z"),
  });
  harness.onTool("weather", () => ({
    temperatureF: 68,
    conditions: "fog",
  }));

  let seq = 0;
  const turn1Events: InferenceEvent[] = [];
  for await (const ev of harness.runInference({
    turns: [userTurn("weather in SF?")],
    source: ANTHROPIC_SOURCE,
    nextSeq: () => ++seq,
  })) {
    turn1Events.push(ev);
  }
  const turn1Done = turn1Events.find((e) => e.type === "inference.done");
  if (turn1Done === undefined || turn1Done.type !== "inference.done") {
    throw new Error("recording: expected inference.done in turn 1");
  }

  for await (const _ev of harness.runInference({
    turns: [
      userTurn("weather in SF?"),
      turn1Done.data.turn,
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_w_1",
            content: [{ type: "text", text: "68F, fog" }],
          },
        ],
        timestamp: 0,
      },
    ],
    source: ANTHROPIC_SOURCE,
    nextSeq: () => ++seq,
  })) {
    // drain
  }
  await harness.finalize();
}

describe("createReplayHarness", () => {
  test("replays a single-turn text session through runInference", async () => {
    const dir = await makeTmpDir();
    await recordSingleTurnTextSession(dir);

    const replay = await createReplayHarness({ sessionDir: dir });
    try {
      expect(replay.manifest.schemaVersion).toBe("2");
      expect(replay.source.provider).toBe("anthropic");
      expect(replay.capturedExchanges).toHaveLength(1);

      const events = await replay.runTurn({ turns: [userTurn("say hi")] });
      const done = events.find((e) => e.type === "inference.done");
      if (done === undefined || done.type !== "inference.done") {
        throw new Error("expected inference.done");
      }
      const textBlock = done.data.turn.content.find((c) => c.type === "text");
      if (textBlock === undefined || textBlock.type !== "text") {
        throw new Error("expected a text block in the final turn");
      }
      expect(textBlock.text).toBe("Hello, world!");

      replay.assertFullyConsumed();
    } finally {
      replay.dispose();
    }
  });

  test("replays a tool-roundtrip session and serves captured dispatch verbatim", async () => {
    const dir = await makeTmpDir();
    await recordToolRoundtripSession(dir);

    const replay = await createReplayHarness({ sessionDir: dir });
    try {
      expect(replay.capturedExchanges).toHaveLength(2);
      expect(replay.capturedDispatches).toHaveLength(1);
      expect(replay.capturedDispatches[0]?.toolName).toBe("weather");

      // Turn 1: drive the user prompt, observe the tool call.
      const turn1Events = await replay.runTurn({
        turns: [userTurn("weather in SF?")],
      });
      const turn1Done = turn1Events.find((e) => e.type === "inference.done");
      if (turn1Done === undefined || turn1Done.type !== "inference.done") {
        throw new Error("expected inference.done in turn 1");
      }
      const toolCall = turn1Done.data.turn.content.find(
        (c) => c.type === "tool_call",
      );
      if (toolCall === undefined || toolCall.type !== "tool_call") {
        throw new Error("expected a tool_call block in turn 1");
      }
      expect(toolCall.name).toBe("weather");

      // Turn 2: thread the tool_result block back into the user
      // turn. The recording-time helper threaded a hardcoded
      // "68F, fog" string (not `JSON.stringify(dispatch.result)`)
      // so the replay must mirror that exact string to make the
      // body-aware matcher fire.
      const turn2Events = await replay.runTurn({
        turns: [
          userTurn("weather in SF?"),
          turn1Done.data.turn,
          {
            role: "user",
            content: [
              {
                type: "tool_result",
                callId: toolCall.id,
                content: [{ type: "text", text: "68F, fog" }],
              },
            ],
            timestamp: 0,
          },
        ],
      });
      const turn2Done = turn2Events.find((e) => e.type === "inference.done");
      if (turn2Done === undefined || turn2Done.type !== "inference.done") {
        throw new Error("expected inference.done in turn 2");
      }
      const finalText = turn2Done.data.turn.content.find(
        (c) => c.type === "text",
      );
      if (finalText === undefined || finalText.type !== "text") {
        throw new Error("expected a text block in the final turn");
      }
      expect(finalText.text).toBe("It is 68F and foggy in SF.");

      replay.assertFullyConsumed();
    } finally {
      replay.dispose();
    }
  });

  test("surfaces SessionReplayMismatchError when the initial turn diverges from capture", async () => {
    const dir = await makeTmpDir();
    await recordSingleTurnTextSession(dir);

    const replay = await createReplayHarness({ sessionDir: dir });
    try {
      await expect(
        replay.runTurn({
          turns: [userTurn("a completely different prompt")],
        }),
      ).rejects.toBeInstanceOf(SessionReplayMismatchError);
    } finally {
      replay.dispose();
    }
  });

  test("rejects when session.json carries an unknown schema version", async () => {
    const dir = await makeTmpDir();
    await fs.mkdir(path.join(dir, "exchanges", "0"), { recursive: true });
    await fs.writeFile(
      path.join(dir, "session.json"),
      JSON.stringify({
        schemaVersion: "999",
        source: { provider: "x", model: "y", baseURL: "z" },
        origin: "live",
        capturedAt: "2026-05-25T12:00:00Z",
      }),
    );
    await expect(createReplayHarness({ sessionDir: dir })).rejects.toThrow(
      /Invalid capture manifest/,
    );
  });

  test("rejects when no exchanges exist", async () => {
    const dir = await makeTmpDir();
    await fs.writeFile(
      path.join(dir, "session.json"),
      JSON.stringify({
        schemaVersion: "2",
        source: { provider: "x", model: "y", baseURL: "z" },
        origin: "live",
        capturedAt: "2026-05-25T12:00:00Z",
      }),
    );
    await expect(createReplayHarness({ sessionDir: dir })).rejects.toThrow(
      /contains no exchanges/,
    );
  });

  test("rejects a raw-byte (Files-API upload) exchange the runInference driver cannot drive", async () => {
    const dir = await makeTmpDir();
    await fs.writeFile(
      path.join(dir, "session.json"),
      JSON.stringify({
        schemaVersion: "2",
        source: { provider: "x", model: "y", baseURL: "z" },
        origin: "live",
        capturedAt: "2026-05-25T12:00:00Z",
      }),
    );
    await fs.mkdir(path.join(dir, "exchanges", "0"), { recursive: true });
    await fs.writeFile(
      path.join(dir, "exchanges", "0", "request.bin"),
      new Uint8Array([1, 2, 3]),
    );
    await fs.writeFile(
      path.join(dir, "exchanges", "0", "request-headers.json"),
      "{}",
    );
    await fs.writeFile(
      path.join(dir, "exchanges", "0", "response.sse"),
      new Uint8Array(),
    );
    await fs.writeFile(
      path.join(dir, "exchanges", "0", "response-headers.json"),
      "{}",
    );
    await expect(createReplayHarness({ sessionDir: dir })).rejects.toThrow(
      /raw-byte request \(Files-API upload\).*cannot drive/s,
    );
  });

  test("rejects an exchange with both request.json and request.bin", async () => {
    const dir = await makeTmpDir();
    await fs.writeFile(
      path.join(dir, "session.json"),
      JSON.stringify({
        schemaVersion: "2",
        source: { provider: "x", model: "y", baseURL: "z" },
        origin: "live",
        capturedAt: "2026-05-25T12:00:00Z",
      }),
    );
    await fs.mkdir(path.join(dir, "exchanges", "0"), { recursive: true });
    await fs.writeFile(path.join(dir, "exchanges", "0", "request.json"), "{}");
    await fs.writeFile(
      path.join(dir, "exchanges", "0", "request.bin"),
      new Uint8Array([1, 2, 3]),
    );
    await expect(createReplayHarness({ sessionDir: dir })).rejects.toThrow(
      /both.*request\.json and request\.bin/s,
    );
  });

  test("rejects an exchange with neither request.json nor request.bin", async () => {
    const dir = await makeTmpDir();
    await fs.writeFile(
      path.join(dir, "session.json"),
      JSON.stringify({
        schemaVersion: "2",
        source: { provider: "x", model: "y", baseURL: "z" },
        origin: "live",
        capturedAt: "2026-05-25T12:00:00Z",
      }),
    );
    await fs.mkdir(path.join(dir, "exchanges", "0"), { recursive: true });
    await expect(createReplayHarness({ sessionDir: dir })).rejects.toThrow(
      /no request\.json and no request\.bin/,
    );
  });

  test("requestBytesMatch compares raw request bodies by byte equality", () => {
    expect(
      requestBytesMatch(new Uint8Array([1, 2, 3]), new Uint8Array([1, 2, 3])),
    ).toBe(true);
    expect(
      requestBytesMatch(new Uint8Array([1, 2, 3]), new Uint8Array([1, 2, 4])),
    ).toBe(false);
    expect(
      requestBytesMatch(new Uint8Array([1, 2, 3]), new Uint8Array([1, 2])),
    ).toBe(false);
    expect(requestBytesMatch(new Uint8Array(), new Uint8Array())).toBe(true);
  });

  test("rejects when an exchange has both response.sse and response.json", async () => {
    const dir = await makeTmpDir();
    await fs.writeFile(
      path.join(dir, "session.json"),
      JSON.stringify({
        schemaVersion: "2",
        source: { provider: "x", model: "y", baseURL: "z" },
        origin: "live",
        capturedAt: "2026-05-25T12:00:00Z",
      }),
    );
    await fs.mkdir(path.join(dir, "exchanges", "0"), { recursive: true });
    await fs.writeFile(path.join(dir, "exchanges", "0", "request.json"), "{}");
    await fs.writeFile(
      path.join(dir, "exchanges", "0", "request-headers.json"),
      "{}",
    );
    await fs.writeFile(
      path.join(dir, "exchanges", "0", "response.sse"),
      new Uint8Array(),
    );
    await fs.writeFile(path.join(dir, "exchanges", "0", "response.json"), "{}");
    await fs.writeFile(
      path.join(dir, "exchanges", "0", "response-headers.json"),
      "{}",
    );
    await expect(createReplayHarness({ sessionDir: dir })).rejects.toThrow(
      /both response\.sse and response\.json/,
    );
  });

  test("rejects a dispatch whose captured result collides with the delayed-envelope shape", async () => {
    const dir = await makeTmpDir();
    await fs.writeFile(
      path.join(dir, "session.json"),
      JSON.stringify({
        schemaVersion: "2",
        source: { provider: "x", model: "y", baseURL: "z" },
        origin: "live",
        capturedAt: "2026-05-25T12:00:00Z",
      }),
    );
    await fs.mkdir(path.join(dir, "exchanges", "0"), { recursive: true });
    await fs.writeFile(path.join(dir, "exchanges", "0", "request.json"), "{}");
    await fs.writeFile(
      path.join(dir, "exchanges", "0", "request-headers.json"),
      "{}",
    );
    await fs.writeFile(
      path.join(dir, "exchanges", "0", "response.sse"),
      new Uint8Array(),
    );
    await fs.writeFile(
      path.join(dir, "exchanges", "0", "response-headers.json"),
      "{}",
    );
    await fs.mkdir(path.join(dir, "dispatches"), { recursive: true });
    await fs.writeFile(
      path.join(dir, "dispatches", "0-weather.json"),
      JSON.stringify({
        args: { location: "SF" },
        result: { result: "inner", virtualDelayMs: 50 },
      }),
    );
    await expect(createReplayHarness({ sessionDir: dir })).rejects.toThrow(
      /delayed envelope/,
    );
  });

  test("serves captured dispatches for parallel tool_calls in streaming order", async () => {
    // Pins the per-tool FIFO contract the replay harness uses: when
    // two captured dispatches share a tool name, they must be served
    // back to the reactor in the order they were observed at
    // `inference.tool_call.end`. A future change to the production
    // iterator that re-orders parallel tool_calls within a single
    // message would break this — and that breakage must surface as a
    // body divergence rather than a silently-wrong dispatch result.
    const dir = await makeTmpDir();
    let exchangeIndex = 0;
    const harness = createRecordingHarness({
      outputDir: dir,
      source: {
        provider: "anthropic",
        model: "claude-test",
        baseURL: "https://api.anthropic.com",
      },
      maxExchanges: 2,
      redactRequestHeaders: [],
      redactResponseHeaders: [],
      fetch: async () => {
        if (exchangeIndex === 0) {
          exchangeIndex++;
          return sseResponse(
            wire.completeResponse("anthropic", {
              toolCalls: [
                {
                  callId: "call_a",
                  name: "lookup",
                  argsJSON: '{"key":"A"}',
                },
                {
                  callId: "call_b",
                  name: "lookup",
                  argsJSON: '{"key":"B"}',
                },
              ],
              headUsage: ZERO_USAGE,
              tailUsage: { ...ZERO_USAGE, output: 2 },
            }),
          );
        }
        exchangeIndex++;
        return sseResponse(
          wire.completeResponse("anthropic", {
            text: "done",
            headUsage: ZERO_USAGE,
            tailUsage: { ...ZERO_USAGE, output: 1 },
          }),
        );
      },
      bypassCIGuardForTests: true,
    });

    const observed: string[] = [];
    harness.onTool("lookup", (args) => {
      if (
        args !== null &&
        typeof args === "object" &&
        "key" in args &&
        typeof args.key === "string"
      ) {
        observed.push(args.key);
        return { key: args.key, value: `result-${args.key}` };
      }
      throw new Error("unexpected lookup args");
    });

    let seq = 0;
    const t1Events: InferenceEvent[] = [];
    for await (const ev of harness.runInference({
      turns: [userTurn("look up A and B")],
      source: ANTHROPIC_SOURCE,
      nextSeq: () => ++seq,
    })) {
      t1Events.push(ev);
    }
    const t1Done = t1Events.find((e) => e.type === "inference.done");
    if (t1Done === undefined || t1Done.type !== "inference.done") {
      throw new Error("expected inference.done in turn 1");
    }
    for await (const _ev of harness.runInference({
      turns: [
        userTurn("look up A and B"),
        t1Done.data.turn,
        {
          role: "user",
          content: [
            {
              type: "tool_result",
              callId: "call_a",
              content: [{ type: "text", text: "rA" }],
            },
            {
              type: "tool_result",
              callId: "call_b",
              content: [{ type: "text", text: "rB" }],
            },
          ],
          timestamp: 0,
        },
      ],
      source: ANTHROPIC_SOURCE,
      nextSeq: () => ++seq,
    })) {
      // drain
    }
    await harness.finalize();

    expect(observed).toEqual(["A", "B"]);

    const replay = await createReplayHarness({ sessionDir: dir });
    try {
      expect(replay.capturedDispatches).toHaveLength(2);
      expect(replay.capturedDispatches[0]?.args).toEqual({ key: "A" });
      expect(replay.capturedDispatches[1]?.args).toEqual({ key: "B" });

      // Drive turn 1: the body-aware matcher routes the fetch and
      // production runInference dispatches both tools. The replay
      // harness's per-tool FIFO must serve A's result first, then
      // B's. If FIFO ordering ever flips, the next turn's request
      // body would carry the wrong values for the call_a/call_b
      // tool_result blocks — surfaced as SessionReplayMismatchError
      // on the second runTurn.
      await replay.runTurn({ turns: [userTurn("look up A and B")] });
    } finally {
      replay.dispose();
    }
  });

  test("throws SessionReplayMismatchError when runTurn is called past the captured exchange count", async () => {
    const dir = await makeTmpDir();
    await recordSingleTurnTextSession(dir);
    const replay = await createReplayHarness({ sessionDir: dir });
    try {
      await replay.runTurn({ turns: [userTurn("say hi")] });
      await expect(
        replay.runTurn({ turns: [userTurn("say hi again")] }),
      ).rejects.toBeInstanceOf(SessionReplayMismatchError);
    } finally {
      replay.dispose();
    }
  });

  test("KNOWN tool overrun: queue exhaustion throws kind=dispatches_over_consumed", async () => {
    // A captured tool has N dispatches; replay-time invocation N+1
    // exhausts the per-tool queue. The queue handler throws inside
    // the inner harness's runInference iterator and the rejection
    // surfaces via `collectResult` — but it's already a
    // SessionReplayMismatchError, so runTurn re-throws it unchanged.
    const dir = await makeTmpDir();
    await recordToolRoundtripSession(dir);

    // Mutate exchange 0 to emit TWO tool_calls for "weather"; the
    // capture's dispatches/ dir still has only one entry, so the
    // second invocation exhausts the queue.
    const newResponse = mergeChunks(
      wire.completeResponse("anthropic", {
        toolCalls: [
          {
            callId: "call_w_1",
            name: "weather",
            argsJSON: '{"location":"SF"}',
          },
          {
            callId: "call_w_2",
            name: "weather",
            argsJSON: '{"location":"SF"}',
          },
        ],
        headUsage: ZERO_USAGE,
        tailUsage: { ...ZERO_USAGE, output: 2 },
      }),
    );
    await fs.writeFile(
      path.join(dir, "exchanges", "0", "response.sse"),
      newResponse,
    );

    const replay = await createReplayHarness({ sessionDir: dir });
    try {
      let err: unknown = null;
      try {
        await replay.runTurn({ turns: [userTurn("weather in SF?")] });
      } catch (e) {
        err = e;
      }
      expect(err).toBeInstanceOf(SessionReplayMismatchError);
      if (err instanceof SessionReplayMismatchError) {
        expect(err.kind).toBe("dispatches_over_consumed");
        expect(err.toolName).toBe("weather");
      }
    } finally {
      replay.dispose();
    }
  });

  test("UNKNOWN tool: 'no handler registered' error is translated to dispatches_over_consumed", async () => {
    // Production runInference emits a tool_call.end for a tool name
    // the capture has zero dispatches for. The inner harness throws
    // its "no handler was registered" error; runTurn's regex on
    // session-replay.ts pattern-matches the message and translates
    // to SessionReplayMismatchError. Pin this so a wording change to
    // the inner harness's error message surfaces as a deliberate
    // failure here rather than as a silent regression.
    const dir = await makeTmpDir();
    await recordToolRoundtripSession(dir);
    // Drop the captured dispatch directory entirely so the wire's
    // tool_call.end has no corresponding onTool handler at replay.
    await fs.rm(path.join(dir, "dispatches"), {
      recursive: true,
      force: true,
    });

    const replay = await createReplayHarness({ sessionDir: dir });
    try {
      let err: unknown = null;
      try {
        await replay.runTurn({ turns: [userTurn("weather in SF?")] });
      } catch (e) {
        err = e;
      }
      expect(err).toBeInstanceOf(SessionReplayMismatchError);
      if (err instanceof SessionReplayMismatchError) {
        expect(err.kind).toBe("dispatches_over_consumed");
        expect(err.toolName).toBe("weather");
      }
    } finally {
      replay.dispose();
    }
  });

  test("poisons the harness after a runTurn failure so retries fail fast", async () => {
    const dir = await makeTmpDir();
    await recordSingleTurnTextSession(dir);
    const replay = await createReplayHarness({ sessionDir: dir });
    try {
      await expect(
        replay.runTurn({ turns: [userTurn("wrong prompt")] }),
      ).rejects.toBeInstanceOf(SessionReplayMismatchError);
      // A subsequent runTurn must fail loudly rather than try to re-
      // register a matcher on top of the stale one from the failed
      // attempt. The error message says so explicitly.
      await expect(
        replay.runTurn({ turns: [userTurn("say hi")] }),
      ).rejects.toThrow(/poisoned/);
    } finally {
      replay.dispose();
    }
  });

  test("assertFullyConsumed throws when the caller stops short", async () => {
    const dir = await makeTmpDir();
    await recordToolRoundtripSession(dir);
    const replay = await createReplayHarness({ sessionDir: dir });
    try {
      // Only drive turn 1; never call turn 2.
      await replay.runTurn({ turns: [userTurn("weather in SF?")] });
      expect(() => {
        replay.assertFullyConsumed();
      }).toThrow(SessionReplayMismatchError);
    } finally {
      replay.dispose();
    }
  });
});

describe("replayResponsesForParsing (anything-goes)", () => {
  async function writeParserSession(
    dir: string,
    exchanges: {
      requestBin?: Uint8Array;
      responseBytes: Uint8Array;
      contentType: string;
    }[],
  ): Promise<void> {
    await fs.writeFile(
      path.join(dir, "session.json"),
      JSON.stringify({
        schemaVersion: "2",
        source: {
          provider: "anthropic",
          model: "claude-test",
          baseURL: "https://api.anthropic.com",
        },
        origin: "live",
        capturedAt: "2026-05-25T12:00:00Z",
      }),
    );
    for (let i = 0; i < exchanges.length; i++) {
      const ex = exchanges[i];
      if (ex === undefined) continue;
      const exDir = path.join(dir, "exchanges", String(i));
      await fs.mkdir(exDir, { recursive: true });
      if (ex.requestBin !== undefined) {
        await fs.writeFile(path.join(exDir, "request.bin"), ex.requestBin);
      } else {
        await fs.writeFile(path.join(exDir, "request.json"), "{}");
      }
      await fs.writeFile(path.join(exDir, "request-headers.json"), "{}");
      const respName = ex.contentType.includes("event-stream")
        ? "response.sse"
        : "response.json";
      await fs.writeFile(path.join(exDir, respName), ex.responseBytes);
      await fs.writeFile(
        path.join(exDir, "response-headers.json"),
        JSON.stringify({ "content-type": ex.contentType }),
      );
    }
  }

  test("drives a JSON exchange terminating on a tool_call without an auto-dispatch throw", async () => {
    const dir = await makeTmpDir();
    const toolCallBody = JSON.stringify({
      type: "message",
      role: "assistant",
      model: "claude-test",
      content: [
        {
          type: "tool_use",
          id: "toolu_1",
          name: "get_weather",
          input: { location: "SF" },
        },
      ],
      stop_reason: "tool_use",
      usage: { input_tokens: 5, output_tokens: 3 },
    });
    await writeParserSession(dir, [
      {
        responseBytes: new TextEncoder().encode(toolCallBody),
        contentType: "application/json",
      },
    ]);

    const results = await replayResponsesForParsing({ sessionDir: dir });
    expect(results).toHaveLength(1);
    const r = results[0];
    if (r === undefined || r.kind !== "replayed") {
      throw new Error("expected a replayed result");
    }
    const types = r.events.map((e) => e.type);
    expect(types).toContain("inference.tool_call.start");
    expect(types).toContain("inference.tool_call.end");
    expect(types).toContain("inference.done");
    expect(types).not.toContain("inference.error");
  });

  test("drives an SSE plain-text exchange, parsing text deltas", async () => {
    const dir = await makeTmpDir();
    const sseBytes = mergeChunks(
      wire.completeResponse("anthropic", { text: "Hello" }),
    );
    await writeParserSession(dir, [
      { responseBytes: sseBytes, contentType: "text/event-stream" },
    ]);

    const results = await replayResponsesForParsing({ sessionDir: dir });
    expect(results).toHaveLength(1);
    const r = results[0];
    if (r === undefined || r.kind !== "replayed") {
      throw new Error("expected a replayed result");
    }
    expect(
      r.events.filter((e) => e.type === "inference.text.delta").length,
    ).toBeGreaterThan(0);
    expect(r.events.some((e) => e.type === "inference.done")).toBe(true);
  });

  test("drives every exchange in a multi-exchange session", async () => {
    const dir = await makeTmpDir();
    await writeParserSession(dir, [
      {
        responseBytes: mergeChunks(
          wire.completeResponse("anthropic", { text: "One" }),
        ),
        contentType: "text/event-stream",
      },
      {
        responseBytes: mergeChunks(
          wire.completeResponse("anthropic", { text: "Two" }),
        ),
        contentType: "text/event-stream",
      },
    ]);

    const results = await replayResponsesForParsing({ sessionDir: dir });
    expect(results.map((r) => r.kind)).toEqual(["replayed", "replayed"]);
  });

  test("skips a raw upload exchange but replays the generate exchange", async () => {
    const dir = await makeTmpDir();
    const generateBody = JSON.stringify({
      type: "message",
      role: "assistant",
      model: "claude-test",
      content: [{ type: "text", text: "done" }],
      stop_reason: "end_turn",
      usage: { input_tokens: 5, output_tokens: 2 },
    });
    await writeParserSession(dir, [
      {
        requestBin: new Uint8Array([1, 2, 3]),
        responseBytes: new TextEncoder().encode('{"file":"x"}'),
        contentType: "application/json",
      },
      {
        responseBytes: new TextEncoder().encode(generateBody),
        contentType: "application/json",
      },
    ]);

    const results = await replayResponsesForParsing({ sessionDir: dir });
    expect(results).toHaveLength(2);
    const upload = results[0];
    const generate = results[1];
    if (upload === undefined || upload.kind !== "skipped") {
      throw new Error("expected the upload exchange to be skipped");
    }
    expect(upload.reason).toBe("raw_request");
    expect(generate?.kind).toBe("replayed");
  });

  test("surfaces a parse failure as one inference.error without re-fetching", async () => {
    const dir = await makeTmpDir();
    await writeParserSession(dir, [
      {
        responseBytes: new TextEncoder().encode("this is not json at all"),
        contentType: "application/json",
      },
    ]);

    // A malformed body must not trip the fetchCallCount === 1 guard: the
    // abort-only retry policy surfaces the parse failure as a single
    // inference.error rather than a retry that re-opens the fetch.
    const results = await replayResponsesForParsing({ sessionDir: dir });
    expect(results).toHaveLength(1);
    const r = results[0];
    if (r === undefined || r.kind !== "replayed") {
      throw new Error("expected a replayed result");
    }
    expect(r.events.filter((e) => e.type === "inference.error")).toHaveLength(
      1,
    );
    expect(r.events.some((e) => e.type === "inference.done")).toBe(false);
  });

  test("applies the provided invariants over each replayed exchange's decoded events", async () => {
    const dir = await makeTmpDir();
    const toolCallBody = JSON.stringify({
      type: "message",
      role: "assistant",
      model: "claude-test",
      content: [
        {
          type: "tool_use",
          id: "toolu_1",
          name: "get_weather",
          input: { location: "SF" },
        },
      ],
      stop_reason: "tool_use",
      usage: { input_tokens: 5, output_tokens: 3 },
    });
    await writeParserSession(dir, [
      {
        responseBytes: new TextEncoder().encode(toolCallBody),
        contentType: "application/json",
      },
    ]);

    const seenBatches: InferenceEvent[][] = [];
    const recordingInvariant: Invariant = {
      name: "recording_probe",
      check(events) {
        seenBatches.push([...events]);
        return [
          {
            invariant: "recording_probe",
            message: "always reports",
            events: [],
          },
        ];
      },
    };

    const results = await replayResponsesForParsing({
      sessionDir: dir,
      invariants: [recordingInvariant],
    });
    expect(results).toHaveLength(1);
    const r = results[0];
    if (r === undefined || r.kind !== "replayed") {
      throw new Error("expected a replayed result");
    }

    // The invariant ran over the real decoded event stream, not an empty array.
    const firstBatch = seenBatches[0];
    if (firstBatch === undefined) {
      throw new Error("expected the invariant to have been invoked once");
    }
    const seenTypes = firstBatch.map((e) => e.type);
    expect(seenTypes).toContain("inference.tool_call.end");
    expect(seenTypes).toContain("inference.done");

    // Its violation surfaces on the replayed result.
    expect(r.violations).toHaveLength(1);
    expect(r.violations[0]?.invariant).toBe("recording_probe");
  });

  test("reports no violations for a clean capture under the default invariants", async () => {
    const dir = await makeTmpDir();
    const sseBytes = mergeChunks(
      wire.completeResponse("anthropic", { text: "Hello" }),
    );
    await writeParserSession(dir, [
      { responseBytes: sseBytes, contentType: "text/event-stream" },
    ]);

    const results = await replayResponsesForParsing({ sessionDir: dir });
    expect(results).toHaveLength(1);
    const r = results[0];
    if (r === undefined || r.kind !== "replayed") {
      throw new Error("expected a replayed result");
    }
    expect(r.violations).toEqual([]);
  });
});
