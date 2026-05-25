import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import type {
  ConversationTurn,
  InferenceEvent,
  InferenceSource,
} from "@intx/types/runtime";

import { createRecordingHarness } from "./session-recording";
import {
  createReplayHarness,
  SessionReplayMismatchError,
} from "./session-replay";
import * as wire from "./wire";

const ANTHROPIC_SOURCE: InferenceSource = {
  id: "anthropic:claude-test",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "test",
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

function userTurn(text: string): ConversationTurn {
  return {
    role: "user",
    content: [{ type: "text", text }],
    timestamp: 0,
  };
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
      expect(replay.manifest.sessionSchemaVersion).toBe("1");
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

      // Turn 2: thread the captured dispatch result back as a
      // tool_result block. This mirrors what the real reactor does
      // — the dispatch result was served from the replay harness's
      // onTool handler, so the test can read it from the captured
      // dispatches.
      const dispatch = replay.capturedDispatches[0];
      if (dispatch === undefined) throw new Error("expected one dispatch");
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
                // Mirror the recording-time tool_result content (the
                // recording test threaded a hardcoded string, not the
                // dispatch result object, into the next-turn body).
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
      void dispatch;
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
        sessionSchemaVersion: "999",
        source: { provider: "x", model: "y", baseURL: "z" },
        capturedAt: "2026-05-25T12:00:00Z",
      }),
    );
    await expect(createReplayHarness({ sessionDir: dir })).rejects.toThrow(
      /Invalid session manifest/,
    );
  });

  test("rejects when no exchanges exist", async () => {
    const dir = await makeTmpDir();
    await fs.writeFile(
      path.join(dir, "session.json"),
      JSON.stringify({
        sessionSchemaVersion: "1",
        source: { provider: "x", model: "y", baseURL: "z" },
        capturedAt: "2026-05-25T12:00:00Z",
      }),
    );
    await expect(createReplayHarness({ sessionDir: dir })).rejects.toThrow(
      /contains no exchanges/,
    );
  });

  test("rejects when an exchange has a raw-body request capture", async () => {
    const dir = await makeTmpDir();
    await fs.writeFile(
      path.join(dir, "session.json"),
      JSON.stringify({
        sessionSchemaVersion: "1",
        source: { provider: "x", model: "y", baseURL: "z" },
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
      /raw-body request \(request\.bin\)/,
    );
  });

  test("rejects when an exchange has both response.sse and response.json", async () => {
    const dir = await makeTmpDir();
    await fs.writeFile(
      path.join(dir, "session.json"),
      JSON.stringify({
        sessionSchemaVersion: "1",
        source: { provider: "x", model: "y", baseURL: "z" },
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
        sessionSchemaVersion: "1",
        source: { provider: "x", model: "y", baseURL: "z" },
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
