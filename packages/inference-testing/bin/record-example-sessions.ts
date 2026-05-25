// Regenerate the committed example session captures under
// `packages/inference-testing/sessions/`.
//
// The script uses the test seam (`fetch` override paired with
// `bypassCIGuardForTests`) and synthetic provider wire bytes built with
// the package's `wire` helpers, so no real provider credentials are
// needed. The resulting sessions are deterministic and committed as
// fixtures for the integration test under
// `tests/inference-testing/session-replay.test.ts`.
//
// To regenerate after a manifest-schema or capture-format change:
//
//     bun packages/inference-testing/bin/record-example-sessions.ts

import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import type {
  ConversationTurn,
  InferenceEvent,
  InferenceSource,
} from "@intx/types/runtime";

import { createRecordingHarness } from "../src/session-recording";
import * as wire from "../src/wire";

const ZERO_USAGE = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

const FROZEN_NOW = (): Date => new Date("2026-05-25T12:00:00Z");

const ANTHROPIC_SOURCE: InferenceSource = {
  id: "anthropic:claude-test",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "session-replay-stub",
  model: "claude-test",
};

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const SESSIONS_ROOT = path.resolve(__dirname, "..", "sessions");

function userTurn(text: string): ConversationTurn {
  return {
    role: "user",
    content: [{ type: "text", text }],
    timestamp: 0,
  };
}

function sseResponse(chunks: Uint8Array[]): Response {
  let total = 0;
  for (const c of chunks) total += c.byteLength;
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    merged.set(c, offset);
    offset += c.byteLength;
  }
  return new Response(merged, {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  });
}

function expectDone(
  events: InferenceEvent[],
  label: string,
): InferenceEvent & { type: "inference.done" } {
  const done = events.find((e) => e.type === "inference.done");
  if (done === undefined || done.type !== "inference.done") {
    throw new Error(`${label}: expected inference.done event`);
  }
  return done;
}

async function recordToolRoundtrip(): Promise<void> {
  const dir = path.join(SESSIONS_ROOT, "anthropic-tool-roundtrip");
  await fs.rm(dir, { recursive: true, force: true });

  let exchangeIndex = 0;
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
      if (exchangeIndex === 0) {
        exchangeIndex++;
        return sseResponse(
          wire.completeResponse("anthropic", {
            toolCalls: [
              {
                callId: "call_weather_1",
                name: "weather",
                argsJSON: '{"location":"SF"}',
              },
            ],
            headUsage: ZERO_USAGE,
            tailUsage: { ...ZERO_USAGE, output: 1 },
          }),
        );
      }
      exchangeIndex++;
      return sseResponse(
        wire.completeResponse("anthropic", {
          text: "It is 68F and foggy in SF.",
          headUsage: ZERO_USAGE,
          tailUsage: { ...ZERO_USAGE, output: 8 },
        }),
      );
    },
    bypassCIGuardForTests: true,
    now: FROZEN_NOW,
  });
  harness.onTool("weather", () => ({
    temperatureF: 68,
    conditions: "fog",
  }));

  let seq = 0;
  const turn1Events: InferenceEvent[] = [];
  for await (const ev of harness.runInference({
    turns: [userTurn("What is the weather in SF?")],
    source: ANTHROPIC_SOURCE,
    nextSeq: () => ++seq,
  })) {
    turn1Events.push(ev);
  }
  const turn1Done = expectDone(turn1Events, "tool-roundtrip turn 1");

  for await (const _ev of harness.runInference({
    turns: [
      userTurn("What is the weather in SF?"),
      turn1Done.data.turn,
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_weather_1",
            content: [
              {
                type: "text",
                text: JSON.stringify({
                  temperatureF: 68,
                  conditions: "fog",
                }),
              },
            ],
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
  // eslint-disable-next-line no-console -- this is a CLI script
  console.log(`Recorded: ${dir}`);
}

async function recordMultiToolMultiTurn(): Promise<void> {
  const dir = path.join(SESSIONS_ROOT, "anthropic-multi-tool-multi-turn");
  await fs.rm(dir, { recursive: true, force: true });

  // Plan:
  //   Turn 1: user asks "weather SF and time UTC?"
  //   Response 1: tool_call weather(SF) + tool_call time(UTC)
  //   Dispatches 0,1: weather→68F fog, time→12:00Z
  //   Turn 2: assistant tool_calls + user tool_results
  //   Response 2: "SF: 68F foggy at 12:00 UTC. Anything else?"
  //   Turn 3: user asks "what about NYC?"
  //   Response 3: tool_call weather(NYC)
  //   Dispatch 2: weather → 55F rain
  //   Turn 4: assistant tool_call + user tool_result
  //   Response 4: "NYC: 55F and rainy."
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
      const index = exchangeIndex++;
      if (index === 0) {
        return sseResponse(
          wire.completeResponse("anthropic", {
            toolCalls: [
              {
                callId: "call_w_1",
                name: "weather",
                argsJSON: '{"location":"SF"}',
              },
              {
                callId: "call_t_1",
                name: "time",
                argsJSON: '{"zone":"UTC"}',
              },
            ],
            headUsage: ZERO_USAGE,
            tailUsage: { ...ZERO_USAGE, output: 2 },
          }),
        );
      }
      if (index === 1) {
        return sseResponse(
          wire.completeResponse("anthropic", {
            text: "SF: 68F and foggy at 12:00 UTC. Anything else?",
            headUsage: ZERO_USAGE,
            tailUsage: { ...ZERO_USAGE, output: 14 },
          }),
        );
      }
      if (index === 2) {
        return sseResponse(
          wire.completeResponse("anthropic", {
            toolCalls: [
              {
                callId: "call_w_2",
                name: "weather",
                argsJSON: '{"location":"NYC"}',
              },
            ],
            headUsage: ZERO_USAGE,
            tailUsage: { ...ZERO_USAGE, output: 1 },
          }),
        );
      }
      return sseResponse(
        wire.completeResponse("anthropic", {
          text: "NYC: 55F and rainy.",
          headUsage: ZERO_USAGE,
          tailUsage: { ...ZERO_USAGE, output: 6 },
        }),
      );
    },
    bypassCIGuardForTests: true,
    now: FROZEN_NOW,
  });

  const weatherResults: Record<string, unknown> = {
    SF: { temperatureF: 68, conditions: "fog" },
    NYC: { temperatureF: 55, conditions: "rain" },
  };
  harness.onTool("weather", (args) => {
    if (
      args !== null &&
      typeof args === "object" &&
      "location" in args &&
      typeof args.location === "string"
    ) {
      const result = weatherResults[args.location];
      if (result !== undefined) return result;
    }
    throw new Error(
      `recording: unexpected weather args: ${JSON.stringify(args)}`,
    );
  });
  harness.onTool("time", () => ({ iso: "2026-05-25T12:00:00Z" }));

  let seq = 0;
  const t1Events: InferenceEvent[] = [];
  for await (const ev of harness.runInference({
    turns: [userTurn("Weather in SF and the time in UTC?")],
    source: ANTHROPIC_SOURCE,
    nextSeq: () => ++seq,
  })) {
    t1Events.push(ev);
  }
  const t1Done = expectDone(t1Events, "multi-tool turn 1");

  const t2Events: InferenceEvent[] = [];
  for await (const ev of harness.runInference({
    turns: [
      userTurn("Weather in SF and the time in UTC?"),
      t1Done.data.turn,
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_w_1",
            content: [
              {
                type: "text",
                text: JSON.stringify({
                  temperatureF: 68,
                  conditions: "fog",
                }),
              },
            ],
          },
          {
            type: "tool_result",
            callId: "call_t_1",
            content: [
              {
                type: "text",
                text: JSON.stringify({ iso: "2026-05-25T12:00:00Z" }),
              },
            ],
          },
        ],
        timestamp: 0,
      },
    ],
    source: ANTHROPIC_SOURCE,
    nextSeq: () => ++seq,
  })) {
    t2Events.push(ev);
  }
  const t2Done = expectDone(t2Events, "multi-tool turn 2");

  const t3Events: InferenceEvent[] = [];
  for await (const ev of harness.runInference({
    turns: [
      userTurn("Weather in SF and the time in UTC?"),
      t1Done.data.turn,
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_w_1",
            content: [
              {
                type: "text",
                text: JSON.stringify({
                  temperatureF: 68,
                  conditions: "fog",
                }),
              },
            ],
          },
          {
            type: "tool_result",
            callId: "call_t_1",
            content: [
              {
                type: "text",
                text: JSON.stringify({ iso: "2026-05-25T12:00:00Z" }),
              },
            ],
          },
        ],
        timestamp: 0,
      },
      t2Done.data.turn,
      userTurn("What about NYC?"),
    ],
    source: ANTHROPIC_SOURCE,
    nextSeq: () => ++seq,
  })) {
    t3Events.push(ev);
  }
  const t3Done = expectDone(t3Events, "multi-tool turn 3");

  for await (const _ev of harness.runInference({
    turns: [
      userTurn("Weather in SF and the time in UTC?"),
      t1Done.data.turn,
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_w_1",
            content: [
              {
                type: "text",
                text: JSON.stringify({
                  temperatureF: 68,
                  conditions: "fog",
                }),
              },
            ],
          },
          {
            type: "tool_result",
            callId: "call_t_1",
            content: [
              {
                type: "text",
                text: JSON.stringify({ iso: "2026-05-25T12:00:00Z" }),
              },
            ],
          },
        ],
        timestamp: 0,
      },
      t2Done.data.turn,
      userTurn("What about NYC?"),
      t3Done.data.turn,
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_w_2",
            content: [
              {
                type: "text",
                text: JSON.stringify({
                  temperatureF: 55,
                  conditions: "rain",
                }),
              },
            ],
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
  // eslint-disable-next-line no-console -- this is a CLI script
  console.log(`Recorded: ${dir}`);
}

async function main(): Promise<void> {
  await fs.mkdir(SESSIONS_ROOT, { recursive: true });
  await recordToolRoundtrip();
  await recordMultiToolMultiTurn();
}

await main();
