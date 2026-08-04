import { describe, test, expect, afterEach } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import * as path from "node:path";

import { loadCaptureManifest } from "@intx/inference-discovery/catalog";
import {
  createRecordingHarness,
  createReplayHarness,
  replayResponsesForParsing,
  userTurn,
  wire,
} from "@intx/inference-testing";
import type { InferenceEvent, InferenceSource } from "@intx/types/runtime";

import {
  convertWireCapability,
  convertWireToSession,
} from "./convert-wire-to-session";

const REPO_ROOT = path.resolve(import.meta.dirname, "..");

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
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "wire-convert-"));
  tmpDirs.push(dir);
  return dir;
}

// Write one wire leaf (a directory holding a request/response pair). A JSON
// request is stringified; a raw request is written verbatim as request.bin.
async function writeLeaf(
  leafDir: string,
  opts: {
    requestBody?: unknown;
    requestBin?: Uint8Array;
    responseBytes?: Uint8Array;
    contentType?: string;
  },
): Promise<void> {
  await fs.mkdir(leafDir, { recursive: true });
  if (opts.requestBin !== undefined) {
    await fs.writeFile(path.join(leafDir, "request.bin"), opts.requestBin);
  } else {
    await fs.writeFile(
      path.join(leafDir, "request.json"),
      JSON.stringify(opts.requestBody),
    );
  }
  await fs.writeFile(path.join(leafDir, "request-headers.json"), "{}");
  const contentType = opts.contentType ?? "application/json";
  const responseName = contentType.includes("event-stream")
    ? "response.sse"
    : "response.json";
  await fs.writeFile(
    path.join(leafDir, responseName),
    opts.responseBytes ?? new TextEncoder().encode("{}"),
  );
  await fs.writeFile(
    path.join(leafDir, "response-headers.json"),
    JSON.stringify({ "content-type": contentType }),
  );
}

async function readDispatch(
  sessionDir: string,
  filename: string,
): Promise<unknown> {
  const raw = await fs.readFile(
    path.join(sessionDir, "dispatches", filename),
    "utf-8",
  );
  return JSON.parse(raw);
}

async function listDir(dir: string): Promise<string[]> {
  try {
    return (await fs.readdir(dir)).sort();
  } catch (cause) {
    const code =
      cause !== null && typeof cause === "object" && "code" in cause
        ? cause.code
        : undefined;
    if (code === "ENOENT") return [];
    throw cause;
  }
}

const SYNTHETIC_SOURCE = {
  provider: "anthropic",
  model: "claude-test",
  baseURL: "https://api.anthropic.com",
};

describe("convertWireToSession dispatch reconstruction", () => {
  test("pairs an Anthropic tool_use with its tool_result by id", async () => {
    const wireDir = await makeTmpDir();
    const sessionDir = await makeTmpDir();
    await writeLeaf(wireDir, {
      requestBody: {
        messages: [
          { role: "user", content: "weather?" },
          {
            role: "assistant",
            content: [
              {
                type: "tool_use",
                id: "tu_1",
                name: "get_weather",
                input: { location: "SF" },
              },
            ],
          },
          {
            role: "user",
            content: [
              {
                type: "tool_result",
                tool_use_id: "tu_1",
                content: '{"tempF":70}',
              },
            ],
          },
        ],
      },
    });

    await convertWireToSession({
      wireDir,
      sessionDir,
      source: SYNTHETIC_SOURCE,
      origin: "live",
      capturedAt: "2026-05-25T12:00:00Z",
    });

    expect(await listDir(path.join(sessionDir, "dispatches"))).toEqual([
      "0-get_weather.json",
    ]);
    expect(await readDispatch(sessionDir, "0-get_weather.json")).toEqual({
      args: { location: "SF" },
      result: '{"tempF":70}',
    });
  });

  test("parses OpenAI tool_call arguments and keeps the tool result string", async () => {
    const wireDir = await makeTmpDir();
    const sessionDir = await makeTmpDir();
    await writeLeaf(wireDir, {
      requestBody: {
        messages: [
          { role: "user", content: "weather?" },
          {
            role: "assistant",
            tool_calls: [
              {
                id: "call_1",
                type: "function",
                function: {
                  name: "get_weather",
                  arguments: '{"location":"SF"}',
                },
              },
            ],
          },
          {
            role: "tool",
            tool_call_id: "call_1",
            content: '{"tempF":70}',
          },
        ],
      },
    });

    await convertWireToSession({
      wireDir,
      sessionDir,
      source: { ...SYNTHETIC_SOURCE, provider: "openai-compatible" },
      origin: "live",
      capturedAt: "2026-05-25T12:00:00Z",
    });

    expect(await readDispatch(sessionDir, "0-get_weather.json")).toEqual({
      args: { location: "SF" },
      result: '{"tempF":70}',
    });
  });

  test("pairs Google functionCall/functionResponse by position", async () => {
    const wireDir = await makeTmpDir();
    const sessionDir = await makeTmpDir();
    await writeLeaf(wireDir, {
      requestBody: {
        contents: [
          { role: "user", parts: [{ text: "weather?" }] },
          {
            role: "model",
            parts: [
              {
                functionCall: { name: "get_weather", args: { location: "SF" } },
              },
            ],
          },
          {
            role: "user",
            parts: [
              {
                functionResponse: {
                  name: "get_weather",
                  response: { tempF: 70 },
                },
              },
            ],
          },
        ],
      },
    });

    await convertWireToSession({
      wireDir,
      sessionDir,
      source: { ...SYNTHETIC_SOURCE, provider: "google-genai" },
      origin: "live",
      capturedAt: "2026-05-25T12:00:00Z",
    });

    expect(await readDispatch(sessionDir, "0-get_weather.json")).toEqual({
      args: { location: "SF" },
      result: { tempF: 70 },
    });
  });

  test("pairs two Google calls of the same tool name by order, not name", async () => {
    const wireDir = await makeTmpDir();
    const sessionDir = await makeTmpDir();
    await writeLeaf(wireDir, {
      requestBody: {
        contents: [
          { role: "user", parts: [{ text: "weather in SF and NY?" }] },
          {
            role: "model",
            parts: [
              {
                functionCall: { name: "get_weather", args: { location: "SF" } },
              },
              {
                functionCall: { name: "get_weather", args: { location: "NY" } },
              },
            ],
          },
          {
            role: "user",
            parts: [
              {
                functionResponse: {
                  name: "get_weather",
                  response: { city: "SF" },
                },
              },
              {
                functionResponse: {
                  name: "get_weather",
                  response: { city: "NY" },
                },
              },
            ],
          },
        ],
      },
    });

    await convertWireToSession({
      wireDir,
      sessionDir,
      source: { ...SYNTHETIC_SOURCE, provider: "google-genai" },
      origin: "live",
      capturedAt: "2026-05-25T12:00:00Z",
    });

    expect(await listDir(path.join(sessionDir, "dispatches"))).toEqual([
      "0-get_weather.json",
      "1-get_weather.json",
    ]);
    expect(await readDispatch(sessionDir, "0-get_weather.json")).toEqual({
      args: { location: "SF" },
      result: { city: "SF" },
    });
    expect(await readDispatch(sessionDir, "1-get_weather.json")).toEqual({
      args: { location: "NY" },
      result: { city: "NY" },
    });
  });

  test("throws when a tool_result has no matching tool_call", async () => {
    const wireDir = await makeTmpDir();
    const sessionDir = await makeTmpDir();
    await writeLeaf(wireDir, {
      requestBody: {
        messages: [
          {
            role: "user",
            content: [
              { type: "tool_result", tool_use_id: "orphan", content: "{}" },
            ],
          },
        ],
      },
    });

    await expect(
      convertWireToSession({
        wireDir,
        sessionDir,
        source: SYNTHETIC_SOURCE,
        origin: "live",
        capturedAt: "2026-05-25T12:00:00Z",
      }),
    ).rejects.toThrow(/no matching tool_call/);
  });

  test("throws when a Google functionResponse name diverges from its call", async () => {
    const wireDir = await makeTmpDir();
    const sessionDir = await makeTmpDir();
    await writeLeaf(wireDir, {
      requestBody: {
        contents: [
          {
            role: "model",
            parts: [
              {
                functionCall: { name: "get_weather", args: { location: "SF" } },
              },
            ],
          },
          {
            role: "user",
            parts: [
              { functionResponse: { name: "get_time", response: { hour: 9 } } },
            ],
          },
        ],
      },
    });

    await expect(
      convertWireToSession({
        wireDir,
        sessionDir,
        source: { ...SYNTHETIC_SOURCE, provider: "google-genai" },
        origin: "live",
        capturedAt: "2026-05-25T12:00:00Z",
      }),
    ).rejects.toThrow(/names .* but the functionCall names/);
  });

  test("throws when a reconstructed tool name is not a bare identifier", async () => {
    const wireDir = await makeTmpDir();
    const sessionDir = await makeTmpDir();
    await writeLeaf(wireDir, {
      requestBody: {
        messages: [
          {
            role: "assistant",
            content: [
              {
                type: "tool_use",
                id: "tu_1",
                name: "evil/../escape",
                input: {},
              },
            ],
          },
          {
            role: "user",
            content: [
              { type: "tool_result", tool_use_id: "tu_1", content: "{}" },
            ],
          },
        ],
      },
    });

    await expect(
      convertWireToSession({
        wireDir,
        sessionDir,
        source: SYNTHETIC_SOURCE,
        origin: "live",
        capturedAt: "2026-05-25T12:00:00Z",
      }),
    ).rejects.toThrow(/not a bare identifier/);
  });

  test("reconstructs no dispatches for a single-turn tool_call capture", async () => {
    const wireDir = await makeTmpDir();
    const sessionDir = await makeTmpDir();
    // A single-turn function-calling capture: the request carries only the
    // user prompt; the assistant's tool_use lives in the response, which the
    // converter copies but never reads for dispatches.
    await writeLeaf(wireDir, {
      requestBody: { messages: [{ role: "user", content: "weather?" }] },
    });

    await convertWireToSession({
      wireDir,
      sessionDir,
      source: SYNTHETIC_SOURCE,
      origin: "live",
      capturedAt: "2026-05-25T12:00:00Z",
    });

    expect(await listDir(path.join(sessionDir, "dispatches"))).toEqual([]);
    expect(await listDir(path.join(sessionDir, "exchanges"))).toEqual(["0"]);
  });
});

describe("convertWireCapability", () => {
  test("reads the wire manifest and maps the catalog provider to its adapter", async () => {
    const wireDir = await makeTmpDir();
    const sessionDir = await makeTmpDir();
    await fs.writeFile(
      path.join(wireDir, "manifest.json"),
      JSON.stringify({
        provider: "opencode-zen",
        model: "mimo-v2.5",
        capability: "function-calling",
        capturedAt: "2026-07-22T16:52:24.465Z",
        observedModelVersion: "mimo-v2.5-0722",
        schemaVersion: "1",
      }),
    );
    await writeLeaf(wireDir, {
      requestBody: { messages: [{ role: "user", content: "hi" }] },
    });

    await convertWireCapability({
      wireDir,
      sessionDir,
      baseURL: "https://opencode.ai/zen/v1",
      origin: "live",
    });

    const manifest = await loadCaptureManifest(sessionDir);
    expect(manifest.source.provider).toBe("openai-compatible");
    expect(manifest.source.model).toBe("mimo-v2.5");
    expect(manifest.source.baseURL).toBe("https://opencode.ai/zen/v1");
    expect(manifest.capability).toBe("function-calling");
    expect(manifest.observedModelVersion).toBe("mimo-v2.5-0722");
    expect(manifest.origin).toBe("live");
  });
});

describe("convertWireToSession exchange layout", () => {
  test("orders a files-api upload before its generate exchange and skips its raw body for dispatches", async () => {
    const wireDir = await makeTmpDir();
    const sessionDir = await makeTmpDir();
    await writeLeaf(path.join(wireDir, "upload"), {
      requestBin: new Uint8Array([1, 2, 3]),
      responseBytes: new TextEncoder().encode('{"file":"f_1"}'),
    });
    await writeLeaf(path.join(wireDir, "generate"), {
      requestBody: { messages: [{ role: "user", content: "describe f_1" }] },
    });

    await convertWireToSession({
      wireDir,
      sessionDir,
      source: SYNTHETIC_SOURCE,
      origin: "live",
      capturedAt: "2026-05-25T12:00:00Z",
    });

    expect(await listDir(path.join(sessionDir, "exchanges"))).toEqual([
      "0",
      "1",
    ]);
    expect(await listDir(path.join(sessionDir, "exchanges", "0"))).toContain(
      "request.bin",
    );
    expect(await listDir(path.join(sessionDir, "exchanges", "1"))).toContain(
      "request.json",
    );
    expect(await listDir(path.join(sessionDir, "dispatches"))).toEqual([]);
  });

  test("copies exchange payloads verbatim and writes a valid v2 manifest", async () => {
    const wireDir = await makeTmpDir();
    const sessionDir = await makeTmpDir();
    const requestBody = { messages: [{ role: "user", content: "hi" }] };
    const responseBytes = new TextEncoder().encode('{"ok":true}');
    await writeLeaf(wireDir, { requestBody, responseBytes });

    await convertWireToSession({
      wireDir,
      sessionDir,
      source: SYNTHETIC_SOURCE,
      origin: "live",
      capturedAt: "2026-05-25T12:00:00Z",
      capability: "function-calling",
      observedModelVersion: "claude-test-20260525",
    });

    const copiedRequest = await fs.readFile(
      path.join(sessionDir, "exchanges", "0", "request.json"),
    );
    expect(new Uint8Array(copiedRequest)).toEqual(
      new Uint8Array(await fs.readFile(path.join(wireDir, "request.json"))),
    );
    const copiedResponse = await fs.readFile(
      path.join(sessionDir, "exchanges", "0", "response.json"),
    );
    expect(new Uint8Array(copiedResponse)).toEqual(responseBytes);

    const manifest = await loadCaptureManifest(sessionDir);
    expect(manifest.schemaVersion).toBe("2");
    expect(manifest.origin).toBe("live");
    expect(manifest.source).toEqual(SYNTHETIC_SOURCE);
    expect(manifest.capability).toBe("function-calling");
    expect(manifest.observedModelVersion).toBe("claude-test-20260525");
  });
});

// Real committed multi-turn fixtures, sampled one per provider family. The
// reconstructed dispatch record is asserted against literals read by hand from
// the fixture — never recomputed with the converter's own extractor — so the
// assertion proves the frozen-on-disk encoding matches the wire, not that the
// extractor equals itself.
describe("convertWireToSession against real wire fixtures", () => {
  const REAL_FIXTURES = [
    {
      name: "anthropic",
      provider: "anthropic",
      wire: "packages/inference-discovery-anthropic/wire/anthropic/claude-haiku-4-5-20251001/function-calling-multi-turn",
      expected: {
        args: { location: "Boston, MA" },
        result:
          '{"location":"Boston, MA","temperatureF":68,"conditions":"clear"}',
      },
    },
    {
      name: "opencode-zen",
      provider: "openai-compatible",
      wire: "packages/inference-discovery-openai/wire/opencode-zen/mimo-v2.5/function-calling-multi-turn",
      expected: {
        args: { location: "Boston, MA" },
        result:
          '{"location":"Boston, MA","temperatureF":68,"conditions":"clear"}',
      },
    },
    {
      name: "google-genai",
      provider: "google-genai",
      wire: "packages/inference-discovery-google-genai/wire/google-genai/gemini-2.5-pro/function-calling-multi-turn",
      expected: {
        args: { location: "Boston, MA" },
        result: {
          location: "Boston, MA",
          temperatureF: 68,
          conditions: "clear",
        },
      },
    },
  ] as const;

  for (const fixture of REAL_FIXTURES) {
    test(`reconstructs the ${fixture.name} dispatch to match the wire bytes`, async () => {
      const sessionDir = await makeTmpDir();
      await convertWireToSession({
        wireDir: path.join(REPO_ROOT, fixture.wire),
        sessionDir,
        source: {
          provider: fixture.provider,
          model: "fixture-model",
          baseURL: "https://fixture.invalid",
        },
        origin: "live",
        capturedAt: "2026-07-22T16:52:24.465Z",
      });

      // The single dispatch is named for the reconstructed tool, indexed 0.
      expect(await listDir(path.join(sessionDir, "dispatches"))).toEqual([
        "0-get_weather.json",
      ]);
      expect(await readDispatch(sessionDir, "0-get_weather.json")).toEqual(
        fixture.expected,
      );
    });

    test(`decodes every ${fixture.name} exchange through the real adapter`, async () => {
      const sessionDir = await makeTmpDir();
      await convertWireToSession({
        wireDir: path.join(REPO_ROOT, fixture.wire),
        sessionDir,
        source: {
          provider: fixture.provider,
          model: "fixture-model",
          baseURL: "https://fixture.invalid",
        },
        origin: "live",
        capturedAt: "2026-07-22T16:52:24.465Z",
      });

      const results = await replayResponsesForParsing({ sessionDir });
      expect(results.length).toBeGreaterThan(0);
      for (const result of results) {
        expect(result.kind).toBe("replayed");
      }
    });
  }

  // The converted Anthropic session is deliberately NOT driven through the
  // whole-request body match of createReplayHarness. The inference adapter
  // serializes a tool_result as an array of text blocks
  // (content: [{type:"text",text}]), whereas the captured Anthropic wire
  // carries the tool_result content as a bare string — both are valid
  // Anthropic API shapes. A canonical body match on the tool_result turn
  // therefore diverges for reasons that have nothing to do with a byte-faithful
  // converter, so exchange decoding is verified through the parser path above
  // instead.
});

// Records a real, adapter-consistent multi-turn session (the same tool called
// twice), then reshapes it into the wire-leaf layout so the converter's own
// output can be driven back through the strict replay harness.
async function recordRepeatedToolSession(dir: string): Promise<void> {
  let exchangeIndex = 0;
  const harness = createRecordingHarness({
    outputDir: dir,
    source: {
      provider: "anthropic",
      model: "claude-test",
      baseURL: "https://api.anthropic.com",
    },
    maxExchanges: 3,
    redactRequestHeaders: ["x-api-key"],
    redactResponseHeaders: [],
    fetch: async () => {
      if (exchangeIndex === 0) {
        exchangeIndex++;
        return new Response(
          mergeChunks(
            wire.completeResponse("anthropic", {
              toolCalls: [
                {
                  callId: "call_1",
                  name: "weather",
                  argsJSON: '{"city":"SF"}',
                },
              ],
              headUsage: ZERO_USAGE,
              tailUsage: { ...ZERO_USAGE, output: 1 },
            }),
          ),
          { status: 200, headers: { "content-type": "text/event-stream" } },
        );
      }
      if (exchangeIndex === 1) {
        exchangeIndex++;
        return new Response(
          mergeChunks(
            wire.completeResponse("anthropic", {
              toolCalls: [
                {
                  callId: "call_2",
                  name: "weather",
                  argsJSON: '{"city":"NY"}',
                },
              ],
              headUsage: ZERO_USAGE,
              tailUsage: { ...ZERO_USAGE, output: 1 },
            }),
          ),
          { status: 200, headers: { "content-type": "text/event-stream" } },
        );
      }
      exchangeIndex++;
      return new Response(
        mergeChunks(
          wire.completeResponse("anthropic", {
            text: "SF is foggy and NY is clear.",
            headUsage: ZERO_USAGE,
            tailUsage: { ...ZERO_USAGE, output: 5 },
          }),
        ),
        { status: 200, headers: { "content-type": "text/event-stream" } },
      );
    },
    bypassCIGuardForTests: true,
    now: () => new Date("2026-05-25T12:00:00Z"),
  });
  harness.onTool("weather", () => ({ conditions: "known" }));

  let seq = 0;
  const t1: InferenceEvent[] = [];
  for await (const ev of harness.runInference({
    turns: [userTurn("weather in SF and NY?")],
    source: ANTHROPIC_SOURCE,
    nextSeq: () => ++seq,
  })) {
    t1.push(ev);
  }
  const t1Done = t1.find((e) => e.type === "inference.done");
  if (t1Done === undefined || t1Done.type !== "inference.done") {
    throw new Error("recording: expected inference.done in turn 1");
  }

  const t2: InferenceEvent[] = [];
  for await (const ev of harness.runInference({
    turns: [
      userTurn("weather in SF and NY?"),
      t1Done.data.turn,
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_1",
            content: [{ type: "text", text: "SF-fog" }],
          },
        ],
        timestamp: 0,
      },
    ],
    source: ANTHROPIC_SOURCE,
    nextSeq: () => ++seq,
  })) {
    t2.push(ev);
  }
  const t2Done = t2.find((e) => e.type === "inference.done");
  if (t2Done === undefined || t2Done.type !== "inference.done") {
    throw new Error("recording: expected inference.done in turn 2");
  }

  for await (const _ev of harness.runInference({
    turns: [
      userTurn("weather in SF and NY?"),
      t1Done.data.turn,
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_1",
            content: [{ type: "text", text: "SF-fog" }],
          },
        ],
        timestamp: 0,
      },
      t2Done.data.turn,
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_2",
            content: [{ type: "text", text: "NY-clear" }],
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

function mergeChunks(chunks: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const chunk of chunks) total += chunk.byteLength;
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return merged;
}

// Reshape a recorded session's exchanges/<i>/ into wire turn-(i+1)/ leaves.
async function transcribeSessionToWire(
  sessionDir: string,
  wireDir: string,
): Promise<void> {
  const exchangesDir = path.join(sessionDir, "exchanges");
  const indices = (await fs.readdir(exchangesDir)).sort(
    (a, b) => Number(a) - Number(b),
  );
  for (const index of indices) {
    const turnDir = path.join(wireDir, `turn-${String(Number(index) + 1)}`);
    await fs.mkdir(turnDir, { recursive: true });
    for (const file of await fs.readdir(path.join(exchangesDir, index))) {
      await fs.copyFile(
        path.join(exchangesDir, index, file),
        path.join(turnDir, file),
      );
    }
  }
}

describe("convertWireToSession end-to-end through strict replay", () => {
  test("a repeated-tool multi-turn session replays and fully consumes its dispatches", async () => {
    const recordedDir = await makeTmpDir();
    const wireDir = await makeTmpDir();
    const sessionDir = await makeTmpDir();
    await recordRepeatedToolSession(recordedDir);
    await transcribeSessionToWire(recordedDir, wireDir);

    await convertWireToSession({
      wireDir,
      sessionDir,
      source: SYNTHETIC_SOURCE,
      origin: "live",
      capturedAt: "2026-05-25T12:00:00Z",
    });

    // Both dispatches are the same tool, indexed in call order; the strict
    // harness pops them from one per-tool FIFO queue.
    expect(await listDir(path.join(sessionDir, "dispatches"))).toEqual([
      "0-weather.json",
      "1-weather.json",
    ]);

    const replay = await createReplayHarness({ sessionDir });
    try {
      expect(replay.capturedExchanges).toHaveLength(3);

      const t1 = await replay.runTurn({
        turns: [userTurn("weather in SF and NY?")],
      });
      const t1Done = t1.find((e) => e.type === "inference.done");
      if (t1Done === undefined || t1Done.type !== "inference.done") {
        throw new Error("replay: expected inference.done in turn 1");
      }
      const call1 = t1Done.data.turn.content.find(
        (c) => c.type === "tool_call",
      );
      if (call1 === undefined || call1.type !== "tool_call") {
        throw new Error("replay: expected a tool_call in turn 1");
      }

      const t2 = await replay.runTurn({
        turns: [
          userTurn("weather in SF and NY?"),
          t1Done.data.turn,
          {
            role: "user",
            content: [
              {
                type: "tool_result",
                callId: call1.id,
                content: [{ type: "text", text: "SF-fog" }],
              },
            ],
            timestamp: 0,
          },
        ],
      });
      const t2Done = t2.find((e) => e.type === "inference.done");
      if (t2Done === undefined || t2Done.type !== "inference.done") {
        throw new Error("replay: expected inference.done in turn 2");
      }
      const call2 = t2Done.data.turn.content.find(
        (c) => c.type === "tool_call",
      );
      if (call2 === undefined || call2.type !== "tool_call") {
        throw new Error("replay: expected a tool_call in turn 2");
      }

      await replay.runTurn({
        turns: [
          userTurn("weather in SF and NY?"),
          t1Done.data.turn,
          {
            role: "user",
            content: [
              {
                type: "tool_result",
                callId: call1.id,
                content: [{ type: "text", text: "SF-fog" }],
              },
            ],
            timestamp: 0,
          },
          t2Done.data.turn,
          {
            role: "user",
            content: [
              {
                type: "tool_result",
                callId: call2.id,
                content: [{ type: "text", text: "NY-clear" }],
              },
            ],
            timestamp: 0,
          },
        ],
      });

      replay.assertFullyConsumed();
    } finally {
      replay.dispose();
    }
  });
});
