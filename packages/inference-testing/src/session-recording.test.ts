import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import type { InferenceEvent, InferenceSource } from "@intx/types/runtime";

import {
  createRecordingHarness,
  SessionRecordingBudgetExceededError,
} from "./session-recording";
import { loadSessionManifest } from "./session-manifest";
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
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "session-recording-"));
  tmpDirs.push(dir);
  return dir;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

async function readJSONRecord(
  filePath: string,
): Promise<Record<string, unknown>> {
  const text = await fs.readFile(filePath, "utf-8");
  const parsed: unknown = JSON.parse(text);
  if (!isRecord(parsed)) {
    throw new Error(`expected ${filePath} to parse as a JSON object`);
  }
  return parsed;
}

function expectStringField(
  record: Record<string, unknown>,
  field: string,
): string {
  const value = record[field];
  if (typeof value !== "string") {
    throw new Error(
      `expected field "${field}" to be a string, got ${String(value)}`,
    );
  }
  return value;
}

function sseResponseFromChunks(chunks: Uint8Array[]): Response {
  let total = 0;
  for (const chunk of chunks) total += chunk.byteLength;
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new Response(merged, {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  });
}

describe("createRecordingHarness construction guards", () => {
  test("requires fetch and bypassCIGuardForTests to be paired", () => {
    expect(() =>
      createRecordingHarness({
        outputDir: "/tmp/unused",
        source: ANTHROPIC_SOURCE,
        maxExchanges: 1,
        redactRequestHeaders: [],
        redactResponseHeaders: [],
        fetch: async () => new Response(),
      }),
    ).toThrow(/must be supplied together/);

    expect(() =>
      createRecordingHarness({
        outputDir: "/tmp/unused",
        source: ANTHROPIC_SOURCE,
        maxExchanges: 1,
        redactRequestHeaders: [],
        redactResponseHeaders: [],
        bypassCIGuardForTests: true,
      }),
    ).toThrow(/must be supplied together/);
  });

  test("rejects non-positive maxExchanges", () => {
    expect(() =>
      createRecordingHarness({
        outputDir: "/tmp/unused",
        source: ANTHROPIC_SOURCE,
        maxExchanges: 0,
        redactRequestHeaders: [],
        redactResponseHeaders: [],
        fetch: async () => new Response(),
        bypassCIGuardForTests: true,
      }),
    ).toThrow(/maxExchanges must be a positive integer/);
  });

  test("CI guard fires when CI is set and the bypass seam is not engaged", () => {
    const previous = process.env["CI"];
    process.env["CI"] = "1";
    try {
      expect(() =>
        createRecordingHarness({
          outputDir: "/tmp/unused",
          source: ANTHROPIC_SOURCE,
          maxExchanges: 1,
          redactRequestHeaders: [],
          redactResponseHeaders: [],
        }),
      ).toThrow(/must not run in CI/);
    } finally {
      if (previous === undefined) delete process.env["CI"];
      else process.env["CI"] = previous;
    }
  });
});

describe("createRecordingHarness end-to-end", () => {
  test("records a single-turn text response to disk", async () => {
    const dir = await makeTmpDir();
    const harness = createRecordingHarness({
      outputDir: dir,
      source: ANTHROPIC_SOURCE,
      maxExchanges: 4,
      redactRequestHeaders: ["x-api-key"],
      redactResponseHeaders: ["set-cookie"],
      fetch: async () => {
        const chunks = wire.completeResponse("anthropic", {
          text: "Hello, world!",
          headUsage: ZERO_USAGE,
          tailUsage: { ...ZERO_USAGE, output: 3 },
        });
        return sseResponseFromChunks(chunks);
      },
      bypassCIGuardForTests: true,
      now: () => new Date("2026-05-25T12:00:00Z"),
    });

    let seq = 0;
    const events: InferenceEvent[] = [];
    for await (const ev of harness.runInference({
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "say hi" }],
          timestamp: 0,
        },
      ],
      source: ANTHROPIC_SOURCE,
      nextSeq: () => ++seq,
    })) {
      events.push(ev);
    }
    await harness.finalize();

    expect(events.some((e) => e.type === "inference.done")).toBe(true);

    const manifest = await loadSessionManifest(dir);
    expect(manifest.sessionSchemaVersion).toBe("1");
    expect(manifest.source.provider).toBe("anthropic");
    expect(manifest.capturedAt).toBe("2026-05-25T12:00:00.000Z");

    const exchangeDir = path.join(dir, "exchanges", "0");
    const exchangeFiles = (await fs.readdir(exchangeDir)).sort();
    expect(exchangeFiles).toEqual([
      "request-headers.json",
      "request.json",
      "response-headers.json",
      "response.sse",
    ]);

    const requestBody = await readJSONRecord(
      path.join(exchangeDir, "request.json"),
    );
    expect(typeof requestBody["messages"]).toBe("object");
  });

  test("redacts configured request and response headers", async () => {
    const dir = await makeTmpDir();
    const harness = createRecordingHarness({
      outputDir: dir,
      source: ANTHROPIC_SOURCE,
      maxExchanges: 1,
      redactRequestHeaders: ["x-api-key", "authorization"],
      redactResponseHeaders: ["set-cookie"],
      fetch: async () => {
        const chunks = wire.completeResponse("anthropic", {
          text: "ok",
          headUsage: ZERO_USAGE,
          tailUsage: { ...ZERO_USAGE, output: 1 },
        });
        return new Response(sseResponseFromChunks(chunks).body, {
          status: 200,
          headers: {
            "content-type": "text/event-stream",
            "set-cookie": "session=abc",
            "x-trace-id": "trace-1",
          },
        });
      },
      bypassCIGuardForTests: true,
    });

    let seq = 0;
    for await (const _ev of harness.runInference({
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "?" }],
          timestamp: 0,
        },
      ],
      source: ANTHROPIC_SOURCE,
      nextSeq: () => ++seq,
    })) {
      // drain
    }
    await harness.finalize();

    const requestHeaders = await readJSONRecord(
      path.join(dir, "exchanges", "0", "request-headers.json"),
    );
    expect(expectStringField(requestHeaders, "x-api-key")).toBe("<REDACTED>");

    const responseHeaders = await readJSONRecord(
      path.join(dir, "exchanges", "0", "response-headers.json"),
    );
    expect(expectStringField(responseHeaders, "set-cookie")).toBe("<REDACTED>");
    expect(expectStringField(responseHeaders, "x-trace-id")).toBe("trace-1");
  });

  test("captures a tool dispatch with args and result", async () => {
    const dir = await makeTmpDir();
    let exchangeIndex = 0;
    const harness = createRecordingHarness({
      outputDir: dir,
      source: ANTHROPIC_SOURCE,
      maxExchanges: 4,
      redactRequestHeaders: [],
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
          return sseResponseFromChunks(chunks);
        }
        exchangeIndex++;
        const chunks = wire.completeResponse("anthropic", {
          text: "It is 68F in SF.",
          headUsage: ZERO_USAGE,
          tailUsage: { ...ZERO_USAGE, output: 5 },
        });
        return sseResponseFromChunks(chunks);
      },
      bypassCIGuardForTests: true,
    });

    const handlerArgs: unknown[] = [];
    harness.onTool("weather", (args) => {
      handlerArgs.push(args);
      return { temperatureF: 68, conditions: "fog" };
    });

    let seq = 0;
    // Drive turn 1.
    const events1: InferenceEvent[] = [];
    for await (const ev of harness.runInference({
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "weather in SF?" }],
          timestamp: 0,
        },
      ],
      source: ANTHROPIC_SOURCE,
      nextSeq: () => ++seq,
    })) {
      events1.push(ev);
    }
    const turn1Done = events1.find((e) => e.type === "inference.done");
    if (turn1Done === undefined || turn1Done.type !== "inference.done") {
      throw new Error("expected inference.done in turn 1");
    }

    // Drive turn 2 with the tool_result threaded in.
    for await (const _ev of harness.runInference({
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "weather in SF?" }],
          timestamp: 0,
        },
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

    expect(handlerArgs).toEqual([{ location: "SF" }]);

    const dispatches = await fs.readdir(path.join(dir, "dispatches"));
    expect(dispatches).toContain("0-weather.json");

    const dispatch = await readJSONRecord(
      path.join(dir, "dispatches", "0-weather.json"),
    );
    expect(dispatch["args"]).toEqual({ location: "SF" });
    expect(dispatch["result"]).toEqual({
      temperatureF: 68,
      conditions: "fog",
    });

    const exchangeDirs = (await fs.readdir(path.join(dir, "exchanges"))).sort();
    expect(exchangeDirs).toEqual(["0", "1"]);
  });

  test("budget guard fires when maxExchanges is exceeded", async () => {
    const dir = await makeTmpDir();
    const harness = createRecordingHarness({
      outputDir: dir,
      source: ANTHROPIC_SOURCE,
      maxExchanges: 1,
      redactRequestHeaders: [],
      redactResponseHeaders: [],
      fetch: async () => {
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
        return sseResponseFromChunks(chunks);
      },
      bypassCIGuardForTests: true,
    });
    harness.onTool("weather", () => ({ ok: true }));

    let seq = 0;
    const drive = async (): Promise<void> => {
      // Drive turn 1 successfully.
      const events: InferenceEvent[] = [];
      for await (const ev of harness.runInference({
        turns: [
          {
            role: "user",
            content: [{ type: "text", text: "?" }],
            timestamp: 0,
          },
        ],
        source: ANTHROPIC_SOURCE,
        nextSeq: () => ++seq,
      })) {
        events.push(ev);
      }
      const done = events.find((e) => e.type === "inference.done");
      if (done === undefined || done.type !== "inference.done") {
        throw new Error("expected inference.done in turn 1");
      }
      // Turn 2 must trip the budget at the first fetch.
      for await (const _ev of harness.runInference({
        turns: [
          {
            role: "user",
            content: [{ type: "text", text: "?" }],
            timestamp: 0,
          },
          done.data.turn,
          {
            role: "user",
            content: [
              {
                type: "tool_result",
                callId: "call_w_1",
                content: [{ type: "text", text: "ok" }],
              },
            ],
            timestamp: 0,
          },
        ],
        source: ANTHROPIC_SOURCE,
        nextSeq: () => ++seq,
      })) {
        // drain — the iterator should reject
      }
    };
    await expect(drive()).rejects.toBeInstanceOf(
      SessionRecordingBudgetExceededError,
    );
  });

  test("falls back to raw capture for a malformed JSON request body", async () => {
    const dir = await makeTmpDir();
    const seen: { body: string | null } = { body: null };
    const harness = createRecordingHarness({
      outputDir: dir,
      source: ANTHROPIC_SOURCE,
      maxExchanges: 1,
      redactRequestHeaders: [],
      redactResponseHeaders: [],
      fetch: async (_input, init) => {
        seen.body = typeof init?.body === "string" ? init.body : "(non-string)";
        const chunks = wire.completeResponse("anthropic", {
          text: "ok",
          headUsage: ZERO_USAGE,
          tailUsage: { ...ZERO_USAGE, output: 1 },
        });
        return sseResponseFromChunks(chunks);
      },
      bypassCIGuardForTests: true,
    });

    // Drive a hand-crafted recording fetch with a malformed JSON
    // body — production adapters never do this, but the recording
    // wrapper is positioned as a transparent observer and must not
    // crash on inputs the adapter would forward.
    await harness.deps.fetch("https://example.invalid/v1/messages", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"not json',
    });
    await harness.finalize();

    // The body was forwarded to the underlying fetch byte-for-byte.
    expect(seen.body).toBe('{"not json');

    // The capture wrote request.bin (raw fallback), not request.json.
    const exchangeFiles = (
      await fs.readdir(path.join(dir, "exchanges", "0"))
    ).sort();
    expect(exchangeFiles).toContain("request.bin");
    expect(exchangeFiles).not.toContain("request.json");
  });

  test("omits init.body on requests with no body so GET/HEAD adapters work", async () => {
    const dir = await makeTmpDir();
    const seen: { bodyKeyPresent: boolean | null; method: string | null } = {
      bodyKeyPresent: null,
      method: null,
    };
    const harness = createRecordingHarness({
      outputDir: dir,
      source: ANTHROPIC_SOURCE,
      maxExchanges: 1,
      redactRequestHeaders: [],
      redactResponseHeaders: [],
      fetch: async (_input, init) => {
        // The wrapper must not pass `body` at all on GET/HEAD — undici
        // rejects bodies on those methods, including the empty string.
        seen.bodyKeyPresent = init !== undefined && "body" in init;
        seen.method = init?.method ?? null;
        return new Response("{}", {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      },
      bypassCIGuardForTests: true,
    });

    await harness.deps.fetch("https://example.invalid/health", {
      method: "GET",
    });
    await harness.finalize();

    expect(seen.method).toBe("GET");
    expect(seen.bodyKeyPresent).toBe(false);
  });

  test("writes session.json even when finalize is called early in a partial recording", async () => {
    const dir = await makeTmpDir();
    const harness = createRecordingHarness({
      outputDir: dir,
      source: ANTHROPIC_SOURCE,
      maxExchanges: 1,
      redactRequestHeaders: [],
      redactResponseHeaders: [],
      fetch: async () =>
        sseResponseFromChunks(
          wire.completeResponse("anthropic", {
            text: "hi",
            headUsage: ZERO_USAGE,
            tailUsage: { ...ZERO_USAGE, output: 1 },
          }),
        ),
      bypassCIGuardForTests: true,
      now: () => new Date("2026-05-25T12:00:00Z"),
    });

    // Call finalize without ever calling runInference. The README
    // advertises that an aborted recording produces a (truncated
    // but readable) session — at minimum that means a loadable
    // session.json must exist.
    await harness.finalize();

    const parsed = await readJSONRecord(path.join(dir, "session.json"));
    expect(expectStringField(parsed, "sessionSchemaVersion")).toBe("1");
  });
});
