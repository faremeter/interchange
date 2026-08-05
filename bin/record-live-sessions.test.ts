// Exercise the live-session recorder's orchestration through a stub fetch, so
// the multi-turn drive loop, tool-call extraction, dispatch recording, and
// manifest write are covered without a real provider call. The live CLI runs
// the same recordLiveSession with no fetch override (real network) — only the
// seam differs.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { wire, type RecordingFetchLike } from "@intx/inference-testing";
import { loadCaptureManifest } from "@intx/inference-discovery/catalog";

import { recordLiveSession } from "./record-live-sessions";

const ZERO_USAGE = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

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

async function readJSON(file: string): Promise<unknown> {
  return JSON.parse(await fs.readFile(file, "utf8"));
}

describe("recordLiveSession", () => {
  let dir: string;

  beforeEach(async () => {
    dir = await fs.mkdtemp(path.join(os.tmpdir(), "record-live-test-"));
  });

  afterEach(async () => {
    await fs.rm(dir, { recursive: true, force: true });
  });

  test("drives a two-turn tool conversation and writes the session", async () => {
    let exchange = 0;
    const observedBodies: unknown[] = [];
    const stubFetch: RecordingFetchLike = async (_input, init) => {
      const rawBody = init?.body;
      const bodyText =
        typeof rawBody === "string"
          ? rawBody
          : rawBody instanceof Uint8Array
            ? new TextDecoder().decode(rawBody)
            : "";
      observedBodies.push(bodyText.length > 0 ? JSON.parse(bodyText) : null);
      if (exchange++ === 0) {
        return sseResponse(
          wire.completeResponse("anthropic", {
            toolCalls: [
              {
                callId: "call_weather_1",
                name: "weather",
                argsJSON: '{"location":"San Francisco"}',
              },
            ],
            headUsage: ZERO_USAGE,
            tailUsage: { ...ZERO_USAGE, output: 1 },
          }),
        );
      }
      return sseResponse(
        wire.completeResponse("anthropic", {
          text: "It is 68F and foggy in San Francisco.",
          headUsage: ZERO_USAGE,
          tailUsage: { ...ZERO_USAGE, output: 9 },
        }),
      );
    };

    await recordLiveSession({
      brand: "anthropic",
      model: "claude-test",
      apiKey: "record-live-test-stub",
      baseURL: "https://api.anthropic.com",
      outputDir: dir,
      fetch: stubFetch,
      now: () => new Date("2026-05-22T00:00:00Z"),
    });

    // Two turns were driven: the tool-call turn and the follow-up that feeds
    // the tool result back.
    expect(exchange).toBe(2);

    const exchangeDirs = (await fs.readdir(path.join(dir, "exchanges"))).sort();
    expect(exchangeDirs).toEqual(["0", "1"]);

    // The tool call the model emitted is recorded as a dispatch, with the
    // handler's result — the same value fed back into turn 2's request.
    expect(
      await readJSON(path.join(dir, "dispatches", "0-weather.json")),
    ).toEqual({
      args: { location: "San Francisco" },
      result: {
        location: "San Francisco",
        temperatureF: 68,
        conditions: "fog",
      },
    });

    // Turn 2's request body carries the tool_result the recorder threaded back.
    const turn2Body = JSON.stringify(observedBodies[1]);
    expect(turn2Body).toContain("tool_result");
    expect(turn2Body).toContain("68");

    // Origin is "synthetic" here: a supplied fetch override is the recording
    // harness's synthetic seam. A real live run passes no override and records
    // "live".
    const manifest = await loadCaptureManifest(dir);
    expect(manifest.schemaVersion).toBe("2");
    expect(manifest.origin).toBe("synthetic");
    expect(manifest.source).toEqual({
      provider: "anthropic",
      model: "claude-test",
      baseURL: "https://api.anthropic.com",
    });
  });

  test("throws when the model emits no tool call", async () => {
    const stubFetch: RecordingFetchLike = async () =>
      sseResponse(
        wire.completeResponse("anthropic", {
          text: "It is sunny.",
          headUsage: ZERO_USAGE,
          tailUsage: { ...ZERO_USAGE, output: 3 },
        }),
      );

    await expect(
      recordLiveSession({
        brand: "anthropic",
        model: "claude-test",
        apiKey: "record-live-test-stub",
        baseURL: "https://api.anthropic.com",
        outputDir: dir,
        fetch: stubFetch,
        now: () => new Date("2026-05-22T00:00:00Z"),
      }),
    ).rejects.toThrow(/no tool call in turn 1/);
  });

  test("throws for a brand with no adapter mapping", async () => {
    const stubFetch: RecordingFetchLike = async () =>
      sseResponse(
        wire.completeResponse("anthropic", {
          text: "hi",
          headUsage: ZERO_USAGE,
          tailUsage: { ...ZERO_USAGE, output: 1 },
        }),
      );

    await expect(
      recordLiveSession({
        brand: "not-a-brand",
        model: "m",
        apiKey: "k",
        baseURL: "https://example.test",
        outputDir: dir,
        fetch: stubFetch,
        now: () => new Date("2026-05-22T00:00:00Z"),
      }),
    ).rejects.toThrow(/no adapter mapping/);
  });
});
