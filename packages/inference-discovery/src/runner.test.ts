import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { CapabilityIntent } from "./catalog";
import type { CaptureStep, CapturedResponse, ProviderPlugin } from "./plugin";
import { runCapture, type FetchLike } from "./runner";

const INTENT: CapabilityIntent = { prompt: "hi" };

function* singleStepIterator(opts: {
  model: string;
  capability: string;
  intent: CapabilityIntent;
}): Generator<CaptureStep, void, CapturedResponse> {
  yield {
    kind: "json",
    subdir: null,
    url: `https://example.test/${opts.model}/${opts.capability}`,
    body: { prompt: opts.intent.prompt },
  };
}

function makePlugin(overrides: Partial<ProviderPlugin> = {}): ProviderPlugin {
  return {
    name: "test-provider",
    models: ["test-model"],
    redactRequestHeaders: ["x-api-key"],
    redactResponseHeaders: [],
    buildAuthHeaders: () => ({ "X-Api-Key": "secret-key" }),
    iterateCaptureSteps: singleStepIterator,
    ...overrides,
  };
}

async function makeTempDir(): Promise<string> {
  return await fs.mkdtemp(path.join(os.tmpdir(), "runner-test-"));
}

function bodyToString(body: string | Uint8Array): string {
  return typeof body === "string" ? body : new TextDecoder().decode(body);
}

describe("runCapture", () => {
  let dir: string;

  beforeEach(async () => {
    dir = await makeTempDir();
  });

  afterEach(async () => {
    await fs.rm(dir, { recursive: true, force: true });
  });

  test("captures a JSON response into the expected files", async () => {
    let observedURL = "";
    let observedHeaders: Record<string, string> = {};
    let observedBody: string | Uint8Array = "";

    const stubFetch: FetchLike = async (url, init) => {
      observedURL = url;
      observedHeaders = init.headers;
      observedBody = init.body;
      return new Response(JSON.stringify({ reply: "hello" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    };

    await runCapture({
      plugin: makePlugin(),
      model: "test-model",
      capability: "plain-text",
      intent: INTENT,
      outDir: dir,
      now: () => new Date("2026-05-22T00:00:00Z"),
      fetch: stubFetch,
    });

    expect(observedURL).toBe("https://example.test/test-model/plain-text");
    expect(observedHeaders["Content-Type"]).toBe("application/json");
    expect(observedHeaders["X-Api-Key"]).toBe("secret-key");
    expect(JSON.parse(bodyToString(observedBody))).toEqual({ prompt: "hi" });

    const entries = (await fs.readdir(dir)).sort();
    expect(entries).toEqual([
      "manifest.json",
      "request-headers.json",
      "request.json",
      "response-headers.json",
      "response.json",
    ]);

    const responseBody = JSON.parse(
      await fs.readFile(path.join(dir, "response.json"), "utf8"),
    );
    expect(responseBody).toEqual({ reply: "hello" });

    const reqHeaders = JSON.parse(
      await fs.readFile(path.join(dir, "request-headers.json"), "utf8"),
    );
    expect(reqHeaders["X-Api-Key"]).toBe("<REDACTED>");
    expect(reqHeaders["Content-Type"]).toBe("application/json");

    const manifest = JSON.parse(
      await fs.readFile(path.join(dir, "manifest.json"), "utf8"),
    );
    expect(manifest).toEqual({
      provider: "test-provider",
      model: "test-model",
      capability: "plain-text",
      capturedAt: "2026-05-22T00:00:00.000Z",
      schemaVersion: "1",
    });
  });

  test("captures an SSE response into response.sse only", async () => {
    const sseBody = 'data: {"chunk":1}\n\ndata: {"chunk":2}\n\n';
    const stubFetch: FetchLike = async () =>
      new Response(sseBody, {
        status: 200,
        headers: { "Content-Type": "text/event-stream" },
      });

    await runCapture({
      plugin: makePlugin(),
      model: "test-model",
      capability: "plain-text-streaming",
      intent: INTENT,
      outDir: dir,
      now: () => new Date("2026-05-22T00:00:00Z"),
      fetch: stubFetch,
    });

    const entries = (await fs.readdir(dir)).sort();
    expect(entries).toContain("response.sse");
    expect(entries).not.toContain("response.json");

    const written = await fs.readFile(path.join(dir, "response.sse"), "utf8");
    expect(written).toBe(sseBody);
  });

  test("throws on unsupported response content-type", async () => {
    const stubFetch: FetchLike = async () =>
      new Response("oops", {
        status: 200,
        headers: { "Content-Type": "text/plain" },
      });

    await expect(
      runCapture({
        plugin: makePlugin(),
        model: "test-model",
        capability: "plain-text",
        intent: INTENT,
        outDir: dir,
        fetch: stubFetch,
      }),
    ).rejects.toThrow(/text\/plain/);
  });

  test("invokes extractReasoningTrace for reasoning-content captures and writes the trace", async () => {
    let invoked = false;
    let payload: unknown = null;
    const plugin = makePlugin({
      extractReasoningTrace: (parsed) => {
        invoked = true;
        payload = parsed;
        return { fieldPath: "x", text: "thoughts" };
      },
    });
    const stubFetch: FetchLike = async () =>
      new Response(JSON.stringify({ reasoning: "step", final: "answer" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });

    await runCapture({
      plugin,
      model: "test-model",
      capability: "reasoning-content",
      intent: INTENT,
      outDir: dir,
      fetch: stubFetch,
    });

    expect(invoked).toBe(true);
    expect(payload).toEqual({ reasoning: "step", final: "answer" });

    const trace = JSON.parse(
      await fs.readFile(path.join(dir, "reasoning-trace.json"), "utf8"),
    );
    expect(trace).toEqual({ fieldPath: "x", text: "thoughts" });
  });

  test("invokes extractReasoningTrace for redacted-thinking captures and writes the trace", async () => {
    let invoked = false;
    const plugin = makePlugin({
      extractReasoningTrace: () => {
        invoked = true;
        return { fieldPath: "content[0].data", text: "<redacted>" };
      },
    });
    const stubFetch: FetchLike = async () =>
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });

    await runCapture({
      plugin,
      model: "test-model",
      capability: "redacted-thinking",
      intent: INTENT,
      outDir: dir,
      fetch: stubFetch,
    });

    expect(invoked).toBe(true);
    const trace = JSON.parse(
      await fs.readFile(path.join(dir, "reasoning-trace.json"), "utf8"),
    );
    expect(trace).toEqual({ fieldPath: "content[0].data", text: "<redacted>" });
  });

  test("does not write reasoning-trace.json when extractReasoningTrace returns null", async () => {
    const plugin = makePlugin({
      extractReasoningTrace: () => null,
    });
    const stubFetch: FetchLike = async () =>
      new Response(JSON.stringify({}), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });

    await runCapture({
      plugin,
      model: "test-model",
      capability: "reasoning-content",
      intent: INTENT,
      outDir: dir,
      fetch: stubFetch,
    });

    const entries = await fs.readdir(dir);
    expect(entries).not.toContain("reasoning-trace.json");
  });

  test("does not invoke extractReasoningTrace for non-reasoning captures", async () => {
    let invoked = false;
    const plugin = makePlugin({
      extractReasoningTrace: () => {
        invoked = true;
        return null;
      },
    });
    const stubFetch: FetchLike = async () =>
      new Response(JSON.stringify({}), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });

    await runCapture({
      plugin,
      model: "test-model",
      capability: "plain-text",
      intent: INTENT,
      outDir: dir,
      fetch: stubFetch,
    });

    expect(invoked).toBe(false);
  });

  test("walks all steps of a multi-step generator and writes them into subdirs", async () => {
    let fetchCalls = 0;
    const observedURLs: string[] = [];
    const observedBodies: unknown[] = [];

    function* twoStep(opts: {
      model: string;
      capability: string;
      intent: CapabilityIntent;
    }): Generator<CaptureStep, void, CapturedResponse> {
      const first = yield {
        kind: "json",
        subdir: "turn-1",
        url: `https://example.test/${opts.model}/${opts.capability}/turn-1`,
        body: { prompt: opts.intent.prompt },
      };
      yield {
        kind: "json",
        subdir: "turn-2",
        url: `https://example.test/${opts.model}/${opts.capability}/turn-2`,
        body: { prior: first.parsed, prompt: "follow-up" },
      };
    }

    const plugin = makePlugin({ iterateCaptureSteps: twoStep });

    const stubFetch: FetchLike = async (url, init) => {
      fetchCalls += 1;
      observedURLs.push(url);
      observedBodies.push(JSON.parse(bodyToString(init.body)));
      return new Response(JSON.stringify({ step: fetchCalls }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    };

    await runCapture({
      plugin,
      model: "test-model",
      capability: "function-calling-multi-turn",
      intent: INTENT,
      outDir: dir,
      fetch: stubFetch,
    });

    expect(fetchCalls).toBe(2);
    expect(observedURLs).toEqual([
      "https://example.test/test-model/function-calling-multi-turn/turn-1",
      "https://example.test/test-model/function-calling-multi-turn/turn-2",
    ]);
    expect(observedBodies[1]).toEqual({
      prior: { step: 1 },
      prompt: "follow-up",
    });

    const turn1Entries = (await fs.readdir(path.join(dir, "turn-1"))).sort();
    expect(turn1Entries).toEqual([
      "request-headers.json",
      "request.json",
      "response-headers.json",
      "response.json",
    ]);
    const turn2Body = JSON.parse(
      await fs.readFile(path.join(dir, "turn-2", "response.json"), "utf8"),
    );
    expect(turn2Body).toEqual({ step: 2 });

    const rootEntries = (await fs.readdir(dir)).sort();
    expect(rootEntries).toContain("manifest.json");
    const manifest = JSON.parse(
      await fs.readFile(path.join(dir, "manifest.json"), "utf8"),
    );
    expect(manifest).toMatchObject({
      provider: "test-provider",
      model: "test-model",
      capability: "function-calling-multi-turn",
      schemaVersion: "1",
    });
  });

  test("captures a raw-bytes step into request.bin with the supplied content-type", async () => {
    const payload = new Uint8Array([0x25, 0x50, 0x44, 0x46, 0x2d, 0x31]);
    let observedHeaders: Record<string, string> = {};
    let observedBody: string | Uint8Array = "";

    function* rawIterator(): Generator<CaptureStep, void, CapturedResponse> {
      yield {
        kind: "raw",
        subdir: "upload",
        url: "https://example.test/upload",
        method: "POST",
        contentType: "application/pdf",
        headers: { "X-Upload-Protocol": "raw" },
        body: payload,
      };
    }

    const plugin = makePlugin({ iterateCaptureSteps: rawIterator });
    const stubFetch: FetchLike = async (_url, init) => {
      observedHeaders = init.headers;
      observedBody = init.body;
      return new Response(JSON.stringify({ fileId: "abc" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    };

    await runCapture({
      plugin,
      model: "test-model",
      capability: "files-api-reference",
      intent: INTENT,
      outDir: dir,
      fetch: stubFetch,
    });

    expect(observedHeaders["Content-Type"]).toBe("application/pdf");
    expect(observedHeaders["X-Upload-Protocol"]).toBe("raw");
    expect(observedHeaders["X-Api-Key"]).toBe("secret-key");
    if (typeof observedBody === "string") {
      throw new Error("expected raw step to send Uint8Array body");
    }
    expect(observedBody).toBeInstanceOf(Uint8Array);
    expect(Array.from(observedBody)).toEqual(Array.from(payload));

    const uploadEntries = (await fs.readdir(path.join(dir, "upload"))).sort();
    expect(uploadEntries).toEqual([
      "request-headers.json",
      "request.bin",
      "response-headers.json",
      "response.json",
    ]);
    const writtenBytes = await fs.readFile(
      path.join(dir, "upload", "request.bin"),
    );
    expect(Array.from(writtenBytes)).toEqual(Array.from(payload));
  });

  test("rejects step.headers that collide with plug-in auth headers", async () => {
    function* collidingIterator(): Generator<
      CaptureStep,
      void,
      CapturedResponse
    > {
      yield {
        kind: "json",
        subdir: null,
        url: "https://example.test/collide",
        headers: { "x-api-key": "step-override" },
        body: {},
      };
    }
    const plugin = makePlugin({ iterateCaptureSteps: collidingIterator });
    const stubFetch: FetchLike = async () =>
      new Response("{}", {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });

    await expect(
      runCapture({
        plugin,
        model: "test-model",
        capability: "plain-text",
        intent: INTENT,
        outDir: dir,
        fetch: stubFetch,
      }),
    ).rejects.toThrow(/override plug-in auth header/);
  });

  test("step.headers can override the default content-type without auth collision", async () => {
    let observedHeaders: Record<string, string> = {};

    function* overrideIterator(): Generator<
      CaptureStep,
      void,
      CapturedResponse
    > {
      yield {
        kind: "json",
        subdir: null,
        url: "https://example.test/override",
        headers: { "Content-Type": "application/x-overridden+json" },
        body: { x: 1 },
      };
    }
    const plugin = makePlugin({ iterateCaptureSteps: overrideIterator });
    const stubFetch: FetchLike = async (_url, init) => {
      observedHeaders = init.headers;
      return new Response("{}", {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    };

    await runCapture({
      plugin,
      model: "test-model",
      capability: "plain-text",
      intent: INTENT,
      outDir: dir,
      fetch: stubFetch,
    });

    expect(observedHeaders["Content-Type"]).toBe(
      "application/x-overridden+json",
    );
    expect(observedHeaders["X-Api-Key"]).toBe("secret-key");
  });

  test("throws when the iterator yields no steps", async () => {
    function* empty(): Generator<CaptureStep, void, CapturedResponse> {
      // intentionally yields nothing
    }
    const plugin = makePlugin({ iterateCaptureSteps: empty });
    const stubFetch: FetchLike = async () =>
      new Response("{}", {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });

    await expect(
      runCapture({
        plugin,
        model: "test-model",
        capability: "plain-text",
        intent: INTENT,
        outDir: dir,
        fetch: stubFetch,
      }),
    ).rejects.toThrow(/no capture steps/);
  });
});
