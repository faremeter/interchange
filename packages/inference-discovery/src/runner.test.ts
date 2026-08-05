import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { CapabilityIntent } from "./catalog";
import type { CaptureStep, CapturedResponse, ProviderPlugin } from "./plugin";
import { runCapture, type FetchLike } from "./runner";

const INTENT: CapabilityIntent = { prompt: "hi" };

// The rig resolves a provider's adapter name and base URL from the catalog and
// asserts the dialed origin matches, so a unit plug-in uses a real catalog
// brand and dials that brand's endpoint. `anthropic` maps to the `anthropic`
// adapter and `https://api.anthropic.com`.
const BRAND = "anthropic";
const BASE = "https://api.anthropic.com";

function* singleStepIterator(opts: {
  model: string;
  capability: string;
  intent: CapabilityIntent;
}): Generator<CaptureStep, void, CapturedResponse> {
  yield {
    kind: "json",
    url: `${BASE}/v1/messages`,
    // A request body carries a messages array; the rig reconstructs tool
    // dispatches from the final request, and the extractor requires it.
    body: { model: opts.model, messages: [], prompt: opts.intent.prompt },
  };
}

function makePlugin(overrides: Partial<ProviderPlugin> = {}): ProviderPlugin {
  return {
    name: BRAND,
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

async function readJSON(file: string): Promise<unknown> {
  return JSON.parse(await fs.readFile(file, "utf8"));
}

describe("runCapture", () => {
  let dir: string;

  beforeEach(async () => {
    dir = await makeTempDir();
  });

  afterEach(async () => {
    await fs.rm(dir, { recursive: true, force: true });
  });

  test("captures a JSON response into an exchange and writes session.json", async () => {
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

    expect(observedURL).toBe(`${BASE}/v1/messages`);
    expect(observedHeaders["Content-Type"]).toBe("application/json");
    expect(observedHeaders["X-Api-Key"]).toBe("secret-key");
    expect(JSON.parse(bodyToString(observedBody))).toEqual({
      model: "test-model",
      messages: [],
      prompt: "hi",
    });

    const rootEntries = (await fs.readdir(dir)).sort();
    expect(rootEntries).toEqual(["exchanges", "session.json"]);

    const exchangeEntries = (
      await fs.readdir(path.join(dir, "exchanges", "0"))
    ).sort();
    expect(exchangeEntries).toEqual([
      "request-headers.json",
      "request.json",
      "response-headers.json",
      "response.json",
    ]);

    expect(
      await readJSON(path.join(dir, "exchanges", "0", "response.json")),
    ).toEqual({ reply: "hello" });

    expect(
      await readJSON(path.join(dir, "exchanges", "0", "request-headers.json")),
    ).toMatchObject({
      "X-Api-Key": "<REDACTED>",
      "Content-Type": "application/json",
    });

    expect(await readJSON(path.join(dir, "session.json"))).toEqual({
      schemaVersion: "2",
      source: {
        provider: "anthropic",
        model: "test-model",
        baseURL: "https://api.anthropic.com",
      },
      origin: "live",
      capturedAt: "2026-05-22T00:00:00.000Z",
      capability: "plain-text",
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

    const exchangeEntries = await fs.readdir(path.join(dir, "exchanges", "0"));
    expect(exchangeEntries).toContain("response.sse");
    expect(exchangeEntries).not.toContain("response.json");

    const written = await fs.readFile(
      path.join(dir, "exchanges", "0", "response.sse"),
      "utf8",
    );
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

  test("walks all steps of a multi-step generator into numbered exchanges", async () => {
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
        url: `${BASE}/v1/messages`,
        body: { model: opts.model, messages: [], prompt: opts.intent.prompt },
      };
      yield {
        kind: "json",
        url: `${BASE}/v1/messages`,
        body: { model: opts.model, messages: [], prior: first.parsed },
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
      now: () => new Date("2026-05-22T00:00:00Z"),
      fetch: stubFetch,
    });

    expect(fetchCalls).toBe(2);
    expect(observedURLs).toEqual([
      `${BASE}/v1/messages`,
      `${BASE}/v1/messages`,
    ]);
    expect(observedBodies[1]).toEqual({
      model: "test-model",
      messages: [],
      prior: { step: 1 },
    });

    const exchange0 = (
      await fs.readdir(path.join(dir, "exchanges", "0"))
    ).sort();
    expect(exchange0).toEqual([
      "request-headers.json",
      "request.json",
      "response-headers.json",
      "response.json",
    ]);
    expect(
      await readJSON(path.join(dir, "exchanges", "1", "response.json")),
    ).toEqual({ step: 2 });

    expect(await readJSON(path.join(dir, "session.json"))).toMatchObject({
      schemaVersion: "2",
      capability: "function-calling-multi-turn",
    });
  });

  test("captures a raw-bytes step into request.bin with the supplied content-type", async () => {
    const payload = new Uint8Array([0x25, 0x50, 0x44, 0x46, 0x2d, 0x31]);
    let observedHeaders: Record<string, string> = {};
    let observedBody: string | Uint8Array = "";

    function* rawIterator(): Generator<CaptureStep, void, CapturedResponse> {
      yield {
        kind: "raw",
        url: `${BASE}/v1/files`,
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
      now: () => new Date("2026-05-22T00:00:00Z"),
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

    const uploadEntries = (
      await fs.readdir(path.join(dir, "exchanges", "0"))
    ).sort();
    expect(uploadEntries).toEqual([
      "request-headers.json",
      "request.bin",
      "response-headers.json",
      "response.json",
    ]);
    const writtenBytes = await fs.readFile(
      path.join(dir, "exchanges", "0", "request.bin"),
    );
    expect(Array.from(writtenBytes)).toEqual(Array.from(payload));
  });

  test("orders a raw upload before a JSON generate across exchanges", async () => {
    // The files-api capture is the one shape where exchange ordering is
    // load-bearing: the raw upload must land at exchange 0 and the JSON
    // generate at exchange 1, because the generate request references the
    // uploaded file. The rig numbers exchanges by generator-yield order;
    // this pins that a raw-then-json yield sequence produces request.bin
    // at 0 and request.json at 1, with the upload response threaded into
    // the generate body.
    const upload = new Uint8Array([0x25, 0x50, 0x44, 0x46]);

    function* uploadThenGenerate(opts: {
      model: string;
      capability: string;
      intent: CapabilityIntent;
    }): Generator<CaptureStep, void, CapturedResponse> {
      const uploaded = yield {
        kind: "raw",
        url: `${BASE}/v1/files`,
        method: "POST",
        contentType: "application/pdf",
        body: upload,
      };
      yield {
        kind: "json",
        url: `${BASE}/v1/messages`,
        body: { model: opts.model, messages: [], fileRef: uploaded.parsed },
      };
    }

    const plugin = makePlugin({ iterateCaptureSteps: uploadThenGenerate });
    let call = 0;
    const stubFetch: FetchLike = async () => {
      call += 1;
      return new Response(
        JSON.stringify(call === 1 ? { fileId: "f-1" } : { ok: true }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    };

    await runCapture({
      plugin,
      model: "test-model",
      capability: "files-api-reference",
      intent: INTENT,
      outDir: dir,
      now: () => new Date("2026-05-22T00:00:00Z"),
      fetch: stubFetch,
    });

    const exchange0 = (
      await fs.readdir(path.join(dir, "exchanges", "0"))
    ).sort();
    expect(exchange0).toContain("request.bin");
    expect(exchange0).not.toContain("request.json");

    const exchange1 = (
      await fs.readdir(path.join(dir, "exchanges", "1"))
    ).sort();
    expect(exchange1).toContain("request.json");
    expect(exchange1).not.toContain("request.bin");

    const writtenUpload = await fs.readFile(
      path.join(dir, "exchanges", "0", "request.bin"),
    );
    expect(Array.from(writtenUpload)).toEqual(Array.from(upload));
    expect(
      await readJSON(path.join(dir, "exchanges", "1", "request.json")),
    ).toMatchObject({ fileRef: { fileId: "f-1" } });
  });

  test("rejects step.headers that collide with plug-in auth headers", async () => {
    function* collidingIterator(): Generator<
      CaptureStep,
      void,
      CapturedResponse
    > {
      yield {
        kind: "json",
        url: `${BASE}/v1/messages`,
        headers: { "x-api-key": "step-override" },
        body: { messages: [] },
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
        url: `${BASE}/v1/messages`,
        headers: { "Content-Type": "application/x-overridden+json" },
        body: { messages: [] },
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

  test("throws when the provider has no catalog adapter mapping", async () => {
    const plugin = makePlugin({ name: "not-a-catalog-brand" });
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
    ).rejects.toThrow(/no adapter mapping/);
  });

  test("throws when the dialed origin differs from the recorded base URL", async () => {
    function* offBrandIterator(): Generator<
      CaptureStep,
      void,
      CapturedResponse
    > {
      yield {
        kind: "json",
        url: "https://evil.test/v1/messages",
        body: { messages: [] },
      };
    }
    const plugin = makePlugin({ iterateCaptureSteps: offBrandIterator });
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
    ).rejects.toThrow(/dialed https:\/\/evil\.test/);
  });
});
