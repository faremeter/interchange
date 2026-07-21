import { describe, test, expect } from "bun:test";

import {
  createDefaultScheduler,
  HarnessId,
  runInference,
  type Dependencies,
  type InferenceHarnessOptions,
} from "./harness";
import {
  createBuiltinRegistry,
  createDefaultDependencies,
  loadAdapterRegistry,
} from "./providers";
import type { AdapterFactory, AdapterRegistry } from "./adapter";
import type {
  ConversationTurn,
  InferenceEvent,
  InferenceSource,
} from "@intx/types/runtime";

const SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet-20240620",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "test",
  model: "claude-3-5-sonnet-20240620",
};

function userTurn(text: string): ConversationTurn {
  return {
    role: "user",
    content: [{ type: "text", text }],
    timestamp: 0,
  };
}

async function collect(
  iter: AsyncIterable<InferenceEvent>,
): Promise<InferenceEvent[]> {
  const out: InferenceEvent[] = [];
  for await (const ev of iter) out.push(ev);
  return out;
}

// Only needed because `globalThis.fetch` reassignment must satisfy the
// Bun-augmented type (which carries a `preconnect` static). `deps.fetch`
// uses the narrow `Dependencies.fetch` shape and accepts a plain function.
function makeGlobalFetchStub(
  handler: (
    input: string | URL | Request,
    init?: RequestInit,
  ) => Promise<Response>,
): typeof globalThis.fetch {
  return Object.assign(handler, { preconnect: () => undefined });
}

describe("runInference — Dependencies parameter", () => {
  test("invokes deps.fetch instead of globalThis.fetch", async () => {
    const calls: { url: string; method: string | undefined }[] = [];

    const deps: Dependencies = {
      fetch: (input, init) => {
        const url =
          typeof input === "string"
            ? input
            : input instanceof URL
              ? input.toString()
              : input.url;
        calls.push({ url, method: init?.method });
        return Promise.resolve(
          new Response("", {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          }),
        );
      },
      scheduler: createDefaultScheduler(),
      adapters: createBuiltinRegistry(),
    };

    const originalFetch = globalThis.fetch;
    globalThis.fetch = makeGlobalFetchStub(() => {
      throw new Error(
        "globalThis.fetch must not be called when deps.fetch is provided",
      );
    });

    let events: InferenceEvent[];
    try {
      let seq = 0;
      events = await collect(
        runInference({
          turns: [userTurn("hello")],
          source: SOURCE,
          nextSeq: () => ++seq,
          deps,
        }),
      );
    } finally {
      globalThis.fetch = originalFetch;
    }

    expect(calls).toHaveLength(1);
    const firstCall = calls[0];
    if (firstCall === undefined) {
      throw new Error("expected one fetch call");
    }
    expect(firstCall.url).toBe("https://api.anthropic.com/v1/messages");
    expect(firstCall.method).toBe("POST");

    const startEvent = events.find((e) => e.type === "inference.start");
    const doneEvent = events.find((e) => e.type === "inference.done");
    if (startEvent === undefined) throw new Error("missing inference.start");
    if (doneEvent === undefined) throw new Error("missing inference.done");
  });

  test("propagates errors from deps.fetch without falling back to globalThis.fetch", async () => {
    const deps: Dependencies = {
      fetch: () => Promise.reject(new Error("simulated network failure")),
      scheduler: createDefaultScheduler(),
      adapters: createBuiltinRegistry(),
    };

    const originalFetch = globalThis.fetch;
    let globalFetchCalled = false;
    globalThis.fetch = makeGlobalFetchStub(() => {
      globalFetchCalled = true;
      throw new Error("globalThis.fetch must not be called");
    });

    let events: InferenceEvent[];
    try {
      let seq = 0;
      events = await collect(
        runInference({
          turns: [userTurn("hello")],
          source: SOURCE,
          nextSeq: () => ++seq,
          deps,
        }),
      );
    } finally {
      globalThis.fetch = originalFetch;
    }

    expect(globalFetchCalled).toBe(false);
    const errorEvent = events.find((e) => e.type === "inference.error");
    if (errorEvent === undefined) throw new Error("missing inference.error");
    expect(errorEvent.data.error.category).toBe("retryable");
    expect(errorEvent.data.error.message).toContain(
      "simulated network failure",
    );
  });

  // The crash-loudly contract: a missing or malformed `deps.fetch` is a
  // programmer bug, not a transport failure. `runInference` must throw a
  // plain Error out of the generator (lazily, on first iteration — the
  // throw fires from inside `for await`, not at the `runInference(...)`
  // call site) and must not yield any event, including `inference.start`.

  test("throws plainly when deps.fetch is undefined", async () => {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- modeling a JS caller that assigned undefined to deps.fetch
    const deps = { fetch: undefined } as unknown as Dependencies;
    const iter = runInference({
      turns: [userTurn("hello")],
      source: SOURCE,
      nextSeq: () => 1,
      deps,
    });

    const events: InferenceEvent[] = [];
    let thrown: unknown;
    try {
      for await (const ev of iter) events.push(ev);
    } catch (e) {
      thrown = e;
    }

    if (!(thrown instanceof Error)) {
      throw new Error("expected runInference to throw an Error");
    }
    expect(thrown.message).toContain("deps.fetch must be a function");
    expect(thrown.message).toContain("undefined");
    expect(events).toEqual([]);
  });

  test("throws plainly when deps.fetch is a non-function value", async () => {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- modeling a JS caller that assigned a non-function value to deps.fetch
    const deps = { fetch: "not a function" } as unknown as Dependencies;
    const iter = runInference({
      turns: [userTurn("hello")],
      source: SOURCE,
      nextSeq: () => 1,
      deps,
    });

    const events: InferenceEvent[] = [];
    let thrown: unknown;
    try {
      for await (const ev of iter) events.push(ev);
    } catch (e) {
      thrown = e;
    }

    if (!(thrown instanceof Error)) {
      throw new Error("expected runInference to throw an Error");
    }
    expect(thrown.message).toContain("deps.fetch must be a function");
    expect(thrown.message).toContain("string");
    expect(events).toEqual([]);
  });

  test("throws plainly when deps is omitted entirely", async () => {
    const baseOpts: Omit<InferenceHarnessOptions, "deps"> = {
      turns: [userTurn("hello")],
      source: SOURCE,
      nextSeq: () => 1,
    };
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- modeling a JS caller that omitted the required `deps` field
    const opts = baseOpts as unknown as InferenceHarnessOptions;
    const iter = runInference(opts);

    const events: InferenceEvent[] = [];
    let thrown: unknown;
    try {
      for await (const ev of iter) events.push(ev);
    } catch (e) {
      thrown = e;
    }

    if (!(thrown instanceof Error)) {
      throw new Error("expected runInference to throw an Error");
    }
    expect(thrown.message).toContain("deps.fetch must be a function");
    expect(events).toEqual([]);
  });
});

describe("createDefaultDependencies", () => {
  test("delegates calls to globalThis.fetch as bound at factory-call time", async () => {
    const calls: { url: string; method: string | undefined }[] = [];
    const originalFetch = globalThis.fetch;
    globalThis.fetch = makeGlobalFetchStub((input, init) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input.url;
      calls.push({ url, method: init?.method });
      return Promise.resolve(new Response("", { status: 204 }));
    });

    try {
      const deps = createDefaultDependencies();
      const response = await deps.fetch("https://example.test/ping", {
        method: "POST",
      });
      expect(response.status).toBe(204);
    } finally {
      globalThis.fetch = originalFetch;
    }

    expect(calls).toEqual([
      { url: "https://example.test/ping", method: "POST" },
    ]);
  });

  test("does not stamp the HarnessId tag", () => {
    const deps = createDefaultDependencies();
    expect(Object.getOwnPropertySymbols(deps)).toEqual([]);
  });
});

// `source.defaults` carries model-bound knobs that the harness merges
// into the per-call `InferenceOptions` at the top of `runInference`
// before the adapter sees anything. The contract: per-call wins over
// source-bound; source-bound applies when per-call omits the key.
//
// These tests use the openai-compatible adapter because it carries the
// fully-populated `max_tokens` floor through to the request body
// (anthropic's adapter only forwards `max_tokens` when set), so the
// merge result is observable at the wire.
describe("runInference — source.defaults merge precedence", () => {
  const OPENAI_SOURCE: InferenceSource = {
    id: "openai:gpt-test",
    provider: "openai",
    baseURL: "https://api.openai.test/v1",
    apiKey: "test",
    model: "gpt-test",
  };

  function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
  }

  async function captureMaxTokens(opts: {
    source: InferenceSource;
    perCallMaxTokens?: number;
  }): Promise<number | undefined> {
    let captured: Record<string, unknown> | undefined;
    const deps: Dependencies = {
      fetch: (_input, init) => {
        const body = typeof init?.body === "string" ? init.body : "";
        const parsed: unknown = body === "" ? {} : JSON.parse(body);
        if (isRecord(parsed)) {
          captured = parsed;
        }
        return Promise.resolve(
          new Response("", {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          }),
        );
      },
      scheduler: createDefaultScheduler(),
      adapters: createBuiltinRegistry(),
    };
    let seq = 0;
    await collect(
      runInference({
        turns: [userTurn("hi")],
        source: opts.source,
        ...(opts.perCallMaxTokens !== undefined
          ? { inferenceOptions: { maxTokens: opts.perCallMaxTokens } }
          : {}),
        nextSeq: () => ++seq,
        deps,
      }),
    );
    if (captured === undefined) throw new Error("no request body captured");
    const value = captured["max_tokens"];
    return typeof value === "number" ? value : undefined;
  }

  test("source-bound default applies when the per-call option is absent", async () => {
    const sourceWithDefault: InferenceSource = {
      ...OPENAI_SOURCE,
      defaults: { maxTokens: 1024 },
    };
    const max = await captureMaxTokens({ source: sourceWithDefault });
    expect(max).toBe(1024);
  });

  test("per-call option overrides the source-bound default", async () => {
    const sourceWithDefault: InferenceSource = {
      ...OPENAI_SOURCE,
      defaults: { maxTokens: 1024 },
    };
    const max = await captureMaxTokens({
      source: sourceWithDefault,
      perCallMaxTokens: 8192,
    });
    expect(max).toBe(8192);
  });

  test("neither set: the adapter's compile-time floor applies", async () => {
    const max = await captureMaxTokens({ source: OPENAI_SOURCE });
    // Falls through to openai.ts's `options.maxTokens ?? 4096` floor.
    expect(max).toBe(4096);
  });
});

// providerOptions on InferenceSourceDefaults is the model-bound bag of
// provider-native knobs. Per-call InferenceOptions.providerOptions
// overrides via the same shallow-spread merge that handles maxTokens.
// The merge is shallow: a per-call providerOptions object wholesale
// replaces the source-bound one rather than deep-merging per key.
describe("runInference — providerOptions merge precedence", () => {
  function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
  }

  async function captureAdapterOptions(opts: {
    sourceProviderOptions?: Record<string, unknown>;
    perCallProviderOptions?: Record<string, unknown>;
  }): Promise<Record<string, unknown> | undefined> {
    // A fresh provider name per call keeps independent runs isolated. The
    // custom adapter is injected through `deps.adapters`: a manifest entry
    // points at a synthetic module that `loadAdapterRegistry` resolves via
    // an injected importer, so nothing is loaded from disk.
    const providerName = `test-provideroptions-${Math.random().toString(36).slice(2)}`;
    let captured: Record<string, unknown> | undefined | "absent" = "absent";

    const make: AdapterFactory = () => ({
      buildRequest: (_messages, _model, options) => {
        captured = isRecord(options.providerOptions)
          ? options.providerOptions
          : undefined;
        return {
          url: "/test",
          headers: { "content-type": "application/json" },
          body: "{}",
        };
      },
      parseResponse: () => [],
    });
    const adapters = await loadAdapterRegistry(
      [{ provider: providerName, specifier: "x", export: "make" }],
      { import: () => Promise.resolve({ make }) },
    );

    const source: InferenceSource = {
      id: `${providerName}:test-model`,
      provider: providerName,
      baseURL: "https://test.invalid",
      apiKey: "test",
      model: "test-model",
      ...(opts.sourceProviderOptions !== undefined
        ? { defaults: { providerOptions: opts.sourceProviderOptions } }
        : {}),
    };

    const deps: Dependencies = {
      fetch: () =>
        Promise.resolve(
          new Response("", {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          }),
        ),
      scheduler: createDefaultScheduler(),
      adapters,
    };

    let seq = 0;
    await collect(
      runInference({
        turns: [userTurn("hi")],
        source,
        ...(opts.perCallProviderOptions !== undefined
          ? {
              inferenceOptions: {
                providerOptions: opts.perCallProviderOptions,
              },
            }
          : {}),
        nextSeq: () => ++seq,
        deps,
      }),
    );

    if (captured === "absent")
      throw new Error("adapter buildRequest not called");
    return captured;
  }

  test("source-bound providerOptions reaches the adapter when no per-call override", async () => {
    const seen = await captureAdapterOptions({
      sourceProviderOptions: { user: "user_123", store: false },
    });
    expect(seen).toEqual({ user: "user_123", store: false });
  });

  test("per-call providerOptions wholesale replaces the source-bound bag", async () => {
    const seen = await captureAdapterOptions({
      sourceProviderOptions: { user: "user_123", store: false },
      perCallProviderOptions: { user: "user_999" },
    });
    // Shallow merge: per-call object wins entirely, source-bound `store`
    // does NOT survive the override.
    expect(seen).toEqual({ user: "user_999" });
  });

  test("neither set: the adapter sees options.providerOptions === undefined", async () => {
    const seen = await captureAdapterOptions({});
    expect(seen).toBeUndefined();
  });
});

// The JSDoc on `Dependencies` documents which reflective APIs leak the
// optional `[HarnessId]` tag. Pin those claims so a future refactor that
// makes the tag enumerable (e.g., renaming it to a string key) cannot
// silently turn a safe serializer into a leak.
describe("Dependencies — reflective exposure of HarnessId", () => {
  function stampedDeps(): Dependencies {
    return {
      fetch: () => Promise.resolve(new Response("")),
      scheduler: createDefaultScheduler(),
      adapters: createBuiltinRegistry(),
      [HarnessId]: Symbol("test-harness"),
    };
  }

  test("JSON.stringify ignores symbol-keyed fields", () => {
    // Probe with a serializable string value at the symbol key. If
    // `HarnessId` were ever changed from a symbol to a string key, the
    // serializer would walk it and the assertion would fail. The
    // string-keyed control proves the test isn't passing just because
    // `JSON.stringify` produced an empty object for unrelated reasons.
    const probe = {
      visible: "yes",
      [HarnessId]: "leaked-value",
    };
    expect(JSON.stringify(probe)).toBe('{"visible":"yes"}');
  });

  test("Object.getOwnPropertySymbols exposes the tag", () => {
    const deps = stampedDeps();
    expect(Object.getOwnPropertySymbols(deps)).toContain(HarnessId);
  });

  test("Reflect.ownKeys exposes the tag", () => {
    const deps = stampedDeps();
    expect(Reflect.ownKeys(deps)).toContain(HarnessId);
  });
});

// ---------------------------------------------------------------------------
// runInference — source-identity stamping on inference events
//
// The harness snapshots `{id, provider, model}` from the active source at
// the top of the call and stamps that descriptor onto every inference.usage
// and inference.done event for that call. The snapshot defends against
// `applyInferenceSourceFields` (or any other in-place mutation of the
// shared source object) firing between call start and inference.done: the
// identity stamped onto the events must reflect the source that *began*
// the call, not whatever the active source happens to be at done-time.
// ---------------------------------------------------------------------------

describe("runInference — source-identity stamping", () => {
  test("emits LastCycleSource on inference.done matching the call-start source", async () => {
    const deps: Dependencies = {
      fetch: () =>
        Promise.resolve(
          new Response("", {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          }),
        ),
      scheduler: createDefaultScheduler(),
      adapters: createBuiltinRegistry(),
    };
    let seq = 0;
    const events = await collect(
      runInference({
        turns: [userTurn("hi")],
        source: SOURCE,
        nextSeq: () => ++seq,
        deps,
      }),
    );

    const doneEvent = events.find((e) => e.type === "inference.done");
    if (doneEvent === undefined) throw new Error("missing inference.done");
    expect(doneEvent.data.source).toEqual({
      sourceId: SOURCE.id,
      provider: SOURCE.provider,
      model: SOURCE.model,
    });
  });

  test("two calls with different sources stamp their own descriptor", async () => {
    const sourceA: InferenceSource = {
      id: "anthropic:claude-A",
      provider: "anthropic",
      baseURL: "https://api.anthropic.com",
      apiKey: "test",
      model: "claude-A",
    };
    const sourceB: InferenceSource = {
      id: "openai:gpt-B",
      provider: "openai",
      baseURL: "https://api.openai.test/v1",
      apiKey: "test",
      model: "gpt-B",
    };
    const deps: Dependencies = {
      fetch: () =>
        Promise.resolve(
          new Response("", {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          }),
        ),
      scheduler: createDefaultScheduler(),
      adapters: createBuiltinRegistry(),
    };

    let seq = 0;
    const eventsA = await collect(
      runInference({
        turns: [userTurn("call-A")],
        source: sourceA,
        nextSeq: () => ++seq,
        deps,
      }),
    );
    const doneA = eventsA.find((e) => e.type === "inference.done");
    if (doneA === undefined) throw new Error("missing inference.done for A");
    expect(doneA.data.source).toEqual({
      sourceId: "anthropic:claude-A",
      provider: "anthropic",
      model: "claude-A",
    });

    seq = 0;
    const eventsB = await collect(
      runInference({
        turns: [userTurn("call-B")],
        source: sourceB,
        nextSeq: () => ++seq,
        deps,
      }),
    );
    const doneB = eventsB.find((e) => e.type === "inference.done");
    if (doneB === undefined) throw new Error("missing inference.done for B");
    expect(doneB.data.source).toEqual({
      sourceId: "openai:gpt-B",
      provider: "openai",
      model: "gpt-B",
    });
  });

  test("hot-swap mid-call: inference.done reflects the call-start source, not the post-mutation fields", async () => {
    // Simulate the harness's `setSource` pattern: a single InferenceSource
    // object is mutated in place via `applyInferenceSourceFields`. If the
    // call captured a reference to the live object instead of snapshotting
    // its identifying fields, the descriptor on inference.done would
    // observe whatever id/provider/model the swap mutated in.
    const activeSource: InferenceSource = {
      id: "anthropic:claude-pre",
      provider: "anthropic",
      baseURL: "https://api.anthropic.com",
      apiKey: "test",
      model: "claude-pre",
    };

    // Mutate the source between fetch invocation and stream consumption.
    // The Response body resolves synchronously here, but the harness still
    // reads `source.*` (model in the request body) on the way out; the
    // post-mutation values must NOT show up on the inference.done event
    // because the snapshot at call start already captured the pre-swap
    // identity.
    const deps: Dependencies = {
      fetch: () => {
        activeSource.id = "openai:gpt-post";
        activeSource.provider = "openai";
        activeSource.baseURL = "https://api.openai.test/v1";
        activeSource.apiKey = "test-post";
        activeSource.model = "gpt-post";
        return Promise.resolve(
          new Response("", {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          }),
        );
      },
      scheduler: createDefaultScheduler(),
      adapters: createBuiltinRegistry(),
    };

    let seq = 0;
    const events = await collect(
      runInference({
        turns: [userTurn("hi")],
        source: activeSource,
        nextSeq: () => ++seq,
        deps,
      }),
    );

    const doneEvent = events.find((e) => e.type === "inference.done");
    if (doneEvent === undefined) throw new Error("missing inference.done");
    expect(doneEvent.data.source).toEqual({
      sourceId: "anthropic:claude-pre",
      provider: "anthropic",
      model: "claude-pre",
    });
  });
});

describe("runInference — source quirks reach the adapter registry", () => {
  // A registry that records the quirks argument every resolve receives and
  // returns a do-nothing adapter, so the test observes exactly what the
  // harness forwards without depending on any real provider.
  function spyRegistry(): {
    registry: AdapterRegistry;
    quirksCalls: unknown[];
  } {
    const quirksCalls: unknown[] = [];
    const registry: AdapterRegistry = {
      has: () => true,
      resolve: (_source, quirks) => {
        quirksCalls.push(quirks);
        return {
          buildRequest: () => ({
            url: "/v1/messages",
            headers: {},
            body: "{}",
          }),
          parseResponse: () => [],
        };
      },
    };
    return { registry, quirksCalls };
  }

  function depsWith(registry: AdapterRegistry): Dependencies {
    return {
      fetch: () =>
        Promise.resolve(
          new Response("", {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          }),
        ),
      scheduler: createDefaultScheduler(),
      adapters: registry,
    };
  }

  test("forwards the source's quirks bag to resolve", async () => {
    const { registry, quirksCalls } = spyRegistry();
    let seq = 0;
    await collect(
      runInference({
        turns: [userTurn("hi")],
        source: { ...SOURCE, quirks: { forceAssistantReasoningContent: true } },
        nextSeq: () => ++seq,
        deps: depsWith(registry),
      }),
    );
    expect(quirksCalls).toEqual([{ forceAssistantReasoningContent: true }]);
  });

  test("resolves with undefined quirks when the source has none", async () => {
    const { registry, quirksCalls } = spyRegistry();
    let seq = 0;
    await collect(
      runInference({
        turns: [userTurn("hi")],
        source: SOURCE,
        nextSeq: () => ++seq,
        deps: depsWith(registry),
      }),
    );
    expect(quirksCalls).toEqual([undefined]);
  });
});
