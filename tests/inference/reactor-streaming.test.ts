import { describe, test, expect } from "bun:test";

import { createReactor } from "@intx/inference";
import { createDefaultDependencies } from "@intx/inference/providers";
import type {
  ReactorConfig,
  Reactor,
  ReactorEmittedEvent,
  CorrelationValidator,
  Dependencies,
  InferenceHarnessOptions,
} from "@intx/inference";
import { setupHarness, wire } from "@intx/inference-testing";
import type { Harness } from "@intx/inference-testing";
import type {
  ReactorDirector,
  ReactorAction,
  ReactorInboundEvent,
  ReactorState,
  ReactorCapabilities,
  ContextStore,
  ToolRunner,
  InferenceEvent,
  InboundMessage,
  ConversationTurn,
  PendingOperation,
  TokenUsage,
  ContextCommit,
  BeforeToolExtension,
  Compactor,
  TransformRecord,
} from "@intx/types/runtime";

// ---------------------------------------------------------------------------
// Helpers (duplicated from packages/inference/src/reactor.test.ts so the
// top-level tests tree avoids a workspace dependency cycle).
// ---------------------------------------------------------------------------

function emptyUsage(): TokenUsage {
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, thinking: 0 };
}

function makeContextStore(turns: ConversationTurn[] = []): ContextStore {
  async function commit(
    options: { message: string },
    _signal?: AbortSignal,
  ): Promise<ContextCommit> {
    return { hash: "abc", message: options.message, timestamp: Date.now() };
  }

  return {
    async load() {
      return {
        turns,
        pendingOperations: [],
        tokenUsage: emptyUsage(),
        connectorState: null,
      };
    },
    setConnectorState() {
      /* noop */
    },
    commit,
    async branch() {
      /* noop */
    },
    async log() {
      return [];
    },
    async readAt() {
      return [];
    },
    async writeBlob() {
      /* noop */
    },
    async readBlob() {
      throw new Error("not implemented");
    },
    async writePrompt() {
      /* noop */
    },
    async writeResponse() {
      /* noop */
    },
    async writeManifest() {
      /* noop */
    },
    async writeTurns() {
      /* noop */
    },
    async writeMetadata() {
      /* noop */
    },
    async readManifestHistory() {
      throw new Error("not implemented");
    },
  };
}

function noopToolRunner(): ToolRunner {
  return {
    async run(call) {
      return { callId: call.id, content: "ok" };
    },
  };
}

type EventWaiter = {
  predicate: (e: ReactorEmittedEvent) => boolean;
  resolve: (e: ReactorEmittedEvent) => void;
};

/**
 * Accumulate the reactor's emitted events and hand out waiters over them.
 * `onEvent` is the reactor's own emit callback, so it is the signal a waiter
 * resolves from. `events` stays a plain array because the assertions read the
 * whole history back out of it after the wait.
 */
function collectEvents(): {
  events: ReactorEmittedEvent[];
  onEvent: (e: ReactorEmittedEvent) => void;
  waitFor: (type: ReactorEmittedEvent["type"]) => Promise<ReactorEmittedEvent>;
} {
  const events: ReactorEmittedEvent[] = [];
  const waiters: EventWaiter[] = [];

  function waitFor(
    type: ReactorEmittedEvent["type"],
  ): Promise<ReactorEmittedEvent> {
    const predicate = (e: ReactorEmittedEvent) => e.type === type;
    // `waitFor` can be called after the event already fired; the
    // already-collected scan covers that, the waiter list the rest.
    const emitted = events.find(predicate);
    if (emitted !== undefined) return Promise.resolve(emitted);
    return new Promise<ReactorEmittedEvent>((resolve) => {
      waiters.push({ predicate, resolve });
    });
  }

  return {
    events,
    onEvent: (e: ReactorEmittedEvent) => {
      events.push(e);
      const stillWaiting: EventWaiter[] = [];
      for (const waiter of waiters) {
        if (waiter.predicate(e)) waiter.resolve(e);
        else stillWaiting.push(waiter);
      }
      waiters.length = 0;
      waiters.push(...stillWaiting);
    },
    waitFor,
  };
}

function makeInboundMessage(correlationId?: string): InboundMessage {
  return {
    ref: { uid: 1, mailbox: "INBOX" },
    headers: {
      from: "test@example.com",
      to: ["agent@example.com"],
      date: new Date().toISOString(),
      messageId: `msg-${Math.random()}`,
      ...(correlationId !== undefined
        ? { interchangeCorrelationId: correlationId }
        : {}),
    },
    flags: [],
    content: "hello",
    signatureStatus: "missing",
  };
}

let testSessionCounter = 0;

type DirectorHandler<E> = (
  event: E,
  state: ReactorState,
  caps: ReactorCapabilities,
) => ReactorAction | ReactorAction[] | Promise<ReactorAction | ReactorAction[]>;

type DirectorTable = {
  [K in ReactorInboundEvent["type"]]?: DirectorHandler<
    Extract<ReactorInboundEvent, { type: K }>
  >;
};

function directorFromTable(
  table: DirectorTable,
  defaultAction: "done" | "wait" = "done",
): ReactorDirector {
  return {
    async decide(event, state, caps) {
      // Correlated-union problem: table[event.type] is typed as a union of
      // all handlers, but we know it matches this event.
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- table[event.type] is typed for this event.type by the DirectorTable mapped type
      const handler = table[event.type] as
        | DirectorHandler<typeof event>
        | undefined;
      if (handler !== undefined) {
        return handler(event, state, caps);
      }
      return defaultAction === "done" ? caps.done() : caps.wait();
    },
  };
}

type TestReactorOverrides = {
  director?: ReactorDirector;
  toolRunner?: ToolRunner;
  contextStore?: ContextStore;
  correlationValidator?: CorrelationValidator;
  inferenceRunner?: (
    opts: InferenceHarnessOptions,
  ) => AsyncGenerator<InferenceEvent>;
  beforeToolExtensions?: BeforeToolExtension[];
  afterCheckpoint?: () => Promise<void>;
  onShutdown?: () => Promise<void>;
  sessionId?: string;
  gateTimeout?: number;
  shutdownTimeoutMs?: number;
  deps?: Dependencies;
  source?: ReactorConfig["source"];
  compactors?: Record<string, Compactor>;
};

type TestReactorHandle = {
  reactor: Reactor;
  events: ReactorEmittedEvent[];
  waitFor: (type: ReactorEmittedEvent["type"]) => Promise<ReactorEmittedEvent>;
};

function createTestReactor(
  overrides: TestReactorOverrides = {},
): TestReactorHandle {
  const { events, onEvent, waitFor } = collectEvents();
  const sessionId = overrides.sessionId ?? `test-sess-${++testSessionCounter}`;

  const config: ReactorConfig = {
    sessionId,
    director: overrides.director ?? directorFromTable({}),
    source: overrides.source ?? {
      id: "anthropic:test-model",
      provider: "anthropic",
      baseURL: "https://api.anthropic.com",
      credentialId: "test",
      model: "test-model",
    },
    toolRunner: overrides.toolRunner ?? noopToolRunner(),
    contextStore: overrides.contextStore ?? makeContextStore(),
    onEvent,
    deps: overrides.deps ?? createDefaultDependencies(),
    readMaterial: () => ({ secret: "test-secret" }),
    shutdownTimeoutMs: overrides.shutdownTimeoutMs ?? 100,
    ...(overrides.correlationValidator !== undefined
      ? { correlationValidator: overrides.correlationValidator }
      : {}),
    ...(overrides.inferenceRunner !== undefined
      ? { inferenceRunner: overrides.inferenceRunner }
      : {}),
    ...(overrides.beforeToolExtensions !== undefined
      ? { beforeToolExtensions: overrides.beforeToolExtensions }
      : {}),
    ...(overrides.afterCheckpoint !== undefined
      ? { afterCheckpoint: overrides.afterCheckpoint }
      : {}),
    ...(overrides.onShutdown !== undefined
      ? { onShutdown: overrides.onShutdown }
      : {}),
    ...(overrides.gateTimeout !== undefined
      ? { gateTimeout: overrides.gateTimeout }
      : {}),
    ...(overrides.compactors !== undefined
      ? { compactors: overrides.compactors }
      : {}),
  };

  const reactor = createReactor(config);

  return { reactor, events, waitFor };
}

function getEvent<T extends ReactorEmittedEvent["type"]>(
  events: ReactorEmittedEvent[],
  type: T,
): Extract<ReactorEmittedEvent, { type: T }> {
  const found = events.find(
    (e): e is Extract<ReactorEmittedEvent, { type: T }> => e.type === type,
  );
  if (found === undefined) {
    throw new Error(`No event of type '${type}' found`);
  }
  return found;
}

function truncatingCompactor(name: string): Compactor {
  return {
    name,
    version: "1",
    async apply(turns, _ctx) {
      const kept = turns.slice(-1);
      return {
        output: kept,
        record: {
          strategy: name,
          version: "1",
          parameters: {},
          reason: "explicit",
          decisions: { kept: kept.length, dropped: turns.length - kept.length },
        },
      };
    },
  };
}

function makeRecordingContextStore(): {
  store: ContextStore;
  commits: { message: string; turns: ConversationTurn[] }[];
  manifests: TransformRecord[][];
  metadata: { pendingOperations: PendingOperation[]; tokenUsage: TokenUsage }[];
  blobs: { key: string; bytes: Uint8Array; contentType?: string }[];
  lastWrittenTurns: ConversationTurn[];
} {
  const commits: { message: string; turns: ConversationTurn[] }[] = [];
  const manifests: TransformRecord[][] = [];
  const metadata: {
    pendingOperations: PendingOperation[];
    tokenUsage: TokenUsage;
  }[] = [];
  const blobs: { key: string; bytes: Uint8Array; contentType?: string }[] = [];
  let lastWrittenTurns: ConversationTurn[] = [];

  const store: ContextStore = {
    async load() {
      return {
        turns: [],
        pendingOperations: [],
        tokenUsage: emptyUsage(),
        connectorState: null,
      };
    },
    setConnectorState() {
      /* noop */
    },
    async commit(options) {
      commits.push({
        message: options.message,
        turns: [...lastWrittenTurns],
      });
      return {
        hash: `c${String(commits.length)}`,
        message: options.message,
        timestamp: Date.now(),
      };
    },
    async branch() {
      /* noop */
    },
    async log() {
      return [];
    },
    async readAt() {
      return [];
    },
    async writeBlob(key, bytes, contentType) {
      blobs.push({
        key,
        bytes,
        ...(contentType !== undefined ? { contentType } : {}),
      });
    },
    async readBlob() {
      throw new Error("not implemented");
    },
    async writePrompt() {
      /* noop */
    },
    async writeResponse() {
      /* noop */
    },
    async writeManifest(records) {
      manifests.push([...records]);
    },
    async writeTurns(turns) {
      lastWrittenTurns = [...turns];
    },
    async writeMetadata(m) {
      metadata.push({
        pendingOperations: [...m.pendingOperations],
        tokenUsage: { ...m.tokenUsage },
      });
    },
    async readManifestHistory() {
      throw new Error("not implemented");
    },
  };

  return {
    store,
    commits,
    manifests,
    metadata,
    blobs,
    get lastWrittenTurns() {
      return lastWrittenTurns;
    },
  };
}

// ---------------------------------------------------------------------------
// Port B (7a): wire-driven inference-path tests
//
// Same reactor-side assertions as the `createReactor — inference path`
// block in reactor.test.ts, but fed through the real fetch → parseSSE →
// provider adapter → reactor pipeline via the `@intx/inference-testing`
// harness. The synthetic runner tests keep validating the state machine
// cheaply in-process; this file proves the same end state from real bytes.
//
// See the audit at
// `dispatch/intr-60-inference-testing/7a-port_reactor_streaming_subset/audit.md`
// for which reactor.test.ts tests stay on the synthetic path.
// ---------------------------------------------------------------------------

const ANTHROPIC_SOURCE = {
  id: "anthropic:test-model",
  provider: "anthropic" as const,
  baseURL: "https://api.anthropic.com",
  credentialId: "test",
  model: "test-model",
};

const OPENAI_SOURCE = {
  id: "openai:test-model",
  provider: "openai" as const,
  baseURL: "https://api.openai.com/v1",
  credentialId: "test",
  model: "test-model",
};

async function withHarness<T>(body: (h: Harness) => Promise<T>): Promise<T> {
  const harness = setupHarness();
  try {
    return await body(harness);
  } finally {
    harness.dispose();
  }
}

describe("createReactor — inference path [wire-driven]", () => {
  test("Anthropic: infer drives inference.done with assembled turn and merged usage", async () => {
    await withHarness(async (harness) => {
      const headUsage: TokenUsage = {
        input: 100,
        output: 0,
        cacheRead: 10,
        cacheWrite: 5,
        thinking: 0,
      };
      const tailUsage: TokenUsage = {
        input: 0,
        output: 50,
        cacheRead: 0,
        cacheWrite: 0,
        thinking: 0,
      };

      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);
      const chunks = wire.completeResponse("anthropic", {
        text: "Hello from the model",
        headUsage,
        tailUsage,
      });
      stream.enqueueAll(chunks, { startAt: 10 });
      const when = 10 + chunks.length;

      let stateAtInferenceDone: ReactorState | undefined;
      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        source: ANTHROPIC_SOURCE,
        director: directorFromTable({
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, state, caps) => {
            stateAtInferenceDone = state;
            return caps.done();
          },
        }),
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(when + 10);
      await waitFor("reactor.done");

      const inferDone = getEvent(events, "inference.done");
      expect(inferDone.data.turn.content[0]).toEqual({
        type: "text",
        text: "Hello from the model",
      });
      // Head usage (message_start) and tail usage (message_delta) are
      // accumulated by the streaming harness.
      expect(inferDone.data.usage).toEqual({
        input: 100,
        output: 50,
        cacheRead: 10,
        cacheWrite: 5,
        thinking: 0,
      });

      if (stateAtInferenceDone === undefined)
        throw new Error("director never received inference.done");
      expect(stateAtInferenceDone.tokenUsage).toEqual({
        input: 100,
        output: 50,
        cacheRead: 10,
        cacheWrite: 5,
        thinking: 0,
      });

      const lastMsg =
        stateAtInferenceDone.turns[stateAtInferenceDone.turns.length - 1];
      if (lastMsg === undefined)
        throw new Error("no messages in state snapshot");
      expect(lastMsg.role).toBe("assistant");
      expect(lastMsg.content[0]).toEqual({
        type: "text",
        text: "Hello from the model",
      });
    });
  });

  test("OpenAI: infer drives inference.done with assembled turn and tail usage", async () => {
    await withHarness(async (harness) => {
      const tailUsage: TokenUsage = {
        input: 100,
        output: 50,
        cacheRead: 10,
        cacheWrite: 0,
        thinking: 0,
      };

      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);
      const chunks = wire.completeResponse("openai", {
        text: "Hello from the model",
        tailUsage,
      });
      stream.enqueueAll(chunks, { startAt: 10 });
      const when = 10 + chunks.length;

      let stateAtInferenceDone: ReactorState | undefined;
      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        source: OPENAI_SOURCE,
        director: directorFromTable({
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, state, caps) => {
            stateAtInferenceDone = state;
            return caps.done();
          },
        }),
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(when + 10);
      await waitFor("reactor.done");

      const inferDone = getEvent(events, "inference.done");
      expect(inferDone.data.turn.content[0]).toEqual({
        type: "text",
        text: "Hello from the model",
      });
      expect(inferDone.data.usage).toEqual(tailUsage);

      if (stateAtInferenceDone === undefined)
        throw new Error("director never received inference.done");
      expect(stateAtInferenceDone.tokenUsage).toEqual(tailUsage);

      const lastMsg =
        stateAtInferenceDone.turns[stateAtInferenceDone.turns.length - 1];
      if (lastMsg === undefined)
        throw new Error("no messages in state snapshot");
      expect(lastMsg.role).toBe("assistant");
    });
  });

  test("Anthropic: streaming text deltas accumulate into final assistant turn", async () => {
    await withHarness(async (harness) => {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);

      // Emit text one token at a time so multiple inference.text.delta events
      // fire through the reactor before the terminal inference.done.
      const tokens = ["Hel", "lo, ", "wor", "ld!"];
      stream.enqueueAt(10, wire.anthropic.messageStart());
      stream.enqueueAt(
        11,
        wire.anthropic.contentBlockStart({ index: 0, kind: "text", text: "" }),
      );
      let when = 12;
      for (const token of tokens) {
        stream.enqueueAt(
          when,
          wire.anthropic.contentBlockDelta({
            index: 0,
            kind: "text_delta",
            text: token,
          }),
        );
        when += 1;
      }
      stream.enqueueAt(when, wire.anthropic.contentBlockStop({ index: 0 }));
      when += 1;
      stream.enqueueAt(when, wire.anthropic.messageDelta({ outputTokens: 7 }));
      when += 1;
      stream.enqueueAt(when, wire.anthropic.messageStop());
      when += 1;
      stream.closeAt(when);

      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        source: ANTHROPIC_SOURCE,
        director: directorFromTable({
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, _s, caps) => caps.done(),
        }),
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(when + 10);
      await waitFor("reactor.done");

      const deltas = events.filter(
        (e): e is Extract<InferenceEvent, { type: "inference.text.delta" }> =>
          e.type === "inference.text.delta",
      );
      expect(deltas.length).toBe(tokens.length);
      const deltaTokens = deltas.map((d) => d.data.token);
      expect(deltaTokens).toEqual(tokens);

      const inferDone = getEvent(events, "inference.done");
      expect(inferDone.data.turn.content[0]).toEqual({
        type: "text",
        text: "Hello, world!",
      });
    });
  });

  test("Anthropic: stream errorAt mid-stream produces inference.error with retryable category and captured partial", async () => {
    await withHarness(async (harness) => {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);
      stream.enqueueAt(10, wire.anthropic.messageStart());
      stream.enqueueAt(
        20,
        wire.anthropic.contentBlockStart({ index: 0, kind: "text", text: "" }),
      );
      stream.enqueueAt(
        30,
        wire.anthropic.contentBlockDelta({
          index: 0,
          kind: "text_delta",
          text: "partial output",
        }),
      );
      stream.errorAt(40, new Error("upstream connection reset"));

      let capturedError: { category: string; message: string } | undefined;
      let capturedPartialText: string | undefined;
      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        source: ANTHROPIC_SOURCE,
        director: directorFromTable({
          // Abort-only policy: the default would retry `retryable`
          // and deadlock against the inert scheduler with no second
          // stream registered.
          "message.received": (_e, _s, caps) =>
            caps.infer({ retryPolicy: () => ({ kind: "abort" }) }),
          "inference.error": (e, _s, caps) => {
            capturedError = {
              category: e.error.category,
              message: e.error.message,
            };
            capturedPartialText = e.partial.text;
            return caps.done();
          },
        }),
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(50);
      await waitFor("reactor.done");

      if (capturedError === undefined)
        throw new Error("director never received inference.error");
      expect(capturedError.category).toBe("retryable");
      // The partial body accumulated before the stream error must reach the
      // director so it can decide whether to retry or surface the partial.
      expect(capturedPartialText).toBe("partial output");

      const inferErr = getEvent(events, "inference.error");
      expect(inferErr.data.error.category).toBe("retryable");
      expect(inferErr.data.partial.text).toBe("partial output");
    });
  });

  test("OpenAI: stream errorAt mid-stream produces inference.error with retryable category and captured partial", async () => {
    await withHarness(async (harness) => {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);
      stream.enqueueAt(10, wire.openai.chunk({ content: "partial output" }));
      stream.errorAt(30, new Error("upstream connection reset"));

      let capturedError: { category: string; message: string } | undefined;
      let capturedPartialText: string | undefined;
      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        source: OPENAI_SOURCE,
        director: directorFromTable({
          // See the matching test above; same rationale.
          "message.received": (_e, _s, caps) =>
            caps.infer({ retryPolicy: () => ({ kind: "abort" }) }),
          "inference.error": (e, _s, caps) => {
            capturedError = {
              category: e.error.category,
              message: e.error.message,
            };
            capturedPartialText = e.partial.text;
            return caps.done();
          },
        }),
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(40);
      await waitFor("reactor.done");

      if (capturedError === undefined)
        throw new Error("director never received inference.error");
      expect(capturedError.category).toBe("retryable");
      expect(capturedPartialText).toBe("partial output");

      const inferErr = getEvent(events, "inference.error");
      expect(inferErr.data.error.category).toBe("retryable");
      expect(inferErr.data.partial.text).toBe("partial output");
    });
  });

  test("Anthropic: tool-call deltas accumulate into final assistant tool_call block", async () => {
    await withHarness(async (harness) => {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);
      // Emit the JSON arguments split across multiple input_json_delta events
      // so the harness's argsBuffer accumulator is exercised.
      const argsFragments = ['{"path":', '"/etc/hosts"', "}"];
      stream.enqueueAt(10, wire.anthropic.messageStart());
      stream.enqueueAt(
        11,
        wire.anthropic.contentBlockStart({
          index: 0,
          kind: "tool_use",
          id: "call-1",
          name: "read_file",
        }),
      );
      let when = 12;
      for (const frag of argsFragments) {
        stream.enqueueAt(
          when,
          wire.anthropic.contentBlockDelta({
            index: 0,
            kind: "input_json_delta",
            partialJson: frag,
          }),
        );
        when += 1;
      }
      stream.enqueueAt(when, wire.anthropic.contentBlockStop({ index: 0 }));
      when += 1;
      stream.enqueueAt(when, wire.anthropic.messageDelta({ outputTokens: 3 }));
      when += 1;
      stream.enqueueAt(when, wire.anthropic.messageStop());
      when += 1;
      stream.closeAt(when);

      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        source: ANTHROPIC_SOURCE,
        director: directorFromTable({
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, _s, caps) => caps.done(),
        }),
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(when + 10);
      await waitFor("reactor.done");

      const inferDone = getEvent(events, "inference.done");
      const toolBlock = inferDone.data.turn.content.find(
        (b) => b.type === "tool_call",
      );
      if (toolBlock === undefined || toolBlock.type !== "tool_call") {
        throw new Error("expected tool_call block in assembled turn");
      }
      expect(toolBlock.id).toBe("call-1");
      expect(toolBlock.name).toBe("read_file");
      expect(toolBlock.arguments).toEqual({ path: "/etc/hosts" });
    });
  });

  test("Anthropic: HTTP 400 with context-overflow body produces inference.error category=context_overflow and drives compact-then-reinfer recovery", async () => {
    await withHarness(async (harness) => {
      // First stream: HTTP 400 classified as context overflow via the
      // adapter's extractErrorMessage + isContextOverflowMessage.
      const overflowStream = harness.scenario.createStream();
      const overflowBody =
        '{"error":{"message":"input is too long for the model context window"}}';
      overflowStream.enqueueAt(5, new TextEncoder().encode(overflowBody));
      overflowStream.closeAt(6);

      // Second stream: HTTP 200 with a clean inference.done for the
      // compact-then-reinfer cycle's second fetch.
      const successStream = harness.scenario.createStream();
      const successChunks = wire.completeResponse("anthropic", {
        text: "ok-after-compact",
        headUsage: emptyUsage(),
        tailUsage: emptyUsage(),
      });
      let successWhen = 100;
      for (const chunk of successChunks) {
        successStream.enqueueAt(successWhen, chunk);
        successWhen += 1;
      }
      successStream.closeAt(successWhen);

      // Counter-driven matchers accept the first and second fetches in
      // order, regardless of URL (both hit the same endpoint).
      let fetchCount = 0;
      harness.scenario.whenRequestMatches(
        () => fetchCount++ === 0,
        overflowStream,
        { status: 400 },
      );
      harness.scenario.whenRequestMatches(() => true, successStream);

      const recording = makeRecordingContextStore();
      const compactor = truncatingCompactor("overflow-compactor");

      const director: ReactorDirector = {
        async decide(event, _state, caps) {
          if (event.type === "message.received") {
            return caps.infer();
          }
          if (event.type === "inference.error") {
            if (event.error.category === "context_overflow") {
              return caps.compact("overflow-compactor", "context-overflow");
            }
            return caps.done();
          }
          if (event.type === "inference.done") {
            return caps.done();
          }
          return caps.wait();
        },
      };

      let capturedErrorCategory: string | undefined;
      const recordingDirector: ReactorDirector = {
        async decide(event, state, caps) {
          if (event.type === "inference.error") {
            capturedErrorCategory = event.error.category;
          }
          return director.decide(event, state, caps);
        },
      };

      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        source: ANTHROPIC_SOURCE,
        contextStore: recording.store,
        compactors: { "overflow-compactor": compactor },
        director: recordingDirector,
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(12);
      // The director issued compact; deliver the second message to
      // trigger the second inference cycle.
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(successWhen + 10);
      await waitFor("reactor.done");

      expect(capturedErrorCategory).toBe("context_overflow");

      const inferErr = getEvent(events, "inference.error");
      expect(inferErr.data.error.category).toBe("context_overflow");

      const compactRecord = recording.manifests
        .flat()
        .find((r) => r.strategy === "overflow-compactor");
      expect(compactRecord).toBeDefined();
      expect(compactRecord?.reason).toBe("explicit");

      const validationErr = events
        .filter((e) => e.type === "reactor.error")
        .find(
          (e) =>
            e.type === "reactor.error" &&
            e.data.error.includes("Invalid action"),
        );
      expect(validationErr).toBeUndefined();
    });
  });

  test("Anthropic: HTTP 5xx body produces inference.error category=retryable through classifyHTTPError", async () => {
    // Port of the synthetic error-delivery test (reactor.test.ts:2398),
    // now through the production classifyHTTPError branch: a 503
    // classifies `retryable` and, unlike 429, surfaces straight to the
    // director on the first cycle.
    await withHarness(async (harness) => {
      const stream = harness.scenario.createStream();
      const body = '{"error":{"message":"rate limited"}}';
      stream.enqueueAt(5, new TextEncoder().encode(body));
      stream.closeAt(6);

      harness.scenario.whenRequestMatches(() => true, stream, { status: 503 });

      let capturedError: { category: string; message: string } | undefined;

      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        source: ANTHROPIC_SOURCE,
        director: directorFromTable({
          // Abort-only policy: the default would retry `retryable`
          // and deadlock against the inert scheduler with no second
          // stream registered.
          "message.received": (_e, _s, caps) =>
            caps.infer({ retryPolicy: () => ({ kind: "abort" }) }),
          "inference.error": (e, _s, caps) => {
            capturedError = {
              category: e.error.category,
              message: e.error.message,
            };
            return caps.done();
          },
        }),
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(20);
      await waitFor("reactor.done");

      if (capturedError === undefined)
        throw new Error("director never received inference.error");
      expect(capturedError.category).toBe("retryable");
      expect(capturedError.message).toBe("rate limited");

      const inferErr = getEvent(events, "inference.error");
      expect(inferErr.data.error.category).toBe("retryable");
    });
  });

  test("OpenAI: tool-call argument fragments accumulate into final assistant tool_call block", async () => {
    await withHarness(async (harness) => {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);
      const argsFragments = ['{"path":', '"/etc/hosts"', "}"];
      stream.enqueueAt(10, wire.openai.toolCallStart(0, "call-1", "read_file"));
      let when = 11;
      for (const frag of argsFragments) {
        stream.enqueueAt(when, wire.openai.toolCallArgumentsDelta(0, frag));
        when += 1;
      }
      stream.enqueueAt(when, wire.openai.done());
      when += 1;
      stream.closeAt(when);

      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        source: OPENAI_SOURCE,
        director: directorFromTable({
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, _s, caps) => caps.done(),
        }),
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(when + 10);
      await waitFor("reactor.done");

      const inferDone = getEvent(events, "inference.done");
      const toolBlock = inferDone.data.turn.content.find(
        (b) => b.type === "tool_call",
      );
      if (toolBlock === undefined || toolBlock.type !== "tool_call") {
        throw new Error("expected tool_call block in assembled turn");
      }
      expect(toolBlock.id).toBe("call-1");
      expect(toolBlock.name).toBe("read_file");
      expect(toolBlock.arguments).toEqual({ path: "/etc/hosts" });
    });
  });
});
