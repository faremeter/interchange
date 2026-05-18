// In-process agent runtime.
//
// `createAgent` resolves storage, builds the provider registry and tool
// dispatcher, wires `createReactorAssembly`, and exposes the public Agent
// surface. The agent owns the active provider config object so
// `setProvider` can mutate it in place and the reactor's lazy read at the
// start of each inference call picks up the new credentials.
//
// Composition:
//   - `send()` enqueues into a FIFO `SendQueue` capped at `sendQueueMax`.
//     Per-send `AbortSignal` removes queued items or rejects in-flight
//     callers while letting the reactor cycle finish in the background.
//   - `stream()` returns a bounded `StreamConsumer` iterator; consumers
//     buffer independently and noisy backpressure poisons only the
//     affected iterator.
//   - `close()` aborts the reactor, drains the send queue with
//     `AgentClosedError`, terminates every active stream iterator, waits
//     up to `closeTimeoutMs` for the reactor's shutdown sequence to
//     complete (audit flush, in-flight commits), and finally releases
//     the singleton-per-`contextDir` lock so another agent can open the
//     same directory.
//
// `setProvider` covers all of `provider`/`baseURL`/`apiKey`/`model`.
// Credentials rotate via the shared `providerConfig` object the reactor
// reads lazily at the start of each inference call. The model rotates
// via a thin wrapper the agent puts around the director's
// `capabilities`: every `capabilities.infer(model, ...)` call the
// director makes is rewritten to use the active provider's current
// model, regardless of which model the director itself captured. This
// makes the rotation invisible to both the default director and any
// caller-supplied director that just relays the model it was
// constructed with.

import { createDefaultDirector } from "@interchange/harness";
import {
  createReactorAssembly,
  type AuthzExtensionOptions,
  type Dependencies,
  type ReactorEmittedEvent,
} from "@interchange/inference";
import { createInboundMessage } from "@interchange/mime";
import { createIsogitStore } from "@interchange/storage-isogit";
import type {
  AssistantTurn,
  AuditStore,
  BlobReader,
  ContextCommit,
  ContextStore,
  ConversationTurn,
  InboundMessage,
  ProviderConfig,
  ReactorCapabilities,
  ReactorDirector,
} from "@interchange/types/runtime";

import { acquireContextDirLock, type ContextDirLock } from "./lock";
import { createProviderRegistry, type ProviderRegistry } from "./provider";
import { createSendQueue, type SendQueue } from "./send-queue";
import { createStreamConsumer, type StreamConsumer } from "./stream";
import { createToolRunner, type AgentTool, type AgentToolRunner } from "./tool";

const DEFAULT_SEND_FROM = "user@local";
const DEFAULT_SEND_TO = "agent@local";
const DEFAULT_SEND_QUEUE_MAX = 16;
const DEFAULT_STREAM_BUFFER_MAX = 1024;
const DEFAULT_CLOSE_TIMEOUT_MS = 5000;

export type AgentConfig = {
  /**
   * The conversation/history store. Exactly one of `contextStore` or
   * `contextDir` must be supplied. When `contextStore` is given the caller
   * owns the store's lifetime; the singleton-per-directory lock is not
   * acquired.
   */
  contextStore?: ContextStore;

  /**
   * Path to a directory the agent will manage as an isogit-backed
   * `ContextStore & AuditStore`. The singleton-per-directory lock is
   * acquired when this form is used.
   */
  contextDir?: string;

  /** Pre-configured providers. Must be non-empty. Each entry must have model. */
  providers: ProviderConfig[];
  /** Must match the `model` field of one of `providers`. */
  defaultModel: string;

  /** System prompt for the default director. */
  systemPrompt: string;
  /** Tools registered via `tool()` / `stringTool()`. */
  tools: AgentTool[];
  /** Director override. Defaults to `createDefaultDirector`. */
  director?: ReactorDirector;

  /** Audit store. When omitted with `contextDir`, the isogit store is used. */
  auditStore?: AuditStore;
  /** Authz extension hook. */
  authorize?: AuthzExtensionOptions["authorize"];
  /** Override the default 10k tool-result size cap. */
  sizeCapMaxChars?: number;

  /** Override the auto-generated session ID. */
  sessionId?: string;

  /**
   * Maximum number of pending sends (active + queued). Once reached,
   * additional `send()` calls throw `SendQueueFullError` synchronously.
   * Defaults to 16.
   */
  sendQueueMax?: number;

  /**
   * Maximum events any single `stream()` consumer may buffer. When a
   * consumer falls more than this many events behind the next read on
   * that consumer's iterator throws `StreamBackpressureError`; other
   * consumers are unaffected. Defaults to 1024.
   */
  streamBufferMax?: number;

  /**
   * Inference dependencies (notably `fetch`) for the reactor's underlying
   * `runInference` call. Production callers should leave this undefined —
   * the assembly falls back to `createDefaultDependencies()` which binds
   * `globalThis.fetch`. Pass `setupHarness().deps` from
   * `@interchange/inference-testing` in tests to swap the fetch
   * implementation for a deterministic stub.
   */
  deps?: Dependencies;

  /**
   * Maximum milliseconds `close()` waits for the reactor's shutdown
   * sequence (which flushes audit and any pending commits) before
   * releasing the singleton `contextDir` lock and returning. Defaults
   * to 5000ms. Set to 0 to release immediately without waiting (useful
   * for tests where the reactor's shutdown is intentionally blocked).
   */
  closeTimeoutMs?: number;
};

export type SendOptions = {
  /**
   * Abort signal for this send. When the signal fires before processing
   * the call is dropped from the queue and the promise rejects with the
   * signal's reason. When it fires mid-cycle the promise rejects
   * immediately, but the underlying reactor cycle keeps running because
   * the reactor does not expose per-cycle cancellation — the next queued
   * send waits for that cycle to finish before starting. The reply (if
   * any) is still visible via `stream()` and `history()`.
   */
  signal?: AbortSignal;
  /** Override the default "from" header on the synthetic inbound message. */
  from?: string;
};

export type SendResult = {
  /** Reply text emitted by the director's `reply` action. */
  reply: string;
  /**
   * Full-fidelity assistant turn that produced the reply. Captured from
   * the reactor's `inference.done` event preceding `connector.reply`.
   */
  turn: ConversationTurn;
};

export type Agent = {
  send(
    content: string | InboundMessage,
    opts?: SendOptions,
  ): Promise<SendResult>;
  stream(): AsyncIterable<ReactorEmittedEvent>;
  deliver(message: InboundMessage): void;
  close(): Promise<void>;
  /**
   * Replace the active provider's fields in place. Picked up at the
   * start of the next inference call. `model` is rotated alongside
   * `provider`/`baseURL`/`apiKey`: the agent wraps the director so
   * every `capabilities.infer(model, ...)` call uses the active
   * provider's current model.
   */
  setProvider(config: ProviderConfig): void;
  /**
   * Project conversation history from the underlying context store.
   * Remains callable after `close()` — reads do not need the reactor
   * and the store is not destroyed by close. Returns the full-fidelity
   * `ConversationTurn[]` from the store's latest committed state.
   */
  history(): Promise<ConversationTurn[]>;
  /**
   * List recent checkpoints from the context store. Remains callable
   * after `close()` for the same reason as `history()`.
   */
  checkpoints(limit?: number): Promise<ContextCommit[]>;
  /**
   * Read the conversation turns recorded at a specific commit hash.
   * Remains callable after `close()` for the same reason as
   * `history()`.
   */
  readAt(hash: string): Promise<ConversationTurn[]>;
  readonly blobReader: BlobReader;
};

export class AgentConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AgentConfigError";
  }
}

export class AgentClosedError extends Error {
  constructor() {
    super("agent is closed");
    this.name = "AgentClosedError";
  }
}

export async function createAgent(config: AgentConfig): Promise<Agent> {
  const hasStore = config.contextStore !== undefined;
  const hasDir = config.contextDir !== undefined;
  if (hasStore === hasDir) {
    throw new AgentConfigError(
      "exactly one of contextStore or contextDir is required",
    );
  }

  let contextStore: ContextStore;
  let auditStore: AuditStore | undefined;
  let lock: ContextDirLock | undefined;

  if (config.contextDir !== undefined) {
    lock = acquireContextDirLock(config.contextDir);
    try {
      const store = await createIsogitStore(config.contextDir);
      contextStore = store;
      auditStore = config.auditStore ?? store;
    } catch (cause) {
      lock.release();
      throw cause;
    }
  } else if (config.contextStore !== undefined) {
    contextStore = config.contextStore;
    auditStore = config.auditStore;
  } else {
    throw new AgentConfigError("unreachable: storage form validated above");
  }

  let providerRegistry: ProviderRegistry;
  let toolRunner: AgentToolRunner;
  try {
    providerRegistry = createProviderRegistry({
      providers: config.providers,
      defaultModel: config.defaultModel,
    });
    toolRunner = createToolRunner(config.tools);
  } catch (cause) {
    if (lock !== undefined) lock.release();
    throw cause;
  }

  // Capture the initial model so the capabilities wrapper has a safe
  // fallback if a setProvider call ever leaves active.model undefined.
  if (providerRegistry.active.model === undefined) {
    if (lock !== undefined) lock.release();
    throw new AgentConfigError("active provider must have model defined");
  }
  const initialModel = providerRegistry.active.model;

  let baseDirector: ReactorDirector;
  if (config.director !== undefined) {
    baseDirector = config.director;
  } else {
    baseDirector = createDefaultDirector(
      initialModel,
      config.systemPrompt,
      [...toolRunner.definitions],
      {},
    );
  }

  // Wrap the director's `capabilities` so every `infer(model, ...)` call
  // uses the live model from providerRegistry.active. This lets
  // setProvider rotate `model` (alongside provider/baseURL/apiKey)
  // without requiring the inner director to know about it. The default
  // director captures `model` at construction; this wrapper substitutes
  // the live value at action-build time.
  const director: ReactorDirector = {
    async decide(event, state, capabilities) {
      const wrappedCapabilities: ReactorCapabilities = {
        ...capabilities,
        infer: (_requestedModel, options) => {
          const liveModel = providerRegistry.active.model ?? initialModel;
          return options === undefined
            ? capabilities.infer(liveModel)
            : capabilities.infer(liveModel, options);
        },
      };
      return baseDirector.decide(event, state, wrappedCapabilities);
    },
  };

  const sessionId = config.sessionId ?? crypto.randomUUID();

  const streamBufferMax = config.streamBufferMax ?? DEFAULT_STREAM_BUFFER_MAX;
  const streamConsumers = new Set<StreamConsumer>();

  // Per-active-cycle bookkeeping for send(). The reactor produces one or
  // more inference.done events during a cycle; we keep the most recent
  // assistant turn so the final connector.reply can be paired with the
  // full-fidelity turn (rather than a synthesized text-only fallback).
  type ActiveCycle = { lastAssistantTurn: AssistantTurn | undefined };
  let activeCycle: ActiveCycle | null = null;

  // sendQueue is built after the reactor (since its `start` callback
  // delivers into the reactor), but handleEvent — which is wired into the
  // reactor's assembly — needs to see sendQueue. Assigned exactly once
  // after the reactor exists and before reactor.start(); no event can
  // reach handleEvent before the queue is wired.
  // eslint-disable-next-line prefer-const -- forward declaration; const cannot express this ordering
  let sendQueue: SendQueue<InboundMessage, SendResult>;

  // shutdownComplete resolves from the assembly's onShutdown hook
  // (composed after audit flush by the assembly) or, as a fallback, from
  // handleEvent observing the reactor's terminal `reactor.done` event.
  // close() awaits this (with a timeout) before releasing the
  // contextDir lock so a subsequent createAgent on the same directory
  // sees a quiesced store.
  let resolveShutdown: () => void = () => {
    // Reassigned by the Promise constructor below; seed a no-op so the
    // forward-referenced call in handleEvent is safe even if the
    // Promise constructor has not yet run (it does, synchronously, on
    // the next line).
  };
  const shutdownComplete = new Promise<void>((resolve) => {
    resolveShutdown = resolve;
  });

  function buildSyntheticTurn(text: string): ConversationTurn {
    return {
      role: "assistant",
      content: [{ type: "text", text }],
      ...(providerRegistry.active.model !== undefined
        ? { model: providerRegistry.active.model }
        : {}),
      timestamp: Date.now(),
    };
  }

  function handleEvent(event: ReactorEmittedEvent): void {
    if (activeCycle !== null && event.type === "inference.done") {
      activeCycle.lastAssistantTurn = event.data.turn;
    }

    if (activeCycle !== null) {
      if (event.type === "connector.reply") {
        const turn: ConversationTurn =
          activeCycle.lastAssistantTurn ??
          buildSyntheticTurn(event.data.content);
        activeCycle = null;
        sendQueue.resolveActive({ reply: event.data.content, turn });
      } else if (event.type === "reactor.error" && event.data.fatal) {
        // Only fatal reactor errors terminate the active send. Non-fatal
        // errors (e.g. transient write/commit failures the reactor is
        // recovering from) are surfaced via stream() but must not
        // resolve send() — the cycle is still running and may yet
        // produce connector.reply or a fatal error.
        activeCycle = null;
        sendQueue.rejectActive(new Error(`reactor error: ${event.data.error}`));
      } else if (event.type === "reactor.done") {
        activeCycle = null;
        sendQueue.rejectActive(new AgentClosedError());
      }
    }

    // reactor.done is the reactor's terminal event. Resolve
    // shutdownComplete here in addition to the onShutdown hook so close()
    // does not hang for the full closeTimeoutMs on paths where the hook
    // never fires (e.g. the reactor's context-store load fails during
    // start, or the composed onShutdown wrapper throws during audit
    // flush). resolveShutdown is idempotent.
    if (event.type === "reactor.done") {
      resolveShutdown();
    }

    // Iterate a snapshot so removing closed consumers mid-iteration is
    // not just relying on Set's iteration tolerance.
    for (const consumer of Array.from(streamConsumers)) {
      consumer.push(event);
      if (consumer.closed) {
        streamConsumers.delete(consumer);
      }
    }
  }

  const { reactor, blobReader } = createReactorAssembly({
    sessionId,
    director,
    providerConfig: providerRegistry.active,
    toolRunner,
    contextStore,
    onEvent: handleEvent,
    onShutdown: async () => {
      resolveShutdown();
    },
    ...(auditStore !== undefined ? { auditStore } : {}),
    ...(config.authorize !== undefined ? { authorize: config.authorize } : {}),
    ...(config.sizeCapMaxChars !== undefined
      ? { sizeCapMaxChars: config.sizeCapMaxChars }
      : {}),
    ...(config.deps !== undefined ? { deps: config.deps } : {}),
  });

  sendQueue = createSendQueue<InboundMessage, SendResult>({
    maxDepth: config.sendQueueMax ?? DEFAULT_SEND_QUEUE_MAX,
    start: (message) => {
      activeCycle = { lastAssistantTurn: undefined };
      reactor.deliver(message);
    },
  });

  reactor.start();

  let closed = false;

  function ensureOpen(): void {
    if (closed) throw new AgentClosedError();
  }

  function buildInboundMessage(
    content: string | InboundMessage,
    opts?: SendOptions,
  ): InboundMessage {
    if (typeof content !== "string") return content;
    // Conversation messages use `content` (a string); the mail-builder
    // rejects passing `payload` for conversation types.
    return createInboundMessage({
      from: opts?.from ?? DEFAULT_SEND_FROM,
      to: DEFAULT_SEND_TO,
      content,
      interchangeType: "conversation.message",
    });
  }

  function send(
    content: string | InboundMessage,
    opts?: SendOptions,
  ): Promise<SendResult> {
    // Closed-agent errors come back as rejections so callers can handle
    // them with `.catch()` instead of having to defensively wrap every
    // `agent.send(...)` in a try/catch. `SendQueueFullError` from
    // `sendQueue.enqueue` is left as a synchronous throw — it signals a
    // programmer error (the caller exceeded the configured queue cap)
    // and per the design must fail loud.
    if (closed) return Promise.reject(new AgentClosedError());
    const message = buildInboundMessage(content, opts);
    return sendQueue.enqueue(message, opts?.signal);
  }

  function stream(): AsyncIterable<ReactorEmittedEvent> {
    ensureOpen();
    const consumer = createStreamConsumer(streamBufferMax);
    streamConsumers.add(consumer);
    return consumer.iterator();
  }

  function deliver(message: InboundMessage): void {
    ensureOpen();
    reactor.deliver(message);
  }

  function setProvider(cfg: ProviderConfig): void {
    ensureOpen();
    providerRegistry.setProvider(cfg);
  }

  async function history(): Promise<ConversationTurn[]> {
    const loaded = await contextStore.load();
    return loaded.turns;
  }

  async function checkpoints(limit?: number): Promise<ContextCommit[]> {
    return contextStore.log(limit);
  }

  async function readAt(hash: string): Promise<ConversationTurn[]> {
    return contextStore.readAt(hash);
  }

  async function close(): Promise<void> {
    if (closed) return;
    closed = true;
    reactor.abort("user_disconnect");
    sendQueue.drain(new AgentClosedError());
    activeCycle = null;
    for (const consumer of streamConsumers) consumer.close();
    streamConsumers.clear();

    // Wait for the reactor's shutdown sequence (audit flush, in-flight
    // commits) before releasing the lock so a subsequent createAgent on
    // the same contextDir does not race with background writers against
    // the same .git directory. The timeout is a backstop: if the
    // reactor's shutdown is genuinely stuck (e.g. a parked test fetch
    // that never resolves) we release the lock anyway rather than
    // deadlock the caller. `closeTimeoutMs: 0` disables the wait.
    const timeoutMs = config.closeTimeoutMs ?? DEFAULT_CLOSE_TIMEOUT_MS;
    if (timeoutMs > 0) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<void>((resolve) => {
        timer = setTimeout(resolve, timeoutMs);
      });
      try {
        await Promise.race([shutdownComplete, timeout]);
      } finally {
        if (timer !== undefined) clearTimeout(timer);
      }
    }
    if (lock !== undefined) lock.release();
  }

  return {
    send,
    stream,
    deliver,
    close,
    setProvider,
    history,
    checkpoints,
    readAt,
    blobReader,
  };
}
