// In-process agent runtime.
//
// `createAgent(def, env)` is the single entry point. The `def` is the
// portable, hashable `AgentDefinition` (id, system prompt, tool
// factories, director ref, inference preferences, capabilities, tags).
// The `env` is the runtime environment supplying the active inference
// source, the context store, the working directory, the audit sink,
// the authorize callback, and the director registry. The agent
// instantiates against those: it locks the context directory, walks
// each tool factory to build its tool runner, resolves the director
// against the registry, and wires the result into the reactor
// assembly. The reactor is wrapped exactly once.
//
// Composition:
//   - `send()` enqueues into a FIFO `SendQueue` capped at
//     `env.sendQueueMax`. Per-send `AbortSignal` removes queued items or
//     rejects in-flight callers while letting the reactor cycle finish
//     in the background.
//   - `stream()` returns a bounded `StreamConsumer` iterator; consumers
//     buffer independently and noisy backpressure poisons only the
//     affected iterator.
//   - `close()` aborts the reactor, drains the send queue with
//     `AgentClosedError`, terminates every active stream iterator, waits
//     up to `env.closeTimeoutMs` for the reactor's shutdown sequence to
//     complete (audit flush, in-flight commits), and finally releases
//     the singleton-per-`workdir` lock so another agent can open the
//     same directory.
//
// `setSource` covers the whole source: id/provider/baseURL/apiKey/model
// plus the model-bound `defaults` and `capabilities`. Credentials and
// model rotate together via the shared source object the reactor reads
// lazily at the start of each inference call. The director never names
// a model -- `capabilities.infer(options?)` does not take one -- so the
// active source's model is the single source of truth and rotations
// take effect on the next inference call without any wrapper.
//
// Tool factories are bundle-shaped: each declares `(env) => ToolBundle`
// via `defineTool`. The agent invokes each factory once at construction,
// collects the bundles' definitions, and dispatches calls to the
// owning bundle's `run`. Bundle lifetimes (and any `dispose` step) are
// the caller's responsibility -- the env is the agent's dependency
// contract; the caller owns the lifetime of what it puts in env.

import {
  createReactorAssembly,
  type Dependencies,
  type ReactorEmittedEvent,
} from "@intx/inference";
import { getLogger } from "@intx/log";
import { createInboundMessage } from "@intx/mime";
import type { ErrorRecord } from "@intx/types/audit";
import type {
  AssistantTurn,
  BlobReader,
  ContextCommit,
  ContextStore,
  ConversationTurn,
  InboundMessage,
  InferenceSource,
  ReactorDirector,
  ToolCall,
  ToolDefinition,
  ToolResult,
  ToolRunner,
} from "@intx/types/runtime";

import type { AgentDefinition } from "./definition";
import { validateDirectorConfig } from "./director";
import type { DirectorRef } from "./director-types";
import type { BaseEnv } from "./env";
import { validateEnv } from "./env-validation";
import { acquireContextDirLock, type ContextDirLock } from "./lock";
import { createSourceRegistry } from "./source";
import { createSendQueue, type SendQueue } from "./send-queue";
import { createStreamConsumer, type StreamConsumer } from "./stream";
import { DuplicateToolError, type ToolBundle } from "./tool";

const logger = getLogger(["interchange", "agent"]);

// Synthetic recipient/sender used when `agent.send(content)` is
// called with a plain string. `agent.send` is the in-process API for
// driving an agent without a transport; the synthesized message is
// never sent over the wire, so the addresses are just shape-fillers
// for the reactor's MIME-derived event shape. The `from` field is
// override-able via `SendOptions.from` because callers occasionally
// want to stamp a meaningful sender for audit purposes. The `to`
// field is fixed because no in-tree call path makes a routing or
// audit decision on it: harness-wrapped agents do not surface
// `agent.send` (the `Harness` shape exposes only deliver/setSource/
// stream/close/blobReader), and standalone agents have no addressing
// substrate to begin with. Callers that need an addressable inbound
// message build the `InboundMessage` themselves and pass it to
// `agent.send(message)` directly, bypassing this synthesis path.
const DEFAULT_SEND_FROM = "user@local";
const DEFAULT_SEND_TO = "agent@local";
const DEFAULT_SEND_QUEUE_MAX = 16;
const DEFAULT_STREAM_BUFFER_MAX = 1024;
const DEFAULT_CLOSE_TIMEOUT_MS = 5000;

export type SendOptions = {
  /**
   * Abort signal for this send. When the signal fires before processing
   * the call is dropped from the queue and the promise rejects with the
   * signal's reason. When it fires mid-cycle the promise rejects
   * immediately, but the underlying reactor cycle keeps running because
   * the reactor does not expose per-cycle cancellation -- the next
   * queued send waits for that cycle to finish before starting. The
   * reply (if any) is still visible via `stream()` and `history()`.
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
  /**
   * Begin shutdown. The reactor is aborted, the send queue drains with
   * `AgentClosedError`, every active stream iterator terminates, the
   * shutdown sequence (audit flush + in-flight commits) is awaited up
   * to `env.closeTimeoutMs`, and the singleton-per-workdir lock is
   * released so another agent can open the same directory.
   *
   * Stream consumers are terminated synchronously before
   * `shutdownComplete` is awaited, so any reactor event emitted in the
   * shutdown window (after `reactor.abort()` but before the assembly's
   * `onShutdown` resolves) is no longer visible to a `stream()`
   * iterator. The audit path is not affected: `inference.error` and
   * `reactor.error` records emitted in that window still flow through
   * `accumulatedErrors` and are flushed by `onShutdown`. Callers that
   * need to observe late events should subscribe before `close()` and
   * tolerate the iterator's terminal close.
   */
  close(): Promise<void>;
  /**
   * Replace the active source's fields in place. Picked up at the start
   * of the next inference call. `model` rotates alongside the
   * credentials -- the director does not name a model, so the active
   * source's `model` is what the next inference call uses without any
   * additional plumbing.
   */
  setSource(source: InferenceSource): void;
  /**
   * Project conversation history from the underlying context store.
   * Remains callable after `close()` -- reads do not need the reactor
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

export class AgentClosedError extends Error {
  constructor() {
    super("agent is closed");
    this.name = "AgentClosedError";
  }
}

interface ResolvedTools {
  readonly definitions: readonly ToolDefinition[];
  readonly runner: ToolRunner & {
    readonly definitions: readonly ToolDefinition[];
  };
  /**
   * The bundles `resolveTools` constructed in order. Returned so the
   * surrounding `createAgent` can dispose them on construction
   * failures past `resolveTools` (resolveDirector, createReactorAssembly,
   * createSourceRegistry, etc.) -- the caller never reaches the
   * returned agent in those paths, so the per-`ToolBundle` "caller
   * owns lifetime" contract has no caller to honor it. Once
   * `createAgent` returns successfully, the bundle list is dropped
   * and disposal returns to being the caller's responsibility per the
   * standard `ToolBundle` contract.
   */
  readonly bundles: readonly ToolBundle[];
}

/**
 * Walk each annotated tool factory, build the bundle, and produce a
 * single `ToolRunner` that dispatches calls by tool name to the
 * originating bundle. Throws on duplicate tool names across bundles.
 *
 * Bundle lifetimes (disposal) are the caller's responsibility per the
 * `ToolBundle` contract once `createAgent` returns. While
 * `createAgent` is still constructing -- whether the failure surfaces
 * inside this function or later in `createAgent`'s body -- there is
 * no caller to honor that contract, so the bundles list is exposed
 * for the surrounding `try`/`finally` to dispose on failure.
 */
function resolveTools<EnvReq extends BaseEnv>(
  def: AgentDefinition<EnvReq>,
  env: EnvReq,
): ResolvedTools {
  const byName = new Map<string, ToolBundle>();
  const definitions: ToolDefinition[] = [];
  // Track constructed bundles so we can dispose them on a later
  // factory's failure. Once `resolveTools` returns successfully the
  // caller (createAgent) is the lifetime owner per the `ToolBundle`
  // contract; until then the only reference is in this function.
  const constructed: ToolBundle[] = [];

  try {
    for (const factory of def.toolFactories) {
      const bundle = factory(env);
      constructed.push(bundle);
      for (const definition of bundle.definitions) {
        if (byName.has(definition.name)) {
          throw new DuplicateToolError(definition.name);
        }
        byName.set(definition.name, bundle);
        definitions.push(definition);
      }
    }
  } catch (cause) {
    // Dispose every bundle we did successfully construct before
    // re-raising. Without this, factories that allocate resources at
    // construction time (mail bundles holding an IMAP session, posix
    // bundles spawning an LSP server, etc.) leak when a later
    // factory throws or a duplicate-name collision aborts the walk.
    for (const bundle of constructed) {
      if (bundle.dispose === undefined) continue;
      try {
        // Swallow disposer errors so the original construction
        // failure remains the one the caller sees; a noisy disposer
        // running during rollback would mask the real problem.
        //
        // `void bundle.dispose()` would not be enough on its own: it
        // discards the returned promise but leaves any rejection in
        // flight, which the surrounding synchronous try/catch cannot
        // observe and the runtime surfaces as an unhandled promise
        // rejection. We attach a no-op `.catch` to absorb async
        // rejections and let the throw below propagate immediately
        // (the caller's lock is still held; awaiting rollback would
        // delay the construction failure for no benefit).
        const result = bundle.dispose();
        if (result instanceof Promise) {
          result.catch(() => {
            // Swallow per the comment above.
          });
        }
      } catch {
        // Synchronous throws from a non-async dispose that throws
        // before returning a promise. Same intent as the async path:
        // never let rollback noise mask the original failure.
      }
    }
    throw cause;
  }

  const runner: ToolRunner & { definitions: readonly ToolDefinition[] } = {
    definitions: Object.freeze([...definitions]),
    async run(call: ToolCall, signal: AbortSignal): Promise<ToolResult> {
      const bundle = byName.get(call.name);
      if (bundle === undefined) {
        return {
          callId: call.id,
          content: `unknown tool: ${call.name}`,
          isError: true,
        };
      }
      try {
        return await bundle.run(call, signal);
      } catch (err) {
        return {
          callId: call.id,
          content: err instanceof Error ? err.message : String(err),
          isError: true,
        };
      }
    },
  };

  return { definitions, runner, bundles: constructed };
}

function resolveDirector<EnvReq extends BaseEnv>(
  def: AgentDefinition<EnvReq>,
  env: EnvReq,
  toolDefinitions: readonly ToolDefinition[],
): ReactorDirector {
  const ref: DirectorRef = def.director ?? env.directors.buildDefaultRef();
  const factory = env.directors.resolve(ref);
  // Re-validate ref.config against the factory's registered schema.
  // `defineDirector.build(config)` validates at construction time, but
  // `DirectorRef` is a public structural type -- nothing forces refs
  // through `build`. A hand-constructed ref would otherwise reach the
  // factory body with whatever shape the author wrote.
  validateDirectorConfig(ref.config, factory.configSchema);
  return factory(ref.config, env, {
    systemPrompt: def.systemPrompt,
    toolDefinitions,
  });
}

export async function createAgent<EnvReq extends BaseEnv>(
  def: AgentDefinition<EnvReq>,
  env: EnvReq,
): Promise<Agent> {
  validateEnv(def, env);

  const lock: ContextDirLock = acquireContextDirLock(env.workdir);

  // The construction below acquires several resources before the
  // returned Agent's `close()` becomes reachable. Anything that
  // throws between here and the final return leaks the lock and any
  // tool-bundle resources unless we explicitly release them. Track
  // the success path with a flag, release the lock in `finally` when
  // we never reached the return, and dispose every successfully
  // constructed tool bundle so post-`resolveTools` failures
  // (resolveDirector throw, createReactorAssembly throw,
  // createSourceRegistry throw, reactor.start throw) don't leak the
  // bundles `resolveTools` built. The intra-`resolveTools` rollback
  // disposes bundles that were constructed before the throwing
  // factory; this outer rollback covers the rest.
  let succeeded = false;
  let bundlesForRollback: readonly ToolBundle[] = [];
  try {
    const resolvedTools = resolveTools(def, env);
    bundlesForRollback = resolvedTools.bundles;
    const sourceRegistry = createSourceRegistry({
      sources: [env.source],
      defaultSource: env.source.id,
    });
    const director = resolveDirector(def, env, resolvedTools.definitions);

    const contextStore: ContextStore = env.storage;
    const auditStore = env.audit;
    const authorize = env.authorize;
    const deps: Dependencies | undefined = env.deps;

    const sessionId = env.sessionId ?? crypto.randomUUID();
    const streamBufferMax = env.streamBufferMax ?? DEFAULT_STREAM_BUFFER_MAX;
    const streamConsumers = new Set<StreamConsumer>();

    // Pre-start buffer for events emitted between `reactor.start()` and
    // the first `stream()` consumer attaching. Without this buffer
    // those events fan out into an empty consumer set and are dropped
    // silently: `reactor.start()` runs synchronously inside
    // `createAgent`, before the caller has a chance to register a
    // consumer, so a `reactor.start` event (or any other event the
    // reactor emits during its synchronous startup window) would be
    // lost. We buffer up to `streamBufferMax` events; when the first
    // consumer attaches, the buffer is drained into it and discarded.
    // Subsequent consumers see only events emitted after their own
    // registration, matching the existing per-consumer fan-out
    // semantics. Overflow during the pre-start window drops the
    // oldest events with a log warning rather than throwing: aborting
    // `reactor.start()` mid-startup leaves the agent in a worse state
    // than missing observability for the very earliest events, and a
    // startup that emits more than `streamBufferMax` events before
    // any consumer registers is a pathology the caller can observe
    // via the warning.
    let preStartBuffer: ReactorEmittedEvent[] | undefined = [];
    let preStartBufferOverflows = 0;

    // Per-active-cycle bookkeeping for send(). The reactor produces one or
    // more inference.done events during a cycle; we keep the most recent
    // assistant turn so the final connector.reply can be paired with the
    // full-fidelity turn (rather than a synthesized text-only fallback).
    type ActiveCycle = { lastAssistantTurn: AssistantTurn | undefined };
    let activeCycle: ActiveCycle | null = null;

    // sendQueue is built after the reactor (since its `start` callback
    // delivers into the reactor), but handleEvent -- which is wired
    // into the reactor's assembly -- needs to see sendQueue. Assigned
    // exactly once after the reactor exists and before
    // reactor.start(); no event can reach handleEvent before the
    // queue is wired.
    //
    // The cycle is irreducible at the type level: `handleEvent`
    // reads `sendQueue` from closure; `sendQueue.start` calls
    // `reactor.deliver`; `reactor` is constructed with
    // `onEvent: handleEvent`. Three references, each pointing at
    // the next. `const` requires its initializer at declaration time,
    // which forces the cycle to break at one of these edges --
    // every break either threads an extra parameter through
    // handleEvent (which the reactor's `onEvent` shape does not
    // accept), wraps sendQueue behind a `{ value: SendQueue }` cell
    // (which makes every send-site check for undefined that the
    // construction order already guarantees absent), or splits
    // handleEvent into a factory that takes sendQueue as input
    // (which moves the same forward-declaration problem one level
    // up). The `let` here is the smallest expression of the cycle
    // the language allows; the comment block above is what makes
    // the "assigned before any reachable read" invariant explicit.
    // eslint-disable-next-line prefer-const -- forward declaration; const cannot express this ordering
    let sendQueue: SendQueue<InboundMessage, SendResult>;

    // shutdownComplete resolves from the assembly's onShutdown hook
    // (composed after audit flush by the assembly) or, as a fallback, from
    // handleEvent observing the reactor's terminal `reactor.done` event.
    // close() awaits this (with a timeout) before releasing the
    // workdir lock so a subsequent createAgent on the same directory
    // sees a quiesced store.
    //
    // Use Promise.withResolvers so `resolveShutdown` is bound to the
    // promise's resolve function at the point of declaration rather
    // than after the Promise constructor's synchronous executor runs;
    // the previous pattern needed a no-op seed for a TDZ window that
    // the language already closes synchronously.
    const {
      promise: shutdownComplete,
      resolve: resolveShutdown,
      // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Promise.withResolvers<void>() is the conventional shape for a fire-and-forget settled-signal; matches Promise<void> used elsewhere on this assembly
    } = Promise.withResolvers<void>();

    // Error accumulation. inference.error and reactor.error events
    // observed at the assembly's onEvent boundary accumulate here and
    // flush at the assembly's afterCheckpoint and onShutdown lifecycle
    // hooks. Audit recording is always wired now: env.audit is required.
    //
    // Serialization through `flushInProgress` + `pendingFollowUp`: if
    // a flush is already running, all concurrent callers ride a single
    // shared follow-up promise that fires exactly once after the
    // current flush settles. This prevents the multi-caller race
    // where N concurrent chained continuations each observe
    // `flushInProgress === undefined` in the same microtask drain and
    // start parallel `commitErrors(batch)` invocations on the same
    // prefix -- which would double-commit and incorrectly splice the
    // accumulator. The shared follow-up clears itself before invoking
    // the next flush, so a fourth caller arriving after the follow-up
    // begins still observes a clean state and starts its own flush.
    const accumulatedErrors: ErrorRecord[] = [];
    let errorSeq = 0;
    let flushInProgress: Promise<void> | undefined;
    let pendingFollowUp: Promise<void> | undefined;

    function flushErrors(): Promise<void> {
      if (flushInProgress !== undefined) {
        // If another caller already arranged a follow-up flush after
        // the current one settles, ride that. Otherwise arrange one
        // and let every later concurrent caller share it. Run the
        // follow-up on both fulfilment and rejection: if the in-flight
        // flush failed, the accumulator still holds its records and
        // the next attempt should retry rather than observe the prior
        // failure.
        if (pendingFollowUp !== undefined) return pendingFollowUp;
        pendingFollowUp = flushInProgress.then(
          () => {
            pendingFollowUp = undefined;
            return flushErrors();
          },
          () => {
            pendingFollowUp = undefined;
            return flushErrors();
          },
        );
        return pendingFollowUp;
      }
      if (accumulatedErrors.length === 0) return Promise.resolve();
      const count = accumulatedErrors.length;
      const batch = accumulatedErrors.slice(0, count);
      // Splice only after a successful commit. A throwing audit store
      // must not lose the batch -- the next flush hook (a later
      // afterCheckpoint or the onShutdown drain) retries the same
      // records. Note this means that on a permanent audit-store
      // failure, the accumulator grows unbounded; the assembly's
      // expectation is that commitErrors failures are transient.
      flushInProgress = (async () => {
        try {
          await auditStore.commitErrors(batch);
          accumulatedErrors.splice(0, count);
        } finally {
          flushInProgress = undefined;
        }
      })();
      return flushInProgress;
    }

    function buildSyntheticTurn(text: string): ConversationTurn {
      return {
        role: "assistant",
        content: [{ type: "text", text }],
        model: sourceRegistry.active.model,
        timestamp: Date.now(),
      };
    }

    function handleEvent(event: ReactorEmittedEvent): void {
      if (event.type === "inference.error") {
        accumulatedErrors.push({
          source: "inference",
          category: event.data.error.category,
          message: event.data.error.message,
          fatal: false,
          timestamp: new Date().toISOString(),
          sessionId,
          seq: errorSeq++,
          ...(event.data.error.statusCode !== undefined
            ? { statusCode: event.data.error.statusCode }
            : {}),
        });
      } else if (event.type === "reactor.error") {
        accumulatedErrors.push({
          source: "reactor",
          category: "reactor_error",
          message: event.data.error,
          fatal: event.data.fatal,
          timestamp: new Date().toISOString(),
          sessionId,
          seq: errorSeq++,
        });
      }

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
          // resolve send() -- the cycle is still running and may yet
          // produce connector.reply or a fatal error.
          activeCycle = null;
          sendQueue.rejectActive(
            new Error(`reactor error: ${event.data.error}`),
          );
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

      // Pre-start window: if no consumer has attached yet, buffer the
      // event so the first consumer to attach picks it up. The buffer
      // is discarded after the first drain; later consumers see only
      // events emitted after their own registration. Overflow drops
      // the oldest event with a log warning -- raising here would
      // abort reactor startup, which is worse than missing
      // observability for the earliest events.
      if (preStartBuffer !== undefined && streamConsumers.size === 0) {
        if (preStartBuffer.length >= streamBufferMax) {
          preStartBuffer.shift();
          preStartBufferOverflows += 1;
        }
        preStartBuffer.push(event);
        return;
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
      source: sourceRegistry.active,
      toolRunner: resolvedTools.runner,
      contextStore,
      onEvent: handleEvent,
      auditStore,
      authorize,
      onShutdown: async () => {
        try {
          await flushErrors();
        } finally {
          resolveShutdown();
        }
      },
      afterCheckpoint: flushErrors,
      ...(env.sizeCapMaxChars !== undefined
        ? { sizeCapMaxChars: env.sizeCapMaxChars }
        : {}),
      ...(deps !== undefined ? { deps } : {}),
    });

    sendQueue = createSendQueue<InboundMessage, SendResult>({
      maxDepth: env.sendQueueMax ?? DEFAULT_SEND_QUEUE_MAX,
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
      // `sendQueue.enqueue` is left as a synchronous throw -- it signals a
      // programmer error (the caller exceeded the configured queue cap)
      // and per the design must fail loud.
      if (closed) return Promise.reject(new AgentClosedError());
      const message = buildInboundMessage(content, opts);
      return sendQueue.enqueue(message, opts?.signal);
    }

    function stream(): AsyncIterable<ReactorEmittedEvent> {
      ensureOpen();
      const consumer = createStreamConsumer(streamBufferMax);
      // Drain the pre-start buffer into the first consumer that
      // attaches so events emitted between reactor.start() and the
      // first stream() call are not lost. The buffer is discarded
      // after the first drain -- later consumers see only events
      // emitted after their own registration, matching the per-
      // consumer semantics every other code path expects.
      if (preStartBuffer !== undefined) {
        if (preStartBufferOverflows > 0) {
          logger.warn`pre-start event buffer overflowed by ${preStartBufferOverflows} event(s) before the first stream() consumer attached; oldest events were dropped`;
        }
        for (const event of preStartBuffer) consumer.push(event);
        preStartBuffer = undefined;
      }
      streamConsumers.add(consumer);
      return consumer.iterator();
    }

    function deliver(message: InboundMessage): void {
      ensureOpen();
      reactor.deliver(message);
    }

    function setSource(source: InferenceSource): void {
      ensureOpen();
      sourceRegistry.setSource(source);
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

      // Surface any pre-start buffer state the caller never observed.
      // The buffer drains into the first `stream()` consumer at
      // attachment time and logs its overflow count then. If no
      // consumer ever attached (e.g. a `send()`-only caller that
      // never subscribed to the event stream), the buffer and its
      // overflow counter would silently disappear here without an
      // operator signal. Log the overflow once at close time so a
      // startup pathology that dropped reactor.start-window events
      // is at least observable in the logs.
      if (preStartBuffer !== undefined && preStartBufferOverflows > 0) {
        logger.warn`pre-start event buffer overflowed by ${preStartBufferOverflows} event(s) and no stream() consumer ever attached to drain it; oldest events were dropped`;
      }
      preStartBuffer = undefined;

      // Wait for the reactor's shutdown sequence (audit flush, in-flight
      // commits) before releasing the lock so a subsequent createAgent on
      // the same workdir does not race with background writers against
      // the same .git directory. The timeout is a backstop: if the
      // reactor's shutdown is genuinely stuck (e.g. a parked test fetch
      // that never resolves) we release the lock anyway rather than
      // deadlock the caller. `closeTimeoutMs: 0` disables the wait.
      const timeoutMs = env.closeTimeoutMs ?? DEFAULT_CLOSE_TIMEOUT_MS;
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
      lock.release();
    }

    const agent: Agent = {
      send,
      stream,
      deliver,
      close,
      setSource,
      history,
      checkpoints,
      readAt,
      blobReader,
    };
    succeeded = true;
    return agent;
  } finally {
    if (!succeeded) {
      // Dispose every successfully constructed bundle. Mirror the
      // intra-`resolveTools` rollback shape: swallow async rejections
      // via a `.catch` (a bare `void promise.dispose()` would leave
      // the rejection in flight and surface as an unhandled rejection
      // on the event loop), swallow synchronous throws with the
      // surrounding try/catch, and let the throw the caller actually
      // raised propagate immediately rather than awaiting cleanup.
      for (const bundle of bundlesForRollback) {
        if (bundle.dispose === undefined) continue;
        try {
          const result = bundle.dispose();
          if (result instanceof Promise) {
            result.catch(() => {
              // Swallow per the comment above.
            });
          }
        } catch {
          // Synchronous throws from a non-async dispose that throws
          // before returning a promise. Same intent as the async
          // path: never let rollback noise mask the original failure.
        }
      }
      lock.release();
    }
  }
}
