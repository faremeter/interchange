// In-process agent runtime.
//
// `createAgent(def, env)` is the single entry point: it locks the
// context directory, builds the tool runner and director, wires them
// into one reactor assembly, and returns the Agent handle. The reactor
// is wrapped exactly once. Bundle lifetimes are the caller's
// responsibility -- the env is the agent's dependency contract.

import {
  createReactorAssembly,
  type Dependencies,
  type ReactorEmittedEvent,
} from "@intx/inference";
import { createDefaultDependencies } from "@intx/inference/providers";

import { createUnconfiguredCredentialResolver } from "./credential-resolver";
import { getLogger } from "@intx/log";
import { createInboundMessage } from "@intx/mime";
import type { ErrorRecord } from "@intx/types/audit";
import type {
  ApprovalSnapshot,
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

// Sender/receiver for the synthetic message built when `agent.send`
// receives a plain string. The message never leaves the process, so the
// addresses are shape-fillers; `from` is overridable for audit
// purposes, `to` is fixed because no call path routes or audits on it.
const DEFAULT_SEND_FROM = "user@local";
const DEFAULT_SEND_TO = "agent@local";
const DEFAULT_SEND_QUEUE_MAX = 16;
const DEFAULT_STREAM_BUFFER_MAX = 1024;
const DEFAULT_CLOSE_TIMEOUT_MS = 5000;

export type SendOptions = {
  /**
   * Abort signal for this send. Firing before processing drops the call
   * from the queue; firing mid-cycle rejects the caller immediately but
   * the reactor cycle keeps running in the background.
   */
  signal?: AbortSignal;
  /** Override the default "from" header on the synthetic inbound message. */
  from?: string;
};

export type SendResult =
  | {
      type: "reply";
      /** Reply text emitted by the director's `reply` action. */
      reply: string;
      /**
       * Full assistant turn that produced the reply, from the
       * `inference.done` event preceding `connector.reply`.
       */
      turn: ConversationTurn;
    }
  | {
      /**
       * The reactor parked on a gate before producing a reply; the cycle
       * resumes when the correlated external decision is delivered.
       * `correlationId` identifies the pending operation to resume.
       */
      type: "suspended";
      correlationId: string;
      /**
       * Approver-facing snapshot of the parked tool call, when the reactor
       * carried one on the gate-blocked event.
       */
      approvalSnapshot?: ApprovalSnapshot;
    };

/**
 * A `reactor.gate.blocked` event settled the active send without a
 * `correlationId`, so the suspension has no handle to resume against.
 */
export class GateSuspendedWithoutCorrelationError extends Error {
  readonly gateId: string;

  constructor(gateId: string) {
    super(
      `reactor suspended on gate ${gateId} without a correlationId; the send has no handle to resume against`,
    );
    this.name = "GateSuspendedWithoutCorrelationError";
    this.gateId = gateId;
  }
}

export type Agent = {
  send(
    content: string | InboundMessage,
    opts?: SendOptions,
  ): Promise<SendResult>;
  stream(): AsyncIterable<ReactorEmittedEvent>;
  deliver(message: InboundMessage): void;
  /**
   * Begin shutdown: abort the reactor, drain the send queue with
   * `AgentClosedError`, terminate stream iterators, await the shutdown
   * sequence (audit flush + in-flight commits) up to
   * `env.closeTimeoutMs`, then release the workdir lock. Events emitted
   * in the shutdown window are not visible to `stream()` iterators.
   */
  close(): Promise<void>;
  /**
   * Replace the active source's fields in place; picked up at the start
   * of the next inference call. The director never names a model, so
   * the active source's model rotates with the credentials.
   */
  setSource(source: InferenceSource): void;
  /**
   * Replace the whole ordered source list and activate `defaultSource`.
   */
  setSources(sources: InferenceSource[], defaultSource: string): void;
  /**
   * Project conversation history from the context store. Remains
   * callable after `close()`: reads do not need the reactor.
   */
  history(): Promise<ConversationTurn[]>;
  /** List recent checkpoints from the context store; callable after close. */
  checkpoints(limit?: number): Promise<ContextCommit[]>;
  /** Read the turns recorded at a specific commit hash; callable after close. */
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
   * The bundles `resolveTools` constructed, exposed so `createAgent`
   * can dispose them on construction failures after `resolveTools`.
   */
  readonly bundles: readonly ToolBundle[];
}

/**
 * Walk each annotated tool factory, build the bundle, and produce a
 * single `ToolRunner` that dispatches calls by tool name to the
 * originating bundle. Throws on duplicate tool names.
 */
function resolveTools<EnvReq extends BaseEnv>(
  def: AgentDefinition<EnvReq>,
  env: EnvReq,
): ResolvedTools {
  const byName = new Map<string, ToolBundle>();
  const definitions: ToolDefinition[] = [];
  // Track constructed bundles so a later factory's failure can dispose
  // them; createAgent owns them until resolveTools returns.
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
    // Dispose every bundle constructed before re-raising, so
    // construction-time resources (IMAP sessions, LSP subprocesses)
    // do not leak when a later factory throws.
    for (const bundle of constructed) {
      if (bundle.dispose === undefined) continue;
      try {
        // Absorb async rejections and sync throws so rollback noise
        // never masks the original construction failure.
        const result = bundle.dispose();
        if (result instanceof Promise) {
          result.catch(() => {
            // Swallow.
          });
        }
      } catch {
        // Swallow per the intent above.
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
  compactorNames: readonly string[],
): ReactorDirector {
  const ref: DirectorRef = def.director ?? env.directors.buildDefaultRef();
  const factory = env.directors.resolve(ref);
  // Re-validate the ref's config against the factory's schema: refs
  // are a public structural type, so nothing forces them through
  // `defineDirector.build`.
  validateDirectorConfig(ref.config, factory.configSchema);
  return factory(ref.config, env, {
    systemPrompt: def.systemPrompt,
    toolDefinitions,
    compactorNames,
  });
}

export async function createAgent<EnvReq extends BaseEnv>(
  def: AgentDefinition<EnvReq>,
  env: EnvReq,
): Promise<Agent> {
  validateEnv(def, env);

  const lock: ContextDirLock = acquireContextDirLock(env.workdir);

  // Construction acquires the lock and tool bundles before the
  // returned Agent's close() is reachable. On any failure below,
  // release the lock and dispose the constructed bundles instead of
  // leaking them; this outer rollback covers post-resolveTools
  // failures.
  let succeeded = false;
  let bundlesForRollback: readonly ToolBundle[] = [];
  try {
    const resolvedTools = resolveTools(def, env);
    bundlesForRollback = resolvedTools.bundles;
    const sourceRegistry = createSourceRegistry({
      sources: env.sources,
      defaultSource: env.defaultSource,
    });
    // Snapshot the registered compactor names so the director gets a
    // stable list; treat env.compactors as immutable post-construction.
    const compactorNames: readonly string[] = Object.freeze(
      Object.keys(env.compactors ?? {}),
    );
    const director = resolveDirector(
      def,
      env,
      resolvedTools.definitions,
      compactorNames,
    );

    const contextStore: ContextStore = env.storage;
    const auditStore = env.audit;
    const authorize = env.authorize;
    const deps: Dependencies = env.deps ?? createDefaultDependencies();

    const sessionId = env.sessionId ?? crypto.randomUUID();
    const streamBufferMax = env.streamBufferMax ?? DEFAULT_STREAM_BUFFER_MAX;
    const streamConsumers = new Set<StreamConsumer>();

    // Buffer events emitted between reactor.start() and the first
    // stream() consumer attaching; without it the synchronous startup
    // window's events would be dropped. Drained into the first
    // consumer and discarded; overflow drops the oldest event with a
    // warning.
    let preStartBuffer: ReactorEmittedEvent[] | undefined = [];
    let preStartBufferOverflows = 0;

    // Keep the most recent assistant turn of the active cycle so the
    // final connector.reply pairs with the full-fidelity turn.
    type ActiveCycle = { lastAssistantTurn: AssistantTurn | undefined };
    let activeCycle: ActiveCycle | null = null;

    // sendQueue is built after the reactor but handleEvent reads it via
    // closure; assigned before reactor.start(), so no event can reach
    // handleEvent before the queue is wired.
    // eslint-disable-next-line prefer-const -- forward declaration; const cannot express this ordering
    let sendQueue: SendQueue<InboundMessage, SendResult>;

    // Resolved from the assembly's onShutdown hook, or from handleEvent
    // on the terminal reactor.done event; close() awaits it before
    // releasing the workdir lock.
    const {
      promise: shutdownComplete,
      resolve: resolveShutdown,
      // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Promise.withResolvers<void>() is the conventional shape for a fire-and-forget settled-signal; matches Promise<void> used elsewhere on this assembly
    } = Promise.withResolvers<void>();

    // inference.error / reactor.error events accumulate here and flush
    // at the assembly's afterCheckpoint and onShutdown hooks.
    //
    // Serialization: concurrent flush callers share one follow-up
    // promise so parallel commits never double-splice the accumulator.
    const accumulatedErrors: ErrorRecord[] = [];
    let errorSeq = 0;
    let flushInProgress: Promise<void> | undefined;
    let pendingFollowUp: Promise<void> | undefined;

    function flushErrors(): Promise<void> {
      if (flushInProgress !== undefined) {
        // Ride an existing follow-up if one is arranged; otherwise
        // arrange one shared by every concurrent caller. Runs on both
        // fulfilment and rejection so a failed flush is retried.
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
      // Splice only after a successful commit so a throwing audit
      // store does not lose the batch; the next flush hook retries.
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
          sendQueue.resolveActive({
            type: "reply",
            reply: event.data.content,
            turn,
          });
        } else if (event.type === "reactor.gate.blocked") {
          // Terminal outcome for the active send: the cycle will not
          // resume until the correlated decision is delivered, so
          // settle with the suspended outcome. A gate parked without a
          // correlationId is unresumable -- surface it.
          const { correlationId, approvalSnapshot } = event.data;
          activeCycle = null;
          if (correlationId === undefined) {
            sendQueue.rejectActive(
              new GateSuspendedWithoutCorrelationError(event.data.gateId),
            );
          } else {
            sendQueue.resolveActive({
              type: "suspended",
              correlationId,
              ...(approvalSnapshot !== undefined ? { approvalSnapshot } : {}),
            });
          }
        } else if (event.type === "reactor.error" && event.data.fatal) {
          // Only fatal reactor errors terminate the active send; the
          // cycle may still produce connector.reply otherwise.
          activeCycle = null;
          sendQueue.rejectActive(
            new Error(`reactor error: ${event.data.error}`),
          );
        } else if (event.type === "reactor.done") {
          activeCycle = null;
          sendQueue.rejectActive(new AgentClosedError());
        }
      }

      // Fallback for paths where the onShutdown hook never fires, so
      // close() does not wait the full timeout. resolveShutdown is
      // idempotent.
      if (event.type === "reactor.done") {
        resolveShutdown();
      }

      // Buffer until the first consumer attaches; overflow drops the
      // oldest event with a warning rather than aborting startup.
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
      failOverToNextSource: () => sourceRegistry.failOverToNextSource(),
      resetToPreferredSource: () => sourceRegistry.resetToPreferredSource(),
      readMaterial:
        env.readCurrentMaterial ?? createUnconfiguredCredentialResolver(),
      toolRunner: resolvedTools.runner,
      contextStore,
      onEvent: handleEvent,
      auditStore,
      authorize,
      toolDefinitions: resolvedTools.definitions,
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
      ...(env.doomLoopThreshold !== undefined
        ? { doomLoopThreshold: env.doomLoopThreshold }
        : {}),
      deps,
      ...(env.compactors !== undefined ? { compactors: env.compactors } : {}),
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
      // Closed-agent errors reject so callers can .catch() them;
      // SendQueueFullError stays a synchronous throw (programmer error).
      if (closed) return Promise.reject(new AgentClosedError());
      const message = buildInboundMessage(content, opts);
      return sendQueue.enqueue(message, opts?.signal);
    }

    function stream(): AsyncIterable<ReactorEmittedEvent> {
      ensureOpen();
      const consumer = createStreamConsumer(streamBufferMax);
      // Drain the pre-start buffer into the first consumer so startup
      // events are not lost; the buffer is discarded after the drain.
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

    function setSources(
      sources: InferenceSource[],
      defaultSource: string,
    ): void {
      ensureOpen();
      sourceRegistry.setSources(sources, defaultSource);
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

      // Log a pre-start overflow the caller never observed via stream().
      if (preStartBuffer !== undefined && preStartBufferOverflows > 0) {
        logger.warn`pre-start event buffer overflowed by ${preStartBufferOverflows} event(s) and no stream() consumer ever attached to drain it; oldest events were dropped`;
      }
      preStartBuffer = undefined;

      // Wait for the shutdown sequence before releasing the lock so a
      // subsequent createAgent on the same workdir does not race with
      // background writers. The timeout is a backstop; 0 disables the
      // wait.
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
      setSources,
      history,
      checkpoints,
      readAt,
      blobReader,
    };
    succeeded = true;
    return agent;
  } finally {
    if (!succeeded) {
      // Mirror the intra-resolveTools rollback: absorb async
      // rejections and sync throws, propagate the caller's error.
      for (const bundle of bundlesForRollback) {
        if (bundle.dispose === undefined) continue;
        try {
          const result = bundle.dispose();
          if (result instanceof Promise) {
            result.catch(() => {
              // Swallow.
            });
          }
        } catch {
          // Swallow per the intent above.
        }
      }
      lock.release();
    }
  }
}
