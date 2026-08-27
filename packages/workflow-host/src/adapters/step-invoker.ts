// Production `WorkflowRuntimeEnv.StepInvoker` adapter.
//
// The runtime body sees the runtime-env shape: a callable that takes a
// `StepInvokeRequest` and resolves to a `StepInvokeResult`. This
// adapter translates each call into:
//
//   1. Build a `BaseEnv` for the per-step agent. The caller supplies a
//      `buildEnv` callback that yields every required `BaseEnv` field
//      except `authorize`; the adapter constructs the agent's
//      `authorize` closure on top of the workflow-typed
//      `WorkflowAuthorizeFn`, embedding the per-call `AuthorizeContext`
//      so every authz call originating from the step carries the
//      `{ stepId, attempt, runId }` triple the workflow runtime owns.
//   2. Instantiate the agent via `createAgent(def, env)`. The agent
//      factory is wired through `opts.agentFactory` so tests can inject
//      a stub agent that does not require a real inference source.
//   3. Synthesize an inbound message carrying the step's resolved
//      `input` and deliver it through the agent's in-process send path
//      (`agent.send`). `agent.send` is the in-process API for driving
//      an agent without a transport; the call returns the assistant's
//      reply once the reactor's `connector.reply` lands.
//   4. Capture the reply as the step's `output`. The output shape is
//      `{ reply, turn }` so downstream consumers can either read the
//      plain-text reply or walk the full assistant turn (tool calls,
//      thinking blocks, etc.) without the step output dropping
//      structure.
//   5. Tear down the agent (close + lock release) on every exit path,
//      whether the step completed cleanly, the abort signal fired, or
//      the underlying `agent.send` rejected.
//
// Abort handling: when `signal.aborted` fires mid-step, the adapter
// closes the agent (which drains the send queue with
// `AgentClosedError` and releases the workdir lock) and rejects the
// step with a `DOMException("aborted", "AbortError")`. A pre-aborted
// signal short-circuits without constructing an agent.
//
// Warm-keep mode (design §3b). When `opts.warmCache` is supplied the
// step is the sole step of a long-lived single-step deployment: the
// agent is built once on the first message, cached, and reused on every
// subsequent message rather than torn down per send. The warm path
// diverges from the per-step path at three points:
//   - Construction: a cache hit reuses the warm agent (tools loaded,
//     plugins live, LSP subprocess attached); only a miss builds one.
//   - Abort: the step's abort signal is threaded into `agent.send` so a
//     mid-conversation turn abort cancels just that turn -- the warm
//     agent (and its LSP subprocess) stays alive and usable for the next
//     message. The agent is NOT closed on abort; closing happens only at
//     the run-loop's eviction points (shutdown/undeploy/recycle/drain
//     teardown), which is the abort-one-turn vs teardown distinction.
//   - Teardown: the `finally` does NOT close the agent and does NOT
//     drain the event forwarder -- both span messages and are owned by
//     the warm cache, torn down at eviction.
// Multi-step steps pass no cache and keep instantiate-send-teardown.

import {
  createAgent,
  type Agent,
  type AgentDefinition,
  type AuthorizeFn,
  type BaseEnv,
  type SendResult,
} from "@intx/agent";
import { getLogger } from "@intx/log";
import { createInboundMessage, extractAddrSpec, isMessageId } from "@intx/mime";
import type {
  InboundMessage,
  InferenceEvent,
  InferenceSource,
  Mail,
  MailPartReader,
  MessageAttachment,
} from "@intx/types/runtime";
import { isMail } from "@intx/types/runtime";
import type {
  AuthorizeContext,
  StepInvokeRequest,
  StepInvokeResult,
  StepInvoker,
  WorkflowAuthorizeFn,
} from "@intx/workflow";

import type {
  WarmAgentCache,
  WarmEventSinkRef,
  WarmReplyDrive,
} from "../child/warm-agent-cache";
import { runBodyThenCleanup } from "../run-body-then-cleanup";

const logger = getLogger(["workflow-host", "step-invoker"]);

/**
 * Per-step env contributions the caller of the adapter owns.
 *
 * The adapter constructs the agent's `authorize` closure from
 * `WorkflowAuthorizeFn` + the per-call `AuthorizeContext`; everything
 * else on `BaseEnv` is supplied here. `buildEnv` is invoked once per
 * step and may be async so callers that allocate per-step resources
 * (the per-run workdir, an isogit store rooted under it) can do so
 * without a synchronous-only contract.
 */
export type StepEnvBase = Omit<BaseEnv, "authorize">;

export interface WorkflowStepInvokerOpts {
  /**
   * Workflow-level authorize callback. The adapter constructs a
   * per-step `AuthorizeFn` closure that delegates here with the
   * per-call `AuthorizeContext` already embedded, satisfying the
   * agent harness's `AuthorizeFn<unknown>` slot.
   */
  workflowAuthorize: WorkflowAuthorizeFn;
  /**
   * Build the per-step env minus `authorize`. Invoked once per step
   * invocation; the returned env's `storage`, `workdir`, and other
   * agent-runtime fields belong to that one step and are torn down
   * with the agent.
   *
   * The callback receives the `StepInvokeRequest` so it can derive
   * per-step paths (workdir under the run id, per-attempt storage
   * roots) from the workflow runtime's vocabulary.
   */
  buildEnv: (req: StepInvokeRequest) => Promise<StepEnvBase>;
  /**
   * Optional agent factory override. Defaults to `@intx/agent`'s
   * `createAgent`. Tests inject a stub that returns a deterministic
   * `Agent` without exercising the full reactor assembly.
   */
  agentFactory?: <EnvReq extends BaseEnv>(
    def: AgentDefinition<EnvReq>,
    env: EnvReq,
  ) => Promise<Agent>;
  /**
   * Optional observability sink for the per-step agent's event stream.
   * When supplied, the adapter subscribes the agent's `stream()` before
   * `agent.send` so the inbound `inference.start` and the per-turn /
   * tool-call events are captured, and forwards every `InferenceEvent`
   * here. The subscription is torn down with the agent on every exit
   * path, so no listener outlives the step.
   *
   * `onEvent` is a generic `(event: InferenceEvent) => void` sink: the
   * adapter neither knows nor cares where the events go (a host wires
   * it to its event-channel sender, the hub timeline, a test recorder).
   * Forwarding is best-effort observability -- a throwing sink is
   * logged and swallowed so a downstream consumer's failure cannot
   * abort the step -- but the subscription's own teardown failures
   * still surface.
   *
   * Omitting `onEvent` preserves the prior behaviour: the agent's
   * `stream()` is never consumed and no events are forwarded.
   */
  onEvent?: (event: InferenceEvent) => void;
  /**
   * Warm-agent cache (design §3b). When supplied, the adapter runs in
   * warm-keep mode: the step's agent is built once on the first
   * invocation, cached under the step's identity, and reused on every
   * subsequent invocation instead of being torn down per send. The
   * agent's `close()` (the wrapped teardown that kills the LSP
   * subprocess) runs only when the run-loop that owns the cache evicts
   * it -- not in this adapter's `finally`.
   *
   * Supplying a cache is the explicit warm-keep signal: the run-loop
   * builds and threads a cache only for the single-step long-lived
   * deployment the deploy projection marked a warm candidate. Multi-step
   * steps omit it and keep instantiate-send-teardown, so a multi-step
   * agent is never warm-kept.
   */
  warmCache?: WarmAgentCache;
  /**
   * Live per-step inference-source table the run-loop mutates in place on a
   * rotation. Supplied only on the warm path: after building and storing the
   * warm agent, the adapter re-applies the current table so a rotation that
   * landed during the (async) first build -- which the empty-cache
   * `applySources` no-op could not reach, and which the in-flight build had
   * already captured the prior sources for -- is not lost for the warm
   * agent's life.
   */
  sourcesRef?: { current: Record<string, InferenceSource[]> };
  /**
   * Run-boundary hook for the warm path (design §3c durability). When
   * supplied, the adapter awaits it in the warm path's `finally` -- once
   * per message, after the agent's send settles (whether it completed,
   * aborted, or rejected). The sidecar wires this to flush the warm
   * agent's conversation snapshot to the durable workflow-run substrate,
   * so the conversation survives a child respawn between this message
   * and the next. Awaited (not fire-and-forget) so a respawn landing
   * immediately after the reply cannot lose this message's turns; a
   * flush failure surfaces by rejecting the step rather than silently
   * dropping the durability write.
   *
   * The `key` is the step identity (`authzContext.stepId`), the same key
   * the warm cache uses, so the hook resolves the right per-agent
   * durable store. Omitted on the cold path: a torn-down per-step agent
   * has no cross-run conversation to mirror.
   */
  onRunBoundary?: (key: string) => Promise<void>;
  /**
   * Seed hook for the warm path (design §3c threading). When supplied, the
   * adapter calls it before `agent.send` on every message whose delivered
   * input is a mail-derived `InboundMessage`, passing the step identity
   * (`authzContext.stepId`, the same key `onRunBoundary` and the warm cache
   * use) and that message. The sidecar wires this to the warm agent's
   * durable conversation store, which routes the message onto the connector
   * thread (seeding threadRoot / lastMessageId / replyTo) so the reply path
   * can compose a threaded reply. Awaited before the send so the thread
   * state is committed and durably flushed before the reply is produced; a
   * seed failure surfaces by rejecting the step.
   *
   * Only mail-derived inbound messages seed: an approval-resume inbound
   * carries a synthetic sender and a correlation id, and a synthesized
   * string input is not a message, so neither advances the connector
   * thread. Omitted on the cold path, which has no durable connector state
   * to seed.
   */
  seedInbound?: (key: string, message: InboundMessage) => Promise<void>;
  /**
   * Connector reply-drain hook for the warm path (design §3c). When supplied,
   * the adapter invokes it ONCE -- at the warm agent's first-message build --
   * with the step identity (`authzContext.stepId`, the same key the warm
   * cache, seed, and run-boundary hooks use) and the agent's lifetime event
   * stream. The sidecar wires this to the shared connector reply drain: on
   * every `connector.reply` the agent emits, the drain composes a threaded
   * reply from the durable store's connector thread and sends it through the
   * supervisor-backed outbound bridge, then advances the thread from the send
   * receipt.
   *
   * The returned drive handle exposes the drain's lifetime `done` promise --
   * which settles when the agent's stream ends at eviction, folded into the
   * warm entry's event-forward promise so the cache drains the reply loop
   * alongside the observability forwarder -- plus the per-turn settle barrier
   * (`replySeq` / `waitForReplyAfter`) the warm step gates each reply turn on,
   * so the run parks only after the reply is durably sent.
   *
   * Omitted on the cold path (a torn-down per-step agent has no cross-message
   * connector thread) and whenever the deployment is not warm-kept.
   */
  driveReplies?: (
    key: string,
    stream: ReturnType<Agent["stream"]>,
  ) => WarmReplyDrive;
  /**
   * Reader for the run's inbound-mail parts. When the step input is a decoded
   * `Mail`, the adapter resolves each part's `ref` to its committed bytes
   * through this reader and delivers a real `InboundMessage` (text and/or
   * attachments) to `agent.send`. Supplied by the run child for the top-level
   * run's steps; absent for body steps, where a part whose bytes must be read
   * is refused loudly rather than silently flattened to text.
   */
  mailPartReader?: MailPartReader;
}

/**
 * Construct the production `WorkflowRuntimeEnv.StepInvoker` adapter.
 * The returned callable satisfies the runtime-env interface; the
 * workflow-typed authorize, the per-step env builder, and the agent
 * factory live in closure.
 */
export function createWorkflowStepInvoker(
  opts: WorkflowStepInvokerOpts,
): StepInvoker {
  const agentFactory = opts.agentFactory ?? createAgent;
  return async (req) => invokeStep(opts, agentFactory, req);
}

async function invokeStep(
  opts: WorkflowStepInvokerOpts,
  agentFactory: NonNullable<WorkflowStepInvokerOpts["agentFactory"]>,
  req: StepInvokeRequest,
): Promise<StepInvokeResult> {
  if (req.signal.aborted) {
    // Short-circuit the pre-aborted case before the env builder runs.
    // Building the env may allocate (workdir mkdir, isogit store
    // construction); skipping that work when the caller already
    // cancelled keeps the adapter from churning resources whose
    // disposer we are about to invoke anyway.
    throw abortError(req.signal);
  }

  if (opts.warmCache !== undefined) {
    return invokeWarmStep(opts, opts.warmCache, agentFactory, req);
  }
  return invokeColdStep(opts, agentFactory, req);
}

/**
 * Instantiate-send-teardown path (multi-step steps, and any deployment
 * without a warm cache). The agent is built, sent one message, and torn
 * down on every exit path. This is the original, unchanged behaviour.
 */
async function invokeColdStep(
  opts: WorkflowStepInvokerOpts,
  agentFactory: NonNullable<WorkflowStepInvokerOpts["agentFactory"]>,
  req: StepInvokeRequest,
): Promise<StepInvokeResult> {
  const agent = await buildStepAgent(opts, agentFactory, req);

  // Subscribe the agent's event stream BEFORE `agent.send` so the
  // inbound `inference.start` and the per-turn / tool-call events are
  // captured -- a subscription attached after send would miss the
  // events emitted while the reactor processes the synthesized inbound
  // message. The forwarder runs only when the caller supplied an
  // `onEvent` sink; absent a sink the agent's `stream()` is never
  // consumed (stub agents whose `stream()` throws stay untouched).
  //
  // `message.received` is the single intentional exclusion: it is an
  // assembly-internal dequeue signal, and the hub-facing audit chain
  // expresses per-message work through the `message.run.started` /
  // `message.run.ended` bracket pair instead. The filter is an
  // allowlist-of-everything-except, so new `InferenceEvent` members
  // flow through by default.
  const eventForward = subscribeAgentEvents(agent, opts.onEvent);

  // `agent.close()` is the wrapAgentClose-wrapped close: it runs the
  // agent's own close (idempotent -- releases the workdir lock and tears
  // down stream consumers) and then the plugin/tool-bundle disposers,
  // which now reject the close if a disposer (e.g. the LSP subprocess
  // kill) fails. Route the close through runBodyThenCleanup so a disposer
  // failure surfaces on a clean step but never masks a step error already
  // unwinding from `sendWithAbort`. `eventForward` is drained on every
  // path -- `agent.close()` ends the forwarder's for-await loop, and it
  // never rejects (subscribeAgentEvents swallows), so awaiting it in the
  // cleanup cannot mask either error.
  return runBodyThenCleanup(
    async () =>
      stepResultFromSend(
        await sendWithAbort(agent, req, {
          closeOnAbort: true,
          mailPartReader: opts.mailPartReader,
          // Cold-path per-step agents have no durable connector state; the
          // warm path is the only connector-seeding path.
          seedInbound: undefined,
        }),
      ),
    async () => {
      try {
        await agent.close();
      } finally {
        await eventForward;
      }
    },
    (cause) =>
      logger.error`step invoker: agent.close failed while unwinding a step error; surfacing the step error, close failure: ${cause instanceof Error ? cause.message : String(cause)}`,
  );
}

/**
 * Warm-keep path (design §3b). The agent is built once on the first
 * invocation and cached under the step's identity; every later
 * invocation reuses it. The cache owns the agent's lifetime: this
 * adapter neither closes the agent nor drains its event forwarder on
 * exit -- both span messages and are torn down by the run-loop at an
 * eviction point.
 *
 * The agent's single lifetime stream is forwarded through a mutable
 * per-entry event sink the cache holds. This invocation points the sink
 * at THIS step's `onEvent` before `agent.send` and clears it after, so
 * each run's events reach its own channel and a stray event between
 * messages is dropped rather than delivered to a torn-down channel.
 *
 * A mid-conversation abort cancels only the in-flight turn (the abort
 * signal is threaded into `agent.send`); the warm agent and its LSP
 * subprocess survive for the next message. The agent is closed only at
 * eviction -- the abort-one-turn vs teardown distinction.
 */
async function invokeWarmStep(
  opts: WorkflowStepInvokerOpts,
  warmCache: WarmAgentCache,
  agentFactory: NonNullable<WorkflowStepInvokerOpts["agentFactory"]>,
  req: StepInvokeRequest,
): Promise<StepInvokeResult> {
  const key = req.authzContext.stepId;
  if (key === undefined) {
    // The warm cache is keyed by the step's identity; the workflow
    // runtime threads `stepId` through every step's `AuthorizeContext`,
    // so an absent id is a runtime-wiring bug. Fail loudly rather than
    // warm-keep agents under an ambiguous key that would collide
    // distinct steps onto one cached agent.
    throw new Error(
      "workflow step invoker: warm-keep requires authzContext.stepId; the runtime must thread the step id through every invocation",
    );
  }
  let agent = warmCache.acquire(key);
  if (agent === null) {
    // Lazy first-message build. The agent's stream is consumed once,
    // for its whole life, through the entry's mutable sink ref; the
    // forwarder loop ends only when the agent closes at eviction.
    agent = await buildStepAgent(opts, agentFactory, req);
    const eventSinkRef: WarmEventSinkRef = { current: null };
    const eventForward = subscribeAgentEvents(agent, (event) => {
      const sink = eventSinkRef.current;
      if (sink !== null) sink(event);
    });
    // Establish the connector reply drain over the agent's lifetime stream
    // (design §3c). A second independent consumer of the agent's stream
    // alongside the observability forwarder: on each `connector.reply` it
    // composes a threaded reply from the durable connector thread and sends
    // it through the outbound bridge. Fold its lifetime `done` promise into
    // the stored forwarder promise so the warm cache drains BOTH when it
    // closes the agent at eviction -- the cache awaits one promise per entry,
    // so a caller that wants the reply loop torn down with the agent combines
    // the two here. The drive handle's per-turn barrier is stored on the entry
    // so every message (not just this first build) can gate its reply turn on
    // a durable send. Present only on the warm mail path; a warm deployment
    // with no durable connector state omits it.
    const replyDrive =
      opts.driveReplies !== undefined
        ? opts.driveReplies(key, agent.stream())
        : null;
    const lifetimeForward =
      replyDrive !== null
        ? Promise.all([eventForward, replyDrive.done]).then(() => undefined)
        : eventForward;
    warmCache.store(key, agent, eventSinkRef, lifetimeForward, replyDrive);
    // Re-apply the live source table to the just-built agent. A rotation
    // that arrived during the (async) build hit the still-empty cache as a
    // no-op `applySources` while the build had already captured the prior
    // sources; now that the entry exists, this applies any such rotation so
    // it is not lost for the warm agent's life. No-op when the table is
    // unchanged. The wire boundary guarantees element 0 is the default.
    const live = opts.sourcesRef?.current[key];
    const head = live?.[0];
    if (live !== undefined && head !== undefined) {
      warmCache.applySources(live, head.id);
    }
  }

  if (opts.onEvent !== undefined) {
    warmCache.setEventSink(key, opts.onEvent);
  }
  // Bind the seed hook to this step's identity so it resolves the same
  // per-agent durable store the warm cache and run-boundary flush use.
  const seedInbound = opts.seedInbound;
  // Snapshot the reply barrier BEFORE the send. The agent resolves
  // `agent.send` in the same synchronous step it pushes `connector.reply`
  // onto the drain's stream, so the reply is not yet enqueued when the send
  // resolves -- a snapshot taken after the send would miss this turn's reply.
  const replyDrive = warmCache.getReplyDrive(key);
  const replySeqBeforeSend = replyDrive !== null ? replyDrive.replySeq() : 0;
  try {
    const sendResult = await sendWithAbort(agent, req, {
      closeOnAbort: false,
      mailPartReader: opts.mailPartReader,
      seedInbound:
        seedInbound !== undefined
          ? (message) => seedInbound(key, message)
          : undefined,
    });
    const stepResult = stepResultFromSend(sendResult);
    // Gate the step's return on THIS turn's reply being durably sent, so the
    // run parks -- and the supervisor consumes the inbound mail -- only after
    // the auto-reply reaches the transport. Only a reply turn produces a
    // `connector.reply`; a suspended/gate turn produces none, so it must NOT
    // await the barrier (that reply never arrives and the wait would hang).
    // A failed send resolves the barrier with `ok: false`: fail the turn so
    // the inbound mail is not consumed as replied and the run's claim-check
    // replays it (at-least-once via reprocessing) rather than dropping the
    // reply.
    if (replyDrive !== null && sendResult.type === "reply") {
      const settlement = await replyDrive.waitForReplyAfter(replySeqBeforeSend);
      if (!settlement.ok) {
        throw new Error(
          "workflow step invoker: the warm agent's auto-reply send failed; " +
            "failing the turn so the inbound mail replays rather than being " +
            "consumed with the reply dropped",
          { cause: settlement.cause },
        );
      }
    }
    return stepResult;
  } finally {
    // Do NOT close the agent or drain its forwarder: both span
    // messages and are owned by the warm cache, torn down at eviction.
    // Clear the per-message sink so an event emitted between this send
    // and the next is dropped rather than delivered to this run's
    // torn-down per-run channel.
    warmCache.clearEventSink(key);
    // Run-boundary durability flush (design §3c). Mirror the warm
    // agent's conversation snapshot to the durable substrate once per
    // message, after the send settles, so a respawn before the next
    // message resumes from this message's turns. Awaited so the
    // durability write completes (or surfaces its failure) before the
    // step result is observed.
    if (opts.onRunBoundary !== undefined) {
      await opts.onRunBoundary(key);
    }
  }
}

/**
 * Build the per-step agent: assemble the `BaseEnv`, wrap the
 * workflow-typed authorize into the agent harness's `AuthorizeFn`, and
 * instantiate the agent through the factory. Shared by the cold path and
 * the warm path's first-message build.
 */
async function buildStepAgent(
  opts: WorkflowStepInvokerOpts,
  agentFactory: NonNullable<WorkflowStepInvokerOpts["agentFactory"]>,
  req: StepInvokeRequest,
): Promise<Agent> {
  const envBase = await opts.buildEnv(req);
  const authorize = wrapAuthorize(opts.workflowAuthorize, req.authzContext);
  const env: BaseEnv = { ...envBase, authorize };
  return agentFactory(req.agent, env);
}

/**
 * Drive one `agent.send`, racing it against the step's abort signal.
 *
 * The message sent depends on `req.resume` and its kind. A first invocation
 * sends the synthesized `req.input` content. An `"approval"` resume sends the
 * full correlated `InboundMessage` built from `req.resume`, whose
 * `headers.interchangeCorrelationId` routes through the reactor's
 * `tryCorrelate` to match the rehydrated gate and resume the parked cycle --
 * no second inference cycle. An `"input"` resume sends the decision as plain
 * synthesized content, exactly like a first invocation: it is the step's next
 * turn, with no gate to correlate. Because every path goes through
 * `agent.send`, the returned `SendResult` carries the reactor's full settle
 * arm set: a cycle that parks on a gate settles as `"suspended"` regardless
 * of how the turn was delivered.
 *
 * `closeOnAbort` selects the abort semantics:
 *   - `true` (cold path): the in-flight send is left to settle via
 *     `agent.close()` in the caller's `finally`, which aborts the
 *     reactor and drains the send queue with `AgentClosedError`. We do
 *     not thread the signal into `agent.send`; the abort attribution is
 *     the `DOMException` rejected here, and close tears the agent down.
 *   - `false` (warm path): the abort signal is threaded into
 *     `agent.send`, so a mid-turn abort cancels only this turn. The warm
 *     agent stays alive for the next message; no `agent.close()` runs.
 *
 * In both modes a pre-send abort (the signal already aborted when the
 * executor runs) and a mid-send abort reject with the abort error so the
 * step's abort attribution wins regardless of which side settles first.
 */
async function sendWithAbort(
  agent: Agent,
  req: StepInvokeRequest,
  cfg: {
    closeOnAbort: boolean;
    mailPartReader: MailPartReader | undefined;
    seedInbound: ((message: InboundMessage) => Promise<void>) | undefined;
  },
): Promise<SendResult> {
  // Re-check the abort signal before building the message. `buildEnv` and
  // `agentFactory` (or a warm-cache acquire) yield to the microtask queue, and
  // the caller can fire `signal.abort()` in between. Building the message can
  // itself yield (attachment resolution), so guard here too.
  if (req.signal.aborted) throw abortError(req.signal);
  // Resolve the step input into the value `agent.send` receives. A build
  // failure (a bad resume shape, an unresolvable attachment) rejects the step.
  const { message, mailInbound } = await buildSendMessage(
    req,
    cfg.mailPartReader,
  );
  // Seed the warm agent's connector thread from a mail-derived inbound before
  // the send, so the reply path has thread state (design §3c). Only the
  // mail branch surfaces `mailInbound`; an approval-resume inbound and a
  // synthesized string do not advance the thread. The seed is awaited so its
  // durable flush completes (or surfaces) before the reply is produced; a
  // seed failure rejects the step rather than composing an unthreaded reply.
  if (mailInbound !== null && cfg.seedInbound !== undefined) {
    await cfg.seedInbound(mailInbound);
  }
  let abortListener: (() => void) | null = null;
  try {
    return await new Promise<SendResult>((resolve, reject) => {
      // Re-check after the (async) message build: a mid-build abort must not
      // attach the listener to an already-aborted signal that never fires the
      // event again, or the send would hang to the runtime's step timeout.
      if (req.signal.aborted) {
        reject(abortError(req.signal));
        return;
      }
      const onAbort = (): void => {
        // The abort signal racing the send. On the cold path the
        // caller's `finally` close aborts the reactor and the
        // in-flight `agent.send` rejects shortly after; on the warm
        // path the signal threaded into `agent.send` rejects the
        // send. Either way we reject here so the abort attribution
        // wins regardless of which side settles first.
        reject(abortError(req.signal));
      };
      abortListener = onAbort;
      req.signal.addEventListener("abort", onAbort, { once: true });
      const sendOpts = cfg.closeOnAbort ? undefined : { signal: req.signal };
      agent.send(message, sendOpts).then(resolve, (cause: unknown) => {
        reject(cause instanceof Error ? cause : new Error(String(cause)));
      });
    });
  } finally {
    if (abortListener !== null) {
      req.signal.removeEventListener("abort", abortListener);
    }
  }
}

/**
 * Subscribe the per-step agent's event stream and forward every
 * `InferenceEvent` to `onEvent`. Returns a promise that settles when
 * the forwarder's loop ends -- which happens when `agent.close()`
 * terminates the stream iterator. When `onEvent` is absent the agent's
 * `stream()` is never consumed and the returned promise resolves
 * immediately, so a caller that does not want observability never
 * touches the stream (stub agents whose `stream()` throws stay
 * untouched).
 *
 * Forwarding is best-effort observability: a sink that throws is
 * logged and swallowed so a downstream consumer's failure cannot abort
 * the step. A failure of the stream iterator itself (the agent's
 * teardown surfacing through the iterator) is logged at warn.
 */
function subscribeAgentEvents(
  agent: Agent,
  onEvent: ((event: InferenceEvent) => void) | undefined,
): Promise<void> {
  if (onEvent === undefined) {
    return Promise.resolve();
  }
  const events = agent.stream();
  return (async () => {
    try {
      for await (const event of events) {
        if (event.type === "message.received") continue;
        try {
          onEvent(event);
        } catch (cause) {
          logger.error`step-invoker event sink threw forwarding ${event.type}: ${cause instanceof Error ? cause.message : String(cause)}`;
        }
      }
    } catch (cause) {
      logger.warn`step-invoker event forwarder terminated: ${cause instanceof Error ? cause.message : String(cause)}`;
    }
  })();
}

/**
 * Construct the agent harness's `AuthorizeFn` from the workflow-typed
 * callback. The returned closure ignores its third positional argument
 * (the agent layer's generic context slot, typed `unknown`) and
 * delegates to the workflow-typed authorize with the per-step
 * `AuthorizeContext` captured at closure-build time. This is the same
 * shape the in-memory `runlocal` step invoker uses; surfacing the
 * conversion here keeps the agent layer workflow-unaware.
 */
function wrapAuthorize(
  workflowAuthorize: WorkflowAuthorizeFn,
  authzContext: AuthorizeContext,
): AuthorizeFn {
  return async (resource, action) =>
    workflowAuthorize(resource, action, authzContext);
}

/**
 * Translate a settled `SendResult` into the step's `StepInvokeResult`.
 *
 * A `"reply"` outcome carries the assistant's reply and full-fidelity
 * turn, which become the step output so downstream consumers can read
 * either shape.
 *
 * A `"suspended"` outcome hands the workflow runtime the parked reactor's
 * `correlationId`: the reactor parked on a gate awaiting an external
 * decision. The runtime parks the step on the reserved signal channel for
 * that correlation and, when the decision is delivered, re-invokes with
 * `resume` so `sendWithAbort` sends the correlated inbound and drives the
 * resumed reactor to a real reply -- or, when the resumed cycle re-parks
 * on a second gate, to another `"suspended"` outcome that flows back
 * through here unchanged.
 */
function stepResultFromSend(result: SendResult): StepInvokeResult {
  if (result.type === "suspended") {
    // The reactor parks only on a tool/authz gate -- an APPROVAL. It never
    // parks awaiting the next mail (that "input" park is the workflow-host's
    // decision to re-arm a conversational step, not a reactor outcome), so a
    // suspended `SendResult` is always an approval and MUST carry a snapshot.
    // A snapshot-less suspend (e.g. a director `caps.suspend` wired with no
    // tool definitions) is not a supported approval; classify the failure here
    // at the producer rather than emitting an ambiguous suspend that the
    // runtime would have to reject three hops downstream.
    if (result.approvalSnapshot === undefined) {
      throw new Error(
        `reactor suspended on correlation ${result.correlationId} with no ` +
          `approval snapshot; a snapshot-less suspend is not a supported ` +
          `approval park`,
      );
    }
    return {
      suspend: {
        correlationId: result.correlationId,
        kind: "approval",
        approvalSnapshot: result.approvalSnapshot,
      },
    };
  }
  return { output: { reply: result.reply, turn: result.turn } };
}

/**
 * Resolve the step input into the value `agent.send` receives. The delivery
 * depends on the resume kind and the input shape:
 *
 * - `"approval"` resume: the reactor is parked mid-turn on a tool/authz gate.
 *   Build the full `InboundMessage` stamped with `resume.correlationId` so the
 *   header reaches the reactor's `tryCorrelate` and matches the rehydrated
 *   gate. The object form is load-bearing -- a plain string would drop the
 *   correlation id and the resumed cycle would never match. An approval
 *   decision is never a mail-derived `Mail`, so it stays on this branch.
 * - A mail-derived `Mail` (first invocation or `"input"` resume): project its
 *   parts into a real `InboundMessage` (text and/or attachments), resolving
 *   each non-text part's bytes through the reader.
 * - Anything else (a first invocation or `"input"` resume carrying an
 *   arbitrary step value): synthesize plain text; `agent.send` stamps its own
 *   synthetic addressing.
 */
async function buildSendMessage(
  req: StepInvokeRequest,
  mailPartReader: MailPartReader | undefined,
): Promise<{
  message: string | InboundMessage;
  mailInbound: InboundMessage | null;
}> {
  if (req.resume !== undefined && req.resume.kind !== "input") {
    return {
      message: createInboundMessage({
        from: "signal@local",
        to: "agent@local",
        content: synthesizeInputContent(req.resume.decision),
        interchangeType: "conversation.message",
        correlationId: req.resume.correlationId,
      }),
      mailInbound: null,
    };
  }
  const rawInput = req.resume === undefined ? req.input : req.resume.decision;
  // A step whose input is a decoded `Mail` is projected into the agent's
  // inbound message; the strict `isMail` guard keeps an arbitrary step value
  // from matching. This is the one branch that carries real threading headers,
  // so `mailInbound` surfaces the message for the warm path's connector seed.
  if (isMail(rawInput)) {
    const message = await buildInboundMessageFromMail(rawInput, mailPartReader);
    return { message, mailInbound: message };
  }
  return { message: synthesizeInputContent(rawInput), mailInbound: null };
}

/** Extract a bare addr-spec from a header value, or fall back to a synthetic
 * local address when the value is absent or unparseable. */
function safeAddr(raw: string | undefined, fallback: string): string {
  if (raw === undefined || raw === "") return fallback;
  try {
    return extractAddrSpec(raw);
  } catch {
    return fallback;
  }
}

/**
 * Project a decoded `Mail` into the agent's `InboundMessage`. Text parts become
 * the conversation body; every other part becomes an attachment the reactor
 * turns into a media / document content block. Part bytes are resolved through
 * the reader; a text part small enough to have inlined `text` skips the read.
 * The real sender / recipient headers are carried through so the agent frames
 * the turn with the actual `From:` rather than a synthetic address. The decoded
 * `Message-ID` and, when present, `In-Reply-To` / `References` ride through too,
 * so the delivered message keeps its place in the conversation thread rather
 * than being stamped with a fresh synthesized id. Content is omitted when empty
 * -- `createInboundMessage` rejects an empty string, and an attachments-only
 * message is valid.
 *
 * The reader is required only when a part's bytes must actually be read (a
 * non-text part, or a text part too large to have inlined its `text`). A
 * text-only mail whose parts all inlined -- e.g. one routed to a body step,
 * which is not wired with a reader -- still delivers; a part that needs bytes
 * with no reader is refused loudly rather than silently dropped.
 */
async function buildInboundMessageFromMail(
  mail: Mail,
  mailPartReader: MailPartReader | undefined,
): Promise<InboundMessage> {
  const noReader = (): Error =>
    new Error(
      "workflow step invoker: a mail part's bytes must be read but the step has no mail-part reader wired; inbound parts are not supported for this step",
    );
  const textPieces: string[] = [];
  const attachments: MessageAttachment[] = [];
  for (const part of mail.parts) {
    // A part is conversation body only when it is inline plain text. An
    // attachment-disposition part (even a text/* one, e.g. an attached .txt),
    // and any non-plain-text part (a text/html alternative, an image, an
    // application/* payload), is delivered as an attachment so its bytes and
    // filename survive rather than being folded into the turn.
    const isBody =
      part.disposition !== "attachment" && part.contentType === "text/plain";
    if (isBody) {
      if (part.text !== undefined) {
        textPieces.push(part.text);
        continue;
      }
      if (mailPartReader === undefined) throw noReader();
      textPieces.push(
        new TextDecoder("utf-8", { fatal: false }).decode(
          await mailPartReader.read(part.ref),
        ),
      );
      continue;
    }
    if (mailPartReader === undefined) throw noReader();
    attachments.push({
      name: part.filename ?? part.contentType,
      contentType: part.contentType,
      data: await mailPartReader.read(part.ref),
    });
  }
  const content = textPieces.join("\n").trim();
  // Forward the mail's Message-ID / In-Reply-To / References for threading, but
  // only the well-formed RFC 2822 identifiers. Inbound mail can carry a
  // headerless-derived (sha256) or malformed Message-Id -- a valid claim-check
  // key but not a valid identifier -- and passing it to createInboundMessage
  // would throw and fail the step. When the messageId is omitted here,
  // createInboundMessage synthesizes a valid one; such mail cannot thread.
  const validReferences = mail.headers.references?.filter(isMessageId) ?? [];
  return createInboundMessage({
    from: safeAddr(mail.headers.from, "trigger@local"),
    to: safeAddr(mail.headers.to[0], "agent@local"),
    ...(mail.headers.subject !== undefined
      ? { subject: mail.headers.subject }
      : {}),
    ...(isMessageId(mail.headers.messageId)
      ? { messageId: mail.headers.messageId }
      : {}),
    ...(mail.headers.inReplyTo !== undefined &&
    isMessageId(mail.headers.inReplyTo)
      ? { inReplyTo: mail.headers.inReplyTo }
      : {}),
    ...(validReferences.length > 0 ? { references: validReferences } : {}),
    ...(content.length > 0 ? { content } : {}),
    ...(attachments.length > 0 ? { attachments } : {}),
    interchangeType: "conversation.message",
  });
}

/**
 * Encode the step's resolved `input` as the synthetic inbound message
 * content. The workflow runtime resolves `input` from the step's input
 * selector and hands it to the invoker as `unknown`; `agent.send`
 * expects a string or an `InboundMessage`. JSON-stringify covers the
 * common case (objects, arrays, primitives) and round-trips through
 * the agent's synthetic mail boundary verbatim.
 *
 * Inputs that JSON.stringify cannot serialize (functions, symbols, raw
 * `undefined`) are surfaced as a thrown error rather than a silent
 * `"undefined"` string -- step outputs that depend on the input shape
 * deserve a loud failure if the workflow-defined selector produced a
 * non-serializable value.
 */
function synthesizeInputContent(input: unknown): string {
  if (typeof input === "string") return input;
  const encoded = JSON.stringify(input);
  if (encoded === undefined) {
    throw new Error(
      `workflow step invoker: input of typeof ${typeof input} is not JSON-serializable; the step's input selector must resolve to a serializable value`,
    );
  }
  return encoded;
}

/**
 * Construct the rejection used when `signal.aborted` short-circuits or
 * fires mid-step. Mirrors the DOMException-shaped abort errors the
 * inference harness emits so consumers can `instanceof DOMException` /
 * `name === "AbortError"` against a stable shape across the runtime.
 */
function abortError(signal: AbortSignal): Error {
  const reason = signal.reason;
  if (reason instanceof Error) return reason;
  return new DOMException("aborted", "AbortError");
}
