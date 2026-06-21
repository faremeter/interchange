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
} from "@intx/agent";
import { getLogger } from "@intx/log";
import type { InferenceEvent } from "@intx/types/runtime";
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
} from "../child/warm-agent-cache";

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
  // `message.received` is the single intentional exclusion, matching
  // the in-process harness's forwarder (`default-harness.ts`): it is an
  // assembly-internal dequeue signal, and the hub-facing audit chain
  // expresses per-message work through the `message.run.started` /
  // `message.run.ended` bracket pair instead. The filter is an
  // allowlist-of-everything-except, so new `InferenceEvent` members
  // flow through by default.
  const eventForward = subscribeAgentEvents(agent, opts.onEvent);

  try {
    const sendResult = await sendWithAbort(agent, req, { closeOnAbort: true });
    return { output: { reply: sendResult.reply, turn: sendResult.turn } };
  } finally {
    // `close` is idempotent: a second call after the send already
    // resolved still releases the workdir lock and tears down stream
    // consumers. We await so the lock is gone before the adapter
    // returns -- a subsequent step on the same workdir must not race
    // a still-closing agent.
    await agent.close();
    // `agent.close()` terminates every active `stream()` iterator, so
    // the forwarder's for-await loop has ended (or is about to). Await
    // it after close so the subscription is fully drained before the
    // adapter returns and no listener outlives the step. Awaited last
    // because the loop only ends once close has fired.
    await eventForward;
  }
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
    warmCache.store(key, agent, eventSinkRef, eventForward);
  }

  if (opts.onEvent !== undefined) {
    warmCache.setEventSink(key, opts.onEvent);
  }
  try {
    const sendResult = await sendWithAbort(agent, req, { closeOnAbort: false });
    return { output: { reply: sendResult.reply, turn: sendResult.turn } };
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
  cfg: { closeOnAbort: boolean },
): Promise<{ reply: string; turn: unknown }> {
  let abortListener: (() => void) | null = null;
  try {
    return await new Promise<{ reply: string; turn: unknown }>(
      (resolve, reject) => {
        // Re-check the abort signal inside the executor. `buildEnv` and
        // `agentFactory` (or a warm-cache acquire) yield to the
        // microtask queue, and the caller can fire `signal.abort()`
        // between the entry-time check and here. Without this re-check,
        // a mid-construction abort would attach the listener to an
        // already-aborted signal that never fires the event again, and
        // the send would hang to the workflow runtime's step timeout.
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
        let synthesized: string;
        try {
          synthesized = synthesizeInputContent(req.input);
        } catch (cause) {
          reject(cause instanceof Error ? cause : new Error(String(cause)));
          return;
        }
        const sendOpts = cfg.closeOnAbort ? undefined : { signal: req.signal };
        agent.send(synthesized, sendOpts).then(resolve, (cause: unknown) => {
          reject(cause instanceof Error ? cause : new Error(String(cause)));
        });
      },
    );
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
 * teardown surfacing through the iterator) is logged at warn, mirroring
 * the in-process harness's forwarder.
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
