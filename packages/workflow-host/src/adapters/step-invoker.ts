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

import {
  createAgent,
  type Agent,
  type AgentDefinition,
  type AuthorizeFn,
  type BaseEnv,
} from "@intx/agent";
import type {
  AuthorizeContext,
  StepInvokeRequest,
  StepInvokeResult,
  StepInvoker,
  WorkflowAuthorizeFn,
} from "@intx/workflow";

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

  const envBase = await opts.buildEnv(req);
  const authorize = wrapAuthorize(opts.workflowAuthorize, req.authzContext);
  const env: BaseEnv = { ...envBase, authorize };

  const agent = await agentFactory(req.agent, env);

  let abortListener: (() => void) | null = null;
  try {
    const sendResult = await new Promise<{ reply: string; turn: unknown }>(
      (resolve, reject) => {
        // Re-check the abort signal inside the executor. The two
        // awaits above (`buildEnv`, `agentFactory`) yield to the
        // microtask queue, and the caller can fire `signal.abort()`
        // between the entry-time check and here. Without this re-
        // check, a mid-construction abort would attach the listener
        // to an already-aborted signal that never fires the event
        // again, and the send would hang to the workflow runtime's
        // step timeout.
        if (req.signal.aborted) {
          reject(abortError(req.signal));
          return;
        }
        const onAbort = (): void => {
          // The abort signal racing the send. `agent.close()` aborts
          // the reactor and drains the send queue with
          // `AgentClosedError`; the in-flight `agent.send` rejects
          // shortly after. The send-rejection path below routes
          // through `reject`, but we want the abort attribution to
          // win regardless of which side settles first, so reject
          // here with the abort reason and let the finally block
          // close the agent.
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
        agent.send(synthesized).then(resolve, (cause: unknown) => {
          reject(cause instanceof Error ? cause : new Error(String(cause)));
        });
      },
    );
    return { output: { reply: sendResult.reply, turn: sendResult.turn } };
  } finally {
    if (abortListener !== null) {
      req.signal.removeEventListener("abort", abortListener);
    }
    // `close` is idempotent: a second call after the send already
    // resolved still releases the workdir lock and tears down stream
    // consumers. We await so the lock is gone before the adapter
    // returns -- a subsequent step on the same workdir must not race
    // a still-closing agent.
    await agent.close();
  }
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
