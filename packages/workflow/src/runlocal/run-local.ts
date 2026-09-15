// runLocal entry point.
//
// Wires the in-memory env implementations to the single runtime body.
// The body lives in `runtime/run.ts` and is the same function the
// (future) child-process entry point will invoke. The only differences
// between local and production are which concrete `WorkflowRuntimeEnv`
// is supplied -- there is no `isChildProcess` branching anywhere in
// the body. `runtime/run.test.ts` enforces the discipline at the
// source level.

import {
  createDefaultDirectorRegistry,
  type DirectorRegistry,
} from "@intx/agent";

import type {
  AuthorizeContext,
  WorkflowAuthorizeFn,
} from "../authorize-context";
import type { ActionHandler, WorkflowDefinition } from "../definition/index";
import {
  enumerateInlineLoopBodies,
  rewriteInlineChildWorkflowBodies,
} from "../ontrigger-bodies";
import { runtimeRun, type RuntimeRunOptions } from "../runtime/run";
import { createNoopDrainController } from "../runtime/drain";
import { createEffectContext } from "../runtime/effect-context";
import { createLoopIterationHandle } from "../runtime/loop-iteration-handle";
import type {
  ActionInvoker,
  EffectLedger,
  LoopFnRegistry,
  StepInvoker,
  SpawnChildWorkflow,
  SpawnSuspendableChild,
  WorkflowRun,
  WorkflowRuntimeEnv,
} from "../runtime/env";
import { createInMemoryBlobSubstrate } from "./blob-substrate";
import { createInMemoryRepoStore } from "./repo-store";
import { createInMemoryScheduler } from "./scheduler";
import { createInMemorySignalChannel } from "./signal-channel";

export interface RunLocalOptions extends RuntimeRunOptions {
  /**
   * Override the agent-runner. The default invokes the per-step
   * `env.authorize` so the AuthorizeContext propagation invariant holds
   * in default-stub mode, fails the step on any decision that is not an
   * explicit allow, and otherwise returns a stub `AgentResult`. Tests
   * that exercise real agents wire their own runner (which constructs
   * `createAgent` and calls `agent.send`).
   */
  invokeStep?: StepInvoker;
  /**
   * Override the action-runner. The default resolves the handler ref via
   * `actionResolver`, builds an `EffectContext` against the in-memory
   * ledger, and runs the handler. Tests that need a shared durable
   * ledger across a re-run construct the env directly instead.
   */
  invokeAction?: ActionInvoker;
  /** Resolve an action `handler` ref to a handler function. */
  actionResolver?: (ref: string) => ActionHandler;
  /** Resolve a loop's `while`/`carry` refs to pure functions. */
  loopFns?: LoopFnRegistry;
  /**
   * Workflow-level authorize. Required, with no default: the caller owns
   * the decision about what a local run may do, and a permissive default
   * here made an authorization failure invisible until the workflow was
   * deployed. The deployed authorize refuses to answer when it cannot
   * resolve a decision (no credentials snapshot, no entry for the step);
   * the local surface refuses to invent one.
   */
  authorize: WorkflowAuthorizeFn;
  /**
   * Director registry. Defaults to the canonical built-in registry
   * from `@intx/agent` (the same surface production uses).
   */
  directors?: DirectorRegistry;
  /** Inject a deterministic clock for tests. */
  clock?: () => Date;
  /** Inject a deterministic id generator for tests. */
  newId?: (prefix: string) => string;
  /**
   * Whether a park in this run can be answered from outside it. Defaults to
   * true: a top-level local run is the addressable one, and its caller holds
   * the handle that delivers. The terminal-child spawner passes false, since
   * nothing can address a child run.
   */
  hasUpstreamSignalResolver?: boolean;
}

/**
 * Run a workflow in-process against in-memory env implementations.
 * Production wires the same runtime body to a sidecar-resident env.
 *
 * The returned `WorkflowRun` carries the same surface either way.
 */
export function runLocal(
  definition: WorkflowDefinition,
  options: RunLocalOptions,
): WorkflowRun {
  const directors = options.directors ?? createDefaultDirectorRegistry();
  const authorize = options.authorize;
  const invokeStep: StepInvoker =
    options.invokeStep ?? createDefaultStepInvoker(authorize);
  const effects = createInMemoryEffectLedger();
  const invokeAction: ActionInvoker =
    options.invokeAction ??
    createDefaultActionInvoker(authorize, effects, options.actionResolver);
  const clock = options.clock ?? defaultClock;
  const newId = options.newId ?? defaultNewId;

  // A `childWorkflow` primitive carries its child definition inline. Lift each
  // inline child to a standalone definition keyed by an internal ref and run
  // the rewritten workflow whose children are `{ ref }` -- the shape the
  // runtime dispatches. The in-memory spawn callback resolves each ref from the
  // lifted map, so no separate child resolver is needed. A recursive child that
  // embeds its own child is rewritten again when its run reaches this function.
  const { workflow: rewritten, bodies } =
    rewriteInlineChildWorkflowBodies(definition);
  const childBodies = new Map(bodies.map((b) => [b.ref, b.definition]));
  // A loop keeps its body inline on the primitive; register a ref-keyed copy so
  // the suspendable-loop executor resolves it, exactly as the deployed host
  // does. A loop body may itself contain a `childWorkflow` grandchild, so
  // rewrite each body's inline children to the `{ ref }` form the runtime
  // dispatches and fold the extracted grandchildren into `childBodies` -- the
  // map the iteration's inherited `spawnChild` resolves from -- before that
  // spawn callback is built below. (Nested loops in child-workflow children are
  // enumerated when their own recursive `runLocal` call reaches this point.)
  const loopBodies = new Map<string, WorkflowDefinition>();
  for (const loopBody of enumerateInlineLoopBodies(rewritten)) {
    const bodyRewrite = rewriteInlineChildWorkflowBodies(loopBody.definition);
    loopBodies.set(loopBody.ref, bodyRewrite.workflow);
    for (const grandchild of bodyRewrite.bodies) {
      childBodies.set(grandchild.ref, grandchild.definition);
    }
  }

  const repoStore = createInMemoryRepoStore();
  const env: WorkflowRuntimeEnv = {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: createInMemorySignalChannel({ newId: () => newId("sig") }),
    blobs: createInMemoryBlobSubstrate(),
    directors,
    authorize,
    invokeStep,
    invokeAction,
    effects,
    spawnChild: createInMemorySpawnChild(
      childBodies,
      inheritChildOptions(options),
    ),
    clock,
    newId,
    drain: createNoopDrainController(rewritten),
    hasUpstreamSignalResolver: options.hasUpstreamSignalResolver ?? true,
  };
  // Wired after construction because the loop-iteration executor closes over
  // the env it belongs to, so each iteration's body runs under the parent's
  // inherited env (its repoStore, blobs, effect ledger, invoker, and grants)
  // with only its own signal channel and park sinks.
  env.spawnLoopIteration = createSpawnLoopIteration(env, loopBodies);
  if (options.loopFns !== undefined) {
    env.loopFns = options.loopFns;
  }

  return runtimeRun(rewritten, env, extractRuntimeOptions(options));
}

function extractRuntimeOptions(options: RunLocalOptions): RuntimeRunOptions {
  const out: RuntimeRunOptions = {};
  if (options.triggerPayload !== undefined) {
    out.triggerPayload = options.triggerPayload;
  }
  if (options.consumedMessageId !== undefined) {
    out.consumedMessageId = options.consumedMessageId;
  }
  if (options.runId !== undefined) out.runId = options.runId;
  if (options.resumeFromEvents !== undefined) {
    out.resumeFromEvents = options.resumeFromEvents;
  }
  if (options.depth !== undefined) out.depth = options.depth;
  if (options.maxChildSpawnDepth !== undefined) {
    out.maxChildSpawnDepth = options.maxChildSpawnDepth;
  }
  return out;
}

/**
 * The subset of `RunLocalOptions` a spawned `childWorkflow` inherits from
 * its parent run. The deployed host builds the child's env from the
 * parent's -- the same step invoker, the same authorize (capped to the
 * child's declared resources), the same director registry, and the loop
 * and action resolvers re-loaded from the same deployment closure -- so a
 * local child that reverted to the bare defaults would let a strict env
 * pass a test whose child never saw it.
 *
 * Everything omitted here is either per-run identity the spawn callback
 * supplies itself (`runId`, `triggerPayload`, `depth`) or per-run
 * substrate the child builds fresh (its own event log, blob store, signal
 * channel, and effect ledger), matching how the deployed host scopes each
 * child run's substrate under its own `childRunId`.
 */
type InheritedChildOptions = Pick<
  RunLocalOptions,
  | "authorize"
  | "invokeStep"
  | "invokeAction"
  | "actionResolver"
  | "loopFns"
  | "directors"
  | "clock"
  | "newId"
>;

function inheritChildOptions(options: RunLocalOptions): InheritedChildOptions {
  return {
    authorize: options.authorize,
    ...(options.invokeStep !== undefined
      ? { invokeStep: options.invokeStep }
      : {}),
    ...(options.invokeAction !== undefined
      ? { invokeAction: options.invokeAction }
      : {}),
    ...(options.actionResolver !== undefined
      ? { actionResolver: options.actionResolver }
      : {}),
    ...(options.loopFns !== undefined ? { loopFns: options.loopFns } : {}),
    ...(options.directors !== undefined
      ? { directors: options.directors }
      : {}),
    ...(options.clock !== undefined ? { clock: options.clock } : {}),
    ...(options.newId !== undefined ? { newId: options.newId } : {}),
  };
}

/**
 * Default stub step invoker. Calls the workflow-level authorize so
 * AuthorizeContext propagation is observable, refuses any decision that
 * is not an explicit allow, then returns `{ output: null }`. Returning a
 * stable `null` (rather than echoing the input) keeps the "hello world"
 * path -- a workflow whose step's input resolves to `undefined` because
 * the caller did not supply `triggerPayload` -- from cliffing on the blob
 * substrate's strict non-serializable rejection. Real workflows supply a
 * runner that wraps `createAgent` and `agent.send`.
 *
 * Failing closed mirrors `createEffectContext`, which enforces the same
 * rule for an action's effects: deny, ask, and a null (no matching grant)
 * all block. A stub that discarded the decision would complete a step the
 * deployed agent harness would have refused, which is the whole class of
 * defect a local run is supposed to catch.
 */
function createDefaultStepInvoker(authorize: WorkflowAuthorizeFn): StepInvoker {
  return async ({ agent, authzContext }) => {
    const decision = await authorize(
      `tool:${agent.id}`,
      "invoke",
      authzContext,
    );
    if (decision.effect !== "allow") {
      throw new Error(
        `step agent ${agent.id} was not authorized (${String(decision.effect)})`,
      );
    }
    return { output: null };
  };
}

/**
 * Default action invoker. Resolves the handler ref, builds an
 * EffectContext against the supplied ledger and authorize, and runs the
 * handler. Failing loudly when no resolver is wired mirrors
 * `createNoopSpawnChild`: a silent stub would let action workflows pass
 * tests against effects that never ran.
 */
export function createDefaultActionInvoker(
  authorize: WorkflowAuthorizeFn,
  effects: EffectLedger,
  resolver: ((ref: string) => ActionHandler) | undefined,
): ActionInvoker {
  return async ({ handler, input, requires, authzContext, signal }) => {
    // Refuse before anything is constructed, so a cancelled run resolves no
    // handler and touches no ledger. An action is single-attempt with
    // observable side effects and there is no retry to reconsider the
    // decision, so starting one for a run already known to be cancelled is
    // not recoverable downstream. The step invoker refuses the same way.
    if (signal.aborted) {
      throw abortReason(signal);
    }
    if (!resolver) {
      throw new Error(
        `action ${handler} requires an actionResolver; pass one to runLocal({ actionResolver })`,
      );
    }
    const fn = resolver(handler);
    const ctx = createEffectContext({
      authorize,
      effects,
      requires,
      authzContext,
      input,
    });
    const output = await fn(input, ctx, signal);
    return { output };
  };
}

export function createInMemoryEffectLedger(): EffectLedger {
  const store = new Map<string, { output: unknown }>();
  return {
    async lookup(effectKey) {
      return store.get(effectKey);
    },
    async record(effectKey, output) {
      store.set(effectKey, { output });
    },
  };
}

/**
 * The local suspendable-loop executor: resolve the loop body from the lifted
 * map, give it its own in-memory signal channel, and drive it through the
 * shared park handle over the parent's inherited env. The deployed host wires
 * the same shape with a substrate-backed channel.
 */
export function createSpawnLoopIteration(
  baseEnv: WorkflowRuntimeEnv,
  bodies: ReadonlyMap<string, WorkflowDefinition>,
): SpawnSuspendableChild {
  return async ({
    definitionRef,
    childRunId,
    input,
    depth,
    maxChildSpawnDepth,
    signal,
    resumeFromEvents,
  }) => {
    const definition = bodies.get(definitionRef);
    if (definition === undefined) {
      throw new Error(
        `loop iteration ${definitionRef} has no lifted body definition; the ` +
          `loop body should have been enumerated before the run started`,
      );
    }
    return createLoopIterationHandle(baseEnv, {
      definition,
      childRunId,
      input,
      depth,
      maxChildSpawnDepth,
      ...(resumeFromEvents !== undefined ? { resumeFromEvents } : {}),
      signal,
      signalChannel: createInMemorySignalChannel({
        newId: () => baseEnv.newId("sig"),
      }),
    });
  };
}

export function createInMemorySpawnChild(
  bodies: ReadonlyMap<string, WorkflowDefinition>,
  inherited: InheritedChildOptions,
): SpawnChildWorkflow {
  return async ({
    definitionRef,
    childRunId,
    input,
    signal,
    depth,
    maxChildSpawnDepth,
  }) => {
    // Four awaits separate the child run id allocation from this call, one of
    // them a durable flush, so a cancel can land before the spawner runs. The
    // abort bridge below subscribes to an edge and would miss one already
    // past, leaving the child uncancelled and this function awaiting a
    // terminal that never comes. Refusing outright also avoids writing a whole
    // child log subtree for a run already known to be cancelled, and matches
    // what the deployed spawn adapter does, so a local rehearsal does not
    // diverge from production.
    if (signal.aborted) {
      throw abortReason(signal);
    }

    const resolved = bodies.get(definitionRef);
    if (resolved === undefined) {
      // The runtime dispatched a childWorkflow ref with no lifted definition.
      // Every inline child is lifted into `bodies` before the run starts, so a
      // miss is a rewrite/dispatch bug -- fail loud rather than silently
      // completing against a child that was never executed.
      throw new Error(
        `childWorkflow ${definitionRef} has no lifted definition; the inline child should have been extracted before the run started`,
      );
    }
    // Recursively invoke runLocal for the resolved child against the
    // parent-allocated childRunId so the parent's audit log and the
    // child's own log agree on identity. Carry the depth (already checked
    // one rung up) and the tree-wide ceiling so the child's own spawns keep
    // counting against the same bound, and the inherited env overrides so
    // the child runs under the same authorize and invokers as its parent.
    const child = runLocal(resolved, {
      ...inherited,
      triggerPayload: input,
      runId: childRunId,
      depth,
      maxChildSpawnDepth,
      // Terminal: the parent awaits this child's terminal rather than driving
      // it across parks, and nothing upstream can address the child run, so a
      // park inside it could never be answered.
      hasUpstreamSignalResolver: false,
    });
    const onParentAbort = (): void => {
      void child.cancel("supervisor-operator", "parent cancelled");
    };
    signal.addEventListener("abort", onParentAbort);
    try {
      const result = await child.complete;
      return { terminalStatus: result.terminalStatus };
    } finally {
      // Drop the listener when the child has settled so the parent's
      // per-step abort signal does not retain a reference to a child
      // that no longer needs cancellation.
      signal.removeEventListener("abort", onParentAbort);
    }
  };
}

/**
 * The error an aborted signal should surface: its own reason when it carries
 * one, so a cancel's cause is not replaced by a generic abort.
 */
function abortReason(signal: AbortSignal): Error {
  const reason: unknown = signal.reason;
  if (reason instanceof Error) return reason;
  return new DOMException("aborted", "AbortError");
}

function defaultClock(): Date {
  return new Date();
}

let idCounter = 0;
function defaultNewId(prefix: string): string {
  idCounter += 1;
  return `${prefix}-${String(idCounter)}-${Math.random().toString(36).slice(2, 8)}`;
}

// Re-export the workflow-level authorize context type so call sites
// importing only from `@intx/workflow/runlocal` see a coherent
// surface.
export type { AuthorizeContext, WorkflowAuthorizeFn };
