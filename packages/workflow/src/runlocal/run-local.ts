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
import type { WorkflowDefinition } from "../definition/index";
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
  EffectContext,
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
   * Override the agent-runner. The default returns a stub
   * `AgentResult` after invoking the per-step `env.authorize` so the
   * AuthorizeContext propagation invariant holds in default-stub
   * mode. Tests that exercise real agents wire their own runner
   * (which constructs `createAgent` and calls `agent.send`).
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
   * Workflow-level authorize. Defaults to `() => allow`; tests inject
   * a spy.
   */
  authorize?: WorkflowAuthorizeFn;
  /**
   * Director registry. Defaults to the canonical built-in registry
   * from `@intx/agent` (the same surface production uses).
   */
  directors?: DirectorRegistry;
  /** Inject a deterministic clock for tests. */
  clock?: () => Date;
  /** Inject a deterministic id generator for tests. */
  newId?: (prefix: string) => string;
}

/**
 * Run a workflow in-process against in-memory env implementations.
 * Production wires the same runtime body to a sidecar-resident env.
 *
 * The returned `WorkflowRun` carries the same surface either way.
 */
export function runLocal(
  definition: WorkflowDefinition,
  options: RunLocalOptions = {},
): WorkflowRun {
  const directors = options.directors ?? createDefaultDirectorRegistry();
  const authorize: WorkflowAuthorizeFn =
    options.authorize ??
    (async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }));
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
  // does. (Nested loops in child-workflow children are enumerated when their
  // own recursive `runLocal` call reaches this point.)
  const loopBodies = new Map(
    enumerateInlineLoopBodies(rewritten).map((b) => [b.ref, b.definition]),
  );

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
    spawnChild: createInMemorySpawnChild(childBodies),
    clock,
    newId,
    drain: createNoopDrainController(rewritten),
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
 * Default stub step invoker. Calls the workflow-level authorize so
 * AuthorizeContext propagation is observable, then returns
 * `{ output: null }`. Returning a stable `null` (rather than echoing
 * the input) keeps the "hello world" path -- a workflow whose step's
 * input resolves to `undefined` because the caller did not supply
 * `triggerPayload` -- from cliffing on the blob substrate's strict
 * non-serializable rejection. Real workflows supply a runner that
 * wraps `createAgent` and `agent.send`.
 */
function createDefaultStepInvoker(authorize: WorkflowAuthorizeFn): StepInvoker {
  return async ({ agent, authzContext }) => {
    await authorize(`tool:${agent.id}`, "invoke", authzContext);
    return { output: null };
  };
}

/**
 * An action handler: deterministic host TypeScript that performs its
 * external effects through the capability- and ledger-checked
 * `EffectContext`. The default action invoker resolves a handler ref to
 * one of these.
 */
export type ActionHandler = (
  input: unknown,
  ctx: EffectContext,
  signal: AbortSignal,
) => Promise<unknown>;

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
      ...(resumeFromEvents !== undefined ? { resumeFromEvents } : {}),
      signal,
      signalChannel: createInMemorySignalChannel({
        newId: () => baseEnv.newId("sig"),
      }),
    });
  };
}

function createInMemorySpawnChild(
  bodies: ReadonlyMap<string, WorkflowDefinition>,
): SpawnChildWorkflow {
  return async ({
    definitionRef,
    childRunId,
    input,
    signal,
    depth,
    maxChildSpawnDepth,
  }) => {
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
    // counting against the same bound.
    const child = runLocal(resolved, {
      triggerPayload: input,
      runId: childRunId,
      depth,
      maxChildSpawnDepth,
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
