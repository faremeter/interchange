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
import { runtimeRun, type RuntimeRunOptions } from "../runtime/run";
import { createNoopDrainController } from "../runtime/drain";
import { createEffectContext } from "../runtime/effect-context";
import type {
  ActionInvoker,
  EffectContext,
  EffectLedger,
  StepInvoker,
  SpawnChildWorkflow,
  WorkflowRun,
  WorkflowRuntimeEnv,
} from "../runtime/env";
import { createInMemoryBlobSubstrate } from "./blob-substrate";
import { createLoopIteration } from "./loop-iteration";
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
  /** Resolve a `definitionRef` for `childWorkflow` spawns. */
  childResolver?: (ref: string) => WorkflowDefinition;
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
    spawnChild: createNoopSpawnChild(options.childResolver),
    clock,
    newId,
    drain: createNoopDrainController(definition),
  };
  // Wired after construction because the loop-iteration runner closes
  // over the env it belongs to, so that each iteration's child run
  // shares the parent's repoStore, blobs, and effect ledger.
  env.runLoopIteration = createLoopIteration(env);

  return runtimeRun(definition, env, extractRuntimeOptions(options));
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
function createDefaultActionInvoker(
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

function createInMemoryEffectLedger(): EffectLedger {
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

function createNoopSpawnChild(
  resolver: ((ref: string) => WorkflowDefinition) | undefined,
): SpawnChildWorkflow {
  return async ({ definitionRef, childRunId, input, signal }) => {
    if (!resolver) {
      // The author wired a `childWorkflow` primitive into their
      // workflow but did not supply a resolver. Failing loudly is the
      // right call -- a silent stub-completion would let workflows
      // pass tests against a child that was never executed.
      throw new Error(
        `childWorkflow ${definitionRef} requires a childResolver; pass one to runLocal({ childResolver })`,
      );
    }
    const resolved = resolver(definitionRef);
    // Recursively invoke runLocal for the resolved child against the
    // parent-allocated childRunId so the parent's audit log and the
    // child's own log agree on identity.
    const child = runLocal(resolved, {
      triggerPayload: input,
      runId: childRunId,
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
