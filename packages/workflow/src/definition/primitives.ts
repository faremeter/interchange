// Workflow definition primitives.
//
// Authors compose a workflow's DAG by calling these constructors and
// keying their results into `defineWorkflow({ steps: { ... } })`. Each
// constructor returns a primitive descriptor whose `id` field is
// populated by `defineWorkflow` from the record key; that lets authors
// reuse the same constructor result under different keys without
// repeating themselves.
//
// `drainBehavior` defaults differ per primitive: `step` and `sleep`
// default to `"cancel"` because long compute should not block a
// redeploy past `drainTimeout`; `awaitSignal` defaults to `"wait"`
// because human-in-the-loop pauses are the canonical case the spec
// calls out and operators do not want them silently cancelled at
// redeploy. `map`'s outer node carries no `drainBehavior` -- its inner
// step carries its own.

import type { AgentDefinition, BaseEnv } from "@intx/agent";
import type { Type } from "arktype";

import type { Selector } from "./selectors";

export type DrainBehavior = "cancel" | "wait";

export interface RetryPolicy {
  /** Maximum number of attempts including the first. */
  maxAttempts: number;
  /** Initial backoff in milliseconds; subsequent attempts double it. */
  initialBackoffMs: number;
  /** Cap the backoff so large `maxAttempts` does not produce gigantic timers. */
  maxBackoffMs?: number;
}

export interface PrimitiveBase {
  /**
   * Stable id within the workflow. The constructors leave this empty;
   * `defineWorkflow` populates it from the record key. The runtime
   * always sees a fully-populated value.
   */
  id: string;
  /**
   * Step ids this primitive depends on. The DAG executor schedules a
   * primitive once every dependency has reached a terminal phase.
   * Omitting `after` makes the primitive eligible to start immediately
   * after `RunStarted`.
   */
  after?: readonly string[];
}

/**
 * `agent` is typed as `AgentDefinition<BaseEnv>` at the primitive
 * level. The author may have constructed it with a narrower
 * `AgentDefinition<MailEnv>`; the workflow runtime erases that
 * type-level requirement because the `StepInvoker` boundary
 * (`runtime/env.ts`) hands the agent off to a runtime-supplied
 * callback that decides how to instantiate it. Production wires that
 * callback through to `createAgent`, where `validateEnv` enforces the
 * actual presence of every required env key via tool-factory and
 * director `requires` metadata; the `step()` constructor takes the
 * narrower type for compile-time author ergonomics and erases at
 * storage. The runtime body never reads `agent.toolFactories` itself.
 */
export interface StepPrimitive extends PrimitiveBase {
  kind: "step";
  agent: AgentDefinition<BaseEnv>;
  input?: Selector;
  reads?: readonly Selector[];
  writes?: readonly Selector[];
  retry?: RetryPolicy;
  /** Per-step timeout in milliseconds; enforced via an `AbortSignal`. */
  timeout?: number;
  drainBehavior?: DrainBehavior;
}

export interface MapPrimitive extends PrimitiveBase {
  kind: "map";
  over: Selector;
  step: StepPrimitive;
  retry?: RetryPolicy;
}

export interface GatePrimitive extends PrimitiveBase {
  kind: "gate";
  when: Selector;
  then: string;
  else: string;
}

export interface AwaitSignalPrimitive extends PrimitiveBase {
  kind: "awaitSignal";
  name: string;
  timeout?: number;
  onTimeout?: string;
  drainBehavior?: DrainBehavior;
}

export interface SleepPrimitive extends PrimitiveBase {
  kind: "sleep";
  duration?: number;
  until?: string;
  drainBehavior?: DrainBehavior;
}

export interface ChildWorkflowPrimitive extends PrimitiveBase {
  kind: "childWorkflow";
  definitionRef: string;
  input?: Selector;
  drainBehavior?: DrainBehavior;
}

export interface EscalationPrimitive extends PrimitiveBase {
  kind: "escalation";
  to: string;
  data?: Selector;
}

export type Primitive =
  | StepPrimitive
  | MapPrimitive
  | GatePrimitive
  | AwaitSignalPrimitive
  | SleepPrimitive
  | ChildWorkflowPrimitive
  | EscalationPrimitive;

// =========================================================================
// Constructors
// =========================================================================

export interface StepOpts<EnvReq extends BaseEnv> {
  agent: AgentDefinition<EnvReq>;
  input?: Selector;
  reads?: readonly Selector[];
  writes?: readonly Selector[];
  retry?: RetryPolicy;
  timeout?: number;
  drainBehavior?: DrainBehavior;
  after?: readonly string[];
}

export function step<EnvReq extends BaseEnv>(
  opts: StepOpts<EnvReq>,
): StepPrimitive {
  const drainBehavior: DrainBehavior = opts.drainBehavior ?? "cancel";
  // The narrower `EnvReq` requirements (tool factories that need
  // `transport`, an author-supplied director with extra env keys) live
  // on the agent's tool-factory metadata. The workflow runtime hands
  // the agent off to its `StepInvoker`, which is wired in production
  // through `createAgent` -- `validateEnv` enforces the requirements
  // at instantiation. Erasing the type-level requirement here keeps
  // `Primitive` a flat union the executor can switch on without
  // juggling per-step EnvReq parameters.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the agent's env requirements are checked at StepInvoker time by createAgent's validateEnv; the runtime body itself does not read AgentDefinition.toolFactories
  const agent = opts.agent as AgentDefinition<BaseEnv>;
  const primitive: StepPrimitive = {
    kind: "step",
    id: "",
    agent,
    drainBehavior,
    ...(opts.input !== undefined ? { input: opts.input } : {}),
    ...(opts.reads !== undefined ? { reads: opts.reads } : {}),
    ...(opts.writes !== undefined ? { writes: opts.writes } : {}),
    ...(opts.retry !== undefined ? { retry: opts.retry } : {}),
    ...(opts.timeout !== undefined ? { timeout: opts.timeout } : {}),
    ...(opts.after !== undefined ? { after: opts.after } : {}),
  };
  return primitive;
}

export interface MapOpts {
  over: Selector;
  step: StepPrimitive;
  retry?: RetryPolicy;
  after?: readonly string[];
}

export function map(opts: MapOpts): MapPrimitive {
  return {
    kind: "map",
    id: "",
    over: opts.over,
    step: opts.step,
    ...(opts.retry !== undefined ? { retry: opts.retry } : {}),
    ...(opts.after !== undefined ? { after: opts.after } : {}),
  };
}

export interface GateOpts {
  when: Selector;
  then: string;
  else: string;
  after?: readonly string[];
}

export function gate(opts: GateOpts): GatePrimitive {
  return {
    kind: "gate",
    id: "",
    when: opts.when,
    then: opts.then,
    else: opts.else,
    ...(opts.after !== undefined ? { after: opts.after } : {}),
  };
}

export interface AwaitSignalOpts {
  name: string;
  timeout?: number;
  onTimeout?: string;
  drainBehavior?: DrainBehavior;
  after?: readonly string[];
}

export function awaitSignal(opts: AwaitSignalOpts): AwaitSignalPrimitive {
  const drainBehavior: DrainBehavior = opts.drainBehavior ?? "wait";
  return {
    kind: "awaitSignal",
    id: "",
    name: opts.name,
    drainBehavior,
    ...(opts.timeout !== undefined ? { timeout: opts.timeout } : {}),
    ...(opts.onTimeout !== undefined ? { onTimeout: opts.onTimeout } : {}),
    ...(opts.after !== undefined ? { after: opts.after } : {}),
  };
}

export interface SleepOpts {
  duration?: number;
  until?: string;
  drainBehavior?: DrainBehavior;
  after?: readonly string[];
}

export function sleep(opts: SleepOpts): SleepPrimitive {
  if (opts.duration === undefined && opts.until === undefined) {
    throw new Error("sleep requires either `duration` or `until`");
  }
  if (opts.duration !== undefined && opts.until !== undefined) {
    throw new Error("sleep accepts at most one of `duration` or `until`");
  }
  const drainBehavior: DrainBehavior = opts.drainBehavior ?? "cancel";
  return {
    kind: "sleep",
    id: "",
    drainBehavior,
    ...(opts.duration !== undefined ? { duration: opts.duration } : {}),
    ...(opts.until !== undefined ? { until: opts.until } : {}),
    ...(opts.after !== undefined ? { after: opts.after } : {}),
  };
}

export interface ChildWorkflowOpts {
  definitionRef: string;
  input?: Selector;
  drainBehavior?: DrainBehavior;
  after?: readonly string[];
}

export function childWorkflow(opts: ChildWorkflowOpts): ChildWorkflowPrimitive {
  const drainBehavior: DrainBehavior = opts.drainBehavior ?? "cancel";
  return {
    kind: "childWorkflow",
    id: "",
    definitionRef: opts.definitionRef,
    drainBehavior,
    ...(opts.input !== undefined ? { input: opts.input } : {}),
    ...(opts.after !== undefined ? { after: opts.after } : {}),
  };
}

export interface EscalationOpts {
  to: string;
  data?: Selector;
  after?: readonly string[];
}

export function escalation(opts: EscalationOpts): EscalationPrimitive {
  return {
    kind: "escalation",
    id: "",
    to: opts.to,
    ...(opts.data !== undefined ? { data: opts.data } : {}),
    ...(opts.after !== undefined ? { after: opts.after } : {}),
  };
}

// arktype's `Type` is referenced from this module to keep the
// definition-surface arrow types in one place; `state.schema` consumers
// import it from here.
export type StateSchema = Type;
