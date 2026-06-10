// `defineWorkflow` -- the entry point for authoring a workflow.
//
// Takes an authoring config (either the plural `steps` shape or the
// singular shorthand) and returns a normalized `WorkflowDefinition`:
// portable, hashable data the deploy substrate can ship and the runtime
// can interpret. The normalization populates every primitive's `id`
// from its record key and applies the default-input convention so the
// runtime sees a fully-specified definition with no implicit shape.

import { canonicalizeForHash } from "@intx/agent";
import type { AgentDefinition, BaseEnv } from "@intx/agent";

import { normalizeSingularShorthand } from "./shorthand";
import {
  type AwaitSignalPrimitive,
  type Primitive,
  type SleepPrimitive,
  type StateSchema,
  type StepPrimitive,
} from "./primitives";
import type { Trigger } from "./triggers";

export interface WorkflowDefinition {
  id: string;
  triggers: readonly Trigger[];
  steps: Record<string, Primitive>;
  /**
   * The order steps are listed in. The default-input convention uses
   * the order to pick the `previousStepId` for any step whose `input`
   * was omitted and whose `after` resolves to a single dependency.
   */
  stepOrder: readonly string[];
  state?: { schema?: StateSchema };
}

export interface WorkflowConfig {
  id: string;
  trigger?: Trigger;
  triggers?: readonly Trigger[];
  steps: Record<string, Primitive>;
  state?: { schema?: StateSchema };
}

export interface SingularWorkflowConfig<EnvReq extends BaseEnv> {
  id: string;
  agent: AgentDefinition<EnvReq>;
  trigger?: Trigger;
  triggers?: readonly Trigger[];
  state?: { schema?: StateSchema };
}

/**
 * Discriminate plural (`steps`) from singular (`agent`) authoring
 * shapes. The user-facing overloads accept either; the normalization
 * step funnels both through the same internal shape.
 */
export function defineWorkflow(config: WorkflowConfig): WorkflowDefinition;
export function defineWorkflow<EnvReq extends BaseEnv>(
  config: SingularWorkflowConfig<EnvReq>,
): WorkflowDefinition;
export function defineWorkflow<EnvReq extends BaseEnv>(
  config: WorkflowConfig | SingularWorkflowConfig<EnvReq>,
): WorkflowDefinition {
  const normalized: WorkflowConfig =
    "agent" in config
      ? normalizeSingularShorthand(config)
      : (config as WorkflowConfig);
  return normalize(normalized);
}

function normalize(config: WorkflowConfig): WorkflowDefinition {
  if (!config.id) {
    throw new Error("defineWorkflow requires a non-empty id");
  }

  const triggers = resolveTriggers(config);

  const stepEntries = Object.entries(config.steps);
  if (stepEntries.length === 0) {
    throw new Error("defineWorkflow requires at least one step");
  }
  const seen = new Set<string>();
  const steps: Record<string, Primitive> = {};
  const stepOrder: string[] = [];
  for (const [stepId, primitive] of stepEntries) {
    if (seen.has(stepId)) {
      throw new Error(`duplicate step id ${stepId}`);
    }
    seen.add(stepId);
    if (stepId === "") {
      throw new Error("step ids cannot be empty");
    }
    if (primitive.id !== "" && primitive.id !== stepId) {
      throw new Error(
        `step ${stepId} carries a conflicting embedded id ${primitive.id}; ` +
          `prefer leaving the embedded id empty so defineWorkflow assigns ` +
          `it from the record key`,
      );
    }
    const withId: Primitive = { ...primitive, id: stepId };
    steps[stepId] = applyDefaultInput(withId, stepOrder);
    stepOrder.push(stepId);
  }

  validateAfterRefs(steps);

  const definition: WorkflowDefinition = {
    id: config.id,
    triggers,
    steps,
    stepOrder,
    ...(config.state !== undefined ? { state: config.state } : {}),
  };
  return definition;
}

function resolveTriggers(config: WorkflowConfig): readonly Trigger[] {
  if (config.trigger !== undefined && config.triggers !== undefined) {
    throw new Error("defineWorkflow accepts `trigger` or `triggers`, not both");
  }
  if (config.trigger !== undefined) {
    return [config.trigger];
  }
  if (config.triggers !== undefined) {
    if (config.triggers.length === 0) {
      throw new Error("`triggers` must be non-empty");
    }
    return config.triggers;
  }
  return [{ type: "manual" }];
}

/**
 * Apply the default-input convention to a step or step-bearing
 * primitive. The first step in the record gets `{ from: "trigger.payload" }`;
 * subsequent steps with a single dependency get
 * `{ from: "steps.<previousStepId>.output" }`. Multi-`after` steps
 * without explicit `input` are rejected -- the convention has no
 * principled choice between multiple upstreams.
 *
 * For a `map` primitive, the convention applies to the *inner* step.
 * The inner step's `after` is typically undefined, so it receives the
 * first-step default of `{ from: "trigger.payload" }`. At runtime the
 * `runMap` interpreter overrides the selector root's `trigger.payload`
 * with the per-item value, so the inner step's default-input
 * effectively means "the current item." This is the only place the
 * meaning of `trigger.payload` is locally rebound by the runtime.
 */
function applyDefaultInput(
  primitive: Primitive,
  prior: readonly string[],
): Primitive {
  switch (primitive.kind) {
    case "step":
      return applyDefaultInputStep(primitive, prior);
    case "map":
      return {
        ...primitive,
        step: applyDefaultInputStep(primitive.step, prior),
      };
    case "awaitSignal":
    case "sleep":
    case "gate":
    case "childWorkflow":
    case "escalation":
      return primitive;
  }
}

function applyDefaultInputStep(
  primitive: StepPrimitive,
  prior: readonly string[],
): StepPrimitive {
  if (primitive.input !== undefined) {
    return primitive;
  }
  const after = primitive.after;
  if (after === undefined || after.length === 0) {
    if (prior.length > 0) {
      // A first-step default only makes sense for the literal first
      // record entry; otherwise the author has to pick.
      return primitive;
    }
    return { ...primitive, input: { from: "trigger.payload" } };
  }
  if (after.length === 1) {
    const previousStepId = after[0];
    if (previousStepId === undefined) {
      return primitive;
    }
    return {
      ...primitive,
      input: { from: `steps.${previousStepId}.output` },
    };
  }
  return primitive;
}

function validateAfterRefs(steps: Record<string, Primitive>): void {
  const ids = new Set(Object.keys(steps));
  for (const [stepId, primitive] of Object.entries(steps)) {
    const after = primitive.after;
    if (after !== undefined) {
      for (const dep of after) {
        if (!ids.has(dep)) {
          throw new Error(
            `step ${stepId} declares after ${dep} which is not a known step`,
          );
        }
        if (dep === stepId) {
          throw new Error(`step ${stepId} cannot depend on itself`);
        }
      }
    }
    if (primitive.kind === "gate") {
      if (!ids.has(primitive.then)) {
        throw new Error(
          `gate ${stepId} names then-branch ${primitive.then} which is not a known step`,
        );
      }
      if (!ids.has(primitive.else)) {
        throw new Error(
          `gate ${stepId} names else-branch ${primitive.else} which is not a known step`,
        );
      }
      if (primitive.then === primitive.else) {
        throw new Error(
          `gate ${stepId} has then === else (${primitive.then}); the gate would be meaningless`,
        );
      }
      if (primitive.then === stepId || primitive.else === stepId) {
        throw new Error(`gate ${stepId} cannot name itself as a branch`);
      }
    }
  }
}

/**
 * Produce a deterministic content-addressed hash of a workflow
 * definition. The hash drives the workflow-run record (`RunStarted`
 * carries this value) and the deploy substrate's content-addressing.
 *
 * Definitions carry `AgentDefinition` envelopes whose `toolFactories`
 * are functions (with attached metadata); `canonicalizeForHash`
 * rejects functions, so we project agents down to their hashable
 * fields (identity, prompt, capabilities, inference preferences,
 * director ref, tool-factory metadata ids and `requires` sets, tags)
 * before canonicalizing. The runtime-derived `stepOrder` is also
 * dropped because it is fully determined by the steps record.
 */
export function hashDefinition(definition: WorkflowDefinition): Uint8Array {
  return canonicalizeForHash(projectForHash(definition));
}

function projectForHash(definition: WorkflowDefinition): unknown {
  return {
    id: definition.id,
    triggers: definition.triggers,
    ...(definition.state !== undefined ? { state: definition.state } : {}),
    steps: Object.fromEntries(
      Object.entries(definition.steps).map(([id, primitive]) => [
        id,
        projectPrimitive(primitive),
      ]),
    ),
  };
}

function projectPrimitive(primitive: Primitive): unknown {
  if (primitive.kind === "step") {
    return { ...primitive, agent: projectAgent(primitive.agent) };
  }
  if (primitive.kind === "map") {
    return {
      ...primitive,
      step: { ...primitive.step, agent: projectAgent(primitive.step.agent) },
    };
  }
  return primitive;
}

function projectAgent(agent: StepPrimitive["agent"]): unknown {
  return {
    id: agent.id,
    ...(agent.description !== undefined
      ? { description: agent.description }
      : {}),
    systemPrompt: agent.systemPrompt,
    ...(agent.director !== undefined ? { director: agent.director } : {}),
    toolFactoryIds: agent.toolFactories.map((factory) => ({
      id: factory.id,
      requires: factory.requires,
    })),
    capabilities: agent.capabilities,
    inference: agent.inference,
    ...(agent.tags !== undefined ? { tags: agent.tags } : {}),
  };
}

// Re-export commonly used helpers so call sites import from a single
// definition surface entry point.
export type { AwaitSignalPrimitive, SleepPrimitive, StepPrimitive };
