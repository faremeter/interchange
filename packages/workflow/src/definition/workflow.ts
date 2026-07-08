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

/**
 * `stepId` must be a non-empty sequence of ASCII letters, digits,
 * underscores, and hyphens. The constraint exists so the workflow-deploy
 * orchestrator can derive per-step mail addresses by string concat
 * without escaping. Exported for downstream consumers (notably the
 * orchestrator's per-step address derivation) that need to assert the
 * same shape.
 */
export const STEP_ID_PATTERN = /^[a-zA-Z0-9_-]+$/;

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
    // The workflow-deploy orchestrator derives per-step mail addresses
    // of the form `ins_<deploymentId>-<stepId>@<deploymentDomain>` for
    // multi-step deployments. Constraining `stepId` to
    // `[a-zA-Z0-9_-]+` at definition time means the derived local-part
    // never needs escaping and the address parser at the substrate
    // boundary never sees a step-id-shaped local-part it cannot
    // round-trip.
    if (!STEP_ID_PATTERN.test(stepId)) {
      throw new Error(
        `step id ${JSON.stringify(stepId)} must match ${STEP_ID_PATTERN.source}`,
      );
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
  // Runs after validateAfterRefs so every after/then/else endpoint is
  // already known to name a real step; this pass only rejects cycles.
  validateAcyclic(steps);
  validateLoopBody(steps);

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
    case "action":
    case "loop":
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
    if (primitive.kind === "loop") {
      if (!ids.has(primitive.onExhausted)) {
        throw new Error(
          `loop ${stepId} names onExhausted ${primitive.onExhausted} which is not a known step`,
        );
      }
      if (primitive.onExhausted === stepId) {
        throw new Error(`loop ${stepId} cannot name itself as onExhausted`);
      }
    }
  }
}

// A loop body runs as an isolated child per iteration, so it may not
// contain a suspending primitive (awaitSignal/sleep) or another
// child-spawning primitive (loop/childWorkflow) -- those would nest the
// resume-cancel interaction the first-cut loop deliberately does not
// support. The outer per-level iteration in a real consumer stays in
// host code instead of a nested loop.
const LOOP_BODY_FORBIDDEN = new Set<Primitive["kind"]>([
  "loop",
  "awaitSignal",
  "sleep",
  "childWorkflow",
]);

/**
 * Reject a loop whose body contains a forbidden primitive. This is a
 * separate pass from `validateAcyclic` because that check does not
 * recurse into a loop's nested body definition (the body is its own
 * `WorkflowDefinition`, already normalized by its own `defineWorkflow`
 * call, but its top-level kinds must still be constrained here).
 */
function validateLoopBody(steps: Record<string, Primitive>): void {
  for (const [stepId, primitive] of Object.entries(steps)) {
    if (primitive.kind !== "loop") continue;
    for (const [bodyStepId, bodyPrimitive] of Object.entries(
      primitive.body.steps,
    )) {
      if (LOOP_BODY_FORBIDDEN.has(bodyPrimitive.kind)) {
        throw new Error(
          `loop ${stepId} body step ${bodyStepId} is a ${bodyPrimitive.kind}; ` +
            `a loop body may not contain a loop, awaitSignal, sleep, or childWorkflow`,
        );
      }
    }
  }
}

/**
 * Reject any dependency cycle in the definition. The graph is the union
 * of two edge kinds: an `after: [X]` on step S contributes X -> S (X
 * must precede S), and a `gate` G with branches `then`/`else`
 * contributes G -> then and G -> else. A legitimate gate only ever names
 * forward branches, so its edges run the same direction as the `after`
 * edges and close no loop; a gate that names an ancestor (a back-edge)
 * closes a cycle, which at runtime silently corrupts branch-pruning so
 * the not-selected branch's subtree runs. Rejecting the cycle here makes
 * that unconstructable. The check must include the gate edges: an F2
 * back-edge forms a cycle only in the `after ∪ gate` graph, never in the
 * `after` graph alone, so a pure-`after` check would miss it.
 */
function validateAcyclic(steps: Record<string, Primitive>): void {
  const adjacency = buildDependencyAdjacency(steps);
  const done = new Set<string>();
  const onPath = new Set<string>();
  const path: string[] = [];

  const visit = (node: string): void => {
    onPath.add(node);
    path.push(node);
    for (const next of adjacency.get(node) ?? []) {
      if (onPath.has(next)) {
        const cycle = path.slice(path.indexOf(next));
        cycle.push(next);
        throw new Error(
          `workflow definition has a dependency cycle: ${cycle.join(" -> ")}`,
        );
      }
      if (!done.has(next)) {
        visit(next);
      }
    }
    onPath.delete(node);
    path.pop();
    done.add(node);
  };

  for (const node of Object.keys(steps)) {
    if (!done.has(node)) {
      visit(node);
    }
  }
}

/**
 * Build the dependency adjacency map once (dep -> dependents plus gate ->
 * branch targets) so the cycle check is a single DFS over a precomputed
 * graph rather than an edge rescan per node.
 */
function buildDependencyAdjacency(
  steps: Record<string, Primitive>,
): Map<string, string[]> {
  const adjacency = new Map<string, string[]>();
  const addEdge = (from: string, to: string): void => {
    const edges = adjacency.get(from);
    if (edges === undefined) {
      adjacency.set(from, [to]);
    } else {
      edges.push(to);
    }
  };
  for (const [stepId, primitive] of Object.entries(steps)) {
    if (primitive.after !== undefined) {
      for (const dep of primitive.after) {
        addEdge(dep, stepId);
      }
    }
    if (primitive.kind === "gate") {
      addEdge(stepId, primitive.then);
      addEdge(stepId, primitive.else);
    }
    if (primitive.kind === "loop") {
      // onExhausted is a routing target like a gate branch: normally a
      // forward step (redundant with its `after` edge), but naming an
      // ancestor closes a cycle. Include it so a back-edge is rejected.
      addEdge(stepId, primitive.onExhausted);
    }
  }
  return adjacency;
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
  if (primitive.kind === "loop") {
    // A loop carries an inline body definition whose steps may hold
    // agents (function-valued tool factories). Project the body the same
    // way as the top level so the whole thing is function-free before
    // canonicalization. The body-ban forbids a nested loop, so this
    // recursion is bounded.
    return { ...primitive, body: projectForHash(primitive.body) };
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
