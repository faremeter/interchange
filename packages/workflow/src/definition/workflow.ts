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
import type { CredentialBinding, GrantRequirement } from "@intx/types";

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
  /**
   * The grant requirements a run resolves against the creator's and
   * invoker's authority at trigger time. Each entry declares a resource,
   * action, and source (`creator` or `invoker`); the trigger route
   * materializes the satisfied ones as grants on the run principal.
   */
  grantRequirements?: readonly GrantRequirement[];
  /**
   * The credential bindings a launch resolves against tenant-owned
   * credentials, each mapping a tool package's declared handle to a
   * concrete provider and authorizing the delegation against the
   * binding's authority. The launch reads these from the folded body and
   * materializes a consumer-scoped `credential:{id}` / `use` grant per
   * binding.
   */
  credentialBindings?: readonly CredentialBinding[];
}

export interface WorkflowConfig {
  id: string;
  trigger?: Trigger;
  triggers?: readonly Trigger[];
  steps: Record<string, Primitive>;
  state?: { schema?: StateSchema };
  grantRequirements?: readonly GrantRequirement[];
  credentialBindings?: readonly CredentialBinding[];
}

export interface SingularWorkflowConfig<EnvReq extends BaseEnv> {
  id: string;
  agent: AgentDefinition<EnvReq>;
  trigger?: Trigger;
  triggers?: readonly Trigger[];
  state?: { schema?: StateSchema };
  grantRequirements?: readonly GrantRequirement[];
  credentialBindings?: readonly CredentialBinding[];
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

/**
 * A run id must be a non-empty sequence of ASCII letters, digits, underscores,
 * and hyphens -- the same shape as a step id. A run id is used verbatim as a
 * durable-store path segment (`runs/<runId>/...`) and as the local part of a
 * derived per-step mail address (`<runId>-<stepId>@<domain>`), so an
 * unconstrained caller-supplied id is a path-escape and an unroutable-address
 * hazard. `newId("run")` and the internally-derived child run ids (loop body,
 * onTrigger section) already satisfy this shape; the pattern is enforced at
 * the `runtimeRun` boundary where a run id enters the system.
 */
export const RUN_ID_PATTERN = /^[a-zA-Z0-9_-]+$/;

function normalize(config: WorkflowConfig): WorkflowDefinition {
  if (!config.id) {
    throw new Error("defineWorkflow requires a non-empty id");
  }

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
    // of the form `<runId>-<stepId>@<deploymentDomain>` for
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
    // `__` is the delimiter that joins a step id into the ids the runtime
    // derives from it: an inline-body ref (`<workflowId>__<stepId>`, and under
    // nesting `<parentRef>__<stepId>`), a loop iteration body run id
    // (`<runId>__<loopId>__<index>`), and an onTrigger section body run id
    // (`<sectionId>__<index>`, which is parsed back). A `__` inside a step id
    // would make one of those ids ambiguous with a different chain -- and they
    // key the durable store, so the collision is silent shared-state
    // corruption. Reject it in EVERY step id here, at the boundary that owns
    // the id grammar, so every ref/run-id segment stays atomic. A nested body
    // is its own normalized definition, so this covers every nesting level.
    if (stepId.includes("__")) {
      throw new Error(
        `step id ${JSON.stringify(stepId)} must not contain "__"; ` +
          `it becomes a segment of a runtime id joined by "__"`,
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

  validateSteps(steps);

  // An onTrigger section's `on` is the first-class binding between a
  // trigger and the section it drives, so each section contributes its
  // trigger to the workflow's subscription set.
  const triggers = resolveTriggers(config, collectSectionTriggers(steps));

  const definition: WorkflowDefinition = {
    id: config.id,
    triggers,
    steps,
    stepOrder,
    ...(config.state !== undefined ? { state: config.state } : {}),
    ...(config.grantRequirements !== undefined
      ? { grantRequirements: config.grantRequirements }
      : {}),
    ...(config.credentialBindings !== undefined
      ? { credentialBindings: config.credentialBindings }
      : {}),
  };
  return definition;
}

function resolveTriggers(
  config: WorkflowConfig,
  sectionTriggers: readonly Trigger[],
): readonly Trigger[] {
  if (config.trigger !== undefined && config.triggers !== undefined) {
    throw new Error("defineWorkflow accepts `trigger` or `triggers`, not both");
  }
  if (config.triggers !== undefined && config.triggers.length === 0) {
    throw new Error("`triggers` must be non-empty");
  }
  const declared: readonly Trigger[] =
    config.trigger !== undefined ? [config.trigger] : (config.triggers ?? []);
  // Dedupe by structural identity so a section whose `on` restates an
  // explicitly-declared trigger does not double-subscribe.
  const merged: Trigger[] = [];
  const seen = new Set<string>();
  for (const trigger of [...declared, ...sectionTriggers]) {
    const key = JSON.stringify(trigger);
    if (seen.has(key)) continue;
    seen.add(key);
    merged.push(trigger);
  }
  // A workflow that declares no trigger and has no onTrigger section is
  // manually invoked -- the same default the singular/plural configs carry
  // when `trigger`/`triggers` are omitted.
  if (merged.length === 0) return [{ type: "manual" }];
  return merged;
}

function collectSectionTriggers(
  steps: Record<string, Primitive>,
): readonly Trigger[] {
  const out: Trigger[] = [];
  for (const primitive of Object.values(steps)) {
    if (primitive.kind === "onTrigger") out.push(primitive.on);
  }
  return out;
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
    case "onTrigger":
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

/**
 * Run every step-record validation pass in the order their dependencies
 * require. `validateChildWorkflowBody` re-enters this same suite on an
 * inline child body, so factoring the passes here keeps the top-level and
 * embedded-child validations identical -- a malformed child (dangling
 * `after`, cycle, forbidden loop body, nested section) fails at the parent's
 * authoring time exactly as it would at its own.
 */
function validateSteps(steps: Record<string, Primitive>): void {
  validateAfterRefs(steps);
  // Runs after validateAfterRefs so every after/then/else endpoint is
  // already known to name a real step; this pass only rejects cycles.
  validateAcyclic(steps);
  validateLoopBody(steps);
  validateOnTriggerBody(steps);
  validateChildWorkflowBody(steps);
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
      // onExhausted routes only on exhaustion, so it must depend on the
      // loop. Without `after: [loop]` it would be schedulable from
      // RunStarted and the escalation would fire on every run.
      const target = steps[primitive.onExhausted];
      if (target !== undefined && !(target.after?.includes(stepId) ?? false)) {
        throw new Error(
          `loop ${stepId} onExhausted ${primitive.onExhausted} must name ${stepId} in its after`,
        );
      }
    }
    if (primitive.kind === "awaitSignal" && primitive.onTimeout !== undefined) {
      // onTimeout routes to a successor when the gate's timer fires, so it is
      // only meaningful WITH a timeout; a timeout WITHOUT onTimeout stays legal
      // (the gate fails on timeout, the pre-existing behavior).
      if (primitive.timeout === undefined) {
        throw new Error(
          `awaitSignal ${stepId} names onTimeout ${primitive.onTimeout} but sets no timeout; onTimeout only routes when a timer fires`,
        );
      }
      if (!ids.has(primitive.onTimeout)) {
        throw new Error(
          `awaitSignal ${stepId} names onTimeout ${primitive.onTimeout} which is not a known step`,
        );
      }
      if (primitive.onTimeout === stepId) {
        throw new Error(
          `awaitSignal ${stepId} cannot name itself as onTimeout`,
        );
      }
      // onTimeout routes only on a fired timer, so it must depend on the gate.
      // Without `after: [gate]` it would be schedulable from RunStarted and
      // fire on every run.
      const target = steps[primitive.onTimeout];
      if (target !== undefined && !(target.after?.includes(stepId) ?? false)) {
        throw new Error(
          `awaitSignal ${stepId} onTimeout ${primitive.onTimeout} must name ${stepId} in its after`,
        );
      }
    }
  }
}

// A loop iteration runs through the suspendable-child seam, so its body MAY
// park on an `awaitSignal` and resume (the container relays the signal park up
// its signal path), MAY spawn a `childWorkflow` grandchild -- lifted to a ref
// and depth-counted against the tree-wide ceiling like any other child -- and
// MAY contain a nested `loop`: an inner loop resolves its body ref from the same
// bodies map (env inheritance) and its own signal park relays up through the
// outer container the same way a leaf gate does. It still may not contain:
//   - `sleep` -- a parked sleep leaves the step `awaiting-timer`, and every
//     container park (including a nested loop's) relays a SIGNAL park, not a
//     timer park, so a loop body still has no timer-park resume path (INTR-485);
//   - `onTrigger` -- one subscription layer per run.
const LOOP_BODY_FORBIDDEN = new Set<Primitive["kind"]>(["sleep", "onTrigger"]);

// A static backstop on loop-body NESTING depth. Nesting is fixed by the
// definition tree, so it is bounded here, at construction, rather than by the
// runtime `MAX_CHILD_SPAWN_DEPTH` ceiling (which bounds the dynamic, possibly
// self-referential childWorkflow recursion -- a different constraint that must
// not be spent on static loop structure). Because a body is built bottom-up and
// each `defineWorkflow` validates as it constructs, a definition deeper than
// this cannot be built THROUGH `defineWorkflow`, so no downstream recursive
// reader (`projectForHash`, `runLoop`'s frames) ever sees one from an authored
// definition -- this one guard is the single chokepoint. (A `WorkflowDefinition`
// hand-assembled around `defineWorkflow` bypasses this, the same trust boundary
// every definition-time check relies on.) Small on purpose: nobody hand-authors
// deep loop nesting.
const MAX_LOOP_NESTING_DEPTH = 8;

/**
 * Reject a loop whose body contains a forbidden primitive, at every nesting
 * level, and reject nesting deeper than `MAX_LOOP_NESTING_DEPTH`. Recurses into
 * each loop body -- like `validateChildWorkflowBody` -- so the ban does not
 * depend on the (type-unenforced) invariant that every loop body came from its
 * own `defineWorkflow`; a hand-built body is checked here too. The walk
 * short-circuits at the depth limit, so a pathological input cannot overflow it.
 */
function validateLoopBody(steps: Record<string, Primitive>, depth = 0): void {
  for (const [stepId, primitive] of Object.entries(steps)) {
    if (primitive.kind !== "loop") continue;
    const bodyDepth = depth + 1;
    if (bodyDepth > MAX_LOOP_NESTING_DEPTH) {
      throw new Error(
        `loop ${stepId} nests loop bodies deeper than the maximum ` +
          `${MAX_LOOP_NESTING_DEPTH}; deep static nesting is rejected at ` +
          `definition time so no recursive reader overflows on it`,
      );
    }
    for (const [bodyStepId, bodyPrimitive] of Object.entries(
      primitive.body.steps,
    )) {
      if (LOOP_BODY_FORBIDDEN.has(bodyPrimitive.kind)) {
        throw new Error(
          `loop ${stepId} body step ${bodyStepId} is a ${bodyPrimitive.kind}; ` +
            `a loop body may not contain a sleep or onTrigger`,
        );
      }
    }
    validateLoopBody(primitive.body.steps, bodyDepth);
  }
}

/**
 * Reject an onTrigger section whose body nests another onTrigger. An
 * onTrigger body is otherwise unrestricted at DEFINITION time -- unlike a
 * loop body it may sleep, spawn child workflows, and so on (a loop body may
 * now await signals too, but still not sleep or spawn) -- because an onTrigger
 * section IS the sanctioned long-lived input loop. The single restriction is
 * one subscription layer per run: a section may not contain a section.
 *
 * PENDING INTR-310: a body agent `step` is accepted here but is not yet
 * EXECUTABLE -- per-step agent invocation inside a body is stubbed, so a body
 * runs only non-inference primitives (awaitSignal, sleep, childWorkflow) at
 * runtime today. INTR-310 wires the body invoker + per-body sources, after
 * which "run agent steps" becomes true at runtime as well.
 *
 * A separate pass from `validateAcyclic`, which does not recurse into the
 * body's own (already-normalized) `WorkflowDefinition`.
 */
function validateOnTriggerBody(steps: Record<string, Primitive>): void {
  for (const [stepId, primitive] of Object.entries(steps)) {
    if (primitive.kind !== "onTrigger") continue;
    // Only an inline (authored) body carries steps to constrain here; a
    // deployed `{ ref }` body was validated at its own deploy.
    if (!("inline" in primitive.body)) continue;
    for (const [bodyStepId, bodyPrimitive] of Object.entries(
      primitive.body.inline.steps,
    )) {
      if (bodyPrimitive.kind === "onTrigger") {
        throw new Error(
          `onTrigger ${stepId} body step ${bodyStepId} is itself an ` +
            `onTrigger; an onTrigger body may not nest another section`,
        );
      }
    }
  }
}

/**
 * Recursively validate every inline `childWorkflow` body. A child is an
 * owned import embedded inline, so its full definition must be as valid as a
 * top-level one; this pass re-enters `validateSteps` on the inline body so a
 * malformed embedded child is rejected at the parent's authoring time. A
 * deployed `{ ref }` body was validated at its own deploy and is skipped.
 *
 * A separate pass from `validateAcyclic`, which does not recurse into the
 * child's own (already-normalized) `WorkflowDefinition`. The recursion is
 * bounded by the authored nesting depth.
 */
function validateChildWorkflowBody(steps: Record<string, Primitive>): void {
  for (const primitive of Object.values(steps)) {
    if (primitive.kind !== "childWorkflow") continue;
    if (!("inline" in primitive.definition)) continue;
    validateSteps(primitive.definition.inline.steps);
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
    if (primitive.kind === "awaitSignal" && primitive.onTimeout !== undefined) {
      // An awaitSignal's onTimeout is a routing target like a loop's
      // onExhausted: on a timeout the gate routes to it instead of failing.
      // Same shape -- include the edge so an onTimeout naming an ancestor is
      // rejected as a cycle rather than corrupting branch pruning at runtime.
      addEdge(stepId, primitive.onTimeout);
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
    ...(definition.grantRequirements !== undefined
      ? { grantRequirements: definition.grantRequirements }
      : {}),
    // Bindings change launch-time authorization, so two definitions differing
    // only in their bindings must hash differently -- include them exactly as
    // grantRequirements is included, or the deploy substrate would dedupe them.
    ...(definition.credentialBindings !== undefined
      ? { credentialBindings: definition.credentialBindings }
      : {}),
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
    // canonicalization. A body may itself hold a nested loop, so this
    // recursion traverses it -- bounded by the finite definition tree and
    // the `MAX_LOOP_NESTING_DEPTH` limit enforced at definition time.
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
