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
import type {
  CredentialBinding,
  GrantRequirement,
  SidecarCapabilityPolicy,
} from "@intx/types";

import { normalizeSingularShorthand } from "./shorthand";
import {
  type AwaitSignalPrimitive,
  type Primitive,
  type SleepPrimitive,
  type StateSchema,
  type StepPrimitive,
} from "./primitives";
import {
  isFromSelector,
  isMergeSelector,
  isProjectSelector,
  splitPath,
  type Selector,
} from "./selectors";
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
  sidecarPlacement?: SidecarCapabilityPolicy;
}

export interface WorkflowConfig {
  id: string;
  trigger?: Trigger;
  triggers?: readonly Trigger[];
  steps: Record<string, Primitive>;
  state?: { schema?: StateSchema };
  grantRequirements?: readonly GrantRequirement[];
  credentialBindings?: readonly CredentialBinding[];
  sidecarPlacement?: SidecarCapabilityPolicy;
}

export interface SingularWorkflowConfig<EnvReq extends BaseEnv> {
  id: string;
  agent: AgentDefinition<EnvReq>;
  trigger?: Trigger;
  triggers?: readonly Trigger[];
  state?: { schema?: StateSchema };
  grantRequirements?: readonly GrantRequirement[];
  credentialBindings?: readonly CredentialBinding[];
  sidecarPlacement?: SidecarCapabilityPolicy;
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
    ...(config.sidecarPlacement !== undefined
      ? { sidecarPlacement: config.sidecarPlacement }
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
  // Runs after validateAcyclic so the dependency graph is known acyclic and
  // the reachability walk terminates.
  validateConcurrentAwaitSignalNames(steps);
  // Runs after validateAcyclic so its closure computations reason over an
  // acyclic graph, and after validateAfterRefs so every onFailure handler is
  // known to exist and to `after`-depend on its unit.
  validateOnFailureStraddlers(steps);
  validateLoopBody(steps);
  validateOnTriggerBody(steps);
  validateChildWorkflowBody(steps);
}

/**
 * The `FromSelector` references into `steps.<unitId>.output`, each tagged with
 * whether it is a whole-object read and whether it sits under a narrowing
 * `ProjectSelector`. Tokenized with the shared `splitPath`, so a bracket index
 * (`steps.U.output[0]`) is correctly seen as a deeper read, not a whole one.
 */
function unitOutputRefs(
  selector: Selector,
  unitId: string,
): { whole: boolean; underProject: boolean }[] {
  const refs: { whole: boolean; underProject: boolean }[] = [];
  const walk = (s: Selector, underProject: boolean): void => {
    if (isFromSelector(s)) {
      const [a, b, c] = splitPath(s.from);
      if (
        a?.kind === "key" &&
        a.key === "steps" &&
        b?.kind === "key" &&
        b.key === unitId &&
        c?.kind === "key" &&
        c.key === "output"
      ) {
        refs.push({ whole: splitPath(s.from).length === 3, underProject });
      }
    } else if (isProjectSelector(s)) {
      walk(s.project, true);
    } else if (isMergeSelector(s)) {
      for (const inner of s.merge) walk(inner, underProject);
    }
    // A LiteralSelector carries no path.
  };
  walk(selector, false);
  return refs;
}

/**
 * The selector a straddler evaluates against step outputs: an invocation
 * unit's `input`, a gate's `when`, or an escalation's `data`. loop/map
 * straddlers read outputs through other selector fields (`over`, `while`,
 * `carry`); those are not inspected here -- a residual noted in the
 * sentinel-guard contract, where the runtime still fails loud on a bad read.
 */
function straddlerOutputSelector(p: Primitive): Selector | undefined {
  if (p.kind === "step" || p.kind === "action" || p.kind === "childWorkflow") {
    return p.input;
  }
  if (p.kind === "gate") return p.when;
  if (p.kind === "escalation") return p.data;
  return undefined;
}

/**
 * Enforce the onFailure sentinel-guard contract. When a unit U with handler H
 * fails, U's normal after-dependents are pruned EXCEPT nodes reachable from H,
 * so a "straddler" -- live on both the failure path (reachable from H) and the
 * normal path (reachable from a normal dependent) -- reads U's own output,
 * which on failure is the sentinel `{ failed, stepId, error: { message } }`,
 * not U's success shape. A selector throws at the first missing segment, so a
 * straddler reading a deep, indexed, or project-narrowed path into U's success
 * shape breaks at runtime on the failure path. Only an agent `step` selecting
 * the WHOLE `steps.<U>.output` (to branch on `.failed` in its body) is safe; an
 * action/childWorkflow straddler, or any narrowed read, is rejected. The
 * handler itself must depend only on U and on U-independent nodes -- never on a
 * normal-side dependent the route prunes, whose skip sentinel it would read.
 */
function validateOnFailureStraddlers(steps: Record<string, Primitive>): void {
  for (const [unitId, primitive] of Object.entries(steps)) {
    const onFailure =
      primitive.kind === "step" ||
      primitive.kind === "action" ||
      primitive.kind === "childWorkflow"
        ? primitive.onFailure
        : undefined;
    if (onFailure === undefined) continue;
    const handlerId = onFailure;

    const unitDownstream = downstreamClosure(steps, [unitId]);
    for (const dep of steps[handlerId]?.after ?? []) {
      if (dep !== unitId && unitDownstream.has(dep)) {
        throw new Error(
          `${primitive.kind} ${unitId} onFailure handler ${handlerId} also ` +
            `depends on ${dep}, which the failure route prunes; the handler ` +
            `must depend only on ${unitId}`,
        );
      }
    }

    const failureLive = downstreamClosure(steps, [handlerId]);
    const normalDependents = Object.entries(steps)
      .filter(
        ([id, p]) => id !== handlerId && (p.after?.includes(unitId) ?? false),
      )
      .map(([id]) => id);
    const normalLive = downstreamClosure(steps, normalDependents);
    for (const straddlerId of failureLive) {
      if (!normalLive.has(straddlerId)) continue;
      const straddler = steps[straddlerId];
      if (straddler === undefined) continue;
      const selector = straddlerOutputSelector(straddler);
      if (selector === undefined) continue;
      const refs = unitOutputRefs(selector, unitId);
      if (refs.length === 0) continue;
      if (straddler.kind !== "step") {
        throw new Error(
          `${straddler.kind} ${straddlerId} straddles onFailure unit ` +
            `${unitId} and its handler ${handlerId} and reads ` +
            `steps.${unitId}.output; only an agent step can read the failure ` +
            `sentinel and branch on it`,
        );
      }
      for (const ref of refs) {
        if (!ref.whole || ref.underProject) {
          throw new Error(
            `step ${straddlerId} straddles onFailure unit ${unitId} and its ` +
              `handler ${handlerId} but reads a narrowed steps.${unitId}` +
              `.output; a straddler must select the whole output and branch ` +
              `on .failed`,
          );
        }
      }
    }
  }
}

/**
 * Reject an `onFailure` that sits where it cannot route. onFailure is honored
 * only on a member kind (`step`/`action`/`childWorkflow`) that is a direct
 * entry of a workflow root's `steps` record -- the top level, a childWorkflow
 * inline body, or an onTrigger section body, each validated as its own root.
 * Everywhere else a
 * set onFailure hashes into the approved surface yet never routes, so it is a
 * hash-bound no-op the author cannot see. The first arm rejects the field on
 * any node it does not belong on: the type blocks a constructor author from
 * setting it on a non-member, but a hand-assembled definition rides the open
 * wire schema -- the trust boundary every pass here guards. The second arm
 * rejects the `map` inner-step template, a member kind reached through the map
 * rather than as a root entry.
 */
function assertNoRoutableFailure(stepId: string, primitive: Primitive): void {
  if ("onFailure" in primitive && primitive.onFailure !== undefined) {
    throw new Error(
      `${primitive.kind} ${stepId} may not carry onFailure; onFailure is ` +
        `honored only on a step, action, or childWorkflow at a workflow root`,
    );
  }
  if (primitive.kind === "map" && primitive.step.onFailure !== undefined) {
    throw new Error(
      `map ${stepId} sets onFailure on its inner step; a map inner step is ` +
        `not a routing unit (a failed item is not routed in v1)`,
    );
  }
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
    if (
      primitive.kind === "step" ||
      primitive.kind === "action" ||
      primitive.kind === "childWorkflow"
    ) {
      if (primitive.onFailure !== undefined) {
        if (!ids.has(primitive.onFailure)) {
          throw new Error(
            `${primitive.kind} ${stepId} names onFailure ${primitive.onFailure} which is not a known step`,
          );
        }
        if (primitive.onFailure === stepId) {
          throw new Error(
            `${primitive.kind} ${stepId} cannot name itself as onFailure`,
          );
        }
        // onFailure routes only on a permanent failure, so the handler must
        // depend on the unit. Without `after: [unit]` it would be schedulable
        // from RunStarted and the handler would fire on every run.
        const target = steps[primitive.onFailure];
        if (
          target !== undefined &&
          !(target.after?.includes(stepId) ?? false)
        ) {
          throw new Error(
            `${primitive.kind} ${stepId} onFailure ${primitive.onFailure} must name ${stepId} in its after`,
          );
        }
      }
    } else {
      assertNoRoutableFailure(stepId, primitive);
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
      // A loop iteration is dependent (carry threads output into the next
      // input), so per-iteration "route and proceed" is incoherent -- a routed
      // body step yields no output to carry. Reject onFailure on any body
      // step, including a map's inner step reached through this walk.
      assertNoRoutableFailure(bodyStepId, bodyPrimitive);
    }
    validateLoopBody(primitive.body.steps, bodyDepth);
  }
}

/**
 * Validate an onTrigger section body as a full workflow root, and reject a
 * body that nests another onTrigger. The body runs as its own child run per
 * occurrence, so -- like a childWorkflow body -- it must be as valid as a
 * top-level definition; this pass re-enters `validateSteps` on the inline
 * body, so a malformed section body (dangling after, cycle, forbidden loop
 * body, misplaced onFailure) is rejected at the parent's authoring time. The
 * one restriction ADDED over a top-level root is the subscription-layer ban:
 * a section may not contain a section. Unlike a loop body, a section body may
 * sleep and spawn child workflows -- an onTrigger section IS the sanctioned
 * long-lived input loop.
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
    // The section body runs as its own child run, so validate it as a full
    // root (mirroring validateChildWorkflowBody). This reaches every
    // placement check -- including assertNoRoutableFailure -- so a misplaced
    // onFailure in a hand-assembled section body is rejected here too. The
    // nested-section ban above runs first so its specific message wins.
    validateSteps(primitive.body.inline.steps);
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
    if ("onFailure" in primitive && primitive.onFailure !== undefined) {
      // onFailure is a routing target like a loop's onExhausted: on a permanent
      // failure the unit routes to it instead of failing the run. Include the
      // edge so an onFailure naming an ancestor is rejected as a cycle. This
      // runs after validateAfterRefs has already rejected onFailure on any
      // non-member kind, so it fires only for step/action/childWorkflow.
      addEdge(stepId, primitive.onFailure);
    }
  }
  return adjacency;
}

/**
 * Reject a definition that lets two schedulable-concurrent `awaitSignal` steps
 * await the same signal name. `handleSignalReceived` is a FIFO single-consumer
 * scan keyed on the name, so two gates awaiting one name simultaneously make a
 * delivery's binding order-dependent, and on resume the consumed signal cannot
 * be durably re-bound to a specific gate. INTR-277 added a runtime fail-loud
 * guard for this topology; rejecting it here makes it unauthorable.
 *
 * A loop or inline onTrigger body's awaitSignal park relays up into the parent
 * run over the body's own author signal name, so a container step contributes
 * its body's await names into the parent's concurrency set (recursively through
 * nested loop / inline onTrigger bodies). A childWorkflow runs as a separate
 * run with its own signal namespace, so its awaiters never collide with the
 * parent's and are excluded.
 *
 * Two static-analysis limitations remain, both backstopped by the runtime
 * guard:
 *   - Conservative over-reject: two same-name awaiters on a gate's mutually
 *     exclusive then/else branches are rejected, because deciding they can
 *     never both be live is a dominator analysis whose permissive-direction
 *     error would re-admit the hazard.
 *   - Under-reject: a `{ ref }` onTrigger body is a separately-deployed asset
 *     not visible here, so an await name it relays is not collected; a
 *     collision with such a body is caught by the runtime guard, not here.
 */
function validateConcurrentAwaitSignalNames(
  steps: Record<string, Primitive>,
): void {
  const reaches = makeReachability(buildDependencyAdjacency(steps));
  const awaiters: { name: string; node: string; label: string }[] = [];
  for (const [stepId, primitive] of Object.entries(steps)) {
    if (primitive.kind === "awaitSignal") {
      awaiters.push({ name: primitive.name, node: stepId, label: stepId });
      continue;
    }
    for (const relayed of collectRelayedAwaitSignalNames(primitive)) {
      awaiters.push({
        name: relayed.name,
        node: stepId,
        label: `${stepId} (body awaitSignal ${relayed.bodyStepId})`,
      });
    }
  }
  for (let i = 0; i < awaiters.length; i++) {
    const a = awaiters[i];
    if (a === undefined) continue;
    for (let j = i + 1; j < awaiters.length; j++) {
      const b = awaiters[j];
      if (b === undefined) continue;
      if (a.name !== b.name || a.node === b.node) continue;
      if (!reaches(a.node, b.node) && !reaches(b.node, a.node)) {
        throw new Error(
          `awaitSignal steps ${a.label} and ${b.label} can concurrently ` +
            `await signal name ${a.name}; a signal name may have at most ` +
            `one concurrent awaiter`,
        );
      }
    }
  }
}

/**
 * Collect the author signal names a loop or inline onTrigger body relays up to
 * the parent run, recursing through nested loop and inline onTrigger bodies. A
 * flat `steps` record keys gate branches and onTimeout targets as siblings, so
 * enumerating the record covers them without a graph walk. A childWorkflow body
 * runs in a separate signal namespace and is not relayed, so it is skipped.
 */
function collectRelayedAwaitSignalNames(
  primitive: Primitive,
): { name: string; bodyStepId: string }[] {
  let body: Record<string, Primitive> | undefined;
  if (primitive.kind === "loop") {
    body = primitive.body.steps;
  } else if (primitive.kind === "onTrigger" && "inline" in primitive.body) {
    body = primitive.body.inline.steps;
  }
  if (body === undefined) return [];
  const names: { name: string; bodyStepId: string }[] = [];
  for (const [bodyStepId, bodyPrimitive] of Object.entries(body)) {
    if (bodyPrimitive.kind === "awaitSignal") {
      names.push({ name: bodyPrimitive.name, bodyStepId });
    }
    for (const nested of collectRelayedAwaitSignalNames(bodyPrimitive)) {
      names.push({
        name: nested.name,
        bodyStepId: `${bodyStepId}/${nested.bodyStepId}`,
      });
    }
  }
  return names;
}

/**
 * A memoized descendant-reachability predicate over the dependency adjacency.
 * The graph is acyclic (validateAcyclic runs first), so the walk terminates.
 */
function makeReachability(
  adjacency: Map<string, string[]>,
): (from: string, to: string) => boolean {
  const descendants = new Map<string, Set<string>>();
  const descendantsOf = (start: string): Set<string> => {
    const cached = descendants.get(start);
    if (cached !== undefined) return cached;
    const seen = new Set<string>();
    const stack = [...(adjacency.get(start) ?? [])];
    while (stack.length > 0) {
      const node = stack.pop();
      if (node === undefined || seen.has(node)) continue;
      seen.add(node);
      for (const next of adjacency.get(node) ?? []) {
        stack.push(next);
      }
    }
    descendants.set(start, seen);
    return seen;
  };
  return (from, to) => descendantsOf(from).has(to);
}

/**
 * The transitive closure of `starts` following `after`-edges forward: every
 * step that (transitively) depends on a start. This is the liveness set the
 * runtime's branch prune spares -- the same graph the onFailure straddler
 * guard reasons over, so both share this function rather than drift. Follows
 * `after` edges ONLY, not gate/loop/onFailure routing edges.
 */
export function downstreamClosure(
  steps: Record<string, Primitive>,
  starts: readonly string[],
): Set<string> {
  const visited = new Set<string>();
  const queue: string[] = starts.filter((id) => id in steps);
  while (queue.length > 0) {
    const id = queue.shift();
    if (id === undefined) break;
    if (visited.has(id)) continue;
    visited.add(id);
    for (const [otherId, primitive] of Object.entries(steps)) {
      const after = primitive.after;
      if (after === undefined) continue;
      if (after.includes(id) && !visited.has(otherId)) {
        queue.push(otherId);
      }
    }
  }
  return visited;
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
    ...(definition.sidecarPlacement !== undefined
      ? { sidecarPlacement: definition.sidecarPlacement }
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
