// Deploy-time capability walk.
//
// `walkCapabilities(workflow, registry)` is the structural lift of
// `getRequiredEnvKeys` from `@intx/agent`: the env-validation helper
// walks each step's `AgentDefinition` to collect the env-key surface
// the agent declares at instantiation; this walk traverses the same
// shape and emits the grant-shape strings the deploy-time operator-
// approval gate consumes.
//
// The two walks must stay structurally aligned -- a step whose
// `AgentDefinition` would pass `validateEnv` at runtime must produce
// the matching grant set here. The parity test in
// `capability-walk.test.ts` is the load-bearing structural-identity
// check for that claim.
//
// Grant shapes (v1):
//   - `tool:<factory.id>`            -- every `toolFactories[i].id`
//   - `director:<ref.id>`            -- resolved director ref
//   - `capability:<name>`            -- `AgentDefinition.capabilities`
//   - `inference.source:<id>`        -- `inference.sources[i].provider`
//                                        with `<provider>:<model>`-style
//                                        id (matches the existing
//                                        InferencePreference shape)
//   - `mail.address:<address>`       -- every workflow trigger of type
//                                        `"mail"` (the deployment's
//                                        right to register and receive)
//   - `mail.send:<domain>`           -- the trigger address's domain
//                                        (the deployment's right to
//                                        send mail as that domain)
//
// Wildcard semantics (e.g. `tool:@vendor/foo/*`) are intentionally
// deferred: v1 ships explicit per-tool approvals so the operator UX
// matches what `getRequiredEnvKeys` enforces today. When a deployment
// accumulates enough tool factories that explicit approval is
// unergonomic, the wildcard shape lands as an additive refinement.
//
// Director resolution: the walk calls `effectiveDirectorRef` so the
// absent-director normalization stays identical between this walk and
// `getRequiredEnvKeys`. Director factories loaded from pinned tool
// packages via `interchange.directors` are folded into the registry by
// the caller; this module does not synthesize that registry itself
// because the loader is the layer that owns package materialization.
// An unresolvable director surfaces on `unresolvedDirectors` rather
// than raising -- the orchestrator translates that into a deploy-time
// `"unresolvable director"` failure when it wires this output into
// approval flow.

import type { AgentDefinition, BaseEnv, DirectorRegistry } from "@intx/agent";
import { effectiveDirectorRef, UnknownDirectorIdError } from "@intx/agent";
import type { WorkflowDefinition } from "@intx/workflow/definition";

/**
 * Grant declarations produced for a single workflow step. `grants`
 * carries the grant-shape strings the operator-approval gate consumes.
 * Frozen so downstream consumers cannot mutate the walk output in
 * place.
 */
export interface GrantDeclarations {
  readonly grants: readonly string[];
}

/**
 * The capability walk's result. `perStep` keys are workflow step ids;
 * `unresolvedDirectors` lists every director id the supplied registry
 * could not resolve across the whole walk, so the orchestrator can
 * surface a single deploy-time failure rather than tearing down per
 * step.
 *
 * `unresolvedDirectors` is a readonly array (not an optional) so
 * callers must inspect it explicitly; an optional that resolves to
 * `undefined` is too easy to ignore at the approval-gate site.
 */
export interface CapabilityWalkResult {
  readonly perStep: ReadonlyMap<string, GrantDeclarations>;
  readonly unresolvedDirectors: readonly string[];
}

/**
 * Walk a workflow definition and produce per-step grant declarations.
 *
 * The walk visits every step's agent definition once; trigger-derived
 * grants (`mail.address:` / `mail.send:`) are attached to every step
 * because the workflow's mail-receive and mail-send authority is a
 * deployment-wide property -- any step can be the one whose run
 * consumes inbound mail or generates a reply.
 */
export function walkCapabilities(
  workflow: WorkflowDefinition,
  registry: DirectorRegistry,
): CapabilityWalkResult {
  const triggerGrants = collectTriggerGrants(workflow);
  const unresolved = new Set<string>();
  const perStep = new Map<string, GrantDeclarations>();

  for (const stepId of workflow.stepOrder) {
    const primitive = workflow.steps[stepId];
    if (primitive === undefined) {
      throw new Error(
        `capability walk: step ${stepId} listed in stepOrder is missing from steps`,
      );
    }
    const agent = extractAgent(primitive);
    if (agent === null) {
      // Non-agent primitives carry no agent grants. An `action`
      // additionally contributes its declared `effect:<cap>` grants, and
      // a `loop` contributes the union of its body's grants (so the
      // approval gate sees every agent/action the loop can run); every
      // other non-agent primitive gets only the trigger-derived grants.
      const nonAgentGrants = [
        ...collectActionGrants(primitive),
        ...collectLoopBodyGrants(primitive, registry, unresolved),
      ];
      const merged = mergeGrants(nonAgentGrants, triggerGrants);
      perStep.set(stepId, Object.freeze({ grants: merged }));
      continue;
    }
    const agentGrants = collectAgentGrants(agent, registry, unresolved);
    const merged = mergeGrants(agentGrants, triggerGrants);
    perStep.set(stepId, Object.freeze({ grants: merged }));
  }

  return Object.freeze({
    perStep,
    unresolvedDirectors: Object.freeze([...unresolved]),
  });
}

/**
 * Project a primitive to its agent definition when it carries one.
 * `step` and `map` are the agent-carrying shapes today; other
 * primitives have no agent, so they receive only the trigger-derived
 * grant set.
 */
function extractAgent(
  primitive: WorkflowDefinition["steps"][string],
): AgentDefinition<BaseEnv> | null {
  if (primitive.kind === "step") {
    return primitive.agent;
  }
  if (primitive.kind === "map") {
    return primitive.step.agent;
  }
  return null;
}

/**
 * Collect an action's `effect:<cap>` grants from its declared `requires`
 * set. Non-action primitives contribute none.
 */
function collectActionGrants(
  primitive: WorkflowDefinition["steps"][string],
): string[] {
  if (primitive.kind !== "action") {
    return [];
  }
  const grants = new Set<string>();
  for (const capability of primitive.effect?.requires ?? []) {
    grants.add(`effect:${capability}`);
  }
  return [...grants];
}

/**
 * Collect the union of a loop body's grants (agent grants for its step /
 * map steps, effect grants for its action steps) so the loop node's
 * approval covers every agent and effect the loop can run. The body-ban
 * forbids a nested loop, so this does not recurse further.
 */
function collectLoopBodyGrants(
  primitive: WorkflowDefinition["steps"][string],
  registry: DirectorRegistry,
  unresolved: Set<string>,
): string[] {
  if (primitive.kind !== "loop") {
    return [];
  }
  const grants = new Set<string>();
  for (const bodyStepId of primitive.body.stepOrder) {
    const bodyPrimitive = primitive.body.steps[bodyStepId];
    if (bodyPrimitive === undefined) {
      throw new Error(
        `capability walk: loop body step ${bodyStepId} listed in stepOrder is missing from steps`,
      );
    }
    const bodyAgent = extractAgent(bodyPrimitive);
    if (bodyAgent !== null) {
      for (const grant of collectAgentGrants(bodyAgent, registry, unresolved)) {
        grants.add(grant);
      }
    }
    for (const grant of collectActionGrants(bodyPrimitive)) {
      grants.add(grant);
    }
  }
  return [...grants];
}

function collectAgentGrants(
  agent: AgentDefinition<BaseEnv>,
  registry: DirectorRegistry,
  unresolved: Set<string>,
): string[] {
  const grants = new Set<string>();
  for (const factory of agent.toolFactories) {
    grants.add(`tool:${factory.id}`);
  }
  for (const capability of agent.capabilities) {
    grants.add(`capability:${capability}`);
  }
  for (const source of agent.inference.sources) {
    grants.add(`inference.source:${source.provider}:${source.model}`);
  }
  const ref = effectiveDirectorRef(agent, registry);
  try {
    const directorFactory = registry.resolve(ref);
    grants.add(`director:${directorFactory.id}`);
  } catch (cause) {
    if (!(cause instanceof UnknownDirectorIdError)) throw cause;
    unresolved.add(ref.id);
  }
  return [...grants];
}

function collectTriggerGrants(workflow: WorkflowDefinition): string[] {
  const grants = new Set<string>();
  for (const trigger of workflow.triggers) {
    if (trigger.type !== "mail") continue;
    grants.add(`mail.address:${trigger.to}`);
    const domain = extractDomain(trigger.to);
    if (domain !== null) {
      grants.add(`mail.send:${domain}`);
    }
  }
  return [...grants];
}

function extractDomain(address: string): string | null {
  const at = address.lastIndexOf("@");
  if (at < 0 || at === address.length - 1) {
    return null;
  }
  return address.slice(at + 1);
}

function mergeGrants(
  agentGrants: readonly string[],
  triggerGrants: readonly string[],
): readonly string[] {
  const merged = new Set<string>(agentGrants);
  for (const grant of triggerGrants) {
    merged.add(grant);
  }
  return Object.freeze([...merged]);
}
