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
//   - `tool:<def.name>`              -- every tool name a factory
//                                        declares via `definitions[i].name`
//                                        (per definition, NOT per factory
//                                        id, so the grant matches the
//                                        runtime `tool:<call.name>` the
//                                        gate authorizes against)
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
// than raising -- the deploy flow translates that into a deploy-time
// `"unresolvable director"` failure when it wires this output into
// approval flow.

import type {
  AgentDefinition,
  BaseEnv,
  DirectorRegistry,
  ToolDeclaration,
} from "@intx/agent";
import {
  effectiveDirectorRef,
  toolApprovalEffect,
  UnknownDirectorIdError,
} from "@intx/agent";
import type { GrantEffect } from "@intx/types";
import type { WorkflowDefinition } from "@intx/workflow/definition";

/**
 * Grant declarations produced for a single workflow step. `grants`
 * carries the grant-shape strings the operator-approval gate consumes.
 *
 * `grantEffects` maps each TOOL grant string (`tool:<def.name>`) to the
 * effect the tool's static declaration requested: `"ask"` for a tool
 * gated behind per-invocation approval, `"allow"` otherwise. It covers
 * TOOL grants only -- director/capability/inference.source/mail.* grants
 * live in `grants` and are absent from `grantEffects`.
 *
 * Frozen so downstream consumers cannot mutate the walk output in
 * place.
 */
export interface GrantDeclarations {
  readonly grants: readonly string[];
  readonly grantEffects: ReadonlyMap<string, GrantEffect>;
}

/**
 * The capability walk's result. `perStep` keys are workflow step ids;
 * `unresolvedDirectors` lists every director id the supplied registry
 * could not resolve across the whole walk, so the deploy flow can
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
 * Static tool declarations a plugin package contributes, keyed by the
 * plugin-package name an agent names in `AgentDefinition.plugins`. A
 * plugin package contributes NO agent-visible tool factory (its tools reach
 * the agent through `env.plugins` at run time), so the walk cannot read the
 * plugin's tool grant surface off the definition alone. The caller (the
 * probe, over the materialized closure) loads each declared plugin's static
 * `definitions` and threads them here so the walk emits `tool:<name>` grants
 * for plugin-contributed tools alongside factory-contributed ones. Empty
 * when the walked closure declares no plugin package.
 */
export type PluginToolDefinitions = ReadonlyMap<
  string,
  readonly ToolDeclaration[]
>;

/**
 * Mutable accumulator threaded through the collectors while a single
 * step is walked. `grants` is the deduplicated grant-string set;
 * `effects` maps each TOOL grant string to its effect. The two are
 * frozen into a `GrantDeclarations` once the step is fully walked.
 */
interface GrantSet {
  readonly grants: Set<string>;
  readonly effects: Map<string, GrantEffect>;
}

/**
 * Fold the deployment-wide trigger grants into a step's collected
 * agent/action/loop grants and freeze the result. Trigger grants are
 * never TOOL grants, so they add to `grants` without touching `effects`.
 */
function freezeDeclarations(
  collected: GrantSet,
  triggerGrants: readonly string[],
): GrantDeclarations {
  const grants = new Set<string>(collected.grants);
  for (const grant of triggerGrants) {
    grants.add(grant);
  }
  return Object.freeze({
    grants: Object.freeze([...grants]),
    grantEffects: Object.freeze(new Map(collected.effects)),
  });
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
  pluginDefs: PluginToolDefinitions = new Map(),
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
    // Every top-level step gets a fresh grant set; `collectPrimitiveGrants`
    // routes the step and any nested bodies it carries through one dispatch,
    // so an approval covers every agent, action, and effect the step can run.
    const collected: GrantSet = {
      grants: new Set<string>(),
      effects: new Map(),
    };
    collectPrimitiveGrants(
      primitive,
      registry,
      pluginDefs,
      unresolved,
      collected,
    );
    perStep.set(stepId, freezeDeclarations(collected, triggerGrants));
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
 * Union a single primitive's grants into `collected`: its agent grants (when
 * it carries an agent), its action `effect:<cap>` grants, and -- for a
 * body-bearing primitive -- the grants of every step of its nested body. A
 * loop, an inline onTrigger section, and an inline childWorkflow each run their
 * body per the deployment, so the operator must approve everything the body can
 * run. The walk descends into the authored `{ inline }` form; a `{ ref }` body
 * is a separately-declared asset whose grants were folded in from its own
 * inline form, so it is skipped here.
 *
 * The nesting switch is EXHAUSTIVE: a newly-added primitive kind fails the
 * `never` assignment below at compile time, forcing the walk to decide how to
 * treat it rather than silently dropping a nested closure's grants. A miss here
 * is a silent, fail-open authorization gap because `director:` is not re-gated
 * at runtime -- so the compiler, not a remembered call site, owns coverage.
 */
function collectPrimitiveGrants(
  primitive: WorkflowDefinition["steps"][string],
  registry: DirectorRegistry,
  pluginDefs: PluginToolDefinitions,
  unresolved: Set<string>,
  collected: GrantSet,
): void {
  const agent = extractAgent(primitive);
  if (agent !== null) {
    collectAgentGrants(agent, registry, pluginDefs, unresolved, collected);
  }
  for (const grant of collectActionGrants(primitive)) {
    collected.grants.add(grant);
  }
  switch (primitive.kind) {
    case "loop":
      collectBodyGrants(
        primitive.body,
        registry,
        pluginDefs,
        unresolved,
        collected,
      );
      return;
    case "onTrigger":
      if ("inline" in primitive.body) {
        collectBodyGrants(
          primitive.body.inline,
          registry,
          pluginDefs,
          unresolved,
          collected,
        );
      }
      return;
    case "childWorkflow":
      if ("inline" in primitive.definition) {
        collectBodyGrants(
          primitive.definition.inline,
          registry,
          pluginDefs,
          unresolved,
          collected,
        );
      }
      return;
    case "step":
    case "map":
    case "action":
    case "gate":
    case "escalation":
    case "awaitSignal":
    case "sleep":
      // Leaf primitives: no nested body to descend into.
      return;
    default: {
      const exhaustive: never = primitive;
      throw new Error(
        `capability walk: unhandled primitive kind ${JSON.stringify(
          (exhaustive as { kind: string }).kind,
        )}`,
      );
    }
  }
}

/**
 * Walk every step of a nested body (a loop body, an inline onTrigger section
 * body, or an inline childWorkflow definition) and union its grants into
 * `collected`. Each step routes through `collectPrimitiveGrants`, so a body
 * that itself nests another body is covered by the same single dispatch and
 * the loop-body ban (a validator-owned invariant) simply means the recursion
 * never encounters a nesting primitive inside a loop body.
 *
 * Duplicate-name handling is scoped per body step: `collectAgentGrants` throws
 * on a duplicate within a single agent, but two DIFFERENT body steps that each
 * mint the same `tool:<name>` are distinct runtime agents (the runtime builds
 * one agent per step), so the union across body steps is not a duplicate-name
 * error.
 */
function collectBodyGrants(
  body: WorkflowDefinition,
  registry: DirectorRegistry,
  pluginDefs: PluginToolDefinitions,
  unresolved: Set<string>,
  collected: GrantSet,
): void {
  for (const bodyStepId of body.stepOrder) {
    const bodyPrimitive = body.steps[bodyStepId];
    if (bodyPrimitive === undefined) {
      throw new Error(
        `capability walk: body step ${bodyStepId} listed in stepOrder is missing from steps`,
      );
    }
    collectPrimitiveGrants(
      bodyPrimitive,
      registry,
      pluginDefs,
      unresolved,
      collected,
    );
  }
}

function collectAgentGrants(
  agent: AgentDefinition<BaseEnv>,
  registry: DirectorRegistry,
  pluginDefs: PluginToolDefinitions,
  unresolved: Set<string>,
  collected: GrantSet,
): void {
  // Track the final tool names this agent has minted so a collision
  // across the agent's factories throws here rather than surfacing as a
  // runtime DuplicateToolError (see resolveTools in
  // packages/agent/src/agent.ts) after the deploy has already gone out.
  const seenToolNames = new Set<string>();
  for (const factory of agent.toolFactories) {
    // Intra-factory: a repeated name within one factory's declarations
    // is a declaration bug; the runtime would collapse the two into one
    // dispatch entry, leaving one tool unreachable. Surface it at walk
    // time.
    const seenInFactory = new Set<string>();
    for (const definition of factory.definitions) {
      if (seenInFactory.has(definition.name)) {
        throw new DuplicateWalkToolError(definition.name, factory.id);
      }
      seenInFactory.add(definition.name);
      if (seenToolNames.has(definition.name)) {
        throw new DuplicateWalkToolError(definition.name, factory.id);
      }
      seenToolNames.add(definition.name);
      emitToolGrant(definition, collected);
    }
  }
  // Plugin-contributed tools. A plugin package (`agent.plugins`) exposes
  // its tools through `env.plugins` at run time, so they never appear in
  // `agent.toolFactories`; the loaded static `definitions` supplied by the
  // caller carry the tool names to authorize. A plugin tool sharing a name
  // with a factory tool (or another plugin's tool) is a real collision --
  // both dispatch under the same bare runtime name -- so it flows through
  // the SAME `seenToolNames` guard.
  for (const pluginName of agent.plugins ?? []) {
    const definitions = pluginDefs.get(pluginName);
    if (definitions === undefined) {
      throw new Error(
        `capability walk: agent ${JSON.stringify(agent.id)} declares plugin ${JSON.stringify(pluginName)} but no static tool definitions were loaded for it; a declared plugin whose grant surface cannot be resolved must fail closed`,
      );
    }
    for (const definition of definitions) {
      if (seenToolNames.has(definition.name)) {
        throw new DuplicateWalkToolError(definition.name, pluginName);
      }
      seenToolNames.add(definition.name);
      emitToolGrant(definition, collected);
    }
  }
  for (const capability of agent.capabilities) {
    collected.grants.add(`capability:${capability}`);
  }
  for (const source of agent.inference.sources) {
    collected.grants.add(`inference.source:${source.provider}:${source.model}`);
  }
  const ref = effectiveDirectorRef(agent, registry);
  try {
    const directorFactory = registry.resolve(ref);
    collected.grants.add(`director:${directorFactory.id}`);
  } catch (cause) {
    if (!(cause instanceof UnknownDirectorIdError)) throw cause;
    unresolved.add(ref.id);
  }
}

/**
 * Add a tool's `tool:<name>` grant and its authorization effect to the
 * collected set, applying the ask-wins merge. `collected` is one GrantSet
 * shared across every body step of a loop, so two body steps declaring the
 * same bare tool name write the same `tool:<name>` key; a plain overwrite
 * would let a later unmarked declaration downgrade an earlier `ask` to
 * `allow`, so keep `ask` if either the existing or incoming effect asks.
 * Shared by the factory-declared and plugin-contributed tool paths so both
 * derive the effect through the one canonical `toolApprovalEffect` mapping.
 */
function emitToolGrant(definition: ToolDeclaration, collected: GrantSet): void {
  const grant = `tool:${definition.name}`;
  collected.grants.add(grant);
  const incoming = toolApprovalEffect(definition);
  const existing = collected.effects.get(grant);
  collected.effects.set(
    grant,
    existing === "ask" || incoming === "ask" ? "ask" : incoming,
  );
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

/**
 * Thrown when the walk finds two tool definitions that mint the same
 * final `tool:<name>` grant within a single agent -- whether from a
 * repeated name inside one factory or a collision across two of the
 * agent's factories. Mirrors the runtime `DuplicateToolError` (see
 * `resolveTools` in `packages/agent/src/agent.ts`), surfacing the
 * collision at deploy time so a broken agent fails the walk instead of
 * deploying and then crashing when `createAgent` builds it.
 */
export class DuplicateWalkToolError extends Error {
  readonly toolName: string;
  readonly factoryId: string;

  constructor(toolName: string, factoryId: string) {
    super(
      `capability walk: duplicate tool name ${JSON.stringify(toolName)} ` +
        `(factory ${JSON.stringify(factoryId)})`,
    );
    this.name = "DuplicateWalkToolError";
    this.toolName = toolName;
    this.factoryId = factoryId;
  }
}
