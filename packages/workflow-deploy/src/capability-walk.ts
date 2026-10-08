// Deploy-time capability walk.
//
// `walkCapabilities(workflow, registry)` is the structural lift of
// `getRequiredEnvKeys` from `@intx/agent`: the env-validation helper walks
// each step's `AgentDefinition` to collect the env-key surface the agent
// declares at instantiation; this walk traverses the same shape and emits the
// grant-shape strings the deploy-time operator-approval gate consumes. The two
// walks must stay structurally aligned; the parity test in
// `capability-walk.test.ts` is the load-bearing check for that claim.
//
// Grant shapes (v1):
//   - `tool:<def.name>`              -- every tool name a factory declares
//                                        (per definition, NOT per factory id)
//   - `director:<ref.id>`            -- resolved director ref
//   - `capability:<name>`            -- `AgentDefinition.capabilities`
//   - `inference.source:<id>`        -- `inference.sources[i]` provider:model
//   - `mail.address:<address>`       -- every mail workflow trigger
//   - `mail.send:<domain>`           -- the trigger address's domain
//
// Wildcard semantics (e.g. `tool:@vendor/foo/*`) are intentionally deferred:
// v1 ships explicit per-tool approvals so the operator UX matches what
// `getRequiredEnvKeys` enforces today.
//
// Director resolution uses `effectiveDirectorRef` so the absent-director
// normalization stays identical to `getRequiredEnvKeys`. An unresolvable
// director surfaces on `unresolvedDirectors` rather than raising.

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
import { toolConsumer } from "@intx/authz";
import type { GrantEffect } from "@intx/types";
import {
  EXECUTABLE_STEP_DESCENT,
  walkNestedWorkflowSteps,
} from "@intx/workflow/definition";
import type { WorkflowDefinition } from "@intx/workflow/definition";

/**
 * Grant declarations produced for a single workflow step. `grantEffects` maps
 * each TOOL grant string to the effect its static declaration requested:
 * `"ask"` for a tool gated behind per-invocation approval, `"allow"`
 * otherwise. Covers TOOL grants only; director/capability/inference.source/
 * mail.* grants live in `grants` and are absent from `grantEffects`. Frozen
 * so downstream consumers cannot mutate the walk output in place.
 */
export interface GrantDeclarations {
  readonly grants: readonly string[];
  readonly grantEffects: ReadonlyMap<string, GrantEffect>;
}

/**
 * The capability walk's result: per-step grant declarations keyed by
 * workflow step id, plus every director id the supplied registry could not
 * resolve across the whole walk, so the deploy flow can surface a single
 * deploy-time failure. `unresolvedDirectors` is a required array (not
 * optional) so callers must inspect it explicitly.
 */
export interface CapabilityWalkResult {
  readonly perStep: ReadonlyMap<string, GrantDeclarations>;
  readonly unresolvedDirectors: readonly string[];
}

/**
 * Static tool declarations a plugin package contributes, keyed by the
 * plugin-package name an agent names in `AgentDefinition.plugins`. A plugin
 * contributes NO agent-visible tool factory (its tools reach the agent
 * through `env.plugins` at run time), so the walk cannot read its grant
 * surface off the definition alone; the caller loads each declared plugin's
 * static `definitions` and threads them here. Empty when the walked closure
 * declares no plugin package.
 */
export type PluginToolDefinitions = ReadonlyMap<
  string,
  readonly ToolDeclaration[]
>;

/**
 * Mutable accumulator threaded through the collectors while a single step is
 * walked. Frozen into a `GrantDeclarations` once the step is fully walked.
 */
interface GrantSet {
  readonly grants: Set<string>;
  readonly effects: Map<string, GrantEffect>;
  readonly credentialConsumers: Set<string>;
}

/**
 * Fold the deployment-wide trigger grants into a step's collected
 * agent/action/loop grants and freeze the result. Trigger grants are never
 * TOOL grants, so they add to `grants` without touching `effects`.
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
 * Trigger-derived grants (`mail.address:` / `mail.send:`) are attached to
 * every step because mail-receive/send authority is a deployment-wide
 * property: any step can be the one whose run consumes inbound mail or
 * generates a reply.
 */
export function walkCapabilities(
  workflow: WorkflowDefinition,
  registry: DirectorRegistry,
  pluginDefs: PluginToolDefinitions = new Map(),
): CapabilityWalkResult {
  const walked = walkDefinition(workflow, registry, pluginDefs);
  return Object.freeze({
    perStep: walked.perStep,
    unresolvedDirectors: Object.freeze([...walked.unresolved]),
  });
}

/**
 * Consumer identities (`toolConsumer(factory.id)`) for every tool factory
 * the definition instantiates, including factories that appear only inside
 * an inline nested body. This is the credential-cap input. It is not part
 * of `CapabilityWalkResult`: those strings are not operator-facing grants.
 * Plugin names are absent. A factory with an empty `definitions` array is
 * still present, because the source arm keys Gate 2 on the factory id.
 */
export function collectCredentialConsumers(
  workflow: WorkflowDefinition,
  registry: DirectorRegistry,
  pluginDefs: PluginToolDefinitions = new Map(),
): ReadonlySet<string> {
  return walkDefinition(workflow, registry, pluginDefs).credentialConsumers;
}

function walkDefinition(
  workflow: WorkflowDefinition,
  registry: DirectorRegistry,
  pluginDefs: PluginToolDefinitions,
): {
  readonly perStep: Map<string, GrantDeclarations>;
  readonly unresolved: Set<string>;
  readonly credentialConsumers: ReadonlySet<string>;
} {
  const triggerGrants = collectTriggerGrants(workflow);
  const unresolved = new Set<string>();
  const perStep = new Map<string, GrantDeclarations>();
  const credentialConsumers = new Set<string>();

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
    // The consumer set is the same object the nested visit writes into, then
    // unioned here, so a factory that exists only on an inline body survives.
    const collected = emptyGrantSet();
    collectPrimitiveGrants(
      primitive,
      registry,
      pluginDefs,
      unresolved,
      collected,
    );
    perStep.set(stepId, freezeDeclarations(collected, triggerGrants));
    for (const consumer of collected.credentialConsumers) {
      credentialConsumers.add(consumer);
    }
  }

  return { perStep, unresolved, credentialConsumers };
}

function emptyGrantSet(): GrantSet {
  return {
    grants: new Set<string>(),
    effects: new Map(),
    credentialConsumers: new Set<string>(),
  };
}

/**
 * Project a primitive to its agent definition when it carries one. Only
 * `step` and `map` are agent-carrying shapes today; other primitives
 * receive only the trigger-derived grant set.
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

/** Collect an action's `effect:<cap>` grants from its declared `requires`. */
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
 * Union a single primitive's grants into `collected`: its own grants plus,
 * for a body-bearing primitive, the grants of every step of its nested body.
 * A loop, an inline onTrigger section, and an inline childWorkflow each run
 * their body per the deployment, so the operator must approve everything the
 * body can run -- exactly `EXECUTABLE_STEP_DESCENT`. A `{ ref }` body is a
 * separately-declared asset whose grants were folded in from its own inline
 * form, so the descent skips it.
 *
 * Duplicate-name handling is scoped per body step: a duplicate within a
 * single agent throws, but two DIFFERENT body steps that each mint the same
 * `tool:<name>` are distinct runtime agents, so the union across body steps
 * is not a duplicate-name error.
 */
function collectPrimitiveGrants(
  primitive: WorkflowDefinition["steps"][string],
  registry: DirectorRegistry,
  pluginDefs: PluginToolDefinitions,
  unresolved: Set<string>,
  collected: GrantSet,
): void {
  collectOwnGrants(primitive, registry, pluginDefs, unresolved, collected);
  walkNestedWorkflowSteps({
    primitive,
    descent: EXECUTABLE_STEP_DESCENT,
    context: "capability walk: body ",
    visit: ({ step }) => {
      collectOwnGrants(step, registry, pluginDefs, unresolved, collected);
    },
  });
}

/**
 * Union the grants a single primitive declares in its own right -- its
 * agent's grants and its action's `effect:<cap>` grants -- into `collected`,
 * without descending into anything it nests.
 */
function collectOwnGrants(
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
}

function collectAgentGrants(
  agent: AgentDefinition<BaseEnv>,
  registry: DirectorRegistry,
  pluginDefs: PluginToolDefinitions,
  unresolved: Set<string>,
  collected: GrantSet,
): void {
  // Track final tool names so a collision across the agent's factories or
  // plugins throws here rather than surfacing as a runtime DuplicateToolError
  // after the deploy has already gone out.
  const seenToolNames = new Set<string>();
  for (const factory of agent.toolFactories) {
    // Gate 2 keys the consumer on the factory id, including a factory whose
    // `definitions` array is empty and therefore emits no `tool:` grant.
    collected.credentialConsumers.add(toolConsumer(factory.id));
    // A repeated name within one factory's declarations is a declaration bug;
    // the runtime would collapse the two into one dispatch entry.
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
  // Plugin-contributed tools never appear in `agent.toolFactories` (they reach
  // the agent through `env.plugins`), so the caller-supplied static
  // `definitions` carry the names to authorize. A plugin tool sharing a name
  // with a factory or another plugin is a real collision -- both dispatch
  // under the same bare runtime name -- so it flows through the same
  // `seenToolNames` guard.
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
 * Add a tool's `tool:<name>` grant and its authorization effect, applying
 * the ask-wins merge. `collected` is one GrantSet shared across every body
 * step of a loop, so two body steps declaring the same bare tool name write
 * the same key; a plain overwrite would let a later unmarked declaration
 * downgrade an earlier `ask` to `allow`, so keep `ask` if either side asks.
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
 * Thrown when the walk finds two tool definitions that mint the same final
 * `tool:<name>` grant within a single agent. Mirrors the runtime
 * `DuplicateToolError`, surfacing the collision at deploy time so a broken
 * agent fails the walk instead of deploying and then crashing when
 * `createAgent` builds it.
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
