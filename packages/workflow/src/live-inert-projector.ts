// Live -> inert needs-surface projector.
//
// This is the one place that reifies a live `WorkflowDefinition` -- whose
// steps carry functions (agent tool factories), arktype `Type` objects
// (state schemas), and other non-JSON values -- into pure plain data that
// survives the child->hub process boundary without silently losing the
// tool grant surface. The projector output is what
// `computeWireDefinitionHash` hashes, so any loss here would corrupt the
// content handle the deploy gate, the install-time probe, and re-verify
// all compare by byte equality.
//
// The failure mode this defends against: a naive `JSON.stringify` of a
// live tool factory yields `null` (a factory is a function, and JSON drops
// functions), which erases the entire grant surface while leaving a
// non-empty-looking `toolFactories: [null]`. The projector reads the
// static metadata (`.id`/`.requires`/`.definitions`) off each factory
// function directly, and fails loud if a factory is not a function or is
// missing that metadata -- a reified-to-`null` factory is a corrupted
// grant surface, never a valid empty grant set.
//
// Model sources are canonicalized to their `(provider, model)` identity
// and nothing else. Per-source `parameters` -- the provider-native knob
// bag that may carry credential-adjacent material -- are excluded from the
// projection, and therefore from the hashed preimage, so a credential or
// parameter rotation cannot trip re-verify downstream.
//
// An unreifiable or unknown step shape THROWS. It never projects to
// `null`: a step whose kind is outside the closed primitive set is a
// hydrated-or-tampered definition and must fail loud.

import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type {
  AgentDefinition,
  AnnotatedToolFactory,
  DirectorRef,
  InferencePreference,
  ToolDeclaration,
} from "@intx/agent";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import type { CredentialBinding } from "@intx/types";
import type {
  ActionPrimitive,
  AwaitSignalPrimitive,
  ChildWorkflowPrimitive,
  DrainBehavior,
  EscalationPrimitive,
  GatePrimitive,
  LoopPrimitive,
  MapPrimitive,
  OnTriggerPrimitive,
  Primitive,
  RetryPolicy,
  Selector,
  SleepPrimitive,
  StateSchema,
  StepPrimitive,
  Trigger,
  WorkflowDefinition,
} from "./definition/index";

// ---------------------------------------------------------------------------
// Inert projection shapes
// ---------------------------------------------------------------------------

/** Plain-data mirror of a `ToolDeclaration`: the tool's name and its
 * optional approval mark. */
export interface InertToolDeclaration {
  readonly name: string;
  readonly approval?: "ask";
}

/** Plain-data mirror of an `AnnotatedToolFactory`'s static grant surface:
 * its namespaced id, the env keys it requires, and the tool declarations
 * it contributes. Reified off the factory function's attached metadata,
 * never by serializing the function itself. */
export interface InertToolFactory {
  readonly id: string;
  readonly requires: readonly string[];
  readonly definitions: readonly InertToolDeclaration[];
}

/** A model source canonicalized to its `(provider, model)` identity.
 * Credential and per-source parameter material is deliberately excluded. */
export interface InertModelSource {
  readonly provider: string;
  readonly model: string;
}

/** Plain-data projection of an `AgentDefinition`'s needs surface: identity,
 * prompt, director ref, capabilities, the reified tool grant surface, the
 * canonicalized model sources, and the pass-through metadata. */
export interface InertAgent {
  readonly id: string;
  readonly description?: string;
  readonly systemPrompt: string;
  readonly director?: DirectorRef;
  readonly capabilities: readonly string[];
  readonly toolFactories: readonly InertToolFactory[];
  readonly modelSources: readonly InertModelSource[];
  readonly tags?: Readonly<Record<string, string>>;
  readonly toolPackagePins?: readonly ToolPackagePin[];
}

export interface InertStepStep {
  readonly kind: "step";
  readonly id: string;
  readonly agent: InertAgent;
  readonly after?: readonly string[];
  readonly input?: Selector;
  readonly reads?: readonly Selector[];
  readonly writes?: readonly Selector[];
  readonly retry?: RetryPolicy;
  readonly timeout?: number;
  readonly drainBehavior?: DrainBehavior;
  readonly triggers?: number | "unbounded";
}

export interface InertMap {
  readonly kind: "map";
  readonly id: string;
  readonly over: Selector;
  readonly step: InertStepStep;
  readonly retry?: RetryPolicy;
  readonly after?: readonly string[];
}

export interface InertLoop {
  readonly kind: "loop";
  readonly id: string;
  readonly body: InertWorkflowDefinition;
  readonly while: string;
  readonly carry: string;
  readonly input?: Selector;
  readonly maxIterations: number;
  readonly onExhausted: string;
  readonly drainBehavior?: DrainBehavior;
  readonly after?: readonly string[];
}

export type InertOnTriggerBody =
  | { readonly inline: InertWorkflowDefinition }
  | { readonly ref: string };

export interface InertOnTrigger {
  readonly kind: "onTrigger";
  readonly id: string;
  readonly on: Trigger;
  readonly body: InertOnTriggerBody;
  readonly drainBehavior?: DrainBehavior;
  readonly after?: readonly string[];
}

// The gate/awaitSignal/sleep/childWorkflow/escalation/action primitives
// carry no functions or arktype `Type` values -- they are already pure
// plain data -- so their inert form is structurally identical to the live
// primitive. They are reconstructed field by field below rather than
// aliased so the projection is a self-contained tree.
export type InertStep =
  | InertStepStep
  | InertMap
  | InertLoop
  | InertOnTrigger
  | ActionPrimitive
  | GatePrimitive
  | AwaitSignalPrimitive
  | SleepPrimitive
  | ChildWorkflowPrimitive
  | EscalationPrimitive;

export interface InertWorkflowDefinition {
  readonly id: string;
  readonly triggers: readonly Trigger[];
  readonly stepOrder: readonly string[];
  readonly steps: Record<string, InertStep>;
  readonly state?: { readonly schema?: unknown };
  // The definition's credential bindings -- each maps a tool package's
  // declared handle to a concrete provider/locator. These are pure plain
  // data (no secret material; the secret is resolved fresh at deploy), so
  // they project verbatim. They are part of the hashed surface on purpose:
  // a binding names WHICH provider-backed credential the code may request,
  // so it belongs to what the operator approves. On the code-sourced path
  // the inert projection is the ONLY carrier of the bindings the deploy
  // resolves -- a moved or tampered registry that served code with
  // different bindings would change this projection and fail re-verify.
  readonly credentialBindings?: readonly CredentialBinding[];
}

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

/**
 * Project a live `WorkflowDefinition` into its inert, pure-plain-data
 * needs-surface projection. Throws on any unreifiable step shape or
 * corrupted tool grant surface.
 */
export function projectLiveToInert(
  definition: WorkflowDefinition,
): InertWorkflowDefinition {
  return projectDefinition(definition);
}

/**
 * The wire content hash bound to the projector output: project the live
 * definition to plain data, then hash it. Every party that binds identity,
 * approval, or re-verify to a deployment computes this same value, so the
 * hash is always taken over the inert projection and never over a live
 * object.
 */
export async function computeLiveDefinitionHash(
  definition: WorkflowDefinition,
): Promise<string> {
  return computeWireDefinitionHash(projectLiveToInert(definition));
}

function projectDefinition(
  definition: WorkflowDefinition,
): InertWorkflowDefinition {
  const steps: Record<string, InertStep> = {};
  for (const stepId of definition.stepOrder) {
    const primitive = definition.steps[stepId];
    if (primitive === undefined) {
      throw new Error(
        `live->inert projector: stepOrder names ${JSON.stringify(stepId)} ` +
          `but the steps record has no such entry`,
      );
    }
    steps[stepId] = projectPrimitive(primitive);
  }
  // Every step in the record must be reachable through stepOrder, else a
  // grant surface would silently drop out of the projection.
  for (const stepId of Object.keys(definition.steps)) {
    if (!Object.prototype.hasOwnProperty.call(steps, stepId)) {
      throw new Error(
        `live->inert projector: steps carries ${JSON.stringify(stepId)} ` +
          `which stepOrder does not list`,
      );
    }
  }
  return {
    id: definition.id,
    triggers: definition.triggers.map(projectTrigger),
    stepOrder: [...definition.stepOrder],
    steps,
    ...(definition.state !== undefined
      ? { state: projectState(definition.state) }
      : {}),
    // Bindings carry no secret and are plain data, so they project verbatim.
    // Keeping them in the projection puts the operator-approved credential
    // request surface inside the content hash (see `InertWorkflowDefinition`).
    ...(definition.credentialBindings !== undefined
      ? { credentialBindings: [...definition.credentialBindings] }
      : {}),
  };
}

function projectPrimitive(primitive: Primitive): InertStep {
  switch (primitive.kind) {
    case "step":
      return projectStepPrimitive(primitive);
    case "map":
      return projectMap(primitive);
    case "loop":
      return projectLoop(primitive);
    case "onTrigger":
      return projectOnTrigger(primitive);
    case "action":
      return projectAction(primitive);
    case "gate":
      return projectGate(primitive);
    case "awaitSignal":
      return projectAwaitSignal(primitive);
    case "sleep":
      return projectSleep(primitive);
    case "childWorkflow":
      return projectChildWorkflow(primitive);
    case "escalation":
      return projectEscalation(primitive);
    default:
      return assertUnreifiableStep(primitive);
  }
}

// `primitive` is `never` when every kind above is handled; the parameter
// type therefore makes a missing case a compile error. At runtime a value
// reaching here carries a kind outside the closed primitive set -- a
// hydrated or tampered definition -- and fails loud rather than projecting
// to `null`.
function assertUnreifiableStep(primitive: never): never {
  const kind = (primitive as { readonly kind?: unknown }).kind;
  throw new Error(
    `live->inert projector: cannot reify a step of unknown kind ` +
      `${JSON.stringify(kind)}; an unreifiable step must fail loud, ` +
      `never project to null`,
  );
}

function projectStepPrimitive(step: StepPrimitive): InertStepStep {
  return {
    kind: "step",
    id: step.id,
    agent: projectAgent(step.agent),
    ...(step.after !== undefined ? { after: [...step.after] } : {}),
    ...(step.input !== undefined ? { input: step.input } : {}),
    ...(step.reads !== undefined ? { reads: [...step.reads] } : {}),
    ...(step.writes !== undefined ? { writes: [...step.writes] } : {}),
    ...(step.retry !== undefined ? { retry: step.retry } : {}),
    ...(step.timeout !== undefined ? { timeout: step.timeout } : {}),
    ...(step.drainBehavior !== undefined
      ? { drainBehavior: step.drainBehavior }
      : {}),
    ...(step.triggers !== undefined ? { triggers: step.triggers } : {}),
  };
}

function projectMap(primitive: MapPrimitive): InertMap {
  return {
    kind: "map",
    id: primitive.id,
    over: primitive.over,
    step: projectStepPrimitive(primitive.step),
    ...(primitive.retry !== undefined ? { retry: primitive.retry } : {}),
    ...(primitive.after !== undefined ? { after: [...primitive.after] } : {}),
  };
}

function projectLoop(primitive: LoopPrimitive): InertLoop {
  return {
    kind: "loop",
    id: primitive.id,
    body: projectDefinition(primitive.body),
    while: primitive.while,
    carry: primitive.carry,
    ...(primitive.input !== undefined ? { input: primitive.input } : {}),
    maxIterations: primitive.maxIterations,
    onExhausted: primitive.onExhausted,
    ...(primitive.drainBehavior !== undefined
      ? { drainBehavior: primitive.drainBehavior }
      : {}),
    ...(primitive.after !== undefined ? { after: [...primitive.after] } : {}),
  };
}

function projectOnTrigger(primitive: OnTriggerPrimitive): InertOnTrigger {
  const body: InertOnTriggerBody =
    "inline" in primitive.body
      ? { inline: projectDefinition(primitive.body.inline) }
      : { ref: primitive.body.ref };
  return {
    kind: "onTrigger",
    id: primitive.id,
    on: projectTrigger(primitive.on),
    body,
    ...(primitive.drainBehavior !== undefined
      ? { drainBehavior: primitive.drainBehavior }
      : {}),
    ...(primitive.after !== undefined ? { after: [...primitive.after] } : {}),
  };
}

function projectAction(primitive: ActionPrimitive): ActionPrimitive {
  return {
    kind: "action",
    id: primitive.id,
    handler: primitive.handler,
    ...(primitive.input !== undefined ? { input: primitive.input } : {}),
    ...(primitive.effect !== undefined
      ? { effect: { requires: [...primitive.effect.requires] } }
      : {}),
    ...(primitive.timeout !== undefined ? { timeout: primitive.timeout } : {}),
    ...(primitive.drainBehavior !== undefined
      ? { drainBehavior: primitive.drainBehavior }
      : {}),
    ...(primitive.after !== undefined ? { after: [...primitive.after] } : {}),
  };
}

function projectGate(primitive: GatePrimitive): GatePrimitive {
  return {
    kind: "gate",
    id: primitive.id,
    when: primitive.when,
    then: primitive.then,
    else: primitive.else,
    ...(primitive.after !== undefined ? { after: [...primitive.after] } : {}),
  };
}

function projectAwaitSignal(
  primitive: AwaitSignalPrimitive,
): AwaitSignalPrimitive {
  return {
    kind: "awaitSignal",
    id: primitive.id,
    name: primitive.name,
    ...(primitive.timeout !== undefined ? { timeout: primitive.timeout } : {}),
    ...(primitive.onTimeout !== undefined
      ? { onTimeout: primitive.onTimeout }
      : {}),
    ...(primitive.drainBehavior !== undefined
      ? { drainBehavior: primitive.drainBehavior }
      : {}),
    ...(primitive.after !== undefined ? { after: [...primitive.after] } : {}),
  };
}

function projectSleep(primitive: SleepPrimitive): SleepPrimitive {
  return {
    kind: "sleep",
    id: primitive.id,
    ...(primitive.duration !== undefined
      ? { duration: primitive.duration }
      : {}),
    ...(primitive.until !== undefined ? { until: primitive.until } : {}),
    ...(primitive.drainBehavior !== undefined
      ? { drainBehavior: primitive.drainBehavior }
      : {}),
    ...(primitive.after !== undefined ? { after: [...primitive.after] } : {}),
  };
}

function projectChildWorkflow(
  primitive: ChildWorkflowPrimitive,
): ChildWorkflowPrimitive {
  return {
    kind: "childWorkflow",
    id: primitive.id,
    definitionRef: primitive.definitionRef,
    ...(primitive.input !== undefined ? { input: primitive.input } : {}),
    ...(primitive.drainBehavior !== undefined
      ? { drainBehavior: primitive.drainBehavior }
      : {}),
    ...(primitive.after !== undefined ? { after: [...primitive.after] } : {}),
  };
}

function projectEscalation(
  primitive: EscalationPrimitive,
): EscalationPrimitive {
  return {
    kind: "escalation",
    id: primitive.id,
    to: primitive.to,
    ...(primitive.data !== undefined ? { data: primitive.data } : {}),
    ...(primitive.after !== undefined ? { after: [...primitive.after] } : {}),
  };
}

// INVARIANT: every grant-bearing field of `AgentDefinition` must be enumerated
// here. The projector deliberately enumerates rather than spreads (so it can
// DROP credential-adjacent fields like model-source `parameters` and REIFY
// function-valued tool factories to inert data), which means a grant-bearing
// field added to `AgentDefinition` but not added here would be SILENTLY dropped
// from the inert projection -- and therefore from the approval hash and the
// operator's gated view. The grant-surface reification tests
// (`live-inert-projector.test.ts`) lock the current field coverage; extend them
// alongside any new grant-bearing field.
function projectAgent(agent: AgentDefinition): InertAgent {
  return {
    id: agent.id,
    ...(agent.description !== undefined
      ? { description: agent.description }
      : {}),
    systemPrompt: agent.systemPrompt,
    ...(agent.director !== undefined ? { director: agent.director } : {}),
    capabilities: [...agent.capabilities],
    toolFactories: agent.toolFactories.map(projectToolFactory),
    modelSources: agent.inference.sources.map(projectModelSource),
    ...(agent.tags !== undefined ? { tags: { ...agent.tags } } : {}),
    ...(agent.toolPackagePins !== undefined
      ? { toolPackagePins: [...agent.toolPackagePins] }
      : {}),
  };
}

function projectToolFactory(
  factory: AnnotatedToolFactory,
  index: number,
): InertToolFactory {
  // A live tool factory is a FUNCTION carrying its static metadata. A JSON
  // round-trip turns it into `null`, silently erasing the grant surface --
  // so a factory that is not a function is a lost grant surface and must
  // fail loud, never yield an empty grant set.
  const value: unknown = factory;
  if (typeof value !== "function") {
    const got = value === null ? "null" : typeof value;
    throw new Error(
      `live->inert projector: tool factory at index ${String(index)} is ` +
        `${got}, not a function; its grant surface was lost before ` +
        `projection (a JSON round-trip of a live factory yields null)`,
    );
  }
  const { id, requires, definitions } = factory;
  if (typeof id !== "string" || id.length === 0) {
    throw new Error(
      `live->inert projector: tool factory at index ${String(index)} ` +
        `carries no id; its grant surface metadata is missing`,
    );
  }
  if (!Array.isArray(requires)) {
    throw new Error(
      `live->inert projector: tool factory ${id} carries a non-array ` +
        `requires; its grant surface metadata is corrupt`,
    );
  }
  if (!Array.isArray(definitions)) {
    throw new Error(
      `live->inert projector: tool factory ${id} carries a non-array ` +
        `definitions; the tool grant surface is missing`,
    );
  }
  return {
    id,
    requires: [...requires],
    definitions: definitions.map((declaration) =>
      projectToolDeclaration(declaration, id),
    ),
  };
}

function projectToolDeclaration(
  declaration: ToolDeclaration,
  factoryId: string,
): InertToolDeclaration {
  if (typeof declaration.name !== "string" || declaration.name.length === 0) {
    throw new Error(
      `live->inert projector: tool factory ${factoryId} carries a ` +
        `declaration with no name; the grant surface is corrupt`,
    );
  }
  return {
    name: declaration.name,
    ...(declaration.approval !== undefined
      ? { approval: declaration.approval }
      : {}),
  };
}

function projectModelSource(preference: InferencePreference): InertModelSource {
  // Canonicalize to the (provider, model) identity. `parameters` -- the
  // per-source knob bag that can carry credential-adjacent material -- is
  // excluded from the projection, and therefore from the hashed preimage,
  // so a credential/parameter rotation cannot trip re-verify.
  return { provider: preference.provider, model: preference.model };
}

function projectTrigger(trigger: Trigger): Trigger {
  switch (trigger.type) {
    case "mail":
      return { type: "mail", to: trigger.to };
    case "schedule":
      return { type: "schedule", cron: trigger.cron };
    case "manual":
      return { type: "manual" };
    default:
      return assertUnreifiableTrigger(trigger);
  }
}

function assertUnreifiableTrigger(trigger: never): never {
  const kind = (trigger as { readonly type?: unknown }).type;
  throw new Error(
    `live->inert projector: cannot reify a trigger of unknown type ` +
      `${JSON.stringify(kind)}`,
  );
}

function projectState(state: { schema?: StateSchema }): { schema?: unknown } {
  if (state.schema === undefined) {
    return {};
  }
  return { schema: reifyStateSchema(state.schema) };
}

function reifyStateSchema(schema: StateSchema): unknown {
  // `schema` is an arktype `Type` -- a callable validator, not JSON. A JSON
  // round-trip of the live definition would drop it, so reify it to its
  // stable JSON representation and keep the projection pure plain data.
  const json: unknown = schema.json;
  if (json === undefined) {
    throw new Error(
      "live->inert projector: workflow state.schema is not an arktype Type " +
        "(no `.json` representation to reify)",
    );
  }
  return json;
}
