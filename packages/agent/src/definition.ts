// `AgentDefinition` -- the portable, hashable data that names what an
// agent is. Hashing it yields a deploy hash; walking it surfaces the
// capability and credential grants downstream tooling can require
// approval for at deploy time; resolving it against a runtime env
// (`createAgent(def, env)`) yields a running Agent.
//
// `AgentDefinition` deliberately holds no instance state: everything
// per-instance (active inference source, storage handle, authorize
// callback, audit sink, director registry) lives in the env supplied
// at `createAgent` time.

import type { ToolPackagePin } from "@intx/types/tool-packages";

import type { AnnotatedToolFactory } from "./tool";
import type { BaseEnv } from "./env";
import type { DirectorRef } from "./director-types";

/**
 * Per-source preference describing which providers and models this
 * agent prefers, in order. Hash-only: it participates in deploy-time
 * hashing and grant computation but is not consulted for runtime
 * source selection (the active `env.source` is). Treat as immutable
 * across a deployment.
 */
export interface InferencePreference {
  readonly provider: string;
  readonly model: string;
  readonly parameters?: Readonly<Record<string, unknown>>;
}

/**
 * The portable, hashable shape of an agent.
 *
 * `EnvReq` is the intersection of every contributor's env requirements
 * (`BaseEnv` plus whatever each tool factory and the director declare
 * via `requires`). Use `EnvRequiredByAll` to compute it from a factory
 * tuple; `defineAgent` does this for you.
 *
 * Type-level caveat: a single `ToolFactory<any>` in `toolFactories`
 * collapses `EnvRequiredByAll` to `any`, silently stripping every
 * other factory's requirements. The runtime `validateEnv` is the
 * load-bearing safety guarantee; the type level is best-effort.
 */
export interface AgentDefinition<EnvReq extends BaseEnv = BaseEnv> {
  readonly id: string;
  readonly description?: string;
  readonly systemPrompt: string;
  readonly director?: DirectorRef;
  readonly toolFactories: readonly AnnotatedToolFactory<EnvReq>[];
  /**
   * Tool-package names whose `definePlugin` factories this agent uses
   * (`["@intx/tools-lsp"]`). A plugin package contributes NO
   * agent-visible factory: its plugin factory reaches the agent only
   * through `env.plugins`, so this explicit list is the only way
   * per-step plugin scoping and plugin tool grants can be known from
   * the definition alone. Part of the hashed wire surface, so a
   * tampered plugin set fails re-verify. Absent when unused.
   */
  readonly plugins?: readonly string[];
  readonly capabilities: readonly string[];
  readonly inference: {
    readonly sources: readonly InferencePreference[];
  };
  /**
   * Free-form metadata the agent itself does not consume; a passthrough
   * for downstream consumers (classifiers, audit filtering, catalogs).
   * `Record<string, string>` deliberately: tags are operator-supplied
   * identifiers, not structured data; producers needing structured data
   * should add their own field on a subtype.
   */
  readonly tags?: Readonly<Record<string, string>>;
  /**
   * Tool-package pins the sidecar materializes for this agent, carried
   * on the definition so a folded workflow asset is self-contained.
   */
  readonly toolPackagePins?: readonly ToolPackagePin[];
}

// Type-level helper for computing the intersection of env requirements
// across a tuple of tool factories.

type UnionToIntersection<U> = (
  U extends unknown ? (k: U) => void : never
) extends (k: infer I) => void
  ? I
  : never;

type EnvRequiredBy<F> = F extends AnnotatedToolFactory<infer E> ? E : never;

/**
 * Intersection of env requirements across a tuple of annotated tool
 * factories, narrowed to extend `BaseEnv`.
 *
 * Function parameters are contravariant under strict mode, so the
 * tuple's element constraint must be `AnnotatedToolFactory<any>`: a
 * factory typed `AnnotatedToolFactory<MailEnv>` is not assignable to
 * `AnnotatedToolFactory<BaseEnv>`. A single `<any>` factory in the
 * tuple collapses the intersection to `any` and silently strips the
 * other factories' requirements -- treat it as an opt-out of the type
 * system, not a default. The runtime `validateEnv` is the load-bearing
 * safety guarantee.
 */
export type EnvRequiredByAll<
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- TypeScript cannot express "factory whose env is some subtype of BaseEnv" without contravariant escape; see comment above
  Factories extends readonly AnnotatedToolFactory<any>[],
> = UnionToIntersection<EnvRequiredBy<Factories[number]>> & BaseEnv;

/**
 * Configuration accepted by `defineAgent`. Mirrors `AgentDefinition`
 * but takes `tools` as the input field name and infers `EnvReq` from
 * the supplied factories.
 */
export interface DefineAgentConfig<
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- contravariant escape per the explanation on EnvRequiredByAll above
  Factories extends readonly AnnotatedToolFactory<any>[],
> {
  readonly id: string;
  readonly description?: string;
  readonly systemPrompt: string;
  readonly director?: DirectorRef;
  readonly tools: Factories;
  /** Plugin-package names this agent uses; see `AgentDefinition.plugins`. */
  readonly plugins?: readonly string[];
  readonly capabilities: readonly string[];
  readonly inference: {
    readonly sources: readonly InferencePreference[];
  };
  readonly tags?: Readonly<Record<string, string>>;
}

/**
 * Construct an `AgentDefinition` from authoring-time config. The
 * returned definition has its env requirement computed as the
 * intersection of every supplied factory's `EnvReq`.
 */
export function defineAgent<
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- contravariant escape per the explanation on EnvRequiredByAll above
  const Factories extends readonly AnnotatedToolFactory<any>[],
>(
  config: DefineAgentConfig<Factories>,
): AgentDefinition<EnvRequiredByAll<Factories>> {
  type EnvReq = EnvRequiredByAll<Factories>;
  // The widened factory tuple is structurally identical; the cast
  // adjusts the type's `EnvReq` parameter to match the inferred
  // intersection.
  const toolFactories =
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- adjusting EnvReq parameter; structurally identical
    config.tools as unknown as readonly AnnotatedToolFactory<EnvReq>[];
  const definition: AgentDefinition<EnvReq> = {
    id: config.id,
    systemPrompt: config.systemPrompt,
    toolFactories,
    capabilities: config.capabilities,
    inference: config.inference,
    ...(config.plugins !== undefined ? { plugins: config.plugins } : {}),
    ...(config.description !== undefined
      ? { description: config.description }
      : {}),
    ...(config.director !== undefined ? { director: config.director } : {}),
    ...(config.tags !== undefined ? { tags: config.tags } : {}),
  };
  return definition;
}
