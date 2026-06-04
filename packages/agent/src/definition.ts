// `AgentDefinition` -- the portable, hashable data that names what an
// agent is. Together with `defineAgent`, it produces a deploy unit:
// hashing the definition yields a deploy hash, walking the definition
// surfaces the capability and credential grants downstream tooling
// can require approval for at deploy time, and resolving it against a
// runtime env (`createAgent(def, env)`) yields a running Agent.
//
// `AgentDefinition` deliberately holds no instance state. It is data
// passed around by callers and consumed by downstream tooling (deploy
// scaffolding, metadata registries, admin surfaces). Everything that
// is per-instance (the active inference source, the storage handle,
// the authorize callback, the audit sink, the directors registry)
// lives in the env supplied at `createAgent` time.

import type { AnnotatedToolFactory } from "./tool";
import type { BaseEnv } from "./env";
import type { DirectorRef } from "./director-types";

/**
 * Per-source preference describing which providers and models this
 * agent prefers, in order. The field is **hash-only** -- it
 * participates in deploy-time hashing and grant computation but is
 * not consulted for runtime source selection. The agent uses
 * `env.source` for the active inference call. Downstream tooling that
 * resolves preferences against available credentials sets the active
 * `env.source`. Reordering or mutating this field changes the deploy
 * hash; consumers must treat it as immutable across a deployment.
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
 * via `requires`). Use `EnvRequiredByAll` (below) to compute it from a
 * factory tuple; `defineAgent` does this for you.
 *
 * Note on the type-level enforcement: a single `ToolFactory<any>` in
 * `toolFactories` collapses `EnvRequiredByAll` to `any`, silently
 * stripping the type-level requirements of every other factory in the
 * same definition. The runtime `validateEnv` (presence-only) is the
 * load-bearing safety guarantee; the type level is best-effort
 * guidance for authors who type their factories tightly.
 */
export interface AgentDefinition<EnvReq extends BaseEnv = BaseEnv> {
  readonly id: string;
  readonly description?: string;
  readonly systemPrompt: string;
  readonly director?: DirectorRef;
  readonly toolFactories: readonly AnnotatedToolFactory<EnvReq>[];
  readonly capabilities: readonly string[];
  readonly inference: {
    readonly sources: readonly InferencePreference[];
  };
  /**
   * Free-form metadata the agent itself does not consume. The agent's
   * runtime does not read this field on any path; it is a passthrough
   * surface for downstream consumers -- classifiers grouping
   * definitions, audit consumers filtering on deployment cohort,
   * tooling rendering a definition catalog. The shape is
   * `Record<string, string>` deliberately rather than a richer type:
   * tags are human/operator-supplied identifiers, not structured
   * data, and any consumer that wants to interpret a tag's content
   * does so by name agreement with the producer rather than by
   * shape contract. Producers that need structured per-definition
   * data should add their own field on a subtype rather than nesting
   * encoded JSON in a tag value.
   */
  readonly tags?: Readonly<Record<string, string>>;
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
 * Function parameters are contravariant under TypeScript's strict mode,
 * so the tuple's element constraint must be `AnnotatedToolFactory<any>`
 * rather than `AnnotatedToolFactory<BaseEnv>`. A factory typed
 * `AnnotatedToolFactory<MailEnv>` is **not** assignable to
 * `AnnotatedToolFactory<BaseEnv>` (it accepts only `MailEnv`, not every
 * `BaseEnv`), but it is assignable to `AnnotatedToolFactory<any>`. The
 * runtime `validateEnv` is the load-bearing safety guarantee; the
 * type level is best-effort guidance.
 *
 * **Author-facing footgun.** A single `AnnotatedToolFactory<any>` in
 * the tuple collapses the intersection to `any` and silently strips
 * the type-level env requirements of every other factory in the same
 * `defineAgent` call. Third-party factories typed `<any>` -- whether
 * by oversight or by deliberate escape -- erase the compile-time
 * check that the env shape covers their declared `requires`. The
 * runtime `validateEnv` will still blame the missing keys at
 * construction, but the author loses the editor-time feedback that
 * makes env-DI cheap to use. When importing third-party tool
 * factories, prefer ones whose env shape is explicit, and treat an
 * `<any>` factory the same way you would treat an `any`-typed
 * variable elsewhere in the codebase: an opt-out of the type system,
 * not a default.
 *
 * See the note on `AgentDefinition` above.
 */
export type EnvRequiredByAll<
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- TypeScript cannot express "factory whose env is some subtype of BaseEnv" without contravariant escape; see comment above
  Factories extends readonly AnnotatedToolFactory<any>[],
> = UnionToIntersection<EnvRequiredBy<Factories[number]>> & BaseEnv;

/**
 * Configuration accepted by `defineAgent`. Mirrors `AgentDefinition`
 * but takes `tools` as the input field name (matching the spec's
 * authoring-time shape) and infers `EnvReq` from the supplied
 * factories.
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
    ...(config.description !== undefined
      ? { description: config.description }
      : {}),
    ...(config.director !== undefined ? { director: config.director } : {}),
    ...(config.tags !== undefined ? { tags: config.tags } : {}),
  };
  return definition;
}
