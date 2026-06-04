// The agent's runtime environment contract.
//
// `BaseEnv` is what every `createAgent(def, env)` call requires. Tools
// and directors may extend it with additional keys declared via their
// `defineTool` / `defineDirector` `requires` metadata; the runtime
// `validateEnv` (see `env-validation.ts`)
// asserts presence of every declared key before the agent constructs a
// reactor.
//
// `audit`, `authorize`, and `directors` are required fields. There are
// no read-site defaults: a caller that omits them is making an
// affirmative choice the env contract rejects. No-op implementations for
// tests and examples ship from `@intx/agent/testing`.

import type { AuthzCallResult, Dependencies } from "@intx/inference";
import type {
  AuditStore,
  ContextStore,
  InferenceSource,
} from "@intx/types/runtime";

import type { DirectorRegistry } from "./director-types";

/**
 * Authorization callback shape. Tools call `authorize` before invoking;
 * the reactor assembly's authz extension threads the call through. The
 * shape matches `@intx/inference`'s `AuthzExtensionOptions.authorize`.
 *
 * Callers that need to attach per-call context (tenant id, request id,
 * workflow step) capture it by closure when constructing the function,
 * so the signature stays narrow and each layer specializes its own
 * context without baking another layer's vocabulary into the type.
 */
export type AuthorizeFn = (
  resource: string,
  action: string,
) => Promise<AuthzCallResult>;

/**
 * Required base env for every agent. Tools declare additional keys via
 * `defineTool({ requires })`; directors via `defineDirector({ requires })`.
 *
 * `audit` and `authorize` are required. `directors` is required so
 * `createAgent` can resolve the agent definition's `DirectorRef` (or
 * fall back to the registry's canonical default) without invoking any
 * read-site fallback.
 */
export interface BaseEnv {
  /**
   * Active inference source supplied at instantiation. The agent
   * copies this value into its own internal source registry; later
   * `setSource(...)` calls mutate the registry's copy, not this object.
   * Callers who want to observe the active source after a rotation
   * should track it themselves via the value passed to `setSource`.
   */
  source: InferenceSource;

  /** Backing context store. The caller owns its lifetime. */
  storage: ContextStore;

  /**
   * The directory the agent treats as its singleton lock boundary.
   *
   * For isogit-backed storage this MUST equal the directory passed to
   * `createIsogitStore`. Two agents constructed against the same
   * `workdir` fail the lock; two agents constructed against differing
   * `workdir` values pointing at the same on-disk storage directory
   * will silently corrupt each other -- the invariant is the caller's
   * to maintain.
   *
   * Tools that need a working directory (e.g. `@intx/tools-posix`) read
   * from this field through their env-DI declaration.
   */
  workdir: string;

  /** Audit sink. Required; no read-site fallback. */
  audit: AuditStore;

  /** Authorization callback. Required; no read-site fallback. */
  authorize: AuthorizeFn;

  /** Director registry. Required; no read-site fallback. */
  directors: DirectorRegistry;

  /**
   * Inference dependencies (notably `fetch`) for the reactor's
   * underlying `runInference` call.
   *
   * Production callers omit this field -- the assembly falls back to
   * `createDefaultDependencies()` which binds `globalThis.fetch`. Tests
   * pass `setupHarness().deps` from `@intx/inference-testing` to swap
   * the fetch implementation for a deterministic stub.
   *
   * Optional; do not require this field on the production path.
   */
  deps?: Dependencies;
}

/**
 * Thrown by `validateEnv` when one or more declared env keys are
 * absent on the supplied env, or when the agent definition references
 * a director id the registry could not resolve.
 *
 * `missing` lists the absent env-key names. `contributors` lists every
 * tool / director / `BaseEnv` label that declared at least one missing
 * key. `missingByContributor` pairs each contributor with the specific
 * keys it declared as missing so consumers can render an error UI that
 * tells the author which factory blamed which key (the flat `missing`
 * and `contributors` arrays carry the same data without the join).
 *
 * `unresolvedDirectors` lists `DirectorRef.id`s the registry could not
 * resolve. These are surfaced through a separate field rather than
 * being mixed into `missing` (env-key names) so consumers can
 * distinguish the two failure modes programmatically.
 */
export class AgentEnvError extends Error {
  readonly missing: readonly string[];
  readonly contributors: readonly string[];
  readonly missingByContributor: ReadonlyMap<string, readonly string[]>;

  constructor(
    missing: readonly string[],
    contributors: readonly string[],
    missingByContributor: ReadonlyMap<string, readonly string[]> = new Map(),
  ) {
    super(
      `agent env is missing required keys: ${missing.join(", ")} ` +
        `(required by: ${contributors.join(", ")})`,
    );
    this.name = "AgentEnvError";
    this.missing = missing;
    this.contributors = contributors;
    this.missingByContributor = missingByContributor;
  }
}
