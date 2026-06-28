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
  Compactor,
  ContextStore,
  InferenceSource,
} from "@intx/types/runtime";

import type { DirectorRegistry } from "./director-types";

// `Dependencies` is re-exported from `@intx/inference`, where the
// reactor assembly owns its canonical shape.
export type { Dependencies };

/**
 * Authorization callback shape. Tools call `authorize` before invoking;
 * the reactor assembly's authz extension threads the call through. The
 * shape matches `@intx/inference`'s `AuthzExtensionOptions.authorize`.
 *
 * `Ctx` parameterizes the per-call context the closure receives.
 * `unknown` is the default: bare callers do not interpret it. Higher-
 * layer runtimes that have a richer notion of context (the workflow
 * runtime supplies `{ stepId, attempt, runId }` via `@intx/workflow`'s
 * `AuthorizeContext`) construct a closure whose third arg is ignored
 * and which delegates to a runtime-typed authorize with the context
 * captured at closure-build time. The third arg in the public signature
 * is plumbing so the inference layer can pass through whatever shape
 * the caller's runtime chooses without learning that runtime's
 * vocabulary.
 */
export type AuthorizeFn<Ctx = unknown> = (
  resource: string,
  action: string,
  context: Ctx,
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
   * Ordered inference sources supplied at instantiation. The agent copies
   * these into its own internal source registry; the head of the priority
   * order (the source whose id is `defaultSource`) starts active, and the
   * tail is the failover chain. Later `setSource`/`setSources` calls mutate
   * the registry's copy, not these objects.
   */
  sources: InferenceSource[];

  /** Id of the source that starts active (the head of the priority order). */
  defaultSource: string;

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
   */
  workdir: string;

  /** Audit sink. Required; no read-site fallback. */
  audit: AuditStore;

  /** Authorization callback. Required; no read-site fallback. */
  authorize: AuthorizeFn;

  /** Director registry. Required; no read-site fallback. */
  directors: DirectorRegistry;

  /**
   * Compactors registered for this deployment, keyed by name. The
   * director picks a registered name and emits
   * `caps.compact(name, reason)`; the reactor resolves the name against
   * this map and runs the compactor's `apply()` on the conversation
   * turns. Registered names are surfaced to the director factory at
   * construction via `agentContext.compactorNames` so the director
   * picks against a known set rather than guessing by convention.
   *
   * Optional: omitting the field is the same shape as registering an
   * empty map. A `caps.compact(name, …)` call against an absent or
   * empty registry produces the reactor's existing
   * "no compactor registered" fatal error.
   *
   * Field placement mirrors `directors`: "what's registered at this
   * deployment" is an env question, not an agent-definition question.
   */
  compactors?: Record<string, Compactor>;

  /**
   * Inference dependencies (notably `fetch` and the adapter registry) for
   * the reactor's underlying `runInference` call.
   *
   * Production callers omit this field -- `createAgent` fills it from
   * `@intx/inference/providers`' `createDefaultDependencies()`, which binds
   * `globalThis.fetch` and the built-in adapter registry. Pass an explicit
   * `Dependencies` to override: tests supply `setupHarness().deps` from
   * `@intx/inference-testing` for a deterministic stub fetch, and hosts with
   * custom adapters pass a registry built via `loadAdapterRegistry`.
   *
   * Optional; do not require this field on the production path.
   */
  deps?: Dependencies;

  /**
   * Optional deterministic session id. Production callers omit and let
   * the agent generate a fresh UUID; tests that assert on audit-record
   * sessionIds supply a stable value.
   */
  sessionId?: string;

  /**
   * Override for the default 10 000-character tool-result size cap.
   * Forwarded to the reactor assembly's size-cap transform.
   */
  sizeCapMaxChars?: number;

  /**
   * Maximum number of pending sends (active + queued). Beyond this,
   * `send()` rejects with `SendQueueFullError`. Defaults to 16.
   */
  sendQueueMax?: number;

  /**
   * Maximum events any single `stream()` consumer may buffer. Beyond
   * this, the next read on that consumer's iterator throws
   * `StreamBackpressureError`; other consumers are unaffected. Defaults
   * to 1024.
   */
  streamBufferMax?: number;

  /**
   * Maximum milliseconds `close()` waits for the reactor's shutdown
   * sequence (audit flush, in-flight commits) before releasing the
   * lock and returning. Defaults to 5000. Zero disables the wait
   * (useful for tests whose reactor shutdown is intentionally blocked).
   */
  closeTimeoutMs?: number;

  /**
   * Plugin instances produced by plugin factories the host loaded
   * before instantiating tool factories. Each entry is the value the
   * plugin factory returned (`AnnotatedPluginFactory`'s `Result`).
   *
   * Tool packages that accept plugins read this field and filter for
   * plugins they recognise (by structural shape or a kind marker the
   * host-side packages agree on). The agent runtime delivers plugins
   * without interpreting them — composition is the receiving tool
   * package's responsibility.
   */
  plugins?: readonly unknown[];
}

/**
 * Thrown by `validateEnv` when the env-shape check fails. Two failure
 * modes are reported separately so consumers can react to each:
 *
 * - `missing` lists the env key names that were absent. `contributors`
 *   lists every tool / director / `BaseEnv` label that declared at
 *   least one missing key (`BaseEnv` is the contributor for the six
 *   core fields). `missingByContributor` pairs each contributor with
 *   the specific keys it declared as missing so consumers can render
 *   an error UI that tells the author which factory blamed which key
 *   (the flat `missing` and `contributors` arrays carry the same data
 *   without the join).
 * - `unresolvedDirectors` lists the `DirectorRef.id`s the registry
 *   could not resolve (the agent definition referenced a director the
 *   registry does not contain). These land on a separate field rather
 *   than being mixed into `missing` (env-key names) so consumers can
 *   distinguish the two failure modes programmatically.
 */
export class AgentEnvError extends Error {
  readonly missing: readonly string[];
  readonly contributors: readonly string[];
  readonly missingByContributor: ReadonlyMap<string, readonly string[]>;
  readonly unresolvedDirectors: readonly string[];

  constructor(
    missing: readonly string[],
    contributors: readonly string[],
    missingByContributor: ReadonlyMap<string, readonly string[]> = new Map(),
    unresolvedDirectors: readonly string[] = [],
  ) {
    const parts: string[] = [];
    if (missing.length > 0) {
      parts.push(
        `missing required keys: ${missing.join(", ")} ` +
          `(required by: ${contributors.join(", ")})`,
      );
    }
    if (unresolvedDirectors.length > 0) {
      parts.push(`unresolved director ids: ${unresolvedDirectors.join(", ")}`);
    }
    super(`agent env validation failed: ${parts.join("; ")}`);
    this.name = "AgentEnvError";
    this.missing = missing;
    this.contributors = contributors;
    this.missingByContributor = missingByContributor;
    this.unresolvedDirectors = unresolvedDirectors;
  }
}
