// The agent's runtime environment contract.
//
// `BaseEnv` is what every `createAgent(def, env)` call requires. Tools
// and directors may extend it with additional keys declared via their
// `defineTool` / `defineDirector` `requires` metadata; `validateEnv`
// (see `env-validation.ts`) asserts presence of every declared key
// before the agent constructs a reactor.
//
// `audit`, `authorize`, and `directors` are required fields; there are
// no read-site defaults. No-op implementations for tests and examples
// ship from `@intx/agent/testing`.

import type { AuthzCallResult, Dependencies } from "@intx/inference";
import type { CredentialMaterialResolver } from "@intx/types";
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
 * the reactor assembly's authz extension threads the call through.
 *
 * `Ctx` parameterizes the per-call context the closure receives;
 * `unknown` is the default. Higher-layer runtimes with a richer notion
 * of context (e.g. the workflow runtime's `{ stepId, attempt, runId }`)
 * capture it at closure-build time and ignore the third arg.
 */
export type AuthorizeFn<Ctx = unknown> = (
  resource: string,
  action: string,
  context: Ctx,
) => Promise<AuthzCallResult>;

/**
 * Required base env for every agent. Tools declare additional keys via
 * `defineTool({ requires })`; directors via `defineDirector({ requires })`.
 */
export interface BaseEnv {
  /**
   * Ordered inference sources supplied at instantiation. The head of
   * the priority order (the source whose id is `defaultSource`) starts
   * active; the tail is the failover chain. The registry owns a copy.
   */
  sources: InferenceSource[];

  /** Id of the source that starts active. */
  defaultSource: string;

  /** Backing context store. The caller owns its lifetime. */
  storage: ContextStore;

  /**
   * The directory the agent treats as its singleton lock boundary.
   *
   * For isogit-backed storage this MUST equal the directory passed to
   * `createIsogitStore`; two agents against the same `workdir` fail the
   * lock, while differing `workdir` values pointing at the same on-disk
   * storage would silently corrupt each other. This is not the working
   * tree the filesystem tools operate on -- tools read their own
   * working directory from their own env-DI key.
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
   * director picks a registered name and emits `caps.compact(name,
   * reason)`; the reactor resolves the name against this map. Names are
   * surfaced to the director factory via
   * `agentContext.compactorNames`. Omit for no compactors; a compact
   * against an absent name produces the reactor's "no compactor
   * registered" fatal error.
   */
  compactors?: Record<string, Compactor>;

  /**
   * Inference dependencies (notably `fetch` and the adapter registry)
   * for the reactor's `runInference` call. Production callers omit this
   * field -- `createAgent` fills it from
   * `createDefaultDependencies()`. Tests supply `setupHarness().deps`
   * for a deterministic stub fetch.
   */
  deps?: Dependencies;

  /**
   * Resolves an inference source's credential secret by `credentialId`
   * at send time, from the same credential-material cell tool
   * credentials resolve from. `createAgent` fills a fail-closed default
   * that throws if an inference call actually needs a secret; callers
   * doing real credentialed inference MUST supply one.
   */
  readCurrentMaterial?: CredentialMaterialResolver;

  /** Deterministic session id for tests; production omits it (fresh UUID). */
  sessionId?: string;

  /** Override for the default 10 000-character tool-result size cap. */
  sizeCapMaxChars?: number;

  /**
   * Override for the doom-loop detection threshold: the number of
   * identical consecutive tool-call turns that ends the run (default
   * 3), or `false` to disable detection.
   */
  doomLoopThreshold?: number | false;

  /** Maximum pending sends (active + queued); defaults to 16. */
  sendQueueMax?: number;

  /**
   * Maximum events a single `stream()` consumer may buffer; beyond
   * this the consumer's iterator throws `StreamBackpressureError`.
   * Defaults to 1024.
   */
  streamBufferMax?: number;

  /**
   * Maximum milliseconds `close()` waits for the reactor's shutdown
   * sequence before releasing the lock. Defaults to 5000; zero
   * disables the wait.
   */
  closeTimeoutMs?: number;

  /**
   * Plugin instances produced by host-loaded plugin factories. Tool
   * packages that accept plugins read this field and filter for the
   * ones they recognise; the agent runtime delivers plugins without
   * interpreting them.
   */
  plugins?: readonly unknown[];
}

/**
 * Thrown by `validateEnv` when the env-shape check fails.
 *
 * - `missing` lists absent env keys; `contributors` lists every tool /
 *   director / `BaseEnv` label that declared at least one missing key;
 *   `missingByContributor` pairs each contributor with its keys.
 * - `unresolvedDirectors` lists `DirectorRef.id`s the registry could
 *   not resolve, kept separate from `missing` (env keys) so consumers
 *   can distinguish the two failure modes.
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
