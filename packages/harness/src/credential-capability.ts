// The consumer-gated `credentials` capability: the sub-registry a tool queries
// by its declared handle to obtain a mediated credential. It is the runtime
// gate that enforces the `{ tool }` condition on a materialized
// `credential:{id}` / `use` grant -- the check the launch-time grant
// materialization sets up but does not itself evaluate.
//
// The gate lives here, at the point of use, and fails closed: a handle resolves
// only when the calling consumer holds `credential:{id}` / `use` with the
// grant's `{ tool }` condition matching this consumer. The shaping of the
// handle is delegated to the provider registry; the material is read fresh per
// use (rotation indirection) from the source the binding carries.

import {
  authorizeAction,
  CREDENTIAL_USE_CONDITIONS,
  type GrantRule,
} from "@intx/authz";
import type {
  CredentialCapability,
  CredentialMaterialSource,
  MediatedCredential,
} from "@intx/types";

import type { CredentialProviderRegistry } from "./credential-providers";

/**
 * A binding resolved at launch: which credential backs a declared handle, which
 * provider shapes it, the origin it authenticates to, and how to read its
 * current material. The material source is an indirection over a mutable cell so
 * a rotation reaches an already-shaped handle without a rebuild.
 */
export interface ResolvedCredentialBinding {
  /** The credential row id the handle resolved to; the `credential:{id}` the
   *  use-grant check runs against. */
  credentialId: string;
  /** The provider plugin key that shapes this credential's handle. */
  providerKey: string;
  /** The provider origin the shaped handle authenticates to. */
  origin: string;
  /** Reads the current secret material (rotation indirection). */
  readCurrentMaterial: CredentialMaterialSource;
}

export interface CredentialCapabilityDeps {
  /**
   * The consumer identity of the tool package this capability serves
   * (`tool:<package>`, from `toolConsumer`). Gate 2 checks each grant's
   * `{ tool }` condition against this value; an empty identity fails closed.
   */
  consumer: string;
  /** Resolved bindings keyed by the handle the tool declared. */
  bindings: ReadonlyMap<string, ResolvedCredentialBinding>;
  /** The registry that shapes a credential into a mediated handle. */
  providers: CredentialProviderRegistry;
  /** The grants in effect for this deploy (the consumer's run grants). */
  grants: GrantRule[];
}

/**
 * A `CredentialCapability` plus a host-only `dispose`. The tool sees only
 * `resolve`; the host runs `dispose` on teardown to release every handle shaped
 * through this capability (an http handle holds nothing; a future key-file /
 * socket handle would).
 */
export interface HostCredentialCapability extends CredentialCapability {
  dispose(): Promise<void>;
}

/**
 * Build the consumer-gated `credentials` capability for one tool package.
 *
 * `resolve(handle)` fails closed at every step: an unbound handle throws; a
 * handle the consumer is not authorized to use throws (Gate 2 -- the same
 * `authorizeAction` the model-source path uses, here supplied the credential-use
 * condition registry and this consumer). Only an authorized handle is shaped,
 * once, and memoized so repeated resolves return the same instance and there is
 * a single thing to dispose.
 */
export function createCredentialCapability(
  deps: CredentialCapabilityDeps,
): HostCredentialCapability {
  // Memoize the in-flight PROMISE, not the resolved handle, so two concurrent
  // resolves of the same handle share one gate+shape and yield one instance
  // (caching the value would let both miss the memo and shape twice, orphaning
  // a handle). A deterministic failure -- unbound handle, denied gate, unknown
  // provider -- caches too; it stays failed for this deploy, which is correct
  // since grants do not change mid-deploy.
  const shaped = new Map<string, Promise<MediatedCredential>>();

  function shapeHandle(handle: string): Promise<MediatedCredential> {
    return (async () => {
      const binding = deps.bindings.get(handle);
      if (binding === undefined) {
        throw new Error(
          `no credential is bound to handle "${handle}" for consumer ${deps.consumer}`,
        );
      }

      // Gate 2: fail closed unless the consumer holds credential:{id} / use with
      // the grant's { tool } condition matching this consumer.
      const decision = await authorizeAction(
        deps.grants,
        `credential:${binding.credentialId}`,
        "use",
        { registry: CREDENTIAL_USE_CONDITIONS, consumer: deps.consumer },
      );
      if (!decision.ok) {
        throw new Error(
          `consumer ${deps.consumer} is not authorized to use credential ${binding.credentialId} (${decision.reason})`,
        );
      }

      const provider = deps.providers.resolve(binding.providerKey);
      return provider.shape({
        origin: binding.origin,
        readCurrentMaterial: binding.readCurrentMaterial,
      });
    })();
  }

  return {
    resolve(handle: string): Promise<MediatedCredential> {
      const existing = shaped.get(handle);
      if (existing !== undefined) return existing;
      const pending = shapeHandle(handle);
      shaped.set(handle, pending);
      return pending;
    },

    async dispose(): Promise<void> {
      // Dispose EVERY successfully-shaped handle even if one throws -- a single
      // bad handle must not strand the rest -- then surface any failures loudly
      // rather than swallowing them.
      const settled = await Promise.allSettled([...shaped.values()]);
      shaped.clear();
      const errors: unknown[] = [];
      for (const result of settled) {
        if (result.status !== "fulfilled") continue;
        try {
          await result.value.dispose();
        } catch (error) {
          errors.push(error);
        }
      }
      if (errors.length > 0) {
        throw new AggregateError(
          errors,
          "one or more credential handles failed to dispose",
        );
      }
    },
  };
}
