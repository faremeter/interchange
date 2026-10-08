// The consumer-gated `credentials` capability: the runtime gate
// enforcing the `{ tool }` condition on a `credential:{id}` / `use`
// grant. Fails closed: a handle resolves only when the calling consumer
// holds the grant with a matching `{ tool }` condition. Shaping is
// delegated to the provider registry; material is read fresh per use.

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
import type { ToolCredentialDeclaration } from "@intx/types/package-json";

import type { CredentialProviderRegistry } from "./credential-providers";

/**
 * A launch-time binding: the credential behind a declared handle, the
 * provider that shapes it, the origin it authenticates to, and how to
 * read its current material. The material source indirection lets a
 * rotation reach an already-shaped handle without a rebuild.
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

/**
 * Reconcile a package's declared credential handles against the handles
 * its bindings resolved. A declared handle with no binding is a
 * launch-blocking misconfiguration, so this fails the launch loudly
 * rather than at the tool's first resolve.
 */
export function reconcileDeclaredCredentials(
  consumer: string,
  declared: readonly ToolCredentialDeclaration[],
  boundHandles: ReadonlySet<string>,
): void {
  const missing = declared
    .map((declaration) => declaration.handle)
    .filter((handle) => !boundHandles.has(handle));
  if (missing.length > 0) {
    throw new Error(
      `consumer ${consumer} declares credential handle(s) that no binding resolves: ${missing.join(", ")}`,
    );
  }
}

export interface CredentialCapabilityDeps {
  /**
   * The tool package this capability serves; Gate 2 checks each grant's
   * `{ tool }` condition against it. An empty identity fails closed.
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
 * A `CredentialCapability` plus a host-only `dispose`, run on teardown
 * to release every handle shaped through this capability (an http
 * handle holds nothing; a future key-file / socket handle would).
 */
export interface HostCredentialCapability extends CredentialCapability {
  dispose(): Promise<void>;
}

/**
 * Build the consumer-gated `credentials` capability for one tool
 * package. `resolve` fails closed: an unbound handle throws, and an
 * unauthorized one throws at Gate 2 (the same `authorizeAction` the
 * model-source path uses, with the credential-use condition registry
 * and this consumer). Only an authorized handle is shaped, once, and
 * memoized so there is a single thing to dispose.
 */
export function createCredentialCapability(
  deps: CredentialCapabilityDeps,
): HostCredentialCapability {
  // Memoize the in-flight promise, not the resolved handle, so
  // concurrent resolves share one gate+shape and yield one instance. A
  // deterministic failure caches too; it stays failed for this deploy,
  // and grants do not change mid-deploy.
  const shaped = new Map<string, Promise<MediatedCredential>>();

  function shapeHandle(handle: string): Promise<MediatedCredential> {
    return (async () => {
      const binding = deps.bindings.get(handle);
      if (binding === undefined) {
        throw new Error(
          `no credential is bound to handle "${handle}" for consumer ${deps.consumer}`,
        );
      }

      // Gate 2: fail closed unless the consumer holds credential:{id} /
      // use with the grant's { tool } condition matching this consumer.
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
      // Dispose every shaped handle even if one throws -- a bad handle
      // must not strand the rest -- then surface failures loudly.
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
