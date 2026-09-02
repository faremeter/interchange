// Helpers for the inference credential-material resolver seam
// (`CredentialMaterialResolver` in `@intx/types`). An inference call resolves
// its source's secret by `credentialId` through this seam instead of reading an
// inline `apiKey`, so the source config carries no secret. The sidecar backs it
// with the run's live credential cell; the helpers here cover the two simpler
// cases.

import type {
  CredentialMaterial,
  CredentialMaterialResolver,
} from "@intx/types";

/**
 * A resolver that fails closed on every call. `createAgent` installs this when
 * the env supplies no `readCurrentMaterial`, so an agent whose inference never
 * resolves a credential (a mock adapter emitting no credential sentinel) needs
 * no resolver, while one that DOES reach a credential surfaces a clear error
 * rather than a confusing `undefined`.
 */
export function createUnconfiguredCredentialResolver(): CredentialMaterialResolver {
  return (credentialId: string): CredentialMaterial => {
    throw new Error(
      `no credential resolver configured for this agent, but an inference call needs the secret for credential ${credentialId}; supply env.readCurrentMaterial`,
    );
  };
}

/**
 * A resolver over a fixed `credentialId -> secret` map. For callers that hold
 * their secrets in memory rather than a live cell -- examples, tests, and any
 * single-process agent. Fails closed when a source references a credential the
 * map does not carry, mirroring the cell reader's revoked/absent behavior.
 */
export function createStaticCredentialResolver(
  materials: Record<string, string>,
): CredentialMaterialResolver {
  return (credentialId: string): CredentialMaterial => {
    const secret = materials[credentialId];
    if (secret === undefined) {
      throw new Error(
        `no credential material for ${credentialId} in the static resolver`,
      );
    }
    return { secret };
  };
}
