// Helpers for the inference credential-material resolver seam
// (`CredentialMaterialResolver` in `@intx/types`). An inference call
// resolves its source's secret by `credentialId` through this seam
// instead of reading an inline `apiKey`, so the source config carries
// no secret. The sidecar backs it with the run's live credential cell;
// these helpers cover the two simpler cases.

import type {
  CredentialMaterial,
  CredentialMaterialResolver,
} from "@intx/types";

/**
 * A resolver that fails closed on every call. `createAgent` installs
 * this when the env supplies no `readCurrentMaterial`, so an agent
 * whose inference never resolves a credential needs no resolver, while
 * one that DOES reach a credential surfaces a clear error.
 */
export function createUnconfiguredCredentialResolver(): CredentialMaterialResolver {
  return (credentialId: string): CredentialMaterial => {
    throw new Error(
      `no credential resolver configured for this agent, but an inference call needs the secret for credential ${credentialId}; supply env.readCurrentMaterial`,
    );
  };
}

/**
 * A resolver over a fixed `credentialId -> secret` map, for callers
 * that hold secrets in memory rather than a live cell (examples,
 * tests, single-process agents). Fails closed on unknown ids.
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
