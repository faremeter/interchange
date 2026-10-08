import type { LocalPrincipalSigner } from "@intx/types";

import type { DBExecutor } from "./client";
import type { PrincipalKeyStore } from "./principal-key-store";

/**
 * Resolve a principal's trusted signer identity from the hub's own key store:
 * the returned `publicKey` is the principal's active key as the hub holds it,
 * so a verifier never trusts a key from the wire. Throws when the principal
 * has no active key.
 */
export async function lookupLocalPrincipalSigner(
  principalKeyStore: PrincipalKeyStore,
  principalId: string,
  tx?: DBExecutor,
): Promise<LocalPrincipalSigner> {
  return {
    kind: "local-principal",
    principalId,
    publicKey: await principalKeyStore.getPublicKey(principalId, tx),
  };
}
