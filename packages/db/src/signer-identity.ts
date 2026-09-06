import type { LocalPrincipalSigner } from "@intx/types";

import type { DBExecutor } from "./client";
import type { PrincipalKeyStore } from "./principal-key-store";

/**
 * Resolve a principal's trusted signer identity from the hub's own key store.
 *
 * The returned `publicKey` is the principal's active key as the hub holds it, so
 * a verifier can check a signature against it without trusting any key on the
 * wire. Throws when the principal has no active key -- an unresolvable signer
 * must fail loudly rather than default.
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
