import { signEd25519 } from "./keys";
import { createDetachedSignatureWithSigner } from "./pgp";

/**
 * PGP/MIME detached signature over canonicalized content bytes, bound to a
 * raw Ed25519 private seed; a thin wrapper over
 * `createDetachedSignatureWithSigner`. Returns ASCII-armored text for the
 * `application/pgp-signature` MIME part. Callers holding a `CryptoProvider`
 * use the signer-function primitive directly.
 */
export async function createDetachedSignature(
  content: Uint8Array,
  privateKeyBytes: Uint8Array,
): Promise<Uint8Array> {
  return createDetachedSignatureWithSigner(content, (input) =>
    signEd25519(privateKeyBytes, input),
  );
}
