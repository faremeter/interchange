import { signEd25519 } from "./keys";
import { createDetachedSignatureWithSigner } from "./pgp";

/**
 * Produce a PGP/MIME detached signature for the given content bytes using
 * raw Ed25519 private key seed bytes.
 *
 * The content should already be canonicalized (CRLF line endings, trailing
 * whitespace stripped). The signature is returned as ASCII-armored text
 * suitable for use as the `application/pgp-signature` MIME part.
 *
 * This is a thin wrapper over `createDetachedSignatureWithSigner`: it binds
 * the raw signing primitive (`signEd25519`) to the supplied key. Callers
 * that hold a `CryptoProvider` rather than key bytes use the signer-function
 * primitive directly.
 */
export async function createDetachedSignature(
  content: Uint8Array,
  privateKeyBytes: Uint8Array,
): Promise<Uint8Array> {
  return createDetachedSignatureWithSigner(content, (input) =>
    signEd25519(privateKeyBytes, input),
  );
}
