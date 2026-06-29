/**
 * PGP/MIME signing via CryptoProvider.
 *
 * createDetachedSignature in @intx/crypto signs with raw private key bytes,
 * but callers that only hold a CryptoProvider (which does not expose the
 * private key) need this variant. It delegates to the crypto package's
 * signer-function primitive, handing it the provider's raw Ed25519 sign
 * operation. The OpenPGP packet assembly lives entirely in @intx/crypto;
 * this module only adapts a CryptoProvider into the signer the primitive
 * expects.
 */

import { createDetachedSignatureWithSigner } from "@intx/crypto";
import type { CryptoProvider } from "@intx/types/runtime";

/**
 * Produce a PGP/MIME detached signature using a CryptoProvider.
 *
 * Mirrors createDetachedSignature from @intx/crypto but accepts a
 * CryptoProvider instead of raw private key bytes.
 */
export async function createDetachedSignatureFromProvider(
  content: Uint8Array,
  provider: CryptoProvider,
): Promise<Uint8Array> {
  return createDetachedSignatureWithSigner(content, (input) =>
    provider.sign(input),
  );
}
