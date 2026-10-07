/**
 * PGP/MIME signing via a CryptoProvider.
 *
 * createDetachedSignature in @intx/crypto signs with raw private key bytes;
 * callers that only hold a CryptoProvider (which does not expose the private
 * key) need this variant. OpenPGP packet assembly lives in @intx/crypto;
 * this module adapts a CryptoProvider into the signer that primitive expects.
 */

import { createDetachedSignatureWithSigner } from "@intx/crypto";
import type { CryptoProvider } from "@intx/types/runtime";

/**
 * Produce a PGP/MIME detached signature using a CryptoProvider: like
 * createDetachedSignature from @intx/crypto, but accepts a CryptoProvider
 * instead of raw private key bytes.
 */
export async function createDetachedSignatureFromProvider(
  content: Uint8Array,
  provider: CryptoProvider,
): Promise<Uint8Array> {
  return createDetachedSignatureWithSigner(content, (input) =>
    provider.sign(input),
  );
}
