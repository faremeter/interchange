/**
 * PGP/MIME signing via a CryptoProvider.
 *
 * createDetachedSignature in @intx/crypto signs with raw private key bytes;
 * callers that only hold a CryptoProvider (which does not expose the private
 * key) need this variant. OpenPGP packet assembly lives in @intx/crypto.
 */

import { createDetachedSignatureWithSigner } from "@intx/crypto";
import type { CryptoProvider } from "@intx/types/runtime";

/**
 * Like createDetachedSignature from @intx/crypto, but accepts a
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
