// Read side of the sender-key cache: resolve a sender address to the crypto
// the inbound-mail verify seam consumes.
//
// The mailbox verify seam is typed `(address) => CryptoProvider | undefined`
// and uses only the provider's public key (`getPublicKey()` handed to
// `verifyMimeSignature`). The cache holds a foreign sender's PUBLIC key alone,
// so the wrapped provider can answer `getPublicKey()` but cannot sign or
// verify; those methods throw rather than return a wrong or empty answer.

import type { CryptoProvider } from "@intx/types/runtime";

import type { SenderKeyCache } from "./sender-key-cache";

const NO_PRIVATE_KEY =
  "public-key-only crypto holds no private key; verify an inbound signature " +
  "with verifyMimeSignature over getPublicKey() bytes";

/**
 * Wrap a raw Ed25519 public key as a `CryptoProvider` that can only report
 * the key. `getPublicKey` returns the bytes; `sign`, `signSSH`, and `verify`
 * throw, because none can be answered from a public key alone.
 */
export function createPublicKeyCrypto(publicKey: Uint8Array): CryptoProvider {
  return {
    getPublicKey() {
      return publicKey;
    },
    async sign() {
      throw new Error(NO_PRIVATE_KEY);
    },
    async signSSH() {
      throw new Error(NO_PRIVATE_KEY);
    },
    async verify() {
      throw new Error(NO_PRIVATE_KEY);
    },
  };
}

/**
 * Build a resolver that maps a sender address to a public-key-only
 * `CryptoProvider`, or `undefined` when the cache holds no key for it. Matches
 * the mailbox verify seam's `getCrypto` signature. `undefined` is the seam's
 * defined "no key for this address" sentinel, not a swallowed failure.
 *
 * A cache read that THROWS (the on-disk entry failed to load) is left to
 * propagate: that is a fault about material the cache was given and cannot
 * serve, not the absence `undefined` reports. The inbound-verify caller turns
 * it into its refusing verdict; flattening it to `undefined` would hand the
 * caller a sender condition instead.
 */
export function createSenderCryptoResolver(
  cache: SenderKeyCache,
): (address: string) => CryptoProvider | undefined {
  return (address) => {
    const publicKey = cache.get(address);
    if (publicKey === undefined) return undefined;
    return createPublicKeyCrypto(publicKey);
  };
}
