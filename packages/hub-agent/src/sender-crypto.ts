// Read side of the sender-key cache: resolve a sender address to the crypto
// the inbound-mail verify seam consumes.
//
// The mailbox verify seam is typed as `(address) => CryptoProvider | undefined`
// (`fetchFull`'s `getCrypto`), and it uses only the provider's public key --
// it reads `getPublicKey()` and hands the bytes to `verifyMimeSignature`. The
// cache holds a foreign sender's PUBLIC key alone, so the provider it wraps can
// answer `getPublicKey()` but cannot sign or verify with a private key it does
// not have. Those methods throw rather than return a wrong or empty answer.

import type { CryptoProvider } from "@intx/types/runtime";

import type { SenderKeyCache } from "./sender-key-cache";

const NO_PRIVATE_KEY =
  "public-key-only crypto holds no private key; verify an inbound signature " +
  "with verifyMimeSignature over getPublicKey() bytes";

/**
 * Wrap a raw Ed25519 public key as a `CryptoProvider` that can only report the
 * key. `getPublicKey` returns the bytes; `sign`, `signSSH`, and `verify` throw,
 * because none of them can be answered from a public key alone. This is the
 * shape the mailbox verify seam expects for a sender whose key the hub vouches
 * for but whose private key this process never holds.
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
 * `CryptoProvider`, or `undefined` when the cache holds no key for it. The
 * signature matches the mailbox verify seam's `getCrypto`, so the resolver is a
 * drop-in source of the sender's verification key. `undefined` is the seam's
 * defined "no key for this address" sentinel, not a swallowed failure.
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
