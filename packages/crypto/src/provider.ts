import type { CryptoProvider, KeyPair } from "@intx/types/runtime";
import { signEd25519, verifyEd25519 } from "./keys";
import { createSSHSignature } from "./sshsig";

/**
 * Ed25519 implementation of CryptoProvider, backed by Web Crypto.
 *
 * Each instance is key-bound: constructed with a specific agent's Ed25519
 * key pair and holds the private key internally. `sign` uses the instance's
 * own private key without requiring a separate key parameter.
 *
 * The `sign` and `verify` methods operate on raw Ed25519 signatures (64
 * bytes), not on PGP-formatted output. PGP envelope construction is handled
 * by the transport layer using the `createDetachedSignature` and
 * `verifyDetachedSignature` functions in `sign.ts` and `verify.ts`.
 *
 * This separation allows the CryptoProvider to serve as the low-level
 * signing primitive while the message transport handles PGP framing.
 */
export class Ed25519Crypto implements CryptoProvider {
  readonly #privateKeyBytes: Uint8Array;
  readonly #publicKeyBytes: Uint8Array;

  constructor(keyPair: KeyPair) {
    if (keyPair.privateKey.length !== 32) {
      throw new Error(
        `Ed25519 private key must be 32 bytes, got ${keyPair.privateKey.length}`,
      );
    }
    if (keyPair.publicKey.length !== 32) {
      throw new Error(
        `Ed25519 public key must be 32 bytes, got ${keyPair.publicKey.length}`,
      );
    }
    this.#privateKeyBytes = keyPair.privateKey;
    this.#publicKeyBytes = keyPair.publicKey;
  }

  /**
   * Sign content with the instance's Ed25519 private key.
   *
   * Returns the raw 64-byte Ed25519 signature (r || s in native
   * little-endian format per RFC 8032). The caller is responsible for
   * wrapping this in PGP packet format when needed.
   */
  async sign(content: Uint8Array): Promise<Uint8Array> {
    return signEd25519(this.#privateKeyBytes, content);
  }

  /**
   * Sign `payload` with the SSH signature envelope (sshsig). The returned
   * ASCII-armored block is what `git verify-commit` expects in the commit's
   * `gpgsig` header when the allowed_signers file lists this instance's
   * public key.
   */
  async signSSH(payload: string): Promise<string> {
    return await createSSHSignature(
      payload,
      this.#privateKeyBytes,
      this.#publicKeyBytes,
    );
  }

  /**
   * Verify that a raw 64-byte Ed25519 signature over content was produced
   * by the given public key.
   */
  async verify(
    content: Uint8Array,
    signature: Uint8Array,
    publicKey: Uint8Array,
  ): Promise<boolean> {
    if (signature.length !== 64) {
      throw new Error(
        `Ed25519 signature must be 64 bytes, got ${signature.length}`,
      );
    }
    if (publicKey.length !== 32) {
      throw new Error(
        `Ed25519 public key must be 32 bytes, got ${publicKey.length}`,
      );
    }
    return verifyEd25519(content, signature, publicKey);
  }

  /** The raw 32-byte Ed25519 public key for this instance. */
  getPublicKey(): Uint8Array {
    return this.#publicKeyBytes;
  }
}

/**
 * Factory function for creating an Ed25519Crypto instance.
 */
export function createEd25519Crypto(keyPair: KeyPair): Ed25519Crypto {
  return new Ed25519Crypto(keyPair);
}
