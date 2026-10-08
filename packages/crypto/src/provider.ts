import type { CryptoProvider, KeyPair } from "@intx/types/runtime";
import { signEd25519, verifyEd25519 } from "./keys";
import { createSSHSignature } from "./sshsig";

/**
 * Web Crypto Ed25519 implementation of CryptoProvider, bound to one key
 * pair. `sign`/`verify` use raw 64-byte Ed25519 signatures; PGP framing is
 * handled by the transport via `createDetachedSignature`/
 * `verifyDetachedSignature` (`sign.ts`/`verify.ts`).
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
   * Sign content with the instance's Ed25519 private key, returning the
   * raw 64-byte signature (r || s, native little-endian per RFC 8032).
   */
  async sign(content: Uint8Array): Promise<Uint8Array> {
    return signEd25519(this.#privateKeyBytes, content);
  }

  /**
   * Sign `payload` with the SSH signature envelope (sshsig); the armored
   * block is what `git verify-commit` expects in `gpgsig` when
   * allowed_signers lists this instance's public key.
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

export function createEd25519Crypto(keyPair: KeyPair): Ed25519Crypto {
  return new Ed25519Crypto(keyPair);
}
