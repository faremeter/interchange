// The credential encryption-at-rest seam's one basic implementation.
//
// `createEnvKeyCredentialCipher` is a `CredentialCipher` backed by the general
// AEAD primitive under a single operator-provided key held in process memory. A
// future KMS or envelope-encryption plugin implements the same
// `CredentialCipher` interface and drops in at the composition root with no
// call-site changes.

import type { CredentialCipher } from "@intx/types";

import { aeadEncrypt, aeadDecrypt, AEAD_KEY_BYTES } from "./aead";

/**
 * Build the env-key `CredentialCipher`. Validates the key length at
 * construction so a misconfigured key fails at boot, not at first use. The key
 * is defensively copied so a later mutation of the caller's buffer cannot change
 * the cipher's key.
 */
export function createEnvKeyCredentialCipher(
  key: Uint8Array,
): CredentialCipher {
  if (key.length !== AEAD_KEY_BYTES) {
    throw new Error(
      `createEnvKeyCredentialCipher: key must be ${AEAD_KEY_BYTES} bytes (AES-256), got ${key.length}`,
    );
  }
  const held = new Uint8Array(key);
  return {
    encrypt: (plaintext, aad) => aeadEncrypt(held, plaintext, aad),
    decrypt: (blob, aad) => aeadDecrypt(held, blob, aad),
  };
}

/**
 * A noop `CredentialCipher`: it follows the interface but uses no key and does
 * not encrypt or decrypt -- encrypt returns its input and decrypt returns the
 * stored value unchanged. It is used when no real cipher is configured, in tests
 * and local development. It MUST NOT be the active cipher in production: secrets
 * would be stored unencrypted. The composition root (`apps/hub`) always supplies
 * a real env-key cipher, gated by a required `CREDENTIAL_ENCRYPTION_KEY` at boot,
 * and the hub logs a warning if it ever falls back to this one.
 *
 * Because decrypt returns the value unchanged, a value produced by a real cipher
 * is not readable through this one -- never mix the two on the same data.
 */
export function createNoopCredentialCipher(): CredentialCipher {
  return {
    encrypt: (plaintext) => Promise.resolve(plaintext),
    decrypt: (blob) => Promise.resolve(blob),
  };
}
