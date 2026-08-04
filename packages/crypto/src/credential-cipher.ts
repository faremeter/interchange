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
