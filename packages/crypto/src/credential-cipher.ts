// The credential encryption-at-rest seam's basic implementation:
// `createEnvKeyCredentialCipher` backs a `CredentialCipher` with the AEAD
// primitive under one operator-provided key held in process memory. A future
// KMS or envelope plugin implements the same interface and drops in at the
// composition root with no call-site changes.

import type { CredentialCipher } from "@intx/types";

import { aeadEncrypt, aeadDecrypt, AEAD_KEY_BYTES, isCiphertext } from "./aead";

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
 * Keyless `CredentialCipher`: `encrypt` stores plaintext as-is; `decrypt`
 * passes plaintext through but rejects any `enc:` ciphertext, which no key
 * could read. Used in tests and local development, never in production —
 * the hub's composition root always supplies a real env-key cipher gated by
 * a required `CREDENTIAL_ENCRYPTION_KEY` and warns if it falls back to this.
 */
export function createNoopCredentialCipher(): CredentialCipher {
  return {
    encrypt: (plaintext) => Promise.resolve(plaintext),
    decrypt: (blob) => {
      // No key of any scheme, so reject every `enc:` form (loose
      // `isCiphertext`), not just `enc:aead:`. Reject via promise, not a
      // synchronous throw, so the failure surfaces on decrypt's result.
      if (isCiphertext(blob)) {
        return Promise.reject(
          new Error(
            "createNoopCredentialCipher: refusing to pass an enc: ciphertext " +
              "through as plaintext; this keyless cipher cannot decrypt a value " +
              "a real cipher sealed. Configure CREDENTIAL_ENCRYPTION_KEY to read it.",
          ),
        );
      }
      return Promise.resolve(blob);
    },
  };
}
