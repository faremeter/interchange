// The credential encryption-at-rest seam's one basic implementation.
//
// `createEnvKeyCredentialCipher` is a `CredentialCipher` backed by the general
// AEAD primitive under a single operator-provided key held in process memory. A
// future KMS or envelope-encryption plugin implements the same
// `CredentialCipher` interface and drops in at the composition root with no
// call-site changes.

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
 * A noop `CredentialCipher`: it follows the interface but uses no key. `encrypt`
 * returns its input unchanged, so a secret is stored as plaintext. `decrypt` is
 * identity for a plaintext value but THROWS on an `enc:` ciphertext: holding no
 * key, it cannot read a value a real cipher sealed, and passing that ciphertext
 * through as plaintext would silently deliver a garbage secret. This keeps the
 * interface's decrypt contract -- decrypt never returns a still-encrypted value
 * as plaintext -- which the env-key cipher already honors.
 *
 * It is used when no real cipher is configured, in tests and local development.
 * It MUST NOT be the active cipher in production: secrets would be stored
 * unencrypted. The composition root (`apps/hub`) always supplies a real env-key
 * cipher, gated by a required `CREDENTIAL_ENCRYPTION_KEY` at boot, and the hub
 * logs a warning if it ever falls back to this one.
 */
export function createNoopCredentialCipher(): CredentialCipher {
  return {
    encrypt: (plaintext) => Promise.resolve(plaintext),
    decrypt: (blob) => {
      // A keyless cipher holds no key of ANY scheme, so it rejects every `enc:`
      // ciphertext form (`isCiphertext`, loose) -- not just this module's
      // `enc:aead:` (the strict `PREFIX` the env cipher checks). A value it can
      // pass through must be genuine plaintext. Return a rejected promise --
      // not a synchronous throw -- so the failure surfaces on decrypt's result.
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
