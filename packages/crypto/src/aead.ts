// Authenticated encryption with associated data (AES-256-GCM).
//
// A general AEAD primitive over strings. It is credential-agnostic: callers
// decide what to encrypt and what to authenticate as the `aad`. The credential
// encryption-at-rest seam (`createEnvKeyCredentialCipher`) is built on top.
//
// Ciphertext format: `enc:aead:<keyid>:<base64(iv ‖ ciphertext ‖ tag)>`
//   - `aead` names the cipher scheme this module implements (AES-256-GCM). It
//     lets a stored blob self-identify which cipher decrypts it: a different
//     `CredentialCipher` -- a KMS or envelope plugin -- writes a different
//     scheme (`enc:kms:...`), and a future format change within this scheme
//     bumps the name (e.g. `aead2`).
//   - `keyid` is the first 4 bytes of SHA-256(key), hex -- it self-identifies
//     which key produced the blob (without storing the key) so a key-rotation
//     window can tell old ciphertext from new. A mismatch fails loudly.
//   - iv: 12 random bytes. WebCrypto AES-GCM returns `ciphertext ‖ tag`
//     concatenated and wants it that way on decrypt, so the stored blob is
//     `iv ‖ (subtle output)` -- the 16-byte tag is never split out.
//
// The `aad` (additional authenticated data) is authenticated, not encrypted; it
// is not stored in the blob and must be reconstructed identically at decrypt
// time. Binding it to a record's identity prevents transplanting a ciphertext
// from one record to another.

import { base64Encode, base64Decode } from "@intx/types";

import { asArrayBuffer } from "./keys";

const PREFIX = "enc:aead:";
const IV_BYTES = 12;
const TAG_BYTES = 16;
const KEY_BYTES = 32;

const utf8 = new TextEncoder();
const fromUtf8 = new TextDecoder();

function toHex(bytes: Uint8Array): string {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

async function computeKeyId(key: Uint8Array): Promise<string> {
  const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-256", asArrayBuffer(key)),
  );
  return toHex(digest.slice(0, 4));
}

async function importAesKey(key: Uint8Array): Promise<CryptoKey> {
  return crypto.subtle.importKey("raw", asArrayBuffer(key), "AES-GCM", false, [
    "encrypt",
    "decrypt",
  ]);
}

function assertKeyLength(where: string, key: Uint8Array): void {
  if (key.length !== KEY_BYTES) {
    throw new Error(
      `${where}: key must be ${KEY_BYTES} bytes (AES-256), got ${key.length}`,
    );
  }
}

/**
 * Encrypt `plaintext` with AES-256-GCM under `key`, authenticating `aad`.
 * Returns the versioned `enc:aead:<keyid>:<base64>` ciphertext.
 */
export async function aeadEncrypt(
  key: Uint8Array,
  plaintext: string,
  aad: string,
): Promise<string> {
  assertKeyLength("aeadEncrypt", key);
  const cryptoKey = await importAesKey(key);
  const iv = crypto.getRandomValues(new Uint8Array(IV_BYTES));
  const sealed = new Uint8Array(
    await crypto.subtle.encrypt(
      {
        name: "AES-GCM",
        iv: asArrayBuffer(iv),
        additionalData: asArrayBuffer(utf8.encode(aad)),
      },
      cryptoKey,
      asArrayBuffer(utf8.encode(plaintext)),
    ),
  );
  const blob = new Uint8Array(iv.length + sealed.length);
  blob.set(iv, 0);
  blob.set(sealed, iv.length);
  return `${PREFIX}${await computeKeyId(key)}:${base64Encode(blob)}`;
}

/**
 * Reverse of `aeadEncrypt`. Strict: throws if `value` is not an `enc:aead`
 * ciphertext (a plaintext value is never returned as-is -- that would hide a
 * caller that failed to encrypt, or a record that was never re-keyed). Also
 * throws if the ciphertext was produced by a different key, if it was tampered
 * with, or if `aad` does not match the value used to encrypt.
 */
export async function aeadDecrypt(
  key: Uint8Array,
  value: string,
  aad: string,
): Promise<string> {
  assertKeyLength("aeadDecrypt", key);
  if (!value.startsWith(PREFIX)) {
    throw new Error(
      "aeadDecrypt: value is not an enc:aead ciphertext; refusing to treat it as plaintext",
    );
  }
  const rest = value.slice(PREFIX.length);
  const sep = rest.indexOf(":");
  if (sep === -1) {
    throw new Error(
      "aeadDecrypt: malformed ciphertext (missing key-id separator)",
    );
  }
  const keyIdLabel = rest.slice(0, sep);
  const expected = await computeKeyId(key);
  if (keyIdLabel !== expected) {
    throw new Error(
      `aeadDecrypt: ciphertext key id ${keyIdLabel} does not match the configured key ${expected}`,
    );
  }
  const blob = base64Decode(rest.slice(sep + 1));
  if (blob.length < IV_BYTES + TAG_BYTES) {
    throw new Error(
      "aeadDecrypt: ciphertext is too short to contain an iv and authentication tag",
    );
  }
  const iv = blob.slice(0, IV_BYTES);
  const sealed = blob.slice(IV_BYTES);
  const cryptoKey = await importAesKey(key);
  const plaintext = await crypto.subtle.decrypt(
    {
      name: "AES-GCM",
      iv: asArrayBuffer(iv),
      additionalData: asArrayBuffer(utf8.encode(aad)),
    },
    cryptoKey,
    asArrayBuffer(sealed),
  );
  return fromUtf8.decode(plaintext);
}

/** The AES-256 key length AEAD requires, in bytes. Exported for key validation. */
export const AEAD_KEY_BYTES = KEY_BYTES;
