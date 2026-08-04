/**
 * The pluggable seam for encrypting credential secrets at rest.
 *
 * Every write site (credential / oauth-client create and update) encrypts
 * through this interface; the single read-for-use site decrypts through it. Both
 * depend only on the interface, so the concrete implementation is chosen once at
 * the composition root and swapped without touching any call site.
 *
 * The one basic implementation today is `createEnvKeyCredentialCipher`
 * (@intx/crypto): AES-256-GCM under a single operator-provided key. A future KMS
 * or envelope-encryption plugin implements this same interface and can keep key
 * material inside the KMS, because the seam abstracts the whole encrypt/decrypt
 * operation rather than just supplying key bytes.
 *
 * `aad` (additional authenticated data) binds a ciphertext to its context -- the
 * row id and column -- so a blob cannot be transplanted between rows (or between
 * a row's columns) and still decrypt. Every site builds the `aad` with
 * `credentialAad(id, column)` so the binding is identical on write and read.
 *
 * `decrypt` is strict: it throws on a value that is not a ciphertext produced by
 * `encrypt` rather than returning it as plaintext. A plaintext value reaching
 * decrypt means a write path failed to encrypt or the row was never re-keyed --
 * a failure that must surface, not be silently served.
 */
export interface CredentialCipher {
  encrypt(plaintext: string, aad: string): Promise<string>;
  decrypt(blob: string, aad: string): Promise<string>;
}

/**
 * Build the additional-authenticated-data string binding a credential-secret
 * ciphertext to the row and column it belongs to. The encoding is injective in
 * `(id, column)` -- distinct pairs always produce distinct strings -- so a
 * ciphertext cannot be transplanted to a row/column it was not sealed for even
 * if an id contained the delimiter of a naive `id:column` scheme. The
 * `"credential-secret"` tag domain-separates this use of the AEAD primitive from
 * any other. Both the write and read sites (and the re-key script) MUST build
 * the `aad` through this one function so the value matches.
 */
export function credentialAad(id: string, column: string): string {
  return JSON.stringify(["credential-secret", id, column]);
}
