/**
 * Compute the SHA-256 digest of a UTF-8 string, returned as raw bytes.
 *
 * Used to derive the stored hash of an opaque bearer token so the raw
 * secret is never persisted: callers hash the presented token and compare
 * the digest against the stored one.
 */
export async function sha256(input: string): Promise<Uint8Array> {
  return new Uint8Array(
    await crypto.subtle.digest("SHA-256", new TextEncoder().encode(input)),
  );
}
