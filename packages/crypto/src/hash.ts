/**
 * SHA-256 digest of a UTF-8 string as raw bytes. Used to store the hash of
 * an opaque bearer token instead of the secret itself.
 */
export async function sha256(input: string): Promise<Uint8Array> {
  return new Uint8Array(
    await crypto.subtle.digest("SHA-256", new TextEncoder().encode(input)),
  );
}
