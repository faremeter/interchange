/* eslint-disable @typescript-eslint/no-non-null-assertion -- MIME multipart parsing with bounds checks */
import {
  parseHeaderSection,
  parseMimePart,
  extractBoundary,
  parseMultipart,
} from "@intx/mime";
import { verifyDetachedSignature } from "@intx/crypto";

/**
 * Verify a PGP/MIME `multipart/signed` message against a public key.
 *
 * Extracts the signed-content part and the detached
 * `application/pgp-signature` part from the raw message bytes, then checks
 * the signature with `verifyDetachedSignature`.
 *
 * - `valid` — the detached signature verified against `publicKey`
 * - `invalid` — the signature check failed, or the message could not be
 *   parsed as a signed message
 * - `missing` — the message is not `multipart/signed`, or carries no
 *   `application/pgp-signature` part
 *
 * `raw` must be the original, unmodified message bytes: the signature is
 * recomputed over the exact canonical bytes of the signed part, so a
 * re-serialized message will not verify. `publicKey` is the raw Ed25519
 * public key bytes, as returned by `CryptoProvider.getPublicKey()`.
 */
export async function verifyMimeSignature(
  raw: Uint8Array,
  publicKey: Uint8Array,
): Promise<"valid" | "invalid" | "missing"> {
  try {
    const { headers, bodyOffset } = parseHeaderSection(raw);
    const body = raw.slice(bodyOffset);
    const contentType = headers.get("content-type") ?? "";

    if (!contentType.toLowerCase().includes("multipart/signed")) {
      return "missing";
    }

    const boundary = extractBoundary(contentType);
    if (boundary === undefined) return "missing";

    const parts = parseMultipart(body, boundary);
    if (parts.length < 2) return "missing";

    const signedContentBytes = parts[0]!;
    const sigPartBytes = parts[1]!;
    const sigPart = parseMimePart(sigPartBytes);

    if (
      !sigPart.contentType.toLowerCase().includes("application/pgp-signature")
    ) {
      return "missing";
    }

    const valid = await verifyDetachedSignature(
      signedContentBytes,
      sigPart.body,
      publicKey,
    );

    return valid ? "valid" : "invalid";
  } catch {
    return "invalid";
  }
}
