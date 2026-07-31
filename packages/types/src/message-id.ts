// Canonical Message-ID derivation for a raw RFC 2822 message.
//
// This id identifies the MESSAGE, not the run it triggers. It is the
// claim-check dedup key the inbox pipeline keys on (the same bytes
// delivered twice consume once), and it must be derived identically
// wherever a message is fingerprinted, or a redelivery would be treated
// as a fresh message. This module is the single source of truth those
// call sites import.
//
// A workflow run's id is NOT this value -- every run of a deployment
// shares the deployment's mail address as its stable runId (see
// `deriveWorkflowRunId`). The two ids are distinct: this one is
// per-message, the runId is per-deployment.
//
// The identifier is the `Message-ID` header value when the message
// carries one, and a sha256 of the raw bytes otherwise -- so a message
// from a non-RFC 2822 transport still receives a deterministic id.

import { hexEncode } from "./hex";

/**
 * Derive the canonical Message-ID for a raw message. Returns the parsed
 * `Message-ID` header when present, else the hex-encoded sha256 of the
 * raw bytes.
 */
export async function deriveMessageId(rawMessage: Uint8Array): Promise<string> {
  const messageIdFromHeader = parseMessageIdHeader(rawMessage);
  if (messageIdFromHeader !== null) {
    return messageIdFromHeader;
  }
  const digest = await crypto.subtle.digest(
    "SHA-256",
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- ArrayBuffer-backed at the call site; Web Crypto's BufferSource type rejects Uint8Array<ArrayBufferLike> under TS 5.9 (microsoft/TypeScript#62240)
    rawMessage as Uint8Array<ArrayBuffer>,
  );
  return hexEncode(new Uint8Array(digest));
}

/**
 * Parse the `Message-ID` header value from a raw message, or `null` when
 * the message carries no such header.
 *
 * The parser walks the message until the headers/body separator
 * (`CRLF CRLF` per RFC 2822 §2.1, with the lone-`LF` variant tolerated to
 * match common in-memory senders). Header-field unfolding follows RFC
 * 2822 §2.2.3: a continuation line begins with whitespace and appends to
 * the prior line. Header-name comparison is case-insensitive per RFC 2822
 * §1.2.2.
 */
export function parseMessageIdHeader(rawMessage: Uint8Array): string | null {
  const text = new TextDecoder("utf-8", { fatal: false }).decode(rawMessage);
  // Headers end at the first blank line. RFC 2822 mandates `CRLF CRLF`
  // but tolerate `LF LF` for callers that normalize line endings.
  let headerSection = text;
  const crlfBoundary = text.indexOf("\r\n\r\n");
  const lfBoundary = text.indexOf("\n\n");
  if (crlfBoundary >= 0 && (lfBoundary < 0 || crlfBoundary < lfBoundary)) {
    headerSection = text.slice(0, crlfBoundary);
  } else if (lfBoundary >= 0) {
    headerSection = text.slice(0, lfBoundary);
  }
  // Unfold continuation lines (a line starting with WSP belongs to
  // the prior header field).
  const lines = headerSection.split(/\r?\n/);
  const unfolded: string[] = [];
  for (const line of lines) {
    if (line.length > 0 && (line[0] === " " || line[0] === "\t")) {
      if (unfolded.length === 0) continue;
      unfolded[unfolded.length - 1] += " " + line.trim();
      continue;
    }
    unfolded.push(line);
  }
  for (const line of unfolded) {
    const colon = line.indexOf(":");
    if (colon < 0) continue;
    const name = line.slice(0, colon).trim().toLowerCase();
    if (name !== "message-id") continue;
    return line.slice(colon + 1).trim();
  }
  return null;
}
