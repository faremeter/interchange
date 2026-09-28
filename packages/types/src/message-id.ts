// Canonical Message-ID derivation for a raw RFC 2822 message.
//
// This id identifies the MESSAGE, not the run it triggers. It is the
// claim-check dedup key the inbox pipeline keys on (the same bytes
// delivered twice consume once), and it must be derived identically
// wherever a message is fingerprinted, or a redelivery would be treated
// as a fresh message. This module is the single source of truth those
// call sites import.
//
// A workflow run's id is NOT this value -- a deployment's one addressable
// top-level run uses the local part of its mail address as its stable runId
// (see `deriveWorkflowRunId`). The two ids are distinct: this one is
// per-message, while the top-level runId is per-deployment.
//
// The identifier is the `Message-ID` header value when the message
// carries one, and a sha256 of the raw bytes otherwise -- so a message
// from a non-RFC 2822 transport still receives a deterministic id.

import { hexEncode } from "./hex";

const BARE_LINE_BREAK = /\r(?!\n)|(?<!\r)\n/g;

/**
 * Derive the canonical Message-ID for a raw message. Returns the parsed
 * `Message-ID` header when the message names one, else the hex-encoded sha256
 * of the raw bytes.
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
 * the message carries no such header or carries a blank one.
 *
 * The parser walks the message until the `CRLF CRLF` separator. CRLF is the
 * sole line terminator (RFC 5321 §2.3.8, §4.1.1.4), matching `@intx/mime`'s
 * decoder: a bare CR or LF folds to a space rather than ending a field, so an
 * LF-terminated message names no id here and takes the digest below instead of
 * yielding one read out of unterminated text. Header-field unfolding follows
 * RFC 2822 §2.2.3: a continuation line begins with whitespace and appends to
 * the prior line. Header-name comparison is case-insensitive per RFC 2822
 * §1.2.2.
 */
export function parseMessageIdHeader(rawMessage: Uint8Array): string | null {
  const raw = new TextDecoder("utf-8", { fatal: false }).decode(rawMessage);
  const crlfBoundary = raw.indexOf("\r\n\r\n");
  // A bare CR or LF terminates no field, so it folds to a space; splitting on
  // one would resolve an id from text a sender smuggled inside a field body.
  const text = raw.replace(BARE_LINE_BREAK, " ");
  // An unterminated section must break its lines with CRLF; `@intx/mime`
  // refuses such a message, so name no id rather than read one out of it.
  if (crlfBoundary < 0 && text !== raw) return null;
  const headerSection = crlfBoundary >= 0 ? text.slice(0, crlfBoundary) : text;
  // Unfold continuation lines (a line starting with WSP belongs to
  // the prior header field).
  const lines = headerSection.split("\r\n");
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
    const value = line.slice(colon + 1).trim();
    // A blank header names no id (RFC 2822 §3.6.4 admits no empty `msg-id`).
    // Returning `""` would give every such message the same id, and the dedup
    // key built on it would discard the second as a redelivery of the first.
    if (value.length === 0) return null;
    return value;
  }
  return null;
}
