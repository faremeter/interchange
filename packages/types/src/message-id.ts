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

const BARE_LINE_BREAK = /\r(?!\n)|(?<!\r)\n/;
// Two line breaks back to back, of any flavour. The single-CR branch excludes a
// CR that an LF follows, or backtracking would let one CRLF satisfy both halves.
const BLANK_LINE = /(?:\r\n|\r(?!\n)|\n)(?:\r\n|\r(?!\n)|\n)/;

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
 * The header section ends at the first blank line, whichever line-break flavour
 * wrote it, and the whole message is the section when it carries no blank line.
 * CRLF is the sole line terminator (RFC 5321 §2.3.8, §4.1.1.4), matching
 * `@intx/mime`'s decoder, which refuses a section any other break wrote -- an
 * `LF LF` separator included. Such a message names no id here and takes the
 * digest in `deriveMessageId` instead, because reading one out of text that
 * parser refuses would fold the fields after the break, and the head of the
 * body, into this identifier. Header-field unfolding follows RFC 2822 §2.2.3: a
 * continuation line begins with whitespace and appends to the prior line.
 * Header-name comparison is case-insensitive per RFC 2822 §1.2.2.
 */
export function parseMessageIdHeader(rawMessage: Uint8Array): string | null {
  const raw = new TextDecoder("utf-8", { fatal: false }).decode(rawMessage);
  // Locating the blank line before checking the breaks is what keeps a `CRLF
  // CRLF` the sender left in the body from becoming the separator: the section
  // would then be read from that later offset, absorbing every field before it.
  const boundary = BLANK_LINE.exec(raw);
  const throughBoundary =
    boundary === null ? raw.length : boundary.index + boundary[0].length;
  if (BARE_LINE_BREAK.test(raw.slice(0, throughBoundary))) return null;
  const headerSection = boundary === null ? raw : raw.slice(0, boundary.index);
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
