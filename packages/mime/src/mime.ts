/* eslint-disable @typescript-eslint/no-non-null-assertion -- MIME parser uses bounded array access throughout */
/**
 * MIME byte construction and parsing for Interchange messages.
 *
 * Implements exactly two message shapes per MESSAGE.md:
 *   1. Conversation: multipart/mixed (text/plain plus zero or more
 *      attachment parts) in multipart/signed
 *   2. Structured: application/vnd.interchange+json in multipart/mixed in multipart/signed
 *
 * Produces real RFC 2822 / RFC 2046 / RFC 3156 bytes. The signed content
 * part is produced in MIME canonical form (CRLF line endings) so PGP/MIME
 * verification operates on the same bytes regardless of platform.
 *
 * RFC references verified:
 * - RFC 2822 §2.1.1: lines MUST NOT exceed 998 chars; recommended 78
 * - RFC 2046 §5.1.1: boundary MUST be <= 70 chars; CRLF before each boundary
 * - RFC 3156 §5: multipart/signed; protocol="application/pgp-signature";
 *   micalg=pgp-sha512; first part = signed content; second part = signature
 * - Message-IDs: <uuid@domain> — valid per RFC 2822 §3.6.4 (dot-atom local-part)
 */

import { type } from "arktype";
import { base64Decode, base64Encode } from "@intx/types";
import type {
  MessageAttachment,
  MessageHeaders as ParsedMessageHeaders,
  MessagePart,
} from "@intx/types/runtime";
import { InterchangeType } from "@intx/types/runtime";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type MessageHeaders = {
  from: string;
  to: string[];
  cc: string[] | undefined;
  date: Date;
  messageId: string;
  subject: string | undefined;
  inReplyTo: string | undefined;
  references: string[] | undefined;
  mimeVersion: "1.0";
  interchangeType: string | undefined;
  interchangeCorrelationId: string | undefined;
  interchangeTenantId: string | undefined;
  interchangeAgentId: string | undefined;
  interchangeSessionId: string | undefined;
  interchangeOfferingId: string | undefined;
  interchangeSchemaVersion: string | undefined;
  traceparent: string | undefined;
  tracestate: string | undefined;
};

export type ConversationContent = {
  kind: "conversation";
  text: string;
  attachments?: MessageAttachment[];
};

export type StructuredContent = {
  kind: "structured";
  json: Record<string, unknown>;
  summary?: string;
};

export type MimeAssemblyInput = {
  headers: MessageHeaders;
  content: ConversationContent | StructuredContent;
};

export type ParsedMimePart = {
  contentType: string;
  headers: Map<string, string>;
  body: Uint8Array;
};

export type ParsedMimeMessage = {
  headers: Map<string, string>;
  parts: ParsedMimePart[];
};

// ---------------------------------------------------------------------------
// JMAP Email types (RFC 8621)
// ---------------------------------------------------------------------------

export type JMAPAddress = {
  name: string | null;
  email: string;
};

export type JMAPBodyValue = {
  value: string;
  isEncodingProblem: boolean;
};

export type JMAPBodyPart = {
  partId: string;
  type: string;
};

export type JMAPAttachment = {
  blobId: string;
  name: string | null;
  type: string;
  size: number;
};

export type JMAPEmail = {
  from: JMAPAddress[];
  to: JMAPAddress[];
  subject: string | null;
  sentAt: string | null;
  bodyValues: Record<string, JMAPBodyValue>;
  textBody: JMAPBodyPart[];
  htmlBody: JMAPBodyPart[];
  attachments: JMAPAttachment[];
  headers: Record<string, string>;
};

// ---------------------------------------------------------------------------
// Message-ID generation
// ---------------------------------------------------------------------------

export function generateMessageId(address?: string): string {
  const domain =
    address !== undefined && address.includes("@")
      ? address.split("@")[1]!
      : "local";
  const uuid = crypto.randomUUID();
  return `<${uuid}@${domain}>`;
}

// ---------------------------------------------------------------------------
// Address normalization
// ---------------------------------------------------------------------------

function addressListError(addressLine: string): Error {
  return new Error(
    `extractAddrSpec: address lists are not supported: ${JSON.stringify(addressLine)}`,
  );
}

/**
 * Refuse a value naming more than one address; report where its `<` opens, or
 * -1 for a bare `addr-spec`. A `quoted-string` carries `@`, `<` and `,` as
 * text, so the scan exempts what it spans and refuses unpaired quotes.
 */
function scanSingleAddress(trimmed: string, addressLine: string): number {
  let quoted = false;
  let angleOpen = -1;
  let sawUnbracketedAt = false;
  let i = 0;
  while (i < trimmed.length) {
    const ch = trimmed[i];
    if (quoted && ch === "\\") {
      i += 2;
      continue;
    }
    if (ch === '"') {
      quoted = !quoted;
    } else if (!quoted) {
      if (ch === ",") {
        throw addressListError(addressLine);
      } else if (ch === "@" && angleOpen === -1) {
        sawUnbracketedAt = true;
      } else if (ch === "<") {
        if (angleOpen !== -1 || sawUnbracketedAt) {
          throw addressListError(addressLine);
        }
        angleOpen = i;
      }
    }
    i += 1;
  }
  if (quoted) {
    throw new Error(
      `extractAddrSpec: unterminated quoted string in ${JSON.stringify(addressLine)}`,
    );
  }
  return angleOpen;
}

/**
 * Extract the bare addr-spec (local-part@domain) from a single RFC 5322
 * address value. Strips any display name and surrounding angle brackets,
 * then lowercases the result so case-insensitive comparison falls out
 * naturally.
 *
 * Accepted inputs (exactly one address):
 *   `"Display Name" <user@host>`  → `user@host`
 *   `Display Name <user@host>`    → `user@host`
 *   `<user@host>`                 → `user@host`
 *   `user@host`                   → `user@host`
 *   `  User@Host  `               → `user@host`
 *
 * Rejected (throws) inputs:
 *   - empty or whitespace-only
 *   - a value naming more than one address, however the members are separated
 *   - an input whose double quotes do not pair up
 *   - input with no `@`
 *   - input that produces an empty local-part or domain
 *   - quoted local-parts (e.g. `"a@b"@host`) — technically valid per RFC
 *     5321 §4.1.2 but rare in practice; the simple split below would
 *     misinterpret the inner `@`, so we refuse rather than guess
 *   - content after the closing `>` in an angle-bracketed form
 *     (e.g. `Name <a@b> (comment)`) — would silently fall through to a
 *     misparsed bare-form attempt, so we refuse instead
 *   - trailing content in a bare form (e.g. `a@b (comment)`) — a well-formed
 *     bare addr-spec has no internal whitespace, so we refuse rather than
 *     fold the trailing token into the domain
 *
 * Per RFC 5321 §2.4 the local-part is technically case-sensitive, but no
 * production system honors that; matching case-insensitively is the
 * correct call for routing and identity checks.
 */
export function extractAddrSpec(addressLine: string): string {
  const trimmed = addressLine.trim();
  if (trimmed === "") {
    throw new Error("extractAddrSpec: address is empty");
  }

  const angleOpen = scanSingleAddress(trimmed, addressLine);

  let candidate: string;
  if (angleOpen !== -1) {
    // Angle-bracketed form. Require the `>` to be the trailing
    // non-whitespace character so that input like `Name <a@b> (comment)`
    // is refused rather than re-parsed as a bare addr-spec.
    if (!trimmed.endsWith(">")) {
      throw new Error(
        `extractAddrSpec: trailing content after '>' in ${JSON.stringify(addressLine)}`,
      );
    }
    candidate = trimmed.slice(angleOpen + 1, -1).trim();
    if (candidate.includes(">")) {
      throw new Error(
        `extractAddrSpec: stray '>' inside angle brackets in ${JSON.stringify(addressLine)}`,
      );
    }
  } else {
    // Bare form. An unquoted addr-spec carries no internal whitespace, so
    // treat any as trailing content (e.g. `a@b (comment)`) and refuse rather
    // than mangle the domain. Domain literals carry no internal whitespace, so
    // the only bare inputs this rejects are malformed or quoted local-parts,
    // both of which the function refuses by design anyway.
    if (/\s/.test(trimmed)) {
      throw new Error(
        `extractAddrSpec: trailing content in bare address ${JSON.stringify(addressLine)}`,
      );
    }
    candidate = trimmed;
  }

  // Reject quoted local-parts: the parser below splits on the first `@`,
  // which would corrupt a quoted form whose local-part contains `@`.
  if (candidate.includes('"')) {
    throw new Error(
      `extractAddrSpec: quoted local-parts are not supported: ${JSON.stringify(addressLine)}`,
    );
  }

  const atIndex = candidate.indexOf("@");
  if (atIndex === -1) {
    throw new Error(
      `extractAddrSpec: address has no '@': ${JSON.stringify(addressLine)}`,
    );
  }

  // Reject any further `@` in the candidate — a well-formed addr-spec
  // has exactly one. Multiple `@` is either a quoted form (rejected
  // above) or simply malformed.
  if (candidate.indexOf("@", atIndex + 1) !== -1) {
    throw new Error(
      `extractAddrSpec: multiple '@' in ${JSON.stringify(addressLine)}`,
    );
  }

  const local = candidate.slice(0, atIndex);
  const domain = candidate.slice(atIndex + 1);
  if (local === "" || domain === "") {
    throw new Error(
      `extractAddrSpec: empty local-part or domain in ${JSON.stringify(addressLine)}`,
    );
  }

  return `${local.toLowerCase()}@${domain.toLowerCase()}`;
}

// ---------------------------------------------------------------------------
// RFC 2822 date formatting
// ---------------------------------------------------------------------------

const DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"] as const;
const MONTHS = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
] as const;

export function formatRFC2822Date(date: Date): string {
  const day = DAYS[date.getUTCDay()]!;
  const d = String(date.getUTCDate()).padStart(2, "0");
  const mon = MONTHS[date.getUTCMonth()]!;
  const year = date.getUTCFullYear();
  const h = String(date.getUTCHours()).padStart(2, "0");
  const m = String(date.getUTCMinutes()).padStart(2, "0");
  const s = String(date.getUTCSeconds()).padStart(2, "0");
  return `${day}, ${d} ${mon} ${year} ${h}:${m}:${s} +0000`;
}

// ---------------------------------------------------------------------------
// Boundary generation
// ---------------------------------------------------------------------------

function generateBoundary(): string {
  const bytes = new Uint8Array(18);
  crypto.getRandomValues(bytes);
  return (
    "----=_Part_" +
    Array.from(bytes)
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("")
  );
}

// ---------------------------------------------------------------------------
// Header serialization (RFC 2822)
// ---------------------------------------------------------------------------

const CRLF = "\r\n";

function hdr(name: string, value: string): string {
  assertNoLineBreaks(value, `${name} header`);
  return `${name}: ${value}${CRLF}`;
}

function serializeMessageHeaders(
  h: MessageHeaders,
  contentType: string,
): string {
  let out = "";
  out += hdr("From", h.from);
  out += hdr("To", h.to.join(", "));
  if (h.cc && h.cc.length > 0) {
    out += hdr("Cc", h.cc.join(", "));
  }
  out += hdr("Date", formatRFC2822Date(h.date));
  out += hdr("Message-ID", h.messageId);
  if (h.subject !== undefined) {
    out += hdr("Subject", h.subject);
  }
  if (h.inReplyTo !== undefined) {
    out += hdr("In-Reply-To", h.inReplyTo);
  }
  if (h.references !== undefined && h.references.length > 0) {
    out += hdr("References", h.references.join(" "));
  }
  out += hdr("MIME-Version", "1.0");
  out += hdr("Content-Type", contentType);

  // Interchange headers
  if (h.interchangeType !== undefined) {
    out += hdr("Interchange-Type", h.interchangeType);
  }
  if (h.interchangeCorrelationId !== undefined) {
    out += hdr("Interchange-Correlation-ID", h.interchangeCorrelationId);
  }
  if (h.interchangeTenantId !== undefined) {
    out += hdr("Interchange-Tenant-ID", h.interchangeTenantId);
  }
  if (h.interchangeAgentId !== undefined) {
    out += hdr("Interchange-Agent-ID", h.interchangeAgentId);
  }
  if (h.interchangeSessionId !== undefined) {
    out += hdr("Interchange-Session-ID", h.interchangeSessionId);
  }
  if (h.interchangeOfferingId !== undefined) {
    out += hdr("Interchange-Offering-ID", h.interchangeOfferingId);
  }
  if (h.interchangeSchemaVersion !== undefined) {
    out += hdr("Interchange-Schema-Version", h.interchangeSchemaVersion);
  }
  if (h.traceparent !== undefined) {
    out += hdr("traceparent", h.traceparent);
  }
  if (h.tracestate !== undefined) {
    out += hdr("tracestate", h.tracestate);
  }

  return out;
}

// ---------------------------------------------------------------------------
// MIME part assembly
// ---------------------------------------------------------------------------

/** Reject a value that would break out of its header: a CR or LF ends the field. */
function assertNoLineBreaks(value: string, field: string): void {
  if (/[\r\n]/.test(value)) {
    throw new Error(
      `${field} must not contain CR or LF: ${JSON.stringify(value)}`,
    );
  }
}

/**
 * A double quote would close the `filename="..."` form, and this serializer
 * emits no escape for an inner quote.
 */
function assertAttachmentHeaderSafe(value: string, field: string): void {
  assertNoLineBreaks(value, field);
  if (value.includes('"')) {
    throw new Error(
      `${field} must not contain a double quote: ${JSON.stringify(value)}`,
    );
  }
}

/**
 * Encode bytes as base64, wrapped at 76 columns per RFC 2045. Returns the
 * empty string for empty input.
 */
function base64Lines(bytes: Uint8Array): string {
  const b64 = base64Encode(bytes);
  const lines: string[] = [];
  for (let i = 0; i < b64.length; i += 76) {
    lines.push(b64.slice(i, i + 76));
  }
  return lines.join(CRLF);
}

/**
 * Assemble the signed content for a conversation message.
 *
 * The shape is always multipart/mixed: one text/plain part (BODY[1.1])
 * followed by zero or more binary attachment parts (BODY[1.2..N]). The
 * shape is unconditional — there is no bare text/plain branch — so the
 * writer, the parser, and the signed-bytes contract have one form each.
 *
 * This is the exact bytes that will be hashed for the PGP/MIME signature.
 */
function assembleConversationSignedPart(
  text: string,
  attachments: readonly MessageAttachment[] = [],
): Uint8Array {
  const boundary = generateBoundary();

  // Canonicalize the text part: CRLF line endings, strip trailing
  // whitespace per line.
  const lines = text.split(/\r\n|\r|\n/);
  const canonLines = lines.map((l) => l.replace(/[ \t]+$/, ""));
  const canonical = canonLines.join(CRLF);

  let body = `Content-Type: multipart/mixed; boundary="${boundary}"${CRLF}${CRLF}`;

  // Text part (BODY[1.1])
  body += `--${boundary}${CRLF}`;
  body += `Content-Type: text/plain; charset=utf-8${CRLF}`;
  body += `Content-Transfer-Encoding: 7bit${CRLF}`;
  body += `${CRLF}`;
  body += `${canonical}${CRLF}`;

  // Attachment parts (BODY[1.2..N])
  for (const att of attachments) {
    assertAttachmentHeaderSafe(att.contentType, "attachment contentType");
    assertAttachmentHeaderSafe(att.name, "attachment name");
    body += `--${boundary}${CRLF}`;
    body += `Content-Type: ${att.contentType}${CRLF}`;
    body += `Content-Transfer-Encoding: base64${CRLF}`;
    body += `Content-Disposition: attachment; filename="${att.name}"${CRLF}`;
    body += `${CRLF}`;
    body += `${base64Lines(att.data)}${CRLF}`;
  }

  body += `--${boundary}--${CRLF}`;
  return new TextEncoder().encode(body);
}

/**
 * Assemble the signed content for a structured message (multipart/mixed).
 *
 * This is the exact bytes that will be hashed for the PGP/MIME signature.
 */
function assembleStructuredSignedPart(
  json: Record<string, unknown>,
  summary?: string,
): Uint8Array {
  const boundary = generateBoundary();
  const jsonStr = JSON.stringify(json);

  let body = `Content-Type: multipart/mixed; boundary="${boundary}"${CRLF}${CRLF}`;

  // JSON payload part
  body += `--${boundary}${CRLF}`;
  body += `Content-Type: application/vnd.interchange+json; charset=utf-8${CRLF}`;
  body += `Content-Transfer-Encoding: 7bit${CRLF}`;
  body += `${CRLF}`;
  body += `${jsonStr}${CRLF}`;

  // Optional human-readable summary
  if (summary !== undefined) {
    body += `--${boundary}${CRLF}`;
    body += `Content-Type: text/plain; charset=utf-8${CRLF}`;
    body += `Content-Transfer-Encoding: 7bit${CRLF}`;
    body += `${CRLF}`;
    const lines = summary.split(/\r\n|\r|\n/);
    const canonLines = lines.map((l) => l.replace(/[ \t]+$/, ""));
    body += `${canonLines.join(CRLF)}${CRLF}`;
  }

  body += `--${boundary}--${CRLF}`;
  return new TextEncoder().encode(body);
}

/**
 * Wrap content part and PGP signature into multipart/signed per RFC 3156.
 *
 * RFC 3156 §5: The multipart/signed body MUST consist of exactly two parts.
 * The first part contains the signed data. The second part contains the
 * detached PGP signature in application/pgp-signature.
 *
 * The boundary delimiter lines use CRLF as required by RFC 2046.
 */
function wrapInMultipartSigned(
  signedContentBytes: Uint8Array,
  signatureBytes: Uint8Array,
  boundary: string,
): Uint8Array {
  const signedContent = new TextDecoder().decode(signedContentBytes);
  const signature = new TextDecoder().decode(signatureBytes);

  const enc = new TextEncoder();

  // Per RFC 2046: boundary delimiter = "--" + boundary parameter.
  // The CRLF preceding the boundary belongs to the boundary, not the part.
  // Each part is preceded by: CRLF + "--" + boundary + CRLF
  // The closing delimiter: CRLF + "--" + boundary + "--" + CRLF
  const body =
    `--${boundary}${CRLF}` +
    `${signedContent}` +
    `${CRLF}--${boundary}${CRLF}` +
    `Content-Type: application/pgp-signature${CRLF}` +
    `${CRLF}` +
    `${signature}${CRLF}` +
    `--${boundary}--${CRLF}`;

  return enc.encode(body);
}

// ---------------------------------------------------------------------------
// Full message assembly
// ---------------------------------------------------------------------------

/**
 * Assemble a complete RFC 2822 message from headers, content, and signature
 * bytes. Returns the raw message bytes for storage.
 *
 * The signature bytes must be produced by signing the signed content part
 * bytes (the result of assembleSignedContentPart below).
 */
export function assembleMessage(
  headers: MessageHeaders,
  signedContentBytes: Uint8Array,
  signatureBytes: Uint8Array,
): Uint8Array {
  const outerBoundary = generateBoundary();

  const contentType =
    `multipart/signed; protocol="application/pgp-signature"; ` +
    `micalg=pgp-sha512; boundary="${outerBoundary}"`;

  const headerSection = serializeMessageHeaders(headers, contentType);
  const bodyBytes = wrapInMultipartSigned(
    signedContentBytes,
    signatureBytes,
    outerBoundary,
  );

  const enc = new TextEncoder();
  const headerBytes = enc.encode(headerSection + CRLF);

  const result = new Uint8Array(headerBytes.length + bodyBytes.length);
  result.set(headerBytes, 0);
  result.set(bodyBytes, headerBytes.length);
  return result;
}

/**
 * Build the signed content bytes for a message. These exact bytes are
 * what the CryptoProvider signs. The transport calls this, then signs,
 * then calls assembleMessage with both.
 */
export function assembleSignedContent(
  content: ConversationContent | StructuredContent,
): Uint8Array {
  if (content.kind === "conversation") {
    return assembleConversationSignedPart(content.text, content.attachments);
  }
  return assembleStructuredSignedPart(content.json, content.summary);
}

// ---------------------------------------------------------------------------
// MIME parsing (for fetchHeaders, fetchStructure, fetchPart, fetchFull)
// ---------------------------------------------------------------------------

const CR = 0x0d;
const LF = 0x0a;

/**
 * Refuse a header section that breaks a line with anything but CRLF (RFC 5321
 * §2.3.8, §4.1.1.4). The caller passes the section together with the blank line
 * that ends it, so neither an LF-only section nor an `LF LF` separator survives.
 *
 * Neither reading of a bare break is safe. Splitting on it resolves a field
 * from a value the sender smuggled inside one it controls; folding it collapses
 * every field after it into the value of the field that carries it, suppressing
 * each one. A non-conforming peer is owed an error rather than either parse.
 */
function assertNoBareLineBreaks(section: Uint8Array): void {
  for (let i = 0; i < section.length; i++) {
    const byte = section[i]!;
    if (byte === CR && section[i + 1] === LF) {
      i += 1;
      continue;
    }
    if (byte === CR || byte === LF) {
      throw new Error(
        "parseHeaderSection: a header section must break its lines with CRLF",
      );
    }
  }
}

/** The length of the line break starting at `at`, or 0 when none starts there. */
function lineBreakLength(raw: Uint8Array, at: number): number {
  const byte = raw[at];
  if (byte === CR) return raw[at + 1] === LF ? 2 : 1;
  if (byte === LF) return 1;
  return 0;
}

/**
 * Locate the blank line that ends the header section: the first line break
 * immediately followed by another, whichever flavour either uses. Returns null
 * for a message carrying no blank line.
 *
 * Searching for `CRLF CRLF` alone walks past a blank line written with bare
 * breaks and takes the section from a later offset, so every field before that
 * offset reads as one field body. Admitting the bare blank line as a separator
 * is the mirror of that: a sender who controls one field body ends the section
 * early and strips the fields after it. This search locates the boundary and
 * `assertNoBareLineBreaks` then refuses the section unless CRLF wrote it,
 * including the blank line itself.
 *
 * The pair is the whole test, so a body part that carries no header fields --
 * legal under RFC 2046 §5.1.1, which makes every field of a part optional --
 * offers one leading break, and its content reads as the header section. A
 * leading break is not admitted as a separator here because the message path
 * relies on this scan to step over a spurious blank line and keep the fields
 * behind it; a strict reading of a leading break would take those fields as
 * body and erase the message's originator. The two paths want opposite
 * readings of the same leading break, and this scan serves the message.
 */
function findHeaderBoundary(
  raw: Uint8Array,
): { headerEnd: number; bodyOffset: number } | null {
  let i = 0;
  while (i < raw.length) {
    const first = lineBreakLength(raw, i);
    if (first === 0) {
      i += 1;
      continue;
    }
    const second = lineBreakLength(raw, i + first);
    if (second !== 0) {
      return { headerEnd: i, bodyOffset: i + first + second };
    }
    i += first;
  }
  return null;
}

/**
 * Parse the header section of a raw RFC 2822 message.
 * Returns a map of lowercase header names to their values, and the
 * byte offset where the body starts.
 *
 * Throws for a header section that CRLF did not write, the blank line ending it
 * included. A message with no blank line at all is all header section.
 */
export function parseHeaderSection(raw: Uint8Array): {
  headers: Map<string, string>;
  bodyOffset: number;
  headerEnd: number;
} {
  const headers = new Map<string, string>();

  // The boundary is located in byte space so the returned offset is valid for
  // Uint8Array.slice() even when headers contain multi-byte UTF-8 characters.
  const boundary = findHeaderBoundary(raw);
  const headerEnd = boundary === null ? raw.length : boundary.headerEnd;
  const bodyOffset = boundary === null ? raw.length : boundary.bodyOffset;

  assertNoBareLineBreaks(raw.subarray(0, bodyOffset));

  const headerText = new TextDecoder("utf-8", { fatal: false }).decode(
    raw.subarray(0, headerEnd),
  );
  parseHeaders(headerText, headers);

  return { headers, bodyOffset, headerEnd };
}

/**
 * Fold a bare CR or LF -- one not part of a CRLF -- to a space. Splitting on it
 * would let a sender who controls a field body append a field of its own.
 *
 * Both callers read a region `parseHeaderSection` has already refused a bare
 * break in, so nothing reaches this today. It stays because folding is the safe
 * outcome for a region that refusal ever stops covering, and splitting is not.
 */
function foldBareLineBreaks(text: string): string {
  return text.replace(/\r(?!\n)|(?<!\r)\n/g, " ");
}

function parseHeaders(headerSection: string, out: Map<string, string>): void {
  // Unfold continuation lines (lines starting with whitespace per RFC 2822).
  const unfolded = foldBareLineBreaks(headerSection).replace(
    /\r\n[ \t]+/g,
    " ",
  );
  const lines = unfolded.split(CRLF);
  for (const line of lines) {
    if (line.trim() === "") continue;
    // A field begins with a printable name character (RFC 2822 2.2), so a
    // leading-WSP line continues nothing and names no field.
    if (line.startsWith(" ") || line.startsWith("\t")) continue;
    const colon = line.indexOf(":");
    if (colon === -1) continue;
    const name = line.slice(0, colon).trim().toLowerCase();
    const value = line.slice(colon + 1).trim();
    // For repeated headers (like Received), keep the first value.
    if (!out.has(name)) {
      out.set(name, value);
    }
  }
}

/**
 * Extract the boundary parameter from a Content-Type header value.
 */
export function extractBoundary(contentTypeValue: string): string | undefined {
  const match =
    contentTypeValue.match(/boundary="([^"]+)"/i) ??
    contentTypeValue.match(/boundary=([^\s;]+)/i);
  return match?.[1];
}

/**
 * Parse a multipart body into individual parts.
 *
 * Each part is returned as raw bytes (headers + blank line + body) for
 * further parsing.
 */
export function parseMultipart(
  body: Uint8Array,
  boundary: string,
): Uint8Array[] {
  const text = new TextDecoder("utf-8", { fatal: false }).decode(body);
  const delimiter = `--${boundary}`;
  const parts: Uint8Array[] = [];
  const enc = new TextEncoder();

  let pos = 0;
  while (pos < text.length) {
    // Find next delimiter.
    const delimIdx = text.indexOf(delimiter, pos);
    if (delimIdx === -1) break;

    // Check if it's the closing delimiter.
    const afterDelim = delimIdx + delimiter.length;
    if (text.slice(afterDelim, afterDelim + 2) === "--") break;

    // Skip past the delimiter line (to end of CRLF or LF).
    let partStart = afterDelim;
    if (text[partStart] === "\r") partStart++;
    if (text[partStart] === "\n") partStart++;

    // Find the next delimiter to know where this part ends.
    const nextDelimIdx = text.indexOf("\n" + delimiter, partStart);
    if (nextDelimIdx === -1) break;

    // Part body excludes the trailing CRLF before the next boundary.
    let partEnd = nextDelimIdx;
    // Account for the \n we searched for.
    // We want to include only up to (but not including) the CRLF before "--boundary".
    // nextDelimIdx points to the \n before the delimiter. The part ends before
    // the preceding \r\n (or just \n).
    if (partEnd > partStart && text[partEnd - 1] === "\r") {
      partEnd--;
    }

    const partText = text.slice(partStart, partEnd);
    parts.push(enc.encode(partText));

    pos = nextDelimIdx + 1;
  }

  return parts;
}

/**
 * Parse a single MIME part into its headers and body.
 */
export function parseMimePart(partBytes: Uint8Array): ParsedMimePart {
  const { headers, bodyOffset } = parseHeaderSection(partBytes);
  const contentType = headers.get("content-type") ?? "application/octet-stream";
  const body = partBytes.slice(bodyOffset);
  return { contentType, headers, body };
}

/**
 * Extract a MIME part by dot-separated path from a multipart/signed message.
 *
 * Path "1" returns the signed content part (text/plain or multipart/mixed).
 * Path "1.1" returns the first sub-part of the signed content (JSON payload).
 * Path "2" returns the application/pgp-signature part.
 *
 * This follows IMAP FETCH section specifier semantics (RFC 9051).
 */
export function extractPartByPath(
  raw: Uint8Array,
  partPath: string,
): Uint8Array {
  const { headers, bodyOffset } = parseHeaderSection(raw);
  const body = raw.slice(bodyOffset);
  const contentType = headers.get("content-type") ?? "";

  const steps = partPath.split(".").map((s) => {
    const n = parseInt(s, 10);
    if (isNaN(n) || n < 1) {
      throw new Error(`Invalid part path segment: "${s}"`);
    }
    return n;
  });

  return walkParts(body, contentType, steps, 0);
}

function walkParts(
  body: Uint8Array,
  contentType: string,
  steps: number[],
  depth: number,
): Uint8Array {
  const step = steps[depth];
  if (step === undefined) {
    throw new Error("Part path has no more segments");
  }

  if (!contentType.toLowerCase().startsWith("multipart/")) {
    throw new Error(
      `Cannot index into non-multipart content type: ${contentType}`,
    );
  }

  const boundary = extractBoundary(contentType);
  if (boundary === undefined) {
    throw new Error(`No boundary found in Content-Type: ${contentType}`);
  }

  const parts = parseMultipart(body, boundary);
  if (step > parts.length) {
    throw new Error(`Part ${step} does not exist (only ${parts.length} parts)`);
  }

  const partBytes = parts[step - 1]!;

  if (depth + 1 === steps.length) {
    return partBytes;
  }

  // Need to descend further.
  const part = parseMimePart(partBytes);
  return walkParts(part.body, part.contentType, steps, depth + 1);
}

// ---------------------------------------------------------------------------
// JMAP Email parsing
// ---------------------------------------------------------------------------

/**
 * Parse a RFC 2822 address value into structured JMAP address objects.
 *
 * Handles both "Display Name" <email@example.com> and bare email@example.com
 * forms, as well as comma-separated address lists.
 */
function parseAddressList(value: string): JMAPAddress[] {
  const results: JMAPAddress[] = [];
  // Split on commas that are not inside quoted strings or angle brackets.
  // We handle the two common forms:
  //   1. "Display Name" <email>
  //   2. Display Name <email>
  //   3. <email>
  //   4. email
  const segments = splitAddressList(value);
  for (const segment of segments) {
    const addr = parseOneAddress(segment.trim());
    if (addr !== null) {
      results.push(addr);
    }
  }
  return results;
}

function splitAddressList(value: string): string[] {
  const segments: string[] = [];
  let current = "";
  let depth = 0;
  let inQuote = false;

  for (const ch of value) {
    if (ch === '"' && !inQuote) {
      inQuote = true;
      current += ch;
    } else if (ch === '"' && inQuote) {
      inQuote = false;
      current += ch;
    } else if (ch === "<" && !inQuote) {
      depth++;
      current += ch;
    } else if (ch === ">" && !inQuote) {
      depth--;
      current += ch;
    } else if (ch === "," && depth === 0 && !inQuote) {
      segments.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  if (current.trim() !== "") {
    segments.push(current);
  }
  return segments;
}

function parseOneAddress(segment: string): JMAPAddress | null {
  if (segment === "") return null;

  // "Display Name" <email> or Display Name <email>
  const angleMatch = segment.match(/^(.*?)<([^>]+)>\s*$/);
  if (angleMatch !== null) {
    const rawName = angleMatch[1]!.trim();
    const email = angleMatch[2]!.trim();
    // Strip surrounding quotes from display name if present
    const name =
      rawName === "" ? null : rawName.replace(/^"(.*)"$/, "$1").trim() || null;
    return { name, email };
  }

  // Bare email address
  const bare = segment.trim();
  if (bare !== "") {
    return { name: null, email: bare };
  }

  return null;
}

/**
 * Parse the MIME Date header into an ISO 8601 string.
 *
 * Returns null if the header is missing or the value cannot be parsed.
 */
function parseDateHeader(value: string | undefined): string | null {
  if (value === undefined) return null;
  const date = new Date(value);
  if (isNaN(date.getTime())) return null;
  return date.toISOString();
}

/**
 * Replace every RFC 822 comment in a header value with a single space -- not
 * nothing, or `ba(c)se64` composes a token the sender never wrote. Comments
 * nest (RFC 822 §3.4.3), and a backslash quotes the next character (§3.4.5).
 */
function replaceCommentsWithSpace(value: string): string {
  const kept: string[] = [];
  let depth = 0;
  for (let i = 0; i < value.length; i++) {
    const ch = value.charAt(i);
    if (depth > 0 && ch === "\\") {
      i++;
      continue;
    }
    if (ch === "(") {
      if (depth === 0) kept.push(" ");
      depth++;
      continue;
    }
    if (ch === ")" && depth > 0) {
      depth--;
      continue;
    }
    if (depth === 0) kept.push(ch);
  }
  return kept.join("");
}

/**
 * Resolve the Content-Transfer-Encoding mechanism a part declares. RFC 2045
 * §6.1 makes it a single case-insensitive token admitting no parameters.
 */
export function transferEncodingMechanism(
  headers: Map<string, string>,
): string {
  const withoutComments = replaceCommentsWithSpace(
    headers.get("content-transfer-encoding") ?? "",
  );
  const semicolon = withoutComments.indexOf(";");
  const mechanism =
    semicolon === -1 ? withoutComments : withoutComments.slice(0, semicolon);
  const named = mechanism.trim().toLowerCase();
  // A field naming no mechanism takes the same default as an absent one.
  return named === "" ? "7bit" : named;
}

const RECOGNIZED_TRANSFER_ENCODINGS = new Set([
  "base64",
  "quoted-printable",
  "7bit",
  "8bit",
  "binary",
]);

export function isRecognizedTransferEncoding(mechanism: string): boolean {
  return RECOGNIZED_TRANSFER_ENCODINGS.has(mechanism);
}

/**
 * The content type a leaf part reports, after the RFC 2045 §6.4 rule that an
 * entity under an unrecognized transfer encoding is treated as
 * application/octet-stream. A `multipart/*` wrapper is exempt: its children
 * are located from its declared type. The relabel governs what a part
 * reports, not how it is routed.
 */
export function reportedContentType(
  declaredContentType: string,
  headers: Map<string, string>,
): string {
  if (extractContentTypeMime(declaredContentType).startsWith("multipart/")) {
    return declaredContentType;
  }
  const mechanism = transferEncodingMechanism(headers);
  if (isRecognizedTransferEncoding(mechanism)) return declaredContentType;
  return "application/octet-stream";
}

/**
 * The number of bytes widened per `String.fromCharCode` call. Large enough
 * that the per-call cost is amortised over the buffer, small enough to stay
 * well clear of the engine's limit on argument count.
 */
const BINARY_WIDEN_CHUNK_SIZE = 8192;

/**
 * Widen bytes into a string of one code unit per byte. A UTF-8 decode would
 * fold multi-byte sequences and replace any byte that is not valid UTF-8, and
 * no `TextDecoder` label widens bytes either: the WHATWG Encoding Standard
 * makes "latin1" and "iso-8859-1" labels for windows-1252, which maps 27 of
 * the byte values in 0x80-0x9f to other code points.
 *
 * Widening runs a chunk at a time. One `String.fromCharCode` per byte
 * allocates a single-character string per byte plus an array to hold them,
 * which on a body of tens of megabytes costs several times the body's own
 * size in resident memory, on a caller that runs synchronously.
 */
function bytesToBinaryString(bytes: Uint8Array): string {
  let out = "";
  for (let i = 0; i < bytes.length; i += BINARY_WIDEN_CHUNK_SIZE) {
    out += String.fromCharCode(
      ...bytes.subarray(i, i + BINARY_WIDEN_CHUNK_SIZE),
    );
  }
  return out;
}

/**
 * Decode a MIME body part, handling Content-Transfer-Encoding.
 */
function decodeBodyBytes(
  body: Uint8Array,
  headers: Map<string, string>,
): { value: string; isEncodingProblem: boolean } {
  const cte = transferEncodingMechanism(headers);

  if (cte === "base64") {
    try {
      const raw = new TextDecoder("utf-8", { fatal: false }).decode(body);
      const cleaned = raw.replace(/\s+/g, "");
      const binaryStr = atob(cleaned);
      return { value: binaryStr, isEncodingProblem: false };
    } catch {
      return {
        value: new TextDecoder("utf-8", { fatal: false }).decode(body),
        isEncodingProblem: true,
      };
    }
  }

  if (cte === "quoted-printable") {
    const raw = new TextDecoder("utf-8", { fatal: false }).decode(body);
    return { value: decodeQuotedPrintable(raw), isEncodingProblem: false };
  }

  if (cte === "7bit" || cte === "8bit" || cte === "binary") {
    return {
      value: new TextDecoder("utf-8", { fatal: false }).decode(body),
      isEncodingProblem: false,
    };
  }

  // RFC 2045 §6.4: an unrecognized mechanism is a byte stream, not text.
  return { value: bytesToBinaryString(body), isEncodingProblem: true };
}

function decodeQuotedPrintable(text: string): string {
  return text
    .replace(/=\r\n/g, "")
    .replace(/=\n/g, "")
    .replace(/=([0-9A-Fa-f]{2})/g, (_match, hex: string) =>
      String.fromCharCode(parseInt(hex, 16)),
    );
}

/**
 * Determine whether a MIME part is an attachment based on Content-Disposition
 * and content type.
 */
function isAttachmentPart(
  contentType: string,
  headers: Map<string, string>,
): boolean {
  const disposition = headers.get("content-disposition") ?? "";
  if (disposition.toLowerCase().startsWith("attachment")) return true;

  const ct = contentType.toLowerCase().split(";")[0]!.trim();
  if (ct === "text/plain" || ct === "text/html") return false;

  // Non-text types are treated as attachments unless they are multipart.
  if (ct.startsWith("multipart/")) return false;

  return true;
}

export function extractContentTypeMime(contentType: string): string {
  return contentType.split(";")[0]!.trim().toLowerCase();
}

function extractFilename(headers: Map<string, string>): string | null {
  const disposition = headers.get("content-disposition") ?? "";
  const nameMatch =
    disposition.match(/filename="([^"]+)"/i) ??
    disposition.match(/filename=([^\s;]+)/i);
  if (nameMatch !== null) return nameMatch[1]!;

  const ct = headers.get("content-type") ?? "";
  const ctNameMatch =
    ct.match(/name="([^"]+)"/i) ?? ct.match(/name=([^\s;]+)/i);
  if (ctNameMatch !== null) return ctNameMatch[1]!;

  return null;
}

type WalkContext = {
  mailId: string;
  bodyValues: Record<string, JMAPBodyValue>;
  textBody: JMAPBodyPart[];
  htmlBody: JMAPBodyPart[];
  attachments: JMAPAttachment[];
};

/**
 * Recursively walk MIME parts, populating body values and attachment lists.
 *
 * partPath uses IMAP-style dot-separated numbering (e.g., "1", "1.1", "2.3").
 */
function walkMimePart(
  partBytes: Uint8Array,
  partPath: string,
  ctx: WalkContext,
): void {
  const part = parseMimePart(partBytes);
  const declaredMime = extractContentTypeMime(part.contentType);

  if (declaredMime.startsWith("multipart/")) {
    const boundary = extractBoundary(part.contentType);
    if (boundary === undefined) return;
    const subParts = parseMultipart(part.body, boundary);
    subParts.forEach((subPartBytes, idx) => {
      walkMimePart(subPartBytes, `${partPath}.${idx + 1}`, ctx);
    });
    return;
  }

  const reportedMime = reportedContentType(declaredMime, part.headers);

  if (isAttachmentPart(part.contentType, part.headers)) {
    const blobId = `blob_${ctx.mailId}_${partPath}`;
    ctx.attachments.push({
      blobId,
      name: extractFilename(part.headers),
      type: reportedMime,
      size: part.body.length,
    });
    return;
  }

  const decoded = decodeBodyBytes(part.body, part.headers);
  ctx.bodyValues[partPath] = decoded;

  // Listed on the declared type but reported under the relabeled one: a
  // consumer walking `textBody` would otherwise never learn the part exists.
  if (declaredMime === "text/plain") {
    ctx.textBody.push({ partId: partPath, type: reportedMime });
  } else if (declaredMime === "text/html") {
    ctx.htmlBody.push({ partId: partPath, type: reportedMime });
  }
}

/**
 * Convert raw MIME bytes into a JMAP Email-shaped object.
 *
 * Handles text/plain, multipart/mixed, and multipart/signed message shapes.
 * For multipart/signed (RFC 3156), the signed content part (part 1) is
 * parsed for body and attachments. Signature verification is not performed.
 *
 * @param raw - Raw RFC 2822 message bytes
 * @param mailId - Opaque mail record ID used to generate blob IDs
 */
export function parseMailToEmail(raw: Uint8Array, mailId: string): JMAPEmail {
  const { headers: msgHeaders, bodyOffset } = parseHeaderSection(raw);
  const body = raw.slice(bodyOffset);
  const contentType = msgHeaders.get("content-type") ?? "text/plain";
  const mime = extractContentTypeMime(contentType);

  const ctx: WalkContext = {
    mailId,
    bodyValues: {},
    textBody: [],
    htmlBody: [],
    attachments: [],
  };

  if (mime === "multipart/signed") {
    // RFC 3156: part 1 is the signed content, part 2 is the signature.
    // Parse the content part through to extract body and attachments.
    const boundary = extractBoundary(contentType);
    if (boundary !== undefined) {
      const outerParts = parseMultipart(body, boundary);
      const contentPart = outerParts[0];
      if (contentPart !== undefined) {
        // The content part may itself be text/plain or multipart/mixed.
        // We assign it path "1" and walk it.
        walkMimePart(contentPart, "1", ctx);
      }
    }
  } else if (mime.startsWith("multipart/")) {
    const boundary = extractBoundary(contentType);
    if (boundary !== undefined) {
      const parts = parseMultipart(body, boundary);
      parts.forEach((partBytes, idx) => {
        walkMimePart(partBytes, `${idx + 1}`, ctx);
      });
    }
  } else {
    // Single-part message. Reconstruct minimal part bytes so parseMimePart
    // works; the message's own Content-Transfer-Encoding travels with the type.
    const enc = new TextEncoder();
    const cte = msgHeaders.get("content-transfer-encoding");
    const partHeaderText =
      `Content-Type: ${contentType}\r\n` +
      (cte === undefined ? "" : `Content-Transfer-Encoding: ${cte}\r\n`) +
      "\r\n";
    const partHeaderBytes = enc.encode(partHeaderText);
    const partBytes = new Uint8Array(partHeaderBytes.length + body.length);
    partBytes.set(partHeaderBytes, 0);
    partBytes.set(body, partHeaderBytes.length);
    walkMimePart(partBytes, "1", ctx);
  }

  // Extract Interchange-specific headers.
  const interchangeHeaders: Record<string, string> = {};
  for (const [name, value] of msgHeaders) {
    if (name.startsWith("interchange-")) {
      interchangeHeaders[name] = value;
    }
  }

  return {
    from: parseAddressList(msgHeaders.get("from") ?? ""),
    to: parseAddressList(msgHeaders.get("to") ?? ""),
    subject: msgHeaders.get("subject") ?? null,
    sentAt: parseDateHeader(msgHeaders.get("date")),
    bodyValues: ctx.bodyValues,
    textBody: ctx.textBody,
    htmlBody: ctx.htmlBody,
    attachments: ctx.attachments,
    headers: interchangeHeaders,
  };
}

/**
 * Decode a MIME part body into raw bytes, honoring Content-Transfer-Encoding.
 *
 * Unlike `decodeBodyBytes` (which produces a JMAP string value), this returns
 * the actual bytes. A malformed base64 body throws; an unrecognized mechanism
 * does not — RFC 2045 §6.4 hands back the bytes as they came.
 */
export function decodePartBytes(
  body: Uint8Array,
  headers: Map<string, string>,
): Uint8Array {
  const cte = transferEncodingMechanism(headers);

  if (cte === "base64") {
    const raw = new TextDecoder("utf-8", { fatal: false }).decode(body);
    return base64Decode(raw.replace(/\s+/g, ""));
  }

  if (cte === "quoted-printable") {
    const raw = new TextDecoder("utf-8", { fatal: false }).decode(body);
    const decoded = decodeQuotedPrintable(raw);
    const out = new Uint8Array(decoded.length);
    for (let i = 0; i < decoded.length; i++) {
      out[i] = decoded.charCodeAt(i);
    }
    return out;
  }

  // Identity encoding, including anything unrecognized (RFC 2045 §6.4).
  return body;
}

/**
 * Extract conversation attachments from raw message bytes as
 * `MessageAttachment[]` with decoded payloads.
 *
 * The conversation signed content is a multipart/mixed whose first part is
 * the text body and whose remaining attachment parts (Content-Disposition:
 * attachment) carry the binary payloads. Returns an empty array for any
 * shape without attachment parts — a bare text/plain signed part, a
 * non-multipart/signed message, or a multipart/mixed with only the text
 * part — so callers can use it unconditionally.
 *
 * Counterpart to `assembleConversationSignedPart`: assemble then extract
 * round-trips a `MessageAttachment[]`.
 */
export function extractAttachments(raw: Uint8Array): MessageAttachment[] {
  const { headers, bodyOffset } = parseHeaderSection(raw);
  const body = raw.slice(bodyOffset);
  const mime = extractContentTypeMime(headers.get("content-type") ?? "");

  if (mime !== "multipart/signed") return [];
  const outerBoundary = extractBoundary(headers.get("content-type") ?? "");
  if (outerBoundary === undefined) return [];

  const contentPart = parseMultipart(body, outerBoundary)[0];
  if (contentPart === undefined) return [];

  const signed = parseMimePart(contentPart);
  if (!extractContentTypeMime(signed.contentType).startsWith("multipart/")) {
    return [];
  }
  const innerBoundary = extractBoundary(signed.contentType);
  if (innerBoundary === undefined) return [];

  const attachments: MessageAttachment[] = [];
  for (const subPartBytes of parseMultipart(signed.body, innerBoundary)) {
    const subPart = parseMimePart(subPartBytes);
    if (!isAttachmentPart(subPart.contentType, subPart.headers)) continue;
    attachments.push({
      name: extractFilename(subPart.headers) ?? "attachment",
      contentType: reportedContentType(
        extractContentTypeMime(subPart.contentType),
        subPart.headers,
      ),
      data: decodePartBytes(subPart.body, subPart.headers),
    });
  }
  return attachments;
}

// ---------------------------------------------------------------------------
// Decoded-mail model (Mail / MessagePart) — lossless inbound decoding
// ---------------------------------------------------------------------------

function isInterchangeType(s: string): s is InterchangeType {
  return !(InterchangeType(s) instanceof type.errors);
}

/**
 * Build the typed, ergonomic `MessageHeaders` subset from a parsed header map.
 * Optional fields are included only when present (exactOptionalPropertyTypes-
 * safe). The full, lossless header set is carried separately as `rawHeaders`.
 */
export function buildMessageHeaders(
  headers: Map<string, string>,
): ParsedMessageHeaders {
  const toRaw = headers.get("to") ?? "";
  const to = toRaw
    ? toRaw
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    : [];

  const result: ParsedMessageHeaders = { to };

  // A blank header below is recorded as an absence, not an empty value: RFC
  // 5322 admits no empty `Date`, `Message-ID`, `In-Reply-To` or `From` body.
  //
  // A `Date` that does not parse is an absence too, which the parse check below
  // covers along with the blank one. A consumer builds a `Date` from this string
  // and compares it to a window's bounds; every comparison against an Invalid
  // Date is false, so a value kept here places the message inside every date
  // window, including two that exclude each other. An absence falls outside all
  // of them.
  const date = headers.get("date");
  if (date !== undefined && !Number.isNaN(new Date(date).getTime())) {
    result.date = date;
  }

  const messageId = headers.get("message-id");
  if (messageId !== undefined && messageId.trim().length > 0) {
    result.messageId = messageId;
  }

  // A non-blank value rides through verbatim even when it is not parseable.
  const from = headers.get("from");
  if (from !== undefined && from.trim().length > 0) result.from = from;

  const ccRaw = headers.get("cc");
  if (ccRaw !== undefined) {
    const cc = ccRaw
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    if (cc.length > 0) result.cc = cc;
  }

  const refsRaw = headers.get("references");
  if (refsRaw !== undefined) {
    const refs = refsRaw.split(/\s+/).filter(Boolean);
    if (refs.length > 0) result.references = refs;
  }

  const inReplyTo = headers.get("in-reply-to");
  if (inReplyTo !== undefined && inReplyTo.trim().length > 0) {
    result.inReplyTo = inReplyTo;
  }

  const subject = headers.get("subject");
  if (subject !== undefined) result.subject = subject;

  const listId = headers.get("list-id");
  if (listId !== undefined) result.listId = listId;

  const rawType = headers.get("interchange-type");
  if (rawType !== undefined && isInterchangeType(rawType)) {
    result.interchangeType = rawType;
  }

  const corrId = headers.get("interchange-correlation-id");
  if (corrId !== undefined) result.interchangeCorrelationId = corrId;

  const tenantId = headers.get("interchange-tenant-id");
  if (tenantId !== undefined) result.interchangeTenantId = tenantId;

  const agentId = headers.get("interchange-agent-id");
  if (agentId !== undefined) result.interchangeAgentId = agentId;

  const sessionId = headers.get("interchange-session-id");
  if (sessionId !== undefined) result.interchangeSessionId = sessionId;

  const offeringId = headers.get("interchange-offering-id");
  if (offeringId !== undefined) result.interchangeOfferingId = offeringId;

  const schemaVersion = headers.get("interchange-schema-version");
  if (schemaVersion !== undefined)
    result.interchangeSchemaVersion = schemaVersion;

  const traceparent = headers.get("traceparent");
  if (traceparent !== undefined) result.traceparent = traceparent;

  const tracestate = headers.get("tracestate");
  if (tracestate !== undefined) result.tracestate = tracestate;

  return result;
}

/**
 * Parse every header line in the message's header section into a raw,
 * lossless map of lowercased name to its ordered values. Repeated headers
 * (e.g. `Received`) keep all occurrences; folded continuation lines are
 * unfolded onto the preceding header. Bounded to the header section via
 * `headerEnd` so the whole message body is never decoded here.
 */
function parseRawHeaders(
  raw: Uint8Array,
  headerEnd: number,
): Record<string, string[]> {
  const text = foldBareLineBreaks(
    new TextDecoder("utf-8", { fatal: false }).decode(
      raw.subarray(0, headerEnd),
    ),
  );
  // `__proto__` and `constructor` are legal RFC 5322 field names that collide
  // with Object.prototype members, so the accumulator is a Map.
  const out = new Map<string, string[]>();
  let current: { name: string; value: string } | null = null;
  const flush = (): void => {
    if (current === null) return;
    const key = current.name.trim().toLowerCase();
    const values = out.get(key) ?? [];
    values.push(current.value.trim());
    out.set(key, values);
    current = null;
  };
  for (const line of text.split(CRLF)) {
    // `headerEnd` bounds the section already, so a blank line here is a
    // leading one, not the terminator; breaking would drop every field.
    if (line === "") continue;
    // Nothing to continue means no field (RFC 2822 2.2), not a field of its own.
    if (line.startsWith(" ") || line.startsWith("\t")) {
      if (current !== null) current.value += ` ${line.trim()}`;
      continue;
    }
    const idx = line.indexOf(":");
    if (idx === -1) continue;
    flush();
    current = { name: line.slice(0, idx), value: line.slice(idx + 1) };
  }
  flush();
  // Null prototype for the same reason: an absent name must resolve to nothing.
  const result: Record<string, string[]> = Object.create(null);
  for (const [name, values] of out) result[name] = values;
  return result;
}

function parseDisposition(
  headers: Map<string, string>,
): "inline" | "attachment" | undefined {
  const d = (headers.get("content-disposition") ?? "").trim().toLowerCase();
  if (d.startsWith("attachment")) return "attachment";
  if (d.startsWith("inline")) return "inline";
  return undefined;
}

/**
 * Recursively collect the decoded leaf parts of a MIME part. A multipart part
 * recurses into its children; a leaf part is decoded (transfer-encoding undone)
 * into a `MessagePart`. The PGP/MIME signature part is transport plumbing, not
 * content, so it is skipped -- which unwraps the `multipart/signed` envelope
 * (its two children are the signed content and the signature) for free.
 */
function collectLeafParts(partBytes: Uint8Array): MessagePart[] {
  const part = parseMimePart(partBytes);
  const mime = extractContentTypeMime(part.contentType);
  if (mime === "application/pgp-signature") return [];
  if (mime.startsWith("multipart/")) {
    const boundary = extractBoundary(part.contentType);
    // A multipart part with no boundary is undecodable: its children cannot
    // be located. Silently returning [] would drop that content and break the
    // lossless contract, so surface it as a decode failure the caller drops.
    if (boundary === undefined) {
      throw new Error(
        `decodeMail: ${mime} part has no boundary parameter; cannot decode its children`,
      );
    }
    return parseMultipart(part.body, boundary).flatMap(collectLeafParts);
  }
  const result: MessagePart = {
    contentType: reportedContentType(mime, part.headers),
    content: decodePartBytes(part.body, part.headers),
  };
  const filename = extractFilename(part.headers);
  if (filename !== null) result.filename = filename;
  const disposition = parseDisposition(part.headers);
  if (disposition !== undefined) result.disposition = disposition;
  return [result];
}

/**
 * Decode a raw inbound MIME message into its lossless parts: the typed header
 * subset, the full raw header map, and the flat list of decoded leaf parts
 * (the PGP/MIME signature and multipart wrappers removed). This is the
 * in-memory form; a caller commits each part's bytes to durable storage to
 * produce a JSON-safe `Mail`. Reused across the standalone and deployed
 * ingest paths so both see the same decoding.
 */
export function decodeMail(raw: Uint8Array): {
  headers: ParsedMessageHeaders;
  rawHeaders: Record<string, string[]>;
  parts: MessagePart[];
} {
  const { headers: singleMap, headerEnd } = parseHeaderSection(raw);
  const rawHeaders = parseRawHeaders(raw, headerEnd);
  const headers = buildMessageHeaders(singleMap);
  const parts = collectLeafParts(raw);
  return { headers, rawHeaders, parts };
}
