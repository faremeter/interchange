/* eslint-disable @typescript-eslint/no-non-null-assertion -- MIME parser uses bounded array access throughout */
/**
 * MIME byte construction and parsing for Interchange messages.
 *
 * Two message shapes per docs/MESSAGE.md, both wrapped in multipart/signed:
 * conversation (multipart/mixed of text/plain plus attachments) and
 * structured (application/vnd.interchange+json in multipart/mixed). The
 * signed content part is MIME canonical form (CRLF) so PGP/MIME verification
 * sees the same bytes on every platform. RFC 2822 §2.1.1 caps a line at 998
 * chars; RFC 3156 §5 fixes the multipart/signed shape.
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
 * address value: strip display name and angle brackets, then lowercase so
 * comparisons are case-insensitive.
 *
 * Refuses: empty input, more than one address (however separated), unpaired
 * quotes, no `@`, empty local-part or domain, quoted local-parts, and
 * trailing content after `>` or in a bare form.
 */
export function extractAddrSpec(addressLine: string): string {
  const trimmed = addressLine.trim();
  if (trimmed === "") {
    throw new Error("extractAddrSpec: address is empty");
  }

  const angleOpen = scanSingleAddress(trimmed, addressLine);

  let candidate: string;
  if (angleOpen !== -1) {
    // Require `>` to be the trailing non-whitespace character, so
    // `Name <a@b> (comment)` is refused rather than re-parsed as bare.
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
    // A bare addr-spec carries no internal whitespace; treat any as trailing
    // content and refuse rather than mangle the domain.
    if (/\s/.test(trimmed)) {
      throw new Error(
        `extractAddrSpec: trailing content in bare address ${JSON.stringify(addressLine)}`,
      );
    }
    candidate = trimmed;
  }

  // The first-`@` split below would misread a quoted local-part containing one.
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

  // A well-formed addr-spec has exactly one `@`.
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

/**
 * RFC 5322 §2.1.1: lines MUST NOT exceed 998 chars, 78 recommended. Fold to
 * 78: 998-target lines display poorly and leave no room for a relay that
 * prepends to a header.
 */
const FOLD_TARGET_OCTETS = 78;

/**
 * Insert folding whitespace so a line stays near `FOLD_TARGET_OCTETS`.
 *
 * Folds only at a space in the value (the only place RFC 5322 §2.2.3
 * permits). The continuation carries one leading space; unfolding folds the
 * break plus following whitespace back to one space, so a second leading
 * space would not survive. A run ending in a tab is not a fold point: the
 * tab would be deleted or become a space. A token with no space in it is
 * emitted whole -- there is no legal fold point inside a `msg-id` or `atom`.
 */
function foldHeaderLine(name: string, value: string): string {
  if (value.length === 0) return `${name}:${CRLF}`;

  const head = `${name}: `;
  let out = head;
  let column = head.length;
  let index = 0;

  // Last index of the whitespace run that contains `wsIndex`.
  const runLast = (wsIndex: number): number => {
    let last = wsIndex;
    while (
      last + 1 < value.length &&
      (value[last + 1] === " " || value[last + 1] === "\t")
    ) {
      last += 1;
    }
    return last;
  };

  // The fold must be a space that ends the run: a tab after it would be
  // swallowed as leading whitespace, and a tab as the fold would come back
  // as a space. When the run ends in a tab, the whole run stays here.
  const foldOnRun = (wsIndex: number): void => {
    const last = runLast(wsIndex);
    if (value[last] !== " ") {
      out += value.slice(index, last + 1);
      column += last + 1 - index;
      index = last + 1;
      return;
    }
    out += value.slice(index, last);
    column += last - index;
    index = last + 1;
    out += `${CRLF} `;
    column = 1;
  };

  while (index < value.length) {
    const room = FOLD_TARGET_OCTETS - column;
    if (room > 0 && value.length - index <= room) {
      out += value.slice(index);
      break;
    }

    // The last space in reach whose run ends in a space; a tab-ending run is skipped.
    let breakAt = -1;
    const limit = Math.max(0, Math.min(room, value.length - index));
    for (let offset = 0; offset < limit; offset += 1) {
      const at = index + offset;
      if (value[at] === " " && value[runLast(at)] === " ") breakAt = at;
    }

    if (breakAt !== -1 || value[index] === " ") {
      foldOnRun(breakAt === -1 ? index : breakAt);
      continue;
    }

    // No fold point in reach: emit the token whole.
    const nextSpace = value.indexOf(" ", index);
    const end = nextSpace === -1 ? value.length : nextSpace;
    out += value.slice(index, end);
    column += end - index;
    index = end;
  }

  return out + CRLF;
}

/**
 * Serialize a structured header (address list, id, date, content type).
 * Folded, never encoded: RFC 2047 encoded-words are not permitted in
 * structured-field tokens.
 */
function hdr(name: string, value: string): string {
  assertNoLineBreaks(value, `${name} header`);
  return foldHeaderLine(name, value);
}

/**
 * Bytes of UTF-8 per encoded-word. RFC 2047 §2 caps an encoded-word at 75
 * characters; overhead leaves 63 for base64, and 45 input bytes encode to 60.
 */
const ENCODED_WORD_PAYLOAD_BYTES = 45;

function isAscii(value: string): boolean {
  // eslint-disable-next-line no-control-regex
  return !/[^\x00-\x7F]/.test(value);
}

/**
 * Encode `value` as RFC 2047 encoded-words when it is not already ASCII.
 *
 * Base64 (`B`) for every value: `Q` is prettier on mostly-ASCII text but
 * needs a per-character escape table. Words split on CHARACTER boundaries
 * only: RFC 2047 §5 requires each encoded-word to decode independently.
 */
function encodeHeaderText(value: string): string {
  if (isAscii(value)) return value;

  const bytes = new TextEncoder().encode(value);
  const words: string[] = [];
  let start = 0;

  while (start < bytes.length) {
    let end = Math.min(start + ENCODED_WORD_PAYLOAD_BYTES, bytes.length);
    // Walk back off a continuation byte so the cut lands between characters.
    while (end > start && end < bytes.length) {
      const byte = bytes[end];
      if (byte === undefined || (byte & 0xc0) !== 0x80) break;
      end -= 1;
    }
    const chunk = bytes.subarray(start, end);
    words.push(`=?UTF-8?B?${Buffer.from(chunk).toString("base64")}?=`);
    start = end;
  }

  // Adjacent encoded-words are joined by whitespace, which a receiver deletes
  // (RFC 2047 §6.2); `foldHeaderLine` folds at those same spaces.
  return words.join(" ");
}

/**
 * Serialize an unstructured header (`Subject`). Encoded before folding:
 * encoding creates the fold points for a long non-ASCII subject.
 */
function hdrText(name: string, value: string): string {
  assertNoLineBreaks(value, `${name} header`);
  return foldHeaderLine(name, encodeHeaderText(value));
}

/**
 * Decode RFC 2047 encoded-words in a received header value. Both `B` and `Q`
 * are decoded; only `B` is produced here. Whitespace BETWEEN two words is
 * deleted, elsewhere kept (RFC 2047 §6.2). A word whose charset or encoding
 * cannot be handled is left as it arrived: the encoded form is a visible,
 * reportable fault.
 */
export function decodeHeaderText(value: string): string {
  const word = /=\?([^?]+)\?([BbQq])\?([^?]*)\?=/g;
  if (!word.test(value)) return value;
  word.lastIndex = 0;

  let out = "";
  let cursor = 0;
  let previousWasWord = false;

  for (let match = word.exec(value); match !== null; match = word.exec(value)) {
    const whole = match[0];
    const charset = match[1];
    const encoding = match[2];
    const payload = match[3];
    if (
      charset === undefined ||
      encoding === undefined ||
      payload === undefined
    ) {
      // Unreachable (all three groups are mandatory); narrowed so a future
      // pattern edit cannot turn it into a crash.
      continue;
    }
    const gap = value.slice(cursor, match.index);

    // Whitespace separating two encoded-words is a separator, not content.
    if (!(previousWasWord && gap.trim() === "")) out += gap;

    const decoded = decodeEncodedWord(charset, encoding, payload);
    out += decoded ?? whole;
    previousWasWord = decoded !== undefined;
    cursor = match.index + whole.length;
  }

  return out + value.slice(cursor);
}

/**
 * Decode base64 only when every character is in the alphabet and the bytes
 * re-encode to the same text. `Buffer` accepts illegal characters by
 * ignoring them, which would corrupt the word instead of leaving it as sent.
 */
function decodeBase64Strict(payload: string): Uint8Array | undefined {
  if (!/^[A-Za-z0-9+/]*={0,2}$/.test(payload)) return undefined;
  if (payload.length % 4 === 1) return undefined;
  const bytes = Buffer.from(payload, "base64");
  const canonical = Buffer.from(bytes).toString("base64").replace(/=+$/, "");
  const given = payload.replace(/=+$/, "");
  if (canonical !== given) return undefined;
  return new Uint8Array(bytes);
}

function decodeEncodedWord(
  charset: string,
  encoding: string,
  payload: string,
): string | undefined {
  // RFC 2231 permits a language suffix (`utf-8*en`); drop it.
  const normalized = charset.toLowerCase().split("*")[0] ?? "";
  // Unsupported charsets return undefined so the caller keeps the encoded form.
  const utf8 =
    normalized === "utf-8" ||
    normalized === "utf8" ||
    normalized === "us-ascii";
  const latin1 = normalized === "iso-8859-1" || normalized === "latin1";
  if (!utf8 && !latin1) return undefined;

  let bytes: Uint8Array;
  if (encoding.toLowerCase() === "b") {
    const decoded = decodeBase64Strict(payload);
    if (decoded === undefined) return undefined;
    bytes = decoded;
  } else {
    // Q: `_` is a space, `=XX` is a hex octet. Anything else is literal.
    const octets: number[] = [];
    for (let i = 0; i < payload.length; i += 1) {
      const char = payload[i];
      if (char === undefined) break;
      if (char === "_") {
        octets.push(0x20);
        continue;
      }
      if (char === "=" && i + 2 < payload.length) {
        const hex = payload.slice(i + 1, i + 3);
        if (/^[0-9A-Fa-f]{2}$/.test(hex)) {
          octets.push(parseInt(hex, 16));
          i += 2;
          continue;
        }
      }
      octets.push(char.charCodeAt(0) & 0xff);
    }
    bytes = new Uint8Array(octets);
  }

  // Latin-1 code points are their byte values; no decoder needed.
  if (latin1) {
    return Array.from(bytes, (byte) => String.fromCharCode(byte)).join("");
  }

  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    // Not valid UTF-8 despite the charset; keep the encoded form to show the fault.
    return undefined;
  }
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
    // The only unstructured header this assembler writes.
    out += hdrText("Subject", h.subject);
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
 * A double quote would close the `filename="..."` form, and no escape is
 * emitted for an inner quote. These part headers sit inside the signed bytes
 * with no transfer encoding, so 8-bit octets are one a relay may rewrite.
 */
function assertAttachmentHeaderSafe(value: string, field: string): void {
  assertNoLineBreaks(value, field);
  if (value.includes('"')) {
    throw new Error(
      `${field} must not contain a double quote: ${JSON.stringify(value)}`,
    );
  }
  if (!isAscii(value)) {
    throw new Error(`${field} must be US-ASCII: ${JSON.stringify(value)}`);
  }
}

// ---------------------------------------------------------------------------
// Text part transfer encoding (RFC 2045 §6.7)
// ---------------------------------------------------------------------------
//
// A text part is `7bit` only when it is 7-bit; anything else is
// quoted-printable. The label is load-bearing: a relay rewrites a `7bit`
// part carrying 8-bit octets, changing the exact bytes the PGP/MIME
// signature covers.

/** The longest line quoted-printable permits, including the soft-break `=`. */
const QP_MAX_LINE = 76;

/** RFC 5322 §2.1.1: a line MUST NOT exceed 998 characters, excluding CRLF. */
const MAX_LINE_OCTETS = 998;

function hexEscape(byte: number): string {
  return `=${byte.toString(16).toUpperCase().padStart(2, "0")}`;
}

/**
 * Octets RFC 2045 §6.7 rule 2 permits literally: 33-60 and 62-126. `=` (61)
 * always escapes because it introduces an escape.
 */
function isQuotedPrintableLiteral(byte: number): boolean {
  return (byte >= 33 && byte <= 60) || (byte >= 62 && byte <= 126);
}

/**
 * True when the bytes are already a legal `7bit` body: US-ASCII only, no bare
 * CR or LF, no NUL, no line over the RFC 5322 limit.
 */
function isSevenBitClean(bytes: Uint8Array): boolean {
  let lineOctets = 0;
  for (let i = 0; i < bytes.length; i++) {
    const byte = bytes[i]!;
    if (byte === 0x0d && bytes[i + 1] === 0x0a) {
      i += 1;
      lineOctets = 0;
      continue;
    }
    if (byte === 0x00 || byte === 0x0d || byte === 0x0a || byte > 0x7f) {
      return false;
    }
    lineOctets += 1;
    if (lineOctets > MAX_LINE_OCTETS) return false;
  }
  return true;
}

/**
 * Quoted-printable encode one hard line. Space and tab ride literally except
 * at a line end (RFC 2045 §6.7 rule 3): a relay that strips trailing
 * whitespace would change the signed bytes. Soft breaks land between whole
 * tokens, never inside an `=XX` escape.
 */
function encodeQuotedPrintableLine(bytes: Uint8Array): string {
  let out = "";
  let column = 0;

  // Leave a column for the soft-break `=`; a token is one octet or a
  // three-octet hex escape, so it always fits once `column` is 0.
  const fits = (length: number): boolean =>
    column === 0 || column + length <= QP_MAX_LINE - 1;

  const softBreak = (): void => {
    out += `=${CRLF}`;
    column = 0;
  };

  const writeToken = (token: string): void => {
    if (!fits(token.length)) {
      const trailing: string[] = [];
      while (out.endsWith(" ") || out.endsWith("\t")) {
        const last = out.at(-1);
        if (last === undefined) break;
        trailing.push(last);
        out = out.slice(0, -1);
        column -= 1;
      }
      softBreak();
      for (const ws of trailing.reverse()) {
        const escaped = ws === " " ? "=20" : "=09";
        if (!fits(escaped.length)) softBreak();
        out += escaped;
        column += escaped.length;
      }
    }
    // The escaped whitespace may have filled the line it was moved onto;
    // that line ends in `0` or `9`, so one more break opens a fresh one.
    if (!fits(token.length)) softBreak();
    out += token;
    column += token.length;
  };

  for (let i = 0; i < bytes.length; i++) {
    const byte = bytes[i]!;
    const atEnd = i === bytes.length - 1;
    const token =
      byte === 0x20 || byte === 0x09
        ? atEnd
          ? hexEscape(byte)
          : String.fromCharCode(byte)
        : isQuotedPrintableLiteral(byte)
          ? String.fromCharCode(byte)
          : hexEscape(byte);
    writeToken(token);
  }
  return out;
}

function encodeQuotedPrintable(text: string): string {
  const encoder = new TextEncoder();
  return text
    .split(CRLF)
    .map((line) => encodeQuotedPrintableLine(encoder.encode(line)))
    .join(CRLF);
}

/**
 * Choose the transfer encoding that honestly describes a canonicalized text
 * body (already CRLF-normalized, trailing whitespace stripped per line).
 */
function encodeTextBody(canonical: string): {
  encoding: string;
  body: string;
} {
  if (isSevenBitClean(new TextEncoder().encode(canonical))) {
    return { encoding: "7bit", body: canonical };
  }
  return {
    encoding: "quoted-printable",
    body: encodeQuotedPrintable(canonical),
  };
}

/** CRLF-normalize and strip trailing whitespace per line (MIME canonical form). */
function canonicalizeText(text: string): string {
  return text
    .split(/\r\n|\r|\n/)
    .map((line) => line.replace(/[ \t]+$/, ""))
    .join(CRLF);
}

/** Encode bytes as base64, wrapped at 76 columns per RFC 2045. */
function base64Lines(bytes: Uint8Array): string {
  const b64 = base64Encode(bytes);
  const lines: string[] = [];
  for (let i = 0; i < b64.length; i += 76) {
    lines.push(b64.slice(i, i + 76));
  }
  return lines.join(CRLF);
}

/**
 * Assemble the signed content for a conversation message: always
 * multipart/mixed, one text/plain part (BODY[1.1]) then attachment parts
 * (BODY[1.2..N]) -- no bare text/plain branch, so writer, parser and the
 * signed-bytes contract share one form.
 */
function assembleConversationSignedPart(
  text: string,
  attachments: readonly MessageAttachment[] = [],
): Uint8Array {
  const boundary = generateBoundary();

  const text7bit = encodeTextBody(canonicalizeText(text));

  let body = `Content-Type: multipart/mixed; boundary="${boundary}"${CRLF}${CRLF}`;

  // Text part (BODY[1.1])
  body += `--${boundary}${CRLF}`;
  body += `Content-Type: text/plain; charset=utf-8${CRLF}`;
  body += `Content-Transfer-Encoding: ${text7bit.encoding}${CRLF}`;
  body += `${CRLF}`;
  body += `${text7bit.body}${CRLF}`;

  // Attachment parts (BODY[1.2..N])
  for (const att of attachments) {
    assertAttachmentHeaderSafe(att.contentType, "attachment contentType");
    assertAttachmentHeaderSafe(att.name, "attachment name");
    // A filename is one token; folding cannot bring it back under the limit.
    const typeLine = `Content-Type: ${att.contentType}`;
    const dispositionLine = `Content-Disposition: attachment; filename="${att.name}"`;
    if (typeLine.length > MAX_LINE_OCTETS) {
      throw new Error(
        `attachment contentType header exceeds ${MAX_LINE_OCTETS} octets`,
      );
    }
    if (dispositionLine.length > MAX_LINE_OCTETS) {
      throw new Error(
        `attachment name header exceeds ${MAX_LINE_OCTETS} octets`,
      );
    }
    body += `--${boundary}${CRLF}`;
    body += `${typeLine}${CRLF}`;
    body += `Content-Transfer-Encoding: base64${CRLF}`;
    body += `${dispositionLine}${CRLF}`;
    body += `${CRLF}`;
    body += `${base64Lines(att.data)}${CRLF}`;
  }

  body += `--${boundary}--${CRLF}`;
  return new TextEncoder().encode(body);
}

/** Assemble the signed content for a structured message (multipart/mixed). */
function assembleStructuredSignedPart(
  json: Record<string, unknown>,
  summary?: string,
): Uint8Array {
  const boundary = generateBoundary();
  // Encoded, not canonicalized: stripping trailing whitespace inside a JSON
  // string literal would change the value.
  const jsonPart = encodeTextBody(JSON.stringify(json));

  let body = `Content-Type: multipart/mixed; boundary="${boundary}"${CRLF}${CRLF}`;

  // JSON payload part
  body += `--${boundary}${CRLF}`;
  body += `Content-Type: application/vnd.interchange+json; charset=utf-8${CRLF}`;
  body += `Content-Transfer-Encoding: ${jsonPart.encoding}${CRLF}`;
  body += `${CRLF}`;
  body += `${jsonPart.body}${CRLF}`;

  // Optional human-readable summary
  if (summary !== undefined) {
    const summaryPart = encodeTextBody(canonicalizeText(summary));
    body += `--${boundary}${CRLF}`;
    body += `Content-Type: text/plain; charset=utf-8${CRLF}`;
    body += `Content-Transfer-Encoding: ${summaryPart.encoding}${CRLF}`;
    body += `${CRLF}`;
    body += `${summaryPart.body}${CRLF}`;
  }

  body += `--${boundary}--${CRLF}`;
  return new TextEncoder().encode(body);
}

/**
 * Wrap content and PGP signature into multipart/signed (RFC 3156 §5):
 * signed data first, then the detached signature in
 * application/pgp-signature.
 */
function wrapInMultipartSigned(
  signedContentBytes: Uint8Array,
  signatureBytes: Uint8Array,
  boundary: string,
): Uint8Array {
  const signedContent = new TextDecoder().decode(signedContentBytes);
  const signature = new TextDecoder().decode(signatureBytes);

  const enc = new TextEncoder();

  // RFC 2046: each part is preceded by CRLF + "--" + boundary + CRLF.
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
 * Assemble a complete RFC 2822 message from headers, signed content, and
 * signature bytes. The signature must be produced by signing the
 * `assembleSignedContent` output.
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
 * Build the signed content bytes the CryptoProvider signs: call this, sign
 * the result, then pass both to `assembleMessage`.
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
 * §2.3.8, §4.1.1.4); the blank line that ends the section is included, so
 * neither an LF-only section nor an `LF LF` separator survives.
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
 * immediately followed by another, whichever flavour either uses. Returns
 * null for a message carrying no blank line.
 *
 * Only `CRLF CRLF` may separate: admitting a bare pair lets a sender who
 * controls one field body end the section early and strip the fields after
 * it. `assertNoBareLineBreaks` then refuses the section unless CRLF wrote
 * it. A leading break is not a separator: a body part with no header fields
 * (legal under RFC 2046 §5.1.1) offers one.
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
 * Parse the header section of a raw RFC 2822 message: lowercase header names
 * to values, plus the byte offset where the body starts. Throws for a section
 * CRLF did not write. No blank line means all headers.
 */
export function parseHeaderSection(raw: Uint8Array): {
  headers: Map<string, string>;
  bodyOffset: number;
  headerEnd: number;
} {
  const headers = new Map<string, string>();

  // Byte-space scan so the returned offset stays valid for Uint8Array.slice()
  // when headers contain multi-byte UTF-8 characters.
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
 * Fold a bare CR or LF to a space: splitting on it would let a sender who
 * controls a field body append a field of its own. Both callers read a
 * region `parseHeaderSection` has already refused, so nothing reaches this
 * today.
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
    // A leading-WSP line continues nothing and names no field (RFC 2822 2.2).
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
 * Parse a multipart body into individual parts as raw bytes (headers + blank
 * line + body) for further parsing.
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
    // nextDelimIdx points at the \n before the delimiter; the part ends
    // before the preceding \r\n (or just \n).
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
 * Extract a MIME part by dot-separated IMAP path (RFC 9051): "1" is the
 * signed content part, "1.1" its first sub-part, "2" the signature part.
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
 * Parse an RFC 2822 address value (display-name form or bare) into JMAP
 * address objects, handling comma-separated lists.
 */
function parseAddressList(value: string): JMAPAddress[] {
  const results: JMAPAddress[] = [];
  // Split on commas that are not inside quoted strings or angle brackets.
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
 * Parse the Date header into an ISO 8601 string; null when missing or unparseable.
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
 * nest (§3.4.3); a backslash quotes the next character (§3.4.5).
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
 * The type a leaf part reports, applying RFC 2045 §6.4: an entity under an
 * unrecognized transfer encoding is treated as application/octet-stream. A
 * `multipart/*` wrapper is exempt -- its children are located from its
 * declared type.
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
 * Widen bytes into a string of one code unit per byte. No TextDecoder label
 * widens bytes: "latin1"/"iso-8859-1" label windows-1252, which maps 27 byte
 * values in 0x80-0x9f elsewhere. Widening runs a chunk at a time so one
 * `String.fromCharCode` per byte does not allocate a string per byte.
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
    // Decode escapes to octets first, then read those octets as UTF-8:
    // a multi-byte character arrives as several escapes.
    return {
      value: new TextDecoder("utf-8", { fatal: false }).decode(
        decodeQuotedPrintableBytes(raw),
      ),
      isEncodingProblem: false,
    };
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

/**
 * Undo quoted-printable into a string of one code unit per decoded OCTET;
 * not text, which a caller decodes under the part's charset.
 */
function decodeQuotedPrintable(text: string): string {
  return text
    .replace(/=\r\n/g, "")
    .replace(/=\n/g, "")
    .replace(/=([0-9A-Fa-f]{2})/g, (_match, hex: string) =>
      String.fromCharCode(parseInt(hex, 16)),
    );
}

/** The octets a quoted-printable body carries, narrowed from the widened form. */
function decodeQuotedPrintableBytes(text: string): Uint8Array {
  const widened = decodeQuotedPrintable(text);
  const out = new Uint8Array(widened.length);
  for (let i = 0; i < widened.length; i++) {
    out[i] = widened.charCodeAt(i);
  }
  return out;
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
 * Recursively walk MIME parts into body values and attachment lists.
 * `partPath` uses IMAP-style dot-separated numbering ("1", "1.1", "2.3").
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

  // Listed under the declared type but reported under the relabeled one, so
  // a consumer walking `textBody` still learns the part exists.
  if (declaredMime === "text/plain") {
    ctx.textBody.push({ partId: partPath, type: reportedMime });
  } else if (declaredMime === "text/html") {
    ctx.htmlBody.push({ partId: partPath, type: reportedMime });
  }
}

/**
 * Convert raw MIME bytes into a JMAP Email-shaped object: text/plain,
 * multipart/mixed and multipart/signed (RFC 3156). Signature verification is
 * not performed. `mailId` generates blob IDs.
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
    // RFC 3156: part 1 is the signed content, part 2 the signature.
    const boundary = extractBoundary(contentType);
    if (boundary !== undefined) {
      const outerParts = parseMultipart(body, boundary);
      const contentPart = outerParts[0];
      if (contentPart !== undefined) {
        // The content part may be text/plain or multipart/mixed; walk it as "1".
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
    // Reconstruct minimal part bytes so parseMimePart works; the message's
    // own Content-Transfer-Encoding travels with the type.
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

  // The agent reads this field as text. `decodeMail` keeps the wire form.
  const rawSubject = msgHeaders.get("subject");

  return {
    from: parseAddressList(msgHeaders.get("from") ?? ""),
    to: parseAddressList(msgHeaders.get("to") ?? ""),
    subject: rawSubject === undefined ? null : decodeHeaderText(rawSubject),
    sentAt: parseDateHeader(msgHeaders.get("date")),
    bodyValues: ctx.bodyValues,
    textBody: ctx.textBody,
    htmlBody: ctx.htmlBody,
    attachments: ctx.attachments,
    headers: interchangeHeaders,
  };
}

/**
 * Decode a MIME part body into raw bytes. A malformed base64 body throws; an
 * unrecognized mechanism does not (RFC 2045 §6.4).
 *
 * Limitation: the quoted-printable branch reads wire text as UTF-8 before
 * undoing escapes, so a raw 8-bit byte in a quoted-printable body becomes a
 * replacement character truncated to one byte. Escaped bytes round-trip
 * exactly.
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
    return decodeQuotedPrintableBytes(raw);
  }

  // Identity encoding, including anything unrecognized (RFC 2045 §6.4).
  return body;
}

/**
 * Extract conversation attachments from raw message bytes as decoded
 * `MessageAttachment[]`, each with the IMAP part path of its sibling (`part`)
 * matching `extractPartByPath` numbering. Returns [] for any shape without
 * attachment parts.
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
  for (const [index, subPartBytes] of parseMultipart(
    signed.body,
    innerBoundary,
  ).entries()) {
    const subPart = parseMimePart(subPartBytes);
    if (!isAttachmentPart(subPart.contentType, subPart.headers)) continue;
    attachments.push({
      name: extractFilename(subPart.headers) ?? "attachment",
      contentType: reportedContentType(
        extractContentTypeMime(subPart.contentType),
        subPart.headers,
      ),
      data: decodePartBytes(subPart.body, subPart.headers),
      part: `1.${String(index + 1)}`,
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
 * Build the typed `MessageHeaders` subset from a parsed header map. Optional
 * fields are included only when present (exactOptionalPropertyTypes-safe);
 * the lossless header set is carried separately as `rawHeaders`.
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

  // A blank or unparseable header is an absence, not a value: RFC 5322 admits
  // no empty `Date` body, and a consumer comparing an Invalid Date to a
  // window's bounds gets false for every window.
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

  // Decoded: a subject is text to read, not RFC 2047 transport form.
  const subject = headers.get("subject");
  if (subject !== undefined) result.subject = decodeHeaderText(subject);

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
 * Parse the header section into a lossless map of lowercased name to ordered
 * values. Repeated headers keep all occurrences; folded continuations unfold
 * onto the preceding header. Bounded by `headerEnd` so the body is never
 * decoded here.
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
  // `__proto__`/`constructor` are legal field names that collide with
  // Object.prototype members, so the accumulator is a Map.
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
    // A blank line here is leading (headerEnd bounds the section).
    if (line === "") continue;
    // Nothing to continue means no field (RFC 2822 2.2), not a field of its own.
    if (line.startsWith(" ") || line.startsWith("\t")) {
      if (current !== null) {
        // The leading whitespace is the fold; unfolding replaces that run
        // with one space. Trailing whitespace is content and stays.
        const body = line.replace(/^[ \t]+/, "");
        current.value += ` ${body}`;
      }
      continue;
    }
    const idx = line.indexOf(":");
    if (idx === -1) continue;
    flush();
    current = { name: line.slice(0, idx), value: line.slice(idx + 1) };
  }
  flush();
  // Null prototype: an absent name must resolve to nothing.
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
 * Recursively collect the decoded leaf parts of a MIME part. The PGP/MIME
 * signature part is transport plumbing, not content, so it is skipped --
 * which unwraps the multipart/signed envelope for free.
 */
function collectLeafParts(partBytes: Uint8Array): MessagePart[] {
  const part = parseMimePart(partBytes);
  const mime = extractContentTypeMime(part.contentType);
  if (mime === "application/pgp-signature") return [];
  if (mime.startsWith("multipart/")) {
    const boundary = extractBoundary(part.contentType);
    // A multipart part with no boundary is undecodable; silently returning
    // [] would break the lossless contract.
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
 * (signature and multipart wrappers removed). This is the in-memory form; a
 * caller commits each part's bytes to durable storage to produce a
 * JSON-safe `Mail`. Shared by the standalone and deployed ingest paths.
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
