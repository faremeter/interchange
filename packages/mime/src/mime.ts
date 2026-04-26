/* eslint-disable @typescript-eslint/no-non-null-assertion -- MIME parser uses bounded array access throughout */
/**
 * MIME byte construction and parsing for Interchange messages.
 *
 * Implements exactly two message shapes per MESSAGE.md:
 *   1. Conversation: text/plain in multipart/signed
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

import { randomUUID } from "node:crypto";

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

export function generateMessageId(address: string): string {
  const domain = address.includes("@") ? address.split("@")[1]! : "local";
  const uuid = randomUUID();
  return `<${uuid}@${domain}>`;
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
  return `${name}: ${value}${CRLF}`;
}

function serializeMessageHeaders(
  h: MessageHeaders,
  contentType: string,
): string {
  let out = "";
  out += hdr("From", h.from);
  out += hdr("To", Array.isArray(h.to) ? h.to.join(", ") : (h.to as string));
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

/**
 * Assemble the signed content for a conversation message (text/plain).
 *
 * This is the exact bytes that will be hashed for the PGP/MIME signature.
 * Content-Transfer-Encoding: 7bit (conversation messages are ASCII).
 */
function assembleConversationSignedPart(text: string): Uint8Array {
  // Canonicalize: CRLF line endings, strip trailing whitespace per line.
  const lines = text.split(/\r\n|\r|\n/);
  const canonLines = lines.map((l) => l.replace(/[ \t]+$/, ""));
  const canonical = canonLines.join(CRLF);

  const partHeaders =
    `Content-Type: text/plain; charset=utf-8${CRLF}` +
    `Content-Transfer-Encoding: 7bit${CRLF}`;
  const body = `${partHeaders}${CRLF}${canonical}`;
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
    return assembleConversationSignedPart(content.text);
  }
  return assembleStructuredSignedPart(content.json, content.summary);
}

// ---------------------------------------------------------------------------
// MIME parsing (for fetchHeaders, fetchStructure, fetchPart, fetchFull)
// ---------------------------------------------------------------------------

const CRLF_CRLF = new Uint8Array([0x0d, 0x0a, 0x0d, 0x0a]);
const LF_LF = new Uint8Array([0x0a, 0x0a]);

function findByteSequence(haystack: Uint8Array, needle: Uint8Array): number {
  if (needle.length === 0) return 0;
  const limit = haystack.length - needle.length;
  outer: for (let i = 0; i <= limit; i++) {
    for (let j = 0; j < needle.length; j++) {
      if (haystack[i + j] !== needle[j]) continue outer;
    }
    return i;
  }
  return -1;
}

/**
 * Parse the header section of a raw RFC 2822 message.
 * Returns a map of lowercase header names to their values, and the
 * byte offset where the body starts.
 */
export function parseHeaderSection(raw: Uint8Array): {
  headers: Map<string, string>;
  bodyOffset: number;
} {
  const headers = new Map<string, string>();

  // Search for the blank line separator in byte space so the returned
  // offset is valid for Uint8Array.slice() even when headers contain
  // multi-byte UTF-8 characters.
  const crlfIdx = findByteSequence(raw, CRLF_CRLF);
  const lfIdx = findByteSequence(raw, LF_LF);

  let bodyOffset = raw.length;
  let headerEnd = raw.length;

  if (crlfIdx !== -1 && (lfIdx === -1 || crlfIdx <= lfIdx)) {
    headerEnd = crlfIdx;
    bodyOffset = crlfIdx + 4;
  } else if (lfIdx !== -1) {
    headerEnd = lfIdx;
    bodyOffset = lfIdx + 2;
  }

  const headerText = new TextDecoder("utf-8", { fatal: false }).decode(
    raw.subarray(0, headerEnd),
  );
  parseHeaders(headerText, headers);

  return { headers, bodyOffset };
}

function parseHeaders(headerSection: string, out: Map<string, string>): void {
  // Unfold continuation lines (lines starting with whitespace per RFC 2822).
  const unfolded = headerSection
    .replace(/\r\n[ \t]+/g, " ")
    .replace(/\n[ \t]+/g, " ");
  const lines = unfolded.split(/\r\n|\n/);
  for (const line of lines) {
    if (line.trim() === "") continue;
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
 * Decode a MIME body part, handling Content-Transfer-Encoding.
 */
function decodeBodyBytes(
  body: Uint8Array,
  headers: Map<string, string>,
): { value: string; isEncodingProblem: boolean } {
  const cte = (headers.get("content-transfer-encoding") ?? "7bit")
    .trim()
    .toLowerCase();

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

  // 7bit, 8bit, binary — decode as UTF-8
  return {
    value: new TextDecoder("utf-8", { fatal: false }).decode(body),
    isEncodingProblem: false,
  };
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

function extractContentTypeMime(contentType: string): string {
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
  const mime = extractContentTypeMime(part.contentType);

  if (mime.startsWith("multipart/")) {
    const boundary = extractBoundary(part.contentType);
    if (boundary === undefined) return;
    const subParts = parseMultipart(part.body, boundary);
    subParts.forEach((subPartBytes, idx) => {
      walkMimePart(subPartBytes, `${partPath}.${idx + 1}`, ctx);
    });
    return;
  }

  if (isAttachmentPart(part.contentType, part.headers)) {
    const blobId = `blob_${ctx.mailId}_${partPath}`;
    ctx.attachments.push({
      blobId,
      name: extractFilename(part.headers),
      type: mime,
      size: part.body.length,
    });
    return;
  }

  const decoded = decodeBodyBytes(part.body, part.headers);
  ctx.bodyValues[partPath] = decoded;

  if (mime === "text/plain") {
    ctx.textBody.push({ partId: partPath, type: mime });
  } else if (mime === "text/html") {
    ctx.htmlBody.push({ partId: partPath, type: mime });
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
    // Single-part message (e.g. text/plain).
    // Reconstruct minimal part bytes with content-type header so parseMimePart works.
    const enc = new TextEncoder();
    const ctHeader = `Content-Type: ${contentType}\r\n\r\n`;
    const partBytes = new Uint8Array(enc.encode(ctHeader).length + body.length);
    partBytes.set(enc.encode(ctHeader), 0);
    partBytes.set(body, enc.encode(ctHeader).length);
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
