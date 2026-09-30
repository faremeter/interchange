import { type } from "arktype";
import { getLogger } from "@intx/log";
import type {
  MessageHeaders,
  BodyStructure,
  MessagePart,
  InboundMessage,
  SignatureStatus,
  CryptoProvider,
  MessageRef,
} from "@intx/types/runtime";
import { InterchangeType } from "@intx/types/runtime";
import type { MailboxStore } from "./mailbox";
import { requireMessage } from "./mailbox";
import {
  parseHeaderSection,
  parseMimePart,
  extractBoundary,
  parseMultipart,
  extractContentTypeMime,
  extractPartByPath,
  extractAttachments,
  buildMessageHeaders,
  decodePartBytes,
  isRecognizedTransferEncoding,
  reportedContentType,
  transferEncodingMechanism,
} from "@intx/mime";
import type { ParsedMimePart } from "@intx/mime";
import { verifyMimeSignature } from "./verify-signature";

const logger = getLogger(["interchange", "mailbox", "fetch"]);

const MessagePayload = type({
  type: InterchangeType,
  version: "string",
  body: "Record<string, unknown>",
});

/**
 * Parse the full RFC 2822 headers of a stored message. Reads the message's raw
 * bytes on demand: the parsed set is a superset of the pre-parsed envelope (it
 * carries `cc`, `mimeVersion`, trace headers, ...), so it cannot be served from
 * the envelope metadata alone.
 */
export async function fetchHeaders(
  ref: MessageRef,
  store: MailboxStore,
): Promise<MessageHeaders> {
  requireMessage(store, ref.uid, ref.mailbox);
  const raw = await store.readRaw(ref.uid);
  const { headers } = parseHeaderSection(raw);
  return buildMessageHeaders(headers);
}

/**
 * Compute the MIME tree structure (BODYSTRUCTURE) without transferring content.
 */
export async function fetchStructure(
  ref: MessageRef,
  store: MailboxStore,
): Promise<BodyStructure> {
  requireMessage(store, ref.uid, ref.mailbox);
  const raw = await store.readRaw(ref.uid);
  const { headers, bodyOffset } = parseHeaderSection(raw);
  const body = raw.slice(bodyOffset);
  const contentType = headers.get("content-type") ?? "application/octet-stream";
  return buildStructure(body, contentType);
}

/**
 * Fetch a single MIME part by dot-separated path. `contentType` carries the
 * RFC 2045 §6.4 relabel, since an undecodable part arrives undecoded.
 */
export async function fetchPart(
  ref: MessageRef,
  partPath: string,
  store: MailboxStore,
): Promise<MessagePart> {
  requireMessage(store, ref.uid, ref.mailbox);
  const raw = await store.readRaw(ref.uid);
  const partBytes = extractPartByPath(raw, partPath);
  const part = parseMimePart(partBytes);

  const result: MessagePart = {
    contentType: reportedContentType(part.contentType, part.headers),
    content: decodePartBytes(part.body, part.headers),
  };
  const mechanism = transferEncodingMechanism(part.headers);
  if (mechanism !== "7bit") result.encoding = mechanism;
  return result;
}

/**
 * Decode a leaf part's body into text, or undefined when the octets are not
 * text: an unrecognized mechanism (RFC 2045 §6.4) or encoded data that will not
 * decode. The octets stay reachable through `fetchPart`, which reports them
 * under the §6.4 relabel.
 */
function decodePartText(
  part: ParsedMimePart,
  ref: MessageRef,
): string | undefined {
  if (!isRecognizedTransferEncoding(transferEncodingMechanism(part.headers))) {
    return undefined;
  }
  let decoded: Uint8Array;
  try {
    decoded = decodePartBytes(part.body, part.headers);
  } catch (cause) {
    // Absent text is the answer, but it is not a self-explaining one: a caller
    // reading `undefined` cannot tell "not text" from "would not decode", and
    // an operator would otherwise never learn that a peer sends bodies its own
    // declared encoding does not describe. The record names the message so the
    // octets can be read back through `fetchPart`.
    logger.warn`Message uid=${ref.uid} in mailbox ${ref.mailbox} carries a ${part.contentType} part whose ${transferEncodingMechanism(part.headers)} body did not decode; delivering it without text: ${cause instanceof Error ? cause.message : String(cause)}`;
    return undefined;
  }
  return new TextDecoder("utf-8", { fatal: false }).decode(decoded);
}

/**
 * The leaf part a message's content lives in: part 1.1 under our assembler's
 * multipart/mixed wrapper, part 1 when a peer signed a bare part, and the
 * message itself when it is not multipart at the top level.
 */
function resolveContentPart(
  raw: Uint8Array,
  messageHeaders: Map<string, string>,
  bodyOffset: number,
): ParsedMimePart {
  const declared = messageHeaders.get("content-type") ?? "text/plain";
  if (!extractContentTypeMime(declared).startsWith("multipart/")) {
    return {
      contentType: declared,
      headers: messageHeaders,
      body: raw.slice(bodyOffset),
    };
  }
  const part1 = parseMimePart(extractPartByPath(raw, "1"));
  if (!extractContentTypeMime(part1.contentType).startsWith("multipart/")) {
    return part1;
  }
  return parseMimePart(extractPartByPath(raw, "1.1"));
}

/**
 * Fetch a complete message, verify its PGP/MIME signature, and return
 * a fully parsed InboundMessage.
 */
export async function fetchFull(
  ref: MessageRef,
  store: MailboxStore,
  getCrypto: (fromAddress: string) => CryptoProvider | undefined,
): Promise<InboundMessage> {
  const msg = requireMessage(store, ref.uid, ref.mailbox);
  const raw = await store.readRaw(ref.uid);
  const { headers, bodyOffset } = parseHeaderSection(raw);
  const parsedHeaders = buildMessageHeaders(headers);

  const rawType = parsedHeaders.interchangeType;
  const isConversation =
    rawType === "conversation.message" ||
    rawType === "conversation.join" ||
    rawType === "conversation.leave" ||
    rawType === undefined;

  const signatureStatus = await verifyMessageSignature(
    raw,
    parsedHeaders.from,
    getCrypto,
  );

  const result: InboundMessage = {
    ref,
    headers: parsedHeaders,
    flags: Array.from(msg.flags),
    signatureStatus,
  };

  // A body that did not decode is not text, and both destinations here are
  // text; the message is delivered without one rather than refused.
  const text = decodePartText(
    resolveContentPart(raw, headers, bodyOffset),
    ref,
  );
  if (text !== undefined) {
    if (isConversation) {
      result.content = text;
    } else {
      // Attachments on structured messages are intentionally not parsed: they
      // have no producer today.
      const validated = MessagePayload(JSON.parse(text));
      if (validated instanceof type.errors) {
        throw new Error(`invalid message payload: ${validated.summary}`);
      }
      result.payload = validated;
    }
  }

  // A malformed attachment throws rather than being silently dropped.
  if (isConversation) {
    const attachments = extractAttachments(raw);
    if (attachments.length > 0) {
      result.attachments = attachments;
    }
  }

  return result;
}

async function verifyMessageSignature(
  raw: Uint8Array,
  fromAddress: string | undefined,
  getCrypto: (fromAddress: string) => CryptoProvider | undefined,
): Promise<SignatureStatus> {
  // No originator names no key to verify against, which is the same position
  // as a sender we hold no key for.
  if (fromAddress === undefined) {
    return "unknown";
  }
  const senderCrypto = getCrypto(fromAddress);
  if (senderCrypto === undefined) {
    return "unknown";
  }

  return verifyMimeSignature(raw, senderCrypto.getPublicKey());
}

function buildStructure(body: Uint8Array, contentType: string): BodyStructure {
  const ct = contentType.toLowerCase();
  if (!ct.startsWith("multipart/")) {
    return { contentType, size: body.length };
  }

  const boundary = extractBoundary(contentType);
  if (boundary === undefined) {
    return { contentType, size: body.length };
  }

  const parts = parseMultipart(body, boundary);
  const subStructures: BodyStructure[] = parts.map((partBytes) => {
    const { headers, bodyOffset } = parseHeaderSection(partBytes);
    const partBody = partBytes.slice(bodyOffset);
    const partContentType =
      headers.get("content-type") ?? "application/octet-stream";
    return buildStructure(partBody, partContentType);
  });

  return { contentType, parts: subStructures };
}
