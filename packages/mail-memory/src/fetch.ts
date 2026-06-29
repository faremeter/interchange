/* eslint-disable @typescript-eslint/no-non-null-assertion -- MIME multipart parsing with bounds checks */
import { type } from "arktype";
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
  extractPartByPath,
  extractAttachments,
} from "@intx/mime";
import { buildMessageHeaders } from "./headers";
import { verifyDetachedSignature } from "@intx/crypto";

const MessagePayload = type({
  type: InterchangeType,
  version: "string",
  body: "Record<string, unknown>",
});

/**
 * Parse raw RFC 2822 headers from a stored message.
 */
export function fetchHeaders(
  ref: MessageRef,
  store: MailboxStore,
): MessageHeaders {
  const msg = requireMessage(store, ref.uid, ref.mailbox);
  const { headers } = parseHeaderSection(msg.raw);
  return buildMessageHeaders(headers);
}

/**
 * Compute the MIME tree structure (BODYSTRUCTURE) without transferring content.
 */
export function fetchStructure(
  ref: MessageRef,
  store: MailboxStore,
): BodyStructure {
  const msg = requireMessage(store, ref.uid, ref.mailbox);
  const { headers, bodyOffset } = parseHeaderSection(msg.raw);
  const body = msg.raw.slice(bodyOffset);
  const contentType = headers.get("content-type") ?? "application/octet-stream";
  return buildStructure(body, contentType);
}

/**
 * Fetch a single MIME part by dot-separated path.
 */
export function fetchPart(
  ref: MessageRef,
  partPath: string,
  store: MailboxStore,
): MessagePart {
  const msg = requireMessage(store, ref.uid, ref.mailbox);
  const partBytes = extractPartByPath(msg.raw, partPath);
  const part = parseMimePart(partBytes);

  const enc = part.headers.get("content-transfer-encoding") ?? "7bit";
  let content: Uint8Array;

  if (enc.toLowerCase() === "base64") {
    const b64 = new TextDecoder().decode(part.body).replace(/\s/g, "");
    content = new Uint8Array(Buffer.from(b64, "base64"));
  } else {
    content = part.body;
  }

  const result: MessagePart = {
    contentType: part.contentType,
    content,
  };
  if (enc !== "7bit") result.encoding = enc;
  return result;
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
  const { headers } = parseHeaderSection(msg.raw);
  const parsedHeaders = buildMessageHeaders(headers);

  const rawType = parsedHeaders.interchangeType;
  const isConversation =
    rawType === "conversation.message" ||
    rawType === "conversation.join" ||
    rawType === "conversation.leave" ||
    rawType === undefined;

  const signatureStatus = await verifyMessageSignature(
    msg.raw,
    parsedHeaders.from,
    getCrypto,
  );

  const result: InboundMessage = {
    ref,
    headers: parsedHeaders,
    flags: Array.from(msg.flags),
    signatureStatus,
  };

  try {
    if (isConversation) {
      const part1 = parseMimePart(extractPartByPath(msg.raw, "1"));
      const part1Mime = part1.contentType.split(";")[0]!.trim().toLowerCase();
      if (part1Mime.startsWith("multipart/")) {
        // Conversation shape: multipart/mixed with the text body at 1.1.
        const textPart = parseMimePart(extractPartByPath(msg.raw, "1.1"));
        result.content = new TextDecoder("utf-8", { fatal: false }).decode(
          textPart.body,
        );
      } else {
        // A conversation message is "literally a signed email", so a sender
        // (e.g. a plain mail client) may sign a bare text/plain part with no
        // multipart/mixed wrapper. This branch reads that body directly. Our
        // own assembler always emits multipart/mixed; without this branch a
        // bare text/plain message would fail the 1.1 lookup and silently lose
        // its content to the catch below.
        result.content = new TextDecoder("utf-8", { fatal: false }).decode(
          part1.body,
        );
      }
    } else {
      // Structured messages carry their JSON payload at 1.1. Attachments on
      // structured messages are intentionally not parsed: they have no
      // producer today, so parsing them would handle a shape nobody sends.
      const part11Bytes = extractPartByPath(msg.raw, "1.1");
      const part11 = parseMimePart(part11Bytes);
      const jsonText = new TextDecoder("utf-8", { fatal: false }).decode(
        part11.body,
      );
      const validated = MessagePayload(JSON.parse(jsonText));
      if (validated instanceof type.errors) {
        throw new Error(`invalid message payload: ${validated.summary}`);
      }
      result.payload = validated;
    }
  } catch {
    // If we can't parse the content, return what we have with the signature status.
  }

  // Attachment parsing is deliberately outside the catch above: a malformed
  // attachment must surface as a thrown error, not be silently dropped.
  if (isConversation) {
    const attachments = extractAttachments(msg.raw);
    if (attachments.length > 0) {
      result.attachments = attachments;
    }
  }

  return result;
}

async function verifyMessageSignature(
  raw: Uint8Array,
  fromAddress: string,
  getCrypto: (fromAddress: string) => CryptoProvider | undefined,
): Promise<SignatureStatus> {
  const senderCrypto = getCrypto(fromAddress);
  if (senderCrypto === undefined) {
    return "unknown";
  }

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

    const publicKey = senderCrypto.getPublicKey();
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
