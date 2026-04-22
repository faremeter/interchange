/* eslint-disable @typescript-eslint/no-non-null-assertion -- MIME multipart parsing with bounds checks */
import type {
  MessageHeaders,
  BodyStructure,
  MessagePart,
  InboundMessage,
  SignatureStatus,
  InterchangeType,
  CryptoProvider,
  MessageRef,
} from "@interchange/types/runtime";
import type { MailboxStore } from "./mailbox";
import { requireMessage } from "./mailbox";
import {
  parseHeaderSection,
  parseMimePart,
  extractBoundary,
  parseMultipart,
  extractPartByPath,
} from "@interchange/mime";
import { buildMessageHeaders } from "./headers";
import { verifyDetachedSignature } from "@interchange/crypto-node";

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
  cryptoProviders: Map<string, CryptoProvider>,
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
    cryptoProviders,
  );

  const result: InboundMessage = {
    ref,
    headers: parsedHeaders,
    flags: Array.from(msg.flags),
    signatureStatus,
  };

  try {
    if (isConversation) {
      const part1Bytes = extractPartByPath(msg.raw, "1");
      const part1 = parseMimePart(part1Bytes);
      result.content = new TextDecoder("utf-8", { fatal: false }).decode(
        part1.body,
      );
    } else {
      const part11Bytes = extractPartByPath(msg.raw, "1.1");
      const part11 = parseMimePart(part11Bytes);
      const jsonText = new TextDecoder("utf-8", { fatal: false }).decode(
        part11.body,
      );
      const parsed = JSON.parse(jsonText) as {
        type: InterchangeType;
        version: string;
        body: Record<string, unknown>;
      };
      result.payload = parsed;
    }
  } catch {
    // If we can't parse the content, return what we have with the signature status.
  }

  return result;
}

async function verifyMessageSignature(
  raw: Uint8Array,
  fromAddress: string,
  cryptoProviders: Map<string, CryptoProvider>,
): Promise<SignatureStatus> {
  const senderCrypto = cryptoProviders.get(fromAddress);
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
