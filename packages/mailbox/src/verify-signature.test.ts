import { describe, test, expect } from "bun:test";
import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  generateMessageId,
  type MessageHeaders,
} from "@intx/mime";
import { verifyMimeSignature } from "./verify-signature";

function conversationHeaders(): MessageHeaders {
  return {
    from: "alpha@test.interchange",
    to: ["beta@test.interchange"],
    cc: undefined,
    date: new Date("2026-01-15T12:00:00Z"),
    messageId: generateMessageId("alpha@test.interchange"),
    subject: undefined,
    inReplyTo: undefined,
    references: undefined,
    mimeVersion: "1.0",
    interchangeType: "conversation.message",
    interchangeCorrelationId: undefined,
    interchangeTenantId: undefined,
    interchangeAgentId: undefined,
    interchangeSessionId: undefined,
    interchangeOfferingId: undefined,
    interchangeSchemaVersion: undefined,
    traceparent: undefined,
    tracestate: undefined,
  };
}

/** Build a validly-signed `multipart/signed` conversation message. */
async function signedMessage(
  crypto: Awaited<ReturnType<typeof makeCrypto>>,
  text: string,
): Promise<Uint8Array> {
  const content = assembleSignedContent({ kind: "conversation", text });
  const sig = await createDetachedSignatureFromProvider(content, crypto);
  return assembleMessage(conversationHeaders(), content, sig);
}

async function makeCrypto() {
  return createEd25519Crypto(await generateKeyPair());
}

describe("verifyMimeSignature", () => {
  test("returns valid for a message signed by the given key", async () => {
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, "hello world");

    const status = await verifyMimeSignature(raw, crypto.getPublicKey());
    expect(status).toBe("valid");
  });

  test("returns invalid when checked against a different key", async () => {
    const signer = await makeCrypto();
    const other = await makeCrypto();
    const raw = await signedMessage(signer, "hello world");

    const status = await verifyMimeSignature(raw, other.getPublicKey());
    expect(status).toBe("invalid");
  });

  test("returns invalid, not a throw, for a corrupt signature part", async () => {
    // Signature verification must never let a malformed signature escape as an
    // exception: a corrupt `application/pgp-signature` part is a verdict
    // ("invalid"), not an error the caller has to catch.
    const crypto = await makeCrypto();
    const content = assembleSignedContent({
      kind: "conversation",
      text: "hello world",
    });
    const raw = assembleMessage(
      conversationHeaders(),
      content,
      new TextEncoder().encode("not a valid pgp signature block"),
    );

    const status = await verifyMimeSignature(raw, crypto.getPublicKey());
    expect(status).toBe("invalid");
  });

  test("returns missing for a message that is not multipart/signed", async () => {
    const crypto = await makeCrypto();
    const raw = new TextEncoder().encode(
      [
        "From: alpha@test.interchange",
        "To: beta@test.interchange",
        "Subject: plain",
        "Content-Type: text/plain",
        "",
        "not signed",
      ].join("\r\n"),
    );

    const status = await verifyMimeSignature(raw, crypto.getPublicKey());
    expect(status).toBe("missing");
  });
});
