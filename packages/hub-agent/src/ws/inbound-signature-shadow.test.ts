import { describe, test, expect } from "bun:test";
import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  generateMessageId,
  type MessageHeaders,
} from "@intx/mime";
import { hexEncode } from "@intx/types";

import { shadowVerifyInboundSignature } from "./inbound-signature-shadow";

const AGENT_ADDRESS = "run_anchor@tenant.example";

function headersFrom(from: string): MessageHeaders {
  return {
    from,
    to: ["beta@test.interchange"],
    cc: undefined,
    date: new Date("2026-01-15T12:00:00Z"),
    messageId: generateMessageId(from),
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

async function makeCrypto() {
  return createEd25519Crypto(await generateKeyPair());
}

/** Build a validly-signed `multipart/signed` conversation message. */
async function signedMessage(
  crypto: Awaited<ReturnType<typeof makeCrypto>>,
  from: string,
  text = "hello world",
): Promise<Uint8Array> {
  const content = assembleSignedContent({ kind: "conversation", text });
  const sig = await createDetachedSignatureFromProvider(content, crypto);
  return assembleMessage(headersFrom(from), content, sig);
}

function hexKey(crypto: Awaited<ReturnType<typeof makeCrypto>>): string {
  return hexEncode(crypto.getPublicKey());
}

describe("shadowVerifyInboundSignature", () => {
  test("valid signature whose From matches the stamp: valid/match", async () => {
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, sender);

    const verdict = await shadowVerifyInboundSignature({
      raw,
      authenticatedSender: sender,
      authenticatedSenderPublicKey: hexKey(crypto),
      messageId: "mid-valid",
      agentAddress: AGENT_ADDRESS,
    });

    expect(verdict.signature).toBe("valid");
    expect(verdict.fromMatch).toBe("match");
    expect(verdict.messageFrom).toBe(sender);
  });

  test("signature against a different key: invalid, From unchecked", async () => {
    const sender = "alpha@test.interchange";
    const signer = await makeCrypto();
    const other = await makeCrypto();
    const raw = await signedMessage(signer, sender);

    const verdict = await shadowVerifyInboundSignature({
      raw,
      authenticatedSender: sender,
      authenticatedSenderPublicKey: hexKey(other),
      messageId: "mid-invalid",
      agentAddress: AGENT_ADDRESS,
    });

    expect(verdict.signature).toBe("invalid");
    // A From-binding is only meaningful atop a valid signature.
    expect(verdict.fromMatch).toBe("unchecked");
  });

  test("message that is not multipart/signed: missing", async () => {
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = new TextEncoder().encode(
      [
        `From: ${sender}`,
        "To: beta@test.interchange",
        "Subject: plain",
        "Content-Type: text/plain",
        "",
        "not signed",
      ].join("\r\n"),
    );

    const verdict = await shadowVerifyInboundSignature({
      raw,
      authenticatedSender: sender,
      authenticatedSenderPublicKey: hexKey(crypto),
      messageId: "mid-missing",
      agentAddress: AGENT_ADDRESS,
    });

    expect(verdict.signature).toBe("missing");
    expect(verdict.fromMatch).toBe("unchecked");
  });

  test("valid signature under a forged From: valid/mismatch", async () => {
    // The message is genuinely signed by `signer`'s key (so the hub stamps
    // `signer`'s address + key), but its visible From claims a different
    // sender. A valid signature over a borrowed display identity is the
    // forgery this binding catches.
    const signer = "alpha@test.interchange";
    const forgedFrom = "victim@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, forgedFrom);

    const verdict = await shadowVerifyInboundSignature({
      raw,
      authenticatedSender: signer,
      authenticatedSenderPublicKey: hexKey(crypto),
      messageId: "mid-forged",
      agentAddress: AGENT_ADDRESS,
    });

    expect(verdict.signature).toBe("valid");
    expect(verdict.fromMatch).toBe("mismatch");
    expect(verdict.messageFrom).toBe(forgedFrom);
  });

  test("null sender key: unknown, admitted", async () => {
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, sender);

    const verdict = await shadowVerifyInboundSignature({
      raw,
      authenticatedSender: sender,
      authenticatedSenderPublicKey: null,
      messageId: "mid-null",
      agentAddress: AGENT_ADDRESS,
    });

    expect(verdict.signature).toBe("unknown");
    expect(verdict.fromMatch).toBe("unchecked");
  });

  test("malformed hex key degrades to a distinct error verdict", async () => {
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, sender);

    const verdict = await shadowVerifyInboundSignature({
      raw,
      authenticatedSender: sender,
      authenticatedSenderPublicKey: "not-hex",
      messageId: "mid-badhex",
      agentAddress: AGENT_ADDRESS,
    });

    expect(verdict.signature).toBe("error");
    expect(verdict.fromMatch).toBe("unchecked");
  });

  test("well-formed hex of the wrong length is an error, not a crash", async () => {
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, sender);

    const verdict = await shadowVerifyInboundSignature({
      raw,
      authenticatedSender: sender,
      // 16 bytes of valid hex -- half an Ed25519 key.
      authenticatedSenderPublicKey: "ab".repeat(16),
      messageId: "mid-shortkey",
      agentAddress: AGENT_ADDRESS,
    });

    expect(verdict.signature).toBe("error");
  });

  test("a display-name From still binds to the stamp addr-spec", async () => {
    // extractAddrSpec strips the display name, so `Alpha <a@b>` binds to the
    // bare stamp `a@b` without a false mismatch.
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, "Alpha <alpha@test.interchange>");

    const verdict = await shadowVerifyInboundSignature({
      raw,
      authenticatedSender: "alpha@test.interchange",
      authenticatedSenderPublicKey: hexKey(crypto),
      messageId: "mid-display",
      agentAddress: AGENT_ADDRESS,
    });

    expect(verdict.signature).toBe("valid");
    expect(verdict.fromMatch).toBe("match");
    expect(verdict.messageFrom).toBe("alpha@test.interchange");
  });

  test("an unparseable multi-address From stays unchecked, not a false mismatch", async () => {
    // A shadow must not turn a From it cannot reduce to one addr-spec into a
    // forgery verdict -- that would poison the corpus (and later drop
    // legitimate mail under enforcement). extractAddrSpec rejects the two-@
    // input; the binding degrades to unchecked while the signature stands.
    const crypto = await makeCrypto();
    const raw = await signedMessage(
      crypto,
      "alpha@test.interchange, beta@test.interchange",
    );

    const verdict = await shadowVerifyInboundSignature({
      raw,
      authenticatedSender: "alpha@test.interchange",
      authenticatedSenderPublicKey: hexKey(crypto),
      messageId: "mid-multi",
      agentAddress: AGENT_ADDRESS,
    });

    expect(verdict.signature).toBe("valid");
    expect(verdict.fromMatch).toBe("unchecked");
  });

  test("case-variant From still matches the stamp", async () => {
    // extractAddrSpec normalizes to lowercase, so an upper-case From binds to a
    // lower-case stamp without a false mismatch.
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, "Alpha@Test.Interchange");

    const verdict = await shadowVerifyInboundSignature({
      raw,
      authenticatedSender: "alpha@test.interchange",
      authenticatedSenderPublicKey: hexKey(crypto),
      messageId: "mid-case",
      agentAddress: AGENT_ADDRESS,
    });

    expect(verdict.signature).toBe("valid");
    expect(verdict.fromMatch).toBe("match");
  });
});
