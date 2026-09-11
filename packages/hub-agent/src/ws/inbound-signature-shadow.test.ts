import { describe, test, expect } from "bun:test";
import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  generateMessageId,
  type MessageHeaders,
} from "@intx/mime";
import type { CryptoProvider } from "@intx/types/runtime";

import { shadowVerifyInboundSignature } from "./inbound-signature-shadow";
import { createPublicKeyCrypto } from "../sender-crypto";

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

/**
 * A resolver that hands back `crypto`'s public key for `sender` and misses
 * (returns undefined) for anyone else -- the cache the recipient verifies
 * against.
 */
function cacheFor(
  sender: string,
  crypto: Awaited<ReturnType<typeof makeCrypto>>,
): (address: string) => CryptoProvider | undefined {
  return (address) =>
    address === sender
      ? createPublicKeyCrypto(crypto.getPublicKey())
      : undefined;
}

const emptyCache = (): undefined => undefined;

describe("shadowVerifyInboundSignature", () => {
  test("signature the cached key verifies, From matches: valid/match", async () => {
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, sender);

    const verdict = await shadowVerifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-valid",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(sender, crypto),
    );

    expect(verdict.signature).toBe("valid");
    expect(verdict.fromMatch).toBe("match");
    expect(verdict.messageFrom).toBe(sender);
  });

  test("signature against a different cached key: invalid, From unchecked", async () => {
    const sender = "alpha@test.interchange";
    const signer = await makeCrypto();
    const other = await makeCrypto();
    const raw = await signedMessage(signer, sender);

    const verdict = await shadowVerifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-invalid",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(sender, other),
    );

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

    const verdict = await shadowVerifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-missing",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(sender, crypto),
    );

    expect(verdict.signature).toBe("missing");
    expect(verdict.fromMatch).toBe("unchecked");
  });

  test("valid signature under a forged From: valid/mismatch", async () => {
    // The message is genuinely signed by `crypto` (the key the cache holds for
    // the stamped `signer`), but its visible From claims a different sender. A
    // valid signature over a borrowed display identity is the forgery this
    // binding catches.
    const signer = "alpha@test.interchange";
    const forgedFrom = "victim@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, forgedFrom);

    const verdict = await shadowVerifyInboundSignature(
      {
        raw,
        authenticatedSender: signer,
        messageId: "mid-forged",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(signer, crypto),
    );

    expect(verdict.signature).toBe("valid");
    expect(verdict.fromMatch).toBe("mismatch");
    expect(verdict.messageFrom).toBe(forgedFrom);
  });

  test("cache miss: unknown, admitted", async () => {
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, sender);

    const verdict = await shadowVerifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-miss",
        agentAddress: AGENT_ADDRESS,
      },
      emptyCache,
    );

    expect(verdict.signature).toBe("unknown");
    expect(verdict.fromMatch).toBe("unchecked");
  });

  test("a resolver that throws degrades to error, not a crash", async () => {
    // The resolver call is inside the verify's try, so a throw is contained as
    // a distinct `error` verdict rather than escaping and being mis-logged as a
    // mail-path crash.
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, sender);

    const verdict = await shadowVerifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-resolver-fault",
        agentAddress: AGENT_ADDRESS,
      },
      () => {
        throw new Error("resolver boom");
      },
    );

    expect(verdict.signature).toBe("error");
    expect(verdict.fromMatch).toBe("unchecked");
  });

  test("an unreadable cached key degrades to error, not a crash", async () => {
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, sender);
    // A provider whose getPublicKey throws -- the verify path must contain it
    // as a distinct `error` verdict rather than crash the mail path.
    const faulty: CryptoProvider = {
      getPublicKey() {
        throw new Error("cached key unreadable");
      },
      sign: () => Promise.reject(new Error("verify-only")),
      signSSH: () => Promise.reject(new Error("verify-only")),
      verify: () => Promise.reject(new Error("verify-only")),
    };

    const verdict = await shadowVerifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-key-fault",
        agentAddress: AGENT_ADDRESS,
      },
      () => faulty,
    );

    expect(verdict.signature).toBe("error");
    expect(verdict.fromMatch).toBe("unchecked");
  });

  test("a display-name From still binds to the stamp addr-spec", async () => {
    // extractAddrSpec strips the display name, so `Alpha <a@b>` binds to the
    // bare stamp `a@b` without a false mismatch.
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, "Alpha <alpha@test.interchange>");

    const verdict = await shadowVerifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-display",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(sender, crypto),
    );

    expect(verdict.signature).toBe("valid");
    expect(verdict.fromMatch).toBe("match");
    expect(verdict.messageFrom).toBe("alpha@test.interchange");
  });

  test("an unparseable multi-address From is unparseable, not a false mismatch", async () => {
    // A shadow must not turn a From it cannot reduce to one addr-spec into a
    // forgery verdict -- that would poison the corpus (and later drop
    // legitimate mail under enforcement). extractAddrSpec rejects the two-@
    // input; the binding is `unparseable` -- present but malformed, distinct
    // from a benign no-From `unchecked` -- while the signature stands.
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(
      crypto,
      "alpha@test.interchange, beta@test.interchange",
    );

    const verdict = await shadowVerifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-multi",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(sender, crypto),
    );

    expect(verdict.signature).toBe("valid");
    expect(verdict.fromMatch).toBe("unparseable");
    expect(verdict.messageFrom).toBeNull();
  });

  test("an unparseable From at a cache miss is unparseable, not unchecked", async () => {
    // The From presence is evaluated for every non-error status, not only
    // valid. A present-but-unparseable From under an `unknown` signature is
    // still `unparseable` -- a later enforcement precedence needs to see it.
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(
      crypto,
      "alpha@test.interchange, beta@test.interchange",
    );

    const verdict = await shadowVerifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-multi-miss",
        agentAddress: AGENT_ADDRESS,
      },
      emptyCache,
    );

    expect(verdict.signature).toBe("unknown");
    expect(verdict.fromMatch).toBe("unparseable");
    expect(verdict.messageFrom).toBeNull();
  });

  test("no From header at all stays unchecked, distinct from unparseable", async () => {
    // A message with no From carries no identity claim to bind -- a benign
    // `unchecked` with a null messageFrom, kept distinct from a present but
    // unparseable From.
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = new TextEncoder().encode(
      [
        "To: beta@test.interchange",
        "Subject: no from",
        "Content-Type: text/plain",
        "",
        "no from header",
      ].join("\r\n"),
    );

    const verdict = await shadowVerifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-no-from",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(sender, crypto),
    );

    expect(verdict.fromMatch).toBe("unchecked");
    expect(verdict.messageFrom).toBeNull();
  });

  test("a cache miss with a parseable From is unchecked, not unparseable", async () => {
    // A parseable From under a non-valid signature records `messageFrom` but
    // leaves the binding `unchecked` -- a match is only meaningful atop a valid
    // signature, and this must NOT be mistaken for `unparseable`.
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, sender);

    const verdict = await shadowVerifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-miss-parseable",
        agentAddress: AGENT_ADDRESS,
      },
      emptyCache,
    );

    expect(verdict.signature).toBe("unknown");
    expect(verdict.fromMatch).toBe("unchecked");
    expect(verdict.messageFrom).toBe(sender);
  });

  test("valid signature with an unparseable stamped sender stays unchecked, not unparseable", async () => {
    // The load-bearing asymmetry: a present-and-parseable message From under a
    // VALID signature, but the stamped authenticatedSender itself is not a bare
    // addr-spec (a two-@ string extractAddrSpec refuses). The binding must stay
    // `unchecked` -- the signature is the primary signal, and a shadow must not
    // turn an unparseable STAMP into a false `unparseable` From verdict (that
    // state is reserved for a present-but-malformed message From).
    const badSender = "alpha@test@interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, "alpha@test.interchange");

    const verdict = await shadowVerifyInboundSignature(
      {
        raw,
        authenticatedSender: badSender,
        messageId: "mid-bad-sender",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(badSender, crypto),
    );

    expect(verdict.signature).toBe("valid");
    expect(verdict.messageFrom).toBe("alpha@test.interchange");
    expect(verdict.fromMatch).toBe("unchecked");
  });

  test("case-variant From still matches the stamp", async () => {
    // extractAddrSpec normalizes to lowercase, so an upper-case From binds to a
    // lower-case stamp without a false mismatch.
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, "Alpha@Test.Interchange");

    const verdict = await shadowVerifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-case",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(sender, crypto),
    );

    expect(verdict.signature).toBe("valid");
    expect(verdict.fromMatch).toBe("match");
  });
});
