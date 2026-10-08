import { describe, test, expect } from "bun:test";
import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  decodeMail,
  generateMessageId,
  type MessageHeaders,
} from "@intx/mime";
import type {
  CryptoProvider,
  InboundMailOutcome,
  InboundMailPolicy,
} from "@intx/types/runtime";

import {
  verifyInboundSignature,
  outcomeForVerdict,
  decideInboundAdmission,
  resolveInboundMailPolicy,
  type InboundSignatureVerdict,
  type ResolvedInboundMailPolicy,
} from "./inbound-signature";
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

/**
 * Remove the top-level `From` header, leaving the rest byte-identical. The
 * detached signature covers only the signed content part, so the result
 * still verifies.
 */
function stripFromHeader(raw: Uint8Array): Uint8Array {
  const text = new TextDecoder().decode(raw);
  const bodyStart = text.indexOf("\r\n\r\n");
  if (bodyStart === -1) throw new Error("assembled message has no body");
  const lines = text.slice(0, bodyStart).split("\r\n");
  const kept = lines.filter((line) => !line.toLowerCase().startsWith("from:"));
  if (kept.length === lines.length) {
    throw new Error("assembled message carries no From header to strip");
  }
  return new TextEncoder().encode(kept.join("\r\n") + text.slice(bodyStart));
}

/**
 * Replace the top-level `From` header's VALUE with `value`, leaving the rest
 * byte-identical. Like {@link stripFromHeader} this touches only top-level
 * headers, so the result still verifies. `value` is spliced directly after
 * the colon, so it can carry a CRLF and produce a folded header.
 */
function replaceFromValue(raw: Uint8Array, value: string): Uint8Array {
  const text = new TextDecoder().decode(raw);
  const bodyStart = text.indexOf("\r\n\r\n");
  if (bodyStart === -1) throw new Error("assembled message has no body");
  const lines = text.slice(0, bodyStart).split("\r\n");
  let replaced = false;
  const kept = lines.map((line) => {
    if (!line.toLowerCase().startsWith("from:")) return line;
    replaced = true;
    return `From:${value}`;
  });
  if (!replaced) {
    throw new Error("assembled message carries no From header to replace");
  }
  return new TextEncoder().encode(kept.join("\r\n") + text.slice(bodyStart));
}

/**
 * Insert `line` as its own top-level header field directly after the existing
 * `From`, leaving the rest byte-identical. Like {@link stripFromHeader} this
 * touches only top-level headers, so the result still verifies. `line`
 * carries its own field name, so this builds a message with two `From`
 * fields.
 */
function insertHeaderLineAfterFrom(raw: Uint8Array, line: string): Uint8Array {
  const text = new TextDecoder().decode(raw);
  const bodyStart = text.indexOf("\r\n\r\n");
  if (bodyStart === -1) throw new Error("assembled message has no body");
  const lines = text.slice(0, bodyStart).split("\r\n");
  const at = lines.findIndex((l) => l.toLowerCase().startsWith("from:"));
  if (at === -1) {
    throw new Error("assembled message carries no From header to insert after");
  }
  lines.splice(at + 1, 0, line);
  return new TextEncoder().encode(lines.join("\r\n") + text.slice(bodyStart));
}

/**
 * Restate a decoded mail's originator in the spelling a verdict uses, so the
 * two are directly comparable: `messageFrom` is `string | null`,
 * `Mail.headers.from` is `string | undefined`. Only the absent case is
 * restated; a present address passes through untouched.
 */
function asVerdictOriginator(from: string | undefined): string | null {
  return from === undefined ? null : from;
}

describe("verifyInboundSignature", () => {
  test("signature the cached key verifies, From matches: valid/match", async () => {
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, sender);

    const verdict = await verifyInboundSignature(
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

  test("signature against a different cached key: invalid, From still matches", async () => {
    const sender = "alpha@test.interchange";
    const signer = await makeCrypto();
    const other = await makeCrypto();
    const raw = await signedMessage(signer, sender);

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-invalid",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(sender, other),
    );

    expect(verdict.signature).toBe("invalid");
    // The binding is evaluated whatever the signature check found, so a From
    // naming the stamped sender is `match` even where the signature failed.
    expect(verdict.fromMatch).toBe("match");
    expect(verdict.messageFrom).toBe(sender);
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

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-missing",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(sender, crypto),
    );

    expect(verdict.signature).toBe("missing");
    expect(verdict.fromMatch).toBe("match");
  });

  test("valid signature under a forged From: valid/mismatch", async () => {
    // Genuinely signed by the cached key for the stamped `signer`, but the
    // visible From claims a different sender -- the forgery this binding
    // catches.
    const signer = "alpha@test.interchange";
    const forgedFrom = "victim@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, forgedFrom);

    const verdict = await verifyInboundSignature(
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

  test("a forged From at a cache miss is a mismatch that a relaxed unknown does not admit", async () => {
    // No key is cached for the stamped sender, so the signature axis is
    // `unknown`; the visible From names somebody else entirely. Nothing
    // downstream reads the stamp, so admitting this would deliver a message
    // every consumer attributes to the address in its From. Relaxing `unknown`
    // does not make it admissible: the axes are separately keyed, and
    // relaxing `unknown` cannot buy back verified identity (the companion
    // test below shows the internally-consistent forged pair getting in).
    const stamp = "attacker@remote.example";
    const forgedFrom = "ceo@victim.example";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, forgedFrom);

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: stamp,
        messageId: "mid-forged-unknown",
        agentAddress: AGENT_ADDRESS,
      },
      emptyCache,
    );

    expect(verdict.signature).toBe("unknown");
    expect(verdict.fromMatch).toBe("mismatch");
    expect(verdict.messageFrom).toBe(forgedFrom);
    expect(outcomeForVerdict(verdict)).toBe("mismatchedFrom");

    const decision = decideInboundAdmission(
      verdict,
      resolveInboundMailPolicy({ unknown: "admit" }),
    );
    expect(decision.findings).toEqual(["unknown", "mismatchedFrom"]);
    expect(decision.rejectedBy).toBe("mismatchedFrom");
  });

  test("a consistent From at a cache miss is admitted under a relaxed unknown", async () => {
    // Companion of the case above: the message names the stamped sender, so it
    // raises `unknown` alone and the author who relaxed `unknown` gets it -- an
    // unverified but internally consistent identity claim is accepted.
    const sender = "rotating@remote.example";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, sender);

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-consistent-unknown",
        agentAddress: AGENT_ADDRESS,
      },
      emptyCache,
    );

    expect(verdict.signature).toBe("unknown");
    expect(verdict.fromMatch).toBe("match");

    const decision = decideInboundAdmission(
      verdict,
      resolveInboundMailPolicy({ unknown: "admit" }),
    );
    expect(decision.findings).toEqual(["unknown"]);
    expect(decision.rejectedBy).toBeNull();
  });

  test("cache miss: unknown, with the From still bound", async () => {
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, sender);

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-miss",
        agentAddress: AGENT_ADDRESS,
      },
      emptyCache,
    );

    expect(verdict.signature).toBe("unknown");
    expect(verdict.fromMatch).toBe("match");
  });

  test("a resolver that throws degrades to error, not a crash", async () => {
    // The resolver call is inside the verify's try, so a throw is contained as
    // a distinct `error` verdict rather than escaping as a mail-path crash.
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, sender);

    const verdict = await verifyInboundSignature(
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
    expect(verdict.fromMatch).toBe("notEvaluated");
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

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-key-fault",
        agentAddress: AGENT_ADDRESS,
      },
      () => faulty,
    );

    expect(verdict.signature).toBe("error");
    expect(verdict.fromMatch).toBe("notEvaluated");
  });

  test("a cached key that cannot verify faults, rather than failing the check", async () => {
    // Distinct from the test above: `getPublicKey` answers with bytes no
    // signature can be checked against. The check never runs, so the gate must
    // not report `invalid` (which an author can relax).
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, sender);
    // The production wrapper over a truncated key: the cache validates its
    // entries, but this seam is injected and the wrapper vouches for nothing.
    const truncated = createPublicKeyCrypto(crypto.getPublicKey().slice(0, 16));

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-unusable-key",
        agentAddress: AGENT_ADDRESS,
      },
      () => truncated,
    );

    expect(verdict.signature).toBe("error");
    expect(verdict.fromMatch).toBe("notEvaluated");
    expect(outcomeForVerdict(verdict)).toBe("error");
  });

  test("no policy admits a message whose key could not verify anything", async () => {
    // The security property: a message the gate could not check through is
    // never something an author waves past. This policy relaxes every author
    // key, including `invalid` and `untrustedFrom` -- the two a key fault would
    // land on if reported as a failed check.
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, sender);
    const truncated = createPublicKeyCrypto(crypto.getPublicKey().slice(0, 16));

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-unusable-key-admission",
        agentAddress: AGENT_ADDRESS,
      },
      () => truncated,
    );

    const fullyOpen: InboundMailPolicy = {
      untrustedFrom: "admit",
      absentFrom: "admit",
      invalid: "admit",
      missing: "admit",
      unknown: "admit",
    };
    const admission = decideInboundAdmission(
      verdict,
      resolveInboundMailPolicy(fullyOpen),
    );

    expect(admission.rejectedBy).toBe("error");
    expect(admission.findings).toEqual([]);
  });

  test("a display-name From still binds to the stamp addr-spec", async () => {
    // extractAddrSpec strips the display name, so `Alpha <a@b>` binds to the
    // bare stamp `a@b` without a false mismatch.
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, "Alpha <alpha@test.interchange>");

    const verdict = await verifyInboundSignature(
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
    // The check must not turn a From it cannot reduce to one addr-spec into a
    // forgery verdict -- that would poison the corpus. extractAddrSpec rejects
    // the two-@ input; the binding is `unparseable` (present but malformed,
    // distinct from the benign no-From `absent`) while the signature stands.
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(
      crypto,
      "alpha@test.interchange, beta@test.interchange",
    );

    const verdict = await verifyInboundSignature(
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

  test("an unparseable From at a cache miss is unparseable, not a mismatch", async () => {
    // The From is evaluated for every non-error status, not only valid. A
    // present-but-unparseable From under `unknown` is still `unparseable`: the
    // gate never reduced it to an address, so it must not report the
    // `mismatch` of a From it read and found to name somebody else. The axes
    // are separately keyed; folding them would hand an author one decision
    // where they have two.
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(
      crypto,
      "alpha@test.interchange, beta@test.interchange",
    );

    const verdict = await verifyInboundSignature(
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

  test("no From header at all is absent, distinct from unparseable", async () => {
    // A message with no From carries no identity claim to bind -- `absent`
    // with a null messageFrom, kept distinct from a present but unparseable
    // From.
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

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-no-from",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(sender, crypto),
    );

    expect(verdict.fromMatch).toBe("absent");
    expect(verdict.messageFrom).toBeNull();
  });

  // A `From` that is PRESENT but blank reaches the same `absent` state as no
  // header at all: the value is empty once trimmed, so there is no originator
  // to bind. It must not buy a better outcome than omitting the header, and
  // must not land on `unparseable`, which is keyed to a value the gate could
  // not reduce. The last row folds a whitespace-only continuation line, so
  // unfolding runs before the value is read.
  const blankFromValues: [label: string, value: string][] = [
    ["is empty", ""],
    ["is one space", " "],
    ["is three spaces", "   "],
    ["is a tab", "\t"],
    ["mixes spaces and a tab", " \t "],
    ["folds onto a whitespace-only line", "\r\n   "],
  ];

  for (const [label, value] of blankFromValues) {
    test(`a present From whose value ${label} is absent, not unparseable`, async () => {
      const sender = "alpha@test.interchange";
      const crypto = await makeCrypto();
      const raw = replaceFromValue(await signedMessage(crypto, sender), value);

      const verdict = await verifyInboundSignature(
        {
          raw,
          authenticatedSender: sender,
          messageId: "mid-blank-from",
          agentAddress: AGENT_ADDRESS,
        },
        cacheFor(sender, crypto),
      );

      expect(verdict.signature).toBe("valid");
      expect(verdict.fromMatch).toBe("absent");
      expect(verdict.messageFrom).toBeNull();
      expect(outcomeForVerdict(verdict)).toBe("absentFrom");
      // The same three admission properties the absent-header case has: closed
      // by default, still closed when only the neighbouring From key is
      // relaxed, and open only to the author who relaxed `absentFrom` itself.
      expect(
        decideInboundAdmission(verdict, resolveInboundMailPolicy(undefined))
          .rejectedBy,
      ).toBe("absentFrom");
      expect(
        decideInboundAdmission(
          verdict,
          resolveInboundMailPolicy({ untrustedFrom: "admit" }),
        ).rejectedBy,
      ).toBe("absentFrom");
      expect(
        decideInboundAdmission(
          verdict,
          resolveInboundMailPolicy({ absentFrom: "admit" }),
        ).rejectedBy,
      ).toBeNull();
    });
  }

  test("a validly signed message with no From is absentFrom, not clean", async () => {
    // The binding is `absent` -- there was nothing to bind -- but the gate
    // must not certify its most permissive outcome for a binding it never
    // checked. Every downstream consumer attributes from the message's own
    // `From`, so a verified signature over a message naming no originator is
    // `absentFrom`: the gate holds a sender it cannot certify the message is
    // from.
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = stripFromHeader(await signedMessage(crypto, sender));

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-signed-no-from",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(sender, crypto),
    );

    expect(verdict.signature).toBe("valid");
    expect(verdict.fromMatch).toBe("absent");
    expect(verdict.messageFrom).toBeNull();
    expect(outcomeForVerdict(verdict)).toBe("absentFrom");
  });

  test("relaxing untrustedFrom alone does not admit a message with no From", async () => {
    // The two conditions are separately keyed. An author relaxes
    // `untrustedFrom` to tolerate an external correspondent's odd headers;
    // that judgement says nothing about mail the gate resolved no originator
    // for at all. The latter is `absentFrom`, left at its `reject` default.
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = stripFromHeader(await signedMessage(crypto, sender));

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-relaxed-untrusted-from",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(sender, crypto),
    );

    const outcome = outcomeForVerdict(verdict);
    expect(outcome).toBe("absentFrom");
    // Indexed the way the delivery seam indexes it: the resolved map is total,
    // so the outcome the gate produced selects the author's decision directly.
    expect(resolveInboundMailPolicy({ untrustedFrom: "admit" })[outcome]).toBe(
      "reject",
    );
  });

  test("a cache miss with a parseable From reports the address it bound", async () => {
    // A parseable From under a non-valid signature records `messageFrom` and
    // the comparison's result, and must NOT be mistaken for `unparseable`, nor
    // for the `absent` of a message that names no originator at all.
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, sender);

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-miss-parseable",
        agentAddress: AGENT_ADDRESS,
      },
      emptyCache,
    );

    expect(verdict.signature).toBe("unknown");
    expect(verdict.fromMatch).toBe("match");
    expect(verdict.messageFrom).toBe(sender);
  });

  test("an unparseable stamped sender faults, even under a valid signature", async () => {
    // A present-and-parseable message From under a VALID signature, but the
    // stamped authenticatedSender is not a bare addr-spec (a two-@ string
    // extractAddrSpec refuses). The stamp is the identity the verification is
    // about, so one that cannot be parsed is a fault in its own input:
    // `error`, pinned to reject.
    //
    // The verdict headlines `error` even though the message's own From is
    // perfectly readable: the stamp is parsed above the visible From, so the
    // gate reports no originator at all rather than one it holds and could
    // not compare.
    const badSender = "alpha@test@interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, "alpha@test.interchange");

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: badSender,
        messageId: "mid-bad-sender",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(badSender, crypto),
    );

    expect(verdict.signature).toBe("error");
    expect(verdict.fromMatch).toBe("notEvaluated");
    expect(verdict.messageFrom).toBeNull();
    expect(outcomeForVerdict(verdict)).toBe("error");
  });

  test("an unparseable stamped sender faults when no key is cached", async () => {
    // The stamp is parsed above the cache lookup, so a malformed stamp faults
    // where a cache miss would otherwise have produced `unknown` -- an outcome
    // an author may legitimately relax to admit across a key rotation.
    const badSender = "alpha@test@interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, "alpha@test.interchange");

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: badSender,
        messageId: "mid-bad-sender-miss",
        agentAddress: AGENT_ADDRESS,
      },
      emptyCache,
    );

    expect(verdict.signature).toBe("error");
    expect(verdict.fromMatch).toBe("notEvaluated");
  });

  test("an unparseable stamped sender faults on a message with no From", async () => {
    // The stamp is parsed above the no-From early return too, so a malformed
    // stamp faults even where there is no visible originator to bind it to.
    const badSender = "alpha@test@interchange";
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

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: badSender,
        messageId: "mid-bad-sender-no-from",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(badSender, crypto),
    );

    expect(verdict.signature).toBe("error");
    expect(verdict.fromMatch).toBe("notEvaluated");
  });

  test("the cached key is resolved under the raw stamp, not the parsed one", async () => {
    // extractAddrSpec lowercases, so looking the key up under the parsed stamp
    // would miss a cache keyed on the stamp the hub actually sent. The lookup
    // uses the raw stamp; only the From-binding compares the parsed form.
    const sender = "Alpha@Test.Interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, "alpha@test.interchange");

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: "mid-raw-stamp-key",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(sender, crypto),
    );

    expect(verdict.signature).toBe("valid");
    expect(verdict.fromMatch).toBe("match");
  });

  test("case-variant From still matches the stamp", async () => {
    // extractAddrSpec normalizes to lowercase, so an upper-case From binds to a
    // lower-case stamp without a false mismatch.
    const sender = "alpha@test.interchange";
    const crypto = await makeCrypto();
    const raw = await signedMessage(crypto, "Alpha@Test.Interchange");

    const verdict = await verifyInboundSignature(
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

  // Two top-level `From` fields is malformed under RFC 5322 section 3.6.2,
  // but nothing on the delivery path refuses such a message. What must hold is
  // that the gate binds the SAME value the consumer's `headers.from` names, in
  // either order: if they disagreed, an attacker could get one originator past
  // the gate and a different one delivered. `parseHeaders` keeps the first
  // occurrence, and both the gate and `decodeMail` read through it, which is
  // what makes them agree.
  const DUP_SENDER = "alpha@test.interchange";
  const DUP_ATTACKER = "evil@attacker.example";

  test("duplicate From, legit value first: admitted, and both values survive", async () => {
    const crypto = await makeCrypto();
    const raw = insertHeaderLineAfterFrom(
      await signedMessage(crypto, DUP_SENDER),
      `From: ${DUP_ATTACKER}`,
    );
    const decoded = decodeMail(raw);
    // The fixture really carries two From fields, in this order.
    expect(decoded.rawHeaders["from"]).toEqual([DUP_SENDER, DUP_ATTACKER]);

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: DUP_SENDER,
        messageId: "mid-dup-legit-first",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(DUP_SENDER, crypto),
    );

    expect(verdict.signature).toBe("valid");
    expect(verdict.fromMatch).toBe("match");
    expect(verdict.messageFrom).toBe(DUP_SENDER);
    expect(asVerdictOriginator(decoded.headers.from)).toBe(verdict.messageFrom);
    expect(
      decideInboundAdmission(verdict, resolveInboundMailPolicy(undefined))
        .rejectedBy,
    ).toBe(null);
  });

  test("duplicate From, attacker value first: rejected as mismatchedFrom", async () => {
    const crypto = await makeCrypto();
    // The attacker's value takes the first field and the stamped sender's the
    // second, which is the ordering a relay-prepended header produces.
    const raw = insertHeaderLineAfterFrom(
      replaceFromValue(
        await signedMessage(crypto, DUP_SENDER),
        ` ${DUP_ATTACKER}`,
      ),
      `From: ${DUP_SENDER}`,
    );
    const decoded = decodeMail(raw);
    expect(decoded.rawHeaders["from"]).toEqual([DUP_ATTACKER, DUP_SENDER]);

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: DUP_SENDER,
        messageId: "mid-dup-attacker-first",
        agentAddress: AGENT_ADDRESS,
      },
      cacheFor(DUP_SENDER, crypto),
    );

    // The signature still verifies -- it covers the signed content part, not
    // the top-level headers -- so the rejection is the binding's alone.
    expect(verdict.signature).toBe("valid");
    expect(verdict.fromMatch).toBe("mismatch");
    expect(verdict.messageFrom).toBe(DUP_ATTACKER);
    expect(asVerdictOriginator(decoded.headers.from)).toBe(verdict.messageFrom);
    expect(
      decideInboundAdmission(verdict, resolveInboundMailPolicy(undefined))
        .rejectedBy,
    ).toBe("mismatchedFrom");
  });
});

function verdict(
  signature: InboundSignatureVerdict["signature"],
  fromMatch: InboundSignatureVerdict["fromMatch"],
): InboundSignatureVerdict {
  return {
    signature,
    fromMatch,
    authenticatedSender: AGENT_ADDRESS,
    messageFrom: null,
  };
}

describe("outcomeForVerdict", () => {
  // Every (signature, fromMatch) pair mapped to its outcome. match, mismatch,
  // absent and unparseable accompany any non-error signature status, since the
  // From is evaluated whatever the signature check found. notEvaluated is
  // reachable only on an `error` verdict; rows below a non-error status pin
  // the mapping of a pair the gate does not produce, which
  // `decideInboundAdmission` separately refuses. error dominates regardless of
  // the From axis.
  const table: [
    InboundSignatureVerdict["signature"],
    InboundSignatureVerdict["fromMatch"],
    InboundMailOutcome,
  ][] = [
    ["error", "notEvaluated", "error"],
    ["error", "unparseable", "error"],
    ["error", "match", "error"],
    ["error", "mismatch", "error"],

    ["valid", "match", "clean"],
    ["valid", "absent", "absentFrom"],
    ["valid", "notEvaluated", "untrustedFrom"],
    ["valid", "mismatch", "mismatchedFrom"],
    ["valid", "unparseable", "untrustedFrom"],

    ["invalid", "match", "invalid"],
    ["invalid", "absent", "invalid"],
    ["invalid", "notEvaluated", "invalid"],
    ["invalid", "mismatch", "mismatchedFrom"],
    ["invalid", "unparseable", "untrustedFrom"],

    ["missing", "match", "missing"],
    ["missing", "absent", "missing"],
    ["missing", "notEvaluated", "missing"],
    ["missing", "mismatch", "mismatchedFrom"],
    ["missing", "unparseable", "untrustedFrom"],

    ["unknown", "match", "unknown"],
    ["unknown", "absent", "unknown"],
    ["unknown", "notEvaluated", "unknown"],
    ["unknown", "mismatch", "mismatchedFrom"],
    ["unknown", "unparseable", "untrustedFrom"],
  ];

  for (const [signature, fromMatch, expected] of table) {
    test(`${signature}/${fromMatch} -> ${expected}`, () => {
      expect(outcomeForVerdict(verdict(signature, fromMatch))).toBe(expected);
    });
  }

  test("clean is headlined only by a valid signature with a matched binding", () => {
    // `clean` is the headline the gate claims on its own authority, so it may
    // be claimed only where the binding was actually checked AND found to bind
    // -- never for a binding the gate declined to evaluate. A log line that
    // read `clean` for an unbound message would misreport what the gate found.
    //
    // Both axes are keyed on their own unions, so adding a member to either
    // fails the type-check here rather than escaping the property untested.
    const signatures: Record<
      InboundSignatureVerdict["signature"],
      InboundSignatureVerdict["signature"]
    > = {
      valid: "valid",
      invalid: "invalid",
      missing: "missing",
      unknown: "unknown",
      error: "error",
    };
    const bindings: Record<
      InboundSignatureVerdict["fromMatch"],
      InboundSignatureVerdict["fromMatch"]
    > = {
      match: "match",
      mismatch: "mismatch",
      absent: "absent",
      notEvaluated: "notEvaluated",
      unparseable: "unparseable",
    };

    for (const signature of Object.values(signatures)) {
      for (const fromMatch of Object.values(bindings)) {
        const clean =
          outcomeForVerdict(verdict(signature, fromMatch)) === "clean";
        expect({ signature, fromMatch, clean }).toEqual({
          signature,
          fromMatch,
          clean: signature === "valid" && fromMatch === "match",
        });
      }
    }
  });
});

describe("resolveInboundMailPolicy", () => {
  // The secure baseline: clean admits, everything else rejects. Both an absent
  // policy and an empty object resolve to exactly this.
  const secureBaseline: ResolvedInboundMailPolicy = {
    clean: "admit",
    error: "reject",
    untrustedFrom: "reject",
    mismatchedFrom: "reject",
    absentFrom: "reject",
    invalid: "reject",
    missing: "reject",
    unknown: "reject",
  };

  test("undefined authored resolves to the secure baseline", () => {
    expect(resolveInboundMailPolicy(undefined)).toEqual(secureBaseline);
  });

  test("empty authored policy resolves identically to undefined", () => {
    expect(resolveInboundMailPolicy({})).toEqual(secureBaseline);
  });

  test("a partial policy admits only the outcomes it names", () => {
    const authored: InboundMailPolicy = {
      missing: "admit",
      untrustedFrom: "admit",
    };

    expect(resolveInboundMailPolicy(authored)).toEqual({
      clean: "admit",
      error: "reject",
      untrustedFrom: "admit",
      mismatchedFrom: "reject",
      absentFrom: "reject",
      invalid: "reject",
      missing: "admit",
      unknown: "reject",
    });
  });

  test("error stays reject even when every author-controllable key admits", () => {
    // A policy that relaxes every outcome an author may control still cannot
    // relax `error` -- a fault we could not check through is never admitted.
    const authored: InboundMailPolicy = {
      untrustedFrom: "admit",
      mismatchedFrom: "admit",
      absentFrom: "admit",
      invalid: "admit",
      missing: "admit",
      unknown: "admit",
    };

    expect(resolveInboundMailPolicy(authored)).toEqual({
      clean: "admit",
      error: "reject",
      untrustedFrom: "admit",
      mismatchedFrom: "admit",
      absentFrom: "admit",
      invalid: "admit",
      missing: "admit",
      unknown: "admit",
    });
  });
});

describe("decideInboundAdmission", () => {
  // The neutral policy an author who declared nothing resolves to: `clean`
  // admits, every author-controllable outcome rejects.
  const NEUTRAL = resolveInboundMailPolicy(undefined);
  // Every author-controllable outcome relaxed to admit. `clean` admits and
  // `error` stays pinned to reject, neither being an author's to set.
  const RELAXED = resolveInboundMailPolicy({
    untrustedFrom: "admit",
    mismatchedFrom: "admit",
    absentFrom: "admit",
    invalid: "admit",
    missing: "admit",
    unknown: "admit",
  });
  // The policy an address with no live deployment behind it resolves to: even
  // `clean` is closed. Spelled out here rather than imported so this file's
  // decision tests do not depend on the registry module.
  const FULLY_CLOSED: ResolvedInboundMailPolicy = {
    clean: "reject",
    error: "reject",
    untrustedFrom: "reject",
    mismatchedFrom: "reject",
    absentFrom: "reject",
    invalid: "reject",
    missing: "reject",
    unknown: "reject",
  };

  function admits(
    signature: InboundSignatureVerdict["signature"],
    fromMatch: InboundSignatureVerdict["fromMatch"],
    policy: ResolvedInboundMailPolicy,
  ): boolean {
    return (
      decideInboundAdmission(verdict(signature, fromMatch), policy)
        .rejectedBy === null
    );
  }

  test("a relaxed signature status still admits when the From agrees with the stamp", () => {
    // The over-correction guard. A From naming the stamped sender raises
    // nothing on the binding axis, so the author's `invalid` decision is the
    // only one the admission reads. Were the binding to complain here, an
    // author who relaxed `invalid` would find their decision overridden by an
    // axis that found nothing wrong.
    const decision = decideInboundAdmission(
      verdict("invalid", "match"),
      resolveInboundMailPolicy({ invalid: "admit" }),
    );

    expect(decision.findings).toEqual(["invalid"]);
    expect(decision.rejectedBy).toBeNull();
  });

  // A From that contradicts the stamp is a finding under EVERY signature
  // status, keyed on its own `mismatchedFrom`. Relaxing a signature key is a
  // decision about how the originator's identity was ESTABLISHED; accepting a
  // message that names one identity while the hub stamped another is a
  // decision about identity itself, and the two are held separately.
  //
  // This does not make a relaxed signature key safe: a sender who sets the
  // stamp and the From to the same forged address is internally consistent,
  // reaches `match`, and is admitted -- inherent in admitting an identity no
  // key verified. It removes the inversion of admitting a self-contradictory
  // claim while refusing a message that honestly names nobody.
  for (const signature of ["invalid", "missing", "unknown"] as const) {
    const relaxedSignature = resolveInboundMailPolicy({ [signature]: "admit" });

    test(`relaxing ${signature} does not admit a From that contradicts the stamp`, () => {
      const decision = decideInboundAdmission(
        verdict(signature, "mismatch"),
        relaxedSignature,
      );

      expect(decision.findings).toEqual([signature, "mismatchedFrom"]);
      expect(decision.rejectedBy).toBe("mismatchedFrom");
    });

    test(`relaxing ${signature} admits a From that agrees with the stamp`, () => {
      const decision = decideInboundAdmission(
        verdict(signature, "match"),
        relaxedSignature,
      );

      expect(decision.findings).toEqual([signature]);
      expect(decision.rejectedBy).toBeNull();
    });

    test(`relaxing mismatchedFrom alone does not admit a ${signature} signature behind it`, () => {
      const decision = decideInboundAdmission(
        verdict(signature, "mismatch"),
        resolveInboundMailPolicy({ mismatchedFrom: "admit" }),
      );

      expect(decision.findings).toEqual([signature, "mismatchedFrom"]);
      expect(decision.rejectedBy).toBe(signature);
    });

    test(`a ${signature} signature behind a mismatched From needs both keys relaxed`, () => {
      const decision = decideInboundAdmission(
        verdict(signature, "mismatch"),
        resolveInboundMailPolicy({
          [signature]: "admit",
          mismatchedFrom: "admit",
        }),
      );

      expect(decision.findings).toEqual([signature, "mismatchedFrom"]);
      expect(decision.rejectedBy).toBeNull();
    });
  }

  test("untrustedFrom and mismatchedFrom do not stand in for each other", () => {
    // The two From-axis complaints an author might read as one. Tolerating a
    // correspondent's malformed header says nothing about accepting a From that
    // names somebody other than the stamped sender, and the reverse holds too.
    const mismatch = decideInboundAdmission(
      verdict("valid", "mismatch"),
      resolveInboundMailPolicy({ untrustedFrom: "admit" }),
    );
    expect(mismatch.rejectedBy).toBe("mismatchedFrom");

    const unparseable = decideInboundAdmission(
      verdict("valid", "unparseable"),
      resolveInboundMailPolicy({ mismatchedFrom: "admit" }),
    );
    expect(unparseable.rejectedBy).toBe("untrustedFrom");
  });

  test("relaxing unknown does not admit a message that also names no originator", () => {
    // Two axes, two author judgements. The author relaxed `unknown` to tolerate
    // a sender whose key the cache has not got; they said nothing about mail
    // that names no originator, and `absentFrom` is theirs to leave closed. A
    // decision keyed on one reduced outcome would see only `unknown` here and
    // admit, discarding the `absentFrom` judgement in the admitting direction.
    const decision = decideInboundAdmission(
      verdict("unknown", "absent"),
      resolveInboundMailPolicy({ unknown: "admit", absentFrom: "reject" }),
    );

    expect(decision.findings).toEqual(["unknown", "absentFrom"]);
    expect(decision.rejectedBy).toBe("absentFrom");
  });

  // Relaxing `untrustedFrom` tolerates an external correspondent's odd
  // headers. It is not a decision to stop enforcing the SIGNATURE, yet a
  // decision keyed on one reduced outcome makes it one: `untrustedFrom`
  // outranks the signature axis, so the author's `invalid` / `missing` /
  // `unknown` decision is never consulted and the message is admitted --
  // attaching a malformed `From` switches the signature check off.
  const relaxedUntrustedFrom = resolveInboundMailPolicy({
    untrustedFrom: "admit",
  });
  for (const signature of ["invalid", "missing", "unknown"] as const) {
    test(`relaxing untrustedFrom does not admit a ${signature} signature behind an unparseable From`, () => {
      const decision = decideInboundAdmission(
        verdict(signature, "unparseable"),
        relaxedUntrustedFrom,
      );

      expect(decision.findings).toEqual([signature, "untrustedFrom"]);
      expect(decision.rejectedBy).toBe(signature);
    });
  }

  test("an author who relaxed both axes still admits an unparseable From", () => {
    // The complement of the four cases above, and the proof that weighing the
    // whole finding set does not simply over-reject: where the author relaxed
    // every finding the verdict raised, the message is admitted.
    const decision = decideInboundAdmission(
      verdict("invalid", "unparseable"),
      resolveInboundMailPolicy({
        untrustedFrom: "admit",
        invalid: "admit",
        missing: "admit",
        unknown: "admit",
      }),
    );

    expect(decision.findings).toEqual(["invalid", "untrustedFrom"]);
    expect(decision.rejectedBy).toBeNull();
  });

  test("error rejects under every policy, including one that spells error admit", () => {
    // `error` short-circuits: the fault stopped the check from running, so
    // there is no finding set to weigh. It does not read the policy at all,
    // which is why a hand-built map that admits `error` cannot relax it.
    const admitsEverything: ResolvedInboundMailPolicy = {
      clean: "admit",
      error: "admit",
      untrustedFrom: "admit",
      mismatchedFrom: "admit",
      absentFrom: "admit",
      invalid: "admit",
      missing: "admit",
      unknown: "admit",
    };

    for (const policy of [NEUTRAL, RELAXED, FULLY_CLOSED, admitsEverything]) {
      for (const fromMatch of [
        "notEvaluated",
        "absent",
        "unparseable",
        "match",
        "mismatch",
      ] as const) {
        const decision = decideInboundAdmission(
          verdict("error", fromMatch),
          policy,
        );
        expect(decision.findings).toEqual([]);
        expect(decision.rejectedBy).toBe("error");
      }
    }
  });

  test("valid and match raises nothing and admits under every policy with clean open", () => {
    for (const policy of [NEUTRAL, RELAXED]) {
      const decision = decideInboundAdmission(
        verdict("valid", "match"),
        policy,
      );
      expect(decision.findings).toEqual([]);
      expect(decision.rejectedBy).toBeNull();
    }
  });

  test("the fully-closed policy rejects even a verdict that raises nothing", () => {
    // An address with no live deployment behind it has no author intent to
    // honor, so it admits nothing. An empty finding set resolves to the
    // `clean` key rather than to admission, which is what keeps that true.
    const decision = decideInboundAdmission(
      verdict("valid", "match"),
      FULLY_CLOSED,
    );

    expect(decision.findings).toEqual([]);
    expect(decision.rejectedBy).toBe("clean");
  });

  test("a binding that did not bind is refused under every signature status", () => {
    // `notEvaluated` raises no finding: it is the placeholder on an `error`
    // verdict, and `error` short-circuits above the finding set, so no verdict
    // the gate produces pairs it with a non-error signature status. The
    // decision refuses the pair anyway, and refuses it whatever the signature
    // axis found -- the binding raises nothing of its own, so keying the
    // refusal on an empty finding set would let an author who relaxed the
    // signature status admit an originator the gate never checked.
    for (const signature of [
      "valid",
      "invalid",
      "missing",
      "unknown",
    ] as const) {
      const decision = decideInboundAdmission(
        verdict(signature, "notEvaluated"),
        RELAXED,
      );

      expect({ signature, rejectedBy: decision.rejectedBy }).toEqual({
        signature,
        rejectedBy: "error",
      });
    }
  });

  test("an empty finding set is claimed only by a valid signature that bound", () => {
    // No (signature, binding) pair other than valid/match reaches admission
    // with nothing to complain about. Asserted under the most permissive policy
    // an author can write, so a pair that slipped through would show up here
    // rather than being masked by a rejecting default.
    //
    // Both axes are keyed on their own unions, so adding a member to either
    // fails the type-check here rather than escaping the property untested.
    const signatures: Record<
      InboundSignatureVerdict["signature"],
      InboundSignatureVerdict["signature"]
    > = {
      valid: "valid",
      invalid: "invalid",
      missing: "missing",
      unknown: "unknown",
      error: "error",
    };
    const bindings: Record<
      InboundSignatureVerdict["fromMatch"],
      InboundSignatureVerdict["fromMatch"]
    > = {
      match: "match",
      mismatch: "mismatch",
      absent: "absent",
      notEvaluated: "notEvaluated",
      unparseable: "unparseable",
    };

    for (const signature of Object.values(signatures)) {
      for (const fromMatch of Object.values(bindings)) {
        const decision = decideInboundAdmission(
          verdict(signature, fromMatch),
          RELAXED,
        );
        const uncomplaining =
          decision.findings.length === 0 && decision.rejectedBy === null;
        expect({ signature, fromMatch, uncomplaining }).toEqual({
          signature,
          fromMatch,
          uncomplaining: signature === "valid" && fromMatch === "match",
        });
      }
    }
  });

  // Every (signature, binding) pair the gate produces, crossed with the two
  // policies that bracket what an author can express: the neutral policy that
  // relaxes nothing, and the policy that relaxes every outcome an author
  // controls. `error` is excluded -- it short-circuits and has its own test
  // above. `notEvaluated` below a non-error status is not a pair the gate
  // produces; it is listed because the decision refuses it regardless.
  const decisions: [
    InboundSignatureVerdict["signature"],
    InboundSignatureVerdict["fromMatch"],
    boolean,
    boolean,
  ][] = [
    // signature, binding, admitted under NEUTRAL, admitted under RELAXED
    ["valid", "match", true, true],
    ["valid", "absent", false, true],
    ["valid", "notEvaluated", false, false],
    ["valid", "mismatch", false, true],
    ["valid", "unparseable", false, true],

    ["invalid", "match", false, true],
    ["invalid", "absent", false, true],
    ["invalid", "notEvaluated", false, false],
    ["invalid", "mismatch", false, true],
    ["invalid", "unparseable", false, true],

    ["missing", "match", false, true],
    ["missing", "absent", false, true],
    ["missing", "notEvaluated", false, false],
    ["missing", "mismatch", false, true],
    ["missing", "unparseable", false, true],

    ["unknown", "match", false, true],
    ["unknown", "absent", false, true],
    ["unknown", "notEvaluated", false, false],
    ["unknown", "mismatch", false, true],
    ["unknown", "unparseable", false, true],
  ];

  for (const [signature, fromMatch, underNeutral, underRelaxed] of decisions) {
    test(`${signature}/${fromMatch} -> neutral ${String(underNeutral)}, relaxed ${String(underRelaxed)}`, () => {
      expect(admits(signature, fromMatch, NEUTRAL)).toBe(underNeutral);
      expect(admits(signature, fromMatch, RELAXED)).toBe(underRelaxed);
    });
  }
});
