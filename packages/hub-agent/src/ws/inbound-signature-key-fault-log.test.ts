// The inbound-mail gate's diagnosis line for an unusable cached sender key.
//
// The outcome this fault produces, and the fact that no policy admits it, are
// asserted beside the decision tables in `inbound-signature.test.ts`. What is
// asserted here is the other half: that an operator can tell WHY. Unusable key
// material is an operator condition -- truncated in transit, a hub-side
// resolution bug, a corrupt cache entry -- and it is invisible in the mail flow
// itself, which shows only mail being rejected. Without the line this file
// pins, nothing points at the keyring.
//
// The capture is a process-global logging configuration, which is why this
// suite is its own file rather than a case inside the decision tables.

import {
  describe,
  test,
  expect,
  beforeAll,
  afterAll,
  beforeEach,
} from "bun:test";
import { configureSync, getConfig } from "@intx/log";
import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  generateMessageId,
  type MessageHeaders,
} from "@intx/mime";

import { verifyInboundSignature } from "./inbound-signature";
import { createPublicKeyCrypto } from "../sender-crypto";

const AGENT_ADDRESS = "run_anchor@tenant.example";
const SENDER = "alpha@test.interchange";

type CapturedLog = {
  category: readonly string[];
  level: string;
  message: readonly unknown[];
  properties: Record<string, unknown>;
};

const capturedLogs: CapturedLog[] = [];
const savedLogConfig = getConfig();

beforeAll(() => {
  configureSync({
    reset: true,
    sinks: {
      capture: (record) => {
        capturedLogs.push({
          category: record.category,
          level: record.level,
          message: record.message,
          properties: record.properties,
        });
      },
    },
    loggers: [
      { category: [], lowestLevel: "debug", sinks: ["capture"] },
      {
        category: ["logtape", "meta"],
        lowestLevel: "warning",
        sinks: ["capture"],
      },
    ],
  });
});

afterAll(() => {
  // A null capture means this file loaded without `@intx/log` having installed
  // its default sink, which cannot happen -- importing the package runs the
  // install. Resetting here instead would leave the worker with no logging
  // configuration at all, and the install cannot re-fire to repair it.
  if (!savedLogConfig) {
    throw new Error(
      "no logging configuration was captured before this suite replaced it",
    );
  }
  configureSync({ reset: true, ...savedLogConfig });
});

beforeEach(() => {
  capturedLogs.length = 0;
});

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

async function signedMessage(): Promise<{
  raw: Uint8Array;
  publicKey: Uint8Array;
}> {
  const crypto = createEd25519Crypto(await generateKeyPair());
  const content = assembleSignedContent({
    kind: "conversation",
    text: "hello world",
  });
  const sig = await createDetachedSignatureFromProvider(content, crypto);
  return {
    raw: assembleMessage(headersFrom(SENDER), content, sig),
    publicKey: crypto.getPublicKey(),
  };
}

/**
 * The captured ERROR lines that name the sender key as the condition.
 *
 * LogTape splits a record's message at each placeholder, so the literal text
 * arrives as several pieces with the interpolated values between them. Joining
 * the string pieces is what lets a phrase spanning a placeholder be matched.
 */
function keyFaultLines(): CapturedLog[] {
  return capturedLogs.filter((r) => {
    if (r.level !== "error") return false;
    const text = r.message
      .filter((piece) => typeof piece === "string")
      .join("");
    return text.includes("sender key") && text.includes("unusable");
  });
}

describe("unusable cached sender key logging", () => {
  test("names the key as the condition, with the sender and the byte count", async () => {
    const { raw, publicKey } = await signedMessage();
    const truncated = createPublicKeyCrypto(publicKey.slice(0, 7));

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: SENDER,
        messageId: "mid",
        agentAddress: AGENT_ADDRESS,
      },
      () => truncated,
    );

    expect(verdict.signature).toBe("error");
    const lines = keyFaultLines();
    expect(lines).toHaveLength(1);
    expect(lines[0]?.properties.authenticatedSender).toBe(SENDER);
    expect(lines[0]?.properties.keyBytes).toBe(7);
  });

  test("a signature that merely fails logs no key fault", async () => {
    // The boundary the diagnosis must respect. A well-formed key that is not
    // the signer's is a check that ran and failed -- the author's `invalid` to
    // relax. Naming the keyring here would send an operator hunting a cache
    // problem that does not exist.
    const { raw } = await signedMessage();
    const wrongKey = createEd25519Crypto(
      await generateKeyPair(),
    ).getPublicKey();

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: SENDER,
        messageId: "mid-wrong-key",
        agentAddress: AGENT_ADDRESS,
      },
      () => createPublicKeyCrypto(wrongKey),
    );

    expect(verdict.signature).toBe("invalid");
    expect(keyFaultLines()).toEqual([]);
  });
});
