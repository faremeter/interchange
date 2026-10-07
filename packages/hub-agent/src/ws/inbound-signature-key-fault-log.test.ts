// The inbound-mail gate's diagnosis lines for a sender key it cannot verify
// against.
//
// The outcome each fault produces, and the fact that no policy admits it, are
// asserted beside the decision tables in `inbound-signature.test.ts`. What is
// asserted here is the other half: that an operator can tell WHY. A keyring
// fault is an operator condition -- a truncated write, a hub-side resolution
// bug, a corrupt cache entry -- and it is invisible in the mail flow itself,
// which shows only mail being rejected.
//
// The second suite below drives the PRODUCTION construction path: a real cache
// over a real directory holding a corrupt entry, read through the real
// resolver. An injected stub proves only that the gate refuses bad material it
// is handed; it says nothing about whether a corrupt keyring ever produces any.
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
  afterEach,
} from "bun:test";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { configureSync, getConfig } from "@intx/log";
import { hexEncode } from "@intx/types";
import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  generateMessageId,
  type MessageHeaders,
} from "@intx/mime";

import {
  verifyInboundSignature,
  decideInboundAdmission,
  resolveInboundMailPolicy,
} from "./inbound-signature";
import {
  createPublicKeyCrypto,
  createSenderCryptoResolver,
} from "../sender-crypto";
import { createSenderKeyCache } from "../sender-key-cache";

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

/** The captured ERROR lines the gate emits when the verify itself faulted. */
function verifyFaultLines(): CapturedLog[] {
  return capturedLogs.filter((r) => {
    if (r.level !== "error") return false;
    const text = r.message
      .filter((piece) => typeof piece === "string")
      .join("");
    return text.includes("verify FAULTED");
  });
}

const tempDirs: string[] = [];

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fsp.rm(d, { recursive: true, force: true })),
  );
});

// A plain durable write is enough here; the atomicity and fsync of the
// production primitive are orthogonal to what these tests assert.
async function cacheOverDir(entries: { filename: string; contents: string }[]) {
  const dataDir = await fsp.mkdtemp(
    path.join(os.tmpdir(), "inbound-key-fault-"),
  );
  tempDirs.push(dataDir);
  const dir = path.join(dataDir, "sender-keys");
  await fsp.mkdir(dir, { recursive: true });
  for (const entry of entries) {
    await fsp.writeFile(path.join(dir, entry.filename), entry.contents, "utf8");
  }
  return createSenderKeyCache({
    dataDir,
    writeFileDurable: async (filePath, contents) => {
      await fsp.writeFile(filePath, contents, "utf8");
    },
    removeFileDurable: async (filePath) => {
      await fsp.rm(filePath, { force: true });
    },
  });
}

function senderKeyFile(publicKey: Uint8Array, keep: number) {
  return {
    filename: hexEncode(new TextEncoder().encode(SENDER)),
    contents: JSON.stringify({
      address: SENDER,
      publicKey: hexEncode(publicKey.slice(0, keep)),
    }),
  };
}

describe("a corrupt on-disk sender key driven through the production resolver", () => {
  test("faults rather than reporting the sender as one with no cached key", async () => {
    const { raw, publicKey } = await signedMessage();
    // Truncated key material: what a partial write to the keyring leaves.
    const cache = await cacheOverDir([senderKeyFile(publicKey, 31)]);

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: SENDER,
        messageId: "mid-corrupt-entry",
        agentAddress: AGENT_ADDRESS,
      },
      createSenderCryptoResolver(cache),
    );

    expect(verdict.signature).toBe("error");
    expect(verdict.fromMatch).toBe("notEvaluated");
    const lines = verifyFaultLines();
    expect(lines).toHaveLength(1);
    expect(lines[0]?.properties.authenticatedSender).toBe(SENDER);
    expect(String(lines[0]?.properties.cause)).toContain("failed to load");
  });

  test("a policy relaxing the uncached-sender outcome does not admit it", async () => {
    const { raw, publicKey } = await signedMessage();
    const cache = await cacheOverDir([senderKeyFile(publicKey, 31)]);

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: SENDER,
        messageId: "mid-corrupt-entry-policy",
        agentAddress: AGENT_ADDRESS,
      },
      createSenderCryptoResolver(cache),
    );

    // The document's own worked example. An operator's corrupt file must not be
    // relaxable by a workflow author who never knew about it.
    const decision = decideInboundAdmission(
      verdict,
      resolveInboundMailPolicy({ unknown: "admit" }),
    );

    expect(decision.findings).toEqual([]);
    expect(decision.rejectedBy).toBe("error");
  });

  test("an intact entry over the same path verifies clean", async () => {
    // The control: the harness builds a cache whose reads work, so the refusal
    // above is the corrupt entry and not the construction.
    const { raw, publicKey } = await signedMessage();
    const cache = await cacheOverDir([
      senderKeyFile(publicKey, publicKey.length),
    ]);

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: SENDER,
        messageId: "mid-intact-entry",
        agentAddress: AGENT_ADDRESS,
      },
      createSenderCryptoResolver(cache),
    );

    expect(verdict.signature).toBe("valid");
    expect(verdict.fromMatch).toBe("match");
    expect(verifyFaultLines()).toEqual([]);
  });
});
