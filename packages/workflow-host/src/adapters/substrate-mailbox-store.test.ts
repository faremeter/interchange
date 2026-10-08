import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import { type } from "arktype";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import type { Ed25519Crypto } from "@intx/crypto";
import {
  assembleMessage,
  assembleSignedContent,
  createDetachedSignatureFromProvider,
  generateMessageId,
  parseHeaderSection,
} from "@intx/mime";
import type { MessageHeaders } from "@intx/mime";
import type { CryptoProvider } from "@intx/types/runtime";
import { createRepoStore, workflowRunAuthorize } from "@intx/hub-sessions";
import type {
  KindHandler,
  Principal,
  RepoId,
  RepoStore,
} from "@intx/hub-sessions";
import { executeSearch, fetchFull } from "@intx/mailbox";
import type { StoredEnvelope } from "@intx/mailbox";

import {
  createSubstrateMailboxStore,
  MAILBOX_INBOX_DIR,
  MAILBOX_INBOX_PREFIX,
  MAILBOX_INDEX_FILE,
  MAILBOX_PREFIX,
} from "./substrate-mailbox-store";

const REF = "refs/heads/main";
const tempDirs: string[] = [];

// The backing writes a top-level `mailbox/` subtree. Its push-time
// validation is owned by the hub-replication layer, not this package; a
// permissive handler isolates this backing's persistence contract.
const permissiveHandler: KindHandler = {
  kind: "workflow-run",
  directoryPrefix: "workflow-runs",
  validatePush: () => ({ ok: true }),
  onRefUpdated: () => {
    /* no-op */
  },
};

let signingKey: Awaited<ReturnType<typeof generateKeyPair>>;

beforeAll(async () => {
  signingKey = await generateKeyPair();
});

afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fs.promises.rm(d, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

async function makeTempDir(): Promise<string> {
  const d = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "substrate-mailbox-"),
  );
  tempDirs.push(d);
  return d;
}

function makeStore(dataDir: string): RepoStore {
  return createRepoStore({
    dataDir,
    signingKey,
    handlers: { "workflow-run": permissiveHandler },
    authorize: workflowRunAuthorize,
  });
}

/** The put and delete path sets one `writeTreeDelta` call committed. */
type DeltaRecord = { puts: string[]; deletes: string[] };

// Wrap a `RepoStore` so each `writeTreeDelta` records the exact paths its
// `computeDelta` produced, then delegates unchanged. The wrapper decorates the
// caller's `computeDelta`, so it observes the same puts/deletes the store
// commits.
function createRecordingSubstrate(
  inner: RepoStore,
  recorded: DeltaRecord[],
): RepoStore {
  return {
    ...inner,
    writeTreeDelta(principal, repoId, ref, args) {
      return inner.writeTreeDelta(principal, repoId, ref, {
        ...args,
        computeDelta: async (parentCommitSha, prior) => {
          const delta = await args.computeDelta(parentCommitSha, prior);
          recorded.push({
            puts: Object.keys(delta.puts),
            deletes: [...delta.deletes],
          });
          return delta;
        },
      });
    },
  };
}

type Handles = {
  dataDir: string;
  repoId: RepoId;
  principal: Principal;
};

async function makeHandles(deploymentId: string): Promise<Handles> {
  const dataDir = await makeTempDir();
  const repoId: RepoId = { kind: "workflow-run", id: deploymentId };
  // `Principal` exposes only `kind`; concrete fields are narrowed by kind
  // handlers, so assign through an intermediate shape.
  const principalShape = {
    kind: "workflow-process" as const,
    anchorRunId: deploymentId,
  };
  const principal: Principal = principalShape;
  return { dataDir, repoId, principal };
}

function openStore(handles: Handles, dataDir?: string) {
  return createSubstrateMailboxStore({
    substrate: makeStore(dataDir ?? handles.dataDir),
    repoId: handles.repoId,
    principal: handles.principal,
    ref: REF,
  });
}

function headersFor(fields: {
  from: string;
  to: string[];
  subject: string;
  messageId: string;
  date: Date;
}): MessageHeaders {
  return {
    from: fields.from,
    to: fields.to,
    cc: undefined,
    date: fields.date,
    messageId: fields.messageId,
    subject: fields.subject,
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

async function makeSignedMessage(
  crypto: CryptoProvider,
  fields: {
    from: string;
    to: string[];
    subject: string;
    messageId: string;
    text: string;
    date?: Date;
  },
): Promise<{ raw: Uint8Array; envelope: StoredEnvelope }> {
  const date = fields.date ?? new Date("2026-02-01T00:00:00Z");
  const headers = headersFor({
    from: fields.from,
    to: fields.to,
    subject: fields.subject,
    messageId: fields.messageId,
    date,
  });
  const signedContent = assembleSignedContent({
    kind: "conversation",
    text: fields.text,
  });
  const signature = await createDetachedSignatureFromProvider(
    signedContent,
    crypto,
  );
  const raw = assembleMessage(headers, signedContent, signature);
  const envelope: StoredEnvelope = {
    messageId: fields.messageId,
    from: fields.from,
    to: fields.to,
    subject: fields.subject,
    date,
    inReplyTo: undefined,
    references: [],
    interchangeType: "conversation.message",
    interchangeCorrelationId: undefined,
  };
  return { raw, envelope };
}

async function senderCrypto(): Promise<Ed25519Crypto> {
  return createEd25519Crypto(await generateKeyPair());
}

/**
 * Drop one named header's line from a message's header section, leaving every
 * other byte alone. The detached signature covers the signed-content part
 * inside the body rather than the outer headers, so the message still verifies
 * -- the raw counterpart of an envelope whose field is absent.
 */
function stripHeader(raw: Uint8Array, name: string): Uint8Array {
  const { headerEnd } = parseHeaderSection(raw);
  const prefix = `${name.toLowerCase()}:`;
  const kept = new TextDecoder()
    .decode(raw.subarray(0, headerEnd))
    .split("\r\n")
    .filter((line) => !line.toLowerCase().startsWith(prefix))
    .join("\r\n");
  const keptBytes = new TextEncoder().encode(kept);
  const rest = raw.subarray(headerEnd);
  const out = new Uint8Array(keptBytes.length + rest.length);
  out.set(keptBytes, 0);
  out.set(rest, keptBytes.length);
  return out;
}

/**
 * The nullable envelope fields each committed message holds, in index order.
 * Reads `index.json` as JSON rather than through the store, so the assertion
 * is about the durable on-disk encoding, not a serialize/deserialize pair
 * that could agree on a value no other reader expects.
 */
const IndexEnvelopes = type({
  messages: type({
    envelope: { from: "string | null", date: "string | null" },
  }).array(),
});

async function readIndexEnvelopes(
  handles: Handles,
): Promise<{ from: string | null; date: string | null }[]> {
  const substrate = makeStore(handles.dataDir);
  const reads = await substrate.openCommittedReads(
    handles.principal,
    handles.repoId,
    REF,
  );
  if (reads === null) throw new Error("no committed reads");
  const entries = await reads.listDir(`${MAILBOX_PREFIX}/${MAILBOX_INBOX_DIR}`);
  const entry = entries.find((e) => e.name === MAILBOX_INDEX_FILE);
  if (entry === undefined) {
    throw new Error("the committed tree holds no index.json");
  }
  const raw: unknown = JSON.parse(
    new TextDecoder().decode(await reads.readBlobByOid(entry.oid)),
  );
  const parsed = IndexEnvelopes(raw);
  if (parsed instanceof type.errors) {
    throw new Error(`index.json is not index-shaped: ${parsed.summary}`);
  }
  return parsed.messages.map((m) => m.envelope);
}

/**
 * The `version` every committed `index.json` carries. Written as a literal
 * rather than imported: the loader matches the version exactly and carries no
 * migration arm, so importing the production constant would let both sides
 * move together and assert nothing.
 */
const COMMITTED_INDEX_VERSION = 1;

/**
 * The committed `index.json`, validated only as far as its `version`.
 * Undeclared keys are ignored, so the parsed object still carries every
 * other index field for `rewriteIndexVersion` to spread.
 */
const CommittedIndex = type({
  version: "number",
  "+": "ignore",
});

async function readCommittedIndex(handles: Handles) {
  const substrate = makeStore(handles.dataDir);
  const reads = await substrate.openCommittedReads(
    handles.principal,
    handles.repoId,
    REF,
  );
  if (reads === null) throw new Error("no committed reads");
  const entries = await reads.listDir(`${MAILBOX_PREFIX}/${MAILBOX_INBOX_DIR}`);
  const entry = entries.find((e) => e.name === MAILBOX_INDEX_FILE);
  if (entry === undefined) {
    throw new Error("the committed tree holds no index.json");
  }
  const raw: unknown = JSON.parse(
    new TextDecoder().decode(await reads.readBlobByOid(entry.oid)),
  );
  const parsed = CommittedIndex(raw);
  if (parsed instanceof type.errors) {
    throw new Error(`index.json carries no version: ${parsed.summary}`);
  }
  return parsed;
}

/**
 * Rewrite the committed `index.json` with a different `version`, carrying
 * every other field through unchanged, so a load that fails afterwards can
 * only be failing on the version. The delta puts `index.json` alone, so each
 * `<uid>.eml` carries forward by object id.
 */
async function rewriteIndexVersion(handles: Handles, version: number) {
  const next = { ...(await readCommittedIndex(handles)), version };
  const substrate = makeStore(handles.dataDir);
  await substrate.writeTreeDelta(handles.principal, handles.repoId, REF, {
    computeDelta: async () => ({
      puts: {
        [`${MAILBOX_INBOX_PREFIX}${MAILBOX_INDEX_FILE}`]:
          new TextEncoder().encode(JSON.stringify(next)),
      },
      deletes: [],
    }),
    changedPathPrefixes: new Set([MAILBOX_INBOX_PREFIX]),
    message: "rewrite index.json version",
  });
}

describe("substrate mailbox store", () => {
  test("append assigns monotonic UID and bumps uidNext / highestModSeq", async () => {
    const handles = await makeHandles("dep-append");
    const store = await openStore(handles);

    expect(store.uidNext).toBe(1);
    expect(store.highestModSeq).toBe(0);

    const crypto = await senderCrypto();
    const a = await makeSignedMessage(crypto, {
      from: "a@example.com",
      to: ["run@dep.example.com"],
      subject: "first",
      messageId: generateMessageId("a@example.com"),
      text: "first body",
    });
    const b = await makeSignedMessage(crypto, {
      from: "a@example.com",
      to: ["run@dep.example.com"],
      subject: "second",
      messageId: generateMessageId("a@example.com"),
      text: "second body",
    });

    const uidA = store.append(a.raw, a.envelope, []);
    const uidB = store.append(b.raw, b.envelope, ["\\Seen"]);

    expect(uidA).toBe(1);
    expect(uidB).toBe(2);
    expect(store.uidNext).toBe(3);
    expect(store.highestModSeq).toBe(2);
    expect(store.messages.map((m) => m.uid)).toEqual([1, 2]);
    expect(store.pendingWrites).toBe(true);
  });

  test("search filters by from, header, and flags", async () => {
    const handles = await makeHandles("dep-search");
    const store = await openStore(handles);
    const crypto = await senderCrypto();

    const alice = await makeSignedMessage(crypto, {
      from: "alice@example.com",
      to: ["run@dep.example.com"],
      subject: "hello",
      messageId: generateMessageId("alice@example.com"),
      text: "from alice",
    });
    const bob = await makeSignedMessage(crypto, {
      from: "bob@example.com",
      to: ["run@dep.example.com"],
      subject: "hi",
      messageId: generateMessageId("bob@example.com"),
      text: "from bob",
    });
    const aliceUid = store.append(alice.raw, alice.envelope, ["\\Seen"]);
    store.append(bob.raw, bob.envelope, []);

    const byFrom = await executeSearch("INBOX", store, { from: "alice" });
    expect(byFrom.map((r) => r.uid)).toEqual([aliceUid]);

    // The `header` predicate scans headers the envelope does not carry, so
    // it reads each candidate's raw bytes on demand; before the flush those
    // bytes are the still-pending append raw.
    const bySubjectHeader = await executeSearch("INBOX", store, {
      header: { field: "Subject", contains: "hello" },
    });
    expect(bySubjectHeader.map((r) => r.uid)).toEqual([aliceUid]);

    const seen = await executeSearch("INBOX", store, { hasFlags: ["\\Seen"] });
    expect(seen.map((r) => r.uid)).toEqual([aliceUid]);

    const unseen = await executeSearch("INBOX", store, {
      missingFlags: ["\\Seen"],
    });
    expect(unseen).toHaveLength(1);
    expect(unseen[0]?.uid).not.toBe(aliceUid);
  });

  test("fetchFull returns the raw message and verifies its signature", async () => {
    const handles = await makeHandles("dep-fetch");
    const store = await openStore(handles);
    const crypto = await senderCrypto();
    const msg = await makeSignedMessage(crypto, {
      from: "signer@example.com",
      to: ["run@dep.example.com"],
      subject: "signed",
      messageId: generateMessageId("signer@example.com"),
      text: "verified body",
    });
    const uid = store.append(msg.raw, msg.envelope, []);

    // The resident message model carries no raw bytes; they are read on
    // demand and are byte-identical to the input.
    expect("raw" in (store.find(uid) ?? {})).toBe(false);
    expect(await store.readRaw(uid)).toEqual(msg.raw);

    const getCrypto = (from: string): CryptoProvider | undefined =>
      from === "signer@example.com" ? crypto : undefined;
    const full = await fetchFull({ uid, mailbox: "INBOX" }, store, getCrypto);
    expect(full.headers.from).toBe("signer@example.com");
    expect(full.signatureStatus).toBe("valid");
    expect(full.content).toBe("verified body");
  });

  test("addFlags / removeFlags / remove mutate and advance modseq", async () => {
    const handles = await makeHandles("dep-flags");
    const store = await openStore(handles);
    const crypto = await senderCrypto();
    const one = await makeSignedMessage(crypto, {
      from: "a@example.com",
      to: ["run@dep.example.com"],
      subject: "one",
      messageId: generateMessageId("a@example.com"),
      text: "one",
    });
    const two = await makeSignedMessage(crypto, {
      from: "a@example.com",
      to: ["run@dep.example.com"],
      subject: "two",
      messageId: generateMessageId("a@example.com"),
      text: "two",
    });
    const uid1 = store.append(one.raw, one.envelope, []);
    const uid2 = store.append(two.raw, two.envelope, []);
    expect(store.highestModSeq).toBe(2);

    const flagged = store.addFlags(uid1, ["\\Seen", "\\Flagged"]);
    expect(Array.from(flagged.flags).sort()).toEqual(["\\Flagged", "\\Seen"]);
    expect(store.highestModSeq).toBe(3);

    const unflagged = store.removeFlags(uid1, ["\\Flagged"]);
    expect(Array.from(unflagged.flags)).toEqual(["\\Seen"]);
    expect(store.highestModSeq).toBe(4);

    store.remove(uid2);
    expect(store.messages.map((m) => m.uid)).toEqual([uid1]);
    expect(store.find(uid2)).toBeUndefined();
    // remove advances modseq so a QRESYNC client learns of the vanish.
    expect(store.highestModSeq).toBe(5);
    // uidNext never regresses even though the message was removed.
    expect(store.uidNext).toBe(3);
  });

  test("sync reports changed / vanished deltas and full-resync on uidValidity change", async () => {
    const handles = await makeHandles("dep-sync");
    const store = await openStore(handles);
    const crypto = await senderCrypto();
    const m1 = await makeSignedMessage(crypto, {
      from: "a@example.com",
      to: ["run@dep.example.com"],
      subject: "m1",
      messageId: generateMessageId("a@example.com"),
      text: "m1",
    });
    const m2 = await makeSignedMessage(crypto, {
      from: "a@example.com",
      to: ["run@dep.example.com"],
      subject: "m2",
      messageId: generateMessageId("a@example.com"),
      text: "m2",
    });
    const uid1 = store.append(m1.raw, m1.envelope, []);
    const uid2 = store.append(m2.raw, m2.envelope, []);

    // Client is caught up through the two appends (modseq 2).
    const caughtUp = store.sync({
      uidValidity: store.uidValidity,
      highestModSeq: 2,
    });
    if (caughtUp.resync) throw new Error("unexpected resync");
    expect(caughtUp.changed).toHaveLength(0);
    expect(caughtUp.vanished).toHaveLength(0);

    // A flag change on uid1 and an expunge of uid2 both advance modseq.
    store.addFlags(uid1, ["\\Seen"]);
    store.remove(uid2);

    const delta = store.sync({
      uidValidity: store.uidValidity,
      highestModSeq: 2,
    });
    if (delta.resync) throw new Error("unexpected resync");
    expect(delta.changed.map((m) => m.uid)).toEqual([uid1]);
    expect(delta.vanished).toEqual([uid2]);

    // A mismatched uidValidity forces a full resync carrying the live set.
    const resync = store.sync({
      uidValidity: store.uidValidity + 1,
      highestModSeq: 0,
    });
    if (!resync.resync) throw new Error("expected resync");
    expect(resync.messages.map((m) => m.uid)).toEqual([uid1]);
  });

  test("reopening rebuilds state from the committed index.json", async () => {
    const handles = await makeHandles("dep-reopen");
    const store = await openStore(handles);
    const crypto = await senderCrypto();
    const first = await makeSignedMessage(crypto, {
      from: "a@example.com",
      to: ["run@dep.example.com"],
      subject: "keep",
      messageId: generateMessageId("a@example.com"),
      text: "keep me",
    });
    const second = await makeSignedMessage(crypto, {
      from: "b@example.com",
      to: ["run@dep.example.com"],
      subject: "drop",
      messageId: generateMessageId("b@example.com"),
      text: "drop me",
    });
    const uid1 = store.append(first.raw, first.envelope, ["\\Seen"]);
    const uid2 = store.append(second.raw, second.envelope, []);
    store.remove(uid2);
    await store.flush();
    expect(store.pendingWrites).toBe(false);

    // A second store over the same repo/ref reconstructs the mirror.
    const reopened = await openStore(handles);
    expect(reopened.uidValidity).toBe(store.uidValidity);
    expect(reopened.uidNext).toBe(store.uidNext);
    expect(reopened.highestModSeq).toBe(store.highestModSeq);
    expect(reopened.messages.map((m) => m.uid)).toEqual([uid1]);

    const kept = reopened.find(uid1);
    expect(kept).toBeDefined();
    expect(Array.from(kept?.flags ?? [])).toEqual(["\\Seen"]);
    // The reopened mirror holds metadata only; the raw is read from the
    // committed `<uid>.eml` blob on demand.
    expect(await reopened.readRaw(uid1)).toEqual(first.raw);
    expect(kept?.envelope.from).toBe("a@example.com");
    expect(kept?.envelope.subject).toBe("keep");
    // The expunged message's blob did not survive the flush.
    expect(reopened.find(uid2)).toBeUndefined();

    // fetchFull still verifies the signature over the reloaded raw bytes.
    const getCrypto = (from: string): CryptoProvider | undefined =>
      from === "a@example.com" ? crypto : undefined;
    const full = await fetchFull(
      { uid: uid1, mailbox: "INBOX" },
      reopened,
      getCrypto,
    );
    expect(full.signatureStatus).toBe("valid");
    expect(full.content).toBe("keep me");

    // The reopened store answers QRESYNC vanished from the persisted tombstone.
    const delta = reopened.sync({
      uidValidity: reopened.uidValidity,
      highestModSeq: 1,
    });
    if (delta.resync) throw new Error("unexpected resync");
    expect(delta.vanished).toEqual([uid2]);
  });

  test("does not hold raw resident after flush and reads it from disk on demand", async () => {
    const handles = await makeHandles("dep-no-resident-raw");
    const store = await openStore(handles);
    const crypto = await senderCrypto();
    const msg = await makeSignedMessage(crypto, {
      from: "a@example.com",
      to: ["run@dep.example.com"],
      subject: "resident",
      messageId: generateMessageId("a@example.com"),
      text: "resident body",
    });
    const uid = store.append(msg.raw, msg.envelope, []);

    // The resident message model never carries the raw bytes.
    expect("raw" in (store.find(uid) ?? {})).toBe(false);
    // Before flush the append raw is held pending and is readable.
    expect(await store.readRaw(uid)).toEqual(msg.raw);

    await store.flush();

    // After a successful flush the bytes are dropped from memory. The pinned
    // committed-read snapshot predates the commit, so the writer store can no
    // longer resolve them -- direct proof the raw was released.
    await expect(store.readRaw(uid)).rejects.toThrow(/not resolvable/);

    // A fresh reader opens a new snapshot that sees the committed blob and
    // reads it from disk on demand, byte-identical to the original.
    const reader = await openStore(handles);
    expect("raw" in (reader.find(uid) ?? {})).toBe(false);
    expect(await reader.readRaw(uid)).toEqual(msg.raw);

    // readRaw throws for an absent uid.
    await expect(reader.readRaw(9999)).rejects.toThrow(/not found/);
  });

  test("a fresh store over an empty repo starts at uid 1 with no messages", async () => {
    const handles = await makeHandles("dep-empty");
    const store = await openStore(handles);
    expect(store.messages).toHaveLength(0);
    expect(store.uidNext).toBe(1);
    expect(store.highestModSeq).toBe(0);
    expect(store.pendingWrites).toBe(false);
    // Flushing a clean store issues no commit and stays clean.
    await store.flush();
    expect(store.pendingWrites).toBe(false);
  });

  test("flush writes a delta and does not re-put unchanged prior blobs", async () => {
    const handles = await makeHandles("dep-delta");
    const recorded: DeltaRecord[] = [];
    const substrate = createRecordingSubstrate(
      makeStore(handles.dataDir),
      recorded,
    );
    const open = () =>
      createSubstrateMailboxStore({
        substrate,
        repoId: handles.repoId,
        principal: handles.principal,
        ref: REF,
      });
    const inbox = (name: string) => `mailbox/INBOX/${name}`;

    const store = await open();
    const crypto = await senderCrypto();
    const first = await makeSignedMessage(crypto, {
      from: "a@example.com",
      to: ["run@dep.example.com"],
      subject: "first",
      messageId: generateMessageId("a@example.com"),
      text: "first body",
    });
    const uid1 = store.append(first.raw, first.envelope, []);
    await store.flush();

    // First flush puts index.json and the single new blob, deletes nothing.
    expect(recorded).toHaveLength(1);
    expect(recorded[0]?.puts.sort()).toEqual(
      [inbox("index.json"), inbox("1.eml")].sort(),
    );
    expect(recorded[0]?.deletes).toEqual([]);

    // A second append puts ONLY the new blob; uid1's blob carries forward by
    // object id and is never re-hashed.
    const second = await makeSignedMessage(crypto, {
      from: "b@example.com",
      to: ["run@dep.example.com"],
      subject: "second",
      messageId: generateMessageId("b@example.com"),
      text: "second body",
    });
    const uid2 = store.append(second.raw, second.envelope, []);
    await store.flush();
    expect(recorded).toHaveLength(2);
    expect(recorded[1]?.puts.sort()).toEqual(
      [inbox("index.json"), inbox("2.eml")].sort(),
    );
    expect(recorded[1]?.deletes).toEqual([]);

    // A flag change touches only index.json; no `.eml` is put or deleted.
    store.addFlags(uid1, ["\\Seen"]);
    await store.flush();
    expect(recorded).toHaveLength(3);
    expect(recorded[2]?.puts).toEqual([inbox("index.json")]);
    expect(recorded[2]?.deletes).toEqual([]);

    // Removing uid2 deletes its blob and puts index.json; uid1's blob is
    // neither re-put nor deleted.
    store.remove(uid2);
    await store.flush();
    expect(recorded).toHaveLength(4);
    expect(recorded[3]?.puts).toEqual([inbox("index.json")]);
    expect(recorded[3]?.deletes).toEqual([inbox("2.eml")]);

    // The committed tree still reconstructs identically: uid1 survives with its
    // flag, uid2 is gone.
    const reopened = await open();
    expect(reopened.messages.map((m) => m.uid)).toEqual([uid1]);
    expect(Array.from(reopened.find(uid1)?.flags ?? [])).toEqual(["\\Seen"]);
    expect(reopened.find(uid2)).toBeUndefined();
  });

  test("an absent originator survives the index round trip as an absence", async () => {
    // JSON has no `undefined`, so an envelope that carried no originator is
    // written as `null` and read back as `undefined`. Any string in either
    // position -- an empty one or a stand-in address -- would present the
    // message as having named a sender it never named. Both encodings sit in
    // the same version-1 index, which is what makes the widening safe with no
    // INDEX_VERSION bump.
    const handles = await makeHandles("dep-no-originator");
    const store = await openStore(handles);
    const crypto = await senderCrypto();

    const named = await makeSignedMessage(crypto, {
      from: "alice@example.com",
      to: ["run@dep.example.com"],
      subject: "named",
      messageId: "<named@example.com>",
      text: "named body",
    });
    const source = await makeSignedMessage(crypto, {
      from: "placeholder@example.com",
      to: ["run@dep.example.com"],
      subject: "orphan",
      messageId: "<orphan@example.com>",
      text: "orphan body",
    });
    const orphanRaw = stripHeader(source.raw, "From");
    const orphanEnvelope: StoredEnvelope = {
      ...source.envelope,
      from: undefined,
    };

    const namedUid = store.append(named.raw, named.envelope, []);
    const orphanUid = store.append(orphanRaw, orphanEnvelope, []);
    await store.flush();

    expect((await readIndexEnvelopes(handles)).map((e) => e.from)).toEqual([
      "alice@example.com",
      null,
    ]);

    const reopened = await openStore(handles);
    expect(reopened.find(namedUid)?.envelope.from).toBe("alice@example.com");
    expect(reopened.find(orphanUid)?.envelope.from).toBeUndefined();
  });

  test("an absent date survives the index round trip as an absence", async () => {
    // The same widening the originator got, for the same reason: JSON has no
    // `undefined`, so an envelope whose message named no date is written as
    // `null` and read back as `undefined`. Any string in that position -- the
    // arrival instant included -- would place the message in a search window
    // on a date its sender never stated.
    const handles = await makeHandles("dep-no-date");
    const store = await openStore(handles);
    const crypto = await senderCrypto();

    const dated = await makeSignedMessage(crypto, {
      from: "alice@example.com",
      to: ["run@dep.example.com"],
      subject: "dated",
      messageId: "<dated@example.com>",
      text: "dated body",
    });
    const source = await makeSignedMessage(crypto, {
      from: "alice@example.com",
      to: ["run@dep.example.com"],
      subject: "undated",
      messageId: "<undated@example.com>",
      text: "undated body",
    });
    const undatedRaw = stripHeader(source.raw, "Date");
    const undatedEnvelope: StoredEnvelope = {
      ...source.envelope,
      date: undefined,
    };

    const datedUid = store.append(dated.raw, dated.envelope, []);
    const undatedUid = store.append(undatedRaw, undatedEnvelope, []);
    await store.flush();

    expect((await readIndexEnvelopes(handles)).map((e) => e.date)).toEqual([
      "2026-02-01T00:00:00.000Z",
      null,
    ]);

    const reopened = await openStore(handles);
    expect(reopened.find(datedUid)?.envelope.date).toEqual(
      new Date("2026-02-01T00:00:00Z"),
    );
    expect(reopened.find(undatedUid)?.envelope.date).toBeUndefined();
  });

  test("the loader matches the index version exactly", async () => {
    // `index.json` is a durable on-disk format with no migration arm: the
    // loader matches `version` as an exact literal, so a bump rejects every
    // mailbox already on disk instead of upgrading it. That is what makes a
    // backward-compatible widening of a field safe while the version stays
    // put, and why a change that genuinely needs a bump needs a migration
    // first.
    const handles = await makeHandles("dep-index-version");
    const store = await openStore(handles);
    const crypto = await senderCrypto();
    const msg = await makeSignedMessage(crypto, {
      from: "a@example.com",
      to: ["run@dep.example.com"],
      subject: "versioned",
      messageId: generateMessageId("a@example.com"),
      text: "versioned body",
    });
    const uid = store.append(msg.raw, msg.envelope, []);
    await store.flush();

    // The writer commits the pinned version, so writer and loader agree on one
    // literal rather than on whatever the constant happens to hold.
    expect((await readCommittedIndex(handles)).version).toBe(
      COMMITTED_INDEX_VERSION,
    );

    // Rewriting with the version already committed changes nothing, so the
    // mailbox still loads; without this arm a loader that refused every index
    // would satisfy the rejection arms below.
    await rewriteIndexVersion(handles, COMMITTED_INDEX_VERSION);
    const accepted = await openStore(handles);
    expect(accepted.messages.map((m) => m.uid)).toEqual([uid]);
    expect(accepted.find(uid)?.envelope.from).toBe("a@example.com");

    // Every other version is refused, in both directions: a forward bump and a
    // rollback to an older format are equally unreadable.
    for (const version of [
      COMMITTED_INDEX_VERSION - 1,
      COMMITTED_INDEX_VERSION + 1,
    ]) {
      await rewriteIndexVersion(handles, version);
      await expect(openStore(handles)).rejects.toThrow(/invalid .*index\.json/);
    }
  });
});
