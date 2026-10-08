import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";

import { generateKeyPair } from "@intx/crypto";
import {
  base64Decode,
  base64Encode,
  hexDecode,
  hexEncode,
  signalName,
} from "@intx/types";
import type { InferenceSource, OutboundMessage } from "@intx/types/runtime";
import { isMail } from "@intx/types/runtime";
import type { RepoId, RepoStore } from "@intx/hub-sessions";
import { StaleInboxEnqueueError } from "@intx/hub-sessions";
import type { EnqueueInboxOutcome } from "@intx/hub-sessions";
import {
  createMemoryFrameStream,
  createMemoryNdjsonStream,
  createSupervisorReaper,
  createMockMailBus,
  type MockMailBus,
  createSpawnObserver,
  parseTriggerFireRunIds,
  waitForTriggerFireRunIds,
  waitForUpstreamPayload,
  waitForUpstreamPayloads,
  createChangeNotifier,
  type UpstreamFrameSource,
} from "@intx/workflow-host/testing";
import { resumeFromLog } from "@intx/workflow/state-machine";

import {
  createWorkflowSupervisor,
  type DrainTimeoutAccumulator,
  type DrainTimeoutAccumulatorFactory,
  type DrainTimeoutOpts,
  type InboxPrimitives,
  type MailBusBindings,
  type SubprocessSpawner,
  type SubprocessHandle,
  type SignedPayload,
  type WorkflowSupervisorBindings,
} from "./index";
import {
  assembleCredentialsSnapshot,
  defaultStepRepoId,
  hashGrants,
  STEP_GRANTS_PATH,
} from "./credentials";
import { commitCancelRequested } from "./cancel-signing";
import {
  createControlChannelSender,
  createEventChannelSender,
  ControlPayload,
  receiveControlChannel,
  SignedEnvelope,
  generateHmacKey,
  generateChannelId,
} from "../ipc/index";

/** Frame types the supervisor writes to the child control stream, in write order; asserts grants-updated precedes trigger.fire. */
function parseControlFrameTypes(lines: readonly string[]): string[] {
  const types: string[] = [];
  for (const line of lines) {
    const raw: unknown = JSON.parse(line);
    const signed = SignedEnvelope(raw);
    if (signed instanceof type.errors) continue;
    const payload = ControlPayload(signed.envelope.payload);
    if (payload instanceof type.errors) continue;
    types.push(payload.type);
  }
  return types;
}

/** Parse every signal.deliver frame in the supervisor-to-child stream. */
function parseSignalDelivers(
  lines: readonly string[],
): { signalName: string; signalId: string; payload: unknown }[] {
  const out: { signalName: string; signalId: string; payload: unknown }[] = [];
  for (const line of lines) {
    if (!line.includes("signal.deliver")) continue;
    const raw: unknown = JSON.parse(line);
    const signed = SignedEnvelope(raw);
    if (signed instanceof type.errors) continue;
    const payload = ControlPayload(signed.envelope.payload);
    if (payload instanceof type.errors) continue;
    if (payload.type !== "signal.deliver") continue;
    out.push({
      signalName: payload.data.signalName,
      signalId: payload.data.signalId,
      payload: payload.data.payload,
    });
  }
  return out;
}

/** Parse every mailbox.notify frame, in write order; asserts the eager commit notifies with the assigned uid and envelope. */
function parseMailboxNotifies(lines: readonly string[]): {
  runId: string;
  mailbox: string;
  uid: number;
  headers: {
    from?: string;
    to: string[];
    messageId?: string;
    subject?: string;
  };
}[] {
  const out: {
    runId: string;
    mailbox: string;
    uid: number;
    headers: {
      from?: string;
      to: string[];
      messageId?: string;
      subject?: string;
    };
  }[] = [];
  for (const line of lines) {
    if (!line.includes("mailbox.notify")) continue;
    const raw: unknown = JSON.parse(line);
    const signed = SignedEnvelope(raw);
    if (signed instanceof type.errors) continue;
    const payload = ControlPayload(signed.envelope.payload);
    if (payload instanceof type.errors) continue;
    if (payload.type !== "mailbox.notify") continue;
    out.push({
      runId: payload.data.runId,
      mailbox: payload.data.mailbox,
      uid: payload.data.uid,
      headers: {
        ...(payload.data.headers.from !== undefined
          ? { from: payload.data.headers.from }
          : {}),
        to: payload.data.headers.to,
        ...(payload.data.headers.messageId !== undefined
          ? { messageId: payload.data.headers.messageId }
          : {}),
        ...(payload.data.headers.subject !== undefined
          ? { subject: payload.data.headers.subject }
          : {}),
      },
    });
  }
  return out;
}

/** Resolve with the mailbox.mutate.response answering requestId; waits on the write so only the frame's arrival decides the outcome. */
async function awaitMutateResponse(
  stream: UpstreamFrameSource,
  requestId: string,
): Promise<
  Extract<ControlPayload, { type: "mailbox.mutate.response" }>["data"]
> {
  const payload = await waitForUpstreamPayload(
    stream,
    "mailbox.mutate.response",
    (p) => p.data.requestId === requestId,
  );
  return payload.data;
}

/** Resolve with the `mailbox.call.response` frame answering `requestId`. */
async function awaitCallResponse(
  stream: UpstreamFrameSource,
  requestId: string,
): Promise<Extract<ControlPayload, { type: "mailbox.call.response" }>["data"]> {
  const payload = await waitForUpstreamPayload(
    stream,
    "mailbox.call.response",
    (frame) => frame.data.requestId === requestId,
  );
  return payload.data;
}

/** Parse the Mail payload of each trigger.fire frame; asserts the eager commit left the step-input payload unchanged. */
function parseTriggerFirePayloads(lines: readonly string[]): unknown[] {
  const out: unknown[] = [];
  for (const line of lines) {
    if (!line.includes("trigger.fire")) continue;
    const raw: unknown = JSON.parse(line);
    const signed = SignedEnvelope(raw);
    if (signed instanceof type.errors) continue;
    const payload = ControlPayload(signed.envelope.payload);
    if (payload instanceof type.errors) continue;
    if (payload.type !== "trigger.fire") continue;
    out.push(payload.data.payload);
  }
  return out;
}

/** Build a minimal well-formed RFC 2822 message the supervisor can decode. */
function buildInboundMail(opts: {
  from: string;
  to: string;
  subject: string;
  messageId: string;
  body: string;
  references?: readonly string[];
}): Uint8Array {
  const lines = [
    `From: ${opts.from}`,
    `To: ${opts.to}`,
    `Subject: ${opts.subject}`,
    `Message-ID: ${opts.messageId}`,
    "Date: Tue, 01 Jan 2030 00:00:00 +0000",
    ...(opts.references !== undefined
      ? [`References: ${opts.references.join(" ")}`]
      : []),
    "Content-Type: text/plain; charset=utf-8",
    "",
    opts.body,
  ];
  return new TextEncoder().encode(lines.join("\r\n"));
}

/** Signed conversation with a text body and an attachment; fetchPart needs multipart, fetchFull reports attachments only on multipart/signed. */
function buildSignedConversation(opts: {
  from: string;
  to: string;
  subject: string;
  messageId: string;
  body: string;
  attachment: { filename: string; body: string };
}): Uint8Array {
  const text = [
    "Content-Type: text/plain; charset=utf-8",
    "Content-Transfer-Encoding: 7bit",
    "",
    opts.body,
  ].join("\r\n");
  const file = [
    "Content-Type: text/plain",
    "Content-Transfer-Encoding: 7bit",
    `Content-Disposition: attachment; filename="${opts.attachment.filename}"`,
    "",
    opts.attachment.body,
  ].join("\r\n");
  const mixed = [
    `Content-Type: multipart/mixed; boundary="inner"`,
    "",
    "--inner",
    text,
    "--inner",
    file,
    "--inner--",
  ].join("\r\n");
  const lines = [
    `From: ${opts.from}`,
    `To: ${opts.to}`,
    `Subject: ${opts.subject}`,
    `Message-ID: ${opts.messageId}`,
    "Date: Tue, 01 Jan 2030 00:00:00 +0000",
    "Interchange-Type: conversation.message",
    `Content-Type: multipart/signed; protocol="application/pgp-signature"; micalg=pgp-sha512; boundary="outer"`,
    "",
    "--outer",
    mixed,
    "--outer",
    "Content-Type: application/pgp-signature",
    "",
    "FAKE",
    "--outer--",
    "",
  ];
  return new TextEncoder().encode(lines.join("\r\n"));
}

/** Shape of a captured writeTreePreservingPrefix invocation the mailbox tests inspect via onWrite. */
type CapturedWrite = {
  preservePrefix: string;
  files: Record<string, string | Uint8Array>;
};

const MailboxIndex = type({
  version: "number",
  uidValidity: "number",
  uidNext: "number",
  highestModSeq: "number",
  messages: type({
    uid: "number",
    modseq: "number",
    flags: "string[]",
    "+": "ignore",
  }).array(),
  "+": "ignore",
});

/** Parse every committed mailbox/INBOX/index.json, in write order: the first is the arrival commit, a later one the flag mark. */
function mailboxIndexes(
  writes: readonly CapturedWrite[],
): (typeof MailboxIndex.infer)[] {
  const out: (typeof MailboxIndex.infer)[] = [];
  for (const write of writes) {
    if (write.preservePrefix !== "mailbox/INBOX/") continue;
    const blob = write.files["mailbox/INBOX/index.json"];
    if (blob === undefined) continue;
    const text =
      typeof blob === "string" ? blob : new TextDecoder().decode(blob);
    const parsed: unknown = JSON.parse(text);
    const index = MailboxIndex(parsed);
    if (index instanceof type.errors) {
      throw new Error(`unexpected mailbox index shape: ${index.summary}`);
    }
    out.push(index);
  }
  return out;
}

function createNoopDrainAccumulator(): DrainTimeoutAccumulator {
  return {
    start() {
      // noop
    },
    pause() {
      // noop
    },
    resume() {
      // noop
    },
    stop() {
      // noop
    },
    accumulatedMs() {
      return 0;
    },
    get escalated() {
      return false;
    },
    async disposed() {
      // noop
    },
  };
}

function parseSourcesUpdatedFrames(
  lines: readonly string[],
): { sources: InferenceSource[]; defaultSource: string }[] {
  const out: { sources: InferenceSource[]; defaultSource: string }[] = [];
  for (const line of lines) {
    if (!line.includes("sources-updated")) continue;
    const raw: unknown = JSON.parse(line);
    const signed = SignedEnvelope(raw);
    if (signed instanceof type.errors) continue;
    const payload = ControlPayload(signed.envelope.payload);
    if (payload instanceof type.errors) continue;
    if (payload.type !== "sources-updated") continue;
    out.push({
      sources: payload.data.sources,
      defaultSource: payload.data.defaultSource,
    });
  }
  return out;
}

function parseCredentialsUpdatedFrames(lines: readonly string[]) {
  return lines.flatMap((line) => {
    if (!line.includes("credentials-updated")) return [];
    const raw: unknown = JSON.parse(line);
    const signed = SignedEnvelope(raw);
    if (signed instanceof type.errors) return [];
    const payload = ControlPayload(signed.envelope.payload);
    if (payload instanceof type.errors) return [];
    if (payload.type !== "credentials-updated") return [];
    return [payload.data.delivery];
  });
}

// Like `parseCredentialsUpdatedFrames` but returns the full frame data (delivery plus optional `revoke`) so a test can assert removal.
function parseCredentialsUpdatedData(lines: readonly string[]) {
  return lines.flatMap((line) => {
    if (!line.includes("credentials-updated")) return [];
    const raw: unknown = JSON.parse(line);
    const signed = SignedEnvelope(raw);
    if (signed instanceof type.errors) return [];
    const payload = ControlPayload(signed.envelope.payload);
    if (payload instanceof type.errors) return [];
    if (payload.type !== "credentials-updated") return [];
    return [payload.data];
  });
}

const CancelRequestedBlob = type({
  type: "string",
  seq: "number",
  origin: "string",
  reason: "string",
  signature: {
    principalKind: "string",
    sig: "string",
  },
  "+": "ignore",
});

function readCancelRequestedBlob(
  raw: string,
): typeof CancelRequestedBlob.infer {
  const parsed: unknown = JSON.parse(raw);
  const validated = CancelRequestedBlob(parsed);
  if (validated instanceof type.errors) {
    throw new Error(`unexpected blob shape: ${validated.summary}`);
  }
  return validated;
}

async function makeTempDir(prefix: string): Promise<string> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  return dir;
}

// A mail bus that settles on the handler's returned promise, so ack/withhold is asserted
// directly. settle reads a handlers map nothing deletes from, so it still reaches the handler
// after shutdown -- unlike the inherited deliver, which reads base subscribers emptied by the
// disposer and unregisterAddress. Superset of createMockMailBus's shape.
function createSettleableMailBus(): MockMailBus & {
  settle(address: string, message: Uint8Array): Promise<void>;
} {
  // Composed on the shared double so the registration waiter and history keep working; only a handle on the subscribed handler is added.
  const base = createMockMailBus();
  const handlers = new Map<string, (m: Uint8Array) => Promise<void>>();
  return {
    ...base,
    subscribeMailForAddress(
      address: string,
      handler: (rawMessage: Uint8Array) => Promise<void>,
    ) {
      handlers.set(address, handler);
      return base.subscribeMailForAddress(address, handler);
    },
    settle(address: string, message: Uint8Array): Promise<void> {
      const handler = handlers.get(address);
      if (handler === undefined) {
        throw new Error(`no subscriber for ${address}`);
      }
      return handler(message);
    },
  };
}

// In-memory inbox primitives with only enqueueInbox replaced, so a test drives each enqueue outcome through the real arrival path.
function inboxPrimitivesWithEnqueue(
  enqueue: InboxPrimitives["enqueueInbox"],
): MemoryInboxPrimitives {
  return { ...createMemoryInboxPrimitives(), enqueueInbox: enqueue };
}

function enqueuedOutcome(args: {
  address: string;
  messageId: string;
  receivedAt: number;
  mailAuditRef: { store: string; path: string };
}): EnqueueInboxOutcome {
  return {
    outcome: "enqueued",
    commitSha: "memory",
    inboxKey: `${String(args.receivedAt)}-${args.messageId}`,
    envelope: {
      messageId: args.messageId,
      receivedAt: args.receivedAt,
      address: args.address,
      mailAuditRef: args.mailAuditRef,
    },
  };
}

/** Stub RepoStore satisfying only the subset the supervisor reaches into; other methods throw so an untested path fails precisely. */
function createStubRepoStore(opts: {
  baseDir: string;
  onWrite?: (args: {
    principal: { kind: string };
    repoId: RepoId;
    ref: string;
    preservePrefix: string;
    message: string;
    files: Record<string, string | Uint8Array>;
  }) => void;
  /** Called at the start of the write, before the merge callback; may throw to simulate a substrate write failure reaching the caller unmerged. */
  beforeWrite?: (args: {
    preservePrefix: string;
    message: string;
  }) => void | Promise<void>;
  /** Carry committed files across writes (keyed by repoId/ref/prefix) so a sequence of appends
   * sees prior commits in the merge callback; off by default so per-call assertions do not race. */
  statefulWrites?: boolean;
}): RepoStore {
  const committed = new Map<string, Map<string, Uint8Array>>();
  // Latest commit sha per (repoId, ref) so resolveRef answers the tip a write advanced to; distinct from the fixed commitSha; statefulWrites.
  const refTip = new Map<string, string>();
  let refTipSeq = 0;
  function repoRefKey(repoId: RepoId, ref: string): string {
    return `${repoId.kind}/${repoId.id}\x00${ref}`;
  }
  function keyFor(repoId: RepoId, ref: string, preservePrefix: string): string {
    return `${repoId.kind}/${repoId.id}\x00${ref}\x00${preservePrefix}`;
  }
  // Merge every committed submap for a (repoId, ref) into one repo-root-relative path -> bytes map.
  function mergedTree(repoId: RepoId, ref: string): Map<string, Uint8Array> {
    const prefix = `${repoRefKey(repoId, ref)}\x00`;
    const merged = new Map<string, Uint8Array>();
    for (const [key, files] of committed) {
      if (!key.startsWith(prefix)) continue;
      for (const [path, bytes] of files) merged.set(path, bytes);
    }
    return merged;
  }
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(opts.baseDir, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix(principal, repoId, ref, args) {
      await opts.beforeWrite?.({
        preservePrefix: args.preservePrefix,
        message: args.message,
      });
      const key = keyFor(repoId, ref, args.preservePrefix);
      const existing =
        opts.statefulWrites === true
          ? (committed.get(key) ?? new Map<string, Uint8Array>())
          : new Map<string, Uint8Array>();
      const files = await args.merge(existing);
      opts.onWrite?.({
        principal,
        repoId,
        ref,
        preservePrefix: args.preservePrefix,
        message: args.message,
        files,
      });
      if (opts.statefulWrites === true) {
        const next = new Map<string, Uint8Array>();
        for (const [path, bytes] of Object.entries(files)) {
          if (!path.startsWith(args.preservePrefix)) continue;
          next.set(
            path,
            typeof bytes === "string" ? new TextEncoder().encode(bytes) : bytes,
          );
        }
        committed.set(key, next);
      }
      // Track the tip for every write (even non-stateful) so the eager-mailbox path's post-flush
      // `resolveRef` resolves and does not log a spurious fault when tests do not opt into stateful reads.
      refTipSeq += 1;
      refTip.set(
        repoRefKey(repoId, ref),
        refTipSeq.toString(16).padStart(40, "0"),
      );
      return { commitSha: "deadbeefcafef00d", newlyTerminalRuns: [] };
    },
    // The eager-mailbox flush commits a delta (index.json + a <uid>.eml per appended message);
    // model it against the committed tree and capture the puts via onWrite.
    async writeTreeDelta(principal, repoId, ref, args) {
      const prefixes = [...(args.changedPathPrefixes ?? [])];
      const preservePrefix = prefixes.length === 1 ? (prefixes[0] ?? "") : "";
      await opts.beforeWrite?.({ preservePrefix, message: args.message });
      const parentSha = refTip.get(repoRefKey(repoId, ref)) ?? null;
      const tree = mergedTree(repoId, ref);
      const delta = await args.computeDelta(parentSha, {
        async readBlobByOid(oid: string) {
          const bytes = tree.get(oid);
          if (bytes === undefined) {
            throw new Error(`stub delta prior: no blob for oid ${oid}`);
          }
          return bytes;
        },
        async listDirOids(relPath: string) {
          const base = relPath === "" ? "" : `${relPath}/`;
          const out: { name: string; oid: string }[] = [];
          for (const filePath of tree.keys()) {
            if (base !== "" && !filePath.startsWith(base)) continue;
            const rest = filePath.slice(base.length);
            if (rest.includes("/")) continue;
            out.push({ name: rest, oid: filePath });
          }
          return out;
        },
      });
      opts.onWrite?.({
        principal,
        repoId,
        ref,
        preservePrefix,
        message: args.message,
        files: delta.puts,
      });
      if (opts.statefulWrites === true) {
        const key = keyFor(repoId, ref, preservePrefix);
        const next = new Map(
          committed.get(key) ?? new Map<string, Uint8Array>(),
        );
        for (const d of delta.deletes) {
          if (d.endsWith("/")) {
            for (const p of [...next.keys()]) {
              if (p.startsWith(d)) next.delete(p);
            }
          } else {
            next.delete(d);
          }
        }
        for (const [p, bytes] of Object.entries(delta.puts)) {
          next.set(
            p,
            typeof bytes === "string" ? new TextEncoder().encode(bytes) : bytes,
          );
        }
        committed.set(key, next);
      }
      refTipSeq += 1;
      refTip.set(
        repoRefKey(repoId, ref),
        refTipSeq.toString(16).padStart(40, "0"),
      );
      return { commitSha: "deadbeefcafef00d", newlyTerminalRuns: [] };
    },
    // Without statefulWrites the committed tree is empty, so an open sees an empty INBOX and each flush starts fresh.
    async openCommittedReads(_principal, repoId, ref) {
      const tree = mergedTree(repoId, ref);
      if (tree.size === 0) return null;
      return {
        async listDir(relPath: string) {
          const base = relPath === "" ? "" : `${relPath}/`;
          const entries: { name: string; oid: string; type: "blob" }[] = [];
          for (const filePath of tree.keys()) {
            if (base !== "" && !filePath.startsWith(base)) continue;
            const rest = filePath.slice(base.length);
            if (rest.includes("/")) continue;
            entries.push({ name: rest, oid: filePath, type: "blob" });
          }
          return entries;
        },
        async readBlobByOid(oid: string) {
          const bytes = tree.get(oid);
          if (bytes === undefined) {
            throw new Error(`stub committed reads: no blob for oid ${oid}`);
          }
          return bytes;
        },
        async treeOid() {
          return null;
        },
      };
    },
    async resolveRef(_principal, repoId, ref) {
      return refTip.get(repoRefKey(repoId, ref)) ?? null;
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; missing methods throw via the proxy below
  return new Proxy(stub as RepoStore, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (value !== undefined) return value;
      return () => {
        throw new Error(
          `stub RepoStore: ${String(prop)} not implemented for this test`,
        );
      };
    },
  });
}

/** Per-address claim-check state mirroring the substrate's inbox/processing/consumed dirs, so a call sequence is observable without a repo. */
type MemoryInboxEntry = {
  messageId: string;
  receivedAt: number;
  mailAuditRef: { store: string; path: string };
  rawMessage?: string;
  rejection?: { code: string; message: string };
};

export type MemoryInboxState = {
  inbox: Map<string, MemoryInboxEntry>;
  processing: Map<string, MemoryInboxEntry>;
  consumed: Map<string, MemoryInboxEntry>;
};

export type MemoryInboxPrimitives = InboxPrimitives & {
  /** Snapshot the in-memory state for a given address (testing only). */
  snapshot(address: string): MemoryInboxState;
  /** Resolve once the predicate holds over the snapshots. */
  awaitState(predicate: () => boolean): Promise<void>;
};

function filenameKey(receivedAt: number, messageId: string): string {
  return `${String(receivedAt)}-${messageId}`;
}

function createMemoryInboxPrimitives(): MemoryInboxPrimitives {
  // Reports every mutation so a test can wait for the state it needs instead of re-reading the maps on a timer.
  const changes = createChangeNotifier();
  const byAddress = new Map<string, MemoryInboxState>();
  function getOrCreate(address: string): MemoryInboxState {
    let entry = byAddress.get(address);
    if (entry === undefined) {
      entry = {
        inbox: new Map(),
        processing: new Map(),
        consumed: new Map(),
      };
      byAddress.set(address, entry);
    }
    return entry;
  }
  return {
    awaitState: changes.until,
    snapshot(address: string): MemoryInboxState {
      return getOrCreate(address);
    },
    async enqueueInbox(_store, _principal, _repoId, args) {
      const state = getOrCreate(args.address);
      const key = filenameKey(args.receivedAt, args.messageId);
      // Mirror the real `enqueueInbox` contract: an already-present messageId is a returned outcome (ack-worthy), not a throw.
      if (state.consumed.has(args.messageId)) {
        return { outcome: "already-present", reason: "consumed" };
      }
      for (const existingKey of state.inbox.keys()) {
        if (existingKey.endsWith(`-${args.messageId}`)) {
          return { outcome: "already-present", reason: "already_inbox" };
        }
      }
      for (const existingKey of state.processing.keys()) {
        if (existingKey.endsWith(`-${args.messageId}`)) {
          return { outcome: "already-present", reason: "processing" };
        }
      }
      const envelope: MemoryInboxEntry = {
        messageId: args.messageId,
        receivedAt: args.receivedAt,
        mailAuditRef: args.mailAuditRef,
        ...(args.rawMessage !== undefined
          ? { rawMessage: args.rawMessage }
          : {}),
      };
      state.inbox.set(key, envelope);
      changes.notify();
      return {
        outcome: "enqueued",
        commitSha: "memory-inbox",
        inboxKey: key,
        envelope: {
          messageId: args.messageId,
          receivedAt: args.receivedAt,
          address: args.address,
          mailAuditRef: args.mailAuditRef,
        },
      };
    },
    async dequeueToProcessing(_store, _principal, _repoId, address) {
      const state = getOrCreate(address);
      const entries = [...state.inbox.entries()].sort(([, a], [, b]) => {
        if (a.receivedAt !== b.receivedAt) return a.receivedAt - b.receivedAt;
        if (a.messageId < b.messageId) return -1;
        if (a.messageId > b.messageId) return 1;
        return 0;
      });
      if (entries.length === 0) return null;
      const head = entries[0];
      if (head === undefined) throw new Error("unreachable");
      const [key, envelope] = head;
      state.inbox.delete(key);
      changes.notify();
      state.processing.set(key, envelope);
      changes.notify();
      return {
        commitSha: "memory-inbox",
        key,
        envelope: {
          messageId: envelope.messageId,
          receivedAt: envelope.receivedAt,
          address,
          mailAuditRef: envelope.mailAuditRef,
          ...(envelope.rawMessage !== undefined
            ? { rawMessage: envelope.rawMessage }
            : {}),
        },
      };
    },
    async markConsumed(_store, _principal, _repoId, args) {
      const state = getOrCreate(args.address);
      let foundKey: string | null = null;
      let envelope: MemoryInboxEntry | null = null;
      for (const [key, value] of state.processing) {
        if (value.messageId === args.messageId) {
          foundKey = key;
          envelope = value;
          break;
        }
      }
      if (foundKey === null || envelope === null) {
        throw new Error(
          `claim_check_processing_not_found: ${args.address} ${args.messageId}`,
        );
      }
      state.processing.delete(foundKey);
      changes.notify();
      const consumedEntry: MemoryInboxEntry = {
        ...envelope,
        ...(args.rejection !== undefined ? { rejection: args.rejection } : {}),
      };
      state.consumed.set(args.messageId, consumedEntry);
      changes.notify();
      return {
        commitSha: "memory-inbox",
        envelope: {
          messageId: envelope.messageId,
          receivedAt: envelope.receivedAt,
          address: args.address,
          runId: args.runId,
          consumedAt: args.consumedAt,
          mailAuditRef: envelope.mailAuditRef,
          ...(args.rejection !== undefined
            ? { rejection: args.rejection }
            : {}),
        },
        watermark: 0,
        prunedMessageIds: [],
      };
    },
    async replayProcessingToInbox(_store, _principal, _repoId, address) {
      const state = getOrCreate(address);
      const replayedKeys: string[] = [];
      for (const [key, value] of state.processing) {
        if (state.inbox.has(key)) {
          throw new Error(
            `claim_check_replay_collision: inbox already has ${key}`,
          );
        }
        state.inbox.set(key, value);
        changes.notify();
        replayedKeys.push(key);
      }
      state.processing.clear();
      // The per-entry notify fires while the entry is still in processing; it reports the clear, so a waiter cannot strand on processing emptying.
      changes.notify();
      return { commitSha: "memory-inbox", replayedKeys };
    },
  };
}

async function buildBindings(opts: {
  baseDir: string;
  spawner: SubprocessSpawner;
  signSpy: (kind: string, payload: Uint8Array) => SignedPayload;
  mailBus: MailBusBindings;
  onWrite?: (args: {
    principal: { kind: string };
    repoId: RepoId;
    ref: string;
    preservePrefix: string;
    message: string;
    files: Record<string, string | Uint8Array>;
  }) => void;
  beforeWrite?: (args: {
    preservePrefix: string;
    message: string;
  }) => void | Promise<void>;
  statefulWrites?: boolean;
  inboxPrimitives?: InboxPrimitives;
}): Promise<WorkflowSupervisorBindings> {
  const repoStore = createStubRepoStore({
    baseDir: opts.baseDir,
    ...(opts.onWrite !== undefined ? { onWrite: opts.onWrite } : {}),
    ...(opts.beforeWrite !== undefined
      ? { beforeWrite: opts.beforeWrite }
      : {}),
    ...(opts.statefulWrites === true ? { statefulWrites: true } : {}),
  });
  return {
    repoStore,
    signAsPrincipal: async (kind, payload) => opts.signSpy(kind, payload),
    mailBus: opts.mailBus,
    subprocessSpawner: opts.spawner,
    binaryPath: "/fake/bin/workflow-child",
    substrateEnv: { DATA_DIR: opts.baseDir },
    dynamicSpawnEnv: () => ({}),
    workflowRunRepoId: { kind: "workflow-run", id: "run_deployment-x" },
    workflowRunRef: "refs/heads/main",
    anchorRunId: "run_deployment-x",
    stepCount: 1,
    deploymentMailAddress: "run_deployment-x@example.com",
    readPrincipal: { kind: "supervisor" },
    deriveStepAddress: ({ runId, stepId }) => `${runId}-${stepId}@example.com`,
    inboxPrimitives: opts.inboxPrimitives ?? createMemoryInboxPrimitives(),
  };
}

async function seedStepGrants(
  baseDir: string,
  repoId: RepoId,
  grants: unknown[],
): Promise<void> {
  const dir = path.join(baseDir, repoId.kind, repoId.id);
  await fs.mkdir(path.join(dir, "state"), { recursive: true });
  await fs.writeFile(
    path.join(dir, STEP_GRANTS_PATH),
    JSON.stringify({ grants }),
  );
}

const supervisors = createSupervisorReaper();

afterEach(supervisors.reap);

describe("createWorkflowSupervisor", () => {
  test("factory accepts the documented WorkflowSupervisorBindings shape", async () => {
    const baseDir = await makeTempDir("supervisor-bindings-");
    const bindings = await buildBindings({
      baseDir,
      spawner: () => {
        throw new Error("spawner not invoked in this test");
      },
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus: createMockMailBus(),
    });
    const supervisor = supervisors.track(createWorkflowSupervisor(bindings));
    expect(typeof supervisor.spawn).toBe("function");
    expect(typeof supervisor.requestCancel).toBe("function");
    expect(typeof supervisor.shutdown).toBe("function");
    expect(typeof supervisor.drain).toBe("function");
    expect(typeof supervisor.recycle).toBe("function");
    expect(supervisor.getCredentialsSnapshot()).toBeNull();
  });

  test("spawn completes the IPC handshake, registers mail, and pushes credentials", async () => {
    const baseDir = await makeTempDir("supervisor-spawn-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    // Deterministic IPC keypairs: supervisor's for downstream signing, child's for upstream; the child publishes its public half in ready.
    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    let killed = false;

    let observedEnv: Record<string, string> | undefined;
    // Scoped here, not to the file: `first()` must resolve with THIS fixture's spawn, not whichever spawn happened earliest in the run.
    const spawnObserver = createSpawnObserver();
    let observedBinary: string | undefined;
    const spawner: SubprocessSpawner = ({ binaryPath, env }) => {
      observedBinary = binaryPath;
      observedEnv = env;
      spawnObserver.record(env);
      const handle: SubprocessHandle = {
        pid: 4321,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          killed = true;
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
      return handle;
    };

    const mailBus = createMockMailBus();
    const inbox = createMemoryInboxPrimitives();
    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus,
      inboxPrimitives: inbox,
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
    };
    const supervisor = supervisors.track(createWorkflowSupervisor(bindings));

    const eventsObserved: { type: string }[] = [];
    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      warmKeep: false,

      onInferenceEvent: (event) => {
        eventsObserved.push({ type: event.type });
      },
    });
    // Drive the synthetic child: once the spawner ran, sign a ready frame with the controlled IPC key and inject it upstream.
    observedEnv = await spawnObserver.first();
    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeyPair.privateKey,
      channelId,
      writer: {
        write(line: string) {
          childToSupervisor.inject(line);
        },
      },
    });
    // Wait for the address registration so delivers land in the subscription handler (a pre-subscription deliver is a no-op on the mock bus).
    await mailBus.awaitRegistered("run_deployment-x@example.com");
    // Deliver mail while the supervisor is still in `starting`; the supervisor buffers it and replays it after `ready` lands.
    mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("m1"),
    );
    mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("m2"),
    );
    await childSender.send({
      type: "ready",
      data: {
        childPid: 4321,
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });

    const result = await spawnPromise;
    expect(observedBinary).toBe("/fake/bin/workflow-child");
    expect(observedEnv).toMatchObject({
      DATA_DIR: baseDir,
      DEPLOYMENT_ID: "run_deployment-x",
      DEFINITION_HASH: "def-hash-abc",
      MAILBOX_ADDRESS: "run_deployment-x@example.com",
    });
    expect(observedEnv.IPC_CHANNEL_ID).toMatch(/^[0-9a-f]{32}$/);
    expect(observedEnv.IPC_HMAC_KEY).toMatch(/^[0-9a-f]{64}$/);
    expect(observedEnv.HOST_PUBKEY).toMatch(/^[0-9a-f]{64}$/);
    expect(observedEnv).not.toHaveProperty("HOST_PRIVATE_KEY");
    expect(result.pid).toBe(4321);
    expect(result.channelId).toBe(channelId);
    expect(result.credentialsSnapshot.steps).toHaveLength(1);
    expect(result.credentialsSnapshot.steps[0]?.address).toBe(
      "run_deployment-x-step-1@example.com",
    );
    expect(mailBus.registered()).toContain("run_deployment-x@example.com");
    expect(supervisor.getCredentialsSnapshot()).not.toBeNull();

    // The first buffered mail fires the stable top-level run. The FIFO pipeline holds the second
    // until that run terminates, then rejects it rather than issuing a second trigger.fire.
    const firstFired = await waitForTriggerFireRunIds(supervisorToChild, 1);
    expect(firstFired.length).toBeGreaterThanOrEqual(1);
    const firstRunId = firstFired[0];
    if (firstRunId === undefined) throw new Error("first runId missing");
    await childSender.send({
      type: "terminal.event",
      data: {
        runId: firstRunId,
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });
    await inbox.awaitState(
      () => inbox.snapshot("run_deployment-x@example.com").consumed.size >= 2,
    );
    expect(parseTriggerFireRunIds(supervisorToChild.flushed())).toEqual([
      firstRunId,
    ]);

    await supervisor.shutdown();
    expect(killed).toBe(true);
    expect(mailBus.registered()).not.toContain("run_deployment-x@example.com");
  });

  // Spawn a supervisor against a synthetic child and return the pieces a per-run barrier test
  // needs (child sender, supervisor-to-child stream, mail bus, inbox primitives); onRunStart is
  // threaded so the dispatch loop runs the barrier.
  async function spawnWithRunStart(opts: {
    baseDir: string;
    onRunStart?: WorkflowSupervisorBindings["onRunStart"];
    credentialDelivery?: WorkflowSupervisorBindings["credentialDelivery"];
    drainTimeoutAccumulatorFactory?: DrainTimeoutAccumulatorFactory;
    onWrite?: (args: {
      principal: { kind: string };
      repoId: RepoId;
      ref: string;
      preservePrefix: string;
      message: string;
      files: Record<string, string | Uint8Array>;
    }) => void;
    beforeWrite?: (args: {
      preservePrefix: string;
      message: string;
    }) => void | Promise<void>;
    inboxPrimitives?: MemoryInboxPrimitives;
    mailBus?: ReturnType<typeof createMockMailBus>;
    statefulWrites?: boolean;
    failControlWrite?: () => boolean;
  }) {
    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });

    let observedEnv: Record<string, string> | undefined;
    // Scoped here, not to the file: `first()` must resolve with THIS fixture's spawn, not whichever spawn happened earliest in the run.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      spawnObserver.record(env);
      return {
        pid: 7777,
        controlWriter:
          opts.failControlWrite === undefined
            ? supervisorToChild.writer
            : {
                write(line) {
                  if (opts.failControlWrite?.())
                    throw new Error("Child control channel closed");
                  return supervisorToChild.writer.write(line);
                },
              },
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
    };

    const mailBus = opts.mailBus ?? createMockMailBus();
    const inboxPrimitives =
      opts.inboxPrimitives ?? createMemoryInboxPrimitives();
    const baseBindings = await buildBindings({
      baseDir: opts.baseDir,
      spawner,
      signSpy: () => ({ sig: new Uint8Array(64), principalKind: "supervisor" }),
      mailBus,
      inboxPrimitives,
      ...(opts.onWrite !== undefined ? { onWrite: opts.onWrite } : {}),
      ...(opts.beforeWrite !== undefined
        ? { beforeWrite: opts.beforeWrite }
        : {}),
      ...(opts.statefulWrites === true ? { statefulWrites: true } : {}),
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
      ...(opts.onRunStart !== undefined ? { onRunStart: opts.onRunStart } : {}),
      ...(opts.credentialDelivery !== undefined
        ? { credentialDelivery: opts.credentialDelivery }
        : {}),
      ...(opts.drainTimeoutAccumulatorFactory !== undefined
        ? {
            drainTimeoutAccumulatorFactory: opts.drainTimeoutAccumulatorFactory,
          }
        : {}),
    };
    const supervisor = supervisors.track(createWorkflowSupervisor(bindings));
    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-barrier",
      warmKeep: false,
      onInferenceEvent: () => undefined,
    });
    observedEnv = await spawnObserver.first();
    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeyPair.privateKey,
      channelId,
      writer: {
        write(line: string) {
          childToSupervisor.inject(line);
        },
      },
    });
    await mailBus.awaitRegistered("run_deployment-x@example.com");
    await childSender.send({
      type: "ready",
      data: {
        childPid: 7777,
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
    await spawnPromise;
    return {
      supervisor,
      childSender,
      supervisorToChild,
      mailBus,
      inboxPrimitives,
    };
  }

  const MAILBOX_ADDRESS = "run_deployment-x@example.com";

  test("delivering inbound mail eager-commits an INBOX entry and emits mailbox.notify", async () => {
    const baseDir = await makeTempDir("supervisor-mailbox-commit-");
    const writes: CapturedWrite[] = [];
    // The recorded write is the event these tests wait on: the mailbox index they assert against only changes when one lands.
    const writeChanges = createChangeNotifier();
    const wired = await spawnWithRunStart({
      baseDir,
      statefulWrites: true,
      onWrite: (args) => {
        writes.push({ preservePrefix: args.preservePrefix, files: args.files });
        writeChanges.notify();
      },
    });

    const raw = buildInboundMail({
      from: "sender@example.com",
      to: MAILBOX_ADDRESS,
      subject: "hello",
      messageId: "<msg-1@example.com>",
      body: "first turn",
    });
    wired.mailBus.deliver(MAILBOX_ADDRESS, raw);

    await waitForUpstreamPayloads(wired.supervisorToChild, "mailbox.notify", 1);
    const notifies = parseMailboxNotifies(wired.supervisorToChild.flushed());
    expect(notifies).toHaveLength(1);
    const notify = notifies[0];
    if (notify === undefined) throw new Error("mailbox.notify missing");
    expect(notify.mailbox).toBe("INBOX");
    expect(notify.uid).toBe(1);
    expect(notify.headers.from).toBe("sender@example.com");
    expect(notify.headers.to).toEqual([MAILBOX_ADDRESS]);
    expect(notify.headers.messageId).toBe("<msg-1@example.com>");

    // The arrival commit holds exactly one entry with the assigned uid, unflagged on arrival; the raw bytes are stored verbatim in <uid>.eml.
    const arrival = mailboxIndexes(writes)[0];
    if (arrival === undefined) throw new Error("no mailbox index committed");
    expect(arrival.messages).toHaveLength(1);
    expect(arrival.messages[0]?.uid).toBe(1);
    expect(arrival.messages[0]?.flags).toEqual([]);
    // The <uid>.eml is put once, by the arrival flush that appended it; the later flag-mark flush
    // is a delta touching only index.json, so the blob is committed in the arrival write, not
    // necessarily the last.
    const emlWrite = writes.find(
      (w) =>
        w.preservePrefix === "mailbox/INBOX/" &&
        w.files["mailbox/INBOX/1.eml"] !== undefined,
    );
    const eml = emlWrite?.files["mailbox/INBOX/1.eml"];
    if (eml === undefined) throw new Error("no <uid>.eml committed");
    const emlBytes =
      typeof eml === "string" ? new TextEncoder().encode(eml) : eml;
    expect(emlBytes).toEqual(raw);

    // Regression: the step-input trigger.fire is unchanged -- still a Mail payload, and the notify's runId matches the run the trigger fired.
    await waitForUpstreamPayloads(wired.supervisorToChild, "trigger.fire", 1);
    const fireIds = parseTriggerFireRunIds(wired.supervisorToChild.flushed());
    expect(fireIds).toHaveLength(1);
    const firstFireId = fireIds[0];
    if (firstFireId === undefined)
      throw new Error("first trigger runId missing");
    expect(notify.runId).toBe(firstFireId);
    const payloads = parseTriggerFirePayloads(
      wired.supervisorToChild.flushed(),
    );
    expect(payloads).toHaveLength(1);
    expect(isMail(payloads[0])).toBe(true);

    await wired.supervisor.shutdown();
  });

  test("eager mailbox commit is decoupled from FIFO trigger dispatch", async () => {
    const baseDir = await makeTempDir("supervisor-mailbox-fifo-");
    const wired = await spawnWithRunStart({ baseDir, statefulWrites: true });

    const m1 = buildInboundMail({
      from: "a@example.com",
      to: MAILBOX_ADDRESS,
      subject: "one",
      messageId: "<m1@example.com>",
      body: "one",
    });
    const m2 = buildInboundMail({
      from: "b@example.com",
      to: MAILBOX_ADDRESS,
      subject: "two",
      messageId: "<m2@example.com>",
      body: "two",
    });

    // Deliver sequentially so the arrival order (and the assigned uids) is deterministic; confirm each arrival eager-committed on its own.
    wired.mailBus.deliver(MAILBOX_ADDRESS, m1);
    await waitForUpstreamPayloads(wired.supervisorToChild, "mailbox.notify", 1);
    let notifies = parseMailboxNotifies(wired.supervisorToChild.flushed());
    wired.mailBus.deliver(MAILBOX_ADDRESS, m2);
    await waitForUpstreamPayloads(wired.supervisorToChild, "mailbox.notify", 2);
    notifies = parseMailboxNotifies(wired.supervisorToChild.flushed());
    // Both messages committed with fresh, monotonic uids even though FIFO dispatch holds the second until the first run terminates.
    expect(notifies.map((n) => n.uid)).toEqual([1, 2]);
    expect(notifies.map((n) => n.headers.messageId)).toEqual([
      "<m1@example.com>",
      "<m2@example.com>",
    ]);

    // FIFO dispatch is unchanged: exactly one trigger.fire; the second message waits for
    // termination and is rejected. Wait for the first fire before counting -- the notify waits
    // return on commit, before dispatch forwards anything.
    await waitForTriggerFireRunIds(wired.supervisorToChild, 1);
    const fireIds = parseTriggerFireRunIds(wired.supervisorToChild.flushed());
    expect(fireIds).toHaveLength(1);
    const runId = fireIds[0];
    if (runId === undefined) throw new Error("runId missing");
    await wired.childSender.send({
      type: "terminal.event",
      data: { runId, seq: 0, kind: "RunCompleted", at: "test" },
    });
    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(MAILBOX_ADDRESS).consumed.size >= 2,
    );
    expect(wired.inboxPrimitives.snapshot(MAILBOX_ADDRESS).consumed.size).toBe(
      2,
    );
    expect(parseTriggerFireRunIds(wired.supervisorToChild.flushed())).toEqual([
      runId,
    ]);

    await wired.supervisor.shutdown();
  });

  test("a dispatched message's mailbox entry is flagged Seen and Processed", async () => {
    const baseDir = await makeTempDir("supervisor-mailbox-flags-");
    const writes: CapturedWrite[] = [];
    // The recorded write is the event these tests wait on: the mailbox index they assert against only changes when one lands.
    const writeChanges = createChangeNotifier();
    const wired = await spawnWithRunStart({
      baseDir,
      statefulWrites: true,
      onWrite: (args) => {
        writes.push({ preservePrefix: args.preservePrefix, files: args.files });
        writeChanges.notify();
      },
    });

    const raw = buildInboundMail({
      from: "sender@example.com",
      to: MAILBOX_ADDRESS,
      subject: "flag me",
      messageId: "<flag-1@example.com>",
      body: "turn body",
    });
    wired.mailBus.deliver(MAILBOX_ADDRESS, raw);

    await waitForUpstreamPayloads(wired.supervisorToChild, "trigger.fire", 1);
    const fireIds = parseTriggerFireRunIds(wired.supervisorToChild.flushed());
    const runId = fireIds[0];
    if (runId === undefined) throw new Error("runId missing");
    await wired.childSender.send({
      type: "terminal.event",
      data: { runId, seq: 0, kind: "RunCompleted", at: "test" },
    });

    // The on-dispatch flag mark (fire-and-forget) flushes the \Seen/$Processed flags onto the entry; poll the committed index until they land.
    const latest = () => {
      const all = mailboxIndexes(writes);
      return all[all.length - 1];
    };
    await writeChanges.until(() =>
      (latest()?.messages[0]?.flags ?? []).includes("$Processed"),
    );
    const index = latest();
    if (index === undefined) throw new Error("no mailbox index committed");
    expect(index.messages).toHaveLength(1);
    const flags = index.messages[0]?.flags ?? [];
    expect(flags).toContain("\\Seen");
    expect(flags).toContain("$Processed");

    await wired.supervisor.shutdown();
  });

  test("a child mailbox mutation flags and expunges the owned INBOX", async () => {
    const baseDir = await makeTempDir("supervisor-mailbox-mutate-");
    const writes: CapturedWrite[] = [];
    // The recorded write is the event these tests wait on: the mailbox index they assert against only changes when one lands.
    const writeChanges = createChangeNotifier();
    const wired = await spawnWithRunStart({
      baseDir,
      statefulWrites: true,
      onWrite: (args) => {
        writes.push({ preservePrefix: args.preservePrefix, files: args.files });
        writeChanges.notify();
      },
    });

    // Seed the mailbox: deliver a message so the eager commit assigns uid 1.
    const raw = buildInboundMail({
      from: "sender@example.com",
      to: MAILBOX_ADDRESS,
      subject: "consume me",
      messageId: "<mut-1@example.com>",
      body: "body",
    });
    wired.mailBus.deliver(MAILBOX_ADDRESS, raw);
    await waitForUpstreamPayloads(wired.supervisorToChild, "mailbox.notify", 1);
    const notifies = parseMailboxNotifies(wired.supervisorToChild.flushed());
    expect(notifies[0]?.uid).toBe(1);

    const awaitResponse = (requestId: string) =>
      awaitMutateResponse(wired.supervisorToChild, requestId);

    // A mutation targeting a mailbox the agent does not own is rejected: the supervisor owns only INBOX, never silently mutating another name.
    await wired.childSender.send({
      type: "mailbox.mutate.request",
      data: {
        requestId: "mm-bad-mailbox",
        runId: "run-x",
        mailbox: "Sent",
        op: "expunge",
      },
    });
    const badMailbox = await awaitResponse("mm-bad-mailbox");
    expect(badMailbox.result.ok).toBe(false);
    if (badMailbox.result.ok) throw new Error("expected rejection");
    expect(badMailbox.result.reason).toMatch(/only INBOX is writable/);
    expect(badMailbox.result.condition).toBe("NONEXISTENT");

    // A flag write on an unknown uid fails loudly rather than silently.
    await wired.childSender.send({
      type: "mailbox.mutate.request",
      data: {
        requestId: "mm-bad-uid",
        runId: "run-x",
        mailbox: "INBOX",
        op: "addFlags",
        uid: 999,
        flags: ["\\Deleted"],
      },
    });
    const badUid = await awaitResponse("mm-bad-uid");
    expect(badUid.result.ok).toBe(false);

    // Flag uid 1 \Deleted; the committed index carries the flag.
    await wired.childSender.send({
      type: "mailbox.mutate.request",
      data: {
        requestId: "mm-flag",
        runId: "run-x",
        mailbox: "INBOX",
        op: "addFlags",
        uid: 1,
        flags: ["\\Deleted"],
      },
    });
    const flagged = await awaitResponse("mm-flag");
    expect(flagged.result.ok).toBe(true);
    const latest = () => {
      const all = mailboxIndexes(writes);
      return all[all.length - 1];
    };
    await writeChanges.until(() =>
      (latest()?.messages[0]?.flags ?? []).includes("\\Deleted"),
    );
    expect(latest()?.messages[0]?.flags ?? []).toContain("\\Deleted");

    // Expunge sweeps every \Deleted message out of the live INBOX and reports the swept uids; the index persists (uidNext stays 2, never reused).
    await wired.childSender.send({
      type: "mailbox.mutate.request",
      data: {
        requestId: "mm-expunge",
        runId: "run-x",
        mailbox: "INBOX",
        op: "expunge",
      },
    });
    const expunged = await awaitResponse("mm-expunge");
    expect(expunged.result.ok).toBe(true);
    if (!expunged.result.ok) throw new Error("expected success");
    expect(expunged.result.expungedUids).toEqual([1]);
    await writeChanges.until(() => (latest()?.messages.length ?? 1) === 0);
    const finalIndex = latest();
    if (finalIndex === undefined) throw new Error("no mailbox index committed");
    expect(finalIndex.messages).toHaveLength(0);
    expect(finalIndex.uidNext).toBe(2);

    await wired.supervisor.shutdown();
  });

  test("a removeFlags mutation clears a flag from the owned INBOX entry", async () => {
    const baseDir = await makeTempDir("supervisor-mailbox-removeflags-");
    const writes: CapturedWrite[] = [];
    // The recorded write is the event these tests wait on: the mailbox index they assert against only changes when one lands.
    const writeChanges = createChangeNotifier();
    const wired = await spawnWithRunStart({
      baseDir,
      statefulWrites: true,
      onWrite: (args) => {
        writes.push({ preservePrefix: args.preservePrefix, files: args.files });
        writeChanges.notify();
      },
    });

    wired.mailBus.deliver(
      MAILBOX_ADDRESS,
      buildInboundMail({
        from: "sender@example.com",
        to: MAILBOX_ADDRESS,
        subject: "flag me",
        messageId: "<rmf-1@example.com>",
        body: "body",
      }),
    );
    await waitForUpstreamPayloads(wired.supervisorToChild, "mailbox.notify", 1);
    const notifies = parseMailboxNotifies(wired.supervisorToChild.flushed());
    expect(notifies[0]?.uid).toBe(1);

    const latest = () => {
      const all = mailboxIndexes(writes);
      return all[all.length - 1];
    };
    const flagsNow = () => latest()?.messages[0]?.flags ?? [];

    // Add a custom flag, then clear it -- the removeFlags branch.
    await wired.childSender.send({
      type: "mailbox.mutate.request",
      data: {
        requestId: "rmf-add",
        runId: "run-x",
        mailbox: "INBOX",
        op: "addFlags",
        uid: 1,
        flags: ["\\Flagged"],
      },
    });
    expect(
      (await awaitMutateResponse(wired.supervisorToChild, "rmf-add")).result.ok,
    ).toBe(true);
    await writeChanges.until(() => flagsNow().includes("\\Flagged"));
    expect(flagsNow()).toContain("\\Flagged");

    await wired.childSender.send({
      type: "mailbox.mutate.request",
      data: {
        requestId: "rmf-clear",
        runId: "run-x",
        mailbox: "INBOX",
        op: "removeFlags",
        uid: 1,
        flags: ["\\Flagged"],
      },
    });
    expect(
      (await awaitMutateResponse(wired.supervisorToChild, "rmf-clear")).result
        .ok,
    ).toBe(true);
    await writeChanges.until(() => !flagsNow().includes("\\Flagged"));
    expect(flagsNow()).not.toContain("\\Flagged");

    await wired.supervisor.shutdown();
  });

  test("an expunge sweeps every \\Deleted message and reports all their uids", async () => {
    const baseDir = await makeTempDir("supervisor-mailbox-multi-expunge-");
    const writes: CapturedWrite[] = [];
    // The recorded write is the event these tests wait on: the mailbox index they assert against only changes when one lands.
    const writeChanges = createChangeNotifier();
    const wired = await spawnWithRunStart({
      baseDir,
      statefulWrites: true,
      onWrite: (args) => {
        writes.push({ preservePrefix: args.preservePrefix, files: args.files });
        writeChanges.notify();
      },
    });

    // Deliver two messages so the eager-commit assigns uids 1 and 2.
    wired.mailBus.deliver(
      MAILBOX_ADDRESS,
      buildInboundMail({
        from: "a@example.com",
        to: MAILBOX_ADDRESS,
        subject: "one",
        messageId: "<mx-1@example.com>",
        body: "one",
      }),
    );
    await waitForUpstreamPayloads(wired.supervisorToChild, "mailbox.notify", 1);
    let notifies = parseMailboxNotifies(wired.supervisorToChild.flushed());
    wired.mailBus.deliver(
      MAILBOX_ADDRESS,
      buildInboundMail({
        from: "b@example.com",
        to: MAILBOX_ADDRESS,
        subject: "two",
        messageId: "<mx-2@example.com>",
        body: "two",
      }),
    );
    await waitForUpstreamPayloads(wired.supervisorToChild, "mailbox.notify", 2);
    notifies = parseMailboxNotifies(wired.supervisorToChild.flushed());
    expect(notifies.map((n) => n.uid)).toEqual([1, 2]);

    // Flag both \Deleted, then expunge: the sweep removes both and reports both.
    for (const uid of [1, 2]) {
      await wired.childSender.send({
        type: "mailbox.mutate.request",
        data: {
          requestId: `mx-flag-${String(uid)}`,
          runId: "run-x",
          mailbox: "INBOX",
          op: "addFlags",
          uid,
          flags: ["\\Deleted"],
        },
      });
      expect(
        (
          await awaitMutateResponse(
            wired.supervisorToChild,
            `mx-flag-${String(uid)}`,
          )
        ).result.ok,
      ).toBe(true);
    }

    await wired.childSender.send({
      type: "mailbox.mutate.request",
      data: {
        requestId: "mx-expunge",
        runId: "run-x",
        mailbox: "INBOX",
        op: "expunge",
      },
    });
    const expunged = await awaitMutateResponse(
      wired.supervisorToChild,
      "mx-expunge",
    );
    expect(expunged.result.ok).toBe(true);
    if (!expunged.result.ok) throw new Error("expected success");
    expect(expunged.result.expungedUids).toEqual([1, 2]);

    const latest = () => {
      const all = mailboxIndexes(writes);
      return all[all.length - 1];
    };
    await writeChanges.until(() => (latest()?.messages.length ?? 2) === 0);
    expect(latest()?.messages ?? []).toHaveLength(0);

    await wired.supervisor.shutdown();
  });

  test("an expunge with no \\Deleted message is a no-op that reports no uids", async () => {
    const baseDir = await makeTempDir("supervisor-mailbox-empty-expunge-");
    const writes: CapturedWrite[] = [];
    // The recorded write is the event these tests wait on: the mailbox index they assert against only changes when one lands.
    const writeChanges = createChangeNotifier();
    const wired = await spawnWithRunStart({
      baseDir,
      statefulWrites: true,
      onWrite: (args) => {
        writes.push({ preservePrefix: args.preservePrefix, files: args.files });
        writeChanges.notify();
      },
    });

    wired.mailBus.deliver(
      MAILBOX_ADDRESS,
      buildInboundMail({
        from: "sender@example.com",
        to: MAILBOX_ADDRESS,
        subject: "survivor",
        messageId: "<ee-1@example.com>",
        body: "body",
      }),
    );
    await waitForUpstreamPayloads(wired.supervisorToChild, "mailbox.notify", 1);
    const notifies = parseMailboxNotifies(wired.supervisorToChild.flushed());
    expect(notifies[0]?.uid).toBe(1);

    // Nothing is flagged \Deleted, so the sweep removes nothing and the message survives. The dispatch marks it \Seen/$Processed, never \Deleted.
    await wired.childSender.send({
      type: "mailbox.mutate.request",
      data: {
        requestId: "ee-expunge",
        runId: "run-x",
        mailbox: "INBOX",
        op: "expunge",
      },
    });
    const expunged = await awaitMutateResponse(
      wired.supervisorToChild,
      "ee-expunge",
    );
    expect(expunged.result.ok).toBe(true);
    if (!expunged.result.ok) throw new Error("expected success");
    expect(expunged.result.expungedUids).toEqual([]);

    const all = mailboxIndexes(writes);
    const latest = all[all.length - 1];
    expect(latest?.messages.map((m) => m.uid)).toEqual([1]);

    await wired.supervisor.shutdown();
  });

  test("the supervisor answers mailbox reads, part bytes, and refusals", async () => {
    const baseDir = await makeTempDir("supervisor-mailbox-call-");
    const wired = await spawnWithRunStart({
      baseDir,
      statefulWrites: true,
    });
    const body = "see attached";
    const attachment = "hello-attach";
    const respond = (requestId: string) =>
      awaitCallResponse(wired.supervisorToChild, requestId);

    await wired.childSender.send({
      type: "mailbox.call.request",
      data: { requestId: "mc-list", runId: "run-x", op: "listMailboxes" },
    });
    const listed = await respond("mc-list");
    expect(listed.ok).toBe(true);
    if (!listed.ok || listed.op !== "listMailboxes") {
      throw new Error("expected listMailboxes");
    }
    expect(listed.value).toEqual([{ name: "INBOX" }]);

    await wired.childSender.send({
      type: "mailbox.call.request",
      data: {
        requestId: "mc-create",
        runId: "run-x",
        op: "createMailbox",
        name: "Sent",
      },
    });
    const created = await respond("mc-create");
    expect(created.ok).toBe(false);
    if (created.ok) throw new Error("expected refusal");
    expect(created.condition).toBe("CANNOT");
    expect(created.op).toBe("createMailbox");

    await wired.childSender.send({
      type: "mailbox.call.request",
      data: {
        requestId: "mc-move",
        runId: "run-x",
        op: "move",
        ref: { uid: 1, mailbox: "INBOX" },
        toMailbox: "INBOX",
      },
    });
    const moved = await respond("mc-move");
    expect(moved.ok).toBe(false);
    if (moved.ok) throw new Error("expected refusal");
    expect(moved.condition).toBe("CANNOT");
    expect(moved.reason).toBe('Cannot move/copy a message within "INBOX"');

    await wired.childSender.send({
      type: "mailbox.call.request",
      data: {
        requestId: "mc-missing",
        runId: "run-x",
        op: "search",
        mailbox: "Drafts",
        query: {},
      },
    });
    const missing = await respond("mc-missing");
    expect(missing.ok).toBe(false);
    if (missing.ok) throw new Error("expected refusal");
    expect(missing.condition).toBe("NONEXISTENT");
    expect(missing.op).toBe("search");

    await wired.childSender.send({
      type: "mailbox.call.request",
      data: {
        requestId: "mc-watch",
        runId: "run-x",
        op: "watch",
        mailbox: "INBOX",
      },
    });
    const watched = await respond("mc-watch");
    expect(watched.ok).toBe(true);
    if (!watched.ok || watched.op !== "watch") {
      throw new Error("expected watch");
    }
    expect("value" in watched).toBe(false);

    await wired.childSender.send({
      type: "mailbox.call.request",
      data: {
        requestId: "mc-baddate",
        runId: "run-x",
        op: "search",
        mailbox: "INBOX",
        query: { on: "not-a-date" },
      },
    });
    const badDate = await respond("mc-baddate");
    expect(badDate.ok).toBe(false);
    if (badDate.ok) throw new Error("expected a failed call");
    expect(badDate.condition).toBeUndefined();
    expect(badDate.reason).toContain("not-a-date");

    wired.mailBus.deliver(
      MAILBOX_ADDRESS,
      buildSignedConversation({
        from: "sender@example.com",
        to: MAILBOX_ADDRESS,
        subject: "consume me",
        messageId: "<part-1@example.com>",
        body,
        attachment: { filename: "note.txt", body: attachment },
      }),
    );
    await waitForUpstreamPayloads(wired.supervisorToChild, "mailbox.notify", 1);

    await wired.childSender.send({
      type: "mailbox.call.request",
      data: {
        requestId: "mc-search",
        runId: "run-x",
        op: "search",
        mailbox: "INBOX",
        query: {
          and: [
            { from: "sender@example.com" },
            { on: "2030-01-01T00:00:00.000Z" },
          ],
        },
      },
    });
    const found = await respond("mc-search");
    expect(found.ok).toBe(true);
    if (!found.ok || found.op !== "search") throw new Error("expected search");
    expect(found.value).toEqual([{ uid: 1, mailbox: "INBOX" }]);

    await wired.childSender.send({
      type: "mailbox.call.request",
      data: {
        requestId: "mc-part",
        runId: "run-x",
        op: "fetchPart",
        ref: { uid: 1, mailbox: "INBOX" },
        partPath: "1.2",
      },
    });
    const part = await respond("mc-part");
    expect(part.ok).toBe(true);
    if (!part.ok || part.op !== "fetchPart") {
      throw new Error("expected fetchPart");
    }
    expect(part.value.contentType).toBe("text/plain");
    expect(part.value.encoding).toBeUndefined();
    expect(
      new TextDecoder().decode(base64Decode(part.value.contentBase64)),
    ).toBe(attachment);

    await wired.childSender.send({
      type: "mailbox.call.request",
      data: {
        requestId: "mc-full",
        runId: "run-x",
        op: "fetchFull",
        ref: { uid: 1, mailbox: "INBOX" },
      },
    });
    const full = await respond("mc-full");
    expect(full.ok).toBe(true);
    if (!full.ok || full.op !== "fetchFull") {
      throw new Error("expected fetchFull");
    }
    expect(full.value.signatureStatus).toBe("unknown");
    expect(full.value.content).toBe(body);
    expect(full.value.attachments).toEqual([
      {
        name: "note.txt",
        contentType: "text/plain",
        dataBase64: base64Encode(new TextEncoder().encode(attachment)),
        part: "1.2",
      },
    ]);

    await waitForUpstreamPayloads(wired.supervisorToChild, "trigger.fire", 1);
    const payloads = parseTriggerFirePayloads(
      wired.supervisorToChild.flushed(),
    );
    const mail = payloads[0];
    if (!isMail(mail)) throw new Error("trigger payload is not mail");
    const textPart = mail.parts.find((candidate) => candidate.text === body);
    if (textPart === undefined) throw new Error("text part missing");

    await wired.childSender.send({
      type: "mailbox.call.request",
      data: {
        requestId: "mc-bytes",
        runId: "run-x",
        op: "readMailPart",
        partRef: textPart.ref,
      },
    });
    const bytes = await respond("mc-bytes");
    expect(bytes.ok).toBe(true);
    if (!bytes.ok || bytes.op !== "readMailPart") {
      throw new Error("expected readMailPart");
    }
    expect(
      new TextDecoder().decode(base64Decode(bytes.value.contentBase64)),
    ).toBe(body);

    await wired.childSender.send({
      type: "mailbox.call.request",
      data: {
        requestId: "mc-badref",
        runId: "run-x",
        op: "readMailPart",
        partRef: "not-a-ref",
      },
    });
    const badRef = await respond("mc-badref");
    expect(badRef.ok).toBe(false);
    if (badRef.ok) throw new Error("expected a failed part read");
    expect(badRef.condition).toBeUndefined();

    await wired.supervisor.shutdown();
  });

  test("a mailbox sync reports a new uid and a flag change as arrays", async () => {
    const baseDir = await makeTempDir("supervisor-mailbox-sync-");
    const writes: CapturedWrite[] = [];
    const writeChanges = createChangeNotifier();
    const wired = await spawnWithRunStart({
      baseDir,
      statefulWrites: true,
      onWrite: (args) => {
        writes.push({ preservePrefix: args.preservePrefix, files: args.files });
        writeChanges.notify();
      },
    });
    const respond = (requestId: string) =>
      awaitCallResponse(wired.supervisorToChild, requestId);

    wired.mailBus.deliver(
      MAILBOX_ADDRESS,
      buildInboundMail({
        from: "sender@example.com",
        to: MAILBOX_ADDRESS,
        subject: "one",
        messageId: "<sync-1@example.com>",
        body: "one",
      }),
    );
    await waitForUpstreamPayloads(wired.supervisorToChild, "mailbox.notify", 1);
    const latest = () => {
      const all = mailboxIndexes(writes);
      return all[all.length - 1];
    };
    await writeChanges.until(() =>
      (latest()?.messages[0]?.flags ?? []).includes("$Processed"),
    );

    await wired.childSender.send({
      type: "mailbox.call.request",
      data: {
        requestId: "mc-status",
        runId: "run-x",
        op: "getMailboxStatus",
        mailbox: "INBOX",
      },
    });
    const status = await respond("mc-status");
    expect(status.ok).toBe(true);
    if (!status.ok || status.op !== "getMailboxStatus") {
      throw new Error("expected status");
    }
    expect(status.value.total).toBe(1);
    expect(status.value.unseen).toBe(0);
    expect(status.value.uidNext).toBe(2);

    await wired.childSender.send({
      type: "mailbox.mutate.request",
      data: {
        requestId: "mc-flag",
        runId: "run-x",
        mailbox: "INBOX",
        op: "addFlags",
        uid: 1,
        flags: ["\\Flagged"],
      },
    });
    expect(
      (await awaitMutateResponse(wired.supervisorToChild, "mc-flag")).result.ok,
    ).toBe(true);

    wired.mailBus.deliver(
      MAILBOX_ADDRESS,
      buildInboundMail({
        from: "sender@example.com",
        to: MAILBOX_ADDRESS,
        subject: "two",
        messageId: "<sync-2@example.com>",
        body: "two",
      }),
    );
    await waitForUpstreamPayloads(wired.supervisorToChild, "mailbox.notify", 2);

    await wired.childSender.send({
      type: "mailbox.call.request",
      data: {
        requestId: "mc-sync",
        runId: "run-x",
        op: "sync",
        mailbox: "INBOX",
        uidNext: status.value.uidNext,
        uidValidity: status.value.uidValidity,
        highestModSeq: status.value.highestModSeq,
      },
    });
    const synced = await respond("mc-sync");
    expect(synced.ok).toBe(true);
    if (!synced.ok || synced.op !== "sync") throw new Error("expected sync");
    expect(synced.value.fullResyncRequired).toBe(false);
    expect(synced.value.vanished).toEqual([]);
    expect(synced.value.newMessages).toEqual([{ uid: 2, mailbox: "INBOX" }]);
    expect(synced.value.changed).toEqual([
      { uid: 1, flags: ["\\Seen", "$Processed", "\\Flagged"] },
    ]);

    await wired.supervisor.shutdown();
  });

  test("an append with an unparseable date does not wedge the mailbox", async () => {
    const baseDir = await makeTempDir("supervisor-mailbox-append-date-");
    const wired = await spawnWithRunStart({
      baseDir,
      statefulWrites: true,
    });
    const respond = (requestId: string) =>
      awaitCallResponse(wired.supervisorToChild, requestId);

    await wired.childSender.send({
      type: "mailbox.call.request",
      data: {
        requestId: "mc-bad-append",
        runId: "run-x",
        op: "append",
        mailbox: "INBOX",
        headers: {
          to: [MAILBOX_ADDRESS],
          messageId: "<bad-date@example.com>",
          date: "not-a-date",
        },
      },
    });
    const rejected = await respond("mc-bad-append");
    expect(rejected.ok).toBe(false);
    if (rejected.ok) throw new Error("expected a failed append");
    expect(rejected.condition).toBeUndefined();
    expect(rejected.reason).toContain("not-a-date");

    await wired.childSender.send({
      type: "mailbox.call.request",
      data: {
        requestId: "mc-good-append",
        runId: "run-x",
        op: "append",
        mailbox: "INBOX",
        headers: {
          from: "sender@example.com",
          to: [MAILBOX_ADDRESS],
          messageId: "<good-date@example.com>",
          date: "Tue, 01 Jan 2030 00:00:00 +0000",
          subject: "kept",
        },
        content: "still here",
      },
    });
    const appended = await respond("mc-good-append");
    expect(appended.ok).toBe(true);
    if (!appended.ok || appended.op !== "append") {
      throw new Error("expected append");
    }
    expect(appended.value).toEqual({ uid: 1, mailbox: "INBOX" });

    await wired.childSender.send({
      type: "mailbox.call.request",
      data: {
        requestId: "mc-headers",
        runId: "run-x",
        op: "fetchHeaders",
        ref: { uid: 1, mailbox: "INBOX" },
      },
    });
    const headers = await respond("mc-headers");
    expect(headers.ok).toBe(true);
    if (!headers.ok || headers.op !== "fetchHeaders") {
      throw new Error("expected fetchHeaders");
    }
    expect(headers.value.messageId).toBe("<good-date@example.com>");

    await wired.supervisor.shutdown();
  });

  const PARENT_ID = "<parent@example.com>";
  const PARENT_REFERENCES = [
    "<root@example.com>",
    "not-an-id",
    "<mid@example.com>",
  ];

  async function recordOutbound(opts: {
    label: string;
    sends: readonly {
      inReplyTo?: string;
      references?: readonly string[];
      completeReferences?: boolean;
    }[];
  }): Promise<OutboundMessage[]> {
    const baseDir = await makeTempDir(opts.label);
    const sent: OutboundMessage[] = [];
    const mailBus = createMockMailBus();
    mailBus.sendOutbound = (_sender, message) => {
      sent.push(message);
      return Promise.resolve({
        messageId: "<outbound@example.com>",
        status: "delivered",
      });
    };
    const wired = await spawnWithRunStart({
      baseDir,
      statefulWrites: true,
      mailBus,
    });
    wired.mailBus.deliver(
      MAILBOX_ADDRESS,
      buildInboundMail({
        from: "sender@example.com",
        to: MAILBOX_ADDRESS,
        subject: "parent",
        messageId: PARENT_ID,
        body: "parent body",
        references: PARENT_REFERENCES,
      }),
    );
    await waitForUpstreamPayloads(wired.supervisorToChild, "mailbox.notify", 1);
    for (const [index, send] of opts.sends.entries()) {
      const requestId = `om-refs-${String(index)}`;
      await wired.childSender.send({
        type: "outbound.message",
        data: {
          requestId,
          senderAddress: MAILBOX_ADDRESS,
          ...(send.completeReferences === true
            ? { completeReferences: true }
            : {}),
          message: {
            to: "recipient@example.com",
            type: "conversation.message",
            content: "reply",
            ...(send.inReplyTo !== undefined
              ? { inReplyTo: send.inReplyTo }
              : {}),
            ...(send.references !== undefined
              ? { references: [...send.references] }
              : {}),
          },
        },
      });
      const result = await waitForUpstreamPayload(
        wired.supervisorToChild,
        "outbound.result",
        (payload) => payload.data.requestId === requestId,
      );
      if (!result.data.result.ok) {
        throw new Error(`outbound send failed: ${result.data.result.reason}`);
      }
    }
    await wired.supervisor.shutdown();
    return sent;
  }

  test("a connector reply takes its References chain from the committed parent", async () => {
    const sent = await recordOutbound({
      label: "supervisor-reply-references-",
      sends: [
        {
          inReplyTo: PARENT_ID,
          completeReferences: true,
        },
      ],
    });
    expect(sent[0]?.references).toEqual([...PARENT_REFERENCES, PARENT_ID]);
    expect(sent[0]?.inReplyTo).toBe(PARENT_ID);
  });

  test("a connector reply whose parent is absent keeps references unset", async () => {
    const sent = await recordOutbound({
      label: "supervisor-reply-references-miss-",
      sends: [
        {
          inReplyTo: "<missing@example.com>",
          completeReferences: true,
        },
      ],
    });
    expect(sent[0]?.inReplyTo).toBe("<missing@example.com>");
    expect(sent[0]?.references).toBeUndefined();
  });

  test("a connector reply that already names references keeps that chain", async () => {
    const sent = await recordOutbound({
      label: "supervisor-reply-references-kept-",
      sends: [
        {
          inReplyTo: PARENT_ID,
          references: [],
          completeReferences: true,
        },
        {
          inReplyTo: PARENT_ID,
          references: ["<kept@example.com>"],
          completeReferences: true,
        },
      ],
    });
    expect(sent[0]?.references).toEqual([]);
    expect(sent[1]?.references).toEqual(["<kept@example.com>"]);
  });

  test("a send without completeReferences leaves a bare inReplyTo alone", async () => {
    const sent = await recordOutbound({
      label: "supervisor-reply-references-unmarked-",
      sends: [{ inReplyTo: PARENT_ID }],
    });
    expect(sent[0]?.inReplyTo).toBe(PARENT_ID);
    expect(sent[0]?.references).toBeUndefined();
  });

  test("dispatch pushes a per-run grants-updated before the run's trigger.fire", async () => {
    const baseDir = await makeTempDir("supervisor-barrier-ok-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    // The per-run sink reads grants per run; with onRunStart wired, any grants-updated frame can only come from this barrier.
    const runStartCalls: { runId: string; anchorRunId: string }[] = [];
    const onRunStart: WorkflowSupervisorBindings["onRunStart"] = async (
      args,
    ) => {
      runStartCalls.push(args);
      return assembleCredentialsSnapshot({
        repoStore: createStubRepoStore({ baseDir }),
        principal: { kind: "supervisor" },
        stepOrder: ["step-1"],
        anchorRunId: "run_deployment-x",
        deriveStepAddress: ({ runId, stepId }) =>
          `${runId}-${stepId}@example.com`,
      });
    };

    const wired = await spawnWithRunStart({ baseDir, onRunStart });

    // No grants-updated is pushed at spawn time when onRunStart is wired.
    expect(
      parseControlFrameTypes(wired.supervisorToChild.flushed()),
    ).not.toContain("grants-updated");

    wired.mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("barrier-m1"),
    );

    const [firedRunId] = await waitForTriggerFireRunIds(
      wired.supervisorToChild,
      1,
    );
    if (firedRunId === undefined) {
      throw new Error("trigger.fire frame carried no runId");
    }

    // The barrier is load-bearing on ordering: the run's grants-updated must appear on the child stream STRICTLY before its trigger.fire.
    const frameTypes = parseControlFrameTypes(
      wired.supervisorToChild.flushed(),
    );
    const grantsIdx = frameTypes.indexOf("grants-updated");
    const triggerIdx = frameTypes.indexOf("trigger.fire");
    expect(grantsIdx).toBeGreaterThanOrEqual(0);
    expect(triggerIdx).toBeGreaterThanOrEqual(0);
    expect(grantsIdx).toBeLessThan(triggerIdx);

    // The sink was consulted once for this run with the supervisor's deployment id stamped on.
    expect(runStartCalls).toEqual([
      { runId: firedRunId, anchorRunId: "run_deployment-x" },
    ]);

    await wired.childSender.send({
      type: "terminal.event",
      data: { runId: firedRunId, seq: 0, kind: "RunCompleted", at: "test" },
    });
    await wired.supervisor.shutdown();
  });

  test("deliverSignal refreshes grants immediately before the signal frame", async () => {
    const baseDir = await makeTempDir("supervisor-signal-grants-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const onRunStart: WorkflowSupervisorBindings["onRunStart"] = async () =>
      assembleCredentialsSnapshot({
        repoStore: createStubRepoStore({ baseDir }),
        principal: { kind: "supervisor" },
        stepOrder: ["step-1"],
        anchorRunId: "run_deployment-x",
        deriveStepAddress: ({ runId, stepId }) =>
          `${runId}-${stepId}@example.com`,
      });
    const wired = await spawnWithRunStart({ baseDir, onRunStart });

    // A standing approval's lowered floor must reach the child on the same FIFO before the resume signal, so the resumed run's calls observe it.
    await wired.supervisor.deliverSignal({
      runId: "run_deployment-x",
      signalName: "__signal__:corr-1",
      signalId: "sig_1",
      payload: { outcome: "approved" },
    });

    const frameTypes = parseControlFrameTypes(
      wired.supervisorToChild.flushed(),
    );
    const signalIdx = frameTypes.lastIndexOf("signal.deliver");
    expect(signalIdx).toBeGreaterThanOrEqual(0);
    expect(frameTypes[signalIdx - 1]).toBe("grants-updated");

    await wired.supervisor.shutdown();
  });

  test("deliverGrants skips without throwing when the child is not live", async () => {
    const baseDir = await makeTempDir("supervisor-deliver-grants-skip-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const onRunStart: WorkflowSupervisorBindings["onRunStart"] = async () =>
      assembleCredentialsSnapshot({
        repoStore: createStubRepoStore({ baseDir }),
        principal: { kind: "supervisor" },
        stepOrder: ["step-1"],
        anchorRunId: "run_deployment-x",
        deriveStepAddress: ({ runId, stepId }) =>
          `${runId}-${stepId}@example.com`,
      });
    const wired = await spawnWithRunStart({ baseDir, onRunStart });
    await wired.supervisor.shutdown();

    // After shutdown the child is gone. A mid-run grants refresh for a non-live run is normal
    // (the durable file governs the next barrier), so deliverGrants no-ops rather than throwing.
    const result = await wired.supervisor.deliverGrants("run_deployment-x");
    expect(result).toBe("skipped");
  });

  test("the barrier pushes credentials-updated before trigger.fire when the deployment has credentials", async () => {
    const baseDir = await makeTempDir("supervisor-barrier-creds-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const onRunStart: WorkflowSupervisorBindings["onRunStart"] = async () =>
      assembleCredentialsSnapshot({
        repoStore: createStubRepoStore({ baseDir }),
        principal: { kind: "supervisor" },
        stepOrder: ["step-1"],
        anchorRunId: "run_deployment-x",
        deriveStepAddress: ({ runId, stepId }) =>
          `${runId}-${stepId}@example.com`,
      });
    const delivery = {
      bindings: [
        { handle: "gh", credentialId: "cred_a", consumer: "tool:@acme/tools" },
      ],
      materials: [
        {
          credentialId: "cred_a",
          providerKey: "http",
          origin: "https://api.example.test",
          secret: "sk-real",
        },
      ],
    };

    const wired = await spawnWithRunStart({
      baseDir,
      onRunStart,
      credentialDelivery: delivery,
    });

    wired.mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("barrier-creds-m1"),
    );

    await waitForTriggerFireRunIds(wired.supervisorToChild, 1);

    // The material lands on the child's control stream STRICTLY before the trigger, so a first-step credential resolve already has it in its cell.
    const frameTypes = parseControlFrameTypes(
      wired.supervisorToChild.flushed(),
    );
    const credsIdx = frameTypes.indexOf("credentials-updated");
    const triggerIdx = frameTypes.indexOf("trigger.fire");
    expect(credsIdx).toBeGreaterThanOrEqual(0);
    expect(triggerIdx).toBeGreaterThanOrEqual(0);
    expect(credsIdx).toBeLessThan(triggerIdx);

    // And the delivered material is the deployment's, verbatim.
    const deliveries = parseCredentialsUpdatedFrames(
      wired.supervisorToChild.flushed(),
    );
    expect(deliveries).toContainEqual(delivery);

    await wired.supervisor.shutdown();
  });

  test("a mid-run revoke stays evicted across the pre-trigger barrier", async () => {
    // The barrier re-asserts the live (post-revoke) mirror, not the frozen deploy set, so the eviction stays.
    const baseDir = await makeTempDir("supervisor-revoke-durable-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const onRunStart: WorkflowSupervisorBindings["onRunStart"] = async () =>
      assembleCredentialsSnapshot({
        repoStore: createStubRepoStore({ baseDir }),
        principal: { kind: "supervisor" },
        stepOrder: ["step-1"],
        anchorRunId: "run_deployment-x",
        deriveStepAddress: ({ runId, stepId }) =>
          `${runId}-${stepId}@example.com`,
      });
    // The deploy delivery carries two credentials; cred_a also has a binding.
    const delivery = {
      bindings: [
        { handle: "gh", credentialId: "cred_a", consumer: "tool:@acme/tools" },
      ],
      materials: [
        {
          credentialId: "cred_a",
          providerKey: "http",
          origin: "https://api.example.test",
          secret: "sk-a",
        },
        {
          credentialId: "cred_b",
          providerKey: "http",
          origin: "https://api.example.test",
          secret: "sk-b",
        },
      ],
    };

    const wired = await spawnWithRunStart({
      baseDir,
      onRunStart,
      credentialDelivery: delivery,
    });

    // Revoke cred_a mid-run (as the online DELETE/revoke path does).
    await wired.supervisor.deliverCredentials({
      delivery: { bindings: [], materials: [] },
      revoke: ["cred_a"],
    });

    // Fire a trigger: the pre-trigger barrier re-asserts the credential set.
    wired.mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("revoke-durable-m1"),
    );
    await waitForTriggerFireRunIds(wired.supervisorToChild, 1);

    // The barrier's re-assertion (the last credentials-updated frame) carries the post-revoke set:
    // cred_a is gone (material AND its binding), cred_b survives. It does NOT re-add cred_a from
    // the frozen deploy delivery.
    const deliveries = parseCredentialsUpdatedFrames(
      wired.supervisorToChild.flushed(),
    );
    const last = deliveries[deliveries.length - 1];
    expect(last).toBeDefined();
    expect(last?.materials.map((m) => m.credentialId)).toEqual(["cred_b"]);
    expect(last?.bindings).toEqual([]);

    await wired.supervisor.shutdown();
  });

  test("a throwing onRunStart fails the run and never fires its trigger", async () => {
    const baseDir = await makeTempDir("supervisor-barrier-fail-");
    // No seedStepGrants: the sink throws regardless, standing in for any barrier failure (a
    // broken read, an unauthorized run). The run must fail LOUDLY -- the trigger is never fired
    // against absent grants.
    const onRunStart: WorkflowSupervisorBindings["onRunStart"] = async () => {
      throw new Error("synthetic grants-barrier failure");
    };

    const wired = await spawnWithRunStart({ baseDir, onRunStart });

    wired.mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("barrier-fail-m1"),
    );

    // The failed run is settled through the claim-check pipeline: the message moves to `consumed`. Wait on that observable settle.
    const address = "run_deployment-x@example.com";
    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 1,
    );
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(1);

    // The barrier suppressed the trigger: NO trigger.fire and NO grants-updated ever reached the child for the failed run.
    const frameTypes = parseControlFrameTypes(
      wired.supervisorToChild.flushed(),
    );
    expect(frameTypes).not.toContain("trigger.fire");
    expect(frameTypes).not.toContain("grants-updated");

    await wired.supervisor.shutdown();
  });

  // Inbound-mail ack/withhold mapping: onMailMessage's promise resolves to a durable-receipt ack, rejects to a withhold (the hub redelivers).
  test("durable receipt resolves for a fresh enqueue and for an already-present message", async () => {
    const baseDir = await makeTempDir("supervisor-ack-present-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    let mode: "enqueued" | "already-present" = "enqueued";
    const inboxPrimitives = inboxPrimitivesWithEnqueue(
      async (_store, _principal, _repoId, args) =>
        mode === "already-present"
          ? { outcome: "already-present", reason: "consumed" }
          : enqueuedOutcome(args),
    );
    const mailBus = createSettleableMailBus();
    const wired = await spawnWithRunStart({
      baseDir,
      inboxPrimitives,
      mailBus,
    });
    const address = "run_deployment-x@example.com";

    // A fresh enqueue is durably accepted -> the receipt resolves (ack).
    await expect(
      mailBus.settle(address, new TextEncoder().encode("m-fresh")),
    ).resolves.toBeUndefined();

    // An already-present message is also durably accounted for -> ack, so the hub stops retrying a message the sidecar holds.
    mode = "already-present";
    await expect(
      mailBus.settle(address, new TextEncoder().encode("m-dup")),
    ).resolves.toBeUndefined();

    await wired.supervisor.shutdown();
  });

  test("durable receipt rejects for a transient failure and a stale refusal, and self-heals on redelivery", async () => {
    const baseDir = await makeTempDir("supervisor-withhold-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    let onEnqueue: (args: {
      address: string;
      messageId: string;
      receivedAt: number;
      mailAuditRef: { store: string; path: string };
    }) => EnqueueInboxOutcome = () => {
      throw new Error("disk exploded");
    };
    const inboxPrimitives = inboxPrimitivesWithEnqueue(
      async (_store, _principal, _repoId, args) => onEnqueue(args),
    );
    const mailBus = createSettleableMailBus();
    const wired = await spawnWithRunStart({
      baseDir,
      inboxPrimitives,
      mailBus,
    });
    const address = "run_deployment-x@example.com";

    // (a) A transient failure -> the receipt rejects, so no ack is sent and the hub redelivers.
    await expect(
      mailBus.settle(address, new TextEncoder().encode("m-1")),
    ).rejects.toThrow(/disk exploded/);

    // (b) A stale refusal also withholds, and surfaces as its own loud type rather than blending into generic failure noise.
    onEnqueue = () => {
      throw new StaleInboxEnqueueError("claim_check_stale_enqueue: synthetic");
    };
    let staleCause: unknown;
    try {
      await mailBus.settle(address, new TextEncoder().encode("m-1"));
    } catch (err) {
      staleCause = err;
    }
    expect(staleCause).toBeInstanceOf(StaleInboxEnqueueError);

    // (c) Self-heal: the same message, redelivered once the failure clears, enqueues on a fresh receivedAt and the receipt resolves (ack).
    onEnqueue = (args) => enqueuedOutcome(args);
    await expect(
      mailBus.settle(address, new TextEncoder().encode("m-1")),
    ).resolves.toBeUndefined();

    await wired.supervisor.shutdown();
  });

  test("durable receipt rejects when the supervisor is not accepting mail (phase-drop)", async () => {
    const baseDir = await makeTempDir("supervisor-phase-drop-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    // enqueue would resolve if it were reached; the phase gate must reject first, without touching the inbox.
    const inboxPrimitives = inboxPrimitivesWithEnqueue(
      async (_store, _principal, _repoId, args) => enqueuedOutcome(args),
    );
    const mailBus = createSettleableMailBus();
    const wired = await spawnWithRunStart({
      baseDir,
      inboxPrimitives,
      mailBus,
    });
    await wired.supervisor.shutdown();

    // The bus retained the handler, so this drives onMailMessage's own phase gate: phase is
    // "stopped", so it rejects BEFORE calling enqueue -> the ack is withheld and the hub
    // redelivers into a live generation later.
    await expect(
      mailBus.settle(
        "run_deployment-x@example.com",
        new TextEncoder().encode("m-late"),
      ),
    ).rejects.toThrow(/not accepted: supervisor phase/);
  });

  // Ready-timeout harness: a deterministic FakeTimer registry plus a controllable child whose
  // control reader can be closed to model an exit before ready. createdTimers retains cleared
  // timers so a test can assert the deadline was cancelled.
  async function makeReadyTimeoutHarness(readyTimeoutMs: number) {
    type FakeTimer = { cb: () => void; ms: number; cancelled: boolean };
    const timers = new Set<FakeTimer>();
    const createdTimers: FakeTimer[] = [];
    // Arming a timer is the event `waitForReadyDeadline` waits on; the registry it reads only grows when `setTimer` is called.
    const timerChanges = createChangeNotifier();

    const baseDir = await makeTempDir("supervisor-ready-timeout-");
    const supervisorIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    const killSignals: string[] = [];

    const spawner: SubprocessSpawner = ({ env: _env }) => ({
      pid: 5150,
      controlWriter: supervisorToChild.writer,
      controlReader: childToSupervisor.reader,
      eventReader: eventChildToSupervisor.reader,
      kill: (signal) => {
        killSignals.push(
          typeof signal === "string" ? signal : String(signal ?? ""),
        );
        childToSupervisor.close();
        eventChildToSupervisor.close();
        resolveExit?.(0);
      },
      exited,
    });

    const mailBus = createMockMailBus();
    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({ sig: new Uint8Array(64), principalKind: "supervisor" }),
      mailBus,
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
      readyTimeoutMs,
      setTimer: (cb, ms) => {
        const t: FakeTimer = { cb, ms, cancelled: false };
        timers.add(t);
        createdTimers.push(t);
        timerChanges.notify();
        return t;
      },
      clearTimer: (handle) => {
        if (handle === null || typeof handle !== "object") return;
        for (const t of timers) {
          if (t === handle) {
            t.cancelled = true;
            timers.delete(t);
            return;
          }
        }
      },
    };
    const supervisor = supervisors.track(createWorkflowSupervisor(bindings));

    // Resolve once the spawn has armed its ready deadline (which happens after the spawner is invoked, so this also confirms the child spawned).
    async function waitForReadyDeadline(): Promise<FakeTimer> {
      const armed = () => createdTimers.find((x) => x.ms === readyTimeoutMs);
      await timerChanges.until(() => armed() !== undefined);
      const deadline = armed();
      if (deadline === undefined) {
        throw new Error("ready deadline timer missing after it was armed");
      }
      return deadline;
    }

    return { supervisor, killSignals, childToSupervisor, waitForReadyDeadline };
  }

  const readyTimeoutSpawnOpts = {
    stepOrder: ["step-1"],
    definitionHash: "def-hash-abc",
    warmKeep: false,

    onInferenceEvent: () => {
      /* unused in the ready-timeout tests */
    },
  };

  test("spawn times out, kills the child, rejects, and clears the ready deadline", async () => {
    const h = await makeReadyTimeoutHarness(7_777);
    // Never send `ready`. Spawn blocks on the handshake until the deadline.
    const spawnPromise = h.supervisor.spawn(readyTimeoutSpawnOpts);
    const readyDeadline = await h.waitForReadyDeadline();
    readyDeadline.cb();

    await expect(spawnPromise).rejects.toThrow(
      /child did not emit ready within 7777ms; killed/,
    );
    expect(h.killSignals).toContain("SIGTERM");
    // The unconditional deadline-timer clear ran on the timeout path.
    expect(readyDeadline.cancelled).toBe(true);
  });

  test("spawn clears the ready deadline when the child exits before ready", async () => {
    const h = await makeReadyTimeoutHarness(8_888);
    const spawnPromise = h.supervisor.spawn(readyTimeoutSpawnOpts);
    const readyDeadline = await h.waitForReadyDeadline();

    // Closing the reader rejects ready; the fold to values lets the unconditional deadline clear
    // run (a rejecting race would leak an armed timer for up to readyTimeoutMs).
    h.childToSupervisor.close();

    await expect(spawnPromise).rejects.toThrow(
      /control channel ended before child emitted ready/,
    );
    expect(readyDeadline.cancelled).toBe(true);
  });

  // A spawn that throws after the OS child is running but before ready must not orphan it or
  // leave the address registered; shutdownInternal owns that teardown.
  async function makePreRegistrationFailureHarness(opts: {
    failSubscribe?: boolean;
    failDeriveStepAddress?: boolean;
  }) {
    const baseDir = await makeTempDir("supervisor-spawn-leak-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const supervisorIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    const killSignals: string[] = [];
    const spawner: SubprocessSpawner = () => ({
      pid: 4321,
      controlWriter: supervisorToChild.writer,
      controlReader: childToSupervisor.reader,
      eventReader: eventChildToSupervisor.reader,
      kill: (signal) => {
        killSignals.push(
          typeof signal === "string" ? signal : String(signal ?? ""),
        );
        childToSupervisor.close();
        eventChildToSupervisor.close();
        resolveExit?.(0);
      },
      exited,
    });
    const mailBus = createMockMailBus();
    const bindingsMailBus: MailBusBindings = {
      ...mailBus,
      subscribeMailForAddress:
        opts.failSubscribe === true
          ? () => {
              throw new Error("injected subscribe failure");
            }
          : mailBus.subscribeMailForAddress,
    };
    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({ sig: new Uint8Array(64), principalKind: "supervisor" }),
      mailBus: bindingsMailBus,
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
      ...(opts.failDeriveStepAddress === true
        ? {
            deriveStepAddress: () => {
              throw new Error("injected deriveStepAddress failure");
            },
          }
        : {}),
    };
    return {
      supervisor: supervisors.track(createWorkflowSupervisor(bindings)),
      killSignals,
      registered: mailBus.registered,
    };
  }

  const preRegistrationSpawnOpts = {
    stepOrder: ["step-1"],
    definitionHash: "def-hash-abc",
    warmKeep: false,

    onInferenceEvent: () => {
      /* unused in the pre-registration failure tests */
    },
  };

  test("a spawn whose mail subscription throws kills the child and releases the address", async () => {
    const h = await makePreRegistrationFailureHarness({ failSubscribe: true });
    await expect(h.supervisor.spawn(preRegistrationSpawnOpts)).rejects.toThrow(
      "injected subscribe failure",
    );
    // The address was registered just before subscribe threw; the teardown must unregister it so no orphaned registration survives.
    expect(h.registered()).toHaveLength(0);
    expect(h.killSignals.length).toBeGreaterThan(0);
  });

  test("a spawn whose credentials assembly throws kills the child", async () => {
    const h = await makePreRegistrationFailureHarness({
      failDeriveStepAddress: true,
    });
    await expect(h.supervisor.spawn(preRegistrationSpawnOpts)).rejects.toThrow(
      "injected deriveStepAddress failure",
    );
    expect(h.registered()).toHaveLength(0);
    expect(h.killSignals.length).toBeGreaterThan(0);
  });

  test("drain() forwards the `drain` control frame and arms a drainTimeout accumulator per in-flight run", async () => {
    const baseDir = await makeTempDir("supervisor-drain-arm-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    let observedEnv: Record<string, string> | undefined;
    // Scoped here, not to the file: `first()` must resolve with THIS fixture's spawn, not whichever spawn happened earliest in the run.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      spawnObserver.record(env);
      const handle: SubprocessHandle = {
        pid: 9999,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
      return handle;
    };

    // Mock accumulator factory: each invocation records opts and returns a controllable stub with visible start/stop calls.
    type StubAccumulator = DrainTimeoutAccumulator & {
      __opts: DrainTimeoutOpts;
      __startCount: number;
      __stopCount: number;
    };
    const stubs: StubAccumulator[] = [];
    const factory: DrainTimeoutAccumulatorFactory = (opts) => {
      const stub: StubAccumulator = {
        __opts: opts,
        __startCount: 0,
        __stopCount: 0,
        start() {
          this.__startCount += 1;
        },
        pause() {
          /* unused by the supervisor's arming path */
        },
        resume() {
          /* unused by the supervisor's arming path */
        },
        stop() {
          this.__stopCount += 1;
        },
        accumulatedMs() {
          return 0;
        },
        get escalated() {
          return false;
        },
        disposed() {
          return Promise.resolve();
        },
      };
      stubs.push(stub);
      return stub;
    };

    const mailBus = createMockMailBus();
    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus,
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
      drainTimeoutAccumulatorFactory: factory,
      drainTimeoutMs: 7_500,
    };
    const supervisor = supervisors.track(createWorkflowSupervisor(bindings));

    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      warmKeep: false,

      onInferenceEvent: () => {
        /* unused in this test */
      },
    });
    observedEnv = await spawnObserver.first();
    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeyPair.privateKey,
      channelId,
      writer: {
        write(line: string) {
          childToSupervisor.inject(line);
        },
      },
    });
    await mailBus.awaitRegistered("run_deployment-x@example.com");
    // Two pre-ready messages; the FIFO queue serializes dispatch, so by drain() the second may
    // still be mid-dispatch. The accumulator count reflects whichever runIds remain in-flight.
    mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("drain-msg-A"),
    );
    mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("drain-msg-B"),
    );
    await childSender.send({
      type: "ready",
      data: {
        childPid: 9999,
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
    await spawnPromise;

    // No accumulators armed yet -- drain has not been called.
    expect(stubs).toHaveLength(0);

    // Wait for the first trigger.fire: the replayDone gate makes the first dispatch asynchronous; without this, drain() could arm nothing.
    await waitForTriggerFireRunIds(supervisorToChild, 1);
    expect(
      parseTriggerFireRunIds(supervisorToChild.flushed()).length,
    ).toBeGreaterThanOrEqual(1);

    await supervisor.drain({ deadlineMs: 7_500 });

    // Find the drain frame by payload type rather than tail index: dispatch keeps running, so a trigger.fire can land on either side.
    const forwarded = supervisorToChild.flushed();
    expect(forwarded.length).toBeGreaterThanOrEqual(2);
    const SignedFrame = type({
      envelope: {
        seq: "number",
        channelId: "string",
        payload: {
          type: "string",
          "+": "ignore",
        },
        "+": "ignore",
      },
      "+": "ignore",
    });
    const drainFrame = (() => {
      for (const line of forwarded) {
        const parsed = SignedFrame(JSON.parse(line));
        if (parsed instanceof type.errors) continue;
        if (parsed.envelope.payload.type === "drain") return parsed;
      }
      throw new Error("no drain frame observed on supervisor-to-child stream");
    })();
    expect(drainFrame.envelope.payload).toMatchObject({
      type: "drain",
      data: { deadlineMs: 7_500 },
    });

    // Serial dispatch means drain() sees exactly one in-flight run and arms one accumulator; the second message stays in the inbox.
    expect(stubs.length).toBeGreaterThanOrEqual(1);
    for (const stub of stubs) {
      expect(stub.__startCount).toBe(1);
      expect(stub.__stopCount).toBe(0);
      expect(stub.__opts.anchorRunId).toBe("run_deployment-x");
      expect(stub.__opts.repoId).toEqual({
        kind: "workflow-run",
        id: "run_deployment-x",
      });
      expect(stub.__opts.ref).toBe("refs/heads/main");
      expect(stub.__opts.drainTimeoutMs).toBe(7_500);
      expect(typeof stub.__opts.runId).toBe("string");
      expect(stub.__opts.runId.length).toBeGreaterThan(0);
    }
    const runIds = stubs.map((s) => s.__opts.runId);
    expect(new Set(runIds).size).toBe(stubs.length);

    // Shutdown stops every armed accumulator before tearing the child down.
    await supervisor.shutdown();
    for (const stub of stubs) {
      expect(stub.__stopCount).toBe(1);
    }
  });

  test("drain() escalates via signAsPrincipal when the accumulator's timeout fires", async () => {
    // Production-shaped wiring: the real accumulator commits CancelRequested{origin:"supervisor-drain"} once the fake clock passes drainTimeoutMs.
    const baseDir = await makeTempDir("supervisor-drain-escalate-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    let observedEnv: Record<string, string> | undefined;
    // Scoped here, not to the file: `first()` must resolve with THIS fixture's spawn, not whichever spawn happened earliest in the run.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      spawnObserver.record(env);
      const handle: SubprocessHandle = {
        pid: 8888,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
      return handle;
    };

    type FakeTimer = { cb: () => void; ms: number; cancelled: boolean };
    const timers = new Set<FakeTimer>();
    let fakeNow = 1_700_000_000_000;
    const observedWrites: {
      principal: { kind: string };
      repoId: RepoId;
      ref: string;
      files: Record<string, string | Uint8Array>;
    }[] = [];
    // The captured commit is the event the escalation assertion waits on: the accumulator's CancelRequested reaches this test only as a write.
    const writeChanges = createChangeNotifier();

    const mailBus = createMockMailBus();
    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus,
      onWrite: (args) => {
        observedWrites.push(args);
        writeChanges.notify();
      },
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
      drainTimeoutMs: 1_000,
      now: () => fakeNow,
      setTimer: (cb, ms) => {
        const t: FakeTimer = { cb, ms, cancelled: false };
        timers.add(t);
        return t;
      },
      clearTimer: (handle) => {
        if (handle === null || typeof handle !== "object") return;
        for (const t of timers) {
          if (t === handle) {
            t.cancelled = true;
            timers.delete(t);
            return;
          }
        }
      },
    };
    const supervisor = supervisors.track(createWorkflowSupervisor(bindings));

    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      warmKeep: false,

      onInferenceEvent: () => {
        /* unused in this test */
      },
    });
    observedEnv = await spawnObserver.first();
    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeyPair.privateKey,
      channelId,
      writer: {
        write(line: string) {
          childToSupervisor.inject(line);
        },
      },
    });
    await mailBus.awaitRegistered("run_deployment-x@example.com");
    mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("escalate-msg"),
    );
    await childSender.send({
      type: "ready",
      data: {
        childPid: 8888,
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
    await spawnPromise;

    // Wait for the forwarded trigger.fire so the run is in cohortRunIds when drain() arms; the replayDone gate makes the first dispatch async.
    await waitForTriggerFireRunIds(supervisorToChild, 1);
    expect(
      parseTriggerFireRunIds(supervisorToChild.flushed()).length,
    ).toBeGreaterThanOrEqual(1);

    await supervisor.drain({ deadlineMs: 1_000 });
    expect(timers.size).toBe(1);
    // Advance the fake clock past the timeout and fire the accumulator's pending timer.
    fakeNow += 1_000;
    const due = [...timers];
    for (const t of due) {
      if (t.cancelled) continue;
      timers.delete(t);
      t.cb();
    }
    // Filter to the write carrying the CancelRequested event rather than coupling to other maintenance writes.
    const eventWrites = () =>
      observedWrites.filter((w) =>
        Object.keys(w.files).some((k) => k.includes("/events/")),
      );
    await writeChanges.until(() => eventWrites().length >= 1);
    const writesWithEvents = eventWrites();
    expect(writesWithEvents.length).toBe(1);
    const write = writesWithEvents[0];
    if (write === undefined) {
      throw new Error("no CancelRequested commit captured");
    }
    expect(write.principal.kind).toBe("supervisor");
    expect(write.repoId).toEqual({
      kind: "workflow-run",
      id: "run_deployment-x",
    });
    const eventEntry = Object.entries(write.files).find(([k]) =>
      k.includes("/events/"),
    );
    if (eventEntry === undefined) {
      throw new Error("no event blob captured in the commit");
    }
    const [, blobBytes] = eventEntry;
    const blobJson =
      typeof blobBytes === "string"
        ? blobBytes
        : new TextDecoder().decode(blobBytes);
    const blob = readCancelRequestedBlob(blobJson);
    expect(blob.type).toBe("CancelRequested");
    expect(blob.origin).toBe("supervisor-drain");
    expect(blob.signature.principalKind).toBe("supervisor");

    await supervisor.shutdown();
  });

  test("requestCancel signs CancelRequested via signAsPrincipal for every origin", async () => {
    const baseDir = await makeTempDir("supervisor-cancel-");
    const signSpyCalls: { kind: string; payload: Uint8Array }[] = [];
    const observedWrites: {
      principal: { kind: string };
      repoId: RepoId;
      ref: string;
      files: Record<string, string | Uint8Array>;
    }[] = [];
    const bindings = await buildBindings({
      baseDir,
      spawner: () => {
        throw new Error("spawn not invoked in cancel test");
      },
      signSpy: (kind, payload) => {
        signSpyCalls.push({ kind, payload });
        // Synthetic 64-byte signature with the run id encoded in the first bytes so the test asserts which call produced it.
        const sig = new Uint8Array(64);
        sig[0] = signSpyCalls.length;
        return { sig, principalKind: "supervisor" };
      },
      mailBus: createMockMailBus(),
      onWrite: (args) => observedWrites.push(args),
    });
    const supervisor = supervisors.track(createWorkflowSupervisor(bindings));

    const origins = [
      "self",
      "supervisor-drain",
      "supervisor-operator",
      "hub-admin",
    ] as const;
    for (const origin of origins) {
      const result = await supervisor.requestCancel({
        runId: `run-${origin}`,
        origin,
        reason: `reason for ${origin}`,
        at: "2026-01-01T00:00:00.000Z",
      });
      expect(result.commitSha).toBe("deadbeefcafef00d");
    }

    // Every origin signs as principal kind "supervisor"; the hub-admin variance is enforced at push presentation, not in the signing path.
    expect(signSpyCalls.length).toBe(origins.length);
    for (const call of signSpyCalls) {
      expect(call.kind).toBe("supervisor");
      expect(call.payload).toBeInstanceOf(Uint8Array);
      const text = new TextDecoder().decode(call.payload);
      expect(text).toContain("CancelRequested");
    }

    expect(observedWrites.length).toBe(origins.length);
    for (const write of observedWrites) {
      expect(write.principal.kind).toBe("supervisor");
      expect(write.repoId).toEqual({
        kind: "workflow-run",
        id: "run_deployment-x",
      });
    }
    const firstWrite = observedWrites[0];
    if (firstWrite === undefined) {
      throw new Error("no observed writes captured");
    }
    const firstEntry = Object.entries(firstWrite.files)[0];
    if (firstEntry === undefined) {
      throw new Error("first write produced no files");
    }
    const [, firstBytes] = firstEntry;
    const firstJson =
      typeof firstBytes === "string"
        ? firstBytes
        : new TextDecoder().decode(firstBytes);
    const onDisk = readCancelRequestedBlob(firstJson);
    expect(onDisk.type).toBe("CancelRequested");
    expect(onDisk.origin).toBe("self");
    expect(onDisk.signature.principalKind).toBe("supervisor");
    expect(onDisk.signature.sig).toMatch(/^01[0-9a-f]+$/);
  });

  test("requestCancel reports a committed cancellation when the child wakeup fails", async () => {
    const baseDir = await makeTempDir("supervisor-cancel-wakeup-");
    let failWakeup = false;
    let committed = 0;
    const wired = await spawnWithRunStart({
      baseDir,
      failControlWrite: () => failWakeup,
      onWrite: (args) => {
        if (args.message.startsWith("append CancelRequested")) committed += 1;
      },
    });

    const cancellation = wired.supervisor.requestCancel({
      runId: "run_deployment-x",
      origin: "supervisor-operator",
      reason: "Stop",
      at: new Date().toISOString(),
    });
    await waitForUpstreamPayloads(wired.supervisorToChild, "cancel.prepare", 1);
    failWakeup = true;
    await wired.childSender.send({
      type: "cancel.prepared",
      data: { requestId: "cancel-1" },
    });

    expect((await cancellation).commitSha).toBe("deadbeefcafef00d");
    expect(committed).toBe(1);
    failWakeup = false;
    await wired.supervisor.shutdown();
  });

  test("drain() threads the per-cohort terminal broadcaster into each accumulator's opts", async () => {
    const baseDir = await makeTempDir("supervisor-drain-terminal-source-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    let observedEnv: Record<string, string> | undefined;
    // Scoped here, not to the file: `first()` must resolve with THIS fixture's spawn, not whichever spawn happened earliest in the run.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      spawnObserver.record(env);
      return {
        pid: 7777,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
    };

    type StubAccumulator = DrainTimeoutAccumulator & {
      __opts: DrainTimeoutOpts;
    };
    const stubs: StubAccumulator[] = [];
    const factory: DrainTimeoutAccumulatorFactory = (opts) => {
      const stub: StubAccumulator = {
        __opts: opts,
        start() {
          /* unused */
        },
        pause() {
          /* unused */
        },
        resume() {
          /* unused */
        },
        stop() {
          /* unused */
        },
        accumulatedMs() {
          return 0;
        },
        get escalated() {
          return false;
        },
        disposed() {
          return Promise.resolve();
        },
      };
      stubs.push(stub);
      return stub;
    };

    const mailBus = createMockMailBus();
    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus,
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
      drainTimeoutAccumulatorFactory: factory,
      drainTimeoutMs: 5_000,
    };
    const supervisor = supervisors.track(createWorkflowSupervisor(bindings));

    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      warmKeep: false,

      onInferenceEvent: () => undefined,
    });
    observedEnv = await spawnObserver.first();
    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeyPair.privateKey,
      channelId,
      writer: {
        write(line: string) {
          childToSupervisor.inject(line);
        },
      },
    });
    await mailBus.awaitRegistered("run_deployment-x@example.com");
    mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("term-msg-A"),
    );
    await childSender.send({
      type: "ready",
      data: {
        childPid: 7777,
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
    await spawnPromise;
    // Wait for the forwarded trigger.fire so the run is in cohortRunIds when drain() arms.
    await waitForTriggerFireRunIds(supervisorToChild, 1);
    expect(
      parseTriggerFireRunIds(supervisorToChild.flushed()).length,
    ).toBeGreaterThanOrEqual(1);
    await supervisor.drain({ deadlineMs: 5_000 });

    // The supervisor's per-cohort terminal broadcaster always backs the accumulator's
    // terminal-event source; the accumulator factory sees a non-undefined slot and can mint a
    // per-runId iterator through it.
    expect(stubs).toHaveLength(1);
    const stub = stubs[0];
    if (stub === undefined) throw new Error("expected one stub accumulator");
    expect(stub.__opts.terminalEventSource).toBeDefined();
    const factorySource = stub.__opts.terminalEventSource;
    if (factorySource === undefined) {
      throw new Error(
        "expected accumulator opts to carry a terminalEventSource",
      );
    }
    const iterable = factorySource(stub.__opts.runId);
    const iter = iterable[Symbol.asyncIterator]();
    await iter.return?.(undefined);

    await supervisor.shutdown();
  });

  test("drain() arms the broadcaster-backed accumulator source on the active cohort", async () => {
    const baseDir = await makeTempDir("supervisor-drain-no-term-source-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    let observedEnv: Record<string, string> | undefined;
    // Scoped here, not to the file: `first()` must resolve with THIS fixture's spawn, not whichever spawn happened earliest in the run.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      spawnObserver.record(env);
      return {
        pid: 6666,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
    };

    type StubAccumulator = DrainTimeoutAccumulator & {
      __opts: DrainTimeoutOpts;
    };
    const stubs: StubAccumulator[] = [];
    const factory: DrainTimeoutAccumulatorFactory = (opts) => {
      const stub: StubAccumulator = {
        __opts: opts,
        start() {
          /* unused */
        },
        pause() {
          /* unused */
        },
        resume() {
          /* unused */
        },
        stop() {
          /* unused */
        },
        accumulatedMs() {
          return 0;
        },
        get escalated() {
          return false;
        },
        disposed() {
          return Promise.resolve();
        },
      };
      stubs.push(stub);
      return stub;
    };

    const mailBus = createMockMailBus();
    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus,
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
      drainTimeoutAccumulatorFactory: factory,
      drainTimeoutMs: 5_000,
    };
    const supervisor = supervisors.track(createWorkflowSupervisor(bindings));

    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      warmKeep: false,

      onInferenceEvent: () => undefined,
    });
    observedEnv = await spawnObserver.first();
    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeyPair.privateKey,
      channelId,
      writer: {
        write(line: string) {
          childToSupervisor.inject(line);
        },
      },
    });
    await mailBus.awaitRegistered("run_deployment-x@example.com");
    mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("no-term-msg"),
    );
    await childSender.send({
      type: "ready",
      data: {
        childPid: 6666,
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
    await spawnPromise;
    // Wait for the forwarded trigger.fire so the run is in cohortRunIds when drain() arms.
    await waitForTriggerFireRunIds(supervisorToChild, 1);
    expect(
      parseTriggerFireRunIds(supervisorToChild.flushed()).length,
    ).toBeGreaterThanOrEqual(1);
    await supervisor.drain({ deadlineMs: 5_000 });

    expect(stubs).toHaveLength(1);
    const stub = stubs[0];
    if (stub === undefined) throw new Error("expected one stub accumulator");
    // The accumulator factory always receives a terminal source backed by the active cohort's broadcaster; there is no timer-only path left.
    expect(stub.__opts.terminalEventSource).toBeDefined();

    await supervisor.shutdown();
  });

  test("drain() is a no-op when the supervisor is idle (no spawn has run)", async () => {
    // Pins the defensive contract: drain in idle/stopping/stopped returns silently, throws
    // nothing, forwards no drain frame, and arms no accumulators -- the contract host shutdown
    // sequences rely on.
    const baseDir = await makeTempDir("supervisor-drain-idle-");
    const accumulatorInvocations: DrainTimeoutOpts[] = [];
    const accumulatorFactory: DrainTimeoutAccumulatorFactory = (opts) => {
      accumulatorInvocations.push(opts);
      const stub: DrainTimeoutAccumulator = {
        start() {
          /* unused */
        },
        pause() {
          /* unused */
        },
        resume() {
          /* unused */
        },
        stop() {
          /* unused */
        },
        accumulatedMs() {
          return 0;
        },
        get escalated() {
          return false;
        },
        disposed() {
          return Promise.resolve();
        },
      };
      return stub;
    };
    const bindings = await buildBindings({
      baseDir,
      spawner: () => {
        throw new Error("spawner must not be invoked on the idle drain path");
      },
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus: createMockMailBus(),
    });
    const supervisor = supervisors.track(
      createWorkflowSupervisor({
        ...bindings,
        drainTimeoutAccumulatorFactory: accumulatorFactory,
      }),
    );
    await supervisor.drain({ deadlineMs: 5_000 });
    expect(accumulatorInvocations).toHaveLength(0);
  });

  test("deliverSignal() rejects when the supervisor is idle (no spawn has run)", async () => {
    // Pins the defensive contract: deliverSignal throws off starting/running/recycling; the rejection reaches the hub-link, which logs and drops.
    const baseDir = await makeTempDir("supervisor-deliver-signal-idle-");
    const bindings = await buildBindings({
      baseDir,
      spawner: () => {
        throw new Error("spawner must not be invoked on the idle signal path");
      },
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus: createMockMailBus(),
    });
    const supervisor = supervisors.track(createWorkflowSupervisor(bindings));
    await expect(
      supervisor.deliverSignal({
        runId: "run-stale",
        signalName: "approve",
        signalId: "sig-stale",
        payload: null,
      }),
    ).rejects.toThrow(/deliverSignal called in phase idle/);
  });

  test("deliverSources() rejects when the supervisor is idle (no spawn has run)", async () => {
    // Same phase-guard as deliverSignal: a rotation off starting/running throws rather than writing into a dead child's pipe.
    const baseDir = await makeTempDir("supervisor-deliver-sources-idle-");
    const bindings = await buildBindings({
      baseDir,
      spawner: () => {
        throw new Error("spawner must not be invoked on the idle sources path");
      },
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus: createMockMailBus(),
    });
    const supervisor = supervisors.track(createWorkflowSupervisor(bindings));
    await expect(
      supervisor.deliverSources({
        sources: [
          {
            id: "primary",
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            credentialId: "sk-x",
            model: "claude-test",
          },
        ],
        defaultSource: "primary",
      }),
    ).rejects.toThrow(/deliverSources called in phase idle/);
  });

  test("deliverSources() sends a sources-updated frame when running", async () => {
    const baseDir = await makeTempDir("supervisor-deliver-sources-running-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });

    let observedEnv: Record<string, string> | undefined;
    // Scoped here, not to the file: `first()` must resolve with THIS fixture's spawn, not whichever spawn happened earliest in the run.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      spawnObserver.record(env);
      const handle: SubprocessHandle = {
        pid: 4321,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
      return handle;
    };

    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({ sig: new Uint8Array(64), principalKind: "supervisor" }),
      mailBus: createMockMailBus(),
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
    };
    const supervisor = supervisors.track(createWorkflowSupervisor(bindings));

    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      warmKeep: true,

      onInferenceEvent: () => undefined,
    });
    observedEnv = await spawnObserver.first();
    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeyPair.privateKey,
      channelId,
      writer: {
        write(line: string) {
          childToSupervisor.inject(line);
        },
      },
    });
    await childSender.send({
      type: "ready",
      data: {
        childPid: 4321,
        childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString("hex"),
      },
    });
    await spawnPromise;

    const sources: InferenceSource[] = [
      {
        id: "primary",
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        credentialId: "sk-primary",
        model: "claude-test",
      },
    ];
    await supervisor.deliverSources({ sources, defaultSource: "primary" });

    const frames = parseSourcesUpdatedFrames(supervisorToChild.flushed());
    expect(frames).toHaveLength(1);
    expect(frames[0]?.sources).toEqual(sources);
    expect(frames[0]?.defaultSource).toBe("primary");

    await supervisor.shutdown();
  });

  test("deliverCredentials() while not running advances the mirror without sending, so the next spawn seeds the post-revoke set", async () => {
    // A revoke with no live child must not throw or write, but must advance the mirror so the eviction survives the next spawn.
    const baseDir = await makeTempDir("supervisor-deliver-credentials-idle-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });

    let observedEnv: Record<string, string> | undefined;
    // Scoped here, not to the file: `first()` must resolve with THIS fixture's spawn, not whichever spawn happened earliest in the run.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      spawnObserver.record(env);
      return {
        pid: 4321,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
    };

    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({ sig: new Uint8Array(64), principalKind: "supervisor" }),
      mailBus: createMockMailBus(),
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
      // The deploy delivery: cred_a (with a binding) and cred_b.
      credentialDelivery: {
        bindings: [
          { handle: "gh", credentialId: "cred_a", consumer: "tool:@acme/x" },
        ],
        materials: [
          {
            credentialId: "cred_a",
            providerKey: "http",
            origin: "https://api.example.test",
            secret: "sk-a",
          },
          {
            credentialId: "cred_b",
            providerKey: "http",
            origin: "https://api.example.test",
            secret: "sk-b",
          },
        ],
      },
    };
    const supervisor = supervisors.track(createWorkflowSupervisor(bindings));

    // Revoke cred_a while the supervisor is idle (no child spawned yet). It must resolve without throwing and send nothing.
    await supervisor.deliverCredentials({
      delivery: { bindings: [], materials: [] },
      revoke: ["cred_a"],
    });
    expect(observedEnv).toBeUndefined(); // no spawn
    expect(parseCredentialsUpdatedFrames(supervisorToChild.flushed())).toEqual(
      [],
    ); // nothing sent

    // Now spawn: the spawn re-assertion must carry the post-revoke mirror.
    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-idle",
      warmKeep: true,
      onInferenceEvent: () => undefined,
    });
    observedEnv = await spawnObserver.first();
    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeyPair.privateKey,
      channelId,
      writer: {
        write(line: string) {
          childToSupervisor.inject(line);
        },
      },
    });
    await childSender.send({
      type: "ready",
      data: {
        childPid: 4321,
        childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString("hex"),
      },
    });
    await spawnPromise;

    const deliveries = parseCredentialsUpdatedFrames(
      supervisorToChild.flushed(),
    );
    const last = deliveries[deliveries.length - 1];
    expect(last).toBeDefined();
    expect(last?.materials.map((m) => m.credentialId)).toEqual(["cred_b"]);
    expect(last?.bindings).toEqual([]);

    await supervisor.shutdown();
  });

  test("deliverCredentials() sends a credentials-updated frame when running", async () => {
    const baseDir = await makeTempDir(
      "supervisor-deliver-credentials-running-",
    );
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });

    let observedEnv: Record<string, string> | undefined;
    // Scoped here, not to the file: `first()` must resolve with THIS fixture's spawn, not whichever spawn happened earliest in the run.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      spawnObserver.record(env);
      const handle: SubprocessHandle = {
        pid: 4321,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
      return handle;
    };

    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({ sig: new Uint8Array(64), principalKind: "supervisor" }),
      mailBus: createMockMailBus(),
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
    };
    const supervisor = supervisors.track(createWorkflowSupervisor(bindings));

    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      warmKeep: true,
      onInferenceEvent: () => undefined,
    });
    observedEnv = await spawnObserver.first();
    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeyPair.privateKey,
      channelId,
      writer: {
        write(line: string) {
          childToSupervisor.inject(line);
        },
      },
    });
    await childSender.send({
      type: "ready",
      data: {
        childPid: 4321,
        childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString("hex"),
      },
    });
    await spawnPromise;

    const delivery = {
      bindings: [
        {
          handle: "gh",
          credentialId: "cred_a",
          consumer: "tool:@intx/tools-example",
        },
      ],
      materials: [
        {
          credentialId: "cred_a",
          providerKey: "http",
          origin: "https://api.example.test",
          secret: "sk-real",
        },
      ],
    };
    await supervisor.deliverCredentials({ delivery, revoke: ["cred_gone"] });

    const data = parseCredentialsUpdatedData(supervisorToChild.flushed());
    expect(data).toHaveLength(1);
    expect(data[0]?.delivery).toEqual(delivery);
    // The revoke list rides the same frame so the child drops the named id.
    expect(data[0]?.revoke).toEqual(["cred_gone"]);

    await supervisor.shutdown();
  });
  // ------------------------------------------------------------------
  // Long-lived dispatch path
  // ------------------------------------------------------------------

  test("long-lived: first message fires trigger.fire with stable runId", async () => {
    const baseDir = await makeTempDir("supervisor-long-lived-first-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    const wired = await spawnWithRunStart({
      baseDir,

      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });

    wired.mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("msg-1"),
    );

    // Drive the mock child to terminal so the dispatch loop's markConsumed wait proceeds; the terminal event follows the naming trigger.
    await waitForTriggerFireRunIds(wired.supervisorToChild, 1);
    await wired.childSender.send({
      type: "terminal.event",
      data: {
        runId: "run_deployment-x",
        seq: 0,
        kind: "RunCompleted",
        at: new Date().toISOString(),
      },
    });

    const address = "run_deployment-x@example.com";
    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 1,
    );
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(1);

    const runIds = parseTriggerFireRunIds(wired.supervisorToChild.flushed());
    expect(runIds).toEqual(["run_deployment-x"]);
    await wired.supervisor.shutdown();
  });

  test("long-lived: a trigger.fire run parking on approval releases the dispatch wait", async () => {
    const baseDir = await makeTempDir("supervisor-long-lived-approval-park-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    const wired = await spawnWithRunStart({
      baseDir,

      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });

    wired.mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("msg-1"),
    );

    // The park must follow the trigger that starts the run; a park before the waiter arms is still observed via the generation capture.
    await waitForTriggerFireRunIds(wired.supervisorToChild, 1);

    // An approval park must release the dispatch wait like an input park does; otherwise this hangs to the backstop, never consuming the mail.
    await wired.childSender.send({
      type: "park.notify",
      data: {
        runId: "run_deployment-x",
        correlationId: "corr-approval-1",
        parkKind: "approval",
      },
    });

    const address = "run_deployment-x@example.com";
    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 1,
    );
    // markConsumed only runs once the dispatch wait returns; a wait still hanging on the approval park would leave this at 0 until the backstop.
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(1);

    const runIds = parseTriggerFireRunIds(wired.supervisorToChild.flushed());
    expect(runIds).toEqual(["run_deployment-x"]);
    await wired.supervisor.shutdown();
  });

  test("long-lived: subsequent messages fire signal.deliver after park.notify", async () => {
    const baseDir = await makeTempDir("supervisor-long-lived-signal-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    const wired = await spawnWithRunStart({
      baseDir,

      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });

    wired.mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("msg-1"),
    );

    // Wait for trigger.fire to land before parking, then send park.notify so the unified-dispatch path can complete markConsumed.
    await waitForTriggerFireRunIds(wired.supervisorToChild, 1);

    const address = "run_deployment-x@example.com";

    // Child parks on input signal.
    await wired.childSender.send({
      type: "park.notify",
      data: {
        runId: "run_deployment-x",
        correlationId: "corr-input-1",
        parkKind: "input",
      },
    });

    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 1,
    );
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(1);

    wired.mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("msg-2"),
    );

    // Wait for msg-2's signal.deliver (arming the durable-consume watcher); markConsumed holds until the run re-parks or terminates.
    await waitForUpstreamPayload(wired.supervisorToChild, "signal.deliver");
    // The signal is written but not taken up, so msg-2's markConsumed is held; only msg-1 is
    // consumed. The wait above is the positive barrier: a regressed markConsumed would have run
    // by now, and nothing later signals the take-up. The forbidden event is a premature consume;
    // a slow worker weakens but cannot invert it.
    await new Promise((r) => setTimeout(r, 25));
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(1);

    await wired.childSender.send({
      type: "terminal.event",
      data: {
        runId: "run_deployment-x",
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });

    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 2,
    );
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(2);

    const signals = parseSignalDelivers(wired.supervisorToChild.flushed());
    expect(signals.length).toBeGreaterThanOrEqual(1);
    const firstSignal = signals[0];
    if (firstSignal === undefined) throw new Error("unreachable");
    expect(firstSignal.signalName).toBe(signalName("corr-input-1"));
    expect(firstSignal.signalId).toBeTruthy();
    await wired.supervisor.shutdown();
  });

  test("long-lived: messages before park.notify are queued and flushed", async () => {
    const baseDir = await makeTempDir("supervisor-long-lived-queue-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    const wired = await spawnWithRunStart({
      baseDir,

      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });

    wired.mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("msg-1"),
    );

    // The trigger is forwarded but no park or terminal has been sent, so nothing is consumed until the park below.
    await waitForTriggerFireRunIds(wired.supervisorToChild, 1);

    const address = "run_deployment-x@example.com";
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(0);

    // Deliver second message BEFORE child parks.
    wired.mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("msg-2"),
    );

    // Wait for msg-2 to reach the inbox; while the loop is held on msg-1's wait that enqueue is all that can happen to it.
    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).inbox.size >= 1,
    );

    // Still no signal.deliver and still nothing consumed because the channel is unknown and the first run has not parked.
    let signals = parseSignalDelivers(wired.supervisorToChild.flushed());
    expect(signals.length).toBe(0);
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(0);

    // Now child parks.
    await wired.childSender.send({
      type: "park.notify",
      data: {
        runId: "run_deployment-x",
        correlationId: "corr-input-1",
        parkKind: "input",
      },
    });

    // Wait for the queued message to reach the child stream.
    await waitForUpstreamPayload(wired.supervisorToChild, "signal.deliver");
    signals = parseSignalDelivers(wired.supervisorToChild.flushed());
    expect(signals.length).toBeGreaterThanOrEqual(1);
    const firstSignal = signals[0];
    if (firstSignal === undefined) throw new Error("unreachable");
    expect(firstSignal.signalName).toBe(signalName("corr-input-1"));

    // msg-1 is consumed off its park; msg-2's signal is not taken up, so its markConsumed is
    // held. The wait above is the positive barrier: a regressed markConsumed would have run by
    // now, and nothing later signals the take-up. The forbidden event is msg-2 consuming early;
    // a slow worker weakens but cannot invert it.
    await new Promise((r) => setTimeout(r, 25));
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(1);

    // The signal.deliver above is observed, so its durable-consume watcher is armed; complete the resumed run so markConsumed for msg-2 releases.
    await wired.childSender.send({
      type: "terminal.event",
      data: {
        runId: "run_deployment-x",
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });

    // Both messages are consumed.
    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 2,
    );
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(2);
    await wired.supervisor.shutdown();
  });

  test("long-lived: drain() does not arm accumulators", async () => {
    const baseDir = await makeTempDir("supervisor-long-lived-drain-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    const armedStubs: { runId: string }[] = [];
    const factory: DrainTimeoutAccumulatorFactory = (opts) => {
      armedStubs.push({ runId: opts.runId });
      return createNoopDrainAccumulator();
    };

    const wired = await spawnWithRunStart({
      baseDir,

      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
      drainTimeoutAccumulatorFactory: factory,
    });

    wired.mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("msg-1"),
    );

    // Park the run (after the trigger that names it) so markConsumed proceeds and the run enters
    // the parked state drain skips; waiting on the trigger rather than a duration avoids a
    // load-dependent race.
    await waitForTriggerFireRunIds(wired.supervisorToChild, 1);
    await wired.childSender.send({
      type: "park.notify",
      data: {
        runId: "run_deployment-x",
        correlationId: "corr-input-1",
        parkKind: "input",
      },
    });

    const address = "run_deployment-x@example.com";
    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 1,
    );
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(1);

    await wired.supervisor.drain({ deadlineMs: 5_000 });

    // Drain skips accumulators for runs that have parked (runtime-determined long-lived state).
    expect(armedStubs.length).toBe(0);
    await wired.supervisor.shutdown();
  });

  test("long-lived: grants barrier failure consumes message without firing trigger", async () => {
    const baseDir = await makeTempDir("supervisor-long-lived-barrier-");
    const onRunStart: WorkflowSupervisorBindings["onRunStart"] = async () => {
      throw new Error("synthetic grants-barrier failure");
    };

    const wired = await spawnWithRunStart({
      baseDir,

      onRunStart,
    });

    wired.mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("msg-1"),
    );

    // A failed barrier fans out a synthesized RunFailed to the run's own watcher, so the dispatch
    // reaches markConsumed without the child reporting anything: nothing here has to drive the
    // mock child.
    const address = "run_deployment-x@example.com";
    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 1,
    );
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(1);

    const runIds = parseTriggerFireRunIds(wired.supervisorToChild.flushed());
    expect(runIds).toEqual([]);
    await wired.supervisor.shutdown();
  });

  test("long-lived: mail after terminal is rejected without another trigger.fire", async () => {
    const baseDir = await makeTempDir("supervisor-long-lived-terminal-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    const wired = await spawnWithRunStart({
      baseDir,

      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });

    // First message triggers the run.
    wired.mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("msg-1"),
    );

    // Drive the run to terminal so markConsumed proceeds; the frame follows the forwarded trigger and cannot arrive before the watcher subscribes.
    await waitForTriggerFireRunIds(wired.supervisorToChild, 1);
    await wired.childSender.send({
      type: "terminal.event",
      data: {
        runId: "run_deployment-x",
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });

    const address = "run_deployment-x@example.com";
    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 1,
    );
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(1);

    // Second message arrives after the deployment's one top-level run terminated. It must be durably rejected, not treated as a new run.
    wired.mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("msg-2"),
    );

    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 2,
    );

    const runIds = parseTriggerFireRunIds(wired.supervisorToChild.flushed());
    expect(runIds).toEqual(["run_deployment-x"]);
    expect(
      [...wired.inboxPrimitives.snapshot(address).consumed.values()].some(
        (entry) => entry.rejection?.code === "workflow_run_terminal",
      ),
    ).toBe(true);

    // No signal.deliver because the run never parked.
    const signals = parseSignalDelivers(wired.supervisorToChild.flushed());
    expect(signals.length).toBe(0);

    await wired.supervisor.shutdown();
  });

  test("a recovery-window mail is rejected when the live run terminates before parking", async () => {
    const baseDir = await makeTempDir("supervisor-recovery-terminal-race-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const wired = await spawnWithRunStart({
      baseDir,
      onRunStart: async () =>
        assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        }),
    });
    const address = "run_deployment-x@example.com";

    // Model restart recovery: the child owns the durable live run, but has not yet re-emitted an input park/correlation for it.
    await wired.childSender.send({
      type: "resumed.runs",
      data: { runIds: ["run_deployment-x"] },
    });
    wired.mailBus.deliver(address, new TextEncoder().encode("waiting mail"));
    // The loop moves the mail to processing and waits for the live run: no trigger or signal is
    // forwarded from that branch, so the entry stays put until the terminal below.
    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).processing.size >= 1,
    );
    expect(parseTriggerFireRunIds(wired.supervisorToChild.flushed())).toEqual(
      [],
    );
    expect(parseSignalDelivers(wired.supervisorToChild.flushed())).toEqual([]);

    await wired.childSender.send({
      type: "terminal.event",
      data: {
        runId: "run_deployment-x",
        seq: 1,
        kind: "RunCompleted",
        at: "test",
      },
    });

    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 1,
    );
    expect(parseTriggerFireRunIds(wired.supervisorToChild.flushed())).toEqual(
      [],
    );
    expect(
      [...wired.inboxPrimitives.snapshot(address).consumed.values()][0]
        ?.rejection?.code,
    ).toBe("workflow_run_terminal");

    await wired.supervisor.shutdown();
  });

  test("a clean deployment with grants but no events fires its top-level run", async () => {
    const baseDir = await makeTempDir("supervisor-clean-run-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    const wired = await spawnWithRunStart({
      baseDir,
      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });

    // `grants.json` may be staged before delivery, but without an event log this is still the deployment's one allowed first fire.
    wired.mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("msg-1"),
    );

    await waitForTriggerFireRunIds(wired.supervisorToChild, 1);

    const runIds = parseTriggerFireRunIds(wired.supervisorToChild.flushed());
    expect(runIds.length).toBe(1);
    expect(runIds[0]).toBe("run_deployment-x");
    await wired.supervisor.shutdown();
  });

  test("a terminal durable log is never cleared or fired after supervisor restart", async () => {
    const baseDir = await makeTempDir("supervisor-terminal-restart-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    const runEventsDir = path.join(
      baseDir,
      "workflow-run",
      "run_deployment-x",
      "runs",
      "run_deployment-x",
      "events",
    );
    await fs.mkdir(runEventsDir, { recursive: true });
    await fs.writeFile(
      path.join(runEventsDir, "0.json"),
      JSON.stringify({ type: "RunStarted", seq: 0 }),
    );
    await fs.writeFile(
      path.join(runEventsDir, "1.json"),
      JSON.stringify({ type: "RunCompleted", seq: 1 }),
    );

    const wired = await spawnWithRunStart({
      baseDir,
      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });

    const address = "run_deployment-x@example.com";
    wired.mailBus.deliver(address, new TextEncoder().encode("msg-1"));

    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 1,
    );
    expect(parseTriggerFireRunIds(wired.supervisorToChild.flushed())).toEqual(
      [],
    );
    expect(
      [...wired.inboxPrimitives.snapshot(address).consumed.values()].some(
        (entry) => entry.rejection?.code === "workflow_run_terminal",
      ),
    ).toBe(true);

    await wired.supervisor.shutdown();
  });

  test("an instant park during the pre-wait window still advances the dispatch loop", async () => {
    const baseDir = await makeTempDir("supervisor-instant-park-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    const wired = await spawnWithRunStart({
      baseDir,
      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });

    const address = "run_deployment-x@example.com";
    wired.mailBus.deliver(address, new TextEncoder().encode("msg-1"));

    // Park as soon as the run exists; waiting on the forwarded trigger puts the park in the arming window instead of betting on a duration.
    await waitForTriggerFireRunIds(wired.supervisorToChild, 1);
    await wired.childSender.send({
      type: "park.notify",
      data: {
        runId: "run_deployment-x",
        correlationId: "corr-input-1",
        parkKind: "input",
      },
    });

    // The loop must proceed and consume msg-1 despite the lost park wake.
    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 1,
    );
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(1);

    await wired.supervisor.shutdown();
  });

  test("two mails to a parked run each deliver on the run's fresh correlation", async () => {
    const baseDir = await makeTempDir("supervisor-fresh-corr-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    const wired = await spawnWithRunStart({
      baseDir,
      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });
    const address = "run_deployment-x@example.com";
    // Neither wait carries a deadline (a timed-out wait used to surface as whatever assertion read the state next); lane timeout is the failsafe.
    const waitConsumed = (n: number) =>
      wired.inboxPrimitives.awaitState(
        () => wired.inboxPrimitives.snapshot(address).consumed.size >= n,
      );
    const waitSignals = (n: number) =>
      waitForUpstreamPayload(wired.supervisorToChild, "signal.deliver", () => {
        return (
          parseSignalDelivers(wired.supervisorToChild.flushed()).length >= n
        );
      });

    // Trigger msg-1 and park on corr-1 after the forwarded trigger; a fixed sleep under load let the notification arrive first, stranding mail.
    wired.mailBus.deliver(address, new TextEncoder().encode("msg-1"));
    await waitForTriggerFireRunIds(wired.supervisorToChild, 1);
    await wired.childSender.send({
      type: "park.notify",
      data: {
        runId: "run_deployment-x",
        correlationId: "corr-input-1",
        parkKind: "input",
      },
    });
    await waitConsumed(1);
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(1);

    // Both mails must deliver on the run's CURRENT correlation: msg-2 on corr-1, msg-3 on corr-2 after the re-park, never the stale corr-1.
    wired.mailBus.deliver(address, new TextEncoder().encode("msg-2"));
    wired.mailBus.deliver(address, new TextEncoder().encode("msg-3"));

    await waitSignals(1);
    await wired.childSender.send({
      type: "park.notify",
      data: {
        runId: "run_deployment-x",
        correlationId: "corr-input-2",
        parkKind: "input",
      },
    });
    await waitConsumed(2);

    await waitSignals(2);
    await wired.childSender.send({
      type: "terminal.event",
      data: {
        runId: "run_deployment-x",
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });
    await waitConsumed(3);

    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(3);
    const signals = parseSignalDelivers(wired.supervisorToChild.flushed());
    expect(signals.length).toBe(2);
    expect(signals[0]?.signalName).toBe(signalName("corr-input-1"));
    expect(signals[1]?.signalName).toBe(signalName("corr-input-2"));

    await wired.supervisor.shutdown();
  });

  test("a park-registered run keeps its channel: a mail routes as signal, not a fresh trigger", async () => {
    const baseDir = await makeTempDir("supervisor-resumed-order-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    const wired = await spawnWithRunStart({
      baseDir,
      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });
    const address = "run_deployment-x@example.com";

    // park.notify with no prior trigger.fire (the reconnect/resumed shape): the handler must add
    // cohortRunIds BEFORE the channel, or routing hygiene deletes it as stale and the next mail
    // starts a fresh run.
    await wired.childSender.send({
      type: "park.notify",
      data: {
        runId: "run_deployment-x",
        correlationId: "corr-input-1",
        parkKind: "input",
      },
    });

    wired.mailBus.deliver(address, new TextEncoder().encode("msg-1"));

    await waitForUpstreamPayloads(wired.supervisorToChild, "signal.deliver", 1);
    const signals = parseSignalDelivers(wired.supervisorToChild.flushed());
    expect(signals.length).toBe(1);
    expect(signals[0]?.signalName).toBe(signalName("corr-input-1"));
    expect(
      parseTriggerFireRunIds(wired.supervisorToChild.flushed()).length,
    ).toBe(0);

    // Release the durable-consume wait so shutdown is clean.
    await wired.childSender.send({
      type: "terminal.event",
      data: {
        runId: "run_deployment-x",
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });
    await wired.supervisor.shutdown();
  });

  test("a mail resumes a parked run with the decoded Mail, not raw MIME", async () => {
    const baseDir = await makeTempDir("supervisor-signal-text-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const wired = await spawnWithRunStart({
      baseDir,
      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });
    const address = "run_deployment-x@example.com";

    // Park the run so the next mail routes as signal.deliver.
    await wired.childSender.send({
      type: "park.notify",
      data: {
        runId: "run_deployment-x",
        correlationId: "corr-input-1",
        parkKind: "input",
      },
    });

    // Deliver a real inbound MIME message whose body is "hello turn two".
    const mail = new TextEncoder().encode(
      "Content-Type: text/plain\r\n\r\nhello turn two",
    );
    wired.mailBus.deliver(address, mail);

    await waitForUpstreamPayloads(wired.supervisorToChild, "signal.deliver", 1);
    const signals = parseSignalDelivers(wired.supervisorToChild.flushed());
    expect(signals.length).toBe(1);
    // The frame carries the decoded Mail, resolved at dispatch, not the raw base64 MIME envelope; the text/plain inline text is the body.
    const payload = signals[0]?.payload;
    if (!isMail(payload)) throw new Error("signal payload is not a Mail");
    expect(payload.parts).toHaveLength(1);
    expect(payload.parts[0]?.contentType).toBe("text/plain");
    expect(payload.parts[0]?.text).toBe("hello turn two");

    await wired.childSender.send({
      type: "terminal.event",
      data: {
        runId: "run_deployment-x",
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });
    await wired.supervisor.shutdown();
  });

  test("deliverSignal ships its structured payload through unchanged", async () => {
    const baseDir = await makeTempDir("supervisor-deliversignal-passthrough-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const wired = await spawnWithRunStart({
      baseDir,
      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });

    // A hub-originated signal's structured payload must reach the child verbatim; mail-input extraction is the dispatch loop's concern only.
    await wired.supervisor.deliverSignal({
      runId: "run_deployment-x",
      signalName: "go",
      signalId: "sig-1",
      payload: { resumed: true, n: 7 },
    });

    const signals = parseSignalDelivers(wired.supervisorToChild.flushed());
    expect(signals.length).toBe(1);
    expect(signals[0]?.payload).toEqual({ resumed: true, n: 7 });

    await wired.supervisor.shutdown();
  });

  test("a malformed turn-2 mail is dropped and consumed, not poison-looped", async () => {
    const baseDir = await makeTempDir("supervisor-poison-mail-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const wired = await spawnWithRunStart({
      baseDir,
      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });
    const address = "run_deployment-x@example.com";

    // Park the run so a mail routes as signal.deliver.
    await wired.childSender.send({
      type: "park.notify",
      data: {
        runId: "run_deployment-x",
        correlationId: "corr-input-1",
        parkKind: "input",
      },
    });

    // A mail whose only leaf part carries a malformed body under a recognised content-transfer-
    // encoding: decodeMail throws deterministically. It must be dropped and CONSUMED, not
    // thrown-and-replayed forever.
    const bad = new TextEncoder().encode(
      "Content-Type: text/plain\r\nContent-Transfer-Encoding: base64\r\n\r\n!!! not base64 !!!",
    );
    wired.mailBus.deliver(address, bad);

    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 1,
    );
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(1);
    expect(wired.inboxPrimitives.snapshot(address).processing.size).toBe(0);
    // No signal was delivered for the poison mail.
    expect(parseSignalDelivers(wired.supervisorToChild.flushed()).length).toBe(
      0,
    );

    // The run survived on its correlation: a subsequent VALID mail resumes it.
    const good = new TextEncoder().encode(
      "Content-Type: text/plain\r\n\r\nhello",
    );
    wired.mailBus.deliver(address, good);
    await waitForUpstreamPayloads(wired.supervisorToChild, "signal.deliver", 1);
    const signals = parseSignalDelivers(wired.supervisorToChild.flushed());
    expect(signals.length).toBe(1);
    expect(signals[0]?.signalName).toBe(signalName("corr-input-1"));
    const goodPayload = signals[0]?.payload;
    if (!isMail(goodPayload)) throw new Error("signal payload is not a Mail");
    expect(goodPayload.parts[0]?.text).toBe("hello");

    await wired.childSender.send({
      type: "terminal.event",
      data: {
        runId: "run_deployment-x",
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });
    await wired.supervisor.shutdown();
  });

  test("a transient commit failure leaves the mail reclaimable and the loop alive", async () => {
    const baseDir = await makeTempDir("supervisor-transient-commit-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    // The injected fault is the event this test waits on: the dispatch iteration it fails is the one the assertions describe.
    const partsFaults = createChangeNotifier();
    let partsFaultCount = 0;
    const wired = await spawnWithRunStart({
      baseDir,
      // Fail ONLY the mail-parts commit -- credential and cancel writes still succeed -- to model a transient substrate fault reaching prepareMail.
      beforeWrite: ({ preservePrefix }) => {
        if (preservePrefix.includes("/parts/")) {
          partsFaultCount += 1;
          partsFaults.notify();
          throw new Error("substrate boom (transient)");
        }
      },
      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });
    const address = "run_deployment-x@example.com";

    // Park the run so a mail routes as signal.deliver.
    await wired.childSender.send({
      type: "park.notify",
      data: {
        runId: "run_deployment-x",
        correlationId: "corr-input-1",
        parkKind: "input",
      },
    });

    // A well-formed mail whose parts commit throws a NON-deterministic fault. Unlike a malformed
    // mail (deterministic drop), it must NOT be consumed -- it stays reclaimable in processing --
    // and no signal is delivered.
    const good = new TextEncoder().encode(
      "Content-Type: text/plain\r\n\r\nhello",
    );
    wired.mailBus.deliver(address, good);

    // The fault is raised after the dequeue and before the send; the loop parks without replaying the entry, settling the three states below.
    await partsFaults.until(() => partsFaultCount >= 1);

    const snap = wired.inboxPrimitives.snapshot(address);
    expect(snap.consumed.size).toBe(0);
    expect(snap.processing.size).toBe(1);
    expect(parseSignalDelivers(wired.supervisorToChild.flushed()).length).toBe(
      0,
    );

    // The loop survived the throw rather than crashing: shutdown completes.
    await wired.supervisor.shutdown();
  });

  test("a markConsumed failure leaves the mail reclaimable and the loop alive", async () => {
    const baseDir = await makeTempDir("supervisor-markconsumed-fatal-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const memoryInbox = createMemoryInboxPrimitives();
    let failMarkConsumed = true;
    // The injected failure is the event this test waits on: the refusal is what leaves the entry in `processing`, and nothing else reports it.
    const markConsumedFaults = createChangeNotifier();
    let markConsumedFaultCount = 0;
    const failingInbox: MemoryInboxPrimitives = {
      ...memoryInbox,
      markConsumed: async (...args) => {
        if (failMarkConsumed) {
          markConsumedFaultCount += 1;
          markConsumedFaults.notify();
          throw new Error("injected markConsumed failure");
        }
        return memoryInbox.markConsumed(...args);
      },
    };
    const wired = await spawnWithRunStart({
      baseDir,
      inboxPrimitives: failingInbox,
      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });
    const address = "run_deployment-x@example.com";

    // Drive the run to terminal so dispatch reaches the throwing markConsumed; the frame follows the forwarded trigger, after subscription.
    wired.mailBus.deliver(address, new TextEncoder().encode("msg-1"));
    await waitForTriggerFireRunIds(wired.supervisorToChild, 1);
    await wired.childSender.send({
      type: "terminal.event",
      data: {
        runId: "run_deployment-x",
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });

    // The failure propagates into the dispatch fault handler rather than being swallowed: the
    // mail is NOT recorded consumed -- it stays in processing/, reclaimable -- and the dispatch
    // loop survives the throw.
    await markConsumedFaults.until(() => markConsumedFaultCount >= 1);
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(0);
    expect(wired.inboxPrimitives.snapshot(address).processing.size).toBe(1);

    // Once markConsumed recovers, a second mail is consumed; the run is terminal, so msg-2 is
    // rejected, and the recovered markConsumed recording that rejection is the survival asserted.
    failMarkConsumed = false;
    wired.mailBus.deliver(address, new TextEncoder().encode("msg-2"));
    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 1,
    );
    expect(
      wired.inboxPrimitives.snapshot(address).consumed.size,
    ).toBeGreaterThanOrEqual(1);

    await wired.supervisor.shutdown();
  });

  test("a mail enqueued during a dispatch iteration is picked up, not stranded", async () => {
    const baseDir = await makeTempDir("supervisor-lost-wake-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const address = "run_deployment-x@example.com";
    // The settleable bus returns the handler's promise, so the deliver below lands inside this iteration, awaiting the handler's last wake.
    const mailBus = createSettleableMailBus();
    const memoryInbox = createMemoryInboxPrimitives();
    let armed = true;
    const racingInbox: MemoryInboxPrimitives = {
      ...memoryInbox,
      dequeueToProcessing: async (...args) => {
        if (armed) {
          armed = false;
          // Model a mail landing during this iteration: deliver it (waking the loop and swapping the
          // wake promise), then report the inbox empty. With the capture-after bug the wake is lost;
          // capture-before catches it.
          await mailBus.settle(address, new TextEncoder().encode("msg-1"));
          return null;
        }
        return memoryInbox.dequeueToProcessing(...args);
      },
    };
    const wired = await spawnWithRunStart({
      baseDir,
      mailBus,
      inboxPrimitives: racingInbox,
      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });

    // The forwarded trigger proves the wake was not lost: the loop re-dequeued msg-1 and fired its run; drive it to terminal for consumption.
    await waitForTriggerFireRunIds(wired.supervisorToChild, 1);
    await wired.childSender.send({
      type: "terminal.event",
      data: {
        runId: "run_deployment-x",
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });
    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 1,
    );
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(1);

    await wired.supervisor.shutdown();
  });

  test("a terminal that lands during the trigger's forward window releases the wait", async () => {
    const baseDir = await makeTempDir("supervisor-subscribe-before-fire-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const wired = await spawnWithRunStart({
      baseDir,
      onRunStart: async () => {
        return assembleCredentialsSnapshot({
          repoStore: createStubRepoStore({ baseDir }),
          principal: { kind: "supervisor" },
          stepOrder: ["step-1"],
          anchorRunId: "run_deployment-x",
          deriveStepAddress: ({ runId, stepId }) =>
            `${runId}-${stepId}@example.com`,
        });
      },
    });
    const address = "run_deployment-x@example.com";
    wired.mailBus.deliver(address, new TextEncoder().encode("msg-1"));

    await waitForTriggerFireRunIds(wired.supervisorToChild, 1);
    // The watcher is subscribed before trigger.fire, so an immediate terminal frame cannot be lost between the forward and entering wait.
    await wired.childSender.send({
      type: "terminal.event",
      data: {
        runId: "run_deployment-x",
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });

    // The wait releases and the mail is consumed WITHOUT the (minutes-long) backstop firing.
    await wired.inboxPrimitives.awaitState(
      () => wired.inboxPrimitives.snapshot(address).consumed.size >= 1,
    );
    expect(wired.inboxPrimitives.snapshot(address).consumed.size).toBe(1);

    await wired.supervisor.shutdown();
  });
});

describe("assembleCredentialsSnapshot", () => {
  test("enumerates each step's agent-state repo and pins per-step grants by hash", async () => {
    const baseDir = await makeTempDir("supervisor-creds-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "d1", stepId: "alpha" }),
      [{ resource: "alpha-thing", action: "read" }],
    );
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "d1", stepId: "beta" }),
      [
        { resource: "beta-thing", action: "read" },
        { resource: "beta-thing", action: "write" },
      ],
    );
    const repoStore = createStubRepoStore({ baseDir });
    const snapshot = await assembleCredentialsSnapshot({
      repoStore,
      principal: { kind: "supervisor" },
      stepOrder: ["alpha", "beta"],
      anchorRunId: "d1",
      deriveStepAddress: ({ runId, stepId }) =>
        `${runId}-${stepId}@example.com`,
    });
    expect(snapshot.steps).toHaveLength(2);
    expect(snapshot.steps[0]?.stepId).toBe("alpha");
    expect(snapshot.steps[0]?.address).toBe("d1-alpha@example.com");
    expect(snapshot.steps[0]?.grants).toEqual([
      { resource: "alpha-thing", action: "read" },
    ]);
    expect(snapshot.steps[0]?.contentHash).toBe(
      await hashGrants([{ resource: "alpha-thing", action: "read" }]),
    );
    expect(snapshot.steps[1]?.stepId).toBe("beta");
    expect(snapshot.steps[1]?.grants).toHaveLength(2);
    expect(snapshot.steps[0]?.contentHash).not.toBe(
      snapshot.steps[1]?.contentHash,
    );
  });

  test("treats a missing per-step grants file as an empty grant array", async () => {
    const baseDir = await makeTempDir("supervisor-creds-empty-");
    const repoStore = createStubRepoStore({ baseDir });
    const snapshot = await assembleCredentialsSnapshot({
      repoStore,
      principal: { kind: "supervisor" },
      stepOrder: ["solo"],
      anchorRunId: "d2",
      deriveStepAddress: ({ runId }) => `${runId}@example.com`,
    });
    expect(snapshot.steps).toHaveLength(1);
    expect(snapshot.steps[0]?.grants).toEqual([]);
    expect(snapshot.steps[0]?.contentHash).toBe(await hashGrants([]));
  });

  test("a malformed grants file fails loudly rather than silently treating it as empty", async () => {
    const baseDir = await makeTempDir("supervisor-creds-bad-");
    const repoId = defaultStepRepoId({ runId: "d3", stepId: "s" });
    const dir = path.join(baseDir, repoId.kind, repoId.id);
    await fs.mkdir(path.join(dir, "state"), { recursive: true });
    await fs.writeFile(path.join(dir, STEP_GRANTS_PATH), "not json");
    const repoStore = createStubRepoStore({ baseDir });
    await expect(
      assembleCredentialsSnapshot({
        repoStore,
        principal: { kind: "supervisor" },
        stepOrder: ["s"],
        anchorRunId: "d3",
        deriveStepAddress: () => "d3-s@example.com",
      }),
    ).rejects.toThrow(/is not valid JSON/);
  });
});

describe("commitCancelRequested (low-level)", () => {
  test("attaches the signed payload to the on-disk CancelRequested blob", async () => {
    const baseDir = await makeTempDir("cancel-signing-");
    let observedFiles: Record<string, string | Uint8Array> | undefined;
    const repoStore = createStubRepoStore({
      baseDir,
      onWrite: ({ files }) => {
        observedFiles = files;
      },
    });
    const signed = await commitCancelRequested({
      substrate: repoStore,
      repoId: { kind: "workflow-run", id: "deploy" },
      ref: "refs/heads/main",
      anchorRunId: "deploy",
      runId: "r1",
      origin: "self",
      reason: "tests pass",
      at: "2026-01-01T00:00:00.000Z",
      signAsPrincipal: async (kind, payload) => {
        expect(kind).toBe("supervisor");
        const sig = new Uint8Array(64);
        // Embed the payload length so we can verify it was signed.
        sig[0] = payload.length & 0xff;
        return { sig, principalKind: "supervisor" };
      },
    });
    expect(signed.commitSha).toBe("deadbeefcafef00d");
    expect(signed.seq).toBe(1);
    if (observedFiles === undefined) {
      throw new Error("writeTreePreservingPrefix was not invoked");
    }
    const entry = Object.entries(observedFiles).find(([k]) =>
      k.endsWith("/events/1.json"),
    );
    if (entry === undefined) {
      throw new Error("no events/1.json entry observed in commit");
    }
    const [, blobBytes] = entry;
    const blobJson =
      typeof blobBytes === "string"
        ? blobBytes
        : new TextDecoder().decode(blobBytes);
    const blob = readCancelRequestedBlob(blobJson);
    expect(blob.type).toBe("CancelRequested");
    expect(blob.origin).toBe("self");
    expect(blob.reason).toBe("tests pass");
    expect(blob.signature.principalKind).toBe("supervisor");
    expect(blob.signature.sig.length).toBe(128);
    expect(
      resumeFromLog("r1", [
        {
          kind: "CancelRequested",
          seq: blob.seq,
          origin: "self",
          reason: blob.reason,
          at: "2026-01-01T00:00:00.000Z",
        },
      ]).phase,
    ).toBe("cancelling");
  });
});

describe("IPC integration smoke", () => {
  test("a sender/receiver round-trip on the synthetic streams used by the supervisor tests", async () => {
    // Sanity check that the in-memory stream helpers do not regress the IPC contract the supervisor tests rely on.
    const upstream = createMemoryNdjsonStream();
    const downstream = createMemoryNdjsonStream();
    const keyPair = await generateKeyPair();
    const channelId = generateChannelId();
    const sender = createControlChannelSender({
      privateKeySeed: keyPair.privateKey,
      channelId,
      writer: upstream.writer,
    });
    await sender.send({
      type: "ready",
      data: {
        childPid: 1,
        childPublicKey: hexEncode(keyPair.publicKey),
      },
    });
    expect(upstream.flushed()).toHaveLength(1);

    const eventStream = createMemoryFrameStream();
    const hmacKey = generateHmacKey();
    const eventSender = createEventChannelSender({
      hmacKey,
      channelId,
      // The sender terminates each frame itself, so its bytes go through the writer verbatim; `inject` would append a second terminator.
      writer: eventStream.writer,
    });
    await eventSender.send({
      type: "message.run.started",
      seq: 1,
      data: {
        messageId: "m",
        messageRunId: "r",
        receivedAt: 1,
      },
    });
    eventStream.close();

    // Verify the receiver pipeline picks up the framed bytes.
    const crashes: string[] = [];
    const recvIter = receiveControlChannel({
      publicKey: keyPair.publicKey,
      channelId,
      reader: {
        read(): AsyncIterableIterator<string> {
          return upstream.reader.read();
        },
      },
      onCrash: (reason) => crashes.push(reason),
    });
    upstream.close();
    let firstPayload: { type: string } | undefined;
    for await (const payload of recvIter) {
      firstPayload = { type: payload.type };
      break;
    }
    expect(firstPayload?.type).toBe("ready");
    expect(crashes).toHaveLength(0);
    void downstream;
    void hexDecode;
  });
});

describe("supervisor inbox FIFO dispatch loop", () => {
  async function buildFifoTestFixture(opts: {
    label: string;
    inbox: InboxPrimitives;
    deriveMailAuditRef?: (
      messageId: string,
      rawMessage: Uint8Array,
    ) => { store: string; path: string };
  }) {
    const baseDir = await makeTempDir(opts.label);
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    let observedEnv: Record<string, string> | undefined;
    // Scoped here, not to the file: `first()` must resolve with THIS fixture's spawn, not whichever spawn happened earliest in the run.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      spawnObserver.record(env);
      const handle: SubprocessHandle = {
        pid: 11111,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
      return handle;
    };
    const mailBus = createMockMailBus();
    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({ sig: new Uint8Array(64), principalKind: "supervisor" }),
      mailBus,
      inboxPrimitives: opts.inbox,
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
      ...(opts.deriveMailAuditRef !== undefined
        ? { deriveMailAuditRef: opts.deriveMailAuditRef }
        : {}),
    };
    const supervisor = supervisors.track(createWorkflowSupervisor(bindings));
    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      warmKeep: false,

      onInferenceEvent: () => undefined,
    });
    observedEnv = await spawnObserver.first();
    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeyPair.privateKey,
      channelId,
      writer: {
        write(line: string) {
          childToSupervisor.inject(line);
        },
      },
    });
    await mailBus.awaitRegistered("run_deployment-x@example.com");
    await childSender.send({
      type: "ready",
      data: {
        childPid: 11111,
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
    await spawnPromise;
    return {
      supervisor,
      mailBus,
      supervisorToChild,
      childSender,
    };
  }

  test("default deriveMailAuditRef stamps `in-process` store on enqueued envelopes", async () => {
    const inbox = createMemoryInboxPrimitives();
    const { supervisor, mailBus } = await buildFifoTestFixture({
      label: "fifo-default-audit-",
      inbox,
    });
    mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("audit-default-1"),
    );
    // Wait for the enqueue across any claim-check substate: the loop may pull the entry into
    // processing before the assertion fires, and the entry never leaves all three at once.
    await inbox.awaitState(() => {
      const snap = inbox.snapshot("run_deployment-x@example.com");
      return (
        snap.inbox.size > 0 ||
        snap.processing.size > 0 ||
        snap.consumed.size > 0
      );
    });
    const snapshot = inbox.snapshot("run_deployment-x@example.com");
    const all = [
      ...snapshot.consumed.values(),
      ...snapshot.processing.values(),
      ...snapshot.inbox.values(),
    ];
    expect(all.length).toBeGreaterThanOrEqual(1);
    const first = all[0];
    if (first === undefined) throw new Error("unreachable");
    expect(first.mailAuditRef.store).toBe("in-process");
    expect(first.mailAuditRef.path.length).toBeGreaterThan(0);
    await supervisor.shutdown();
  });

  test("deriveMailAuditRef override is invoked with messageId and stamps the envelope", async () => {
    const inbox = createMemoryInboxPrimitives();
    const observed: { messageId: string; len: number }[] = [];
    // The override being invoked is the event; the poll it replaces re-read the array on a timer until something landed in it.
    const observedChanges = createChangeNotifier();
    const { supervisor, mailBus } = await buildFifoTestFixture({
      label: "fifo-override-audit-",
      inbox,
      deriveMailAuditRef: (messageId, rawMessage) => {
        observed.push({ messageId, len: rawMessage.byteLength });
        observedChanges.notify();
        return {
          store: "test-audit",
          path: `deployment-x/${messageId}`,
        };
      },
    });
    const payload = new TextEncoder().encode("audit-override-1");
    mailBus.deliver("run_deployment-x@example.com", payload);
    await observedChanges.until(() => observed.length > 0);
    expect(observed.length).toBe(1);
    const observedEntry = observed[0];
    if (observedEntry === undefined) throw new Error("unreachable");
    expect(observedEntry.len).toBe(payload.byteLength);
    expect(observedEntry.messageId.length).toBeGreaterThan(0);
    const overrideSnapshot = inbox.snapshot("run_deployment-x@example.com");
    const allEntries = [
      ...overrideSnapshot.inbox.values(),
      ...overrideSnapshot.processing.values(),
      ...overrideSnapshot.consumed.values(),
    ];
    expect(allEntries.length).toBeGreaterThanOrEqual(1);
    const first = allEntries[0];
    if (first === undefined) throw new Error("unreachable");
    expect(first.mailAuditRef.store).toBe("test-audit");
    expect(first.mailAuditRef.path).toBe(
      `deployment-x/${observedEntry.messageId}`,
    );
    await supervisor.shutdown();
  });

  test("two queued messages fire once, then reject the post-terminal message", async () => {
    const inbox = createMemoryInboxPrimitives();
    // Each dispatch is gated on a terminal.event the test mints; until then the loop sits on waitForRunTerminal for the forwarded run.
    const { supervisor, mailBus, supervisorToChild, childSender } =
      await buildFifoTestFixture({
        label: "fifo-serial-",
        inbox,
      });
    // Two messages. The first fires the run; the second waits behind its terminal gate and is then rejected.
    mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("serial-msg-A"),
    );
    mailBus.deliver(
      "run_deployment-x@example.com",
      new TextEncoder().encode("serial-msg-B"),
    );
    // Helper that pulls the runId carried on the first/next trigger.fire frame the supervisor wrote to the child stream.
    function triggerRunIds(): string[] {
      return parseTriggerFireRunIds(supervisorToChild.flushed());
    }
    // Wait for the first trigger.fire to land on the child stream.
    await waitForTriggerFireRunIds(supervisorToChild, 1);
    let firedIds = triggerRunIds();
    expect(firedIds.length).toBe(1);
    // Release the first run's terminal event. The dispatch loop proceeds to markConsumed and pulls the second message.
    const firstRunId = firedIds[0];
    if (firstRunId === undefined) throw new Error("first run not minted");
    await childSender.send({
      type: "terminal.event",
      data: {
        runId: firstRunId,
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });
    await inbox.awaitState(
      () => inbox.snapshot("run_deployment-x@example.com").consumed.size >= 2,
    );
    firedIds = triggerRunIds();
    expect(firedIds).toEqual([firstRunId]);
    const consumed = inbox.snapshot("run_deployment-x@example.com").consumed;
    expect(consumed.size).toBe(2);
    expect(
      [...consumed.values()].some(
        (entry) => entry.rejection?.code === "workflow_run_terminal",
      ),
    ).toBe(true);
    await supervisor.shutdown();
  });

  test("spawn-time replayProcessingToInbox moves orphaned processing entries back to inbox", async () => {
    const inbox = createMemoryInboxPrimitives();
    // Seed a `processing/` entry before the supervisor spawns. The entry should be moved back to `inbox/` during `spawn()`.
    const state = inbox.snapshot("run_deployment-x@example.com");
    state.processing.set("1000-msg-orphan", {
      messageId: "msg-orphan",
      receivedAt: 1000,
      mailAuditRef: { store: "test", path: "test/orphan" },
      // The recovered entry must carry decodable mail bytes: the dispatch loop decodes and commits them before forwarding the trigger.fire.
      rawMessage: base64Encode(
        new TextEncoder().encode("Content-Type: text/plain\r\n\r\norphan body"),
      ),
    });
    // In the unified-dispatch path markConsumed waits for the child to reach terminal or park before consuming the message.
    const { supervisor, supervisorToChild, childSender } =
      await buildFifoTestFixture({
        label: "fifo-replay-spawn-",
        inbox,
      });
    // Wait for the recovered entry's trigger.fire; decode rather than substring-match, so a payload naming "trigger.fire" cannot satisfy it.
    await waitForTriggerFireRunIds(supervisorToChild, 1);
    const triggerFires = supervisorToChild
      .flushed()
      .filter((f) => f.includes("trigger.fire"));
    expect(triggerFires.length).toBeGreaterThanOrEqual(1);

    // Drive the run to terminal so markConsumed can proceed. The wait above already observed the
    // forwarded trigger, and the terminal watcher is subscribed before that forward, so this
    // frame cannot be missed.
    await childSender.send({
      type: "terminal.event",
      data: {
        runId: "run_deployment-x",
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });

    // The processing entry was moved back to inbox during spawn, then dequeued by the dispatch
    // loop and forwarded as trigger.fire. After the child reaches terminal, markConsumed moves it
    // to consumed.
    await inbox.awaitState(
      () => inbox.snapshot("run_deployment-x@example.com").consumed.size >= 1,
    );
    const snapshot = inbox.snapshot("run_deployment-x@example.com");
    expect(snapshot.consumed.size).toBe(1);
    const consumedEntry = [...snapshot.consumed.values()][0];
    if (consumedEntry === undefined) throw new Error("unreachable");
    expect(consumedEntry.messageId).toBe("msg-orphan");
    await supervisor.shutdown();
  });
});
