// Durable conversation state for the warm single-step agent (design §3c,
// §4 Phase D1).
//
// A warm agent's conversation lives in its local isogit ContextStore,
// rooted per run/attempt -- so a child kill/respawn loses it. This module
// mirrors it durably into the workflow-run substrate (the single-writer
// proxy RepoStore, written through the supervisor) at a stable per-agent
// path (`agent-state/<agentKey>/`), and restores it into the local store
// before the rebuilt agent's reactor loads, so multi-turn continuity holds
// across runs and respawn.
//
// Two-tier on-disk layout (design §4 Phase D1): an append-only,
// bucket-sharded WAL plus a periodic compacted checkpoint.
//
//   agent-state/<agentKey>/
//     checkpoint.json        folded full snapshot (turns + metadata)
//     checkpoint.meta.json   { checkpointSeq, turnCount, tokenUsage,
//                              pendingOperations, connectorState }
//     wal/<bucket>/<seq>.json  per-boundary delta blobs
//
// Substrate-merge constraint (load-bearing, design §4 "Substrate-merge
// note"): `writeTreePreservingPrefix`'s merge sees only the DIRECT
// CHILDREN of `preservePrefix`, and `clearPrefix` recursively drops the
// whole prefix subtree before writing the returned set. A WAL blob sits
// two levels below `agent-state/<key>/`, so:
//   - WAL append uses `preservePrefix = agent-state/<key>/wal/<bucket>/`;
//     the bucket's existing blobs are direct children, so the merge
//     pre-image is exactly that bucket (no isogit side-read, other
//     buckets untouched).
//   - Checkpoint write + WAL truncate uses `preservePrefix =
//     agent-state/<key>/`; the merge returns only the checkpoint files
//     and no `wal/...` paths, so the recursive clearPrefix drops the
//     whole WAL subtree in the same atomic commit -- omitting the WAL
//     paths IS the truncate.
//
// The supervisor is the single writer and `agent-state/<key>/...` is
// disjoint from the run-event prefix (`runs/<runId>/events/`), so the
// conversation write never races nor clobbers the run-event log. The
// mirror stays synchronous at the run boundary -- this copy is the sole
// durable copy of the agent's per-turn output.
//
// Defensive: a restore that finds a checkpoint or WAL but cannot
// parse/replay it throws, and a mirror write failure surfaces -- a lost
// or corrupt conversation on respawn is a correctness failure, not a
// silently-fresh start.

import fs from "node:fs";
import path from "node:path";

import { type } from "arktype";

import { getLogger } from "@intx/log";
import { createConnectorRouter } from "@intx/harness";
import type { ConnectorReplyParts, RouteDecision } from "@intx/harness";
import { createIsogitStore } from "@intx/storage-isogit/node";
import type {
  Principal,
  RepoId,
  RepoStore,
} from "@intx/hub-sessions/substrate";
import { WORKFLOW_RUN_AGENT_STATE_PREFIX } from "@intx/hub-sessions/substrate";
import {
  ConnectorThreadState,
  TokenUsage,
  type AuditStore,
  type ContextStore,
  type ConversationTurn,
  type InboundMessage,
  type PendingOperation,
  type SendReceipt,
} from "@intx/types/runtime";

import { isErrnoNotFound } from "./supervisor/credentials";

const logger = getLogger(["sidecar", "workflow-child", "conversation-state"]);

const CHECKPOINT_FILE = "checkpoint.json";
const CHECKPOINT_META_FILE = "checkpoint.meta.json";
const WAL_DIR = "wal";

/**
 * Compaction interval: fold the WAL into a fresh checkpoint once it holds
 * this many turns since the last checkpoint. Bounds the WAL tail (and so
 * the restore-replay length). Measurement-tunable; fixed at 64.
 */
const CHECKPOINT_INTERVAL = 64;

/**
 * WAL directory fan-out bound: turn `seq` lives in bucket
 * `floor(seq / WAL_BUCKET_SIZE)`. Caps any single `wal/<bucket>/` tree so
 * no commit re-hashes a tree that grows with the conversation length.
 * Measurement-tunable; fixed at 128.
 */
const WAL_BUCKET_SIZE = 128;

/**
 * Non-turn reactor metadata stamped onto the checkpoint and every WAL
 * entry, so restore replays it without a separate metadata log.
 */
const SnapshotMetadata = type({
  pendingOperations: "unknown[]",
  tokenUsage: TokenUsage,
  connectorState: ConnectorThreadState.or("null"),
});

/**
 * On-disk shape of the compacted checkpoint blob at
 * `agent-state/<agentKey>/checkpoint.json`. Carries the folded turn
 * history plus the non-turn metadata. Validated on read because it crosses
 * back in from the substrate working tree; a corrupt checkpoint must
 * surface at the boundary, never be half-applied.
 */
const CheckpointSnapshot = type({
  turns: "unknown[]",
  pendingOperations: "unknown[]",
  tokenUsage: TokenUsage,
  connectorState: ConnectorThreadState.or("null"),
});

/**
 * On-disk shape of `checkpoint.meta.json`: the boundary seq the checkpoint
 * folded to (`checkpointSeq`, hence which WAL boundary seqs remain to
 * replay), the folded turn count, and the freshest metadata at fold time.
 * `checkpointSeq` counts MIRROR BOUNDARIES, not turns: a boundary may
 * carry zero or many turns, so the two counts diverge in general.
 */
const CheckpointMeta = type({
  checkpointSeq: "number",
  turnCount: "number",
  pendingOperations: "unknown[]",
  tokenUsage: TokenUsage,
  connectorState: ConnectorThreadState.or("null"),
});

/**
 * On-disk shape of one WAL entry at
 * `agent-state/<agentKey>/wal/<bucket>/<seq>.json`: the 0-or-more new
 * turns one mirror boundary added plus the latest metadata snapshot.
 */
const WalEntry = type({
  seq: "number",
  turns: "unknown[]",
  metadata: SnapshotMetadata,
});

/**
 * Loaded conversation snapshot the restore path applies into the warm
 * agent's local store before its reactor loads.
 */
interface LoadedSnapshot {
  turns: ConversationTurn[];
  pendingOperations: PendingOperation[];
  tokenUsage: TokenUsage;
  connectorState: ConnectorThreadState | null;
}

export interface DurableConversationStoreOpts {
  /**
   * Local per-agent isogit store root. Stable across runs (NOT keyed by
   * runId) so a warm agent's reactor loads the same on-disk store on every
   * message; the substrate is the cross-respawn durable mirror.
   */
  localStoreDir: string;
  /** Commit signer for the local isogit store. */
  signer: (payload: string) => Promise<string>;
  /** Proxy workflow-run substrate (single-writer via the supervisor). */
  substrate: RepoStore;
  /** Workflow-run repo identity for the deployment. */
  workflowRunRepoId: RepoId;
  /** Workflow-run repo ref the conversation snapshot is committed to. */
  workflowRunRef: string;
  /** Principal the substrate write is authored under. */
  principal: Principal;
  /**
   * Stable per-agent key the snapshot is filed under
   * (`agent-state/<agentKey>/`). The warm agent's stepId is the natural
   * key: stable across the agent's whole lifetime and disjoint from any
   * runId.
   */
  agentKey: string;
}

/**
 * A `ContextStore` for the warm agent whose conversation content is
 * durably mirrored to the workflow-run substrate. The reactor sees a
 * normal `ContextStore` (per-cycle commits land in the fast local isogit
 * store); `restoreFromSubstrate` and `mirrorToSubstrate` move the
 * conversation between the local store and the durable substrate layout.
 */
export interface DurableConversationStore {
  /**
   * The store the warm agent's env binds as `storage` and `audit`: both
   * `ContextStore` and `AuditStore`, matching the per-run isogit store the
   * non-warm path uses.
   */
  readonly storage: ContextStore & AuditStore;
  /**
   * Pull the prior conversation from the substrate (checkpoint + WAL-tail
   * replay) into the local store so the agent's reactor `load()` sees it.
   * Called before the warm agent is built (lazy first build and respawn
   * rebuild). Returns `true` when prior state was found and applied,
   * `false` when none exists yet. A read that finds a checkpoint or WAL
   * but cannot parse/replay it throws -- a corrupt durable copy must not
   * silently start the agent fresh.
   */
  restoreFromSubstrate(): Promise<boolean>;
  /**
   * Commit the local store's new turn(s) to the substrate as O(1) WAL
   * appends, folding into a fresh checkpoint when the WAL reaches the
   * compaction interval. Called synchronously at the run boundary. A write
   * failure surfaces.
   */
  mirrorToSubstrate(): Promise<void>;
  /**
   * Advance the connector router from a received inbound message so the
   * warm agent's reply path has thread state: runs the router's pure
   * `route()` then `commit()`. Flushes the advanced connector state into
   * the local store's metadata so the run-boundary mirror persists it and a
   * respawn restore re-seeds the router. Called before the warm agent's
   * send. A metadata write failure surfaces.
   */
  seedInbound(message: InboundMessage): Promise<void>;
  /**
   * Produce the threading headers for a reply on the active connector
   * thread. Throws `NoActiveConnectorThreadError` when no thread has been
   * seeded. The warm mail loop's reply drain reads this to address its
   * outbound reply.
   */
  composeReply(): ConnectorReplyParts;
  /**
   * Advance the connector thread after a reply was sent: forwards to the
   * router's `onReplySent` (which moves `lastMessageId` to the sent
   * reply's Message-ID so the next inbound continuation matches) and
   * flushes the advanced connector state into the local store's metadata
   * the same way `seedInbound` does, so `lastMessageId` persists across
   * turns and a child respawn. Called by the warm mail loop's reply drain
   * after its outbound send settles. Throws
   * `NoActiveConnectorThreadError` when no thread is active. A metadata
   * write failure surfaces.
   */
  onReplySent(receipt: SendReceipt): Promise<void>;
}

export async function createDurableConversationStore(
  opts: DurableConversationStoreOpts,
): Promise<DurableConversationStore> {
  await fs.promises.mkdir(opts.localStoreDir, { recursive: true });
  const baseStorage = await createIsogitStore(opts.localStoreDir, opts.signer);

  // The router's `onStateChanged` is the change-driven commit hook:
  // `seedInbound` and reply sends drive the router (route/commit), so
  // `onStateChanged` fires and enqueues a change-driven mirror behind the
  // op on the shared serialization tail; the run-boundary mirror still
  // commits every boundary. A dropped change-driven mirror is recoverable
  // at the next boundary.
  const connectorRouter = createConnectorRouter({
    onStateChanged: () => {
      void mirrorToSubstrate().catch((cause) => {
        logger.error`connector-state-change conversation mirror failed for ${opts.agentKey}: ${cause instanceof Error ? cause.message : String(cause)}`;
      });
    },
  });

  const agentStatePrefix = `${WORKFLOW_RUN_AGENT_STATE_PREFIX}/${encodeURIComponent(opts.agentKey)}/`;

  // Mirror boundaries already durably committed (checkpoint folded
  // boundaries plus appended WAL entries); the seq of the NEXT WAL entry.
  // `null` until learned lazily from the substrate on the first mirror so a
  // respawn-rebuilt store that did NOT restore never re-commits boundaries
  // the substrate already holds.
  let mirroredBoundaryCount: number | null = null;
  // Turns already durably committed (checkpoint folded turns plus every
  // turn carried by an appended WAL entry). The next mirror appends only
  // `turns.slice(mirroredTurnCount)`, which keeps each append O(1).
  let mirroredTurnCount = 0;
  // Boundary seq the current checkpoint folded to: WAL boundary seqs
  // [checkpointBoundarySeq, mirroredBoundaryCount) are live. Lets a mirror
  // know the live WAL length and when to compact.
  let checkpointBoundarySeq = 0;

  // Serialize the shared-counter critical section. Both `mirrorToSubstrate`
  // and `restoreFromSubstrate` read and advance the counts above, and
  // either can re-enter the other: `connectorRouter.restore()` can fire
  // `onStateChanged` synchronously, which enqueues a mirror. A single
  // per-instance tail runs every op one-at-a-time regardless of entry
  // point, so two overlapping runs cannot read the same boundary seq and
  // emit two WAL entries at it. The tail swallows rejections so one failed
  // op does not poison the chain, while each caller still observes its own
  // op's result through the returned promise.
  //
  // This serializes mirror-vs-mirror and mirror-vs-restore only, NOT the
  // reactor-vs-mirror peek-snapshot window documented on `runMirror` below
  // (nothing must append to the reactor's turn array between its last
  // writeTurns and the mirror's peekTurns).
  let stateOpTail: Promise<unknown> = Promise.resolve();
  function serializeStateOp<T>(op: () => Promise<T>): Promise<T> {
    const result = stateOpTail.then(op, op);
    stateOpTail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  function restoreFromSubstrate(): Promise<boolean> {
    return serializeStateOp(runRestore);
  }

  function mirrorToSubstrate(): Promise<void> {
    return serializeStateOp(runMirror);
  }

  function seedInbound(message: InboundMessage): Promise<void> {
    return serializeStateOp(() => runSeed(message));
  }

  function composeReply(): ConnectorReplyParts {
    return connectorRouter.composeReply();
  }

  function onReplySent(receipt: SendReceipt): Promise<void> {
    return serializeStateOp(() => runReplySent(receipt));
  }

  function substrateAgentStateFsDir(): string {
    const repoDir = opts.substrate.getRepoDir(opts.workflowRunRepoId);
    return path.join(
      repoDir,
      WORKFLOW_RUN_AGENT_STATE_PREFIX,
      encodeURIComponent(opts.agentKey),
    );
  }

  function bucketOf(seq: number): number {
    return Math.floor(seq / WAL_BUCKET_SIZE);
  }

  function walBucketPrefix(bucket: number): string {
    return `${agentStatePrefix}${WAL_DIR}/${String(bucket)}/`;
  }

  function walEntryPath(seq: number): string {
    return `${walBucketPrefix(bucketOf(seq))}${String(seq)}.json`;
  }

  function checkpointPath(): string {
    return `${agentStatePrefix}${CHECKPOINT_FILE}`;
  }

  function checkpointMetaPath(): string {
    return `${agentStatePrefix}${CHECKPOINT_META_FILE}`;
  }

  async function runRestore(): Promise<boolean> {
    const reconstructed = await reconstructDurableConversation(
      substrateAgentStateFsDir(),
      opts.agentKey,
    );
    if (reconstructed === null) {
      // No durable state yet: record empty committed counts so the first
      // mirror appends from boundary seq 0.
      mirroredBoundaryCount = 0;
      mirroredTurnCount = 0;
      checkpointBoundarySeq = 0;
      return false;
    }
    // Write the reconstructed turns + metadata into the local store's
    // working tree and commit, so the agent's reactor `load()` reads the
    // restored conversation. `setConnectorState` buffers the connector
    // state for the metadata write; `restore()` mirrors it into the router
    // so a future change-driven mirror carries the right base.
    await baseStorage.writeTurns(reconstructed.turns);
    baseStorage.setConnectorState(reconstructed.connectorState);
    // Establish the committed counts BEFORE restoring the connector state:
    // `connectorRouter.restore()` can fire `onStateChanged` synchronously
    // (when the restored state differs from current), which enqueues a
    // mirror. Serialization chains that mirror behind this restore, but
    // setting the counts first keeps them correct even if that ordering is
    // ever weakened. The counts reflect the substrate state `reconstructed`
    // was read from, which is durable independent of the local-store
    // commit below.
    mirroredBoundaryCount = reconstructed.boundaryCount;
    mirroredTurnCount = reconstructed.totalTurns;
    checkpointBoundarySeq = reconstructed.checkpointBoundarySeq;
    connectorRouter.restore(reconstructed.connectorState);
    await baseStorage.writeMetadata({
      pendingOperations: reconstructed.pendingOperations,
      tokenUsage: reconstructed.tokenUsage,
    });
    await baseStorage.commit({
      message: `restore conversation for ${opts.agentKey} from substrate`,
    });
    return true;
  }

  /**
   * Append one WAL entry for a mirror boundary (its 0-or-more new turns +
   * current metadata) to its bucket. The merge pre-image is exactly that
   * bucket's existing blobs (direct children of `wal/<bucket>/`), so the
   * append is the bucket's blobs plus one new entry -- O(bucket size),
   * independent of N.
   */
  async function appendWalEntry(
    boundarySeq: number,
    turns: unknown[],
    metadata: {
      pendingOperations: unknown[];
      tokenUsage: TokenUsage;
      connectorState: ConnectorThreadState | null;
    },
  ): Promise<void> {
    const entry = { seq: boundarySeq, turns, metadata };
    const serialized = JSON.stringify(entry);
    const newPath = walEntryPath(boundarySeq);
    await opts.substrate.writeTreePreservingPrefix(
      opts.principal,
      opts.workflowRunRepoId,
      opts.workflowRunRef,
      {
        preservePrefix: walBucketPrefix(bucketOf(boundarySeq)),
        merge: async (existing) => {
          const files: Record<string, string | Uint8Array> = {};
          for (const [blobPath, bytes] of existing) {
            files[blobPath] = bytes;
          }
          files[newPath] = serialized;
          return files;
        },
        message: `append conversation WAL boundary ${String(boundarySeq)} (${String(turns.length)} turn(s)) for ${opts.agentKey}`,
      },
    );
  }

  /**
   * Fold the full conversation into a fresh checkpoint and truncate the
   * WAL in one atomic commit: the merge at `preservePrefix =
   * agent-state/<key>/` returns only the two checkpoint files, so omitting
   * the WAL paths IS the truncate (substrate-merge note above).
   */
  async function writeCheckpoint(
    boundarySeq: number,
    turns: unknown[],
    metadata: {
      pendingOperations: unknown[];
      tokenUsage: TokenUsage;
      connectorState: ConnectorThreadState | null;
    },
  ): Promise<void> {
    const snapshot = {
      turns,
      pendingOperations: metadata.pendingOperations,
      tokenUsage: metadata.tokenUsage,
      connectorState: metadata.connectorState,
    };
    // `metadata` is the freshest snapshot (identical to the last appended
    // WAL entry's), so the fold captures the latest metadata into
    // checkpoint.meta -- a restore from the post-fold checkpoint sees the
    // same metadata the pre-fold WAL tail would have yielded.
    const meta = {
      checkpointSeq: boundarySeq,
      turnCount: turns.length,
      pendingOperations: metadata.pendingOperations,
      tokenUsage: metadata.tokenUsage,
      connectorState: metadata.connectorState,
    };
    await opts.substrate.writeTreePreservingPrefix(
      opts.principal,
      opts.workflowRunRepoId,
      opts.workflowRunRef,
      {
        preservePrefix: agentStatePrefix,
        merge: async () => ({
          [checkpointPath()]: JSON.stringify(snapshot),
          [checkpointMetaPath()]: JSON.stringify(meta),
        }),
        message: `compact conversation checkpoint at boundary ${String(boundarySeq)} (${String(turns.length)} turns) for ${opts.agentKey}`,
      },
    );
  }

  async function runMirror(): Promise<void> {
    // Slice the new turns from the reactor's in-memory array (retained by
    // the local single-writer store at the last writeTurns) instead of
    // re-reading and re-parsing the whole turns.jsonl every boundary; only
    // the bounded metadata.json is read from disk. This rests on a
    // sequencing invariant the store cannot enforce: nothing mutates the
    // reactor's turn array between its last writeTurns and this read. The
    // mirror runs at onRunBoundary after send() settles, and the reactor
    // only appends inside a cycle (each ending in writeTurns), so
    // peekTurns() equals the on-disk state here. Serializing the mirror
    // entry points keeps two mirrors from overlapping this read, but the
    // reactor must still not append between its writeTurns and this peek.
    const turns = baseStorage.peekTurns();
    const metadata = await baseStorage.loadMetadata();

    // First mirror in this store's lifetime that did not run through
    // `restoreFromSubstrate` (which sets the counts): learn the durable
    // counts from the substrate so the append starts at the right boundary
    // seq and never re-commits boundaries the substrate already holds.
    if (mirroredBoundaryCount === null) {
      const reconstructed = await reconstructDurableConversation(
        substrateAgentStateFsDir(),
        opts.agentKey,
      );
      checkpointBoundarySeq = reconstructed?.checkpointBoundarySeq ?? 0;
      mirroredBoundaryCount = reconstructed?.boundaryCount ?? 0;
      mirroredTurnCount = reconstructed?.totalTurns ?? 0;
    }

    // ONE WAL entry per mirror boundary, UNCONDITIONALLY -- even when no
    // new turns were added since the last mirror. The entry carries the
    // 0-or-more new turns plus the freshest metadata, so a
    // turnless-but-metadata-mutating boundary (e.g. a throwing send that
    // still advanced tokenUsage/pendingOperations, since onRunBoundary runs
    // in a finally) still durably commits its metadata. The payload is the
    // turn DELTA plus bounded metadata -- never the whole conversation.
    const newTurns = turns.slice(mirroredTurnCount);
    const boundarySeq = mirroredBoundaryCount;
    await appendWalEntry(boundarySeq, newTurns, metadata);
    mirroredBoundaryCount = boundarySeq + 1;
    // Advance by the count actually persisted -- newTurns is a pre-await
    // snapshot -- not by turns.length. `turns` is the reactor's live array
    // by reference; reading its length after the await would count any turn
    // appended during appendWalEntry as mirrored, so the next mirror would
    // slice past it and drop it from the WAL permanently.
    mirroredTurnCount = mirroredTurnCount + newTurns.length;

    // Compact once the live WAL reaches the interval (measured in mirror
    // boundaries = WAL entries, which bounds both the bucket fan-out and
    // the replay length): fold the full conversation into a fresh
    // checkpoint with the freshest metadata and truncate the WAL. Amortizes
    // the O(N) full rewrite to O(N/K) per boundary.
    if (mirroredBoundaryCount - checkpointBoundarySeq >= CHECKPOINT_INTERVAL) {
      await writeCheckpoint(
        mirroredBoundaryCount,
        turns.slice(0, mirroredTurnCount),
        metadata,
      );
      checkpointBoundarySeq = mirroredBoundaryCount;
    }
  }

  // Classify an inbound message, treating a `route()` throw as passthrough.
  // The router throws when `message.headers.from` is not a parseable bare
  // addr-spec; per the router contract that is a passthrough (do not
  // advance the thread), not a programmer error, so it must not fail the
  // seed. A synthesized passthrough decision commits as a no-op.
  function routeOrPassthrough(message: InboundMessage): RouteDecision {
    try {
      return connectorRouter.route(message);
    } catch (cause) {
      logger.warn`connector route for ${opts.agentKey} could not parse the inbound sender; leaving the thread unadvanced: ${cause instanceof Error ? cause.message : String(cause)}`;
      return { kind: "passthrough" };
    }
  }

  // Advance the connector router from a received inbound message and flush
  // the resulting connector state into the local store's metadata. `commit`
  // fires the router's `onStateChanged`, enqueuing a change-driven mirror
  // that reads the connector state from the local store's metadata (not
  // the router), so the metadata write below is what makes the seeded state
  // reach the substrate. The write preserves the reactor's staged
  // pendingOperations / tokenUsage -- a seed advances only connectorState.
  // A passthrough decision advances nothing and writes nothing.
  async function runSeed(message: InboundMessage): Promise<void> {
    const decision = routeOrPassthrough(message);
    connectorRouter.commit(decision);
    if (decision.kind === "passthrough") return;

    const metadata = await baseStorage.loadMetadata();
    baseStorage.setConnectorState(connectorRouter.snapshot());
    await baseStorage.writeMetadata({
      pendingOperations: metadata.pendingOperations,
      tokenUsage: metadata.tokenUsage,
    });
    await baseStorage.commit({
      message: `seed connector thread for ${opts.agentKey}`,
    });
  }

  // Advance the connector thread after a reply was sent and flush the
  // resulting connector state into the local store's metadata. `onReplySent`
  // moves `lastMessageId` to the reply's Message-ID and fires the router's
  // `onStateChanged`, enqueuing a change-driven mirror that reads the
  // connector state from the local store's metadata, so the metadata write
  // below is what makes the advanced state reach the substrate. The write
  // preserves the reactor's staged pendingOperations / tokenUsage -- an
  // outbound advance touches only connectorState. `onReplySent` throws when
  // no thread is active, which surfaces to the reply drain's failure
  // callback rather than persisting a phantom advance.
  async function runReplySent(receipt: SendReceipt): Promise<void> {
    connectorRouter.onReplySent(receipt);

    const metadata = await baseStorage.loadMetadata();
    baseStorage.setConnectorState(connectorRouter.snapshot());
    await baseStorage.writeMetadata({
      pendingOperations: metadata.pendingOperations,
      tokenUsage: metadata.tokenUsage,
    });
    await baseStorage.commit({
      message: `advance connector thread after reply for ${opts.agentKey}`,
    });
  }

  return {
    storage: baseStorage,
    restoreFromSubstrate,
    mirrorToSubstrate,
    seedInbound,
    composeReply,
    onReplySent,
  };
}

export interface DurableConversationRegistryOpts {
  /** Sidecar data dir; per-agent local stores root under it. */
  dataDir: string;
  /** Workflow-run repo identity for the deployment. */
  workflowRunRepoId: RepoId;
  /** Workflow-run repo ref. */
  workflowRunRef: string;
  /** Proxy workflow-run substrate (single-writer via the supervisor). */
  substrate: RepoStore;
  /** Principal the substrate write is authored under. */
  principal: Principal;
  /** Commit signer for the per-agent local isogit stores. */
  signer: (payload: string) => Promise<string>;
}

/**
 * Per-agent durable-conversation store registry (design §3c). One store
 * per warm agent key, built lazily and reused across runs in the same
 * child. The first `acquire` for a key builds the store and restores its
 * prior conversation snapshot from the substrate -- the path that runs on
 * the lazy first build AND on the respawn rebuild, so the warm agent
 * resumes its conversation across child respawn. The registry is empty
 * after a respawn (it lives in the child's address space); the substrate
 * is the durable mirror that survives.
 */ export interface DurableConversationRegistry {
  acquire(key: string): Promise<DurableConversationStore>;
  get(key: string): DurableConversationStore;
}

export function createDurableConversationRegistry(
  opts: DurableConversationRegistryOpts,
): DurableConversationRegistry {
  const stores = new Map<string, DurableConversationStore>();
  // De-dup concurrent first-acquires for the same key so two in-flight
  // step invocations for one warm agent never build two stores (which
  // would double-restore and split the durable mirror).
  const building = new Map<string, Promise<DurableConversationStore>>();
  function localStoreDir(key: string): string {
    return path.join(
      opts.dataDir,
      "agent-conversation-state",
      opts.workflowRunRepoId.id,
      encodeURIComponent(key),
    );
  }

  async function acquire(key: string): Promise<DurableConversationStore> {
    const existing = stores.get(key);
    if (existing !== undefined) return existing;
    const inFlight = building.get(key);
    if (inFlight !== undefined) return inFlight;
    const promise = (async () => {
      const store = await createDurableConversationStore({
        localStoreDir: localStoreDir(key),
        signer: opts.signer,
        substrate: opts.substrate,
        workflowRunRepoId: opts.workflowRunRepoId,
        workflowRunRef: opts.workflowRunRef,
        principal: opts.principal,
        agentKey: key,
      });
      // Restore the prior conversation BEFORE the store is observable (and
      // before the warm agent's reactor `load()` reads it). On a genuine
      // first-ever run this is a no-op (no checkpoint/WAL yet); on a
      // respawn rebuild it pulls the pre-respawn conversation back from the
      // substrate. A restore failure surfaces -- a lost conversation on
      // respawn is a correctness failure, not a silently-fresh start.
      await store.restoreFromSubstrate();
      stores.set(key, store);
      building.delete(key);
      return store;
    })().catch((cause) => {
      building.delete(key);
      throw cause;
    });
    building.set(key, promise);
    return promise;
  }

  function get(key: string): DurableConversationStore {
    const store = stores.get(key);
    if (store === undefined) {
      throw new Error(
        `sidecar conversation-state: no durable conversation store for ${JSON.stringify(key)}; the run-boundary mirror ran before the warm agent's env was built`,
      );
    }
    return store;
  }

  return { acquire, get };
}

interface SnapshotMetadataValue {
  pendingOperations: unknown[];
  tokenUsage: TokenUsage;
  connectorState: ConnectorThreadState | null;
}

/**
 * The reconstructed conversation plus the bookkeeping the mirror path
 * needs to resume appending: `totalTurns` (checkpoint + WAL), `boundaryCount`
 * (checkpoint's folded boundaries + replayed WAL entries -- the next WAL
 * entry's seq), and `checkpointBoundarySeq` (the seq the checkpoint folded
 * to -- the first WAL boundary seq to expect, so the mirror knows the live
 * WAL length and when to compact).
 */
export interface ReconstructedConversation extends LoadedSnapshot {
  totalTurns: number;
  boundaryCount: number;
  checkpointBoundarySeq: number;
}

/**
 * Reconstruct the warm agent's conversation from the two-tier on-disk
 * layout under `agentStateDir` (`<repoDir>/agent-state/<agentKey>/`): the
 * compacted `checkpoint.json` turns followed by the replayed WAL tail.
 * Pure read against the substrate working tree -- no inference, no commit.
 * Returns `null` when neither a checkpoint nor any WAL exists (the genuine
 * first-ever run). The latest metadata wins: the last replayed WAL entry,
 * or the checkpoint when the WAL is empty. Throws on any corrupt or
 * unparseable blob or a WAL seq gap -- a damaged durable copy must
 * surface, never silently start the agent fresh or drop a turn.
 *
 * Exported so a reader (durability test, recovery audit) reconstructs the
 * conversation through the SAME code path the warm agent's restore uses,
 * rather than re-deriving the WAL/checkpoint fold independently.
 */
export async function reconstructDurableConversation(
  agentStateDir: string,
  agentKey: string,
): Promise<ReconstructedConversation | null> {
  const checkpoint = await readCheckpointFromDir(agentStateDir, agentKey);
  const baseBoundarySeq = checkpoint?.checkpointSeq ?? 0;
  const wal = await readWalTailFromDir(
    agentStateDir,
    agentKey,
    baseBoundarySeq,
  );
  if (checkpoint === null && wal.length === 0) return null;

  const turns: unknown[] = [...(checkpoint?.turns ?? [])];
  // The freshest metadata wins: the last WAL entry, or the checkpoint when
  // the WAL is empty.
  let metadata: SnapshotMetadataValue = checkpoint?.metadata ?? {
    pendingOperations: [],
    tokenUsage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      thinking: 0,
    },
    connectorState: null,
  };
  for (const entry of wal) {
    for (const turn of entry.turns) {
      turns.push(turn);
    }
    metadata = entry.metadata;
  }
  return {
    // The reactor re-narrows turn/operation elements on load; the
    // validators below enforce only the structural envelope, matching the
    // boundary the whole-blob mirror used.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- envelope validated in the read helpers; turn element narrows live in the reactor on load
    turns: turns as ConversationTurn[],
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- envelope validated in the read helpers; pending-operation element narrows live in the reactor on load
    pendingOperations: metadata.pendingOperations as PendingOperation[],
    tokenUsage: metadata.tokenUsage,
    connectorState: metadata.connectorState,
    totalTurns: turns.length,
    boundaryCount: baseBoundarySeq + wal.length,
    checkpointBoundarySeq: baseBoundarySeq,
  };
}

/**
 * Read the checkpoint pair from `agentStateDir`. Returns `null` only when
 * no checkpoint exists yet -- the reconstruction treats that as "no folded
 * turns" (any conversation lives entirely in the WAL). A present-but-corrupt
 * or inconsistent checkpoint throws.
 */
async function readCheckpointFromDir(
  agentStateDir: string,
  agentKey: string,
): Promise<{
  turns: unknown[];
  checkpointSeq: number;
  metadata: SnapshotMetadataValue;
} | null> {
  let metaRaw: string;
  try {
    metaRaw = await fs.promises.readFile(
      path.join(agentStateDir, CHECKPOINT_META_FILE),
      "utf8",
    );
  } catch (cause) {
    if (isErrnoNotFound(cause)) return null;
    throw cause;
  }
  const meta = parseJsonOrThrow(metaRaw, `${agentKey} ${CHECKPOINT_META_FILE}`);
  const validatedMeta = CheckpointMeta(meta);
  if (validatedMeta instanceof type.errors) {
    throw new Error(
      `sidecar conversation-state: ${CHECKPOINT_META_FILE} for ${agentKey} failed validation: ${validatedMeta.summary}; refusing to start the warm agent fresh on a corrupt checkpoint`,
    );
  }
  const snapshotRaw = await fs.promises.readFile(
    path.join(agentStateDir, CHECKPOINT_FILE),
    "utf8",
  );
  const snapshot = parseJsonOrThrow(
    snapshotRaw,
    `${agentKey} ${CHECKPOINT_FILE}`,
  );
  const validatedSnapshot = CheckpointSnapshot(snapshot);
  if (validatedSnapshot instanceof type.errors) {
    throw new Error(
      `sidecar conversation-state: ${CHECKPOINT_FILE} for ${agentKey} failed validation: ${validatedSnapshot.summary}; refusing to start the warm agent fresh on a corrupt checkpoint`,
    );
  }
  if (validatedSnapshot.turns.length !== validatedMeta.turnCount) {
    throw new Error(
      `sidecar conversation-state: ${CHECKPOINT_FILE} for ${agentKey} carries ${String(validatedSnapshot.turns.length)} turns but ${CHECKPOINT_META_FILE} reports turnCount ${String(validatedMeta.turnCount)}; the checkpoint pair is inconsistent`,
    );
  }
  return {
    turns: validatedSnapshot.turns,
    checkpointSeq: validatedMeta.checkpointSeq,
    metadata: {
      pendingOperations: validatedSnapshot.pendingOperations,
      tokenUsage: validatedSnapshot.tokenUsage,
      connectorState: validatedSnapshot.connectorState,
    },
  };
}

/**
 * Read and seq-order the per-boundary WAL entries for boundary seqs >=
 * `fromSeq` from `<agentStateDir>/wal/<bucket>/`. Throws on any
 * unparseable or out-of-shape WAL blob -- a corrupt WAL must surface,
 * never be skipped. Throws on a gap in the boundary seq sequence: a
 * missing seq means a lost append, which would silently drop a boundary's
 * turns + metadata from the reconstruction.
 */
async function readWalTailFromDir(
  agentStateDir: string,
  agentKey: string,
  fromSeq: number,
): Promise<
  { seq: number; turns: unknown[]; metadata: SnapshotMetadataValue }[]
> {
  const walDir = path.join(agentStateDir, WAL_DIR);
  let buckets: string[];
  try {
    buckets = await fs.promises.readdir(walDir);
  } catch (cause) {
    if (isErrnoNotFound(cause)) return [];
    throw cause;
  }
  const entries: {
    seq: number;
    turns: unknown[];
    metadata: SnapshotMetadataValue;
  }[] = [];
  for (const bucket of buckets) {
    const bucketDir = path.join(walDir, bucket);
    const files = await fs.promises.readdir(bucketDir);
    for (const file of files) {
      if (!file.endsWith(".json")) {
        throw new Error(
          `sidecar conversation-state: unexpected non-JSON WAL entry ${WAL_DIR}/${bucket}/${file} for ${agentKey}`,
        );
      }
      const raw = await fs.promises.readFile(
        path.join(bucketDir, file),
        "utf8",
      );
      const parsed = parseJsonOrThrow(
        raw,
        `${agentKey} ${WAL_DIR}/${bucket}/${file}`,
      );
      const validated = WalEntry(parsed);
      if (validated instanceof type.errors) {
        throw new Error(
          `sidecar conversation-state: WAL entry ${WAL_DIR}/${bucket}/${file} for ${agentKey} failed validation: ${validated.summary}; refusing to start the warm agent fresh on a corrupt WAL`,
        );
      }
      if (validated.seq < fromSeq) continue;
      entries.push({
        seq: validated.seq,
        turns: validated.turns,
        metadata: {
          pendingOperations: validated.metadata.pendingOperations,
          tokenUsage: validated.metadata.tokenUsage,
          connectorState: validated.metadata.connectorState,
        },
      });
    }
  }
  entries.sort((a, b) => a.seq - b.seq);
  for (let i = 0; i < entries.length; i += 1) {
    const expected = fromSeq + i;
    const entry = entries[i];
    if (entry === undefined || entry.seq !== expected) {
      throw new Error(
        `sidecar conversation-state: WAL for ${agentKey} has a seq gap (expected ${String(expected)}, found ${String(entry?.seq)}); a lost append would silently drop a boundary's turns and metadata`,
      );
    }
  }
  return entries;
}

function parseJsonOrThrow(raw: string, label: string): unknown {
  try {
    return JSON.parse(raw);
  } catch (cause) {
    throw new Error(
      `sidecar conversation-state: ${label} is not valid JSON; refusing to start the warm agent fresh on a corrupt durable copy`,
      { cause },
    );
  }
}
