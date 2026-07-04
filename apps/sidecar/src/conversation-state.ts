// Durable conversation state for the warm single-step agent (design §3c,
// §4 Phase D1).
//
// A long-lived single-step agent holds its multi-turn conversation in
// the reactor's in-memory turn buffer, backed by a per-step isogit
// `ContextStore`. That store is rooted per run/attempt, so it is lost
// the moment the warm agent's child is killed and respawned: the
// rebuilt agent loads a fresh, empty per-run store and the conversation
// continuity is gone.
//
// This module makes the warm agent's conversation DURABLE in the
// workflow-run substrate (the single-writer proxy `RepoStore`, written
// through the supervisor). The durable copy lives under the workflow-run
// repo at a stable per-agent path (`agent-state/<agentKey>/...`), sibling
// to the per-run event log under `runs/<runId>/...` and NOT confused with
// it. On a new run (and after a child respawn, once the warm agent is
// rebuilt lazily) the conversation is restored from the substrate into
// the agent's local store BEFORE the agent's reactor loads, so multi-turn
// continuity holds across runs and across respawn.
//
// Two-tier on-disk layout (design §4, Phase D1). The prior design wrote
// the WHOLE conversation as a single `conversation.json` blob on every
// message. That re-serialized and re-hashed every prior turn per message,
// so the per-message durable cost grew O(N) in the turn count -- O(N^2)
// over a conversation. D1 replaces it with an append-only, bucket-sharded
// write-ahead log plus a periodic compacted checkpoint:
//
//   agent-state/<agentKey>/
//     checkpoint.json        compacted full snapshot (turns + metadata)
//     checkpoint.meta.json   { checkpointSeq: <boundary>, turnCount,
//                              tokenUsage, pendingOperations,
//                              connectorState }
//     wal/<bucket>/<seq>.json  one per-boundary delta blob keyed by mirror
//                              BOUNDARY seq, carrying that boundary's
//                              0-or-more new turns plus the freshest metadata
//
// The WAL is keyed by mirror BOUNDARY, not by turn: each `mirrorToSubstrate`
// writes exactly one entry, even when the boundary added no new turns. That
// makes metadata (pendingOperations, tokenUsage, connectorState) persist on
// EVERY boundary -- a turnless-but-metadata-mutating boundary still commits
// a zero-turn entry, so restore never reconstructs stale metadata. (Keying
// the entry by turn dropped metadata on turnless boundaries, which regressed
// the byte-for-byte metadata-equivalence invariant; per-boundary keying is
// the fix.)
//
// `bucket = floor(boundarySeq / WAL_BUCKET_SIZE)` (B = 128) bounds any
// single directory's tree-object size so no commit re-hashes a tree that
// grows with N (a flat `wal/<seq>.json` directory would itself be O(N) per
// commit). Compaction every CHECKPOINT_INTERVAL boundaries (K = 64, i.e.
// once the live WAL reaches K entries) folds the WAL into a fresh
// `checkpoint.json` (capturing the freshest metadata in checkpoint.meta) and
// truncates the WAL, so between checkpoints the WAL holds at most K entries
// and per-boundary durable cost is ~O(1) amortized. K and B are constants
// here; the design flags them as measurement-tunable.
//
// Restore = load `checkpoint.json` (folded turns + its metadata) then replay
// the WAL tail in boundary-seq order, concatenating each boundary's turns
// and taking the LATEST entry's metadata (the last WAL entry wins; the
// checkpoint's metadata is the base when the WAL is empty). This is pure
// state reconstruction from recorded outputs -- never re-inference. It
// rebuilds the EXACT turn list + metadata the old whole-blob mirror would
// have restored.
//
// Substrate-merge constraint (load-bearing, design §4 "Substrate-merge
// note"). `writeTreePreservingPrefix`'s `merge` callback receives only
// the DIRECT CHILDREN of `preservePrefix`, and the substrate's
// `clearPrefix` step recursively removes the whole `preservePrefix`
// subtree before writing the merge's returned set (paths outside the
// prefix pass through untouched). A WAL blob is two levels below
// `agent-state/<key>/`, so:
//
//   - WAL append uses `preservePrefix = agent-state/<key>/wal/<bucket>/`.
//     The bucket's existing blobs ARE direct children, so the merge
//     pre-image is exactly that bucket and the append adds one entry --
//     no isogit side-read, and the checkpoint / other buckets are
//     untouched (outside the prefix).
//   - Checkpoint write + WAL truncate uses `preservePrefix =
//     agent-state/<key>/`. The top-level checkpoint files are direct
//     children; the merge returns ONLY those files and NO `wal/...`
//     paths, so the recursive `clearPrefix` at `agent-state/<key>/` drops
//     the entire WAL subtree in the same atomic commit. The truncate
//     needs no nested read: omitting the WAL paths from the returned set
//     IS the truncate.
//
// Persistence sink (the riskiest part, design §6). The connector router's
// `snapshot()` / `restore()` surface and the harness's
// `createWrappedStorageOverrides` are reused, but the persistence sink is
// repointed from the agent's local isogit store to the workflow-run
// substrate. Both the WAL append and the checkpoint write route through
// the proxy `writeTreePreservingPrefix`; because the supervisor is the
// single writer and serializes every write to the workflow-run ref under
// a per-repo lock, and the `agent-state/<key>/...` prefix is disjoint from
// the run-event prefix (`runs/<runId>/events/`), the conversation write
// never races nor clobbers the run-event log -- both pass through the same
// single writer, and the preserve-prefix merge leaves every other subtree
// byte-for-byte intact.
//
// Timing (design §4 Phase D1, invariant 1). This is a STRUCTURE-only
// change. The mirror is still `await`ed synchronously at the same run
// boundary (`onRunBoundary` -> `mirrorToSubstrate`), so every turn is
// still durably committed before the next message is processed. D1
// changes WHAT the write does (O(1) append instead of O(N) whole-blob),
// not WHEN it happens. The run log is NOT yet a durable backstop for the
// turn (it carries a constant ref, design rev-2 FACT 1), so the
// conversation copy here remains the sole durable copy of the agent's
// per-turn output -- which is exactly why D1 keeps the write synchronous.
// The async flusher, run-log enrichment, and crash reconciliation are
// later, conditional phases (D2-D4), not done here.
//
// Commit granularity (greybeard's pick, design §3c open question). The
// design calls for connector-state-change-driven commits via the router's
// `onStateChanged` hook. In the unified-host warm-agent path the agent
// receives synthesized step inputs and never drives the connector router
// (the supervisor owns mail), so `onStateChanged` stays dormant and never
// fires. The commit therefore falls back to the run boundary (per
// message). The `onStateChanged` wiring is retained so a future path that
// does drive the router gets change-driven mirrors for free.
//
// Defensive: a restore that finds a checkpoint or WAL but cannot
// parse/replay it THROWS (a lost or corrupt conversation on respawn is a
// correctness failure, not a silently-fresh start). A mirror write failure
// surfaces so a dropped durability write is visible rather than leaving
// the next respawn to read a stale snapshot.

import fs from "node:fs";
import path from "node:path";

import { type } from "arktype";

import { getLogger } from "@intx/log";
import { createConnectorRouter } from "@intx/harness";
import { createIsogitStore } from "@intx/storage-isogit";
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
  type PendingOperation,
} from "@intx/types/runtime";

const logger = getLogger(["sidecar", "workflow-child", "conversation-state"]);

const CHECKPOINT_FILE = "checkpoint.json";
const CHECKPOINT_META_FILE = "checkpoint.meta.json";
const WAL_DIR = "wal";

/**
 * Compaction interval: fold the WAL into a fresh checkpoint once it holds
 * this many turns since the last checkpoint. Bounds the WAL tail (and so
 * the restore-replay length) between checkpoints. Measurement-tunable
 * (design §6, open question 4); D1 fixes it at 64.
 */
const CHECKPOINT_INTERVAL = 64;

/**
 * WAL directory fan-out bound: turn `seq` lives in bucket
 * `floor(seq / WAL_BUCKET_SIZE)`. Caps any single `wal/<bucket>/` tree at
 * this many entries so no commit re-hashes a tree that grows with the
 * conversation length. Measurement-tunable (design §6, open question 4);
 * D1 fixes it at 128.
 */
const WAL_BUCKET_SIZE = 128;

/**
 * Metadata carried alongside the checkpoint and stamped onto every WAL
 * entry. Small and bounded -- it is NOT the O(N) cost; the turn array is.
 * Stamping the latest metadata on each WAL entry lets the restore replay
 * recover the exact non-turn reactor state without a separate metadata
 * log: the last replayed entry's metadata wins. Because every mirror
 * boundary writes exactly one WAL entry (even a turnless one), the latest
 * metadata is ALWAYS captured durably -- a turnless boundary still commits
 * its advanced metadata as a zero-turn entry.
 */
const SnapshotMetadata = type({
  pendingOperations: "unknown[]",
  tokenUsage: TokenUsage,
  connectorState: ConnectorThreadState.or("null"),
});

/**
 * On-disk shape of the compacted checkpoint blob committed at
 * `agent-state/<agentKey>/checkpoint.json`. Carries the folded turn
 * history (turns 0..checkpointSeq-1) plus the non-turn reactor metadata.
 * Validated on read because it crosses back into the program from the
 * substrate working tree -- a corrupt or partially-written checkpoint must
 * surface at the boundary, never be half-applied into the agent.
 */
const CheckpointSnapshot = type({
  turns: "unknown[]",
  pendingOperations: "unknown[]",
  tokenUsage: TokenUsage,
  connectorState: ConnectorThreadState.or("null"),
});

/**
 * On-disk shape of `agent-state/<agentKey>/checkpoint.meta.json`. The
 * checkpoint pointer the restore path reads first to learn the boundary seq
 * the checkpoint folded to (`checkpointSeq`) -- and therefore which WAL
 * boundary seqs remain to replay -- plus the folded turn count
 * (`turnCount`, = `checkpoint.json`'s turn array length) and the freshest
 * metadata at fold time. `checkpointSeq` counts MIRROR BOUNDARIES, not
 * turns: a boundary may carry zero or many turns, so the boundary count and
 * the turn count diverge in general (they coincide only when every boundary
 * adds exactly one turn).
 */
const CheckpointMeta = type({
  checkpointSeq: "number",
  turnCount: "number",
  pendingOperations: "unknown[]",
  tokenUsage: TokenUsage,
  connectorState: ConnectorThreadState.or("null"),
});

/**
 * On-disk shape of one WAL entry blob at
 * `agent-state/<agentKey>/wal/<bucket>/<seq>.json`. One entry per MIRROR
 * BOUNDARY (keyed by boundary `seq`, not turn index). Records the 0-or-more
 * new turns that boundary added (the O(1) append payload -- it never
 * carries prior turns) plus the latest non-turn metadata snapshot. The
 * append is UNCONDITIONAL: a turnless boundary still writes one entry with
 * `turns: []` so its advanced metadata is durably committed (the invariant
 * the per-turn keying broke -- metadata must persist on EVERY boundary).
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
   * runId) so a warm agent's reactor loads the same on-disk store on
   * every message; the substrate is the cross-respawn durable mirror of
   * this store's conversation content.
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
   * (`agent-state/<agentKey>/`). The warm single-step agent's stepId is
   * the natural key: it is stable across that agent's whole lifetime and
   * disjoint from any runId.
   */
  agentKey: string;
}

/**
 * A `ContextStore` for the warm agent whose conversation content is
 * durably mirrored to the workflow-run substrate. The reactor sees a
 * normal `ContextStore` (its per-cycle commits land in the fast local
 * isogit store); `restoreFromSubstrate` and `mirrorToSubstrate` move the
 * conversation between the local store and the durable substrate layout.
 */
export interface DurableConversationStore {
  /**
   * The store the warm agent's env binds as `storage` and `audit`. It is
   * both `ContextStore` (conversation + connector state) and
   * `AuditStore` (tool-authorization records), matching the per-run
   * isogit store the non-warm path uses.
   */
  readonly storage: ContextStore & AuditStore;
  /**
   * Pull the prior conversation from the substrate (checkpoint + WAL-tail
   * replay) into the local store so the agent's reactor `load()` sees it.
   * Called before the warm agent is built (lazy first build and respawn
   * rebuild). Returns `true` when prior state was found and applied,
   * `false` when none exists yet (the genuine first-ever run). A read that
   * finds a checkpoint or WAL but cannot parse/replay it throws -- a
   * corrupt durable copy is a correctness failure that must not silently
   * start the agent fresh.
   */
  restoreFromSubstrate(): Promise<boolean>;
  /**
   * Commit the local store's new turn(s) to the substrate as O(1) WAL
   * appends, folding into a fresh checkpoint when the WAL reaches the
   * compaction interval. Called synchronously at the run boundary (after
   * the agent's send settles). A write failure surfaces.
   */
  mirrorToSubstrate(): Promise<void>;
}

export async function createDurableConversationStore(
  opts: DurableConversationStoreOpts,
): Promise<DurableConversationStore> {
  await fs.promises.mkdir(opts.localStoreDir, { recursive: true });
  const baseStorage = await createIsogitStore(opts.localStoreDir, opts.signer);

  // Reuse the connector router + the harness storage-override seam. The
  // router's `onStateChanged` is the change-driven commit hook the design
  // names; in the synthesized-step-input warm path it stays dormant (the
  // supervisor owns mail, so the agent never routes a connector message),
  // so the run-boundary mirror is the operative commit trigger. The wiring
  // is retained verbatim so a future path that drives the router gets
  // change-driven mirrors with no further work.
  const connectorRouter = createConnectorRouter({
    onStateChanged: () => {
      void mirrorToSubstrate().catch((cause) => {
        logger.error`connector-state-change conversation mirror failed for ${opts.agentKey}: ${cause instanceof Error ? cause.message : String(cause)}`;
      });
    },
  });

  const agentStatePrefix = `${WORKFLOW_RUN_AGENT_STATE_PREFIX}/${encodeURIComponent(opts.agentKey)}/`;

  // The number of mirror boundaries already durably committed (the
  // checkpoint's folded boundaries plus every appended WAL entry). It is
  // the seq of the NEXT WAL entry. `null` until learned -- lazily from the
  // substrate on the first mirror so a respawn-rebuilt store that did NOT
  // restore never re-commits boundaries the substrate already holds.
  let mirroredBoundaryCount: number | null = null;
  // The number of turns already durably committed (checkpoint folded turns
  // plus every turn carried by an appended WAL entry). The next mirror
  // appends only `turns.slice(mirroredTurnCount)`, which is what keeps each
  // append O(1) in the turn count.
  let mirroredTurnCount = 0;
  // The boundary seq the current checkpoint folded to: WAL boundary seqs
  // [checkpointBoundarySeq, mirroredBoundaryCount) are live. Tracked so a
  // mirror knows the live WAL length (mirroredBoundaryCount -
  // checkpointBoundarySeq) and when to compact.
  let checkpointBoundarySeq = 0;

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

  async function restoreFromSubstrate(): Promise<boolean> {
    const reconstructed = await reconstructDurableConversation(
      substrateAgentStateFsDir(),
      opts.agentKey,
    );
    if (reconstructed === null) {
      // No durable state yet: the next mirror starts the WAL from an empty
      // checkpoint. Record the (empty) committed counts so the first
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
    connectorRouter.restore(reconstructed.connectorState);
    await baseStorage.writeMetadata({
      pendingOperations: reconstructed.pendingOperations,
      tokenUsage: reconstructed.tokenUsage,
    });
    await baseStorage.commit({
      message: `restore conversation for ${opts.agentKey} from substrate`,
    });
    mirroredBoundaryCount = reconstructed.boundaryCount;
    mirroredTurnCount = reconstructed.totalTurns;
    checkpointBoundarySeq = reconstructed.checkpointBoundarySeq;
    return true;
  }

  /**
   * Append one WAL entry for a mirror boundary (its 0-or-more new turns +
   * the current metadata) to its bucket. The merge pre-image is exactly
   * that bucket's existing blobs (direct children of `wal/<bucket>/`), so
   * the append is the bucket's blobs plus the one new entry -- O(bucket
   * size), independent of N. The append is the SINGLE synchronous write per
   * boundary; the entry is keyed by boundary `seq` so a turnless boundary
   * still commits its metadata as a zero-turn entry.
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
   * WAL in one atomic commit at `preservePrefix = agent-state/<key>/`. The
   * merge returns ONLY the two checkpoint files and NO `wal/...` paths;
   * because the substrate's `clearPrefix` recursively removes the whole
   * `agent-state/<key>/` subtree before writing the returned set, omitting
   * the WAL paths IS the truncate.
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
    // `metadata` is the freshest snapshot (the current local-store
    // metadata, identical to the last appended WAL entry's metadata), so
    // the fold captures the latest metadata into checkpoint.meta -- a
    // restore from the post-fold checkpoint sees the same metadata the
    // pre-fold WAL tail would have yielded.
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

  async function mirrorToSubstrate(): Promise<void> {
    // Slice the new turns from the reactor's in-memory array (retained
    // by the local single-writer store at the last writeTurns) instead
    // of re-reading and re-parsing the whole turns.jsonl every
    // boundary; only the bounded metadata.json is read from disk. This
    // rests on a sequencing invariant the store cannot enforce: nothing
    // mutates the reactor's turn array between its last writeTurns and
    // this read. The mirror runs at onRunBoundary after send() settles,
    // and the reactor only appends inside a cycle (each ending in
    // writeTurns), so peekTurns() equals the on-disk state here. A
    // second, concurrent mirror trigger would have to preserve that
    // ordering or slice from a snapshot.
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

    // ONE WAL entry per mirror boundary, UNCONDITIONALLY -- even when no new
    // turns were added since the last mirror. The entry carries the
    // 0-or-more new turns plus the freshest metadata snapshot, so a
    // turnless-but-metadata-mutating boundary (e.g. a throwing send that
    // still advanced tokenUsage/pendingOperations, since onRunBoundary runs
    // in a finally) still durably commits its metadata. The payload is the
    // turn DELTA plus bounded metadata -- never the whole conversation, so
    // the O(N^2) growth stays gone. This is the single synchronous write
    // per boundary on the reply path.
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
    // boundaries = WAL entries, which bounds both the bucket fan-out and the
    // replay length): fold the full conversation into a fresh checkpoint
    // with the freshest metadata and truncate the WAL. Amortizes the
    // unavoidable O(N) full rewrite to O(N/K) per boundary.
    if (mirroredBoundaryCount - checkpointBoundarySeq >= CHECKPOINT_INTERVAL) {
      await writeCheckpoint(
        mirroredBoundaryCount,
        turns.slice(0, mirroredTurnCount),
        metadata,
      );
      checkpointBoundarySeq = mirroredBoundaryCount;
    }
  }

  return {
    storage: baseStorage,
    restoreFromSubstrate,
    mirrorToSubstrate,
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
 */
export interface DurableConversationRegistry {
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
      // substrate (checkpoint + WAL replay). A restore failure surfaces --
      // a lost conversation on respawn is a correctness failure, not a
      // silently-fresh start.
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
 * The reconstructed conversation plus the bookkeeping the mirror path needs
 * to resume appending. `totalTurns` is the full turn count (checkpoint +
 * WAL). `boundaryCount` is the number of mirror boundaries durably
 * committed (checkpoint's folded boundaries + replayed WAL entries) -- the
 * next WAL entry uses this as its boundary seq. `checkpointBoundarySeq` is
 * the boundary seq the checkpoint folded to (the first WAL boundary seq to
 * expect), so the mirror knows the live WAL length and when to compact.
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
 * first-ever run). The latest metadata source wins (the last replayed WAL
 * entry, or the checkpoint when the WAL is empty), mirroring how each
 * mirror stamps the current metadata. Throws on any corrupt/unparseable
 * blob or a WAL seq gap -- a damaged durable copy must surface, never
 * silently start the agent fresh or drop a turn.
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
  // the WAL is empty. Because every boundary writes a WAL entry, the last
  // entry always carries the latest metadata -- including a turnless
  // boundary that advanced only metadata.
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
 * no checkpoint exists yet -- which the reconstruction treats as "no
 * folded turns" (any conversation lives entirely in the WAL). A
 * present-but-corrupt or inconsistent checkpoint throws.
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
 * `fromSeq` from `<agentStateDir>/wal/<bucket>/`. Throws on any unparseable
 * or out-of-shape WAL blob -- a corrupt WAL must surface, never be skipped.
 * Throws on a gap in the boundary seq sequence: a missing seq means a lost
 * append, which would silently drop a boundary's turns + metadata from the
 * reconstruction.
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

function isErrnoNotFound(cause: unknown): boolean {
  if (cause === null || typeof cause !== "object") return false;
  const code = (cause as { code?: unknown }).code;
  return code === "ENOENT";
}
