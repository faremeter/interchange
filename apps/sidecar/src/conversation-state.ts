// Durable conversation state for agent steps (design §3c, §4 Phase D1).
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
// through the supervisor). The durable copy lives in the state directory
// of the step in the run it belongs to
// (`runs/<runId>/steps/<stepId>/state/`), beside that run's event log, so
// the Hub's copy of the repo carries it to a replacement host. After a
// child respawn, once the warm agent is rebuilt lazily, the conversation is
// restored from the substrate into the agent's local store BEFORE the
// agent's reactor loads, so multi-turn continuity holds across respawn and
// failover.
//
// A multi-step workflow's agent steps run cold -- a fresh agent per
// invocation -- and use the same store for each invocation, scoped to one
// attempt (`DurableConversationLifetime`): it restores the attempt's
// committed conversation before the agent loads and is mirrored once the
// turn settles, so a conversational step continues on a replacement host.
// On the host that ran the attempt, the per-attempt local store can be one
// turn ahead of the committed copy: a crash between the reactor's local
// commit and the mirror leaves that turn only there. A restore there keeps
// the local store while it still extends the committed turns
// (`isLocalStateCurrent`), so the turn is not lost and a crash-recovered
// approval park resumes against the gate it recorded. The lead is only
// meaningful over the history it was built on, so the workflow-run pack
// restorer discards these stores whenever the Hub replaces that history.
//
// Deployments that predate that layout kept the conversation at
// `agent-state/<stepId>/`. The first restore that finds state only there
// folds it into a checkpoint in the state directory and then drops the old
// subtree. A crash between the two writes leaves both copies; the next
// restore prefers the state directory and finishes the drop.
//
// A deployment can import state for a step, which the Hub commits as the
// step's seed beside its state directory before the deployment starts. A
// restore that finds no state of the store's own starts from the seed
// instead, and the first mirror commits the seeded turns as the step's own
// state. That covers a step's first turn and a retried attempt starting
// over alike, so a retry starts over from the imported state, not from
// nothing. A restore that finds neither, and no current attempt lead,
// starts the step with no conversation: the committed history, not the
// local store, decides what the agent continues.
//
// The conversation is committed in the step-state format that
// `@intx/hub-sessions/substrate` defines: a compacted checkpoint plus an
// append-only, bucket-sharded WAL keyed by mirror boundary, so each
// boundary's durable write carries only its new turns instead of
// re-serializing the whole conversation. `mirrorToSubstrate` writes exactly
// one WAL entry per boundary, even a turnless one, so metadata advanced
// without a new turn is still durable. Compaction every CHECKPOINT_INTERVAL
// boundaries (K = 64, i.e. once the live WAL reaches K entries) folds the
// WAL into a fresh checkpoint carrying the freshest metadata and truncates
// the WAL, so between checkpoints the WAL holds at most K entries and
// per-boundary durable cost is ~O(1) amortized. K is a constant here; the
// design flags it as measurement-tunable. Restore reconstructs through the
// same format module, so it rebuilds the exact turn list and metadata the
// mirror committed.
//
// Substrate-merge constraint (load-bearing, design §4 "Substrate-merge
// note"). `writeTreePreservingPrefix`'s `merge` callback receives only
// the DIRECT CHILDREN of `preservePrefix`, and the substrate's
// `clearPrefix` step recursively removes the whole `preservePrefix`
// subtree before writing the merge's returned set (paths outside the
// prefix pass through untouched). A WAL blob is two levels below the
// state directory, so:
//
//   - WAL append uses `preservePrefix = <state dir>/wal/<bucket>/`.
//     The bucket's existing blobs ARE direct children, so the merge
//     pre-image is exactly that bucket and the append adds one entry --
//     no isogit side-read, and the checkpoint / other buckets are
//     untouched (outside the prefix).
//   - Checkpoint write + WAL truncate uses `preservePrefix = <state dir>/`.
//     The top-level checkpoint files are direct children; the merge
//     returns ONLY those files and NO `wal/...` paths, so the recursive
//     `clearPrefix` at the state directory drops the entire WAL subtree in
//     the same atomic commit. The truncate needs no nested read: omitting
//     the WAL paths from the returned set IS the truncate.
//
// Persistence sink (the riskiest part, design §6). The connector router's
// `snapshot()` / `restore()` surface and the harness's
// `createWrappedStorageOverrides` are reused, but the persistence sink is
// repointed from the agent's local isogit store to the workflow-run
// substrate. Both the WAL append and the checkpoint write route through
// the proxy `writeTreePreservingPrefix`; because the supervisor is the
// single writer and serializes every write to the workflow-run ref under
// a per-repo lock, and the state directory is disjoint from the run-event
// prefix (`runs/<runId>/events/`), the conversation write never races nor
// clobbers the run-event log -- both pass through the same single writer,
// and the preserve-prefix merge leaves every other subtree byte-for-byte
// intact.
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
// `onStateChanged` hook. The warm-agent path drives the connector router
// through `seedInbound`: each mail-derived inbound message routes and
// commits its thread state before the agent's send, so `onStateChanged`
// fires and enqueues a change-driven mirror. The run-boundary mirror (per
// message) still runs unconditionally, so the two triggers are
// complementary -- the seed persists the connector state promptly, the
// boundary persists the turn delta.
//
// Defensive: a restore that finds a checkpoint or WAL but cannot
// parse/replay it THROWS (a lost or corrupt conversation on respawn is a
// correctness failure, not a silently-fresh start). A mirror write failure
// surfaces so a dropped durability write is visible rather than leaving
// the next respawn to read a stale snapshot.

import fs from "node:fs";
import path from "node:path";

import { getLogger } from "@intx/log";
import { createConnectorRouter } from "@intx/harness";
import type { ConnectorReplyParts, RouteDecision } from "@intx/harness";
import { createIsogitStore } from "@intx/storage-isogit/node";
import type {
  CommittedReads,
  Principal,
  ReconstructedStepState,
  RepoId,
  RepoStore,
  StepStateContent,
  StepStateMetadata,
} from "@intx/hub-sessions/substrate";
import {
  buildStepStateCheckpoint,
  createCommittedStepStateReader,
  readCommittedStepStateSeed,
  reconstructStepState,
  serializeStepStateWalEntry,
  stepStateWalBucket,
  stepStateWalBucketPrefix,
  stepStateWalEntryPath,
  workflowRunLegacyAgentStatePrefix,
  workflowRunStepStatePrefix,
} from "@intx/hub-sessions/substrate";
import type {
  AuditStore,
  ContextStore,
  InboundMessage,
  SendReceipt,
} from "@intx/types/runtime";

const logger = getLogger(["sidecar", "workflow-child", "conversation-state"]);

/**
 * Compaction interval: fold the WAL into a fresh checkpoint once it holds
 * this many turns since the last checkpoint. Bounds the WAL tail (and so
 * the restore-replay length) between checkpoints. Measurement-tunable
 * (design §6, open question 4); D1 fixes it at 64.
 */
const CHECKPOINT_INTERVAL = 64;

/**
 * How long the WAL may grow while compaction keeps failing. A failed
 * compaction loses nothing -- the boundary's WAL entry is already committed
 * -- so the mirror retries it at the next boundary instead of failing the
 * turn. At this length every retry since the interval has failed, and the
 * mirror fails rather than grow the restore replay without bound.
 */
const MAX_UNCOMPACTED_WAL = 2 * CHECKPOINT_INTERVAL;

/**
 * What a store's durable state belongs to. The warm agent keeps one
 * conversation for the life of its deployment, across attempts. A cold
 * step's conversation belongs to one attempt: a retried attempt starts over,
 * so its store neither restores nor appends to an earlier attempt's state.
 */
export type DurableConversationLifetime =
  | { readonly kind: "deployment" }
  | { readonly kind: "attempt"; readonly attempt: number };

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
  /** The run the step belongs to; its state lives under that run. */
  runId: string;
  /** The step whose state this store holds. */
  stepId: string;
  lifetime: DurableConversationLifetime;
}

/**
 * A `ContextStore` for an agent step whose conversation content is
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
   * An attempt-scoped store whose local store is still current keeps it
   * instead (see `isLocalStateCurrent`). A step with no state of its own
   * starts from the seed its deployment imported, when there is one, and
   * with no conversation otherwise, whatever its local store held. Called
   * before the agent is built (lazy first build and respawn rebuild).
   * Returns `true` when state or a seed was found and applied, `false` when
   * neither exists. A read that finds a
   * checkpoint, WAL, or seed but cannot parse/replay it throws -- a corrupt
   * durable copy is a correctness failure that must not silently start the
   * agent fresh.
   */
  restoreFromSubstrate(): Promise<boolean>;
  /**
   * Commit the local store's new turn(s) to the substrate as O(1) WAL
   * appends, folding into a fresh checkpoint when the WAL reaches the
   * compaction interval. Called synchronously at the run boundary (after
   * the agent's send settles). A write failure surfaces.
   */
  mirrorToSubstrate(): Promise<void>;
  /**
   * Advance the connector router from a received inbound message so the
   * warm agent's reply path has thread state. Runs the router's pure
   * `route()` then `commit()`: a `start` seeds threadRoot / lastMessageId /
   * replyTo from the message; a `continue` advances lastMessageId / replyTo
   * and carries prior speakers into `cc`. The advanced connector state is
   * flushed into the local store's metadata so the run-boundary mirror
   * persists it and a respawn restore re-seeds the router. A `passthrough`
   * decision -- no active-thread match, or an unparseable sender -- advances
   * nothing. Called before the warm agent's send so `composeReply()` can
   * compose a threaded reply. A metadata write failure surfaces.
   */
  seedInbound(message: InboundMessage): Promise<void>;
  /**
   * Produce the threading headers for a reply on the active connector
   * thread (the router's `composeReply`). Throws
   * `NoActiveConnectorThreadError` when no thread has been seeded. The warm
   * mail loop's reply drain reads this to address its outbound reply.
   */
  composeReply(): ConnectorReplyParts;
  /**
   * Advance the connector thread after a reply was sent. Forwards to the
   * router's `onReplySent` (which moves `lastMessageId` to the sent reply's
   * Message-ID so the next inbound continuation matches) and flushes the
   * advanced connector state into the local store's metadata the same way
   * `seedInbound` does, so `lastMessageId` persists across turns and across a
   * child respawn. Called by the warm mail loop's reply drain after its
   * outbound send settles. Throws `NoActiveConnectorThreadError` when no
   * thread is active -- advancing outbound state has no meaning without a
   * seeded thread. A metadata write failure surfaces.
   */
  onReplySent(receipt: SendReceipt): Promise<void>;
}

export async function createDurableConversationStore(
  opts: DurableConversationStoreOpts,
): Promise<DurableConversationStore> {
  await fs.promises.mkdir(opts.localStoreDir, { recursive: true });
  const baseStorage = await createIsogitStore(opts.localStoreDir, opts.signer);

  // Reuse the connector router + the harness storage-override seam. The
  // router's `onStateChanged` is the change-driven commit hook the design
  // names. `seedInbound` drives the router (route + commit) on each inbound
  // mail, so `onStateChanged` fires and enqueues a change-driven mirror
  // behind the seed on the shared serialization tail; the run-boundary
  // mirror still commits every boundary. Both triggers persist state, so a
  // dropped change-driven mirror is recoverable at the next boundary.
  const connectorRouter = createConnectorRouter({
    onStateChanged: () => {
      void mirrorToSubstrate().catch((cause) => {
        logger.error`connector-state-change conversation mirror failed for ${opts.stepId}: ${cause instanceof Error ? cause.message : String(cause)}`;
      });
    },
  });

  const statePrefix = workflowRunStepStatePrefix(opts.runId, opts.stepId);
  const legacyStatePrefix = workflowRunLegacyAgentStatePrefix(opts.stepId);
  const stampedAttempt =
    opts.lifetime.kind === "attempt" ? { attempt: opts.lifetime.attempt } : {};
  // Set when the next mirror folds the whole conversation into a fresh
  // checkpoint instead of appending a WAL entry: the substrate holds an
  // earlier attempt's state, which the fold drops in the same commit, or the
  // store started from a seed, whose turns would otherwise sit in one WAL
  // entry that every later append to its bucket carries along.
  let foldNextMirror = false;

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

  // Serialize the shared-counter critical section. Both `mirrorToSubstrate`
  // and `restoreFromSubstrate` read and advance the three counts above, and
  // either can re-enter the other: `connectorRouter.restore()` can fire
  // `onStateChanged` synchronously, which enqueues a mirror. A single
  // per-instance tail runs every such op one-at-a-time regardless of entry
  // point, so two overlapping runs cannot read the same boundary seq and
  // emit two WAL entries at it. Modeled on `runRepoOp` in the hub-agent
  // session manager: the stored tail swallows rejections so one failed op
  // does not poison the chain, while each caller still observes its own op's
  // result (or rejection) through the returned promise.
  //
  // This serializes mirror-vs-mirror and mirror-vs-restore only. It does NOT
  // address the reactor-vs-mirror peek-snapshot window documented on
  // `runMirror` below (nothing must append to the reactor's turn array
  // between its last writeTurns and the mirror's peekTurns) -- that is a
  // different concurrency axis and is out of scope here.
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

  function openCommittedReads(): Promise<CommittedReads | null> {
    return opts.substrate.openCommittedReads(
      opts.principal,
      opts.workflowRunRepoId,
      opts.workflowRunRef,
    );
  }

  /**
   * The committed state this store continues, or `null` to start over. An
   * attempt continues only state stamped with its own attempt; state from an
   * earlier attempt is replaced by this attempt's first mirror.
   */
  async function readOwnState(
    reads: CommittedReads | null,
  ): Promise<ReconstructedStepState | null> {
    const committed = await readCommittedState(reads);
    if (committed === null || opts.lifetime.kind === "deployment") {
      return committed;
    }
    const attempt = opts.lifetime.attempt;
    if (committed.attempt === attempt) return committed;
    if (committed.attempt !== undefined && committed.attempt > attempt) {
      throw new Error(
        `sidecar conversation-state: ${statePrefix} holds attempt ${String(committed.attempt)}, later than attempt ${String(attempt)}; refusing to replace it`,
      );
    }
    foldNextMirror = true;
    return null;
  }

  /**
   * Read the committed state. For a deployment-lifetime store this first
   * moves a deployment's legacy `agent-state/<stepId>/` copy into the state
   * directory. The move is two commits -- fold the legacy conversation into
   * a checkpoint in the state directory, then drop the legacy subtree -- and
   * the state directory wins once it exists, so a crash between them is
   * finished by the next read.
   */
  async function readCommittedState(
    reads: CommittedReads | null,
  ): Promise<ReconstructedStepState | null> {
    if (reads === null) return null;
    const current = await reconstructCommitted(reads, statePrefix, opts.stepId);
    if (opts.lifetime.kind === "attempt") return current;
    if (current !== null) {
      if ((await reads.listDir(legacyStatePrefix.slice(0, -1))).length > 0) {
        await dropLegacyState();
      }
      return current;
    }
    const legacy = await reconstructCommitted(
      reads,
      legacyStatePrefix,
      opts.stepId,
    );
    if (legacy === null) return null;
    await writeCheckpoint(legacy.boundaryCount, legacy.turns, {
      pendingOperations: legacy.pendingOperations,
      tokenUsage: legacy.tokenUsage,
      connectorState: legacy.connectorState,
    });
    await dropLegacyState();
    return { ...legacy, checkpointBoundarySeq: legacy.boundaryCount };
  }

  async function dropLegacyState(): Promise<void> {
    await opts.substrate.writeTreePreservingPrefix(
      opts.principal,
      opts.workflowRunRepoId,
      opts.workflowRunRef,
      {
        preservePrefix: legacyStatePrefix,
        merge: async () => ({}),
        message: `drop legacy agent-state conversation for ${opts.stepId} after moving it to ${statePrefix}`,
      },
    );
  }

  async function runRestore(): Promise<boolean> {
    // Establish the committed counts BEFORE the connector state is restored
    // into the router. `connectorRouter.restore()` can fire `onStateChanged`
    // synchronously (when the restored state differs from current), which
    // enqueues a mirror. Serialization already chains that mirror behind
    // this restore, but setting the counts first keeps them correct even if
    // that ordering guarantee is ever weakened. The counts reflect the
    // substrate state they were read from, which is durable independent of
    // the local-store commit.
    const reads = await openCommittedReads();
    const reconstructed = await readOwnState(reads);
    if (reconstructed !== null) {
      mirroredBoundaryCount = reconstructed.boundaryCount;
      mirroredTurnCount = reconstructed.totalTurns;
      checkpointBoundarySeq = reconstructed.checkpointBoundarySeq;
    } else {
      // No durable state of its own yet: the next mirror starts the WAL from
      // an empty checkpoint, so the committed counts are empty and the first
      // mirror appends from boundary seq 0.
      mirroredBoundaryCount = 0;
      mirroredTurnCount = 0;
      checkpointBoundarySeq = 0;
    }
    if (opts.lifetime.kind === "attempt") {
      // Writing the local state back re-seeds the store's turn snapshot and
      // connector buffer, which a fresh store instance starts without; the
      // next mirror then commits the turns the committed copy lacks.
      const local = await baseStorage.load();
      if (
        isLocalStateCurrent(
          local,
          reconstructed === null ? null : reconstructed.turns,
        )
      ) {
        await loadIntoLocalStore(
          local,
          `resume ${opts.stepId} from its local attempt store`,
        );
        return true;
      }
    }
    if (reconstructed !== null) {
      await loadIntoLocalStore(
        reconstructed,
        `restore conversation for ${opts.stepId} from substrate`,
      );
      return true;
    }
    const seed =
      reads === null
        ? null
        : await readCommittedStepStateSeed(reads, opts.runId, opts.stepId);
    if (seed === null) {
      // Turns the local store still holds are in no commit: a crash beat the
      // first mirror, or the Hub replaced the history that held them. Kept,
      // the next mirror would commit them into the step's history.
      await loadIntoLocalStore(
        {
          turns: [],
          pendingOperations: [],
          tokenUsage: {
            input: 0,
            output: 0,
            cacheRead: 0,
            cacheWrite: 0,
            thinking: 0,
          },
          connectorState: null,
        },
        `start ${opts.stepId} with no conversation`,
      );
      return false;
    }
    // The seed's turns are not yet the step's own state, so the first
    // mirror commits them along with the step's first turn.
    await loadIntoLocalStore(
      { ...seed, pendingOperations: [] },
      `start ${opts.stepId} from the state its deployment imported`,
    );
    foldNextMirror = true;
    return true;
  }

  /**
   * Write a conversation into the local store's working tree and commit, so
   * the agent's reactor `load()` reads it. `setConnectorState` buffers the
   * connector state for the metadata write; `restore()` mirrors it into the
   * router so a future change-driven mirror carries the right base.
   */
  async function loadIntoLocalStore(
    state: StepStateContent,
    message: string,
  ): Promise<void> {
    await baseStorage.writeTurns(state.turns);
    baseStorage.setConnectorState(state.connectorState);
    connectorRouter.restore(state.connectorState);
    await baseStorage.writeMetadata({
      pendingOperations: state.pendingOperations,
      tokenUsage: state.tokenUsage,
    });
    await baseStorage.commit({ message });
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
    metadata: StepStateMetadata,
  ): Promise<void> {
    const serialized = serializeStepStateWalEntry(boundarySeq, turns, metadata);
    const newPath = stepStateWalEntryPath(statePrefix, boundarySeq);
    await opts.substrate.writeTreePreservingPrefix(
      opts.principal,
      opts.workflowRunRepoId,
      opts.workflowRunRef,
      {
        preservePrefix: stepStateWalBucketPrefix(
          statePrefix,
          stepStateWalBucket(boundarySeq),
        ),
        merge: async (existing) => {
          const files: Record<string, string | Uint8Array> = {};
          for (const [blobPath, bytes] of existing) {
            files[blobPath] = bytes;
          }
          files[newPath] = serialized;
          return files;
        },
        message: `append conversation WAL boundary ${String(boundarySeq)} (${String(turns.length)} turn(s)) for ${opts.stepId}`,
      },
    );
  }

  /**
   * Fold the full conversation into a fresh checkpoint and truncate the
   * WAL in one atomic commit with the state directory as `preservePrefix`.
   * The merge returns ONLY the two checkpoint files and NO `wal/...` paths;
   * because the substrate's `clearPrefix` recursively removes the whole
   * state directory before writing the returned set, omitting the WAL
   * paths IS the truncate.
   */
  async function writeCheckpoint(
    boundarySeq: number,
    turns: unknown[],
    metadata: StepStateMetadata,
  ): Promise<void> {
    // `metadata` is the freshest snapshot (the current local-store
    // metadata, identical to the last appended WAL entry's metadata), so
    // the fold captures the latest metadata into checkpoint.meta -- a
    // restore from the post-fold checkpoint sees the same metadata the
    // pre-fold WAL tail would have yielded.
    const checkpoint = buildStepStateCheckpoint(statePrefix, boundarySeq, {
      turns,
      ...metadata,
    });
    await opts.substrate.writeTreePreservingPrefix(
      opts.principal,
      opts.workflowRunRepoId,
      opts.workflowRunRef,
      {
        preservePrefix: statePrefix,
        merge: async () => checkpoint,
        message: `compact conversation checkpoint at boundary ${String(boundarySeq)} (${String(turns.length)} turns) for ${opts.stepId}`,
      },
    );
  }

  async function runMirror(): Promise<void> {
    // Slice the new turns from the reactor's in-memory array (retained
    // by the local single-writer store at the last writeTurns) instead
    // of re-reading and re-parsing the whole turns.jsonl every
    // boundary; only the bounded metadata.json is read from disk. This
    // rests on a sequencing invariant the store cannot enforce: nothing
    // mutates the reactor's turn array between its last writeTurns and
    // this read. The mirror runs at onRunBoundary after send() settles,
    // and the reactor only appends inside a cycle (each ending in
    // writeTurns), so peekTurns() equals the on-disk state here.
    // Serializing the mirror entry points keeps two mirrors from
    // overlapping this read, but the reactor must still not append
    // between its writeTurns and this peek -- that axis is not serialized.
    const turns = baseStorage.peekTurns();
    const metadata = {
      ...(await baseStorage.loadMetadata()),
      ...stampedAttempt,
    };

    // First mirror in this store's lifetime that did not run through
    // `restoreFromSubstrate` (which sets the counts): learn the durable
    // counts from the substrate so the append starts at the right boundary
    // seq and never re-commits boundaries the substrate already holds.
    if (mirroredBoundaryCount === null) {
      const reconstructed = await readOwnState(await openCommittedReads());
      checkpointBoundarySeq = reconstructed?.checkpointBoundarySeq ?? 0;
      mirroredBoundaryCount = reconstructed?.boundaryCount ?? 0;
      mirroredTurnCount = reconstructed?.totalTurns ?? 0;
    }

    if (foldNextMirror) {
      const folded = turns.slice();
      const boundarySeq = mirroredBoundaryCount + 1;
      await writeCheckpoint(boundarySeq, folded, metadata);
      mirroredBoundaryCount = boundarySeq;
      checkpointBoundarySeq = boundarySeq;
      mirroredTurnCount = folded.length;
      foldNextMirror = false;
      return;
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
    const uncompacted = mirroredBoundaryCount - checkpointBoundarySeq;
    if (uncompacted >= CHECKPOINT_INTERVAL) {
      try {
        await writeCheckpoint(
          mirroredBoundaryCount,
          turns.slice(0, mirroredTurnCount),
          metadata,
        );
        checkpointBoundarySeq = mirroredBoundaryCount;
      } catch (cause) {
        if (uncompacted >= MAX_UNCOMPACTED_WAL) {
          throw new Error(
            `sidecar conversation-state: compacting ${statePrefix} failed with ${String(uncompacted)} WAL entries uncompacted`,
            { cause },
          );
        }
        logger.warn`conversation checkpoint for ${opts.stepId} failed at boundary ${String(mirroredBoundaryCount)}; retrying at the next boundary: ${cause instanceof Error ? cause.message : String(cause)}`;
      }
    }
  }

  // Classify an inbound message, treating a `route()` throw as passthrough.
  // The router throws when `message.headers.from` is not a parseable bare
  // addr-spec; per the router contract that is a passthrough (deliver the
  // message to the agent -- the caller's send is separate -- but do not
  // advance the thread), not a programmer error, so it must not fail the
  // seed. A synthesized passthrough decision commits as a no-op.
  function routeOrPassthrough(message: InboundMessage): RouteDecision {
    try {
      return connectorRouter.route(message);
    } catch (cause) {
      logger.warn`connector route for ${opts.stepId} could not parse the inbound sender; leaving the thread unadvanced: ${cause instanceof Error ? cause.message : String(cause)}`;
      return { kind: "passthrough" };
    }
  }

  // Advance the connector router from a received inbound message and flush
  // the resulting connector state into the local store's metadata. `commit`
  // fires the router's `onStateChanged`, which enqueues a change-driven
  // mirror behind this op on the shared serialization tail; because that
  // mirror reads the connector state from the local store's metadata (not
  // from the router), the metadata write below is what makes the seeded
  // state reach the substrate. The write preserves the reactor's staged
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
      message: `seed connector thread for ${opts.stepId}`,
    });
  }

  // Advance the connector thread after a reply was sent and flush the
  // resulting connector state into the local store's metadata. `onReplySent`
  // moves `lastMessageId` to the reply's Message-ID and fires the router's
  // `onStateChanged`, which enqueues a change-driven mirror behind this op on
  // the shared serialization tail; because that mirror reads the connector
  // state from the local store's metadata (not from the router), the metadata
  // write below is what makes the advanced state reach the substrate. The
  // write preserves the reactor's staged pendingOperations / tokenUsage -- an
  // outbound advance touches only connectorState. `onReplySent` throws when no
  // thread is active, which surfaces to the reply drain's failure callback
  // rather than persisting a phantom advance.
  async function runReplySent(receipt: SendReceipt): Promise<void> {
    connectorRouter.onReplySent(receipt);

    const metadata = await baseStorage.loadMetadata();
    baseStorage.setConnectorState(connectorRouter.snapshot());
    await baseStorage.writeMetadata({
      pendingOperations: metadata.pendingOperations,
      tokenUsage: metadata.tokenUsage,
    });
    await baseStorage.commit({
      message: `advance connector thread after reply for ${opts.stepId}`,
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
  /**
   * The deployment's one addressable top-level run. A warm agent's
   * conversation spans every message its deployment serves, so each store
   * files its state under this run rather than the run of whichever
   * message first built the agent.
   */
  runId: string;
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
 * per warm agent, keyed by its step id, built lazily and reused across the
 * agent's messages in the same child. The first `acquire` for a step builds
 * the store and restores its prior conversation from the substrate -- the
 * path that runs on the lazy first build AND on the respawn rebuild, so the
 * warm agent resumes its conversation across child respawn. The registry is
 * empty after a respawn (it lives in the child's address space); the
 * substrate is the durable mirror that survives.
 */
export interface DurableConversationRegistry {
  acquire(stepId: string): Promise<DurableConversationStore>;
  get(stepId: string): DurableConversationStore;
}

export function createDurableConversationRegistry(
  opts: DurableConversationRegistryOpts,
): DurableConversationRegistry {
  const stores = new Map<string, DurableConversationStore>();
  // De-dup concurrent first-acquires for the same step so two in-flight
  // step invocations for one warm agent never build two stores (which
  // would double-restore and split the durable mirror).
  const building = new Map<string, Promise<DurableConversationStore>>();

  function localStoreDir(stepId: string): string {
    return path.join(
      opts.dataDir,
      "agent-conversation-state",
      opts.workflowRunRepoId.id,
      encodeURIComponent(stepId),
    );
  }

  async function acquire(stepId: string): Promise<DurableConversationStore> {
    const existing = stores.get(stepId);
    if (existing !== undefined) return existing;
    const inFlight = building.get(stepId);
    if (inFlight !== undefined) return inFlight;
    const promise = (async () => {
      const store = await createDurableConversationStore({
        localStoreDir: localStoreDir(stepId),
        signer: opts.signer,
        substrate: opts.substrate,
        workflowRunRepoId: opts.workflowRunRepoId,
        workflowRunRef: opts.workflowRunRef,
        principal: opts.principal,
        runId: opts.runId,
        stepId,
        lifetime: { kind: "deployment" },
      });
      // Restore the prior conversation BEFORE the store is observable (and
      // before the warm agent's reactor `load()` reads it). On a genuine
      // first-ever run this is a no-op (no checkpoint/WAL yet); on a
      // respawn rebuild it pulls the pre-respawn conversation back from the
      // substrate (checkpoint + WAL replay). A restore failure surfaces --
      // a lost conversation on respawn is a correctness failure, not a
      // silently-fresh start.
      await store.restoreFromSubstrate();
      stores.set(stepId, store);
      building.delete(stepId);
      return store;
    })().catch((cause) => {
      building.delete(stepId);
      throw cause;
    });
    building.set(stepId, promise);
    return promise;
  }

  function get(stepId: string): DurableConversationStore {
    const store = stores.get(stepId);
    if (store === undefined) {
      throw new Error(
        `sidecar conversation-state: no durable conversation store for ${JSON.stringify(stepId)}; the run-boundary mirror ran before the warm agent's env was built`,
      );
    }
    return store;
  }

  return { acquire, get };
}

type CommittedStepStateArgs = {
  substrate: RepoStore;
  workflowRunRepoId: RepoId;
  workflowRunRef: string;
  principal: Principal;
  runId: string;
  stepId: string;
};

/**
 * Read a step's committed state from its state directory, without writing.
 * Returns `null` when the step has none.
 */
export async function readStepState(
  args: CommittedStepStateArgs,
): Promise<ReconstructedStepState | null> {
  const reads = await openStepStateReads(args);
  if (reads === null) return null;
  return reconstructCommitted(
    reads,
    workflowRunStepStatePrefix(args.runId, args.stepId),
    args.stepId,
  );
}

/**
 * Read a warm agent's committed conversation without writing: its state
 * directory, or, for a deployment that predates that layout and has not
 * restored since, the legacy `agent-state/<stepId>/` copy the next restore
 * moves. Returns `null` when neither exists.
 */
export async function readDurableConversation(
  args: CommittedStepStateArgs,
): Promise<ReconstructedStepState | null> {
  const reads = await openStepStateReads(args);
  if (reads === null) return null;
  return (
    (await reconstructCommitted(
      reads,
      workflowRunStepStatePrefix(args.runId, args.stepId),
      args.stepId,
    )) ??
    reconstructCommitted(
      reads,
      workflowRunLegacyAgentStatePrefix(args.stepId),
      args.stepId,
    )
  );
}

function openStepStateReads(
  args: CommittedStepStateArgs,
): Promise<CommittedReads | null> {
  return args.substrate.openCommittedReads(
    args.principal,
    args.workflowRunRepoId,
    args.workflowRunRef,
  );
}

/**
 * Reconstruct the step state under `stateDir` from a committed tree; see
 * `reconstructStepState`. Step state is always read from a commit, never from
 * the working tree the substrate materializes: a write removes and rewrites
 * its prefix in the working tree before it commits, so a crash in between, or
 * a commit that fails, leaves a working tree that matches no commit.
 */
function reconstructCommitted(
  reads: CommittedReads,
  stateDir: string,
  label: string,
): Promise<ReconstructedStepState | null> {
  return reconstructStepState(
    createCommittedStepStateReader(reads, stateDir),
    label,
  );
}

/**
 * Whether a cold step attempt's local store holds the attempt's current
 * state, given the attempt's committed turns (`null` when the attempt has
 * committed nothing). On the host that runs an attempt the local store never
 * trails what that host committed: a restore fills it before the agent
 * loads, the reactor commits every cycle to it, and the settle mirror commits
 * from it. It leads by the turn in flight when a crash beat the mirror. A
 * store that holds nothing was just created on this host, and one whose
 * turns no longer extend the committed ones was left by a host the attempt
 * has since moved away from; neither is current.
 */
export function isLocalStateCurrent(
  local: {
    readonly turns: readonly unknown[];
    readonly pendingOperations: readonly unknown[];
  },
  committedTurns: readonly unknown[] | null,
): boolean {
  if (local.turns.length === 0 && local.pendingOperations.length === 0) {
    return false;
  }
  if (committedTurns === null) return true;
  if (local.turns.length < committedTurns.length) return false;
  return committedTurns.every(
    (turn, index) =>
      JSON.stringify(turn) === JSON.stringify(local.turns[index]),
  );
}

export function isErrnoNotFound(cause: unknown): boolean {
  if (cause === null || typeof cause !== "object") return false;
  const code = (cause as { code?: unknown }).code;
  return code === "ENOENT";
}
