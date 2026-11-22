// Durable conversation state for agent steps (design §3c, §4 Phase D1).
//
// An agent step's agent holds its multi-turn conversation in the reactor's
// in-memory turn buffer, backed by a per-step isogit `ContextStore` rooted
// per run/attempt. Every agent step builds its agent for each turn and closes
// it when the turn ends, so that local store alone does not carry the
// conversation to the next turn, a child respawn, or a replacement host.
//
// This module makes the step's conversation DURABLE in the workflow-run
// substrate (the single-writer proxy `RepoStore`, written through the
// supervisor). The durable copy lives under the deployment's top-level run,
// in the state directory of the step's key
// (`runs/<topLevelRunId>/steps/<key>/state/`). The key is the step's path of
// step ids joined with `.`, plus `[i]` for a map item (`stepPlaceKey`), so a
// top-level step's key is its step id. The Hub's copy of the repo carries
// the state to a replacement host.
//
// Each invocation opens the store scoped to that invocation
// (`DurableConversationLifetime`): it restores the key's committed
// conversation into the agent's local store BEFORE the agent's reactor loads,
// and is mirrored once the turn settles, so a conversational step continues
// across turns, respawns and failover. Many invocations share one key (loop
// iterations, onTrigger events, a childWorkflow step run again, retries, the
// turns of a one-step workflow), and each continues the conversation the last
// one committed. A turn that fails commits nothing, so a retry continues from
// the last committed turn. The committed state is stamped with the invocation
// that wrote it. An invocation continues its own state as it is; any other
// invocation drops the pending operations and parked reply targets, and rolls
// back a turn parked on an approval to that turn's start
// (`parkedTurnStart`), since only the invocation that parked can resume the
// gate. An earlier version of this store wrote state with no stamp. When
// such state is parked on a gate, an approval resume of that gate continues
// it as its own: the gate's correlation id names the park. State with no
// stamp records no `parkedTurnStart`, and a resume of its park that parks
// again cannot learn it. State parked on a gate with no `parkedTurnStart`
// cannot be rolled back, so another invocation starts the step over as if
// nothing were committed. On the host that ran the attempt,
// the per-attempt local store can be one turn ahead of the committed copy: a
// crash between the reactor's local commit and the mirror leaves that turn
// only there. A restore there keeps the local store while it still extends
// the state the invocation continues (`isLocalStateCurrent`), so the turn is
// not lost and a crash-recovered approval park resumes against the gate it
// recorded. The lead is only meaningful over the history it was built on,
// and that history is never replaced under it: the workflow-run pack
// restorer refuses Hub history that would move a branch the host already
// holds.
//
// A step can carry a seed: a snapshot the Hub commits beside its state
// directory for the step to start from. A restore that continues no committed
// state starts from the seed instead, and the first mirror commits the
// seeded turns as the step's own state. A restore that finds neither, and no
// current attempt lead, starts the step with no conversation: the committed
// history, not the local store, decides what the agent continues.
//
// The conversation is committed in the step-state format that
// `@intx/hub-sessions/substrate` defines: a compacted checkpoint plus an
// append-only, bucket-sharded WAL keyed by mirror boundary, so each
// boundary's durable write carries only its new turns instead of
// re-serializing the whole conversation. `mirrorToSubstrate` writes exactly
// one WAL entry per boundary, even a turnless one, so metadata advanced
// without a new turn is still durable; a mirror that must fold the whole
// conversation (see `foldNextMirror`) writes a fresh checkpoint instead.
// Compaction every CHECKPOINT_INTERVAL
// boundaries (K = 64, i.e. once the live WAL reaches K entries) folds the
// WAL into a fresh checkpoint carrying the freshest metadata and truncates
// the WAL, so per-boundary durable cost is ~O(1) amortized. A fold that
// fails is retried at the next boundary, so the WAL can outgrow K until one
// lands (see MAX_UNCOMPACTED_WAL). K is a constant here; the design flags it
// as measurement-tunable. Restore reconstructs through the same format
// module, so it rebuilds the exact turn list and metadata the mirror
// committed.
//
// Substrate-merge constraint (load-bearing, design §4 "Substrate-merge
// note"). `writeTreePreservingPrefix`'s `merge` callback receives only
// the DIRECT CHILDREN of `preservePrefix`, and the substrate recursively
// removes the whole `preservePrefix` subtree before writing the merge's
// returned set (paths outside the prefix pass through untouched). A WAL blob is two levels below the
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
//     clear at the state directory drops the entire WAL subtree in
//     the same atomic commit. The truncate needs no nested read: omitting
//     the WAL paths from the returned set IS the truncate.
//
// Persistence sink (the riskiest part, design §6). The store keeps its own
// connector router and persists the router's `snapshot()` with the
// conversation; a restore hands the committed connector state to the
// router's `restore()`. The harness's `createWrappedStorageOverrides` is not
// used: the sink is the workflow-run substrate, not the agent's local
// isogit store. Both the WAL append and the checkpoint write route through
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
// boundary (the step conversation's `settle` -> `mirrorToSubstrate`), so
// every turn is still durably committed before the next message is
// processed. D1 changes WHAT the write does (O(1) append instead of O(N)
// whole-blob), not WHEN it happens. The run log is NOT yet a durable
// backstop for the turn (it carries a constant ref, design rev-2 FACT 1),
// so the conversation copy here remains the sole durable copy of the
// agent's per-turn output -- which is exactly why D1 keeps the write
// synchronous.
// The async flusher, run-log enrichment, and crash reconciliation are
// later, conditional phases (D2-D4), not done here.
//
// Commit granularity. The step invoker mirrors the store once per turn,
// after the turn settles; a change of the connector state does not mirror
// it. The seed and the park advance the connector router and the local
// store, so the turn's mirror commits them with the turn's conversation. The
// reply advances only the connector router: the mirror takes the connector
// state from the router, and the local store records the sent reply after
// the mirror commits it (see `saveSentReplyLocally`).
//
// Defensive: a restore that finds a checkpoint or WAL but cannot
// parse/replay it THROWS (a lost or corrupt conversation on respawn is a
// correctness failure, not a silently-fresh start). A mirror write failure
// surfaces so a dropped durability write is visible rather than leaving
// the next respawn to read a stale snapshot.

import fs from "node:fs";

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
  StepStateInvocation,
  StepStateMetadata,
} from "@intx/hub-sessions/substrate";
import {
  buildStepStateCheckpoint,
  createCommittedStepStateReader,
  isSameStepStateInvocation,
  readCommittedStepStateSeed,
  reconstructStepState,
  serializeStepStateWalEntry,
  stepStateWalBucket,
  stepStateWalBucketPrefix,
  stepStateWalEntryPath,
  workflowRunStepStatePrefix,
} from "@intx/hub-sessions/substrate";
import type {
  AuditStore,
  ContextStore,
  InboundMessage,
  SendReceipt,
} from "@intx/types/runtime-core";
import { emptyTokenUsage } from "@intx/types/runtime-core";

const logger = getLogger(["sidecar", "workflow-child", "conversation-state"]);

/**
 * Compaction interval: fold the WAL into a fresh checkpoint once it holds
 * this many entries, one per mirror boundary, since the last checkpoint.
 * Bounds the WAL tail (and so the restore-replay length) between
 * checkpoints. Measurement-tunable (design §6, open question 4); D1 fixes
 * it at 64.
 */
const CHECKPOINT_INTERVAL = 64;

/**
 * How long the WAL may grow while compaction keeps failing. A failed
 * compaction loses nothing -- the boundary's WAL entry is already committed
 * -- so the mirror retries it at the next boundary instead of failing the
 * turn. At this length every retry since the interval has failed, so the
 * next boundary folds instead of appending, and its turn fails if that fold
 * does, rather than grow the restore replay without bound.
 */
const MAX_UNCOMPACTED_WAL = 2 * CHECKPOINT_INTERVAL;

/**
 * What a store's durable state belongs to. A step's store serves one
 * invocation, named by its run, its scoped step id and its attempt, and
 * stamps the state with it. An approval resume also names the correlation id
 * of the gate it resumes (`resumes`). The invocation continues the
 * conversation its key holds; when another invocation wrote that state, the
 * store drops that invocation's pending operations and parked reply targets
 * and rolls back a turn it left parked on an approval.
 */
export type DurableConversationLifetime = {
  readonly kind: "invocation";
  readonly invocation: StepStateInvocation;
  readonly resumes?: string;
};

export interface DurableConversationStoreOpts {
  /**
   * Local per-agent isogit store root. It must be unique to the
   * invocation, so another invocation, a retried attempt included, never
   * loads an earlier invocation's store and continues from the committed
   * state instead. The substrate is the cross-respawn durable mirror of this
   * store's conversation content.
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
   * The deployment's top-level run; the state lives under its `steps/`
   * directory.
   */
  runId: string;
  /**
   * The step segment under the run: the key of the step's place
   * (`stepPlaceKey`). A top-level step's key is its step id.
   */
  stepId: string;
  lifetime: DurableConversationLifetime;
  /**
   * The deployment mailbox address the step's agent receives mail at. The
   * connector router keeps it out of the thread's addressed recipients.
   */
  mailboxAddress: string;
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
   * The store the step agent's env binds as `storage` and `audit`. It is
   * both `ContextStore` (conversation + connector state) and
   * `AuditStore` (tool-authorization records).
   */
  readonly storage: ContextStore & AuditStore;
  /**
   * Pull the prior conversation from the substrate (checkpoint + WAL-tail
   * replay) into the local store so the agent's reactor `load()` sees it.
   * The store continues the committed state, rolled back when another
   * invocation wrote it, and keeps its local store instead while that is
   * still current (see `isLocalStateCurrent`). A step that continues no
   * committed state starts from its seed, when there is one, and with no
   * conversation when there is none, whatever its local store held. A
   * committed `parkedTurnStart` past the committed turns throws. Called
   * before the agent is built. Returns `false` only when the step starts
   * with no conversation. A read that finds a checkpoint, WAL, or seed but
   * cannot parse/replay it throws -- a corrupt durable copy is a correctness
   * failure that must not silently start the agent fresh.
   */
  restoreFromSubstrate(): Promise<boolean>;
  /**
   * Commit the local store's new turn(s) to the substrate as O(1) WAL
   * appends, folding into a fresh checkpoint when the WAL reaches the
   * compaction interval. Called synchronously at the run boundary (after
   * the agent's send settles). A write failure surfaces, and so does a
   * mirror attempted while the local store's last checkpoint has failed.
   * The mirror commits the connector state the router holds. After the
   * commit, it records a sent reply's thread advance in the local store, and
   * logs a failure of that record as a warning.
   */
  mirrorToSubstrate(): Promise<void>;
  /**
   * Advance the connector router from a received inbound message so the
   * step agent's reply path has thread state. Runs the router's pure
   * `route()` then `commit()`; the router owns what each decision does to
   * the thread. The advanced connector state is flushed into the local
   * store's metadata. The run-boundary mirror commits it from the router,
   * and a respawn restore re-seeds the router from the commit. Called before
   * the step agent's send so `composeReply()` can compose a threaded reply.
   * A start or continue sets the router's turn mode to `thread`. A passthrough message leaves
   * the thread unchanged and sets the turn mode to `passthrough`, with its
   * sender as the reply target. The router holds the turn mode in memory
   * until `parkTurn` files it. A metadata write failure surfaces.
   */
  seedInbound(message: InboundMessage): Promise<void>;
  /**
   * Mark the start of a step agent turn (the router's `beginTurn`). A turn
   * that resumes the approval gate `turn.resume` takes the turn mode
   * `parkTurn` filed under that correlation id, and the removal of the
   * filed mode is flushed into the local store's metadata. Any other turn
   * sets the turn mode to `none`, so a turn that seeds no mail replies to
   * nobody: not to an earlier passthrough sender, and not on the thread.
   * Called before `seedInbound` on every step turn. A metadata write
   * failure surfaces.
   */
  beginTurn(turn: { resume?: string }): Promise<void>;
  /**
   * Park the current turn on the approval gate `correlationId` (the
   * router's `parkTurn`): file the turn mode under that id and flush it
   * into the local store's metadata. The run-boundary mirror commits it from
   * the router, so the approval resume answers the same mail, also after a
   * child respawn. A turn that answers no mail writes nothing. A metadata
   * write failure surfaces.
   */
  parkTurn(correlationId: string): Promise<void>;
  /**
   * Whether the current turn answers mail (the router's
   * `turnAnswersMail`): its input was mail, or it resumes a parked turn
   * whose input was mail.
   */
  turnAnswersMail(): boolean;
  /**
   * Produce the threading headers for a reply (the router's
   * `composeReply`): to the turn's passthrough sender when a passthrough
   * seed set one, and on the active connector thread when a start or
   * continue seed set the mode. Returns `null` when the passthrough sender
   * has no address. Throws `TurnAnswersNoMailError` when the turn answers
   * no mail, and `NoActiveConnectorThreadError` when a thread turn has no
   * active thread. The step invoker reads this to address the reply
   * of its turn.
   */
  composeReply(): ConnectorReplyParts | null;
  /**
   * Advance the connector thread after a reply was sent. Forwards to the
   * router's `onReplySent`, which makes the sent reply the parent of the
   * next one: `lastMessageId` becomes its Message-ID, so the next inbound
   * continuation matches, and `lastReferences` becomes `references`, the
   * chain it carried. A reply to a passthrough sender leaves the thread
   * unchanged. The store writes nothing here: the turn's mirror commits
   * the advance from the router, and only then does the local store record
   * it. Called by the step invoker after the outbound send of its turn's
   * reply settles. Throws `TurnAnswersNoMailError` when the turn answers no
   * mail, and `NoActiveConnectorThreadError` when a thread turn has no active
   * thread -- advancing outbound state has no meaning without a seeded
   * thread. Such a throw leaves the thread as it was.
   */
  onReplySent(
    receipt: SendReceipt,
    references: readonly string[],
  ): Promise<void>;
}

export async function createDurableConversationStore(
  opts: DurableConversationStoreOpts,
): Promise<DurableConversationStore> {
  await fs.promises.mkdir(opts.localStoreDir, { recursive: true });
  const baseStorage = await createIsogitStore(opts.localStoreDir, opts.signer);

  // The turn's mirror takes the connector state from the router.
  const connectorRouter = createConnectorRouter({
    ownAddress: opts.mailboxAddress,
  });

  const statePrefix = workflowRunStepStatePrefix(opts.runId, opts.stepId);
  const stamped = { invocation: opts.lifetime.invocation };
  // Set when the next mirror folds the whole conversation into a fresh
  // checkpoint instead of appending a WAL entry: the substrate holds another
  // invocation's state, which the fold drops in the same commit, or the
  // store holds turns none of its commits do yet -- a seed's, or a local
  // copy's that resumed before its first mirror -- which would otherwise sit
  // in one WAL entry that every later append to its bucket carries along.
  let foldNextMirror = false;

  // The turn count at the start of the current logical turn, which a mirror
  // records as `parkedTurnStart` when the turn parked on an approval. An
  // approval resume continues the turn it resumes, so it keeps the start.
  // `null` until a restore or a turn sets it, and while it is unknown (see
  // `startOfTurn`).
  let turnStart: number | null = null;

  // Set when the store advanced the connector thread for a sent reply and
  // its local store does not record the advance yet. The next mirror records
  // it there after it commits.
  let sentReplyUnsaved = false;

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
  // the connector ops write the local store's metadata that a mirror reads.
  // A single per-instance tail runs every such op one-at-a-time regardless
  // of entry point, so two overlapping runs cannot read the same boundary
  // seq and emit two WAL entries at it. The stored tail swallows rejections
  // so one failed op does not poison the chain, while each caller still
  // observes its own op's result (or rejection) through the returned promise.
  //
  // This serializes the store's own ops: mirror, restore, and the connector
  // ops. It does NOT address the reactor-vs-mirror peek-snapshot window
  // documented on `runMirror` below (nothing must append to the reactor's
  // turn array between its last writeTurns and the mirror's peekTurns) --
  // that is a different concurrency axis and is out of scope here.
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

  function beginTurn(turn: { resume?: string }): Promise<void> {
    return serializeStateOp(() => runBeginTurn(turn));
  }

  function parkTurn(correlationId: string): Promise<void> {
    return serializeStateOp(() => runParkTurn(correlationId));
  }

  function turnAnswersMail(): boolean {
    return connectorRouter.turnAnswersMail();
  }

  function composeReply(): ConnectorReplyParts | null {
    return connectorRouter.composeReply();
  }

  function onReplySent(
    receipt: SendReceipt,
    references: readonly string[],
  ): Promise<void> {
    return serializeStateOp(() => runReplySent(receipt, references));
  }

  function openCommittedReads(): Promise<CommittedReads | null> {
    return opts.substrate.openCommittedReads(
      opts.principal,
      opts.workflowRunRepoId,
      opts.workflowRunRef,
    );
  }

  /**
   * The state this store continues from its key's committed state, or
   * `null` when it continues none. An invocation continues its own state as
   * it is (`isOwnStepState`); that is a resume of the invocation, such as an
   * approval resume. Its own state is the state it stamped, or state with no
   * stamp that is parked on the gate it resumes. Any other invocation
   * continues the committed conversation with the pending operations and
   * parked reply targets of the invocation that wrote it dropped, since they
   * name gates only that invocation can resume. When that invocation's last
   * turn parked on an approval, the conversation is rolled back to the start
   * of that turn: the park was never answered, and a turn left on a tool
   * call with no result cannot be continued. When that turn parked on a gate
   * but the state records no `parkedTurnStart`, which state with no stamp,
   * or a resume of such a park that parks again, does, the turn's start is
   * unknown. Such state returns `null`: the invocation starts over from the
   * seed, or with no conversation, and its first mirror folds over the
   * committed state. A later attempt of the same run and step is never
   * continued over.
   *
   * This relies on two invocations of one key never mirroring at the same
   * time: onTrigger drives one body at a time, loop iterations and map items
   * run in sequence, and a retry starts after the failure it retries.
   */
  function continuedState(
    committed: ReconstructedStepState | null,
  ): ContinuedState | null {
    if (committed === null) return committed;
    const { parkedTurnStart } = committed;
    if (
      parkedTurnStart !== undefined &&
      parkedTurnStart > committed.turns.length
    ) {
      throw new Error(
        `sidecar conversation-state: ${statePrefix} records a parked turn starting at turn ${String(parkedTurnStart)}, past its ${String(committed.turns.length)} turns; refusing to restore corrupt step state`,
      );
    }
    const own = opts.lifetime.invocation;
    const stamp = committed.invocation;
    if (isOwnStepState(committed, own, opts.lifetime.resumes)) {
      return committed;
    }
    if (
      stamp !== undefined &&
      stamp.runId === own.runId &&
      stamp.stepId === own.stepId &&
      stamp.attempt > own.attempt
    ) {
      throw new Error(
        `sidecar conversation-state: ${statePrefix} holds attempt ${String(stamp.attempt)} of step ${stamp.stepId} in run ${stamp.runId}, later than attempt ${String(own.attempt)}; refusing to replace it`,
      );
    }
    if (
      parkedTurnStart === undefined &&
      committed.pendingOperations.some((op) => op.suspendedCall !== undefined)
    ) {
      return null;
    }
    return rollBack(committed);
  }

  /**
   * Adopt the key's committed state as the base of this store's next
   * mirror: learn the committed turn and checkpoint counts, and fold the next
   * mirror when the store continues a state other than the committed one.
   * Returns the state the store continues and the committed boundary count,
   * which the caller stores as `mirroredBoundaryCount`.
   */
  function adoptCommitted(committed: ReconstructedStepState | null): {
    continued: ContinuedState | null;
    boundaryCount: number;
  } {
    const continued = continuedState(committed);
    mirroredTurnCount = committed?.totalTurns ?? 0;
    checkpointBoundarySeq = committed?.checkpointBoundarySeq ?? 0;
    // A rolled-back state, or a start over from committed state, is not a
    // prefix the WAL can append to, so the first mirror folds it into a
    // fresh checkpoint.
    if (continued !== committed) foldNextMirror = true;
    return { continued, boundaryCount: committed?.boundaryCount ?? 0 };
  }

  async function readCommittedState(
    reads: CommittedReads | null,
  ): Promise<ReconstructedStepState | null> {
    if (reads === null) return null;
    return reconstructCommitted(reads, statePrefix, opts.stepId);
  }

  async function runRestore(): Promise<boolean> {
    // Establish the committed counts before the connector state is restored
    // into the router. The counts reflect the substrate state they were read
    // from, which is durable independent of the local-store commit.
    const reads = await openCommittedReads();
    const adopted = adoptCommitted(await readCommittedState(reads));
    mirroredBoundaryCount = adopted.boundaryCount;
    const { continued } = adopted;
    // The seed applies only when the store continues no committed state.
    const seed =
      continued !== null || reads === null
        ? null
        : await readCommittedStepStateSeed(reads, opts.runId, opts.stepId);
    // The seed's turns are not yet the step's own state, so the first
    // mirror commits them along with the step's first turn.
    const seeded: StepStateContent | null =
      seed === null
        ? null
        : { ...seed, pendingOperations: [], parkedReplyTargets: {} };
    if (seeded !== null) foldNextMirror = true;
    const base = continued ?? seeded;
    turnStart = startOfTurn(base);
    // Writing the local state back re-seeds the store's turn snapshot and
    // connector buffer, which a fresh store instance starts without; the
    // next mirror then commits the turns the committed copy lacks. The
    // local store is compared with the state this invocation continues, so
    // a park recorded locally over a rolled-back state is kept.
    const local = {
      turns: (await baseStorage.load()).turns,
      ...(await baseStorage.loadMetadata()),
    };
    if (isLocalStateCurrent(local, base === null ? null : base.turns)) {
      await loadIntoLocalStore(
        local,
        `resume ${opts.stepId} from its local attempt store`,
      );
      if (base === null) foldNextMirror = true;
      return true;
    }
    if (continued !== null) {
      await loadIntoLocalStore(
        continued,
        `restore conversation for ${opts.stepId} from substrate`,
      );
      return true;
    }
    if (seeded === null) {
      // The local store holds no turns and no pending operations here, or it
      // would be current and the restore would have returned above. The
      // write clears any other state it holds, such as a connector state.
      await loadIntoLocalStore(
        {
          turns: [],
          pendingOperations: [],
          tokenUsage: emptyTokenUsage(),
          connectorState: null,
          parkedReplyTargets: {},
        },
        `start ${opts.stepId} with no conversation`,
      );
      return false;
    }
    await loadIntoLocalStore(seeded, `start ${opts.stepId} from its seed`);
    return true;
  }

  /**
   * Write a conversation into the local store's working tree and commit, so
   * the agent's reactor `load()` reads it. `setConnectorState` and
   * `setParkedReplyTargets` buffer the connector state and the parked reply
   * targets for the metadata write; the router takes both so the next
   * connector op starts from the restored state. The metadata goes first: a
   * crash before the turns then leaves a store `isLocalStateCurrent`
   * refuses, where turns written alone would pass as current without their
   * pending operations.
   */
  async function loadIntoLocalStore(
    state: StepStateContent,
    message: string,
  ): Promise<void> {
    baseStorage.setConnectorState(state.connectorState);
    baseStorage.setParkedReplyTargets(state.parkedReplyTargets);
    connectorRouter.restore(state.connectorState);
    connectorRouter.restoreParkedReplyTargets(state.parkedReplyTargets);
    await baseStorage.writeMetadata({
      pendingOperations: state.pendingOperations,
      tokenUsage: state.tokenUsage,
    });
    await baseStorage.writeTurns(state.turns);
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
   * because the substrate recursively removes the whole
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
    await commitMirror();
    if (sentReplyUnsaved) await saveSentReplyLocally();
  }

  /**
   * Record a sent reply's thread advance in the local store, after the mirror
   * committed it. The record waits for the mirror because a failed local
   * write marks the local store's checkpoint failed, and the mirror then
   * refuses to commit the turn. Of the reads of the local store, only a
   * later restore of this invocation on this host reads the record, through
   * `isLocalStateCurrent`. When the write fails, or the process stops before
   * it, that restore continues the thread from the mail the reply answered,
   * and its next mirror commits that older thread over the committed
   * advance, so the committed thread no longer names the reply. The next
   * mail on the thread still continues it, because its References name the
   * thread.
   */
  async function saveSentReplyLocally(): Promise<void> {
    sentReplyUnsaved = false;
    try {
      await writeConnectorMetadata(
        `record the sent reply on the connector thread for ${opts.stepId}`,
      );
    } catch (cause) {
      logger.warn`the reply of ${opts.stepId} was sent and its turn committed, but the local copy of its thread record was not saved: ${cause instanceof Error ? cause.message : String(cause)}`;
    }
  }

  async function commitMirror(): Promise<void> {
    // After a failed local checkpoint the files lag the agent's state, and
    // mirroring them would commit that older state as this boundary's.
    if (baseStorage.hasFailedCheckpoint()) {
      throw new Error(
        `sidecar conversation-state: the local store for ${opts.stepId} failed its last checkpoint, so its state cannot be mirrored`,
      );
    }

    // Slice the new turns from the reactor's in-memory array (retained
    // by the local single-writer store at the last writeTurns) instead
    // of re-reading and re-parsing the whole turns.jsonl every
    // boundary; only the bounded metadata.json is read from disk. This
    // rests on a sequencing invariant the store cannot enforce: nothing
    // mutates the reactor's turn array between its last writeTurns and
    // this read. The mirror runs at the run boundary after send() settles,
    // and the reactor only appends inside a cycle (each ending in
    // writeTurns), so peekTurns() equals the on-disk state here.
    // Serializing the mirror entry points keeps two mirrors from
    // overlapping this read, but the reactor must still not append
    // between its writeTurns and this peek -- that axis is not serialized.
    const turns = baseStorage.peekTurns();
    const localMetadata = await baseStorage.loadMetadata();

    // First mirror in this store's lifetime that did not run through
    // `restoreFromSubstrate` (which sets the counts): learn the durable
    // counts from the substrate so the append starts at the right boundary
    // seq and never re-commits boundaries the substrate already holds.
    if (mirroredBoundaryCount === null) {
      const adopted = adoptCommitted(
        await readCommittedState(await openCommittedReads()),
      );
      mirroredBoundaryCount = adopted.boundaryCount;
      turnStart ??= startOfTurn(adopted.continued);
    }

    // A pending approval gate carries the call it re-runs
    // (`suspendedCall`); an async-tool marker does not. Only a park on a gate
    // records where its turn started, so another invocation can roll it back.
    const parkedOnGate = localMetadata.pendingOperations.some(
      (op) => op.suspendedCall !== undefined,
    );
    // The router owns the connector state. The local metadata does not hold
    // a sent reply's thread advance until after the mirror (see
    // `saveSentReplyLocally`).
    const metadata = {
      ...localMetadata,
      connectorState: connectorRouter.snapshot(),
      parkedReplyTargets: connectorRouter.parkedReplyTargets(),
      ...stamped,
      ...(parkedOnGate && turnStart !== null
        ? { parkedTurnStart: turnStart }
        : {}),
    };

    const uncompactedBefore = mirroredBoundaryCount - checkpointBoundarySeq;
    if (foldNextMirror || uncompactedBefore >= MAX_UNCOMPACTED_WAL) {
      const folded = turns.slice();
      const boundarySeq = mirroredBoundaryCount + 1;
      try {
        await writeCheckpoint(boundarySeq, folded, metadata);
      } catch (cause) {
        if (foldNextMirror) throw cause;
        throw new Error(
          `sidecar conversation-state: compacting ${statePrefix} failed with ${String(uncompactedBefore)} WAL entries uncompacted`,
          { cause },
        );
      }
      mirroredBoundaryCount = boundarySeq;
      checkpointBoundarySeq = boundarySeq;
      mirroredTurnCount = folded.length;
      foldNextMirror = false;
      return;
    }

    // Past the fold above, ONE WAL entry per mirror boundary -- even when no
    // new turns were added since the last mirror. The entry carries the
    // 0-or-more new turns plus the freshest metadata snapshot, so a
    // turnless-but-metadata-mutating boundary (e.g. a throwing send that
    // still advanced tokenUsage/pendingOperations, since the run boundary
    // runs in a finally) still durably commits its metadata. The payload is the
    // turn DELTA plus bounded metadata -- never the whole conversation, so
    // the O(N^2) growth stays gone. This is the only synchronous write per
    // boundary on the reply path, except at a compaction boundary.
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
        logger.warn`conversation checkpoint for ${opts.stepId} failed at boundary ${String(mirroredBoundaryCount)}; retrying at the next boundary: ${cause instanceof Error ? cause.message : String(cause)}`;
      }
    }
  }

  // Classify an inbound message, treating a `route()` throw as passthrough.
  // The router throws when the speaker (the authenticated sender, else
  // `message.headers.from`) is not a parseable bare addr-spec; per the router contract that is a passthrough (deliver the
  // message to the agent -- the caller's send is separate -- but do not
  // advance the thread), not a programmer error, so it must not fail the
  // seed. A synthesized passthrough decision names no sender, so its commit
  // leaves the turn with no reply address.
  function routeOrPassthrough(message: InboundMessage): RouteDecision {
    try {
      return connectorRouter.route(message);
    } catch (cause) {
      logger.warn`connector route for ${opts.stepId} could not parse the inbound sender; leaving the thread unadvanced: ${cause instanceof Error ? cause.message : String(cause)}`;
      return { kind: "passthrough" };
    }
  }

  // Advance the connector router from a received inbound message and flush
  // the resulting connector state into the local store's metadata. The
  // turn's mirror takes the connector state from the router; the write
  // records it in the local store, which a restore of this invocation on
  // this host continues while it is current. The write preserves the
  // reactor's staged pendingOperations / tokenUsage -- a seed advances only
  // connectorState.
  // A passthrough commit sets only the router's in-memory turn mode and
  // leaves the thread unchanged, so it writes nothing.
  async function runSeed(message: InboundMessage): Promise<void> {
    const decision = routeOrPassthrough(message);
    connectorRouter.commit(decision);
    if (decision.kind === "passthrough") return;
    await writeConnectorMetadata(`seed connector thread for ${opts.stepId}`);
  }

  // An approval resume removes the turn mode its parked turn filed, so
  // the removal is flushed the same way `runSeed` flushes a thread advance.
  // It reaches the substrate at this turn's run-boundary mirror, so a child
  // respawn before that mirror restores the mode still filed.
  //
  // A turn that is not an approval resume starts a new logical turn at the
  // store's current turn count; an approval resume continues the turn that
  // parked, so it keeps that turn's start.
  async function runBeginTurn(turn: { resume?: string }): Promise<void> {
    if (turn.resume === undefined) {
      turnStart = baseStorage.peekTurns().length;
    }
    if (!connectorRouter.beginTurn(turn)) return;
    await writeConnectorMetadata(`resume parked turn mode for ${opts.stepId}`);
  }

  async function runParkTurn(correlationId: string): Promise<void> {
    if (!connectorRouter.parkTurn(correlationId)) return;
    await writeConnectorMetadata(`park turn mode for ${opts.stepId}`);
  }

  // Flush the router's connector state and parked turn modes into the
  // local store's metadata and commit it. The write preserves the reactor's
  // staged pendingOperations / tokenUsage.
  async function writeConnectorMetadata(message: string): Promise<void> {
    const metadata = await baseStorage.loadMetadata();
    baseStorage.setConnectorState(connectorRouter.snapshot());
    baseStorage.setParkedReplyTargets(connectorRouter.parkedReplyTargets());
    await baseStorage.writeMetadata({
      pendingOperations: metadata.pendingOperations,
      tokenUsage: metadata.tokenUsage,
    });
    await baseStorage.commit({ message });
  }

  // Advance the connector thread after a reply was sent. `onReplySent` makes
  // the reply the thread's parent. The store does not write: the reply went
  // out, so its turn must commit, and a failed local write would stop the
  // mirror. The mirror commits the advance from the router and then records
  // it locally.
  // `onReplySent` throws when the turn answers no mail or when no thread is
  // active, which fails the reply's settlement in its persist phase rather
  // than persisting a phantom advance.
  async function runReplySent(
    receipt: SendReceipt,
    references: readonly string[],
  ): Promise<void> {
    connectorRouter.onReplySent(receipt, references);
    sentReplyUnsaved = true;
  }

  return {
    storage: baseStorage,
    restoreFromSubstrate,
    mirrorToSubstrate,
    seedInbound,
    beginTurn,
    parkTurn,
    turnAnswersMail,
    composeReply,
    onReplySent,
  };
}

type CommittedStepStateArgs = {
  substrate: RepoStore;
  workflowRunRepoId: RepoId;
  workflowRunRef: string;
  principal: Principal;
  /** The deployment's top-level run, whose `steps/` holds the state. */
  runId: string;
  /**
   * The step segment under the run: the key of the step's place
   * (`stepPlaceKey`). A top-level step's key is its step id.
   */
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
 * Whether committed step state belongs to the invocation `own`: the state
 * carries its stamp, or, when `resumes` names the approval gate the
 * invocation resumes, the state carries no stamp and is parked on that gate.
 * An earlier version of this store wrote state with no stamp. A gate's
 * correlation id names one park, so the gate identifies the invocation that
 * parked on it. Stamped state belongs only to
 * the invocation it names, whatever gates it holds.
 */
export function isOwnStepState(
  committed: ReconstructedStepState,
  own: StepStateInvocation,
  resumes: string | undefined,
): boolean {
  if (committed.invocation !== undefined) {
    return isSameStepStateInvocation(committed.invocation, own);
  }
  return (
    resumes !== undefined &&
    committed.pendingOperations.some(
      (op) => op.correlationId === resumes && op.suspendedCall !== undefined,
    )
  );
}

/** The state a store continues, with the start of its parked turn. */
type ContinuedState = StepStateContent & { parkedTurnStart?: number };

/**
 * Roll a committed state back for an invocation that did not write it: drop
 * the pending operations and parked reply targets, and, when the last turn
 * parked on an approval, the turns from that turn's start. The connector
 * state and token usage stay. Returns the state itself when it has nothing
 * to drop, so continuing it appends to the committed history instead of
 * rewriting it.
 */
function rollBack(committed: ReconstructedStepState): ContinuedState {
  if (
    committed.parkedTurnStart === undefined &&
    committed.pendingOperations.length === 0 &&
    Object.keys(committed.parkedReplyTargets).length === 0
  ) {
    return committed;
  }
  return {
    turns: committed.turns.slice(
      0,
      committed.parkedTurnStart ?? committed.turns.length,
    ),
    pendingOperations: [],
    tokenUsage: committed.tokenUsage,
    connectorState: committed.connectorState,
    parkedReplyTargets: {},
  };
}

/**
 * Where the turn a store continues starts, or `null` when it is unknown:
 * state parked on a gate that records no `parkedTurnStart`, which state with
 * no stamp holds. A turn start left unknown is never recorded, so a resume of
 * such a park that parks again leaves the state without one, and another
 * invocation starts over instead of continuing a tool call with no result.
 */
function startOfTurn(state: ContinuedState | null): number | null {
  if (state === null) return 0;
  if (state.parkedTurnStart !== undefined) return state.parkedTurnStart;
  if (state.pendingOperations.some((op) => op.suspendedCall !== undefined)) {
    return null;
  }
  return state.turns.length;
}

/**
 * Whether a step attempt's local store holds the attempt's current
 * state, given the turns of the state the attempt continues (`null` when
 * there are none). On the host that runs an attempt the local store never
 * trails what that host committed: a restore fills it before the agent
 * loads, the reactor commits every cycle to it, and the settle mirror commits
 * from it. It leads by the turn in flight when a crash beat the mirror. A
 * store that holds nothing was just created on this host, and one whose turns
 * no longer extend the committed ones was left by a host the attempt has
 * since moved away from; neither is current. A thrown turn can also leave a
 * lead, such as a park recorded locally before an abort or a failed
 * `parkTurn`. When a crash beats the step's failure record, same-host
 * recovery adopts that park, and `persistColdRecoveredPark` commits it before
 * the runtime records it, so a later host agrees.
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
