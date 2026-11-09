// Production `WorkflowRuntimeEnv.RepoStore` adapter.
//
// The runtime body sees the runtime-env shape: `read(runId)`,
// `append(runId, event)`, `subscribe(runId, opts)`. This adapter
// translates each call into operations against the workflow-run
// substrate (`@intx/hub-sessions` RepoStore plus the workflow-run kind
// handler) for a single deployment's workflow-run repo.
//
// On-disk envelope shape: every event blob committed under
// `runs/<runId>/events/<seq>.json` carries `{ seq, type, ...rest }`
// where `type` is the workflow-event discriminator (matching the
// substrate's `subscribeKind` `type` field and the workflow-run kind
// handler's `EventEnvelope` contract). The state-machine `WorkflowEvent`
// uses `kind` as its discriminator; the adapter performs the
// `kind` <-> `type` translation at the substrate boundary so the
// runtime body and state machine never see a mismatched discriminator.
//
// Append-result error translation (interface-decisions Bonus 1):
//   - `seq_conflict`: the caller supplied an `event.seq` that does not
//     match the seq computed from the substrate's prior tree. The
//     runtime body is the single writer to the run's event log; a
//     mismatch here means a parallel writer landed in between the
//     caller's read and the append under the substrate's per-repo lock.
//     Translated into a thrown Error naming the run and the diverging
//     seqs. No retries: the runtime decides higher up.
//   - `validate_failed`: the substrate's kind handler rejected the
//     prospective tree via `validatePush`. Translated into a thrown
//     Error carrying the handler's `reason` text. No retries.

import { type } from "arktype";

import type {
  Principal,
  RepoId,
  RepoStore as SubstrateRepoStore,
  WriteResult,
} from "@intx/hub-sessions/substrate";
import {
  classifyTerminalEvent,
  parseEventSeq,
  subscribeKind,
  WORKFLOW_RUN_EVENTS_FILE,
  splitCombinedEventLog,
} from "@intx/hub-sessions/substrate";
import type { RepoStore, WorkflowEvent } from "@intx/workflow";

/**
 * Local handle for the runtime-env subscribe options. The workflow
 * package's `RepoStore` interface declares the shape inline but does
 * not export the `SubscribeOpts` alias at the package root; reach in
 * via the parameter-utility so the adapter does not redeclare an
 * incompatible shape.
 */
type SubscribeOpts = Parameters<RepoStore["subscribe"]>[1];

const RUNS_PREFIX = "runs";
const EVENTS_DIR = "events";

/**
 * On-disk envelope shape committed under
 * `runs/<runId>/events/<seq>.json`. Carries the seq cross-check the
 * workflow-run kind handler validates against the filename, the `type`
 * discriminator the substrate's `subscribeKind` filters on, and an
 * open object for the rest of the workflow-event fields. Switching
 * from the catch-all `"+": "ignore"` to `"+": "delete"` would strip
 * unknown fields; the adapter wants them preserved so the round-trip
 * back into `WorkflowEvent` carries every state-machine field.
 */
const OnDiskEnvelope = type({
  seq: "number >= 0",
  type: "string",
  "[string]": "unknown",
});

/**
 * Every state-machine `WorkflowEvent` kind. Used to populate
 * `subscribeKind`'s `kinds` filter so the substrate's typed tail
 * surfaces every event blob the runtime cares about (the substrate
 * helper filters on the `type` field; an empty filter would yield
 * nothing).
 */
const ALL_WORKFLOW_EVENT_TYPES: readonly string[] = [
  "RunStarted",
  "StepStarted",
  "StepCompleted",
  "StepFailed",
  "AttemptScheduled",
  "SignalAwaited",
  "SignalReceived",
  "SignalAwaitAbandoned",
  "TimerSet",
  "TimerFired",
  "CancelRequested",
  "CancelPropagated",
  "ChildSpawned",
  "ChildCancelRequested",
  "ChildCompleted",
  "RunCompleted",
  "RunFailed",
  "RunCancelled",
];

export type WorkflowRunRepoStoreOpts = {
  /**
   * Substrate handle the adapter reads from and writes to. The caller
   * wires this against the substrate's registered workflow-run kind
   * handler -- the adapter's writes land under
   * `runs/<runId>/events/<seq>.json` and the handler's `validatePush`
   * is the layer that catches structural rejections.
   */
  substrate: SubstrateRepoStore;
  /**
   * Workflow-run repo identifying the owning deployment. A single
   * adapter instance services every run inside this deployment; the
   * adapter's `read` / `append` / `subscribe` calls take a `runId` to
   * route within the repo.
   */
  repoId: RepoId;
  /**
   * Principal the adapter presents to the substrate. The workflow-run
   * kind handler accepts a workflow-process principal scoped to the
   * deployment as the runtime body's writer; that is the principal
   * shape the production wiring supplies.
   */
  principal: Principal;
  /**
   * Principal used for a control-plane cancel append (a batch that is
   * entirely `CancelRequested`). The workflow-run kind handler requires a
   * `CancelRequested` be signed by a `supervisor` principal -- a
   * `workflow-process` principal may write run-body events but not a cancel.
   * An in-process child runs under real supervisor authority, so its host
   * supplies a supervisor principal here while run-body events keep their
   * `workflow-process` attribution. Absent when the writer issues no
   * in-process cancel, in which case a cancel fails loud at the push boundary
   * rather than being silently mis-attributed.
   */
  controlPlanePrincipal?: Principal;
  /**
   * Events ref the adapter reads from and writes to. The workflow-run
   * repo layout pins all `runs/<runId>/events/` blobs under a single
   * moving ref. Callers typically supply `"refs/heads/main"`.
   */
  ref: string;
};

/**
 * Construct the production `WorkflowRuntimeEnv.RepoStore` adapter for
 * the supplied deployment. The returned object satisfies the
 * runtime-env interface; the substrate handle and per-deployment
 * routing live in closure.
 */
export function createWorkflowRunRepoStore(
  opts: WorkflowRunRepoStoreOpts,
): RepoStore {
  return {
    async read(runId) {
      return readAllEventsForRun(opts, runId);
    },
    async append(runId, event) {
      await appendBatchEvents(opts, runId, [event]);
    },
    async appendBatch(runId, events) {
      await appendBatchEvents(opts, runId, events);
    },
    subscribe(runId, subOpts) {
      return subscribeRun(opts, runId, subOpts);
    },
  };
}

async function readAllEventsForRun(
  opts: WorkflowRunRepoStoreOpts,
  runId: string,
): Promise<readonly WorkflowEvent[]> {
  // Read the committed tree through the substrate, never the working
  // checkout under `getRepoDir`. `openCommittedReads` pins the ref to its
  // tip commit and serves every read from the git object store, so an
  // enumerate-then-read sequence is a single coherent snapshot even while
  // a concurrent append re-materializes the checkout. The prior
  // implementation read the working tree directly (raw readdir/readFile)
  // and raced that materialization: a blob `readdir` had just enumerated
  // could vanish before `readFile` on a contended filesystem, surfacing a
  // spurious ENOENT. Reading under the substrate's per-repo write lock was
  // the alternative considered and rejected -- it would serialize every
  // read behind the single writer and couple read latency to write
  // contention, whereas the pinned committed tree is lock-free and already
  // consistent because every append lands as exactly one commit.
  const reads = await opts.substrate.openCommittedReads(
    opts.principal,
    opts.repoId,
    opts.ref,
  );
  // Null mirrors the prior readdir-ENOENT contract: an uninitialised repo
  // or an unresolved ref holds no runs at all.
  if (reads === null) return [];

  const runDir = `${RUNS_PREFIX}/${runId}`;
  const runChildren = await reads.listDir(runDir);
  const decoder = new TextDecoder();
  const entries: { seq: number; event: WorkflowEvent }[] = [];

  // A terminated run is sealed into a single combined `events.jsonl`; an
  // in-flight run keeps per-event `events/<seq>.json` files. The two forms
  // are mutually exclusive; a run carrying both is a botched seal, and
  // silently reading only the combined file would mask it, so surface it.
  const combined = runChildren.find(
    (e) => e.type === "blob" && e.name === WORKFLOW_RUN_EVENTS_FILE,
  );
  const perEventDir = runChildren.find(
    (e) => e.type === "tree" && e.name === EVENTS_DIR,
  );
  if (combined !== undefined) {
    if (perEventDir !== undefined) {
      throw new Error(
        `workflow-runtime: run ${runId} carries both a combined ${WORKFLOW_RUN_EVENTS_FILE} and a per-event ${EVENTS_DIR}/ directory`,
      );
    }
    const combinedRaw = decoder.decode(await reads.readBlobByOid(combined.oid));
    for (const line of splitCombinedEventLog(combinedRaw)) {
      entries.push(
        parseEventEnvelope(
          line,
          `${opts.repoId.id}/${runId}/${WORKFLOW_RUN_EVENTS_FILE}`,
        ),
      );
    }
    entries.sort((a, b) => a.seq - b.seq);
    return entries.map((e) => e.event);
  }

  // Per-event form. `listDir` on an absent or non-tree path returns the
  // empty array, so a run with no events reads as the empty log.
  const eventBlobs = await reads.listDir(`${runDir}/${EVENTS_DIR}`);
  for (const child of eventBlobs) {
    if (child.type !== "blob") continue;
    const seqFromName = parseEventSeq(child.name);
    if (seqFromName === null) continue;
    const raw = decoder.decode(await reads.readBlobByOid(child.oid));
    const source = `${opts.repoId.id}/${runId}/${EVENTS_DIR}/${child.name}`;
    const entry = parseEventEnvelope(raw, source);
    if (entry.seq !== seqFromName) {
      throw new Error(
        `workflow-runtime: read ${source} body.seq ${String(entry.seq)} does not match filename seq ${String(seqFromName)}`,
      );
    }
    entries.push(entry);
  }
  entries.sort((a, b) => a.seq - b.seq);
  return entries.map((e) => e.event);
}

// Parse one on-disk event envelope -- a per-event file's bytes or one line
// of a combined `events.jsonl` (which holds the same bytes verbatim). The
// per-event caller additionally cross-checks the seq against the filename;
// the combined form carries the seq only in the body.
function parseEventEnvelope(
  raw: string,
  source: string,
): { seq: number; event: WorkflowEvent } {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new Error(`workflow-runtime: read ${source} is not valid JSON`, {
      cause,
    });
  }
  const envelope = OnDiskEnvelope(parsed);
  if (envelope instanceof type.errors) {
    throw new Error(
      `workflow-runtime: read ${source} envelope invalid: ${envelope.summary}`,
    );
  }
  return { seq: envelope.seq, event: onDiskToWorkflowEvent(envelope) };
}

/**
 * Translate a validated on-disk envelope (`{seq, type, ...rest}`) into
 * the state-machine `WorkflowEvent` shape (`{seq, kind, ...rest}`).
 * The state-machine `WorkflowEvent` discriminated union is narrowed by
 * the next layer (`applyEvent` / `resumeFromLog`); the adapter
 * surfaces a structural object that carries the discriminator under
 * its state-machine field name without re-asserting through the
 * discriminated union here.
 */
function onDiskToWorkflowEvent(
  envelope: typeof OnDiskEnvelope.infer,
): WorkflowEvent {
  const { seq, type: typeStr, ...rest } = envelope;
  const built: Record<string, unknown> = { ...rest, kind: typeStr, seq };
  // The runtime body and state machine read events through
  // `WorkflowEvent`'s `kind` discriminator; the adapter has confirmed
  // the on-disk envelope carries a string `type` and integer `seq`, so
  // the constructed object satisfies the discriminator contract. The
  // narrow against the discriminated-union variants lives in the
  // state machine (`applyEvent` / `resumeFromLog`), not here -- the
  // in-memory store in `runlocal` follows the same pattern and stores
  // `WorkflowEvent` objects opaquely. Synthesizing a 17-variant
  // arktype validator at the adapter layer would duplicate the
  // state-machine narrow.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- WorkflowEvent's discriminated union is narrowed downstream by the state machine; no runtime validator at this layer
  return built as unknown as WorkflowEvent;
}

/**
 * Translate a state-machine `WorkflowEvent` (using `kind` as the
 * discriminator) into the on-disk envelope shape (`{seq, type,
 * ...rest}`) the workflow-run kind handler validates and the
 * substrate's `subscribeKind` helper filters on. Exported so the
 * supervisor's terminal-commit path encodes a supervisor-authored
 * `RunFailed` through the same single source of the on-disk shape.
 */
export function workflowEventToOnDisk(
  event: WorkflowEvent,
  seq: number,
): Record<string, unknown> {
  const { kind, seq: _eventSeq, ...rest } = event;
  return { seq, type: kind, ...rest };
}

/**
 * A per-event file of a run's log in a parent tree. `oid` reads the
 * file's bytes through the delta's `readBlobByOid`.
 */
export type PriorRunEvent = { seq: number; oid: string };

type SubstrateDeltaReads = Parameters<
  Parameters<SubstrateRepoStore["writeTreeDelta"]>[3]["computeDelta"]
>[1];

/**
 * Append to a run's per-event log in one commit that writes only the
 * new event files. `compute` receives the run's events in the parent
 * tree, ascending by seq, and returns the files to add, keyed by path;
 * an empty result leaves the log unchanged. Reading the parent's file
 * names instead of its bytes keeps the blobs an append reads and writes
 * independent of how many events the run already holds; the events tree
 * is still listed and rewritten whole, so that part grows with the run.
 */
export async function appendRunEvents(args: {
  substrate: SubstrateRepoStore;
  principal: Principal;
  repoId: RepoId;
  ref: string;
  runId: string;
  message: string;
  compute: (
    events: readonly PriorRunEvent[],
    prior: SubstrateDeltaReads,
  ) => Promise<Record<string, string | Uint8Array>>;
}): Promise<WriteResult> {
  const dir = `${RUNS_PREFIX}/${args.runId}/${EVENTS_DIR}`;
  return args.substrate.writeTreeDelta(args.principal, args.repoId, args.ref, {
    changedPathPrefixes: new Set([`${dir}/`]),
    message: args.message,
    computeDelta: async (_parentCommitSha, prior) => {
      const events: PriorRunEvent[] = [];
      for (const entry of await prior.listDirOids(dir)) {
        const seq = parseEventSeq(entry.name);
        if (seq !== null) events.push({ seq, oid: entry.oid });
      }
      events.sort((a, b) => a.seq - b.seq);
      return { puts: await args.compute(events, prior), deletes: [] };
    },
  });
}

/**
 * What a duplicate or terminal check reads from one event: whether it is
 * terminal, the `signalId` of a `SignalReceived`, and the `timerId` of a
 * `TimerFired`. An event whose bytes are not a JSON object yields no
 * facts.
 */
type RunEventFacts = {
  terminal: boolean;
  signalId: string | undefined;
  timerFiredId: string | undefined;
};

/**
 * What a run's log holds that a signal or timer append must not repeat:
 * whether the run has ended, and the signal ids and fired timer ids it
 * already holds.
 */
export type RunEventSummary = {
  terminal: boolean;
  signalIds: ReadonlySet<string>;
  timerFiredIds: ReadonlySet<string>;
};

// A live run's summary and the last event it has read. Validation keeps
// every committed event unchanged, so a summary stays true for as long as
// the parent still holds the same blob at the cursor's seq; any other
// listing (a reset, a restore, a sealed log) drops it.
type CachedRunSummary = {
  cursor: PriorRunEvent;
  signalIds: Set<string>;
  timerFiredIds: Set<string>;
};

// One map per substrate store, dropped with it. A run's entry is dropped
// when this store writes or reads the run's terminal event. A run that
// ends through another process, or is drained without ending, keeps its
// small entry until its next signal or timer append or until the store
// goes.
const runSummariesByStore = new WeakMap<
  object,
  Map<string, CachedRunSummary>
>();

function runSummaryKey(repoId: RepoId, runId: string): string {
  return `${repoId.kind}/${repoId.id}/${runId}`;
}

/**
 * Summarize `events`, the run's per-event log in the parent tree in seq
 * order. A run's appends on one `substrate` read each event once: a call
 * reads only the events past the previous call's cursor. An ended run,
 * which keeps no summary, costs one read, of its last event.
 */
export async function readRunEventSummary(
  substrate: SubstrateRepoStore,
  repoId: RepoId,
  runId: string,
  events: readonly PriorRunEvent[],
  prior: SubstrateDeltaReads,
): Promise<RunEventSummary> {
  let runs = runSummariesByStore.get(substrate);
  if (runs === undefined) {
    runs = new Map();
    runSummariesByStore.set(substrate, runs);
  }
  const key = runSummaryKey(repoId, runId);
  const read = new Map<string, RunEventFacts>();
  const factsOf = async (event: PriorRunEvent): Promise<RunEventFacts> => {
    let facts = read.get(event.oid);
    if (facts === undefined) {
      facts = eventFactsOf(await prior.readBlobByOid(event.oid));
      read.set(event.oid, facts);
    }
    return facts;
  };

  let cached = runs.get(key);
  if (cached !== undefined && !listingHolds(events, cached.cursor)) {
    runs.delete(key);
    cached = undefined;
  }
  let unread = events;
  if (cached === undefined) {
    const last = events.at(-1);
    if (last === undefined) {
      return {
        terminal: false,
        signalIds: new Set(),
        timerFiredIds: new Set(),
      };
    }
    // A terminal event can only be the last, so an ended run is known
    // from that one read and keeps no summary.
    if ((await factsOf(last)).terminal) {
      return { terminal: true, signalIds: new Set(), timerFiredIds: new Set() };
    }
    cached = { cursor: last, signalIds: new Set(), timerFiredIds: new Set() };
  } else {
    const cursorSeq = cached.cursor.seq;
    unread = events.filter((event) => event.seq > cursorSeq);
  }
  for (const event of unread) {
    const facts = await factsOf(event);
    if (facts.terminal) {
      runs.delete(key);
      return {
        terminal: true,
        signalIds: cached.signalIds,
        timerFiredIds: cached.timerFiredIds,
      };
    }
    if (facts.signalId !== undefined) cached.signalIds.add(facts.signalId);
    if (facts.timerFiredId !== undefined) {
      cached.timerFiredIds.add(facts.timerFiredId);
    }
    cached.cursor = event;
  }
  runs.set(key, cached);
  return {
    terminal: false,
    signalIds: cached.signalIds,
    timerFiredIds: cached.timerFiredIds,
  };
}

/** Drop the summary of a run that has ended. */
function forgetRunEventSummary(
  substrate: SubstrateRepoStore,
  repoId: RepoId,
  runId: string,
): void {
  runSummariesByStore.get(substrate)?.delete(runSummaryKey(repoId, runId));
}

function listingHolds(
  events: readonly PriorRunEvent[],
  cursor: PriorRunEvent,
): boolean {
  for (let i = events.length - 1; i >= 0; i--) {
    const event = events[i];
    if (event === undefined || event.seq < cursor.seq) return false;
    if (event.seq === cursor.seq) return event.oid === cursor.oid;
  }
  return false;
}

const EventFactsBody = type({
  type: "string",
  "signalId?": "unknown",
  "timerId?": "unknown",
  "+": "ignore",
});

function eventFactsOf(bytes: Uint8Array): RunEventFacts {
  const none = {
    terminal: false,
    signalId: undefined,
    timerFiredId: undefined,
  };
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    // A corrupt blob is rejected by validatePush at write time.
    return none;
  }
  const body = EventFactsBody(parsed);
  if (body instanceof type.errors) return none;
  return {
    terminal: classifyTerminalEvent(body.type).terminal,
    signalId:
      body.type === "SignalReceived" && typeof body.signalId === "string"
        ? body.signalId
        : undefined,
    timerFiredId:
      body.type === "TimerFired" && typeof body.timerId === "string"
        ? body.timerId
        : undefined,
  };
}

/** The path of the per-event file at `seq` in a run's log. */
export function runEventPath(runId: string, seq: number): string {
  return `${RUNS_PREFIX}/${runId}/${EVENTS_DIR}/${String(seq)}.json`;
}

/**
 * Append one or more contiguous events to a run's event log in a
 * SINGLE durable commit. The events must carry strictly-monotonic,
 * gap-free seqs continuing the prior tree's tip (the first event's seq
 * is `priorLastSeq + 1`); N events cost one commit instead of N. An
 * empty `events` array is a no-op. The seq-contiguity check is
 * performed against the supplied events themselves so a caller that
 * buffered a gap surfaces a single-writer conflict rather than a silent
 * gap in the tree.
 */
async function appendBatchEvents(
  opts: WorkflowRunRepoStoreOpts,
  runId: string,
  events: readonly WorkflowEvent[],
): Promise<void> {
  if (events.length === 0) return;
  const firstEvent = events[0];
  if (firstEvent === undefined) throw new Error("unreachable");
  const lastEvent = events[events.length - 1];
  if (lastEvent === undefined) throw new Error("unreachable");
  // A control-plane cancel append (an isolated `CancelRequested`, which the
  // kind handler requires be signed by a supervisor principal) is written under
  // `controlPlanePrincipal` when the host supplied one; every other batch --
  // including run-body events -- keeps the workflow-process `principal`. A mixed
  // batch is never a cancel, so it stays on `principal`.
  const principal =
    opts.controlPlanePrincipal !== undefined &&
    events.every((event) => event.kind === "CancelRequested")
      ? opts.controlPlanePrincipal
      : opts.principal;
  let seqConflict: { expected: number; supplied: number } | null = null;
  try {
    await appendRunEvents({
      substrate: opts.substrate,
      principal,
      repoId: opts.repoId,
      ref: opts.ref,
      runId,
      message:
        events.length === 1
          ? `append workflow event ${firstEvent.kind} for run ${runId}`
          : `append ${String(events.length)} workflow events ${firstEvent.kind}..${lastEvent.kind} for run ${runId}`,
      compute: async (existing) => {
        // The runtime body emits events at `state.lastSeq + 1` and
        // `emptyState.lastSeq = 0`, so the first append on an empty
        // events tree carries seq=1. The adapter mirrors that
        // convention: the prior tree's lastSeq is the highest seq under
        // the run's events, or 0 when no events exist yet; the expected
        // next seq is `priorLastSeq + 1`. Every event in the batch must
        // continue contiguously from there.
        const priorLastSeq = existing.at(-1)?.seq ?? 0;
        const files: Record<string, string | Uint8Array> = {};
        let expectedSeq = priorLastSeq + 1;
        for (const event of events) {
          if (event.seq !== expectedSeq) {
            // Capture the divergence and write nothing, so the commit
            // leaves the log unchanged; the throw happens outside the
            // delta callback so the adapter's error carries the full
            // context.
            seqConflict = { expected: expectedSeq, supplied: event.seq };
            return {};
          }
          files[runEventPath(runId, expectedSeq)] = JSON.stringify(
            workflowEventToOnDisk(event, expectedSeq),
          );
          expectedSeq += 1;
        }
        return files;
      },
    });
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    if (message.startsWith("path_violation: ")) {
      const reason = message.slice("path_violation: ".length);
      throw new Error(reason, { cause });
    }
    throw cause;
  }
  if (
    seqConflict === null &&
    events.some((event) => classifyTerminalEvent(event.kind).terminal)
  ) {
    forgetRunEventSummary(opts.substrate, opts.repoId, runId);
  }
  if (seqConflict !== null) {
    const conflict: { expected: number; supplied: number } = seqConflict;
    throw new Error(
      `workflow-runtime: seq conflict on append to ${runId}; single-writer invariant violated (expected seq ${String(conflict.expected)} from prior tree, caller supplied ${String(conflict.supplied)})`,
    );
  }
}

async function* subscribeRun(
  opts: WorkflowRunRepoStoreOpts,
  runId: string,
  subOpts: SubscribeOpts,
): AsyncIterableIterator<{ seq: number; event: WorkflowEvent }> {
  // `subscribeKind` requires a `kinds` filter; supplying every known
  // workflow-event `type` keeps the runtime body's contract intact (it
  // wants every event for the run, not a subset). The substrate helper
  // also surfaces per-run attribution via the entry's `runId`, which
  // we use to filter just this run's events. Replay-then-live mode
  // mirrors `SubscribeOpts.from`: `"head"` emits only events committed
  // strictly after subscription, `{ seq }` enumerates prior events at
  // or after the supplied seq before transitioning to live.
  const kindOpts: Parameters<typeof subscribeKind>[5] = {
    signal: subOpts.signal,
    from: subOpts.from,
    kinds: ALL_WORKFLOW_EVENT_TYPES,
  };
  if (subOpts.bufferLimit !== undefined) {
    kindOpts.bufferLimit = subOpts.bufferLimit;
  }
  const iter = subscribeKind(
    opts.substrate,
    opts.principal,
    opts.repoId,
    opts.ref,
    OnDiskEnvelope,
    kindOpts,
  );
  for await (const entry of iter) {
    if (entry.runId !== runId) continue;
    const event = onDiskToWorkflowEvent(entry.event);
    yield { seq: entry.event.seq, event };
  }
}
