// Production `WorkflowRuntimeEnv.RepoStore` adapter: translates
// `read` / `append` / `subscribe` into operations against the
// workflow-run substrate for one deployment's repo. On-disk envelopes
// carry `{ seq, type, ...rest }` (`type` = workflow-event
// discriminator); the state-machine `WorkflowEvent` uses `kind`, and
// the adapter performs the `kind` <-> `type` translation at the
// substrate boundary.
//
// Error translation: `seq_conflict` (caller seq diverges from the
// prior tree -- a single-writer violation) throws naming the run and
// both seqs; `validate_failed` (kind-handler `validatePush`
// rejection) throws carrying the handler's `reason`. No retries: the
// runtime decides higher up.

import { type } from "arktype";

import type {
  Principal,
  RepoId,
  RepoStore as SubstrateRepoStore,
} from "@intx/hub-sessions/substrate";
import {
  parseEventSeq,
  subscribeKind,
  WORKFLOW_RUN_EVENTS_FILE,
  splitCombinedEventLog,
} from "@intx/hub-sessions/substrate";
import type { RepoStore, WorkflowEvent } from "@intx/workflow";

/**
 * Subscribe options from the workflow `RepoStore` interface, which does
 * not export the alias; reach in via the parameter-utility so the
 * adapter does not redeclare an incompatible shape.
 */
type SubscribeOpts = Parameters<RepoStore["subscribe"]>[1];

const RUNS_PREFIX = "runs";
const EVENTS_DIR = "events";

/**
 * On-disk envelope under `runs/<runId>/events/<seq>.json`: the seq the
 * kind handler cross-checks against the filename, the `type`
 * discriminator `subscribeKind` filters on, and an open object for the
 * rest of the workflow-event fields. `"+": "delete"` would strip
 * unknown fields; the round-trip into `WorkflowEvent` wants them kept.
 */
const OnDiskEnvelope = type({
  seq: "number >= 0",
  type: "string",
  "[string]": "unknown",
});

/** Every `WorkflowEvent` kind, for `subscribeKind`'s `kinds` filter. */
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
  /** Substrate handle the adapter reads from and writes to. */
  substrate: SubstrateRepoStore;
  /** Workflow-run repo identifying the owning deployment. */
  repoId: RepoId;
  /** Principal the adapter presents to the substrate. */
  principal: Principal;
  /**
   * Principal for a control-plane cancel append (a batch that is
   * entirely `CancelRequested`): the kind handler requires a cancel be
   * signed by a `supervisor` principal. Absent, a cancel fails loud at
   * the push boundary rather than being silently mis-attributed.
   */
  controlPlanePrincipal?: Principal;
  /** Ref the adapter reads from and writes to (typically `"refs/heads/main"`). */
  ref: string;
};

/** Construct the production `WorkflowRuntimeEnv.RepoStore` adapter. */
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
  // Read the pinned committed tree, never the lagging working checkout:
  // `openCommittedReads` serves one coherent snapshot even while a
  // concurrent append re-materializes the checkout. The prior direct
  // readdir/readFile raced that materialization (a just-enumerated blob
  // could vanish before readFile); the per-repo write lock was rejected
  // as it would serialize every read behind the single writer.
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

  // A terminated run is sealed into a combined `events.jsonl`; an
  // in-flight run keeps per-event `events/<seq>.json` files. The forms
  // are mutually exclusive; a run carrying both is a botched seal, so
  // surface it.
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

  // Per-event form; an absent path lists as the empty log.
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

// Parse one on-disk envelope (a per-event file's bytes or one line of a
// combined `events.jsonl`; the per-event caller also cross-checks the
// seq against the filename).
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
 * Translate a validated on-disk envelope into the state-machine
 * `WorkflowEvent` shape (`type` -> `kind`). The discriminated union is
 * narrowed downstream by `applyEvent` / `resumeFromLog`, not here.
 */
function onDiskToWorkflowEvent(
  envelope: typeof OnDiskEnvelope.infer,
): WorkflowEvent {
  const { seq, type: typeStr, ...rest } = envelope;
  const built: Record<string, unknown> = { ...rest, kind: typeStr, seq };
  // The on-disk envelope carries a string `type` and integer `seq`, so
  // the built object satisfies the discriminator contract; narrowing
  // the union variants lives in the state machine (`applyEvent` /
  // `resumeFromLog`), not here.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- WorkflowEvent's discriminated union is narrowed downstream by the state machine; no runtime validator at this layer
  return built as unknown as WorkflowEvent;
}

/**
 * Translate a state-machine `WorkflowEvent` into the on-disk envelope
 * shape (`kind` -> `type`). Exported so the supervisor's terminal-commit
 * path encodes a `RunFailed` through the same single source.
 */
export function workflowEventToOnDisk(
  event: WorkflowEvent,
  seq: number,
): Record<string, unknown> {
  const { kind, seq: _eventSeq, ...rest } = event;
  return { seq, type: kind, ...rest };
}

/**
 * Append contiguous events to a run's event log in a SINGLE durable
 * commit: strictly-monotonic, gap-free seqs continuing the prior tree's
 * tip, written as one `writeTreePreservingPrefix` tree-rewrite. An
 * empty array is a no-op.
 */
async function appendBatchEvents(
  opts: WorkflowRunRepoStoreOpts,
  runId: string,
  events: readonly WorkflowEvent[],
): Promise<void> {
  if (events.length === 0) return;
  const prefix = `${RUNS_PREFIX}/${runId}/${EVENTS_DIR}/`;
  const firstEvent = events[0];
  if (firstEvent === undefined) throw new Error("unreachable");
  const lastEvent = events[events.length - 1];
  if (lastEvent === undefined) throw new Error("unreachable");
  // A batch that is entirely `CancelRequested` is signed by
  // `controlPlanePrincipal` when supplied; everything else keeps the
  // workflow-process `principal`.
  const principal =
    opts.controlPlanePrincipal !== undefined &&
    events.every((event) => event.kind === "CancelRequested")
      ? opts.controlPlanePrincipal
      : opts.principal;
  let seqConflict: { expected: number; supplied: number } | null = null;
  try {
    await opts.substrate.writeTreePreservingPrefix(
      principal,
      opts.repoId,
      opts.ref,
      {
        preservePrefix: prefix,
        merge: async (existing) => {
          // The runtime body emits at `state.lastSeq + 1` (first
          // append on an empty tree carries seq=1); the expected next
          // seq is the prior tree's max seq plus one.
          let priorLastSeq = 0;
          for (const filepath of existing.keys()) {
            const name = filepath.slice(prefix.length);
            const seq = parseEventSeq(name);
            if (seq === null) continue;
            if (seq > priorLastSeq) priorLastSeq = seq;
          }
          const files: Record<string, string | Uint8Array> = {};
          for (const [k, v] of existing) files[k] = v;
          let expectedSeq = priorLastSeq + 1;
          for (const event of events) {
            if (event.seq !== expectedSeq) {
              // Record the divergence and return the tree unchanged so
              // the commit short-circuits; the throw happens outside
              // the merge callback so the error carries full context.
              seqConflict = { expected: expectedSeq, supplied: event.seq };
              const passthrough: Record<string, string | Uint8Array> = {};
              for (const [k, v] of existing) passthrough[k] = v;
              return passthrough;
            }
            const onDisk = workflowEventToOnDisk(event, expectedSeq);
            files[`${prefix}${String(expectedSeq)}.json`] =
              JSON.stringify(onDisk);
            expectedSeq += 1;
          }
          return files;
        },
        message:
          events.length === 1
            ? `append workflow event ${firstEvent.kind} for run ${runId}`
            : `append ${String(events.length)} workflow events ${firstEvent.kind}..${lastEvent.kind} for run ${runId}`,
      },
    );
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    if (message.startsWith("path_violation: ")) {
      const reason = message.slice("path_violation: ".length);
      throw new Error(reason, { cause });
    }
    throw cause;
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
  // `subscribeKind` requires a `kinds` filter; every known type keeps
  // the runtime body's contract (it wants every event for the run).
  // Entries are filtered to this run via `runId`; `from` semantics
  // mirror `SubscribeOpts.from` (`"head"` or a prior seq).
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
