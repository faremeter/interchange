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
//     caller's read and the merge under the substrate's per-repo lock.
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
} from "@intx/hub-sessions";
import { subscribeKind } from "@intx/hub-sessions";
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

const EVENT_FILENAME_RE = /^(0|[1-9][0-9]*)\.json$/;

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
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const dir = opts.substrate.getRepoDir(opts.repoId);
  const eventsDir = path.join(dir, RUNS_PREFIX, runId, EVENTS_DIR);
  let filenames: string[];
  try {
    filenames = await fs.readdir(eventsDir);
  } catch (cause) {
    if (isErrnoNotFound(cause)) return [];
    throw cause;
  }
  const entries: { seq: number; event: WorkflowEvent }[] = [];
  for (const name of filenames) {
    const match = EVENT_FILENAME_RE.exec(name);
    if (match === null) continue;
    const seqStr = match[1];
    if (seqStr === undefined) continue;
    const seqFromName = Number.parseInt(seqStr, 10);
    const raw = await fs.readFile(path.join(eventsDir, name), "utf8");
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch (cause) {
      throw new Error(
        `workflow-runtime: read ${opts.repoId.id}/${runId}/${EVENTS_DIR}/${name} is not valid JSON`,
        { cause },
      );
    }
    const envelope = OnDiskEnvelope(parsed);
    if (envelope instanceof type.errors) {
      throw new Error(
        `workflow-runtime: read ${opts.repoId.id}/${runId}/${EVENTS_DIR}/${name} envelope invalid: ${envelope.summary}`,
      );
    }
    if (envelope.seq !== seqFromName) {
      throw new Error(
        `workflow-runtime: read ${opts.repoId.id}/${runId}/${EVENTS_DIR}/${name} body.seq ${String(envelope.seq)} does not match filename seq ${String(seqFromName)}`,
      );
    }
    entries.push({
      seq: envelope.seq,
      event: onDiskToWorkflowEvent(envelope),
    });
  }
  entries.sort((a, b) => a.seq - b.seq);
  return entries.map((e) => e.event);
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
 * substrate's `subscribeKind` helper filters on.
 */
function workflowEventToOnDisk(
  event: WorkflowEvent,
  seq: number,
): Record<string, unknown> {
  const { kind, seq: _eventSeq, ...rest } = event;
  return { seq, type: kind, ...rest };
}

/**
 * Append one or more contiguous events to a run's event log in a
 * SINGLE durable commit. The events must carry strictly-monotonic,
 * gap-free seqs continuing the prior tree's tip (the first event's seq
 * is `priorLastSeq + 1`); the merge writes every `events/<seq>.json`
 * blob into one `writeTreePreservingPrefix` tree-rewrite, so N events
 * cost one tree-rewrite + one ref-advance instead of N. An empty
 * `events` array is a no-op. The seq-contiguity check is performed
 * against the supplied events themselves so a caller that buffered a
 * gap surfaces a single-writer conflict rather than a silent gap in
 * the tree.
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
  let seqConflict: { expected: number; supplied: number } | null = null;
  try {
    await opts.substrate.writeTreePreservingPrefix(
      opts.principal,
      opts.repoId,
      opts.ref,
      {
        preservePrefix: prefix,
        merge: async (existing) => {
          // The runtime body emits events at `state.lastSeq + 1` and
          // `emptyState.lastSeq = 0`, so the first append on an empty
          // events tree carries seq=1. The adapter mirrors that
          // convention: the prior tree's lastSeq is the maximum seq
          // observed under the prefix, or 0 when no events exist yet;
          // the expected next seq is `priorLastSeq + 1`. Every event
          // in the batch must continue contiguously from there.
          let priorLastSeq = 0;
          for (const filepath of existing.keys()) {
            const name = filepath.slice(prefix.length);
            const match = EVENT_FILENAME_RE.exec(name);
            if (match === null) continue;
            const seqStr = match[1];
            if (seqStr === undefined) continue;
            const seq = Number.parseInt(seqStr, 10);
            if (seq > priorLastSeq) priorLastSeq = seq;
          }
          const files: Record<string, string | Uint8Array> = {};
          for (const [k, v] of existing) files[k] = v;
          let expectedSeq = priorLastSeq + 1;
          for (const event of events) {
            if (event.seq !== expectedSeq) {
              // Capture the divergence and return an unchanged tree so
              // the substrate's commit short-circuits (the kind handler
              // accepts an empty diff against the same prior tree); the
              // throw happens outside the merge callback so the
              // adapter's error carries the full context. The empty
              // tree returned here preserves the existing prefix
              // bit-for-bit so the rollback path inside the substrate
              // does not need to fire.
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

function isErrnoNotFound(cause: unknown): boolean {
  if (cause === null || typeof cause !== "object") return false;
  const code = (cause as { code?: unknown }).code;
  return code === "ENOENT";
}
