import { type, type Type } from "arktype";
import {
  parseEventSeq,
  WORKFLOW_RUN_EVENTS_DIR,
  WORKFLOW_RUN_RUNS_PREFIX,
} from "../workflow-run-kind";
import type { CommittedReads, Principal, RepoId, RepoStore } from "./types";

/**
 * Substrate envelope shape. Imported textually rather than from
 * `types.ts` so this helper has a local, validator-checked view of
 * what `RepoStore.subscribe` yields and does not depend on the
 * substrate's TypeScript type for runtime safety.
 */
const SubstrateEvent = type({
  type: "'ref.updated'",
  ref: "string",
  "oldSha?": "string | null",
  newSha: "string",
});

type EventCandidate = {
  /** Workflow-event seq parsed from the filename. */
  seq: number;
  /** runId from the parent path segment. */
  runId: string;
  /** Repo-root-relative blob path. */
  blobPath: string;
  /** Git object id of the event blob, for a direct cache-backed read. */
  oid: string;
};

const KindEnvelope = type({
  type: "string",
});

export type SubscribeKindOpts = {
  signal: AbortSignal;
  from: "head" | { seq: number };
  /**
   * Workflow-event kinds to surface. Matches against the inner
   * payload's `type` field after the validator narrows the blob. The
   * helper yields only events whose `type` appears in this list.
   */
  kinds: readonly string[];
  bufferLimit?: number;
};

export type SubscribeKindEntry<T> = {
  /**
   * Workflow-event seq parsed from the committed filename
   * (`runs/<runId>/events/<seq>.json`). Distinct from the substrate's
   * commit-level seq exposed by `RepoStore.subscribe`.
   */
  seq: number;
  /**
   * Owning runId, parsed from the committed blob's path. The substrate
   * stores each run's event log under `runs/<runId>/events/`; the helper
   * surfaces the path segment so consumers can attribute each yielded
   * event to its run without re-walking the tree. Required by the
   * workflow-host scheduler's live-ingest path, which routes incoming
   * `TimerSet` events to per-run queue entries.
   */
  runId: string;
  event: T;
};

/**
 * Typed entrypoint over `RepoStore.subscribe` for the workflow-run
 * event log. The substrate emits one ref-update envelope per commit;
 * each commit may add one or more `runs/<runId>/events/<seq>.json`
 * blobs. This helper loads those blobs from the new commit's tree,
 * narrows each through the supplied arktype validator, applies the
 * kinds filter against the inner `type` discriminator, and yields one
 * `{ seq, event }` entry per matching blob.
 *
 * The path-layout vocabulary (the `runs/`/`events/` prefixes and the
 * `<seq>.json` filename shape) lives in the workflow-run kind handler,
 * which is the authority for what may land under this prefix; this
 * helper imports it rather than re-encoding it. The substrate
 * `subscribe` it wraps is a generic ref-tail primitive and knows
 * nothing about workflow-runs.
 *
 * Diff scope: the helper enumerates event blobs present in the new
 * commit but absent from the old commit. A commit that does not add
 * any event blobs (e.g. a kind-handler-internal rewrite) produces no
 * entries even if the kinds filter would otherwise match earlier
 * events.
 */
export async function* subscribeKind<V extends Type>(
  store: RepoStore,
  principal: Principal,
  repoId: RepoId,
  ref: string,
  validator: V,
  opts: SubscribeKindOpts,
): AsyncGenerator<SubscribeKindEntry<V["infer"]>, void, void> {
  const kindsAllowed = new Set<string>(opts.kinds);
  const subscribeOpts: {
    signal: AbortSignal;
    from: "head" | { seq: number };
    bufferLimit?: number;
  } = {
    signal: opts.signal,
    from: opts.from,
  };
  if (opts.bufferLimit !== undefined) {
    subscribeOpts.bufferLimit = opts.bufferLimit;
  }
  const iter = store.subscribe(principal, repoId, ref, subscribeOpts);

  for await (const { event } of iter) {
    const envelope = SubstrateEvent(event);
    if (envelope instanceof type.errors) {
      throw new Error(`subscribe_kind_envelope_invalid: ${envelope.summary}`);
    }
    const newReads = await store.openCommittedReadsAtCommit(
      principal,
      repoId,
      envelope.newSha,
    );
    // A commit a concurrent GC pruned between the ref-update event and
    // this read yields nothing; the substrate surfaces that as `null`.
    if (newReads === null) continue;
    const oldReads =
      envelope.oldSha === undefined || envelope.oldSha === null
        ? null
        : await store.openCommittedReadsAtCommit(
            principal,
            repoId,
            envelope.oldSha,
          );
    const candidates = await collectAddedEventBlobs(newReads, oldReads);
    for (const candidate of candidates) {
      const raw = await newReads.readBlobByOid(candidate.oid);
      let parsedJson: unknown;
      try {
        parsedJson = JSON.parse(new TextDecoder().decode(raw));
      } catch (cause) {
        throw new Error(`subscribe_kind_invalid_json: ${candidate.blobPath}`, {
          cause,
        });
      }
      const kind = KindEnvelope(parsedJson);
      if (kind instanceof type.errors) {
        throw new Error(
          `subscribe_kind_missing_type: ${candidate.blobPath}: ${kind.summary}`,
        );
      }
      if (!kindsAllowed.has(kind.type)) continue;
      const narrowed = validator(parsedJson);
      if (narrowed instanceof type.errors) {
        throw new Error(
          `subscribe_kind_validation_failed: ${candidate.blobPath}: ${narrowed.summary}`,
        );
      }
      yield { seq: candidate.seq, runId: candidate.runId, event: narrowed };
    }
  }
}

/**
 * Diff the `runs/<runId>/events/` subtree between the new and old commit
 * reads and return the event candidates present in the new commit but
 * not the old. `oldReads` is `null` for the ref's first commit (no prior
 * tip) or when the prior commit is no longer in the object store, in
 * which case every event in the new commit is treated as added.
 * Filenames that fail the `<seq>.json` shape are surfaced as errors
 * rather than silently skipped — the workflow-run kind handler's
 * validatePush is the authority for what may land under this prefix, so
 * an unexpected shape here is a substrate-side invariant violation.
 */
async function collectAddedEventBlobs(
  newReads: CommittedReads,
  oldReads: CommittedReads | null,
): Promise<EventCandidate[]> {
  const newSet = await enumerateEventBlobs(newReads);
  if (oldReads === null) {
    return [...newSet.values()].sort(byRunThenSeq);
  }
  const oldSet = await enumerateEventBlobs(oldReads);
  const out: EventCandidate[] = [];
  for (const [key, candidate] of newSet) {
    if (oldSet.has(key)) continue;
    out.push(candidate);
  }
  out.sort(byRunThenSeq);
  return out;
}

function byRunThenSeq(a: EventCandidate, b: EventCandidate): number {
  if (a.runId < b.runId) return -1;
  if (a.runId > b.runId) return 1;
  return a.seq - b.seq;
}

async function enumerateEventBlobs(
  reads: CommittedReads,
): Promise<Map<string, EventCandidate>> {
  const out = new Map<string, EventCandidate>();
  const runs = await reads.listDir(WORKFLOW_RUN_RUNS_PREFIX);
  for (const runEntry of runs) {
    if (runEntry.type !== "tree") continue;
    const runId = runEntry.name;
    const events = await reads.listDir(
      `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/${WORKFLOW_RUN_EVENTS_DIR}`,
    );
    for (const blob of events) {
      if (blob.type !== "blob") continue;
      const blobPath = `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/${WORKFLOW_RUN_EVENTS_DIR}/${blob.name}`;
      const seq = parseEventSeq(blob.name);
      if (seq === null) {
        throw new Error(
          `subscribe_kind_unexpected_event_filename: ${blobPath}`,
        );
      }
      out.set(`${runId}/${blob.name}`, {
        seq,
        runId,
        blobPath,
        oid: blob.oid,
      });
    }
  }
  return out;
}
