import fs from "node:fs";
import git from "isomorphic-git";
import { type, type Type } from "arktype";
import { hasCode } from "@intx/types";
import type { Principal, RepoId, RepoStore } from "./types";

/**
 * Path-layout constants for the workflow-run repo's event log. The
 * workflow-run kind handler commits one JSON blob per event under
 * `runs/<runId>/events/<seq>.json` (the layout decided in the
 * pre-build interface decisions for the workflow-run repo). The
 * `subscribeKind` helper owns this layout so the substrate `subscribe`
 * stays generic and free of workflow-event vocabulary.
 */
const RUNS_PREFIX = "runs";
const EVENTS_DIR = "events";

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

/**
 * Per-event filename shape. Filenames are `<seq>.json` under
 * `runs/<runId>/events/`. `<seq>` is a non-negative decimal integer.
 */
const EVENT_FILENAME_RE = /^(0|[1-9][0-9]*)\.json$/;

type EventCandidate = {
  /** Workflow-event seq parsed from the filename. */
  seq: number;
  /** runId from the parent path segment. */
  runId: string;
  /** Repo-root-relative blob path. */
  blobPath: string;
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
 * The path layout is owned here, not in the substrate: the substrate
 * `subscribe` is a generic ref-tail primitive and knows nothing about
 * workflow-runs.
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
  const dir = store.getRepoDir(repoId);
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
    const candidates = await collectAddedEventBlobs(
      dir,
      envelope.oldSha ?? null,
      envelope.newSha,
    );
    for (const candidate of candidates) {
      const raw = await readBlobAtCommit(
        dir,
        envelope.newSha,
        candidate.blobPath,
      );
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
      yield { seq: candidate.seq, event: narrowed };
    }
  }
}

/**
 * Walk the `runs/<runId>/events/` subtree at `newSha` and at `oldSha`
 * (when present) and return the set of event candidates present in
 * the new commit but not in the old. Filenames that fail the
 * `<seq>.json` shape are surfaced as errors rather than silently
 * skipped — the workflow-run kind handler's validatePush is the
 * authority for what may land under this prefix, so an unexpected
 * shape here is a substrate-side invariant violation.
 */
async function collectAddedEventBlobs(
  dir: string,
  oldSha: string | null,
  newSha: string,
): Promise<EventCandidate[]> {
  const newSet = await enumerateEventBlobs(dir, newSha);
  if (oldSha === null) {
    return [...newSet.values()].sort(byRunThenSeq);
  }
  const oldSet = await enumerateEventBlobs(dir, oldSha);
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
  dir: string,
  commitOid: string,
): Promise<Map<string, EventCandidate>> {
  const out = new Map<string, EventCandidate>();
  let commit: Awaited<ReturnType<typeof git.readCommit>>;
  try {
    commit = await git.readCommit({ fs, dir, oid: commitOid });
  } catch (err) {
    if (hasCode(err) && err.code === "NotFoundError") return out;
    throw err;
  }
  const runsOid = await lookupSubtree(dir, commit.commit.tree, RUNS_PREFIX);
  if (runsOid === null) return out;
  const { tree: runs } = await git.readTree({ fs, dir, oid: runsOid });
  for (const runEntry of runs) {
    if (runEntry.type !== "tree") continue;
    const runId = runEntry.path;
    const eventsOid = await lookupSubtree(dir, runEntry.oid, EVENTS_DIR);
    if (eventsOid === null) continue;
    const { tree: events } = await git.readTree({ fs, dir, oid: eventsOid });
    for (const blob of events) {
      if (blob.type !== "blob") continue;
      const match = EVENT_FILENAME_RE.exec(blob.path);
      if (match === null) {
        throw new Error(
          `subscribe_kind_unexpected_event_filename: ${RUNS_PREFIX}/${runId}/${EVENTS_DIR}/${blob.path}`,
        );
      }
      const seqStr = match[1];
      if (seqStr === undefined) throw new Error("unreachable");
      const seq = Number.parseInt(seqStr, 10);
      const blobPath = `${RUNS_PREFIX}/${runId}/${EVENTS_DIR}/${blob.path}`;
      out.set(`${runId}/${blob.path}`, { seq, runId, blobPath });
    }
  }
  return out;
}

async function lookupSubtree(
  dir: string,
  parentTreeOid: string,
  name: string,
): Promise<string | null> {
  const { tree } = await git.readTree({ fs, dir, oid: parentTreeOid });
  const entry = tree.find((e) => e.path === name);
  if (entry === undefined) return null;
  if (entry.type !== "tree") return null;
  return entry.oid;
}

async function readBlobAtCommit(
  dir: string,
  commitOid: string,
  filepath: string,
): Promise<Uint8Array> {
  const { blob } = await git.readBlob({ fs, dir, oid: commitOid, filepath });
  return blob;
}
