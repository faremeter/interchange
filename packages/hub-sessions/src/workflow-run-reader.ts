import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";

import type { RepoId } from "./repo-store/types";
import type { RepoStore } from "./repo-store/types";
import {
  requireEventSeq,
  WORKFLOW_RUN_EVENTS_DIR,
  WORKFLOW_RUN_RUNS_PREFIX,
} from "./workflow-run-kind";
import {
  WORKFLOW_RUN_EVENTS_FILE,
  splitCombinedEventLog,
} from "./workflow-run-event-log";

/**
 * A workflow-run event as committed under
 * `runs/<runId>/events/<seq>.json`. The discriminator field is `type`;
 * the per-type body is opaque to the reader and surfaced verbatim as
 * `body` so consumers narrow on the discriminator they care about.
 */
export type WorkflowRunEvent = {
  seq: number;
  type: string;
  body: Record<string, unknown>;
};

/**
 * Reader for a workflow-run repo's committed event log. Projects the
 * `runs/<runId>/events/<seq>.json` substrate the workflow-process child
 * writes (validated at push time by the workflow-run kind handler) into
 * seq-ordered in-memory events, mirroring the read-model approach the
 * per-session timeline reconstruction uses against an agent-state repo.
 *
 * The reader is read-only: it composes `RepoStore.getRepoDir` (a pure
 * path computation) with direct `isomorphic-git` tree/blob reads. It
 * never writes, so it carries no authorize gate of its own; callers
 * gate access at their own boundary (the REST routes use a
 * `workflow-run:<runId>` grant check).
 */
export interface WorkflowRunReader {
  /**
   * Enumerate the run ids present under `runs/` on `ref`. Returns an
   * empty array when the repo has not been initialised yet (no on-disk
   * repoDir, no ref, or no `runs/` tree). A corrupt repo, a
   * present-but-malformed tree, or any other unexpected isomorphic-git
   * error propagates so the caller sees the failure rather than
   * treating it as "no runs yet".
   */
  listRunIds(repoId: RepoId, ref: string): Promise<string[]>;
  /**
   * Read every event under `runs/<runId>/events/` on `ref` and return
   * them in ascending `seq` order. Returns an empty array when the run
   * has not yet committed any events or the repo/ref has not been
   * created. A blob that parses but is missing a string `type`
   * discriminator is a substrate-invariant violation and throws rather
   * than being silently dropped.
   */
  readRunEvents(
    repoId: RepoId,
    ref: string,
    runId: string,
  ): Promise<WorkflowRunEvent[]>;
  /**
   * List the runs present under `runs/` at one pinned ref tip and read the
   * latest event of each run `include` selects, without reading prior blobs.
   * The returned map holds exactly the selected runs, with `null` for a run
   * directory that has no events yet. A missing Git object propagates
   * rather than reading as absent history.
   */
  readLatestRunEvents(
    repoId: RepoId,
    ref: string,
    include: (runId: string) => boolean,
  ): Promise<{
    tip: string | null;
    events: Map<string, WorkflowRunEvent | null>;
  }>;
  /** Recheck the ref after taking the allocation lock. */
  resolveRefTip(repoId: RepoId, ref: string): Promise<string | null>;
  /**
   * Whether the repository exists on disk, separating a repository whose ref
   * was never written from one that is missing entirely.
   */
  hasRepository(repoId: RepoId): Promise<boolean>;
}

export function createWorkflowRunReader(
  repoStore: RepoStore,
): WorkflowRunReader {
  function repoDirOrNull(repoId: RepoId): string | null {
    try {
      return repoStore.getRepoDir(repoId);
    } catch {
      // getRepoDir throws only when the repoId fails the kind handler's
      // slug validation; an uninitialised-but-valid repo returns a path
      // that does not yet exist on disk. A validation failure here means
      // the caller handed an id the substrate would never have written,
      // which is indistinguishable from "no such run repo" at this read
      // boundary.
      return null;
    }
  }

  async function resolveRefOrNull(
    dir: string,
    ref: string,
  ): Promise<string | null> {
    try {
      return await git.resolveRef({ fs, dir, ref });
    } catch (cause) {
      if (
        cause instanceof git.Errors.NotFoundError ||
        (cause instanceof Error && /ENOENT|not found/i.test(cause.message))
      ) {
        return null;
      }
      throw cause;
    }
  }

  async function listRunIds(repoId: RepoId, ref: string): Promise<string[]> {
    const dir = repoDirOrNull(repoId);
    if (dir === null) return [];
    const oid = await resolveRefOrNull(dir, ref);
    if (oid === null) return [];
    let tree: Awaited<ReturnType<typeof git.readTree>>;
    try {
      tree = await git.readTree({
        fs,
        dir,
        oid,
        filepath: WORKFLOW_RUN_RUNS_PREFIX,
      });
    } catch (cause) {
      if (cause instanceof git.Errors.NotFoundError) return [];
      throw cause;
    }
    return tree.tree
      .filter((entry) => entry.type === "tree")
      .map((entry) => entry.path);
  }

  async function readRunEvents(
    repoId: RepoId,
    ref: string,
    runId: string,
  ): Promise<WorkflowRunEvent[]> {
    const dir = repoDirOrNull(repoId);
    if (dir === null) return [];
    const oid = await resolveRefOrNull(dir, ref);
    if (oid === null) return [];
    const runDir = `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}`;
    let runTree: Awaited<ReturnType<typeof git.readTree>>;
    try {
      runTree = await git.readTree({ fs, dir, oid, filepath: runDir });
    } catch (cause) {
      if (cause instanceof git.Errors.NotFoundError) return [];
      throw cause;
    }
    // A terminated run is sealed into a single combined `events.jsonl`;
    // an in-flight run keeps per-event `events/<seq>.json` files. The two
    // forms are mutually exclusive in a run directory; a run carrying both
    // is a botched seal, and silently preferring one would mask it, so
    // surface it instead.
    const combined = runTree.tree.find(
      (e) => e.type === "blob" && e.path === WORKFLOW_RUN_EVENTS_FILE,
    );
    const perEventDir = runTree.tree.find(
      (e) => e.type === "tree" && e.path === WORKFLOW_RUN_EVENTS_DIR,
    );
    if (combined !== undefined && perEventDir !== undefined) {
      throw new Error(
        `workflow-run reader: run ${runId} carries both a combined ${WORKFLOW_RUN_EVENTS_FILE} and a per-event ${WORKFLOW_RUN_EVENTS_DIR}/ directory`,
      );
    }
    if (combined !== undefined) {
      const blob = await git.readBlob({ fs, dir, oid: combined.oid });
      const source = `${runDir}/${WORKFLOW_RUN_EVENTS_FILE}`;
      const events: WorkflowRunEvent[] = [];
      for (const line of splitCombinedEventLog(
        new TextDecoder().decode(blob.blob),
      )) {
        events.push(parseRunEventLine(line, source));
      }
      events.sort((a, b) => a.seq - b.seq);
      return events;
    }
    const eventsDir = `${runDir}/${WORKFLOW_RUN_EVENTS_DIR}`;
    let tree: Awaited<ReturnType<typeof git.readTree>>;
    try {
      tree = await git.readTree({ fs, dir, oid, filepath: eventsDir });
    } catch (cause) {
      if (cause instanceof git.Errors.NotFoundError) return [];
      throw cause;
    }
    const events: WorkflowRunEvent[] = [];
    for (const entry of tree.tree) {
      if (entry.type !== "blob") continue;
      const path = `${eventsDir}/${entry.path}`;
      const seq = requireEventSeq(entry.path, path);
      const blob = await git.readBlob({ fs, dir, oid: entry.oid });
      const parsed = parseEventObject(
        new TextDecoder().decode(blob.blob),
        path,
      );
      const type = parsed["type"];
      if (typeof type !== "string") {
        throw new Error(
          `workflow-run reader: event at ${path} is missing a string \`type\` field`,
        );
      }
      // The per-event form carries the seq in the filename; when the body
      // also carries one, the two must agree (the combined form reads the
      // seq from the body, so a disagreement would make the forms diverge).
      const bodySeq = parsed["seq"];
      if (typeof bodySeq === "number" && bodySeq !== seq) {
        throw new Error(
          `workflow-run reader: event at ${path} body seq ${String(bodySeq)} does not match filename seq ${String(seq)}`,
        );
      }
      events.push({ seq, type, body: parsed });
    }
    events.sort((a, b) => a.seq - b.seq);
    return events;
  }

  async function resolveRefTip(
    repoId: RepoId,
    ref: string,
  ): Promise<string | null> {
    const dir = repoDirOrNull(repoId);
    return dir === null ? null : resolveRefOrNull(dir, ref);
  }

  async function hasRepository(repoId: RepoId): Promise<boolean> {
    const dir = repoDirOrNull(repoId);
    if (dir === null) return false;
    try {
      await fs.promises.stat(path.join(dir, ".git"));
      return true;
    } catch (cause) {
      if (cause instanceof Error && "code" in cause && cause.code === "ENOENT")
        return false;
      throw cause;
    }
  }

  async function readLatestRunEvents(
    repoId: RepoId,
    ref: string,
    include: (runId: string) => boolean,
  ): Promise<{
    tip: string | null;
    events: Map<string, WorkflowRunEvent | null>;
  }> {
    const events = new Map<string, WorkflowRunEvent | null>();
    const dir = repoDirOrNull(repoId);
    if (dir === null) return { tip: null, events };
    const tip = await resolveRefOrNull(dir, ref);
    if (tip === null) return { tip, events };
    // Walk trees by oid rather than by filepath: isomorphic-git reports an
    // absent path and a missing object with the same NotFoundError, and
    // callers act on an empty result, so only a path absent from a readable
    // parent tree may read as "no history".
    const root = await git.readTree({ fs, dir, oid: tip });
    const runsEntry = root.tree.find(
      (entry) => entry.path === WORKFLOW_RUN_RUNS_PREFIX,
    );
    if (runsEntry === undefined) return { tip, events };
    const runsTree = await git.readTree({ fs, dir, oid: runsEntry.oid });
    for (const entry of runsTree.tree) {
      if (entry.type !== "tree" || !include(entry.path)) continue;
      events.set(
        entry.path,
        await readLatestEventAt(dir, entry.path, entry.oid),
      );
    }
    return { tip, events };
  }

  async function readLatestEventAt(
    dir: string,
    runId: string,
    runTreeOid: string,
  ): Promise<WorkflowRunEvent | null> {
    const runDir = `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}`;
    const runTree = await git.readTree({ fs, dir, oid: runTreeOid });
    const combined = runTree.tree.find(
      (entry) =>
        entry.type === "blob" && entry.path === WORKFLOW_RUN_EVENTS_FILE,
    );
    const perEventDir = runTree.tree.find(
      (entry) =>
        entry.type === "tree" && entry.path === WORKFLOW_RUN_EVENTS_DIR,
    );
    if (combined !== undefined && perEventDir !== undefined) {
      throw new Error(
        `workflow-run reader: run ${runId} carries both a combined ${WORKFLOW_RUN_EVENTS_FILE} and a per-event ${WORKFLOW_RUN_EVENTS_DIR}/ directory`,
      );
    }
    if (combined !== undefined) {
      const blob = await git.readBlob({ fs, dir, oid: combined.oid });
      const source = `${runDir}/${WORKFLOW_RUN_EVENTS_FILE}`;
      const last = splitCombinedEventLog(
        new TextDecoder().decode(blob.blob),
      ).at(-1);
      return last === undefined ? null : parseRunEventLine(last, source);
    }
    if (perEventDir === undefined) return null;
    const eventsDir = `${runDir}/${WORKFLOW_RUN_EVENTS_DIR}`;
    const tree = await git.readTree({ fs, dir, oid: perEventDir.oid });
    let latest: { seq: number; oid: string; path: string } | undefined;
    for (const entry of tree.tree) {
      if (entry.type !== "blob") continue;
      const path = `${eventsDir}/${entry.path}`;
      const seq = requireEventSeq(entry.path, path);
      if (latest === undefined || seq > latest.seq)
        latest = { seq, oid: entry.oid, path };
    }
    if (latest === undefined) return null;
    const blob = await git.readBlob({ fs, dir, oid: latest.oid });
    const parsed = parseEventObject(
      new TextDecoder().decode(blob.blob),
      latest.path,
    );
    const eventType = parsed["type"];
    if (typeof eventType !== "string") {
      throw new Error(
        `workflow-run reader: event at ${latest.path} is missing a string \`type\` field`,
      );
    }
    const bodySeq = parsed["seq"];
    if (typeof bodySeq === "number" && bodySeq !== latest.seq) {
      throw new Error(
        `workflow-run reader: event at ${latest.path} body seq ${String(bodySeq)} does not match filename seq ${String(latest.seq)}`,
      );
    }
    return { seq: latest.seq, type: eventType, body: parsed };
  }

  // Parse one combined-log line (the verbatim text of a former
  // `events/<seq>.json` blob): the seq is read from the body, since the
  // combined form drops the per-event filename that carried it.
  function parseRunEventLine(line: string, source: string): WorkflowRunEvent {
    const parsed = parseEventObject(line, source);
    const seq = parsed["seq"];
    const type = parsed["type"];
    if (typeof seq !== "number") {
      throw new Error(
        `workflow-run reader: event in ${source} is missing a numeric \`seq\` field`,
      );
    }
    if (typeof type !== "string") {
      throw new Error(
        `workflow-run reader: event in ${source} is missing a string \`type\` field`,
      );
    }
    return { seq, type, body: parsed };
  }

  function parseEventObject(
    text: string,
    source: string,
  ): Record<string, unknown> {
    const parsed: unknown = JSON.parse(text);
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      Array.isArray(parsed)
    ) {
      throw new Error(
        `workflow-run reader: event at ${source} is not a JSON object`,
      );
    }
    return { ...parsed };
  }

  return {
    listRunIds,
    readRunEvents,
    readLatestRunEvents,
    resolveRefTip,
    hasRepository,
  };
}
