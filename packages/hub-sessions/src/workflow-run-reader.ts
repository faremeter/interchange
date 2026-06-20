import fs from "node:fs";
import git from "isomorphic-git";

import type { RepoId } from "./repo-store/types";
import type { RepoStore } from "./repo-store/types";
import {
  WORKFLOW_RUN_EVENTS_DIR,
  WORKFLOW_RUN_RUNS_PREFIX,
} from "./workflow-run-kind";

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

const EVENT_FILENAME_RE = /^(0|[1-9][0-9]*)\.json$/;

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
 * `workflow-run:<deploymentId>` grant check).
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
    const eventsDir = `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/${WORKFLOW_RUN_EVENTS_DIR}`;
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
      const m = EVENT_FILENAME_RE.exec(entry.path);
      if (m === null || m[1] === undefined) continue;
      const seq = Number.parseInt(m[1], 10);
      const blob = await git.readBlob({ fs, dir, oid: entry.oid });
      const parsed: unknown = JSON.parse(new TextDecoder().decode(blob.blob));
      if (
        typeof parsed !== "object" ||
        parsed === null ||
        Array.isArray(parsed)
      ) {
        throw new Error(
          `workflow-run reader: event at ${entry.path} is not a JSON object`,
        );
      }
      const body: Record<string, unknown> = { ...parsed };
      const type = body["type"];
      if (typeof type !== "string") {
        throw new Error(
          `workflow-run reader: event at ${entry.path} is missing a string \`type\` field`,
        );
      }
      events.push({ seq, type, body });
    }
    events.sort((a, b) => a.seq - b.seq);
    return events;
  }

  return { listRunIds, readRunEvents };
}
