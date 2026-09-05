// Supervisor-authored terminal-event commit for the crash-loop guard.
//
// When the crash-loop guard latches, the workflow-process child is dead
// and cannot commit its own terminal event, yet the deployment's run must
// reach a terminal state so its external `workflow_run.status` flips to
// `failed` -- the sole durable, queryable signal of a crash-loop. The
// supervisor's `crash-looping` phase is in-memory and per-process; no
// external reader observes it. The supervisor, as the sole writer of the
// workflow-run repo, authors a `RunFailed` for the deployment's stable
// run so the crash-loop leaves a durable tombstone.
//
// Unlike `commitCancelRequested`, a terminal workflow event carries no
// signature: only `CancelRequested` is signed (for its origin<->principal
// cross-check at push validation), so this writer is unsigned. It writes
// under the `supervisor` principal, which the workflow-run kind handler
// authorizes for the deployment's own event log (`repoId.id ===
// anchorRunId`); terminal events have no per-type authorship check.

import { type } from "arktype";

import { getLogger } from "@intx/log";
import type { RunFailed } from "@intx/workflow";
import { parseEventSeq } from "@intx/hub-sessions/substrate";
import type {
  RepoId,
  RepoStore as SubstrateRepoStore,
  WorkflowRunSupervisorPrincipal,
} from "@intx/hub-sessions/substrate";

import { workflowEventToOnDisk } from "../adapters/repo-store";

const logger = getLogger(["workflow-host", "supervisor", "terminal-commit"]);

/** Path layout inside the workflow-run repo: `runs/<runId>/events/<seq>.json`. */
const RUNS_PREFIX = "runs";
const EVENTS_DIR = "events";

/**
 * Terminal run-event kinds, mirroring the runtime's terminal vocabulary
 * (`RunCompleted`/`RunFailed`/`RunCancelled`). Inlined because the workflow
 * package exports only the phase-level `isTerminalRunPhase`, not an
 * event-kind set; the child runtime (`run-child`) and the substrate adapter
 * (`repo-store`) inline the same three kinds. Reducing the log to a phase to
 * reuse `isTerminalRunPhase` would be strictly heavier for a last-event
 * type check.
 */
const TERMINAL_EVENT_KINDS = new Set<string>([
  "RunCompleted",
  "RunFailed",
  "RunCancelled",
]);

const SUPERVISOR_PRINCIPAL_KIND = "supervisor" as const;

/**
 * On-disk event envelope, validated at the substrate read boundary. Only
 * `seq` and `type` are load-bearing here (max-seq computation and the
 * terminal-lock check); the rest of the event body is ignored.
 */
const OnDiskEventEnvelope = type({
  seq: "number >= 0",
  type: "string",
  "+": "ignore",
});

export type CommitRunFailedOpts = {
  /** Substrate handle the supervisor writes through. */
  substrate: SubstrateRepoStore;
  /** Workflow-run repo for this deployment. */
  repoId: RepoId;
  /** Events ref the workflow-run repo writes to. */
  ref: string;
  /** Anchor run id used to construct the supervisor principal. */
  anchorRunId: string;
  /** Run id whose event log receives the RunFailed entry. */
  runId: string;
  /** ISO-8601 commit timestamp the event carries. */
  at: string;
  /** Operator-facing failure reason on `RunFailed.error.message`. */
  message: string;
};

export type CommitRunFailedResult = {
  /** Substrate-assigned commit SHA the write produced. */
  commitSha: string;
  /**
   * True when a `RunFailed` was appended; false when the run was already
   * terminal and the write was a no-op (terminal-lock respected).
   */
  appended: boolean;
};

/**
 * Append a supervisor-authored `RunFailed` to a run's event log, unless the
 * run is already terminal. The next seq is computed inside the substrate
 * merge (atomic against any concurrent writer under the per-repo lock): the
 * first event on an empty tree lands at seq 1 (the runtime's convention),
 * otherwise at `maxSeq + 1` so the log stays seq-contiguous. If the run's
 * highest-seq event is already terminal, the write is a no-op so push
 * validation's terminal-lock is never tripped.
 */
export async function commitRunFailed(
  opts: CommitRunFailedOpts,
): Promise<CommitRunFailedResult> {
  const prefix = `${RUNS_PREFIX}/${opts.runId}/${EVENTS_DIR}/`;
  const principal: WorkflowRunSupervisorPrincipal = {
    kind: SUPERVISOR_PRINCIPAL_KIND,
    anchorRunId: opts.anchorRunId,
  };
  const decoder = new TextDecoder();
  let appended = false;
  const { commitSha } = await opts.substrate.writeTreePreservingPrefix(
    principal,
    opts.repoId,
    opts.ref,
    {
      preservePrefix: prefix,
      merge: async (existing) => {
        let maxSeq = -1;
        let maxPath: string | null = null;
        for (const filepath of existing.keys()) {
          const name = filepath.slice(prefix.length);
          const seq = parseEventSeq(name);
          if (seq === null) continue;
          if (seq > maxSeq) {
            maxSeq = seq;
            maxPath = filepath;
          }
        }
        const carried: Record<string, string | Uint8Array> = {};
        for (const [k, v] of existing) carried[k] = v;
        // Terminal-lock: appending a terminal event after an existing one
        // is rejected at push validation. If the run already ended, its
        // `workflow_run.status` is already terminal, so the crash-loop
        // tombstone is redundant -- no-op rather than push a rejected write.
        //
        // This detects the per-event (`events/<seq>.json`) form only, not
        // the sealed combined-log (`events.jsonl`) form, which lives outside
        // `preservePrefix`. That is sufficient here because the anchor run
        // this commit targets is sealed only when the DEPLOYMENT itself is
        // terminal, and the crash-loop latch fires only while the deployment
        // is running -- so the run is never in the sealed form at this call.
        // If a sealed run ever reached here, the append would produce a tree
        // carrying both forms, which push validation rejects, and the caller
        // logs the failure (best-effort tombstone) rather than corrupting.
        if (maxPath !== null) {
          const raw = existing.get(maxPath);
          if (raw !== undefined) {
            const parsed = OnDiskEventEnvelope(JSON.parse(decoder.decode(raw)));
            if (parsed instanceof type.errors) {
              throw new Error(
                `commitRunFailed: event blob ${maxPath} failed validation: ${parsed.summary}`,
              );
            }
            if (TERMINAL_EVENT_KINDS.has(parsed.type)) {
              return carried;
            }
          }
        }
        const nextSeq = maxSeq < 0 ? 1 : maxSeq + 1;
        const event: RunFailed = {
          kind: "RunFailed",
          seq: nextSeq,
          at: opts.at,
          error: { message: opts.message },
        };
        const onDisk = workflowEventToOnDisk(event, nextSeq);
        carried[`${prefix}${String(nextSeq)}.json`] = JSON.stringify(onDisk);
        appended = true;
        return carried;
      },
      message: `append RunFailed (crash-loop) for run ${opts.runId}`,
    },
  );
  if (!appended) {
    logger.info`commitRunFailed: run ${opts.runId} already terminal; no RunFailed appended`;
  }
  return { commitSha, appended };
}
