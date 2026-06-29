// Run-event signing path for the per-deployment supervisor.
//
// The supervisor commits the canonical run-lifecycle event chain
// (`RunStarted`, `StepStarted`, `StepCompleted`, `RunCompleted`) to
// the workflow-run repo on behalf of the deployment. Both the trivial
// branch (no IPC, no child) and the multi-step branch's
// supervisor-side bookkeeping route through this module so the on-
// disk shape is identical regardless of process topology.
//
// The substrate-side workflow-run kind handler validates that the
// signing principal is `supervisor` and that the principal's
// `deploymentId` equals `repoId.id`; the surface here mirrors the
// `commitCancelRequested` shape so an audit-log walker sees the same
// envelope conventions across every event kind the supervisor
// authors.

import type {
  RepoId,
  RepoStore as SubstrateRepoStore,
  WorkflowRunSupervisorPrincipal,
} from "@intx/hub-sessions";
import {
  WORKFLOW_RUN_EVENTS_FILE,
  encodeCombinedEventLog,
} from "@intx/hub-sessions";
import { hexEncode } from "@intx/types";

import { SUPERVISOR_PRINCIPAL_KIND } from "./cancel-signing";
import type { PrincipalSigner, SignedPayload } from "./types";

const RUNS_PREFIX = "runs";
const EVENTS_DIR = "events";
const EVENT_FILENAME_RE = /^(0|[1-9][0-9]*)\.json$/;
const TERMINAL_EVENT_TYPES = new Set<string>([
  "RunCompleted",
  "RunFailed",
  "RunCancelled",
]);

/**
 * Shape of the run-lifecycle events the supervisor commits inline.
 * The discriminator mirrors the workflow state-machine's
 * `WorkflowEvent` union without pulling `@intx/workflow` into the
 * supervisor module's dependency closure: the supervisor only needs
 * to know the on-disk envelope shape, not the full transition
 * semantics. The transition function in `@intx/workflow` is the
 * authoritative validator for the chain.
 */
export type SupervisorRunEvent =
  | {
      readonly kind: "RunStarted";
      readonly runId: string;
      readonly at: string;
      readonly definitionHash: string;
      readonly trigger: { readonly type: string; readonly payload: unknown };
      readonly consumedMessageId?: string;
    }
  | {
      readonly kind: "StepStarted";
      readonly runId: string;
      readonly at: string;
      readonly stepId: string;
      readonly attempt: number;
      readonly input: { readonly ref: string };
    }
  | {
      readonly kind: "StepCompleted";
      readonly runId: string;
      readonly at: string;
      readonly stepId: string;
      readonly attempt: number;
      readonly output: { readonly ref: string };
    }
  | {
      readonly kind: "RunCompleted";
      readonly runId: string;
      readonly at: string;
    };

export type CommitRunEventOpts = {
  /** Substrate handle the supervisor writes through. */
  substrate: SubstrateRepoStore;
  /** Workflow-run repo for this deployment. */
  repoId: RepoId;
  /** Events ref the workflow-run repo writes to. */
  ref: string;
  /** Deployment id used to construct the supervisor principal. */
  deploymentId: string;
  /** Event to commit. */
  event: SupervisorRunEvent;
  /** Host-supplied per-principal signing callback. */
  signAsPrincipal: PrincipalSigner;
};

export type CommitRunEventResult = {
  /** Substrate-assigned commit SHA the append produced. */
  commitSha: string;
  /** Per-run sequence number the append landed at. */
  seq: number;
  /** Signature the supervisor attached to the event. */
  signature: SignedPayload;
};

/**
 * Build the canonical bytes the supervisor signs for a run-lifecycle
 * event. The on-disk envelope adds a `signature` field after the
 * signature is computed; signing the payload without that field keeps
 * the verifier's reconstruction trivial (strip `signature`,
 * canonicalize, verify).
 *
 * The substrate-level discriminator field on disk is `type` rather
 * than `kind` so the workflow-run kind handler's `EventEnvelope`
 * validator matches every supervisor-authored blob without a
 * per-event-kind translation.
 */
function buildPayloadBytes(args: {
  seq: number;
  event: SupervisorRunEvent;
}): Uint8Array {
  const canonical = canonicalize({ seq: args.seq, event: args.event });
  return new TextEncoder().encode(JSON.stringify(canonical));
}

function canonicalize(args: {
  seq: number;
  event: SupervisorRunEvent;
}): Record<string, unknown> {
  const { seq, event } = args;
  switch (event.kind) {
    case "RunStarted": {
      const base: Record<string, unknown> = {
        type: "RunStarted",
        seq,
        runId: event.runId,
        at: event.at,
        definitionHash: event.definitionHash,
        trigger: event.trigger,
      };
      if (event.consumedMessageId !== undefined) {
        base["consumedMessageId"] = event.consumedMessageId;
      }
      return base;
    }
    case "StepStarted":
      return {
        type: "StepStarted",
        seq,
        runId: event.runId,
        at: event.at,
        stepId: event.stepId,
        attempt: event.attempt,
        input: event.input,
      };
    case "StepCompleted":
      return {
        type: "StepCompleted",
        seq,
        runId: event.runId,
        at: event.at,
        stepId: event.stepId,
        attempt: event.attempt,
        output: event.output,
      };
    case "RunCompleted":
      return {
        type: "RunCompleted",
        seq,
        runId: event.runId,
        at: event.at,
      };
  }
}

/**
 * Commit a run-lifecycle event signed by the supervisor. The append
 * is atomic against concurrent supervisor commits via the substrate's
 * `writeTreePreservingPrefix` lock; the merge callback computes the
 * next per-run seq by scanning the existing files under the run's
 * `events/` subtree.
 */
export async function commitRunEvent(
  opts: CommitRunEventOpts,
): Promise<CommitRunEventResult> {
  const prefix = `${RUNS_PREFIX}/${opts.event.runId}/${EVENTS_DIR}/`;
  const principal: WorkflowRunSupervisorPrincipal = {
    kind: SUPERVISOR_PRINCIPAL_KIND,
    deploymentId: opts.deploymentId,
  };
  let resolved: { seq: number; signature: SignedPayload } | null = null;
  const { commitSha } = await opts.substrate.writeTreePreservingPrefix(
    principal,
    opts.repoId,
    opts.ref,
    {
      preservePrefix: prefix,
      merge: async (existing) => {
        let maxSeq = -1;
        for (const filepath of existing.keys()) {
          const name = filepath.slice(prefix.length);
          const match = EVENT_FILENAME_RE.exec(name);
          if (match === null) continue;
          const seqStr = match[1];
          if (seqStr === undefined) continue;
          const seq = Number.parseInt(seqStr, 10);
          if (seq > maxSeq) maxSeq = seq;
        }
        const nextSeq = maxSeq + 1;
        const payloadBytes = buildPayloadBytes({
          seq: nextSeq,
          event: opts.event,
        });
        const signature = await opts.signAsPrincipal(
          SUPERVISOR_PRINCIPAL_KIND,
          payloadBytes,
        );
        resolved = { seq: nextSeq, signature };
        const onDisk: Record<string, unknown> = {
          ...canonicalize({ seq: nextSeq, event: opts.event }),
          signature: serializeSignedPayload(signature),
        };
        const files: Record<string, string | Uint8Array> = {};
        for (const [k, v] of existing) files[k] = v;
        files[`${prefix}${String(nextSeq)}.json`] = JSON.stringify(onDisk);
        return files;
      },
      message: `append ${opts.event.kind} for run ${opts.event.runId}`,
    },
  );
  if (resolved === null) {
    throw new Error(
      `supervisor run-event-signing: merge callback did not assign a sequence number for run ${opts.event.runId}`,
    );
  }
  const final: { seq: number; signature: SignedPayload } = resolved;
  return { commitSha, seq: final.seq, signature: final.signature };
}

export type CompactRunEventsOpts = {
  /** Substrate handle the supervisor writes through. */
  substrate: SubstrateRepoStore;
  /** Workflow-run repo for this deployment. */
  repoId: RepoId;
  /** Events ref the workflow-run repo writes to. */
  ref: string;
  /** Deployment id used to construct the supervisor principal. */
  deploymentId: string;
  /** Run to seal. */
  runId: string;
};

/**
 * Fold a terminated run's per-event `events/<seq>.json` blobs into one
 * combined `events.jsonl`, dropping the per-event files. This shrinks the
 * repo's file count -- and so every per-commit cost that scales with it --
 * without losing any event.
 *
 * Idempotent and terminal-only: a run already sealed (no `events/` subtree)
 * or one whose latest event is not terminal is left untouched, so the call
 * is safe to repeat. The live caller fires it once per run, right after the
 * run terminates; a bounded recovery sweep that would re-fire it to seal a
 * run whose fold a crash interrupted is tracked separately (INTR-229).
 *
 * The combined file is the verbatim byte concatenation of the per-event
 * blobs in seq order (`encodeCombinedEventLog`), the exact shape the
 * workflow-run kind handler's compaction validation requires. It is written
 * as a sibling of `events/`, so returning it from the merge while omitting
 * the per-event files lets the substrate's prefix clear drop them.
 */
export async function compactRunEvents(
  opts: CompactRunEventsOpts,
): Promise<{ compacted: boolean }> {
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const dir = opts.substrate.getRepoDir(opts.repoId);
  const eventsDir = path.join(dir, RUNS_PREFIX, opts.runId, EVENTS_DIR);

  // Cheap pre-check off the working tree to skip an empty commit when there
  // is nothing to seal (already combined, or not yet terminal). The merge
  // re-reads the prefix under the per-repo lock, so the seal stays
  // consistent if another writer raced in between.
  let filenames: string[];
  try {
    filenames = await fs.readdir(eventsDir);
  } catch (cause) {
    if (isErrnoNotFound(cause)) return { compacted: false };
    throw cause;
  }
  const seqs: number[] = [];
  for (const name of filenames) {
    const match = EVENT_FILENAME_RE.exec(name);
    if (match === null || match[1] === undefined) continue;
    seqs.push(Number.parseInt(match[1], 10));
  }
  if (seqs.length === 0) return { compacted: false };
  const lastRaw = await fs.readFile(
    path.join(eventsDir, `${String(Math.max(...seqs))}.json`),
    "utf8",
  );
  let parsed: unknown;
  try {
    parsed = JSON.parse(lastRaw);
  } catch {
    return { compacted: false };
  }
  if (typeof parsed !== "object" || parsed === null || !("type" in parsed)) {
    return { compacted: false };
  }
  const lastType = (parsed as { type?: unknown }).type;
  if (typeof lastType !== "string" || !TERMINAL_EVENT_TYPES.has(lastType)) {
    return { compacted: false };
  }

  const prefix = `${RUNS_PREFIX}/${opts.runId}/${EVENTS_DIR}/`;
  const combinedPath = `${RUNS_PREFIX}/${opts.runId}/${WORKFLOW_RUN_EVENTS_FILE}`;
  const principal: WorkflowRunSupervisorPrincipal = {
    kind: SUPERVISOR_PRINCIPAL_KIND,
    deploymentId: opts.deploymentId,
  };
  let sealed = false;
  await opts.substrate.writeTreePreservingPrefix(
    principal,
    opts.repoId,
    opts.ref,
    {
      preservePrefix: prefix,
      merge: async (existing) => {
        const entries: { seq: number; bytes: Uint8Array }[] = [];
        for (const [filepath, bytes] of existing) {
          const name = filepath.slice(prefix.length);
          const match = EVENT_FILENAME_RE.exec(name);
          if (match === null || match[1] === undefined) {
            throw new Error(
              `supervisor run-event-signing: unexpected non-event file ${filepath} under run ${opts.runId}; refusing to compact`,
            );
          }
          entries.push({ seq: Number.parseInt(match[1], 10), bytes });
        }
        if (entries.length === 0) return {};
        entries.sort((a, b) => a.seq - b.seq);
        sealed = true;
        return {
          [combinedPath]: encodeCombinedEventLog(entries.map((e) => e.bytes)),
        };
      },
      message: `compact run ${opts.runId} events`,
    },
  );
  return { compacted: sealed };
}

function isErrnoNotFound(cause: unknown): boolean {
  if (cause === null || typeof cause !== "object") return false;
  return (cause as { code?: unknown }).code === "ENOENT";
}

function serializeSignedPayload(signed: SignedPayload): {
  principalKind: string;
  sig: string;
} {
  return {
    principalKind: signed.principalKind,
    sig: hexEncode(signed.sig),
  };
}
