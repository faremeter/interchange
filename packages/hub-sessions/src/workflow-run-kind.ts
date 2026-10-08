// KindHandler for the `workflow-run` repo kind.
//
// A workflow-run repo holds per-deployment runtime state for in-flight
// workflow runs. `RepoId.id` is the owning deployment id. Top-level
// layout:
//
//   - `runs/<runId>/events/<seq>.json` — append-only event log; the
//     filename seq must match the body `seq`.
//   - `runs/<runId>/blobs/<sha256-hex>` — content-addressed step
//     outputs, opaque bytes, append-only and immutable.
//   - `addresses/<urlEncoded(address)>/inbox|processing/<receivedAt>-<messageId>.json`
//     — claim-check FIFO queues; `consumed/<messageId>.json` is the
//     dedup index, pruned by the per-address `watermark.json`.
//   - `agent-state/<agentKey>/` — mutable per-agent conversation
//     snapshots.
//   - `mailbox/INBOX/` — the warm agent's durable inbox.
//   - `.gitignore` — supplied by the asset routes' genesis init body.
//
// `control/...` is reserved and has no v1 use case.
//
// Invariants enforced at push:
//   - Event blobs are append-only: any path present in the prior tree
//     must reappear byte-identical.
//   - Terminal-phase lock: once a run carries a `RunCompleted`,
//     `RunFailed`, or `RunCancelled` entry, no higher-seq event may
//     appear for the same run.
//   - `CancelRequested` events carry a known `origin` (`self`,
//     `supervisor-drain`, `supervisor-operator`, `hub-admin`) and a
//     non-empty `reason`; the signing principal must match the origin
//     (`hub-admin` -> `hub`, the rest -> `supervisor`).
//   - Claim-check: `addresses/` segments must round-trip URL-encoding;
//     an address holds only `inbox/`, `processing/`, `consumed/`, and
//     `watermark.json`; queue filenames carry `<receivedAt>-<messageId>`
//     and consumed filenames `<messageId>`, all matching their body
//     fields. A messageId appears at most once across the three states.
//     Consumed entries are immutable and may be deleted only by a
//     watermark-passed retention prune. Newly added processing entries
//     must match a prior inbox entry; newly added consumed entries must
//     match a prior processing entry.
//   - The watermark is monotonic: it only advances, and `enqueueInbox`
//     refuses inbound strictly below it (its dedup entry may have been
//     pruned). `replayProcessingToInbox` is exempt: an in-flight
//     `processing/` entry is already past dedup.
//   - `agent-state/` entries must be non-empty `<agentKey>/` directories
//     that round-trip URL-encoding; their contents are opaque.
//   - Mailbox: `mailbox/` holds only `INBOX/`, which holds `index.json`
//     (mutable, must persist once written) and `<uid>.eml` blobs
//     (immutable; a prior blob may vanish as a legal expunge).
//   - `runs/<runId>/` allows only `events/`, `blobs/`, `events.jsonl`,
//     `grants.json`, and `parts/`. A sealed run (combined
//     `events.jsonl`) must fold its prior per-event blobs byte-for-byte
//     in seq order.
//
// Authz: `hub` has full access; `workflow-process` and `supervisor`
// read/write their own deployment's event log (verified via
// `repoId.id === anchorRunId`); `sidecar` is read-only (createPack,
// resolveRef); `user` is gated by the route layer's pre-resolved verdict.

import fs from "node:fs";
import git from "isomorphic-git";
import { type } from "arktype";
import { getLogger } from "@intx/log";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";
import {
  authorizeUserPrincipal,
  type AuthorizeFn,
  type CommittedReads,
  type KindHandler,
  type NewlyTerminalRun,
  type PriorDeltaReads,
  type Principal,
  type RepoId,
  type RepoStore,
  type ValidatePushResult,
} from "./repo-store";
import {
  WORKFLOW_RUN_EVENTS_FILE,
  splitCombinedEventLog,
  encodeCombinedEventLog,
} from "./workflow-run-event-log";

const logger = getLogger(["hub-sessions", "workflow-run-kind"]);

// Workflow-run events commit on the substrate's default branch; the
// supervisor wires the workflow-process child against this ref.
export const WORKFLOW_RUN_REF = "refs/heads/main";

export function workflowRunRepoIdForAddress(agentAddress: string) {
  return {
    kind: "workflow-run",
    id: deriveWorkflowRunRepoId(agentAddress),
  } as const satisfies RepoId;
}

export type WorkflowRunHubPrincipal = { readonly kind: "hub" };

export type WorkflowRunSidecarPrincipal = {
  readonly kind: "sidecar";
  readonly agentId: string;
};

export type WorkflowRunWorkflowProcessPrincipal = {
  readonly kind: "workflow-process";
  readonly anchorRunId: string;
  readonly runId?: string;
};

export type WorkflowRunSupervisorPrincipal = {
  readonly kind: "supervisor";
  readonly anchorRunId: string;
};

export type WorkflowRunPrincipal =
  | WorkflowRunHubPrincipal
  | WorkflowRunSidecarPrincipal
  | WorkflowRunWorkflowProcessPrincipal
  | WorkflowRunSupervisorPrincipal;

export const WORKFLOW_RUN_GITIGNORE_PATH = ".gitignore";
export const WORKFLOW_RUN_RUNS_PREFIX = "runs";
export const WORKFLOW_RUN_EVENTS_DIR = "events";
export const WORKFLOW_RUN_BLOBS_DIR = "blobs";
export const WORKFLOW_RUN_GRANTS_FILE = "grants.json";
export const WORKFLOW_RUN_ADDRESSES_PREFIX = "addresses";
export const WORKFLOW_RUN_CONTROL_PREFIX = "control";
export const WORKFLOW_RUN_INBOX_DIR = "inbox";
export const WORKFLOW_RUN_PROCESSING_DIR = "processing";
export const WORKFLOW_RUN_CONSUMED_DIR = "consumed";

/**
 * Inbound-mail part bytes committed as real files at
 * `runs/<runId>/parts/<urlEncoded(messageId)>/<index>-<name>` so binary
 * content survives the JSON event log. One directory per message, one
 * file per part. Immutable once written, like `blobs/`.
 */
export const WORKFLOW_RUN_PARTS_DIR = "parts";

/**
 * Per-address retention watermark blob: the `receivedAt` horizon below
 * which consumed entries may be pruned and inbound enqueues are refused
 * as stale. Monotonic.
 */
export const WORKFLOW_RUN_WATERMARK_FILE = "watermark.json";

/**
 * Default retention horizon for the consumed dedup index, in
 * milliseconds. Used only when no operator `CONSUMED_RETENTION_MS` is
 * supplied; 24h keeps a day's volume of dedup entries while letting a
 * duplicate from a retrying upstream still hit one.
 *
 * INVARIANT (operator-owned): a redelivery re-stamps `Date.now()` at
 * enqueue, so a fresh `receivedAt` always sits a full horizon above the
 * watermark and can never be stale-refused; duplicates are caught by the
 * dedup index instead. Only `replayProcessingToInbox` carries an old
 * `receivedAt` back, and it bypasses the stale gate. If a redelivery
 * source ever carries the original `receivedAt` into `enqueueInbox`,
 * the horizon must cover that source's maximum redelivery window.
 */
export const DEFAULT_CONSUMED_RETENTION_MS = 24 * 60 * 60 * 1000;

/**
 * Per-agent conversation-state subtree (design §3c): the warm agent's
 * durable multi-turn context, surviving child respawn. MUTABLE — each
 * run boundary overwrites the snapshot — so it is exempt from the
 * append-only walks `runs/` is subject to; the only push-time
 * constraint is the `<agentKey>` directory-layer shape.
 */
export const WORKFLOW_RUN_AGENT_STATE_PREFIX = "agent-state";

/**
 * Warm single-step agent's durable inbox, replicated to the hub under
 * `mailbox/INBOX/`:
 *
 *   - `index.json` — MUTABLE (rewritten each flush), but must persist
 *     once written: dropping it resets uidValidity/uidNext on reopen.
 *   - `<uid>.eml` — one message per file, opaque and IMMUTABLE: a
 *     retained file must reappear byte-identically, like `blobs/`. A
 *     prior file may vanish — a legal expunge; the bytes stay in git
 *     history (workflow-run repos are never GC'd).
 *
 * Only these two entry kinds are permitted under `mailbox/INBOX/`.
 */
export const WORKFLOW_RUN_MAILBOX_PREFIX = "mailbox";
export const WORKFLOW_RUN_MAILBOX_INBOX_DIR = "INBOX";
export const WORKFLOW_RUN_MAILBOX_INDEX_FILE = "index.json";

/** Allowed top-level entries; anything else fails the push. `control/` stays absent (no v1 use). */
const ALLOWED_TOP_LEVEL = new Set<string>([
  WORKFLOW_RUN_RUNS_PREFIX,
  WORKFLOW_RUN_ADDRESSES_PREFIX,
  WORKFLOW_RUN_AGENT_STATE_PREFIX,
  WORKFLOW_RUN_MAILBOX_PREFIX,
  WORKFLOW_RUN_GITIGNORE_PATH,
]);

/** `<uid>.eml` filename shape: `<uid>` is a decimal integer >= 1. */
const MAILBOX_EML_FILENAME_RE = /^[1-9][0-9]*\.eml$/;

const CLAIM_CHECK_SUBDIRS = new Set<string>([
  WORKFLOW_RUN_INBOX_DIR,
  WORKFLOW_RUN_PROCESSING_DIR,
  WORKFLOW_RUN_CONSUMED_DIR,
]);

/** Per-event filename shape: a decimal integer followed by `.json`. */
const EVENT_FILENAME_RE = /^(0|[1-9][0-9]*)\.json$/;

/**
 * Parse the seq from an event filename `<seq>.json`, or `null` for an
 * illegal name. The single definition of the filename shape; callers
 * decide what an illegal name means (skip vs. surface).
 */
export function parseEventSeq(filename: string): number | null {
  const match = EVENT_FILENAME_RE.exec(filename);
  if (match === null) return null;
  const seqStr = match[1];
  if (seqStr === undefined) return null;
  return Number.parseInt(seqStr, 10);
}

/**
 * Narrow an event filename to its seq, throwing when illegal. A name
 * reaching a reader is corruption (validatePush keeps illegal names from
 * landing), so skipping it would silently drop an event. `context` names
 * the offending path in the error.
 */
export function requireEventSeq(filename: string, context: string): number {
  const seq = parseEventSeq(filename);
  if (seq === null) {
    throw new Error(`event_filename_invalid: ${context}`);
  }
  return seq;
}

/** Blob filename shape: a lowercase 64-character sha256 hex string. */
const BLOB_FILENAME_RE = /^[0-9a-f]{64}$/;

/**
 * Mail-part filename shape: `<index>-<name>` under
 * `runs/<runId>/parts/<urlEncoded(messageId)>/`. The workflow-host
 * ingest owns the encoding; bytes are opaque and immutable like `blobs/`.
 */
const PART_FILENAME_RE = /^(0|[1-9][0-9]*)-(.+)$/;

/**
 * Byte cap on mail-part path components (message segment and
 * `<index>-<name>` filenames): untrusted message-ids can URL-encode
 * past the filesystem's 255-byte limit, so reject at the boundary.
 */
export const MAX_MAIL_PART_PATH_COMPONENT_BYTES = 255;

function mailPartComponentByteLength(component: string): number {
  return new TextEncoder().encode(component).length;
}

/**
 * Allowed entries under `runs/<runId>/`: the append-only `events/` log,
 * the content-addressed `blobs/`, the sealed `events.jsonl` (compaction),
 * the grants file (written by the hub's `run.grants` frame ahead of the
 * trigger), and the mail-part `parts/` subtree. The walks treat
 * `grants.json` as inert, not part of the event log.
 */
const RUN_DIR_ALLOWED_CHILDREN = new Set<string>([
  WORKFLOW_RUN_EVENTS_DIR,
  WORKFLOW_RUN_BLOBS_DIR,
  // A terminated run's event log, sealed from the per-event `events/`
  // files into one combined file by a compaction commit.
  WORKFLOW_RUN_EVENTS_FILE,
  WORKFLOW_RUN_GRANTS_FILE,
  // Inbound-mail mail part bytes committed as real files. See
  // WORKFLOW_RUN_PARTS_DIR; validated by enumerateRunParts and
  // held immutable by the same prior-tree byte-equality walk as blobs.
  WORKFLOW_RUN_PARTS_DIR,
]);

/** Filename shape for inbox/processing entries: `<receivedAt>-<messageId>.json`. */
const QUEUE_FILENAME_RE = /^(0|[1-9][0-9]*)-(.+)\.json$/;

/** Filename shape for consumed entries: `<messageId>.json`. */
const CONSUMED_FILENAME_RE = /^(.+)\.json$/;

/**
 * JSON envelope for inbox and processing entries: `messageId` (dedup
 * key), `receivedAt` (FIFO key prefix), `address` (decoded), and
 * `mailAuditRef`. `rawMessage` is the base64 raw MIME bytes, inlined so
 * the workflow-process child can read its step input by messageId at
 * `trigger.fired` time — the supervisor is the sole mail owner and has
 * no separate durable byte store (§3a). Survives the inbox→processing
 * transition verbatim.
 */
const ClaimCheckEnvelope = type({
  messageId: "string > 0",
  receivedAt: "number >= 0",
  address: "string > 0",
  mailAuditRef: {
    store: "string > 0",
    path: "string > 0",
  },
  "rawMessage?": "string > 0",
  "+": "ignore",
});

/**
 * Consumed-entry envelope: the dedup index keyed by messageId,
 * preserving the original `receivedAt` for audit and carrying the
 * consuming runId.
 */
const ConsumedEnvelope = type({
  messageId: "string > 0",
  receivedAt: "number >= 0",
  address: "string > 0",
  runId: "string > 0",
  consumedAt: "number >= 0",
  mailAuditRef: {
    store: "string > 0",
    path: "string > 0",
  },
  "rejection?": {
    code: "string > 0",
    message: "string > 0",
  },
  "+": "ignore",
});

/**
 * Watermark envelope: a `receivedAt` horizon, monotonic. Prunes drop
 * consumed entries strictly below it; `enqueueInbox` refuses inbound
 * strictly below it.
 */
const WatermarkEnvelope = type({
  watermark: "number >= 0",
  "+": "ignore",
});

export type ClaimCheckEnvelope = typeof ClaimCheckEnvelope.infer;
export type ConsumedEnvelope = typeof ConsumedEnvelope.infer;
export type WatermarkEnvelope = typeof WatermarkEnvelope.infer;

/**
 * Terminal event discriminators mapped to the `workflow_run.status`
 * value each settles. A run carrying one must receive no higher-seq
 * event. Hand-rolled copy of the runtime's terminal-run vocabulary
 * (`@intx/workflow`), which `@intx/hub-sessions` must not depend on;
 * keep in sync or the double-driver collision `scanRunsForBoot` guards
 * against reopens.
 */
type TerminalRunStatus = "completed" | "failed" | "cancelled";

const TERMINAL_EVENT_STATUS: ReadonlyMap<string, TerminalRunStatus> = new Map([
  ["RunCompleted", "completed"],
  ["RunFailed", "failed"],
  ["RunCancelled", "cancelled"],
]);

/** Membership set derived from `TERMINAL_EVENT_STATUS` so the two cannot drift. */
const TERMINAL_EVENT_TYPES = new Set<string>(TERMINAL_EVENT_STATUS.keys());

/** Classify an event type against the terminal-status vocabulary. */
export function classifyTerminalEvent(
  eventType: string,
): { terminal: true; status: TerminalRunStatus } | { terminal: false } {
  const status = TERMINAL_EVENT_STATUS.get(eventType);
  return status === undefined
    ? { terminal: false }
    : { terminal: true, status };
}

/** True when this commit authors the blob (absent from the prior tree). */
async function blobIsNewlyAdded(
  blobPath: string,
  priorReadBlob: (path: string) => Promise<Uint8Array | null>,
): Promise<boolean> {
  return (await priorReadBlob(blobPath)) === null;
}

/** Known CancelRequested origins; mirrors `@intx/workflow`'s CANCEL_ORIGINS. */
const CANCEL_REQUESTED_ORIGINS = new Set<string>([
  "self",
  "supervisor-drain",
  "supervisor-operator",
  "hub-admin",
]);

/**
 * Per-origin signing-principal kind. Only `hub-admin` is minted by a
 * `hub` principal; the supervisor signs the other three (for `self`
 * on the child's behalf, which has no keypair). Lookup misses fail.
 */
const CANCEL_ORIGIN_TO_PRINCIPAL_KIND: ReadonlyMap<string, string> = new Map([
  ["self", "supervisor"],
  ["supervisor-drain", "supervisor"],
  ["supervisor-operator", "supervisor"],
  ["hub-admin", "hub"],
]);

/** Cross-event shape for `runs/<runId>/events/` blobs. */
const EventEnvelope = type({
  type: "string",
  seq: "number >= 0",
  "+": "ignore",
});

/** Structural validator for the `CancelRequested` payload fields. */
const CancelRequestedFields = type({
  origin: "string",
  reason: "string > 0",
  "+": "ignore",
});

const SidecarPrincipal = type({
  kind: "'sidecar'",
  agentId: "string",
});

const WorkflowProcessPrincipal = type({
  kind: "'workflow-process'",
  anchorRunId: "string",
  "runId?": "string",
});

const SupervisorPrincipal = type({
  kind: "'supervisor'",
  anchorRunId: "string",
});

type RunEventBlob = {
  runId: string;
  filename: string;
  filenameSeq: number;
  blobPath: string;
};

/**
 * Resolve `changedPathPrefixes` into the run ids the commit could have
 * touched, or `undefined` to validate every run (when the substrate
 * could not bound the change set, or a prefix names `runs/` without a
 * specific run). Non-run prefixes contribute nothing.
 */
function runScopeFromChangedPrefixes(
  changedPathPrefixes: ReadonlySet<string> | undefined,
): Set<string> | undefined {
  if (changedPathPrefixes === undefined) return undefined;
  const runsPrefix = `${WORKFLOW_RUN_RUNS_PREFIX}/`;
  const runIds = new Set<string>();
  for (const prefix of changedPathPrefixes) {
    if (prefix === WORKFLOW_RUN_RUNS_PREFIX || prefix === runsPrefix) {
      // The `runs/` subtree changed but the substrate could not name
      // which run; fall back to validating every run.
      return undefined;
    }
    if (!prefix.startsWith(runsPrefix)) continue;
    const rest = prefix.slice(runsPrefix.length);
    const slash = rest.indexOf("/");
    if (slash <= 0) return undefined;
    runIds.add(rest.slice(0, slash));
  }
  return runIds;
}

/**
 * Walk the prospective tree into a (runId → events[]) map. Illegal
 * filenames fail the push; with `scopeRunIds`, only those runs are
 * walked (see the substrate's `changedPathPrefixes` contract).
 */
async function enumerateEventBlobs(
  listDir: (path: string) => Promise<string[]>,
  scopeRunIds?: ReadonlySet<string>,
): Promise<
  | { ok: true; runs: Map<string, RunEventBlob[]> }
  | { ok: false; reason: string }
> {
  const runs = new Map<string, RunEventBlob[]>();
  // With a scope, walk only the touched runs: untouched runs are carried
  // forward byte-identical, so their invariants cannot change. A scoped
  // id may name a run absent from this tree; `listDir` returns `[]` for
  // it, which the empty-children guards below handle.
  const runIds =
    scopeRunIds === undefined
      ? await listDir(WORKFLOW_RUN_RUNS_PREFIX)
      : Array.from(scopeRunIds);
  for (const runId of runIds) {
    const runDirPath = `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}`;
    const runChildren = await listDir(runDirPath);
    // A scoped id may name a run absent from this tree (the change set
    // is the union of both trees' touched runs); skip the empty listing.
    if (scopeRunIds !== undefined && runChildren.length === 0) continue;
    const offender = runChildren.find((c) => !RUN_DIR_ALLOWED_CHILDREN.has(c));
    if (offender !== undefined) {
      return {
        ok: false,
        reason: `run directory ${runDirPath} contains unexpected entry ${JSON.stringify(offender)}; only "${WORKFLOW_RUN_EVENTS_DIR}", "${WORKFLOW_RUN_BLOBS_DIR}", "${WORKFLOW_RUN_EVENTS_FILE}", "${WORKFLOW_RUN_GRANTS_FILE}", and "${WORKFLOW_RUN_PARTS_DIR}" are allowed`,
      };
    }
    const hasCombined = runChildren.includes(WORKFLOW_RUN_EVENTS_FILE);
    const hasPerEvent = runChildren.includes(WORKFLOW_RUN_EVENTS_DIR);
    if (hasCombined && hasPerEvent) {
      return {
        ok: false,
        reason: `run directory ${runDirPath} carries both a combined "${WORKFLOW_RUN_EVENTS_FILE}" and a per-event "${WORKFLOW_RUN_EVENTS_DIR}" subtree`,
      };
    }
    // A sealed (combined) run carries no per-event entries; it is validated
    // by the combined-form path, not this per-event enumeration.
    if (hasCombined) continue;
    if (!hasPerEvent) {
      // The pre-first-event window: `grants.json` and `parts/` may land
      // before the child's first event. Carry such a run forward; any
      // other events-less shape stays rejected below.
      const nonPreEvent = runChildren.filter(
        (c) => c !== WORKFLOW_RUN_GRANTS_FILE && c !== WORKFLOW_RUN_PARTS_DIR,
      );
      if (nonPreEvent.length === 0) {
        continue;
      }
      return {
        ok: false,
        reason: `run directory ${runDirPath} is missing required "${WORKFLOW_RUN_EVENTS_DIR}" subdirectory`,
      };
    }
    const eventsDirPath = `${runDirPath}/${WORKFLOW_RUN_EVENTS_DIR}`;
    const filenames = await listDir(eventsDirPath);
    const entries: RunEventBlob[] = [];
    for (const filename of filenames) {
      const match = EVENT_FILENAME_RE.exec(filename);
      if (match === null) {
        return {
          ok: false,
          reason: `event filename ${eventsDirPath}/${filename} does not match <seq>.json`,
        };
      }
      const seqStr = match[1];
      if (seqStr === undefined) {
        return {
          ok: false,
          reason: `event filename ${eventsDirPath}/${filename} produced no seq capture`,
        };
      }
      entries.push({
        runId,
        filename,
        filenameSeq: Number.parseInt(seqStr, 10),
        blobPath: `${eventsDirPath}/${filename}`,
      });
    }
    entries.sort((a, b) => a.filenameSeq - b.filenameSeq);
    runs.set(runId, entries);
  }
  return { ok: true, runs };
}

/**
 * Validate combined-form (sealed) runs and return the run ids that
 * legitimately carry `events.jsonl`, so the deletion-direction guard
 * allows their per-event files to vanish. Accepted prior states: already
 * combined (immutable bytes), per-event (the combined file must be the
 * byte-for-byte fold of the prior blobs in seq order — the audit-
 * integrity boundary), or absent (a fresh sealed run; own structure
 * validated).
 */
async function validateCombinedEventRuns(
  listDir: (path: string) => Promise<string[]>,
  readBlob: (path: string) => Promise<Uint8Array>,
  priorListDir: (path: string) => Promise<string[]>,
  priorReadBlob: (path: string) => Promise<Uint8Array | null>,
  scopeRunIds: ReadonlySet<string> | undefined,
): Promise<
  { ok: true; combinedRunIds: Set<string> } | { ok: false; reason: string }
> {
  const combinedRunIds = new Set<string>();
  const runIds =
    scopeRunIds === undefined
      ? await listDir(WORKFLOW_RUN_RUNS_PREFIX)
      : Array.from(scopeRunIds);
  for (const runId of runIds) {
    const runDirPath = `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}`;
    const children = await listDir(runDirPath);
    if (!children.includes(WORKFLOW_RUN_EVENTS_FILE)) continue;
    const combinedPath = `${runDirPath}/${WORKFLOW_RUN_EVENTS_FILE}`;
    const combinedBytes = await readBlob(combinedPath);
    const content = new TextDecoder().decode(combinedBytes);

    const priorChildren = await priorListDir(runDirPath);
    if (priorChildren.includes(WORKFLOW_RUN_EVENTS_FILE)) {
      // Sealed once, immutable thereafter.
      const immutable = await checkPriorByteEquality(
        combinedPath,
        readBlob,
        priorReadBlob,
      );
      if (!immutable.ok) return immutable;
    } else if (priorChildren.includes(WORKFLOW_RUN_EVENTS_DIR)) {
      const structure = checkCombinedStructure(runId, combinedPath, content);
      if (!structure.ok) return structure;
      const fold = await checkCompactionFold(
        runId,
        runDirPath,
        combinedBytes,
        priorListDir,
        priorReadBlob,
      );
      if (!fold.ok) return fold;
    } else {
      const structure = checkCombinedStructure(runId, combinedPath, content);
      if (!structure.ok) return structure;
    }
    combinedRunIds.add(runId);
  }
  return { ok: true, combinedRunIds };
}

/**
 * Assert a compaction's combined file reproduces the prior per-event
 * blobs verbatim, in seq order, nothing added/dropped/reordered/mutated.
 * Rebuilds the expected bytes through the writer's own encoder and
 * compares exact equality.
 */
async function checkCompactionFold(
  runId: string,
  runDirPath: string,
  combinedBytes: Uint8Array,
  priorListDir: (path: string) => Promise<string[]>,
  priorReadBlob: (path: string) => Promise<Uint8Array | null>,
): Promise<ValidatePushResult> {
  const priorEventsDir = `${runDirPath}/${WORKFLOW_RUN_EVENTS_DIR}`;
  const priorEntries: { seq: number; path: string }[] = [];
  for (const filename of await priorListDir(priorEventsDir)) {
    const match = EVENT_FILENAME_RE.exec(filename);
    if (match === null || match[1] === undefined) {
      return {
        ok: false,
        reason: `prior event filename ${priorEventsDir}/${filename} does not match <seq>.json; cannot validate compaction of run ${runId}`,
      };
    }
    priorEntries.push({
      seq: Number.parseInt(match[1], 10),
      path: `${priorEventsDir}/${filename}`,
    });
  }
  priorEntries.sort((a, b) => a.seq - b.seq);
  const priorBlobs: Uint8Array[] = [];
  for (const entry of priorEntries) {
    const bytes = await priorReadBlob(entry.path);
    if (bytes === null) {
      return {
        ok: false,
        reason: `prior event ${entry.path} is unreadable; cannot validate compaction of run ${runId}`,
      };
    }
    priorBlobs.push(bytes);
  }
  // Byte equality, not decoded-string equality: each event is signed over
  // its own bytes, so the sealed file must be the verbatim concatenation
  // of the prior blobs, not merely decode-equivalent to it.
  const expected = encodeCombinedEventLog(priorBlobs);
  const sameBytes =
    combinedBytes.byteLength === expected.byteLength &&
    combinedBytes.every((b, i) => b === expected[i]);
  if (!sameBytes) {
    return {
      ok: false,
      reason: `run ${runId} compaction does not fold its prior events verbatim: ${runDirPath}/${WORKFLOW_RUN_EVENTS_FILE} must equal the run's prior events/<seq>.json blobs joined in seq order`,
    };
  }
  return { ok: true };
}

/**
 * Validate a combined log's own structure: valid envelopes, contiguous
 * seqs, one terminal event and it is last. Used when there is no prior
 * per-event form to bridge against.
 */
function checkCombinedStructure(
  runId: string,
  combinedPath: string,
  content: string,
): ValidatePushResult {
  const lines = splitCombinedEventLog(content);
  if (lines.length === 0) {
    return { ok: false, reason: `combined event log ${combinedPath} is empty` };
  }
  let baseSeq: number | null = null;
  let terminalSeq: number | null = null;
  for (const [i, line] of lines.entries()) {
    let body: unknown;
    try {
      body = JSON.parse(line);
    } catch {
      return {
        ok: false,
        reason: `combined event log ${combinedPath} line ${String(i)} is not valid JSON`,
      };
    }
    const validated = EventEnvelope(body);
    if (validated instanceof type.errors) {
      return {
        ok: false,
        reason: `combined event log ${combinedPath} line ${String(i)} envelope invalid: ${validated.summary}`,
      };
    }
    if (baseSeq === null) {
      baseSeq = validated.seq;
    } else if (validated.seq !== baseSeq + i) {
      return {
        ok: false,
        reason: `combined event log ${combinedPath} has a sequence gap at line ${String(i)} (expected seq ${String(baseSeq + i)}, got ${String(validated.seq)})`,
      };
    }
    if (terminalSeq !== null) {
      return {
        ok: false,
        reason: `combined event log ${combinedPath} has an event at seq ${String(validated.seq)} after terminal at seq ${String(terminalSeq)}`,
      };
    }
    if (TERMINAL_EVENT_TYPES.has(validated.type)) {
      terminalSeq = validated.seq;
    }
  }
  if (terminalSeq === null) {
    return {
      ok: false,
      reason: `combined event log ${combinedPath} for run ${runId} has no terminal event; only a terminated run is sealed`,
    };
  }
  return { ok: true };
}

type RunBlobEntry = {
  runId: string;
  filename: string;
  blobPath: string;
};

/**
 * Walk `runs/<runId>/blobs/` (optional) and validate each filename
 * against the sha256-hex shape. Returns the flat entry list for the
 * caller's immutability checks.
 */
async function enumerateRunBlobs(
  listDir: (path: string) => Promise<string[]>,
  scopeRunIds?: ReadonlySet<string>,
): Promise<
  { ok: true; blobs: RunBlobEntry[] } | { ok: false; reason: string }
> {
  const out: RunBlobEntry[] = [];
  // With a scope, walk only the touched runs (see enumerateEventBlobs).
  const runIds =
    scopeRunIds === undefined
      ? await listDir(WORKFLOW_RUN_RUNS_PREFIX)
      : Array.from(scopeRunIds);
  for (const runId of runIds) {
    const runDirPath = `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}`;
    const runChildren = await listDir(runDirPath);
    if (!runChildren.includes(WORKFLOW_RUN_BLOBS_DIR)) continue;
    const blobsDirPath = `${runDirPath}/${WORKFLOW_RUN_BLOBS_DIR}`;
    const filenames = await listDir(blobsDirPath);
    for (const filename of filenames) {
      if (!BLOB_FILENAME_RE.test(filename)) {
        return {
          ok: false,
          reason: `blob filename ${blobsDirPath}/${filename} does not match a lowercase 64-character sha256 hex string`,
        };
      }
      out.push({
        runId,
        filename,
        blobPath: `${blobsDirPath}/${filename}`,
      });
    }
  }
  return { ok: true, blobs: out };
}

type RunPartEntry = {
  runId: string;
  messageSegment: string;
  filename: string;
  blobPath: string;
};

/**
 * Walk `runs/<runId>/parts/<messageSegment>/` (optional) and validate:
 * round-tripping, byte-capped segments; each must be a non-empty
 * directory; filenames must match `<index>-<name>` within the cap.
 * Returns the flat entry list for immutability checks.
 */
async function enumerateRunParts(
  listDir: (path: string) => Promise<string[]>,
  scopeRunIds?: ReadonlySet<string>,
): Promise<
  { ok: true; parts: RunPartEntry[] } | { ok: false; reason: string }
> {
  const out: RunPartEntry[] = [];
  // With a scope, walk only the touched runs (see enumerateEventBlobs).
  const runIds =
    scopeRunIds === undefined
      ? await listDir(WORKFLOW_RUN_RUNS_PREFIX)
      : Array.from(scopeRunIds);
  for (const runId of runIds) {
    const runDirPath = `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}`;
    const runChildren = await listDir(runDirPath);
    if (!runChildren.includes(WORKFLOW_RUN_PARTS_DIR)) continue;
    const partsDirPath = `${runDirPath}/${WORKFLOW_RUN_PARTS_DIR}`;
    const messageSegments = await listDir(partsDirPath);
    for (const messageSegment of messageSegments) {
      const roundTrip = checkUrlSegmentRoundTrip(messageSegment);
      if (!roundTrip.ok) {
        return {
          ok: false,
          reason: `mail part message ${roundTrip.reason} under ${partsDirPath}`,
        };
      }
      if (
        mailPartComponentByteLength(messageSegment) >
        MAX_MAIL_PART_PATH_COMPONENT_BYTES
      ) {
        return {
          ok: false,
          reason: `mail part message segment ${JSON.stringify(messageSegment)} under ${partsDirPath} exceeds the ${String(MAX_MAIL_PART_PATH_COMPONENT_BYTES)}-byte path-component limit`,
        };
      }
      const messageDirPath = `${partsDirPath}/${messageSegment}`;
      const filenames = await listDir(messageDirPath);
      // A message segment must be a directory carrying at least one part
      // file; an empty listing is a dangling blob or empty directory, and
      // both are rejected so untrusted content has no silent-accept path.
      if (filenames.length === 0) {
        return {
          ok: false,
          reason: `mail part message segment ${JSON.stringify(messageSegment)} under ${partsDirPath} is not a directory carrying mail part files`,
        };
      }
      for (const filename of filenames) {
        if (!PART_FILENAME_RE.test(filename)) {
          return {
            ok: false,
            reason: `mail part filename ${messageDirPath}/${filename} does not match <index>-<name>`,
          };
        }
        if (
          mailPartComponentByteLength(filename) >
          MAX_MAIL_PART_PATH_COMPONENT_BYTES
        ) {
          return {
            ok: false,
            reason: `mail part filename ${messageDirPath}/${filename} exceeds the ${String(MAX_MAIL_PART_PATH_COMPONENT_BYTES)}-byte path-component limit`,
          };
        }
        // Each part must be a leaf blob: the permissive `<index>-<name>`
        // shape would otherwise admit a nested directory (breaking the
        // one-file-per-part invariant, and making the immutability
        // resolver `readBlob` a tree path and throw).
        const filePath = `${messageDirPath}/${filename}`;
        if ((await listDir(filePath)).length > 0) {
          return {
            ok: false,
            reason: `mail part ${filePath} is a directory; each mail part must be a single file`,
          };
        }
        out.push({
          runId,
          messageSegment,
          filename,
          blobPath: filePath,
        });
      }
    }
  }
  return { ok: true, parts: out };
}

/**
 * Enforce parts immutability against the prior tree, both directions.
 * A prior path must carry the same git blob OID (filenames are not
 * content-addressed, so the OID compare is the load-bearing guard, and
 * it avoids re-reading the bytes). No deletion: reclaim drops the whole
 * run subtree outside validatePush. Shape is enforced by
 * `enumerateRunParts`.
 */
async function validateRunPartsSubtree(args: {
  listDir: (path: string) => Promise<string[]>;
  priorListDir: (path: string) => Promise<string[]>;
  readBlob: (path: string) => Promise<Uint8Array>;
  priorReadBlob: (path: string) => Promise<Uint8Array | null>;
  listDirOids:
    | ((path: string) => Promise<{ name: string; oid: string }[]>)
    | undefined;
  priorListDirOids:
    | ((path: string) => Promise<{ name: string; oid: string }[]>)
    | undefined;
  scopeRunIds: ReadonlySet<string> | undefined;
}): Promise<ValidatePushResult> {
  const prospective = await enumerateRunParts(args.listDir, args.scopeRunIds);
  if (!prospective.ok) return prospective;
  const prior = await enumerateRunParts(args.priorListDir, args.scopeRunIds);
  if (!prior.ok) {
    return {
      ok: false,
      reason: `prior tree's mail parts subtree is structurally invalid: ${prior.reason}`,
    };
  }

  const prospectiveOid = makeListingOidResolver(
    "prospective",
    args.listDirOids,
    async (p) => (await git.hashBlob({ object: await args.readBlob(p) })).oid,
  );
  const priorOid = makeListingOidResolver(
    "prior",
    args.priorListDirOids,
    async (p) => {
      const bytes = await args.priorReadBlob(p);
      if (bytes === null) {
        throw new Error(
          `mail parts: prior entry ${p} was enumerated but its bytes could not be read`,
        );
      }
      return (await git.hashBlob({ object: bytes })).oid;
    },
  );

  const prospectivePaths = new Set(prospective.parts.map((e) => e.blobPath));
  const priorPaths = new Set(prior.parts.map((e) => e.blobPath));

  for (const entry of prospective.parts) {
    if (!priorPaths.has(entry.blobPath)) continue; // newly added
    const [next, before] = await Promise.all([
      prospectiveOid(entry.blobPath),
      priorOid(entry.blobPath),
    ]);
    if (next !== before) {
      return {
        ok: false,
        reason: `mail part ${entry.blobPath} bytes diverge from the prior tree (blob OID ${next} vs ${before}); mail part files are immutable once written`,
      };
    }
  }
  for (const entry of prior.parts) {
    if (prospectivePaths.has(entry.blobPath)) continue;
    return {
      ok: false,
      reason: `mail part ${entry.blobPath} present in the prior tree is missing from the prospective tree; mail part files are immutable once written`,
    };
  }
  return { ok: true };
}

/**
 * Enforce blob immutability via prior-tree byte equality: a blob
 * present in the prior tree must reappear byte-identical.
 */
async function checkBlobPriorByteEquality(
  blobPath: string,
  readBlob: (path: string) => Promise<Uint8Array>,
  priorReadBlob: (path: string) => Promise<Uint8Array | null>,
): Promise<ValidatePushResult> {
  const prior = await priorReadBlob(blobPath);
  if (prior === null) return { ok: true };
  const prospective = await readBlob(blobPath);
  if (prior.byteLength !== prospective.byteLength) {
    return {
      ok: false,
      reason: `blob ${blobPath} bytes diverge from the prior tree (lengths ${String(prior.byteLength)} vs ${String(prospective.byteLength)}); blob entries are immutable once written`,
    };
  }
  for (let i = 0; i < prior.byteLength; i++) {
    if (prior[i] !== prospective[i]) {
      return {
        ok: false,
        reason: `blob ${blobPath} bytes diverge from the prior tree at offset ${String(i)}; blob entries are immutable once written`,
      };
    }
  }
  return { ok: true };
}

type ParsedEventBlob = {
  entry: RunEventBlob;
  body: { type: string; seq: number; [k: string]: unknown };
};

async function parseEventBlob(
  entry: RunEventBlob,
  readBlob: (path: string) => Promise<Uint8Array>,
): Promise<
  { ok: true; parsed: ParsedEventBlob } | { ok: false; reason: string }
> {
  let raw: Uint8Array;
  try {
    raw = await readBlob(entry.blobPath);
  } catch (cause) {
    return {
      ok: false,
      reason: `event ${entry.blobPath} could not be read from the tree: ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    };
  }
  let body: unknown;
  try {
    body = JSON.parse(new TextDecoder().decode(raw));
  } catch (cause) {
    return {
      ok: false,
      reason: `event ${entry.blobPath} is not valid JSON: ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    };
  }
  const validated = EventEnvelope(body);
  if (validated instanceof type.errors) {
    return {
      ok: false,
      reason: `event ${entry.blobPath} envelope invalid: ${validated.summary}`,
    };
  }
  if (validated.seq !== entry.filenameSeq) {
    return {
      ok: false,
      reason: `event ${entry.blobPath} body.seq ${String(validated.seq)} does not match filename seq ${String(entry.filenameSeq)}`,
    };
  }
  if (validated.type === "CancelRequested") {
    const cancelFields = CancelRequestedFields(body);
    if (cancelFields instanceof type.errors) {
      return {
        ok: false,
        reason: `event ${entry.blobPath} CancelRequested payload invalid: ${cancelFields.summary}`,
      };
    }
    if (!CANCEL_REQUESTED_ORIGINS.has(cancelFields.origin)) {
      return {
        ok: false,
        reason: `event ${entry.blobPath} CancelRequested origin ${JSON.stringify(cancelFields.origin)} is not a recognised CancelOrigin`,
      };
    }
  }
  return { ok: true, parsed: { entry, body: validated } };
}

/**
 * Prior-tree byte equality for events: newly added passes; a retained
 * path must reappear byte-identical. Owns the append-only invariant at
 * the handler scope.
 */
async function checkPriorByteEquality(
  blobPath: string,
  readBlob: (path: string) => Promise<Uint8Array>,
  priorReadBlob: (path: string) => Promise<Uint8Array | null>,
): Promise<ValidatePushResult> {
  const prior = await priorReadBlob(blobPath);
  if (prior === null) return { ok: true };
  const prospective = await readBlob(blobPath);
  if (prior.byteLength !== prospective.byteLength) {
    return {
      ok: false,
      reason: `event ${blobPath} bytes diverge from the prior tree (lengths ${String(prior.byteLength)} vs ${String(prospective.byteLength)}); event blobs are append-only`,
    };
  }
  for (let i = 0; i < prior.byteLength; i++) {
    if (prior[i] !== prospective[i]) {
      return {
        ok: false,
        reason: `event ${blobPath} bytes diverge from the prior tree at offset ${String(i)}; event blobs are append-only`,
      };
    }
  }
  return { ok: true };
}

/**
 * Require a URL-encoded segment to round-trip through decode/encode so
 * consumers can rely on a single canonical encoding. Shared by every
 * encoded-identity subtree (`addresses/`, `agent-state/`, mail parts).
 */
function checkUrlSegmentRoundTrip(segment: string):
  | {
      ok: true;
      decoded: string;
    }
  | {
      ok: false;
      reason: string;
    } {
  let decoded: string;
  try {
    decoded = decodeURIComponent(segment);
  } catch (cause) {
    return {
      ok: false,
      reason: `segment ${JSON.stringify(segment)} is not a valid URL-encoded string: ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    };
  }
  const reencoded = encodeURIComponent(decoded);
  if (reencoded !== segment) {
    return {
      ok: false,
      reason: `segment ${JSON.stringify(segment)} does not round-trip URL-encoding (re-encoded as ${JSON.stringify(reencoded)})`,
    };
  }
  return { ok: true, decoded };
}

type ClaimCheckBlob = {
  kind: "inbox" | "processing" | "consumed";
  addressSegment: string;
  decodedAddress: string;
  filename: string;
  /**
   * Filename-extracted receivedAt for inbox / processing. Absent on
   * consumed (which is keyed by messageId only).
   */
  receivedAtFromFilename: number | null;
  /**
   * Filename-extracted messageId. For inbox / processing this is the
   * post-dash tail of the basename; for consumed it is the bare
   * basename.
   */
  messageIdFromFilename: string;
  blobPath: string;
  /**
   * Git blob object id of the entry, resolved for `consumed` entries
   * when the enumeration is given an OID resolver. Two entries at the
   * same path whose OIDs match are byte-identical (git trees are
   * content-addressed), so the consumed immutability check compares
   * OIDs instead of re-reading both blobs for retained entries. Left
   * `undefined` on inbox/processing entries, which the resolver does
   * not cover.
   */
  oid?: string;
};

/**
 * FIFO comparator: numeric `receivedAt`, messageId tiebreak. A string
 * compare would order `"100-…"` before `"99-…"`, breaking FIFO.
 */
function compareQueueEntries(a: ClaimCheckBlob, b: ClaimCheckBlob): number {
  const aReceivedAt = a.receivedAtFromFilename;
  const bReceivedAt = b.receivedAtFromFilename;
  if (aReceivedAt === null || bReceivedAt === null) {
    throw new Error(
      "compareQueueEntries: queue entries must carry a parsed receivedAt",
    );
  }
  if (aReceivedAt !== bReceivedAt) return aReceivedAt - bReceivedAt;
  const aId = a.messageIdFromFilename;
  const bId = b.messageIdFromFilename;
  if (aId < bId) return -1;
  if (aId > bId) return 1;
  return 0;
}

type ClaimCheckAddressBucket = {
  decodedAddress: string;
  inbox: ClaimCheckBlob[];
  processing: ClaimCheckBlob[];
  consumed: ClaimCheckBlob[];
  /**
   * Repo-root-relative path of the address's `watermark.json` when the
   * tree carries one, else `null`. The blob's parsed value is read on
   * demand via `readBlob`/`priorReadBlob` (not eagerly, to keep the
   * enumeration pure of body reads).
   */
  watermarkPath: string | null;
};

async function enumerateClaimCheckBlobs(
  listDir: (path: string) => Promise<string[]>,
  resolveConsumedOid?: (blobPath: string) => Promise<string>,
): Promise<
  | { ok: true; perAddress: Map<string, ClaimCheckAddressBucket> }
  | { ok: false; reason: string }
> {
  const perAddress = new Map<string, ClaimCheckAddressBucket>();
  const segments = await listDir(WORKFLOW_RUN_ADDRESSES_PREFIX);
  for (const segment of segments) {
    const roundTrip = checkUrlSegmentRoundTrip(segment);
    if (!roundTrip.ok) {
      return { ok: false, reason: `address ${roundTrip.reason}` };
    }
    const addrDir = `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${segment}`;
    const children = await listDir(addrDir);
    for (const child of children) {
      if (CLAIM_CHECK_SUBDIRS.has(child)) continue;
      if (child === WORKFLOW_RUN_WATERMARK_FILE) continue;
      return {
        ok: false,
        reason: `address directory ${addrDir} contains unexpected entry ${JSON.stringify(child)}; allowed: "${WORKFLOW_RUN_INBOX_DIR}", "${WORKFLOW_RUN_PROCESSING_DIR}", "${WORKFLOW_RUN_CONSUMED_DIR}", "${WORKFLOW_RUN_WATERMARK_FILE}"`,
      };
    }
    const bucket: ClaimCheckAddressBucket = perAddress.get(segment) ?? {
      decodedAddress: roundTrip.decoded,
      inbox: [],
      processing: [],
      consumed: [],
      watermarkPath: null,
    };
    if (children.includes(WORKFLOW_RUN_WATERMARK_FILE)) {
      bucket.watermarkPath = `${addrDir}/${WORKFLOW_RUN_WATERMARK_FILE}`;
    }
    for (const subdir of CLAIM_CHECK_SUBDIRS) {
      if (!children.includes(subdir)) continue;
      const dirPath = `${addrDir}/${subdir}`;
      const filenames = await listDir(dirPath);
      for (const filename of filenames) {
        if (
          subdir === WORKFLOW_RUN_INBOX_DIR ||
          subdir === WORKFLOW_RUN_PROCESSING_DIR
        ) {
          const match = QUEUE_FILENAME_RE.exec(filename);
          if (match === null) {
            return {
              ok: false,
              reason: `${subdir} filename ${dirPath}/${filename} does not match <receivedAt>-<messageId>.json`,
            };
          }
          const receivedAtStr = match[1];
          const messageId = match[2];
          if (receivedAtStr === undefined || messageId === undefined) {
            return {
              ok: false,
              reason: `${subdir} filename ${dirPath}/${filename} produced no captures`,
            };
          }
          const entry: ClaimCheckBlob = {
            kind: subdir === WORKFLOW_RUN_INBOX_DIR ? "inbox" : "processing",
            addressSegment: segment,
            decodedAddress: roundTrip.decoded,
            filename,
            receivedAtFromFilename: Number.parseInt(receivedAtStr, 10),
            messageIdFromFilename: messageId,
            blobPath: `${dirPath}/${filename}`,
          };
          if (subdir === WORKFLOW_RUN_INBOX_DIR) bucket.inbox.push(entry);
          else bucket.processing.push(entry);
        } else {
          const match = CONSUMED_FILENAME_RE.exec(filename);
          if (match === null) {
            return {
              ok: false,
              reason: `${WORKFLOW_RUN_CONSUMED_DIR} filename ${dirPath}/${filename} does not match <messageId>.json`,
            };
          }
          const messageId = match[1];
          if (messageId === undefined) {
            return {
              ok: false,
              reason: `${WORKFLOW_RUN_CONSUMED_DIR} filename ${dirPath}/${filename} produced no message-id capture`,
            };
          }
          const consumedBlobPath = `${dirPath}/${filename}`;
          const consumedEntry: ClaimCheckBlob = {
            kind: "consumed",
            addressSegment: segment,
            decodedAddress: roundTrip.decoded,
            filename,
            receivedAtFromFilename: null,
            messageIdFromFilename: messageId,
            blobPath: consumedBlobPath,
          };
          if (resolveConsumedOid !== undefined) {
            consumedEntry.oid = await resolveConsumedOid(consumedBlobPath);
          }
          bucket.consumed.push(consumedEntry);
        }
      }
    }
    // FIFO ordering: sort by the parsed numeric receivedAt prefix
    // with a lexicographic messageId tiebreak. String-sorting the
    // raw filename would put "99-…" after "100-…" because '9' > '1',
    // breaking the FIFO invariant for non-uniform digit widths.
    bucket.inbox.sort(compareQueueEntries);
    bucket.processing.sort(compareQueueEntries);
    bucket.consumed.sort((a, b) =>
      a.filename < b.filename ? -1 : a.filename > b.filename ? 1 : 0,
    );
    perAddress.set(segment, bucket);
  }
  return { ok: true, perAddress };
}

/**
 * Read and validate a per-address watermark; an absent blob reads as 0
 * (never pruned, nothing refused). Works with `readBlob` or
 * `priorReadBlob`.
 */
async function readWatermark(
  watermarkPath: string,
  readBlob: (path: string) => Promise<Uint8Array | null>,
): Promise<{ ok: true; watermark: number } | { ok: false; reason: string }> {
  let raw: Uint8Array | null;
  try {
    raw = await readBlob(watermarkPath);
  } catch (cause) {
    return {
      ok: false,
      reason: `watermark ${watermarkPath} could not be read from the tree: ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    };
  }
  if (raw === null) return { ok: true, watermark: 0 };
  let bodyJson: unknown;
  try {
    bodyJson = JSON.parse(new TextDecoder().decode(raw));
  } catch (cause) {
    return {
      ok: false,
      reason: `watermark ${watermarkPath} is not valid JSON: ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    };
  }
  const validated = WatermarkEnvelope(bodyJson);
  if (validated instanceof type.errors) {
    return {
      ok: false,
      reason: `watermark ${watermarkPath} envelope invalid: ${validated.summary}`,
    };
  }
  return { ok: true, watermark: validated.watermark };
}

async function parseConsumedBlob(
  entry: ClaimCheckBlob,
  readBlob: (path: string) => Promise<Uint8Array>,
): Promise<
  { ok: true; body: ConsumedEnvelope } | { ok: false; reason: string }
> {
  return parseConsumedBlobFrom(entry, readBlob);
}

/**
 * Read and validate a consumed envelope from a `priorReadBlob`-shaped
 * reader; `null` (absent) is structural damage, since the caller only
 * passes paths the prior tree is known to carry.
 */
async function parseConsumedBlobFrom(
  entry: ClaimCheckBlob,
  readBlob: (path: string) => Promise<Uint8Array | null>,
): Promise<
  { ok: true; body: ConsumedEnvelope } | { ok: false; reason: string }
> {
  let raw: Uint8Array | null;
  try {
    raw = await readBlob(entry.blobPath);
  } catch (cause) {
    return {
      ok: false,
      reason: `consumed ${entry.blobPath} could not be read from the tree: ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    };
  }
  if (raw === null) {
    return {
      ok: false,
      reason: `consumed ${entry.blobPath} was enumerated in the tree but its bytes could not be read`,
    };
  }
  let bodyJson: unknown;
  try {
    bodyJson = JSON.parse(new TextDecoder().decode(raw));
  } catch (cause) {
    return {
      ok: false,
      reason: `consumed ${entry.blobPath} is not valid JSON: ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    };
  }
  const validated = ConsumedEnvelope(bodyJson);
  if (validated instanceof type.errors) {
    return {
      ok: false,
      reason: `consumed ${entry.blobPath} envelope invalid: ${validated.summary}`,
    };
  }
  if (validated.messageId !== entry.messageIdFromFilename) {
    return {
      ok: false,
      reason: `consumed ${entry.blobPath} body.messageId ${JSON.stringify(validated.messageId)} does not match filename messageId ${JSON.stringify(entry.messageIdFromFilename)}`,
    };
  }
  if (validated.address !== entry.decodedAddress) {
    return {
      ok: false,
      reason: `consumed ${entry.blobPath} body.address ${JSON.stringify(validated.address)} does not match decoded address segment ${JSON.stringify(entry.decodedAddress)}`,
    };
  }
  return { ok: true, body: validated };
}

async function parseQueueBlob(
  entry: ClaimCheckBlob,
  readBlob: (path: string) => Promise<Uint8Array>,
): Promise<
  { ok: true; body: ClaimCheckEnvelope } | { ok: false; reason: string }
> {
  let raw: Uint8Array;
  try {
    raw = await readBlob(entry.blobPath);
  } catch (cause) {
    return {
      ok: false,
      reason: `${entry.kind} ${entry.blobPath} could not be read from the tree: ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    };
  }
  let bodyJson: unknown;
  try {
    bodyJson = JSON.parse(new TextDecoder().decode(raw));
  } catch (cause) {
    return {
      ok: false,
      reason: `${entry.kind} ${entry.blobPath} is not valid JSON: ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    };
  }
  const validated = ClaimCheckEnvelope(bodyJson);
  if (validated instanceof type.errors) {
    return {
      ok: false,
      reason: `${entry.kind} ${entry.blobPath} envelope invalid: ${validated.summary}`,
    };
  }
  if (validated.messageId !== entry.messageIdFromFilename) {
    return {
      ok: false,
      reason: `${entry.kind} ${entry.blobPath} body.messageId ${JSON.stringify(validated.messageId)} does not match filename messageId ${JSON.stringify(entry.messageIdFromFilename)}`,
    };
  }
  if (validated.receivedAt !== entry.receivedAtFromFilename) {
    return {
      ok: false,
      reason: `${entry.kind} ${entry.blobPath} body.receivedAt ${String(validated.receivedAt)} does not match filename receivedAt ${String(entry.receivedAtFromFilename)}`,
    };
  }
  if (validated.address !== entry.decodedAddress) {
    return {
      ok: false,
      reason: `${entry.kind} ${entry.blobPath} body.address ${JSON.stringify(validated.address)} does not match decoded address segment ${JSON.stringify(entry.decodedAddress)}`,
    };
  }
  return { ok: true, body: validated };
}

/** Hash a consumed entry's bytes to its git blob OID (the no-listing fallback). */
async function hashConsumedBlobOid(bytes: Uint8Array): Promise<string> {
  const { oid } = await git.hashBlob({ object: bytes });
  return oid;
}

/**
 * Resolve a consumed entry's git blob OID. With a substrate `listDirOids`
 * listing the OID comes from the tree (one cached `readTree` per
 * directory); otherwise it falls back to hashing the bytes. `sideLabel`
 * distinguishes prior vs. prospective in errors.
 */
function makeListingOidResolver(
  sideLabel: string,
  listDirOids:
    | ((path: string) => Promise<{ name: string; oid: string }[]>)
    | undefined,
  hashFallback: (blobPath: string) => Promise<string>,
): (blobPath: string) => Promise<string> {
  const dirOidCache = new Map<string, Map<string, string>>();
  return async (blobPath) => {
    if (listDirOids !== undefined) {
      const slash = blobPath.lastIndexOf("/");
      const dir = blobPath.slice(0, slash);
      const name = blobPath.slice(slash + 1);
      let byName = dirOidCache.get(dir);
      if (byName === undefined) {
        byName = new Map<string, string>();
        for (const entry of await listDirOids(dir)) {
          byName.set(entry.name, entry.oid);
        }
        dirOidCache.set(dir, byName);
      }
      const oid = byName.get(name);
      if (oid === undefined) {
        throw new Error(
          `delta claim-check: ${sideLabel} tree listing has no OID for enumerated consumed entry ${blobPath}`,
        );
      }
      return oid;
    }
    return hashFallback(blobPath);
  };
}

function makePriorConsumedOidResolver(
  priorReadBlob: (path: string) => Promise<Uint8Array | null>,
  priorListDirOids:
    | ((path: string) => Promise<{ name: string; oid: string }[]>)
    | undefined,
): (blobPath: string) => Promise<string> {
  return makeListingOidResolver("prior", priorListDirOids, async (blobPath) => {
    const bytes = await priorReadBlob(blobPath);
    if (bytes === null) {
      throw new Error(
        `delta claim-check: consumed entry ${blobPath} was enumerated in the prior tree but its bytes could not be read`,
      );
    }
    return hashConsumedBlobOid(bytes);
  });
}

/**
 * Validate the `addresses/...` claim-check subtree: filename shapes,
 * envelopes, address round-trip, per-messageId atomicity across the
 * three states, consumed immutability, and the inbox→processing /
 * processing→consumed transitions against the prior tree. The consumed
 * index is validated by its per-commit DELTA: retained entries (same
 * name, same OID) are skipped, added entries are parsed, removed
 * entries are checked against the watermark.
 */
async function validateClaimCheckSubtree(
  listDir: (path: string) => Promise<string[]>,
  readBlob: (path: string) => Promise<Uint8Array>,
  priorReadBlob: (path: string) => Promise<Uint8Array | null>,
  priorListDir: (path: string) => Promise<string[]>,
  priorListDirOids?: (path: string) => Promise<{ name: string; oid: string }[]>,
  listDirOids?: (path: string) => Promise<{ name: string; oid: string }[]>,
): Promise<ValidatePushResult> {
  // Surface each consumed entry's git blob OID during enumeration
  // straight from the tree listing when the substrate provides it,
  // falling back to hashing the bytes otherwise.
  const prospectiveConsumedOid = makeListingOidResolver(
    "prospective",
    listDirOids,
    async (blobPath) => hashConsumedBlobOid(await readBlob(blobPath)),
  );
  const priorConsumedOid = makePriorConsumedOidResolver(
    priorReadBlob,
    priorListDirOids,
  );

  const enumerated = await enumerateClaimCheckBlobs(
    listDir,
    prospectiveConsumedOid,
  );
  if (!enumerated.ok) return enumerated;
  const priorEnumerated = await enumerateClaimCheckBlobs(
    priorListDir,
    priorConsumedOid,
  );
  if (!priorEnumerated.ok) {
    // The prior tree is the committed state — if its claim-check
    // shape is already broken, surface it with a distinct rejection
    // prefix so an operator can tell prior-state damage from a
    // misconfigured push.
    return {
      ok: false,
      reason: `prior tree's claim-check subtree is structurally invalid: ${priorEnumerated.reason}`,
    };
  }

  const emptyBucket = (decodedAddress: string): ClaimCheckAddressBucket => ({
    decodedAddress,
    inbox: [],
    processing: [],
    consumed: [],
    watermarkPath: null,
  });
  // Iterate the union of both trees' segments so a wiped address still
  // runs its prior-retention checks.
  const allSegments = new Set<string>([
    ...enumerated.perAddress.keys(),
    ...priorEnumerated.perAddress.keys(),
  ]);
  for (const segment of allSegments) {
    const priorBucket = priorEnumerated.perAddress.get(segment);
    const prospectiveBucketForSegment = enumerated.perAddress.get(segment);
    const decodedAddress =
      prospectiveBucketForSegment?.decodedAddress ??
      priorBucket?.decodedAddress;
    if (decodedAddress === undefined) {
      throw new Error(
        `validateClaimCheckSubtree: segment ${JSON.stringify(segment)} appeared in the union of prospective and prior segments but neither bucket carries a decoded address`,
      );
    }
    const bucket = prospectiveBucketForSegment ?? emptyBucket(decodedAddress);
    // Atomicity: one entry per messageId across the three states. Keying
    // on (messageId, kind, filename) keeps two same-messageId inbox
    // entries at different receivedAt values distinct — a Set-of-kinds
    // would collapse them and miss the collision.
    const messageIdToLocations = new Map<
      string,
      { kind: "inbox" | "processing" | "consumed"; filename: string }[]
    >();
    for (const entry of [...bucket.inbox, ...bucket.processing]) {
      const parsed = await parseQueueBlob(entry, readBlob);
      if (!parsed.ok) return parsed;
      const list = messageIdToLocations.get(entry.messageIdFromFilename) ?? [];
      list.push({ kind: entry.kind, filename: entry.filename });
      messageIdToLocations.set(entry.messageIdFromFilename, list);
    }
    for (const entry of bucket.consumed) {
      // The messageId is the filename stem, so this needs no blob read;
      // retained entries are proven immutable by the OID compare below.
      const list = messageIdToLocations.get(entry.messageIdFromFilename) ?? [];
      list.push({ kind: entry.kind, filename: entry.filename });
      messageIdToLocations.set(entry.messageIdFromFilename, list);
    }
    for (const [messageId, locations] of messageIdToLocations) {
      if (locations.length > 1) {
        const sorted = [...locations].sort((a, b) => {
          if (a.kind !== b.kind) return a.kind < b.kind ? -1 : 1;
          if (a.filename !== b.filename)
            return a.filename < b.filename ? -1 : 1;
          return 0;
        });
        const kinds = new Set(sorted.map((l) => l.kind));
        if (kinds.size > 1) {
          return {
            ok: false,
            reason: `address ${JSON.stringify(bucket.decodedAddress)} message ${JSON.stringify(messageId)} appears in multiple queue states ${JSON.stringify(Array.from(kinds).sort())}; at most one of inbox/processing/consumed is permitted`,
          };
        }
        const kind = sorted[0]?.kind;
        if (kind === undefined) throw new Error("unreachable");
        return {
          ok: false,
          reason: `address ${JSON.stringify(bucket.decodedAddress)} message ${JSON.stringify(messageId)} appears at multiple ${kind} positions ${JSON.stringify(sorted.map((l) => l.filename))}; at most one entry per messageId is permitted`,
        };
      }
    }

    // Consumed entries are immutable: a retained path must carry the
    // same OID (content-addressed, so no byte re-read). Load-bearing for
    // exactly-once — a mutated `receivedAt` could fake a below-watermark
    // prune and let a re-submission miss dedup.
    const priorConsumedOidByPath = new Map<string, string>();
    for (const e of priorBucket?.consumed ?? []) {
      if (e.oid === undefined) {
        throw new Error(
          `delta claim-check: prior consumed entry ${e.blobPath} was enumerated without an OID`,
        );
      }
      priorConsumedOidByPath.set(e.blobPath, e.oid);
    }
    for (const entry of bucket.consumed) {
      const priorOid = priorConsumedOidByPath.get(entry.blobPath);
      if (priorOid === undefined) continue; // newly added; validated below
      if (entry.oid === undefined) {
        throw new Error(
          `delta claim-check: prospective consumed entry ${entry.blobPath} was enumerated without an OID`,
        );
      }
      if (entry.oid !== priorOid) {
        return {
          ok: false,
          reason: `consumed ${entry.blobPath} bytes diverge from the prior tree (blob OID ${entry.oid} vs ${priorOid}); consumed entries are immutable once written`,
        };
      }
    }

    const prospectiveConsumedPaths = new Set<string>(
      bucket.consumed.map((e) => e.blobPath),
    );
    const prospectiveProcessingPaths = new Set<string>(
      bucket.processing.map((e) => e.blobPath),
    );
    const prospectiveInboxByFilename = new Map<string, ClaimCheckBlob>();
    const prospectiveInboxPaths = new Set<string>();
    for (const e of bucket.inbox) {
      prospectiveInboxByFilename.set(e.filename, e);
      prospectiveInboxPaths.add(e.blobPath);
    }
    const prospectiveProcessingByFilename = new Map<string, ClaimCheckBlob>();
    for (const e of bucket.processing)
      prospectiveProcessingByFilename.set(e.filename, e);
    const prospectiveConsumedByMessageId = new Map<string, ClaimCheckBlob>();
    for (const e of bucket.consumed)
      prospectiveConsumedByMessageId.set(e.messageIdFromFilename, e);

    // Deletion direction: reject any prior path that vanishes except via
    // a permitted transition (or, for consumed, a watermark-passed
    // prune). The watermark is a monotonic `receivedAt` horizon;
    // resolve both sides up front so every drop is bound to it.
    let prospectiveWatermark = 0;
    if (bucket.watermarkPath !== null) {
      const wm = await readWatermark(bucket.watermarkPath, (p) => readBlob(p));
      if (!wm.ok) return wm;
      prospectiveWatermark = wm.watermark;
    }
    let priorWatermark = 0;
    if (priorBucket?.watermarkPath != null) {
      const wm = await readWatermark(priorBucket.watermarkPath, priorReadBlob);
      if (!wm.ok) return wm;
      priorWatermark = wm.watermark;
    }
    if (prospectiveWatermark < priorWatermark) {
      return {
        ok: false,
        reason: `address ${JSON.stringify(decodedAddress)} retention watermark regressed from ${String(priorWatermark)} to ${String(prospectiveWatermark)}; the watermark is monotonically non-decreasing`,
      };
    }

    if (priorBucket !== undefined) {
      // A dropped consumed entry must be strictly below the prospective
      // watermark — prune only what the watermark passed. The suffix
      // relation is deliberately NOT enforced: a retained entry may sit
      // below the watermark (a late-consumed or replayed message), which
      // gives only extra dedup.
      for (const e of priorBucket.consumed) {
        if (prospectiveConsumedPaths.has(e.blobPath)) continue;
        const priorParsed = await parseConsumedBlobFrom(e, priorReadBlob);
        if (!priorParsed.ok) return priorParsed;
        const receivedAt = priorParsed.body.receivedAt;
        if (receivedAt >= prospectiveWatermark) {
          return {
            ok: false,
            reason: `consumed ${e.blobPath} present in the prior tree is missing from the prospective tree but its receivedAt ${String(receivedAt)} is not below the retention watermark ${String(prospectiveWatermark)}; consumed entries may be pruned only once the watermark has passed them`,
          };
        }
      }
      for (const e of priorBucket.processing) {
        if (prospectiveProcessingPaths.has(e.blobPath)) continue;
        // A processing entry may vanish only into a matching consumed
        // entry (markConsumed) or back to inbox under the same filename
        // (replayProcessingToInbox); anything else is an in-flight loss.
        const consumedMatch = prospectiveConsumedByMessageId.get(
          e.messageIdFromFilename,
        );
        const inboxMatch = prospectiveInboxByFilename.get(e.filename);
        if (consumedMatch !== undefined || inboxMatch !== undefined) continue;
        return {
          ok: false,
          reason: `processing ${e.blobPath} present in the prior tree is missing from the prospective tree without a matching consumed or inbox transition; in-flight processing entries cannot be silently dropped`,
        };
      }
      for (const e of priorBucket.inbox) {
        if (prospectiveInboxPaths.has(e.blobPath)) continue;
        // A prior inbox entry may vanish only into processing (same
        // filename) or consumed (same messageId); anything else is an
        // inbound-mail loss.
        const processingMatch = prospectiveProcessingByFilename.get(e.filename);
        const consumedMatch = prospectiveConsumedByMessageId.get(
          e.messageIdFromFilename,
        );
        if (processingMatch !== undefined || consumedMatch !== undefined)
          continue;
        return {
          ok: false,
          reason: `inbox ${e.blobPath} present in the prior tree is missing from the prospective tree without a matching processing or consumed transition; pending inbox entries cannot be silently dropped`,
        };
      }
    }

    const priorInboxByFilename = new Map<string, ClaimCheckBlob>();
    const priorProcessingByMessageId = new Map<string, ClaimCheckBlob>();
    if (priorBucket !== undefined) {
      for (const e of priorBucket.inbox)
        priorInboxByFilename.set(e.filename, e);
      for (const e of priorBucket.processing)
        priorProcessingByMessageId.set(e.messageIdFromFilename, e);
    }
    const priorProcessingPaths = new Set<string>(
      (priorBucket?.processing ?? []).map((e) => e.blobPath),
    );
    const priorConsumedPaths = new Set<string>(
      (priorBucket?.consumed ?? []).map((e) => e.blobPath),
    );

    // A newly-added processing entry must match a prior-tree inbox entry
    // at the same filename: inbox→processing is the only way to grow it.
    for (const entry of bucket.processing) {
      if (priorProcessingPaths.has(entry.blobPath)) continue;
      const priorInbox = priorInboxByFilename.get(entry.filename);
      if (priorInbox === undefined) {
        return {
          ok: false,
          reason: `processing ${entry.blobPath} is newly added but the prior tree has no matching inbox entry ${JSON.stringify(`${WORKFLOW_RUN_ADDRESSES_PREFIX}/${segment}/${WORKFLOW_RUN_INBOX_DIR}/${entry.filename}`)}; processing entries must originate from a prior-tree inbox entry`,
        };
      }
    }

    // A newly-added consumed entry must match a prior-tree processing
    // entry at the same address+messageId, with matching receivedAt.
    for (const entry of bucket.consumed) {
      if (priorConsumedPaths.has(entry.blobPath)) continue;
      const priorProcessing = priorProcessingByMessageId.get(
        entry.messageIdFromFilename,
      );
      if (priorProcessing === undefined) {
        return {
          ok: false,
          reason: `consumed ${entry.blobPath} is newly added but the prior tree has no matching processing entry for messageId ${JSON.stringify(entry.messageIdFromFilename)}; consumed entries must originate from a prior-tree processing entry`,
        };
      }
      const parsed = await parseConsumedBlob(entry, readBlob);
      if (!parsed.ok) return parsed;
      const consumedBody = parsed.body;
      if (consumedBody.receivedAt !== priorProcessing.receivedAtFromFilename) {
        return {
          ok: false,
          reason: `consumed ${entry.blobPath} body.receivedAt ${String(consumedBody.receivedAt)} does not match the prior processing entry's receivedAt ${String(priorProcessing.receivedAtFromFilename)} for messageId ${JSON.stringify(entry.messageIdFromFilename)}`,
        };
      }
    }
  }

  return { ok: true };
}

/**
 * Enforce the principal-vs-origin map for `CancelRequested`: the
 * signing principal kind must match the origin's required signer.
 */
function checkCancelOriginPrincipal(
  blobPath: string,
  origin: string,
  principal: Principal,
): ValidatePushResult {
  const required = CANCEL_ORIGIN_TO_PRINCIPAL_KIND.get(origin);
  if (required === undefined) {
    return {
      ok: false,
      reason: `event ${blobPath} CancelRequested origin ${JSON.stringify(origin)} has no principal-kind binding`,
    };
  }
  if (principal.kind !== required) {
    return {
      ok: false,
      reason: `event ${blobPath} CancelRequested origin ${JSON.stringify(origin)} requires principal.kind=${JSON.stringify(required)} but the push was signed by principal.kind=${JSON.stringify(principal.kind)}`,
    };
  }
  return { ok: true };
}

/**
 * Path-scope a `workflow-process` principal to its own `runs/<runId>/`
 * subtree; `addresses/` is rejected outright (the supervisor owns the
 * claim-check single-writer contract). Only fires for `workflow-process`
 * principals.
 */
async function enforceWorkflowProcessPathScope(
  principal: Principal,
  topLevelTreePaths: readonly string[],
  listDir: (path: string) => Promise<string[]>,
): Promise<ValidatePushResult> {
  if (principal.kind !== "workflow-process") return { ok: true };
  const parsed = WorkflowProcessPrincipal(principal);
  if (parsed instanceof type.errors) {
    // Unreachable against the real authorize callback; fail closed so a
    // permissive substitute (e.g. a test `allowAll`) cannot bypass the
    // path-scope enforcement.
    return {
      ok: false,
      reason: `workflow-process principal is malformed: ${parsed.summary}`,
    };
  }
  if (topLevelTreePaths.includes(WORKFLOW_RUN_ADDRESSES_PREFIX)) {
    return {
      ok: false,
      reason: `workflow-process principal may not write under ${WORKFLOW_RUN_ADDRESSES_PREFIX}/; the supervisor owns the claim-check subtree`,
    };
  }
  if (
    parsed.runId !== undefined &&
    topLevelTreePaths.includes(WORKFLOW_RUN_RUNS_PREFIX)
  ) {
    const runIds = await listDir(WORKFLOW_RUN_RUNS_PREFIX);
    for (const runId of runIds) {
      if (runId !== parsed.runId) {
        return {
          ok: false,
          reason: `workflow-process principal scoped to runId ${JSON.stringify(parsed.runId)} may not write under ${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/`,
        };
      }
    }
  }
  return { ok: true };
}

/**
 * Validate `agent-state/` shape (design §3c): each entry must be a
 * round-tripping `<agentKey>/` directory, not a dangling blob; contents
 * are opaque (the warm agent's ContextStore owns them).
 */
async function validateAgentStateSubtree(
  topLevelTreePaths: readonly string[],
  listDir: (path: string) => Promise<string[]>,
): Promise<ValidatePushResult> {
  if (!topLevelTreePaths.includes(WORKFLOW_RUN_AGENT_STATE_PREFIX)) {
    return { ok: true };
  }
  const segments = await listDir(WORKFLOW_RUN_AGENT_STATE_PREFIX);
  for (const segment of segments) {
    const roundTrip = checkUrlSegmentRoundTrip(segment);
    if (!roundTrip.ok) {
      return {
        ok: false,
        reason: `agent-state ${roundTrip.reason}`,
      };
    }
    // Every entry must be a `<agentKey>/` directory: a directory lists
    // its children, a dangling blob lists as empty.
    const children = await listDir(
      `${WORKFLOW_RUN_AGENT_STATE_PREFIX}/${segment}`,
    );
    if (children.length === 0) {
      return {
        ok: false,
        reason: `agent-state entry ${JSON.stringify(segment)} is a blob directly under ${WORKFLOW_RUN_AGENT_STATE_PREFIX}/; entries must be a <agentKey>/ directory carrying the agent's snapshot files`,
      };
    }
  }
  return { ok: true };
}

/**
 * Enforce retained `<uid>.eml` byte equality; an absent prior blob (a
 * legal expunge) never reaches here. Mirrors the blobs walk.
 */
async function checkMailboxEmlPriorByteEquality(
  emlPath: string,
  readBlob: (path: string) => Promise<Uint8Array>,
  priorReadBlob: (path: string) => Promise<Uint8Array | null>,
): Promise<ValidatePushResult> {
  const prior = await priorReadBlob(emlPath);
  if (prior === null) return { ok: true };
  const prospective = await readBlob(emlPath);
  if (prior.byteLength !== prospective.byteLength) {
    return {
      ok: false,
      reason: `mailbox message ${emlPath} bytes diverge from the prior tree (lengths ${String(prior.byteLength)} vs ${String(prospective.byteLength)}); a retained mailbox message is immutable`,
    };
  }
  for (let i = 0; i < prior.byteLength; i++) {
    if (prior[i] !== prospective[i]) {
      return {
        ok: false,
        reason: `mailbox message ${emlPath} bytes diverge from the prior tree at offset ${String(i)}; a retained mailbox message is immutable`,
      };
    }
  }
  return { ok: true };
}

/**
 * Walk and validate `mailbox/INBOX/`: only `index.json` and `<uid>.eml`
 * files, each a leaf blob (a directory lists its children, a blob lists
 * as empty). Returns the `.eml` paths plus `indexPresent` for the
 * index-continuity guard. An absent subtree contributes nothing.
 */
async function enumerateMailboxInbox(
  listDir: (path: string) => Promise<string[]>,
): Promise<
  | { ok: true; emlPaths: Set<string>; indexPresent: boolean }
  | { ok: false; reason: string }
> {
  const emlPaths = new Set<string>();
  let indexPresent = false;
  const mailboxChildren = await listDir(WORKFLOW_RUN_MAILBOX_PREFIX);
  if (mailboxChildren.length === 0) return { ok: true, emlPaths, indexPresent };
  for (const child of mailboxChildren) {
    if (child !== WORKFLOW_RUN_MAILBOX_INBOX_DIR) {
      return {
        ok: false,
        reason: `mailbox subtree contains unexpected entry ${JSON.stringify(child)} under ${WORKFLOW_RUN_MAILBOX_PREFIX}/; only "${WORKFLOW_RUN_MAILBOX_INBOX_DIR}" is allowed`,
      };
    }
  }
  const inboxPath = `${WORKFLOW_RUN_MAILBOX_PREFIX}/${WORKFLOW_RUN_MAILBOX_INBOX_DIR}`;
  const inboxEntries = await listDir(inboxPath);
  // An empty `INBOX` listing is a dangling blob at `mailbox/INBOX`, not
  // a directory: git never records an empty directory.
  if (inboxEntries.length === 0) {
    return {
      ok: false,
      reason: `mailbox ${inboxPath} is a blob, not a directory carrying "${WORKFLOW_RUN_MAILBOX_INDEX_FILE}" and <uid>.eml message files`,
    };
  }
  for (const entry of inboxEntries) {
    const entryPath = `${inboxPath}/${entry}`;
    if (entry === WORKFLOW_RUN_MAILBOX_INDEX_FILE) {
      if ((await listDir(entryPath)).length > 0) {
        return {
          ok: false,
          reason: `mailbox ${entryPath} is a directory; the mailbox index must be a single file`,
        };
      }
      indexPresent = true;
      continue;
    }
    if (!MAILBOX_EML_FILENAME_RE.test(entry)) {
      return {
        ok: false,
        reason: `mailbox entry ${entryPath} does not match "${WORKFLOW_RUN_MAILBOX_INDEX_FILE}" or <uid>.eml (uid a decimal integer >= 1)`,
      };
    }
    if ((await listDir(entryPath)).length > 0) {
      return {
        ok: false,
        reason: `mailbox message ${entryPath} is a directory; each message must be a single .eml file`,
      };
    }
    emlPaths.add(entryPath);
  }
  return { ok: true, emlPaths, indexPresent };
}

/**
 * Validate `mailbox/INBOX/` (conversational-mailbox design): shape via
 * `enumerateMailboxInbox`, retained `.eml` byte-equality, and index
 * continuity. An absent prior blob is a legal expunge (the bytes stay in
 * git history — workflow-run repos are never GC'd); a dropped
 * `index.json` is rejected because it would reset uidValidity/uidNext on
 * the next open.
 */
async function validateMailboxSubtree(
  listDir: (path: string) => Promise<string[]>,
  readBlob: (path: string) => Promise<Uint8Array>,
  priorListDir: (path: string) => Promise<string[]>,
  priorReadBlob: (path: string) => Promise<Uint8Array | null>,
): Promise<ValidatePushResult> {
  const prospective = await enumerateMailboxInbox(listDir);
  if (!prospective.ok) return prospective;
  const prior = await enumerateMailboxInbox(priorListDir);
  if (!prior.ok) {
    return {
      ok: false,
      reason: `prior tree's mailbox subtree is structurally invalid: ${prior.reason}`,
    };
  }
  for (const emlPath of prospective.emlPaths) {
    if (!prior.emlPaths.has(emlPath)) continue; // newly added
    const immutable = await checkMailboxEmlPriorByteEquality(
      emlPath,
      readBlob,
      priorReadBlob,
    );
    if (!immutable.ok) return immutable;
  }
  if (prior.indexPresent && !prospective.indexPresent) {
    return {
      ok: false,
      reason: `mailbox ${WORKFLOW_RUN_MAILBOX_PREFIX}/${WORKFLOW_RUN_MAILBOX_INBOX_DIR}/${WORKFLOW_RUN_MAILBOX_INDEX_FILE} present in the prior tree is missing from the prospective tree; the mailbox index must persist so uidValidity/uidNext continuity holds across reopens`,
    };
  }
  return { ok: true };
}

export const workflowRunKindHandler: KindHandler = {
  kind: "workflow-run",
  directoryPrefix: "workflow-runs",
  async validatePush({
    repoId,
    ref,
    principal,
    topLevelTreePaths,
    readBlob,
    listDir,
    listDirOids,
    priorReadBlob,
    priorListDir,
    priorListDirOids,
    changedPathPrefixes,
  }): Promise<ValidatePushResult> {
    // Bound the per-run walks to the runs this commit touched: a
    // prefix-preserving commit carries every other run forward
    // byte-identical, so their invariants were validated when last
    // written. Stays `undefined` (validate all) when the substrate
    // could not bound the change set.
    const scopeRunIds = runScopeFromChangedPrefixes(changedPathPrefixes);
    for (const entry of topLevelTreePaths) {
      if (
        entry.startsWith(`${WORKFLOW_RUN_CONTROL_PREFIX}/`) ||
        entry === WORKFLOW_RUN_CONTROL_PREFIX
      ) {
        return {
          ok: false,
          reason: `top-level entry ${JSON.stringify(entry)} is under the unsupported ${WORKFLOW_RUN_CONTROL_PREFIX}/ subtree`,
        };
      }
      if (!ALLOWED_TOP_LEVEL.has(entry)) {
        return {
          ok: false,
          reason: `unexpected top-level entry ${JSON.stringify(entry)}; allowed: "${WORKFLOW_RUN_RUNS_PREFIX}", "${WORKFLOW_RUN_ADDRESSES_PREFIX}", "${WORKFLOW_RUN_AGENT_STATE_PREFIX}", "${WORKFLOW_RUN_MAILBOX_PREFIX}", "${WORKFLOW_RUN_GITIGNORE_PATH}"`,
        };
      }
    }

    const scopingCheck = await enforceWorkflowProcessPathScope(
      principal,
      topLevelTreePaths,
      listDir,
    );
    if (!scopingCheck.ok) {
      logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${scopingCheck.reason}`;
      return scopingCheck;
    }

    const agentStateCheck = await validateAgentStateSubtree(
      topLevelTreePaths,
      listDir,
    );
    if (!agentStateCheck.ok) {
      logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${agentStateCheck.reason}`;
      return agentStateCheck;
    }

    const priorTopLevels = await priorListDir("");
    const addressesPresent =
      topLevelTreePaths.includes(WORKFLOW_RUN_ADDRESSES_PREFIX) ||
      priorTopLevels.includes(WORKFLOW_RUN_ADDRESSES_PREFIX);
    if (addressesPresent) {
      // Enter claim-check validation when either tree carries
      // `addresses/`; a prospective tree that omits it must still run
      // the walk so prior entries' deletion-direction invariants fire.
      const claimCheck = await validateClaimCheckSubtree(
        listDir,
        readBlob,
        priorReadBlob,
        priorListDir,
        priorListDirOids,
        listDirOids,
      );
      if (!claimCheck.ok) {
        logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${claimCheck.reason}`;
        return claimCheck;
      }
    }

    const mailboxPresent =
      topLevelTreePaths.includes(WORKFLOW_RUN_MAILBOX_PREFIX) ||
      priorTopLevels.includes(WORKFLOW_RUN_MAILBOX_PREFIX);
    if (mailboxPresent) {
      // Enter mailbox validation when either tree carries `mailbox/`;
      // dropping the subtree must still trip the index-continuity guard.
      const mailboxCheck = await validateMailboxSubtree(
        listDir,
        readBlob,
        priorListDir,
        priorReadBlob,
      );
      if (!mailboxCheck.ok) {
        logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${mailboxCheck.reason}`;
        return mailboxCheck;
      }
    }

    const runsPresent =
      topLevelTreePaths.includes(WORKFLOW_RUN_RUNS_PREFIX) ||
      priorTopLevels.includes(WORKFLOW_RUN_RUNS_PREFIX);
    if (!runsPresent) {
      // No `runs/` in either tree is the genesis state: `.gitignore`-only
      // or claim-check-only trees are accepted so the asset routes' init
      // can land before any run produced an event.
      return { ok: true };
    }

    const enumerated = await enumerateEventBlobs(listDir, scopeRunIds);
    if (!enumerated.ok) {
      logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${enumerated.reason}`;
      return { ok: false, reason: enumerated.reason };
    }

    const newlyTerminalRuns: NewlyTerminalRun[] = [];
    for (const [runId, entries] of enumerated.runs) {
      if (entries.length === 0) {
        return {
          ok: false,
          reason: `run ${runId} has an empty events directory`,
        };
      }
      // Contiguity: per-run events must run without gaps from the
      // first entry's seq, or a consumer iterating by seq would skip
      // silently. `entries` is sorted above.
      const firstEntry = entries[0];
      if (firstEntry === undefined) throw new Error("unreachable");
      const baseSeq = firstEntry.filenameSeq;
      for (let i = 0; i < entries.length; i++) {
        const e = entries[i];
        if (e === undefined) throw new Error("unreachable");
        const expectedSeq = baseSeq + i;
        if (e.filenameSeq !== expectedSeq) {
          const expectedPath = `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/${WORKFLOW_RUN_EVENTS_DIR}/${String(expectedSeq)}.json`;
          return {
            ok: false,
            reason: `run ${runId} events have a sequence gap: ${expectedPath} is missing (next observed is ${e.blobPath})`,
          };
        }
      }
      let terminalSeq: number | null = null;
      let terminalType: string | null = null;
      for (const entry of entries) {
        const priorCheck = await checkPriorByteEquality(
          entry.blobPath,
          readBlob,
          priorReadBlob,
        );
        if (!priorCheck.ok) {
          logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${priorCheck.reason}`;
          return priorCheck;
        }
        const parsed = await parseEventBlob(entry, readBlob);
        if (!parsed.ok) {
          logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${parsed.reason}`;
          return { ok: false, reason: parsed.reason };
        }
        if (parsed.parsed.body.type === "CancelRequested") {
          const origin = parsed.parsed.body.origin;
          if (typeof origin !== "string") {
            return {
              ok: false,
              reason: `event ${entry.blobPath} CancelRequested origin must be a string`,
            };
          }
          // The origin-vs-signer rule is a write-time check on the commit
          // that authors the event; a later commit carrying the cancel
          // forward (e.g. the run's own cascade write) is signed
          // differently and must not be rejected. Byte equality already
          // proves a carried-forward blob unchanged.
          if (await blobIsNewlyAdded(entry.blobPath, priorReadBlob)) {
            const principalCheck = checkCancelOriginPrincipal(
              entry.blobPath,
              origin,
              principal,
            );
            if (!principalCheck.ok) {
              logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${principalCheck.reason}`;
              return principalCheck;
            }
          }
        }
        if (terminalSeq !== null) {
          return {
            ok: false,
            reason: `run ${runId} has event at seq ${String(entry.filenameSeq)} after terminal ${terminalType} at seq ${String(terminalSeq)}`,
          };
        }
        const classified = classifyTerminalEvent(parsed.parsed.body.type);
        if (classified.terminal) {
          terminalSeq = entry.filenameSeq;
          terminalType = parsed.parsed.body.type;
          // Emit the signal only when this commit adds the terminal
          // blob; a commit carrying an already-terminal run forward
          // (e.g. compaction) must not double-fire.
          if (await blobIsNewlyAdded(entry.blobPath, priorReadBlob)) {
            const terminalBytes = await readBlob(entry.blobPath);
            newlyTerminalRuns.push({
              runId,
              status: classified.status,
              terminalEventJson: new TextDecoder().decode(terminalBytes),
            });
          }
        }
      }
    }

    const combinedRuns = await validateCombinedEventRuns(
      listDir,
      readBlob,
      priorListDir,
      priorReadBlob,
      scopeRunIds,
    );
    if (!combinedRuns.ok) {
      logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${combinedRuns.reason}`;
      return { ok: false, reason: combinedRuns.reason };
    }

    const blobsEnumerated = await enumerateRunBlobs(listDir, scopeRunIds);
    if (!blobsEnumerated.ok) {
      logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${blobsEnumerated.reason}`;
      return { ok: false, reason: blobsEnumerated.reason };
    }
    for (const blob of blobsEnumerated.blobs) {
      const immutability = await checkBlobPriorByteEquality(
        blob.blobPath,
        readBlob,
        priorReadBlob,
      );
      if (!immutability.ok) {
        logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${immutability.reason}`;
        return immutability;
      }
    }

    // Deletion direction for the runs subtree: the prospective walks
    // only see present paths, so enumerate the prior tree under the
    // same shapes and reject any prior path that does not reappear.
    const priorEnumerated = await enumerateEventBlobs(
      priorListDir,
      scopeRunIds,
    );
    if (!priorEnumerated.ok) {
      return {
        ok: false,
        reason: `prior tree's runs subtree is structurally invalid: ${priorEnumerated.reason}`,
      };
    }
    const prospectiveEventPaths = new Set<string>();
    for (const entries of enumerated.runs.values()) {
      for (const e of entries) prospectiveEventPaths.add(e.blobPath);
    }
    for (const entries of priorEnumerated.runs.values()) {
      for (const e of entries) {
        if (prospectiveEventPaths.has(e.blobPath)) continue;
        // A run sealed by this commit legitimately drops its per-event
        // files; the fold was validated byte-for-byte above.
        if (combinedRuns.combinedRunIds.has(e.runId)) continue;
        return {
          ok: false,
          reason: `event ${e.blobPath} present in the prior tree is missing from the prospective tree; event blobs are append-only`,
        };
      }
    }
    const priorBlobsEnumerated = await enumerateRunBlobs(
      priorListDir,
      scopeRunIds,
    );
    if (!priorBlobsEnumerated.ok) {
      return {
        ok: false,
        reason: `prior tree's blobs subtree is structurally invalid: ${priorBlobsEnumerated.reason}`,
      };
    }
    const prospectiveBlobPaths = new Set<string>(
      blobsEnumerated.blobs.map((b) => b.blobPath),
    );
    for (const b of priorBlobsEnumerated.blobs) {
      if (prospectiveBlobPaths.has(b.blobPath)) continue;
      return {
        ok: false,
        reason: `blob ${b.blobPath} present in the prior tree is missing from the prospective tree; blob entries are immutable once written`,
      };
    }

    const partsCheck = await validateRunPartsSubtree({
      listDir,
      priorListDir,
      readBlob,
      priorReadBlob,
      listDirOids,
      priorListDirOids,
      scopeRunIds,
    });
    if (!partsCheck.ok) {
      logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${partsCheck.reason}`;
      return partsCheck;
    }

    return { ok: true, newlyTerminalRuns };
  },
  onRefUpdated() {
    // No cached index today. Consumers read events through the
    // substrate's subscribe / blob-read API.
  },
};

export const workflowRunAuthorize: AuthorizeFn = (
  principal: Principal,
  repoId,
  ref,
  action,
) => {
  if (repoId.kind !== "workflow-run") {
    return {
      allowed: false,
      reason: `workflow-run authorize received non-workflow-run repo ${repoId.kind}/${repoId.id}`,
    };
  }

  if (principal.kind === "hub") {
    return { allowed: true };
  }

  if (principal.kind === "workflow-process") {
    const parsed = WorkflowProcessPrincipal(principal);
    if (parsed instanceof type.errors) {
      return {
        allowed: false,
        reason: `workflow-process principal is malformed: ${parsed.summary}`,
      };
    }
    if (parsed.anchorRunId !== repoId.id) {
      return {
        allowed: false,
        reason: `workflow-process deployment ${parsed.anchorRunId} cannot access workflow-run ${repoId.id}`,
      };
    }
    switch (action) {
      case "init":
      case "writeTree":
      case "receivePack":
      case "createPack":
      case "resolveRef":
        return { allowed: true };
      default: {
        const _exhaustive: never = action;
        return {
          allowed: false,
          reason: `unhandled action: ${String(_exhaustive)}`,
        };
      }
    }
  }

  if (principal.kind === "supervisor") {
    const parsed = SupervisorPrincipal(principal);
    if (parsed instanceof type.errors) {
      return {
        allowed: false,
        reason: `supervisor principal is malformed: ${parsed.summary}`,
      };
    }
    if (parsed.anchorRunId !== repoId.id) {
      return {
        allowed: false,
        reason: `supervisor deployment ${parsed.anchorRunId} cannot access workflow-run ${repoId.id}`,
      };
    }
    switch (action) {
      case "init":
      case "writeTree":
      case "receivePack":
      case "createPack":
      case "resolveRef":
        return { allowed: true };
      default: {
        const _exhaustive: never = action;
        return {
          allowed: false,
          reason: `unhandled action: ${String(_exhaustive)}`,
        };
      }
    }
  }

  if (principal.kind === "sidecar") {
    const parsed = SidecarPrincipal(principal);
    if (parsed instanceof type.errors) {
      return {
        allowed: false,
        reason: `sidecar principal is malformed: ${parsed.summary}`,
      };
    }
    switch (action) {
      case "createPack":
      case "resolveRef":
        return { allowed: true };
      case "init":
      case "writeTree":
      case "receivePack":
        return {
          allowed: false,
          reason: `sidecars may only read workflow-run repos, not ${action}`,
        };
      default: {
        const _exhaustive: never = action;
        return {
          allowed: false,
          reason: `unhandled action: ${String(_exhaustive)}`,
        };
      }
    }
  }

  if (principal.kind === "user") {
    return authorizeUserPrincipal({
      principal,
      repoId,
      ref,
      action,
      resourcePrefix: "workflow-run",
    });
  }

  // Fail closed on any kind not handled above. The tenant-level
  // `workflow` principal kind is a grant owner, never a workflow-run
  // bearer, so it is intentionally left denied.
  return {
    allowed: false,
    reason: `unknown principal kind: ${principal.kind}`,
  };
};

// ---------------------------------------------------------------------
// Claim-check API.
//
// Four operations over `RepoStore.writeTreeDelta` give the workflow
// runtime a FIFO claim-check queue per address: `enqueueInbox`,
// `dequeueToProcessing`, `markConsumed`, and `replayProcessingToInbox`
// (recovery). Each is scoped to its per-address subtree via
// `changedPathPrefixes`; the substrate runs each `computeDelta` under
// the per-repo lock against a `prior` view and applies the returned
// targeted delta in one atomic commit, carrying untouched entries
// forward by OID.

function claimCheckCommitRef(): string {
  // All claim-check operations commit to one canonical ref so
  // subscribers see a single commit stream.
  return "refs/heads/events";
}

function addressSegmentFor(address: string): string {
  // Mirror the encoder `validatePush` requires (it rejects
  // non-round-trip segments).
  return encodeURIComponent(address);
}

function addressPrefix(addressSegment: string): string {
  return `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/`;
}

function inboxPath(addressSegment: string, key: string): string {
  return `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/${WORKFLOW_RUN_INBOX_DIR}/${key}.json`;
}

function processingPath(addressSegment: string, key: string): string {
  return `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/${WORKFLOW_RUN_PROCESSING_DIR}/${key}.json`;
}

function consumedPath(addressSegment: string, messageId: string): string {
  return `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/${WORKFLOW_RUN_CONSUMED_DIR}/${messageId}.json`;
}

function watermarkPath(addressSegment: string): string {
  return `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/${WORKFLOW_RUN_WATERMARK_FILE}`;
}

function filenameKey(receivedAt: number, messageId: string): string {
  return `${String(receivedAt)}-${messageId}`;
}

type ClaimCheckEntry = { name: string; oid: string };

type AddressListing = {
  inbox: ClaimCheckEntry[];
  processing: ClaimCheckEntry[];
  consumed: ClaimCheckEntry[];
  watermark: number;
};

/**
 * Read one address's listing from the parent commit: filenames and OIDs
 * under `{inbox,processing,consumed}/` plus the watermark, names and
 * OIDs only — the entry a leg moves is read separately by OID. An empty
 * listing covers the address-absent first-write states.
 */
async function readAddressListing(
  prior: PriorDeltaReads,
  addressSegment: string,
): Promise<AddressListing> {
  const listing: AddressListing = {
    inbox: [],
    processing: [],
    consumed: [],
    watermark: 0,
  };
  const addrDir = `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}`;
  for (const child of await prior.listDirOids(addrDir)) {
    if (child.name === WORKFLOW_RUN_WATERMARK_FILE) {
      const blob = await prior.readBlobByOid(child.oid);
      listing.watermark = parseWatermark(blob, watermarkPath(addressSegment));
      continue;
    }
    const bucket =
      child.name === WORKFLOW_RUN_INBOX_DIR
        ? listing.inbox
        : child.name === WORKFLOW_RUN_PROCESSING_DIR
          ? listing.processing
          : child.name === WORKFLOW_RUN_CONSUMED_DIR
            ? listing.consumed
            : null;
    if (bucket === null) continue;
    for (const entry of await prior.listDirOids(`${addrDir}/${child.name}`)) {
      bucket.push({ name: entry.name, oid: entry.oid });
    }
  }
  return listing;
}

function utf8(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decodeQueueEnvelopeOrThrow(
  bytes: Uint8Array,
  blobPath: string,
): ClaimCheckEnvelope {
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(bytes));
  } catch (cause) {
    throw new Error(`claim_check_corrupt_json: ${blobPath}`, { cause });
  }
  const validated = ClaimCheckEnvelope(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `claim_check_envelope_invalid: ${blobPath}: ${validated.summary}`,
    );
  }
  return validated;
}

function decodeConsumedReceivedAtOrThrow(
  bytes: Uint8Array,
  blobPath: string,
): number {
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(bytes));
  } catch (cause) {
    throw new Error(`claim_check_corrupt_json: ${blobPath}`, { cause });
  }
  const validated = ConsumedEnvelope(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `claim_check_consumed_invalid: ${blobPath}: ${validated.summary}`,
    );
  }
  return validated.receivedAt;
}

/** Decode a watermark blob; callers treat an absent blob as 0. */
function parseWatermark(bytes: Uint8Array, watermarkFull: string): number {
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(bytes));
  } catch (cause) {
    throw new Error(`claim_check_corrupt_json: ${watermarkFull}`, { cause });
  }
  const validated = WatermarkEnvelope(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `claim_check_watermark_invalid: ${watermarkFull}: ${validated.summary}`,
    );
  }
  return validated.watermark;
}

export type EnqueueInboxArgs = {
  address: string;
  messageId: string;
  receivedAt: number;
  mailAuditRef: { store: string; path: string };
  /**
   * Base64 of the inbound mail's raw MIME bytes. Inlined on the
   * claim-check envelope so the workflow-process child can recover its
   * step input by messageId at `trigger.fired` time (§3a -- the
   * supervisor is the sole mail owner and has no separate durable byte
   * store the child reads). Omit to stamp only the audit ref.
   */
  rawMessage?: string;
};

export type EnqueueInboxResult = {
  commitSha: string;
  inboxKey: string;
  envelope: ClaimCheckEnvelope;
};

/**
 * Which state an `enqueueInbox` call found the messageId in. Every value
 * is positive evidence the bytes are durably on disk, so a caller gating
 * a receipt on the enqueue may safely acknowledge on any of them.
 */
export type EnqueueAlreadyPresentReason =
  | "duplicate"
  | "already_inbox"
  | "processing"
  | "consumed";

/**
 * `enqueueInbox` outcome. A value (not an exception) because the
 * return/throw boundary is the ack/withhold boundary: a returned outcome
 * is safe to ack, a throw is not. Only `enqueued` added a new entry, so
 * only it wakes a dispatch-driving loop.
 */
export type EnqueueInboxOutcome =
  | ({ outcome: "enqueued" } & EnqueueInboxResult)
  | { outcome: "already-present"; reason: EnqueueAlreadyPresentReason };

/**
 * Internal signal from the `enqueueInbox` merge callback when the
 * messageId is already present; caught at the boundary into an
 * `already-present` outcome. Carries the reason so the boundary need not
 * re-derive it.
 */
class InboxEntryAlreadyPresent extends Error {
  constructor(
    readonly reason: EnqueueAlreadyPresentReason,
    message: string,
  ) {
    super(message);
    this.name = "InboxEntryAlreadyPresent";
  }
}

/**
 * Thrown by `enqueueInbox` when the inbound's `receivedAt` is strictly
 * below the retention watermark. Refusal under uncertainty, not proof of
 * prior receipt: the dedup entry may have been pruned, so a duplicate
 * cannot be ruled out. A caller gating a durable-receipt ack MUST NOT
 * acknowledge on this. Its own type so it surfaces as a distinct signal.
 *
 * Structurally unreachable on the mail-inbound path today: enqueues
 * always carry a freshly stamped `receivedAt`, a full horizon above the
 * watermark, and the only path that carries an old `receivedAt` back
 * (`replayProcessingToInbox`) bypasses this gate. If a redelivery source
 * ever carries the original `receivedAt` here, this becomes reachable and
 * the withhold-not-ack handling becomes load-bearing.
 */
export class StaleInboxEnqueueError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StaleInboxEnqueueError";
  }
}

/**
 * Append a new inbox entry for `address`. Rejects when a same-messageId
 * entry already exists in any queue state — including a prior inbox
 * entry at a different `receivedAt` — catching the concurrent-enqueue
 * race the per-repo lock alone cannot surface.
 */
export async function enqueueInbox(
  store: RepoStore,
  principal: Principal,
  repoId: RepoId,
  args: EnqueueInboxArgs,
): Promise<EnqueueInboxOutcome> {
  const addressSegment = addressSegmentFor(args.address);
  const ref = claimCheckCommitRef();
  const inboxKey = filenameKey(args.receivedAt, args.messageId);
  const envelope: ClaimCheckEnvelope = {
    messageId: args.messageId,
    receivedAt: args.receivedAt,
    address: args.address,
    mailAuditRef: args.mailAuditRef,
    ...(args.rawMessage !== undefined ? { rawMessage: args.rawMessage } : {}),
  };
  const newInboxPath = inboxPath(addressSegment, inboxKey);
  const inboxFname = `${inboxKey}.json`;
  const consumedFname = `${args.messageId}.json`;
  const messageIdSuffix = `-${args.messageId}.json`;
  // Already-present throws are caught into an outcome; stale-refusal and
  // I/O failures propagate. The split is the ack/withhold boundary:
  // return = safe to ack, throw = withhold.
  let commitSha: string;
  try {
    ({ commitSha } = await store.writeTreeDelta(principal, repoId, ref, {
      changedPathPrefixes: new Set([addressPrefix(addressSegment)]),
      message: `enqueue inbox ${args.address} ${args.messageId}`,
      computeDelta: async (_parentCommitSha, prior) => {
        const listing = await readAddressListing(prior, addressSegment);
        // Refuse enqueue below the watermark: the dedup entry may have
        // been pruned, so a duplicate cannot be ruled out. Above the
        // watermark the consumed/ index is authoritative; below it, refuse.
        if (args.receivedAt < listing.watermark) {
          throw new StaleInboxEnqueueError(
            `claim_check_stale_enqueue: address ${args.address} message ${args.messageId} receivedAt ${String(args.receivedAt)} is below the retention watermark ${String(listing.watermark)}; its dedup entry may have been pruned, so it is refused as definitively-stale`,
          );
        }
        if (listing.inbox.some((e) => e.name === inboxFname)) {
          throw new InboxEntryAlreadyPresent(
            "duplicate",
            `claim_check_duplicate_inbox: ${newInboxPath} already exists`,
          );
        }
        // consumed/ is keyed by messageId alone, so this is an exact lookup.
        if (listing.consumed.some((e) => e.name === consumedFname)) {
          throw new InboxEntryAlreadyPresent(
            "consumed",
            `claim_check_already_consumed: address ${args.address} message ${args.messageId} is already in the consumed dedup index`,
          );
        }
        if (listing.processing.some((e) => e.name.endsWith(messageIdSuffix))) {
          throw new InboxEntryAlreadyPresent(
            "processing",
            `claim_check_already_processing: address ${args.address} message ${args.messageId} is currently in processing`,
          );
        }
        // Reject a second inbox entry for the same messageId at a
        // different receivedAt; surfacing it here keeps the bad tree off
        // the substrate.
        const inboxDup = listing.inbox.find((e) =>
          e.name.endsWith(messageIdSuffix),
        );
        if (inboxDup !== undefined) {
          throw new InboxEntryAlreadyPresent(
            "already_inbox",
            `claim_check_already_inbox: address ${args.address} message ${args.messageId} is already in the inbox at ${inboxPath(addressSegment, inboxDup.name.slice(0, -".json".length))}`,
          );
        }
        return {
          puts: { [newInboxPath]: utf8(JSON.stringify(envelope)) },
          deletes: [],
        };
      },
    }));
  } catch (err) {
    if (err instanceof InboxEntryAlreadyPresent) {
      return { outcome: "already-present", reason: err.reason };
    }
    throw err;
  }
  return { outcome: "enqueued", commitSha, inboxKey, envelope };
}

export type DequeueToProcessingResult = {
  commitSha: string;
  key: string;
  envelope: ClaimCheckEnvelope;
} | null;

/**
 * Move the FIFO-first inbox entry for `address` to processing; `null`
 * when the inbox is empty. FIFO is the numeric `receivedAt` prefix with
 * a messageId tiebreak — raw string sorting would put `"100-…"` ahead of
 * `"99-…"`.
 */
export async function dequeueToProcessing(
  store: RepoStore,
  principal: Principal,
  repoId: RepoId,
  address: string,
): Promise<DequeueToProcessingResult> {
  const addressSegment = addressSegmentFor(address);
  const ref = claimCheckCommitRef();
  let dequeued: { key: string; envelope: ClaimCheckEnvelope } | null = null;
  const { commitSha } = await store.writeTreeDelta(principal, repoId, ref, {
    changedPathPrefixes: new Set([addressPrefix(addressSegment)]),
    message: `dequeue ${address}`,
    computeDelta: async (_parentCommitSha, prior) => {
      const listing = await readAddressListing(prior, addressSegment);
      const inboxDir = `${addressPrefix(addressSegment)}${WORKFLOW_RUN_INBOX_DIR}/`;
      // Sort by numeric receivedAt, messageId tiebreak; a raw string
      // sort misorders non-uniform digit widths.
      type InboxCandidate = {
        entry: ClaimCheckEntry;
        receivedAt: number;
        messageId: string;
      };
      const candidates: InboxCandidate[] = [];
      for (const entry of listing.inbox) {
        const m = QUEUE_FILENAME_RE.exec(entry.name);
        if (m === null || m[1] === undefined || m[2] === undefined) {
          throw new Error(
            `claim_check_invalid_inbox_filename: ${inboxDir}${entry.name}`,
          );
        }
        candidates.push({
          entry,
          receivedAt: Number.parseInt(m[1], 10),
          messageId: m[2],
        });
      }
      candidates.sort((a, b) => {
        if (a.receivedAt !== b.receivedAt) return a.receivedAt - b.receivedAt;
        if (a.messageId < b.messageId) return -1;
        if (a.messageId > b.messageId) return 1;
        return 0;
      });
      const first = candidates[0];
      if (first === undefined) {
        // Empty inbox: no-op commit; the caller reads `dequeued === null`.
        dequeued = null;
        return { puts: {}, deletes: [] };
      }
      const firstPath = `${inboxDir}${first.entry.name}`;
      const key = first.entry.name.slice(0, -".json".length);
      const bytes = await prior.readBlobByOid(first.entry.oid);
      const envelope = decodeQueueEnvelopeOrThrow(bytes, firstPath);
      dequeued = { key, envelope };
      return {
        puts: { [processingPath(addressSegment, key)]: bytes },
        deletes: [firstPath],
      };
    },
  });
  if (dequeued === null) return null;
  const captured: { key: string; envelope: ClaimCheckEnvelope } = dequeued;
  return { commitSha, key: captured.key, envelope: captured.envelope };
}

export type ReadProcessingEntryResult = {
  envelope: ClaimCheckEnvelope;
} | null;

/**
 * Read the processing entry for `messageId` at `address` without
 * mutating the tree; `null` when none exists. The read half of mailbox
 * ownership (§3a): the supervisor forwards `trigger.fired{messageId}` to
 * the child, which recovers the inbound bytes here. A flat working-tree
 * read of `addresses/<seg>/processing/` (the substrate materializes
 * claim-check commits there); issues no commit, so it cannot race
 * `markConsumed`.
 */
export async function readProcessingEntry(
  store: RepoStore,
  _principal: Principal,
  repoId: RepoId,
  address: string,
  messageId: string,
): Promise<ReadProcessingEntryResult> {
  const addressSegment = addressSegmentFor(address);
  const repoDir = store.getRepoDir(repoId);
  const processingDir = `${repoDir}/${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/${WORKFLOW_RUN_PROCESSING_DIR}`;
  const suffix = `-${messageId}.json`;
  let filenames: string[];
  try {
    filenames = await fs.promises.readdir(processingDir);
  } catch (cause) {
    // A missing processing directory is the legitimate "no entry yet"
    // state; any other failure surfaces.
    if (
      cause instanceof Error &&
      (cause as NodeJS.ErrnoException).code === "ENOENT"
    ) {
      return null;
    }
    throw cause;
  }
  for (const filename of filenames) {
    if (!filename.endsWith(suffix)) continue;
    const blobPath = `${processingDir}/${filename}`;
    const bytes = await fs.promises.readFile(blobPath);
    const envelope = decodeQueueEnvelopeOrThrow(
      new Uint8Array(bytes),
      blobPath,
    );
    return { envelope };
  }
  return null;
}

export type MarkConsumedArgs = {
  address: string;
  messageId: string;
  runId: string;
  consumedAt: number;
  /**
   * Present when the supervisor deliberately refused the message instead of
   * delivering it to the run. The consumed entry remains the durable dedup
   * record, while Hub projection uses this detail to fail (rather than settle)
   * an allocation-dispatch row.
   */
  rejection?: {
    code: string;
    message: string;
  };
  /**
   * Retention horizon for the consumed dedup index, in milliseconds.
   * The commit advances the per-address watermark to
   * `consumedAt - retentionHorizonMs` (never backward, never past the
   * entry being written) and prunes consumed entries below it. The
   * boot edge resolves the operator's `CONSUMED_RETENTION_MS` config
   * to a concrete value and threads it here. Omit to apply
   * `DEFAULT_CONSUMED_RETENTION_MS` (24h).
   */
  retentionHorizonMs?: number;
};

export type MarkConsumedResult = {
  commitSha: string;
  envelope: ConsumedEnvelope;
  /** Watermark the commit advanced to (epoch-ms `receivedAt` horizon). */
  watermark: number;
  /** messageIds whose consumed entries this commit pruned. */
  prunedMessageIds: string[];
};

/**
 * Atomically remove the processing entry for `messageId`, write the
 * `consumed/<messageId>.json` dedup entry (preserving the processing
 * envelope's `receivedAt`/`mailAuditRef` as audit), advance the
 * watermark to `max(prior, min(consumedAt - horizon, thisEntry.receivedAt))`
 * — monotonic, never past the new entry — and prune consumed entries
 * strictly below it, keeping `consumed/` bounded to ~one horizon's
 * volume. Throws if no matching processing entry exists.
 */
export async function markConsumed(
  store: RepoStore,
  principal: Principal,
  repoId: RepoId,
  args: MarkConsumedArgs,
): Promise<MarkConsumedResult> {
  const addressSegment = addressSegmentFor(args.address);
  const ref = claimCheckCommitRef();
  const retentionHorizonMs =
    args.retentionHorizonMs ?? DEFAULT_CONSUMED_RETENTION_MS;
  let consumedEnvelope: ConsumedEnvelope | null = null;
  let advancedWatermark = 0;
  const prunedMessageIds: string[] = [];
  const { commitSha } = await store.writeTreeDelta(principal, repoId, ref, {
    changedPathPrefixes: new Set([addressPrefix(addressSegment)]),
    message: `consume ${args.address} ${args.messageId}`,
    computeDelta: async (_parentCommitSha, prior) => {
      const listing = await readAddressListing(prior, addressSegment);
      const consumedFull = consumedPath(addressSegment, args.messageId);
      const consumedFname = `${args.messageId}.json`;
      if (listing.consumed.some((e) => e.name === consumedFname)) {
        throw new Error(
          `claim_check_already_consumed: ${consumedFull} already in the dedup index`,
        );
      }
      const processingDir = `${addressPrefix(addressSegment)}${WORKFLOW_RUN_PROCESSING_DIR}/`;
      const processingEntry = listing.processing.find((e) =>
        e.name.endsWith(`-${args.messageId}.json`),
      );
      if (processingEntry === undefined) {
        throw new Error(
          `claim_check_processing_not_found: address ${args.address} message ${args.messageId} has no processing entry`,
        );
      }
      const processingFull = `${processingDir}${processingEntry.name}`;
      const processingBytes = await prior.readBlobByOid(processingEntry.oid);
      const processingEnvelope = decodeQueueEnvelopeOrThrow(
        processingBytes,
        processingFull,
      );
      const envelope: ConsumedEnvelope = {
        messageId: args.messageId,
        receivedAt: processingEnvelope.receivedAt,
        address: args.address,
        runId: args.runId,
        consumedAt: args.consumedAt,
        mailAuditRef: processingEnvelope.mailAuditRef,
        ...(args.rejection !== undefined ? { rejection: args.rejection } : {}),
      };
      consumedEnvelope = envelope;

      // The watermark only advances, never past the entry this commit
      // writes (a late-consumed message may sit below the horizon and is
      // pruned once the watermark passes its receivedAt).
      const horizonBoundary = args.consumedAt - retentionHorizonMs;
      const newWatermark = Math.max(
        listing.watermark,
        Math.min(horizonBoundary, envelope.receivedAt),
      );
      advancedWatermark = newWatermark;

      // Prune the consumed tail: drop any retained entry strictly below
      // the new watermark. The one leg that must scan the index, since
      // filenames carry only the messageId and the receivedAt lives in
      // the bytes.
      const consumedDir = `${addressPrefix(addressSegment)}${WORKFLOW_RUN_CONSUMED_DIR}/`;
      const deletes: string[] = [processingFull];
      for (const entry of listing.consumed) {
        const blobPath = `${consumedDir}${entry.name}`;
        const bytes = await prior.readBlobByOid(entry.oid);
        const consumedReceivedAt = decodeConsumedReceivedAtOrThrow(
          bytes,
          blobPath,
        );
        if (consumedReceivedAt < newWatermark) {
          prunedMessageIds.push(entry.name.slice(0, -".json".length));
          deletes.push(blobPath);
        }
      }
      return {
        puts: {
          [consumedFull]: utf8(JSON.stringify(envelope)),
          [watermarkPath(addressSegment)]: utf8(
            JSON.stringify({ watermark: newWatermark }),
          ),
        },
        deletes,
      };
    },
  });
  if (consumedEnvelope === null) throw new Error("unreachable");
  const captured: ConsumedEnvelope = consumedEnvelope;
  return {
    commitSha,
    envelope: captured,
    watermark: advancedWatermark,
    prunedMessageIds,
  };
}

export type ReplayProcessingToInboxResult = {
  commitSha: string;
  replayedKeys: string[];
};

export type ScanRunsForBootResult = {
  ownedMessageIds: Set<string>;
  pendingSealRunIds: string[];
};

/**
 * One working-tree walk of `runs/` returning the spawn-time recovery
 * inputs:
 *
 * - `ownedMessageIds`: `consumedMessageId`s of non-terminal runs, fed to
 *   `replayProcessingToInbox` so a live run's message is not re-admitted
 *   and dispatched a second time while its durable log is re-driven —
 *   two runtime bodies racing on one runId corrupt the terminal.
 * - `pendingSealRunIds`: terminal runs still in per-event form (an
 *   interrupted fold); the recovery sweep re-runs the idempotent fold.
 *
 * The run logs live on a different ref (`refs/heads/main`) than the
 * claim-check subtree (`refs/heads/events`), so this lives at the caller.
 * A sealed run contributes to neither set; an absent `runs/` yields empty
 * results.
 */
export async function scanRunsForBoot(
  store: RepoStore,
  repoId: RepoId,
): Promise<ScanRunsForBootResult> {
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const repoDir = store.getRepoDir(repoId);
  const runsDir = path.join(repoDir, WORKFLOW_RUN_RUNS_PREFIX);
  let runIds: string[];
  try {
    runIds = await fs.readdir(runsDir);
  } catch (cause) {
    if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") {
      return { ownedMessageIds: new Set(), pendingSealRunIds: [] };
    }
    throw cause;
  }
  const owned = new Set<string>();
  const pendingSealRunIds: string[] = [];
  for (const runId of runIds) {
    const runDir = path.join(runsDir, runId);
    // A sealed run is terminal by the handler's invariant — only a
    // terminated run is sealed — so it owns nothing and is already folded.
    let sealed = false;
    try {
      await fs.access(path.join(runDir, WORKFLOW_RUN_EVENTS_FILE));
      sealed = true;
    } catch (cause) {
      // ENOENT is the normal "not sealed" case; any other error falls
      // through to the events-dir read below, so warn and continue.
      if (
        !(cause instanceof Error) ||
        !("code" in cause) ||
        cause.code !== "ENOENT"
      ) {
        logger.warn`scanRunsForBoot: stat of the sealed-log file for run ${runId} failed: ${cause instanceof Error ? cause.message : String(cause)}`;
      }
      sealed = false;
    }
    if (sealed) continue;
    const eventsDir = path.join(runDir, WORKFLOW_RUN_EVENTS_DIR);
    let files: string[];
    try {
      files = await fs.readdir(eventsDir);
    } catch (cause) {
      // ENOENT means no events yet (grants may be staged first); skip.
      // A non-ENOENT error drops the run from both sets — a live run
      // dropped from ownedMessageIds gets re-admitted and double-dispatched
      // on the same runId. Surface it, but still skip.
      if (
        !(cause instanceof Error) ||
        !("code" in cause) ||
        cause.code !== "ENOENT"
      ) {
        logger.error`scanRunsForBoot: reading events for run ${runId} failed; skipping it may re-admit its message and start a second run on the same runId: ${cause instanceof Error ? cause.message : String(cause)}`;
      }
      continue;
    }
    let terminal = false;
    let consumedMessageId: string | undefined;
    for (const file of files) {
      if (!file.endsWith(".json")) continue;
      let parsed: unknown;
      try {
        parsed = JSON.parse(
          await fs.readFile(path.join(eventsDir, file), "utf8"),
        );
      } catch (cause) {
        // A corrupt event drops this run's classification: a missed
        // RunStarted re-admits its message (second run on the same runId),
        // a missed terminal skips a needed seal. Surface it, but skip the
        // file. ENOENT is a benign vanish-mid-scan race.
        if (
          !(cause instanceof Error) ||
          !("code" in cause) ||
          cause.code !== "ENOENT"
        ) {
          logger.error`scanRunsForBoot: reading event ${file} for run ${runId} failed; skipping it may re-admit its message and start a second run on the same runId: ${cause instanceof Error ? cause.message : String(cause)}`;
        }
        continue;
      }
      if (
        typeof parsed !== "object" ||
        parsed === null ||
        !("type" in parsed)
      ) {
        continue;
      }
      const type = parsed.type;
      if (typeof type !== "string") continue;
      if (TERMINAL_EVENT_TYPES.has(type)) {
        terminal = true;
        break;
      }
      if (type === "RunStarted" && "consumedMessageId" in parsed) {
        const mid = parsed.consumedMessageId;
        if (typeof mid === "string") consumedMessageId = mid;
      }
    }
    if (terminal) {
      pendingSealRunIds.push(runId);
      continue;
    }
    if (consumedMessageId !== undefined) owned.add(consumedMessageId);
  }
  return { ownedMessageIds: owned, pendingSealRunIds };
}

export type WorkflowRunLifecycle = "absent" | "live" | "terminal";

/**
 * Classify a run's lifecycle from a read surface shared by the committed
 * and working-tree readers: sealed log = terminal; otherwise the latest
 * event decides, and no events = absent. The surface owns all read
 * details, so this core never sees a raw read error.
 */
async function classifyRunLifecycle<
  E extends { readonly seq: number },
>(surface: {
  sealedLogPresent(): Promise<boolean>;
  listEventEntries(): Promise<readonly E[]>;
  readEvent(entry: E): Promise<unknown>;
}): Promise<WorkflowRunLifecycle> {
  if (await surface.sealedLogPresent()) return "terminal";
  const entries = await surface.listEventEntries();
  const latest = entries.reduce<E | undefined>(
    (candidate, entry) =>
      candidate === undefined || entry.seq > candidate.seq ? entry : candidate,
    undefined,
  );
  if (latest !== undefined) {
    const parsed = await surface.readEvent(latest);
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      "type" in parsed &&
      typeof parsed.type === "string" &&
      TERMINAL_EVENT_TYPES.has(parsed.type)
    ) {
      return "terminal";
    }
  }
  return entries.length === 0 ? "absent" : "live";
}

/** Read one run's lifecycle from a committed workflow-run tree. */
export async function readCommittedWorkflowRunLifecycle(
  reads: CommittedReads | null,
  runId: string,
): Promise<WorkflowRunLifecycle> {
  if (reads === null) return "absent";
  const runPath = `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}`;
  const eventsPath = `${runPath}/${WORKFLOW_RUN_EVENTS_DIR}`;
  return classifyRunLifecycle<{ seq: number; name: string; oid: string }>({
    async sealedLogPresent() {
      const runChildren = await reads.listDir(runPath);
      return runChildren.some(
        (entry) =>
          entry.type === "blob" && entry.name === WORKFLOW_RUN_EVENTS_FILE,
      );
    },
    async listEventEntries() {
      const eventEntries = await reads.listDir(eventsPath);
      return eventEntries.flatMap((entry) => {
        if (entry.type !== "blob") return [];
        const seq = parseEventSeq(entry.name);
        return seq === null ? [] : [{ seq, name: entry.name, oid: entry.oid }];
      });
    },
    async readEvent(entry) {
      const eventPath = `${eventsPath}/${entry.name}`;
      try {
        return JSON.parse(
          new TextDecoder().decode(await reads.readBlobByOid(entry.oid)),
        );
      } catch (cause) {
        throw new Error(`workflow_run_event_unreadable: ${eventPath}`, {
          cause,
        });
      }
    },
  });
}

/**
 * Read one run's lifecycle from the working tree. `grants.json` alone is
 * still absent — grants are staged before the first event, the durable
 * proof of firing; a sealed log is terminal. The supervisor consults this
 * when in-memory cohort membership is empty (a new deployment, or briefly
 * during recovery), so the distinction decides fire-vs-not.
 */
export async function readWorkflowRunLifecycle(
  store: RepoStore,
  repoId: RepoId,
  runId: string,
): Promise<WorkflowRunLifecycle> {
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const runDir = path.join(
    store.getRepoDir(repoId),
    WORKFLOW_RUN_RUNS_PREFIX,
    runId,
  );
  const eventsDir = path.join(runDir, WORKFLOW_RUN_EVENTS_DIR);
  return classifyRunLifecycle<{ seq: number; name: string }>({
    async sealedLogPresent() {
      try {
        await fs.access(path.join(runDir, WORKFLOW_RUN_EVENTS_FILE));
        return true;
      } catch (cause) {
        if (
          !(cause instanceof Error) ||
          !("code" in cause) ||
          cause.code !== "ENOENT"
        ) {
          throw cause;
        }
        return false;
      }
    },
    async listEventEntries() {
      let files: string[];
      try {
        files = await fs.readdir(eventsDir);
      } catch (cause) {
        if (
          cause instanceof Error &&
          "code" in cause &&
          cause.code === "ENOENT"
        ) {
          return [];
        }
        throw cause;
      }
      return files.flatMap((file) => {
        const seq = parseEventSeq(file);
        return seq === null ? [] : [{ seq, name: file }];
      });
    },
    async readEvent(entry) {
      const eventPath = path.join(eventsDir, entry.name);
      try {
        return JSON.parse(await fs.readFile(eventPath, "utf8"));
      } catch (cause) {
        throw new Error(`workflow_run_event_unreadable: ${eventPath}`, {
          cause,
        });
      }
    },
  });
}

export type ReplayProcessingToInboxOpts = {
  /**
   * MessageIds whose run is still live and owns the message: re-admitting
   * it would dispatch a second run on the same runId, so these entries
   * stay in `processing/` until the run's `markConsumed`. The caller
   * computes the set from the run logs on `refs/heads/main`, which this
   * operation's ref (`refs/heads/events`) cannot see. Absent = replay
   * every processing entry.
   */
  ownedMessageIds?: ReadonlySet<string>;
};

/**
 * Recovery path: move every processing entry at `address` back to
 * inbox under its original `<receivedAt>-<messageId>` key so FIFO
 * ordering survives a crash. Atomic across all entries — a partial
 * replay would corrupt the FIFO discipline. Returns the moved keys;
 * empty when nothing was in processing (a no-op commit).
 *
 * Watermark carve-out (load-bearing — do NOT "tighten" this): the
 * replay skips the `receivedAt < watermark` stale-reject that
 * `enqueueInbox` applies. A processing entry is already past dedup, so
 * re-admitting a below-watermark one is correct; applying the reject
 * here would silently lose a legitimately in-flight message. The
 * watermark only gates fresh inbound at the enqueue boundary.
 */
export async function replayProcessingToInbox(
  store: RepoStore,
  principal: Principal,
  repoId: RepoId,
  address: string,
  opts: ReplayProcessingToInboxOpts = {},
): Promise<ReplayProcessingToInboxResult> {
  const addressSegment = addressSegmentFor(address);
  const ref = claimCheckCommitRef();
  const ownedMessageIds = opts.ownedMessageIds ?? new Set<string>();
  const replayedKeys: string[] = [];
  const { commitSha } = await store.writeTreeDelta(principal, repoId, ref, {
    changedPathPrefixes: new Set([addressPrefix(addressSegment)]),
    message: `replay processing ${address}`,
    computeDelta: async (_parentCommitSha, prior) => {
      const listing = await readAddressListing(prior, addressSegment);
      const processingDir = `${addressPrefix(addressSegment)}${WORKFLOW_RUN_PROCESSING_DIR}/`;
      const inboxDir = `${addressPrefix(addressSegment)}${WORKFLOW_RUN_INBOX_DIR}/`;
      const inboxNames = new Set(listing.inbox.map((e) => e.name));
      const puts: Record<string, string | Uint8Array> = {};
      const deletes: string[] = [];
      for (const entry of listing.processing) {
        const bytes = await prior.readBlobByOid(entry.oid);
        // An entry owned by a live run stays in processing (its re-drive
        // already owns the message; re-admitting would dispatch a second
        // run on the same runId). Only genuinely orphaned entries are
        // replayed; the caller precomputed the owned set.
        const envelope = decodeQueueEnvelopeOrThrow(
          bytes,
          `${processingDir}${entry.name}`,
        );
        if (ownedMessageIds.has(envelope.messageId)) {
          continue;
        }
        const inboxFull = `${inboxDir}${entry.name}`;
        if (inboxNames.has(entry.name)) {
          throw new Error(
            `claim_check_replay_collision: ${inboxFull} already exists; cannot replay processing entry`,
          );
        }
        // Re-admit without the enqueue stale-reject: the entry is already
        // past dedup, so a below-watermark receivedAt is no reason to
        // refuse it. Do not tighten this.
        puts[inboxFull] = bytes;
        deletes.push(`${processingDir}${entry.name}`);
        replayedKeys.push(entry.name.slice(0, -".json".length));
      }
      return { puts, deletes };
    },
  });
  return { commitSha, replayedKeys };
}
