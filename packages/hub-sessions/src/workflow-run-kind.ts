// KindHandler for the `workflow-run` repo kind.
//
// A workflow-run repo holds per-deployment runtime state for one or
// more in-flight workflow runs. `RepoId.id` is the owning deployment
// id. The repo's top-level layout is:
//
//   - `runs/<runId>/events/<seq>.json` — per-run event log entries.
//     Each entry is a JSON object whose body carries a `type`
//     discriminator (the on-disk event vocabulary used by the
//     workflow-run repo) and a `seq` field that matches the integer
//     in the filename. Filenames are decimal integers ranging from
//     `0` upward; the on-disk seq numbering owns the ordering and
//     the per-blob `seq` field is the redundant cross-check.
//   - `runs/<runId>/blobs/<sha256-hex>` — content-addressed step
//     outputs the production `BlobSubstrate` adapter spills here when
//     a value's JSON-stringified form exceeds the inline-encoding
//     threshold. The filename is a lowercase 64-character sha256 hex
//     string; the blob value is opaque bytes. Blobs are append-only
//     and immutable: any blob present in the prior tree must carry
//     byte-identical contents in the prospective tree.
//   - `addresses/<urlEncoded(address)>/inbox/<receivedAt>-<messageId>.json`
//     — pending inbound mail for the address, FIFO-ordered by the
//     filename's parsed numeric `receivedAt` prefix (with a
//     lexicographic messageId tiebreak). The filename keeps the
//     decimal `<receivedAt>` form unpadded; the substrate sorts by
//     parsed integer rather than string so the FIFO invariant holds
//     for non-uniform digit widths (e.g. `99-…` precedes `100-…`).
//   - `addresses/<urlEncoded(address)>/processing/<receivedAt>-<messageId>.json`
//     — messages currently being handled. Same filename shape and
//     JSON envelope as the inbox entry; a `dequeueToProcessing`
//     commit atomically removes the inbox entry and adds the
//     processing entry preserving the filename key.
//   - `addresses/<urlEncoded(address)>/consumed/<messageId>.json` —
//     dedup index keyed by messageId. A `markConsumed` commit
//     atomically removes the matching processing entry and writes
//     this dedup entry. The dedup index is bounded by a per-address
//     retention watermark (see `watermark.json`): a `markConsumed`
//     commit prunes consumed entries whose `receivedAt` falls below
//     the watermark so the index reaches a bounded steady state
//     instead of growing one entry per message forever.
//   - `addresses/<urlEncoded(address)>/watermark.json` — the
//     per-address retention watermark. Carries a single
//     `receivedAt`-horizon value: the oldest `receivedAt` a consumed
//     entry may still retain. The watermark only ever advances
//     (monotonic non-decreasing). `enqueueInbox` rejects any inbound
//     whose `receivedAt` is strictly below the watermark as
//     definitively-stale (its dedup entry may have been pruned, so a
//     duplicate cannot be ruled out -- refuse loudly rather than risk
//     reprocessing). Above the watermark the `consumed/` index is
//     authoritative; below it, refuse. The watermark advances only as
//     the prune advances, both under the single writer, so the two
//     never diverge. The stale-reject applies ONLY to fresh inbound at
//     `enqueueInbox`; `replayProcessingToInbox` is intentionally exempt
//     (a recovered in-flight `processing/` entry is already past dedup
//     -- see that function's note).
//   - `.gitignore` — supplied by the asset routes' genesis init body.
//
// The control-plane subtree (`control/...`) is not part of this
// commit's surface and has no v1 use case.
//
// Event-log invariants enforced at push:
//   - Each event body's `seq` matches the integer in its filename.
//   - Per-run event filenames are unique decimal integers (guaranteed
//     by the tree shape) and validatePush verifies the body's `seq`
//     field carries the same number, so the on-disk seq sequence and
//     the per-blob seq cannot diverge.
//   - Terminal-phase lock: once a run's events include a `RunCompleted`,
//     `RunFailed`, or `RunCancelled` entry, no event with a strictly
//     greater seq may appear for the same run.
//   - Append-only via prior-tree byte comparison: every event blob
//     that exists at the same path in the parent commit's tree must
//     match the prospective blob byte-for-byte. Newly-added event
//     paths (those absent from the prior tree) are accepted. The
//     substrate exposes the prior tree via `priorReadBlob` /
//     `priorListDir` on the validatePush args so the constraint is
//     owned by this handler rather than relying on caller-layer
//     discipline.
//   - A `CancelRequested` event must carry an `origin` in the known
//     set (`self`, `supervisor-drain`, `supervisor-operator`,
//     `hub-admin`) and a non-empty `reason`.
//   - Principal-vs-origin enforcement for `CancelRequested`: a
//     `hub-admin` origin requires the signing principal to be `hub`;
//     the other three
//     origins (`self`, `supervisor-drain`, `supervisor-operator`)
//     require the signing principal to be `supervisor` — the
//     supervisor signs on the child's behalf for `self`, and signs
//     for itself on the drain / operator cases. A principal that does
//     not match the declared origin produces a rejection naming both
//     sides so a misconfigured writer surfaces at the boundary
//     rather than as a downstream mystery.
//
// Claim-check subtree invariants enforced at push:
//   - The `<urlEncoded>` segment under `addresses/` must round-trip
//     cleanly through `decodeURIComponent` followed by
//     `encodeURIComponent`. A segment that does not round-trip is
//     rejected so consumers can rely on a single canonical encoding.
//   - The only entries permitted under an `addresses/<urlEncoded>/`
//     subtree are the directories `inbox`, `processing`, and
//     `consumed`, plus the single `watermark.json` file. Other names
//     under an address fail the push.
//   - Inbox and processing filenames must match
//     `<receivedAt>-<messageId>.json` where `receivedAt` is a decimal
//     epoch-ms integer. The body's `receivedAt` matches the filename
//     `receivedAt` and the body's `messageId` matches the filename
//     `messageId`. The body's `address` field must decode to the
//     URL-encoded segment.
//   - Consumed filenames must match `<messageId>.json`. The body's
//     `messageId` matches the filename `messageId`. The body carries
//     a `consumedBy` run id and the `receivedAt` of the original
//     consume for audit.
//   - Atomicity: a given `<messageId>` appears in at most one
//     filename across `inbox`, `processing`, and `consumed` combined,
//     per address per prospective commit. Two inbox entries with the
//     same `<messageId>` but different `<receivedAt>` are rejected as
//     a same-state collision; the cross-state check fires when the
//     same messageId appears in inbox+processing, inbox+consumed, or
//     processing+consumed.
//   - `consumed/<messageId>.json` bytes are immutable: a prospective
//     commit that mutates the bytes of a consumed entry RETAINED from
//     the prior tree is rejected by the same prior-tree byte-equality
//     guard used for run events. A retained consumed entry may be
//     DELETED only as a watermark-consistent retention prune (see the
//     watermark invariants below); any other deletion is rejected.
//   - Retention prune (the bounded-`consumed/` contract): the consumed
//     dedup index may shrink only by a watermark-passed prune. A
//     consumed entry present in the prior tree may be absent from the
//     prospective tree only when (a) its `receivedAt` is strictly
//     below the prospective `watermark.json` value (you may prune only
//     what the watermark passed) and (b) the watermark did not regress
//     (`prospective watermark >= prior watermark`). A RETAINED entry is
//     NOT required to sit at or above the watermark: a message consumed
//     long after receipt (or one replayed back in-flight after a crash)
//     may legitimately carry a below-watermark `receivedAt` and survive
//     until a later commit prunes it. Retaining it gives only EXTRA
//     dedup -- a re-submission at or above the watermark still hits the
//     entry, one below it is stale-rejected at enqueue -- so it never
//     weakens exactly-once.
//   - Inbox→processing transition: a processing entry that is newly
//     added (not present in the prior tree) must be backed by a
//     matching inbox entry in the prior tree at the same
//     `<receivedAt>-<messageId>.json` key. If the prior tree does
//     not show that inbox entry the transition is rejected so a
//     direct write into `processing/` cannot bypass the inbox.
//   - Processing→consumed transition: a consumed entry that is
//     newly added (not present in the prior tree) must be backed by
//     a processing entry in the prior tree at the same address with
//     the same messageId. The receivedAt and messageId carried in
//     the prior processing envelope must equal the values carried in
//     the new consumed envelope so the audit trail is unambiguous.
//
// Authz:
//   - `hub` principal: full access.
//   - `workflow-process` principal: read/write its own deployment's
//     event log. The principal carries `{ deploymentId, runId? }`;
//     this handler verifies `repoId.id === deploymentId`.
//   - `supervisor` principal: read/write its own deployment's event
//     log. The principal carries `{ deploymentId }`; this handler
//     verifies `repoId.id === deploymentId`.
//   - `sidecar` principal: read-only (createPack, resolveRef) for
//     resume.
//   - `user` principal: gated by bearer-token claims and the route
//     layer's pre-resolved authz verdict, mirroring the convention
//     used by the other kinds.

import fs from "node:fs";
import git from "isomorphic-git";
import { type } from "arktype";
import { getLogger } from "@intx/log";
import { glob, repoActionToGrantVerb } from "@intx/hub-common";
import {
  UserPrincipal,
  type AuthorizeFn,
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

export type WorkflowRunHubPrincipal = { readonly kind: "hub" };

export type WorkflowRunSidecarPrincipal = {
  readonly kind: "sidecar";
  readonly agentId: string;
};

export type WorkflowRunWorkflowProcessPrincipal = {
  readonly kind: "workflow-process";
  readonly deploymentId: string;
  readonly runId?: string;
};

export type WorkflowRunSupervisorPrincipal = {
  readonly kind: "supervisor";
  readonly deploymentId: string;
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
export const WORKFLOW_RUN_ADDRESSES_PREFIX = "addresses";
export const WORKFLOW_RUN_CONTROL_PREFIX = "control";
export const WORKFLOW_RUN_INBOX_DIR = "inbox";
export const WORKFLOW_RUN_PROCESSING_DIR = "processing";
export const WORKFLOW_RUN_CONSUMED_DIR = "consumed";

/**
 * Filename of the per-address retention watermark blob, a direct child
 * of `addresses/<urlEncoded>/` (a file, not a directory). Carries the
 * monotonic `receivedAt`-horizon below which consumed entries may be
 * pruned and at-or-below which inbound enqueues are refused as stale.
 */
export const WORKFLOW_RUN_WATERMARK_FILE = "watermark.json";

/**
 * Default retention horizon for the consumed dedup index, in
 * milliseconds. The boot edge resolves the operator's
 * `CONSUMED_RETENTION_MS` config to a concrete value and threads it
 * into `markConsumed`; this default applies only when no operator
 * value is supplied. 24 hours is the conservative default: long enough
 * that a duplicate from a retrying upstream within a day is still
 * deduped by a retained consumed entry, short enough that `consumed/`
 * reaches a bounded steady state of one day's message volume.
 *
 * INVARIANT (operator-owned): the horizon must be >= the longest
 * window in which the same `messageId` could legitimately be
 * re-submitted and still must be caught as a duplicate. There is no
 * automatic internal mail redelivery in the system today, so this is
 * the external re-submission window. If an at-least-once redelivery
 * source is ever added, the horizon must be >= its maximum redelivery
 * window or dedup breaks; a breach surfaces LOUDLY (a too-late
 * re-submission carrying an old `receivedAt` is refused at enqueue,
 * not silently reprocessed) rather than as silent double-processing.
 */
export const DEFAULT_CONSUMED_RETENTION_MS = 24 * 60 * 60 * 1000;

/**
 * Per-agent durable conversation-state subtree (design §3c). A
 * long-lived single-step agent's multi-turn conversation context is
 * committed under `agent-state/<agentKey>/...` so it survives child
 * respawn: on respawn the rebuilt warm agent reads its prior
 * conversation back from here before the resumed run replays.
 *
 * Unlike `runs/` (append-only events, immutable blobs) this subtree is
 * MUTABLE: each run boundary overwrites the agent's conversation
 * snapshot with the latest turns. It is therefore exempt from the
 * append-only / deletion-direction walks `runs/` is subject to; the
 * only push-time constraint is segment shape (a single round-trip-safe
 * `<agentKey>` directory layer below the prefix).
 */
export const WORKFLOW_RUN_AGENT_STATE_PREFIX = "agent-state";

/**
 * Allowed top-level entries in the prospective tree. Anything else
 * fails the push. `control/` has no v1 use and stays absent.
 */
const ALLOWED_TOP_LEVEL = new Set<string>([
  WORKFLOW_RUN_RUNS_PREFIX,
  WORKFLOW_RUN_ADDRESSES_PREFIX,
  WORKFLOW_RUN_AGENT_STATE_PREFIX,
  WORKFLOW_RUN_GITIGNORE_PATH,
]);

const CLAIM_CHECK_SUBDIRS = new Set<string>([
  WORKFLOW_RUN_INBOX_DIR,
  WORKFLOW_RUN_PROCESSING_DIR,
  WORKFLOW_RUN_CONSUMED_DIR,
]);

/** Per-event filename shape: a decimal integer followed by `.json`. */
const EVENT_FILENAME_RE = /^(0|[1-9][0-9]*)\.json$/;

/**
 * Per-blob filename shape for the `runs/<runId>/blobs/` subtree: a
 * lowercase 64-character sha256 hex string. Pins the regex to the key
 * the production `BlobSubstrate` adapter computes via `sha256Hex` so a
 * non-canonical key (uppercase hex, truncated digest, alternate
 * encoding) fails the push at the boundary rather than landing
 * silently.
 */
const BLOB_FILENAME_RE = /^[0-9a-f]{64}$/;

/**
 * Subdirectories the kind handler accepts under `runs/<runId>/`. The
 * `events/` subtree carries the append-only event log; the `blobs/`
 * subtree carries opaque, content-addressed step outputs the
 * `BlobSubstrate` adapter spills there when a value exceeds the
 * inline-encoding threshold.
 */
const RUN_DIR_ALLOWED_CHILDREN = new Set<string>([
  WORKFLOW_RUN_EVENTS_DIR,
  WORKFLOW_RUN_BLOBS_DIR,
  // A terminated run's event log, sealed from the per-event `events/`
  // files into one combined file by a compaction commit.
  WORKFLOW_RUN_EVENTS_FILE,
]);

/**
 * Filename shape for inbox and processing entries:
 * `<receivedAt>-<messageId>.json`. `receivedAt` is a decimal integer
 * (epoch ms); `messageId` is captured as the rest of the basename and
 * is validated separately against the body's `messageId`.
 */
const QUEUE_FILENAME_RE = /^(0|[1-9][0-9]*)-(.+)\.json$/;

/** Filename shape for consumed entries: `<messageId>.json`. */
const CONSUMED_FILENAME_RE = /^(.+)\.json$/;

/**
 * JSON envelope carried by inbox and processing entries. Keys:
 *   - `messageId`: dedup key for the inbound message.
 *   - `receivedAt`: epoch-ms timestamp the reactor accepted the
 *     message; sortable FIFO key prefix.
 *   - `address`: decoded canonical address (not URL-encoded).
 *   - `mailAuditRef`: pointer to the raw mail bytes in the mail-audit
 *     store. For the in-process single-agent path a separate
 *     `MailAuditStore` holds the authoritative bytes and this ref joins
 *     onto it.
 *   - `rawMessage`: base64 of the inbound mail's raw MIME bytes,
 *     inlined so the workflow-process child can read its step input by
 *     messageId at `trigger.fired` time. The supervisor is the sole
 *     mail owner under the unified-execution host (§3a); it has no
 *     separate durable byte store the child can read, so the bytes ride
 *     the claim-check envelope itself. Present whenever the supervisor
 *     enqueued the entry; omitted by callers that only stamp the audit
 *     ref. The bytes survive the inbox→processing transition verbatim
 *     (the dequeue copies the entry bytes), so a `trigger.fired` for a
 *     processing entry can always recover the input.
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
 * JSON envelope carried by consumed entries. The consumed entry is the
 * canonical dedup index keyed by messageId; the envelope preserves
 * the originating receivedAt for audit and carries the runId that
 * consumed the message.
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
  "+": "ignore",
});

/**
 * JSON envelope carried by the per-address `watermark.json` blob. The
 * `watermark` is a `receivedAt` horizon (epoch ms): the oldest
 * `receivedAt` a consumed entry may still retain. It only ever
 * advances. A retention prune drops consumed entries strictly below
 * it; `enqueueInbox` refuses any inbound strictly below it.
 */
const WatermarkEnvelope = type({
  watermark: "number >= 0",
  "+": "ignore",
});

export type ClaimCheckEnvelope = typeof ClaimCheckEnvelope.infer;
export type ConsumedEnvelope = typeof ConsumedEnvelope.infer;
export type WatermarkEnvelope = typeof WatermarkEnvelope.infer;

/**
 * Terminal event discriminators. A run whose log contains an entry
 * with one of these `type` values must not receive any event with a
 * strictly greater seq.
 */
const TERMINAL_EVENT_TYPES = new Set<string>([
  "RunCompleted",
  "RunFailed",
  "RunCancelled",
]);

/**
 * Recognised CancelRequested origins. Mirrors the workflow package's
 * `CANCEL_ORIGINS` vocabulary; inlined here so the substrate does
 * not depend on `@intx/workflow`.
 */
const CANCEL_REQUESTED_ORIGINS = new Set<string>([
  "self",
  "supervisor-drain",
  "supervisor-operator",
  "hub-admin",
]);

/**
 * Per-origin signing-principal kind. `hub-admin` is the only origin
 * a `hub` principal may mint; the other three originate inside the
 * supervisor's trust boundary (the supervisor signs `self` on behalf
 * of the workflow-process since the child has no asymmetric keypair,
 * and signs the `supervisor-drain` / `supervisor-operator` audit-
 * distinction
 * cases for itself). Lookup misses fail the push.
 */
const CANCEL_ORIGIN_TO_PRINCIPAL_KIND: ReadonlyMap<string, string> = new Map([
  ["self", "supervisor"],
  ["supervisor-drain", "supervisor"],
  ["supervisor-operator", "supervisor"],
  ["hub-admin", "hub"],
]);

/**
 * Cross-event shape carried by every blob committed under
 * `runs/<runId>/events/`. The discriminator field on disk is `type`,
 * matching the convention used by the substrate's `subscribeKind`
 * helper and the workflow-host scheduler.
 */
const EventEnvelope = type({
  type: "string",
  seq: "number >= 0",
  "+": "ignore",
});

/**
 * Structural validator for the `CancelRequested` payload's
 * cancellation-specific fields. The kind handler verifies the origin
 * is a known CancelOrigin and the reason is a non-empty string; the
 * principal-vs-origin map collapses because every origin is
 * supervisor-signed in this design.
 */
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
  deploymentId: "string",
  "runId?": "string",
});

const SupervisorPrincipal = type({
  kind: "'supervisor'",
  deploymentId: "string",
});

type RunEventBlob = {
  runId: string;
  filename: string;
  filenameSeq: number;
  blobPath: string;
};

/**
 * Resolve the substrate's `changedPathPrefixes` into the set of run ids
 * the commit could have touched, or `undefined` to validate every run.
 *
 * Returns `undefined` (validate-all) when the substrate could not bound
 * the change set, or when a change prefix reaches into `runs/` without
 * naming a specific run (`runs` or `runs/` alone). A change prefix that
 * never touches `runs/` -- e.g. a claim-check write under `addresses/`
 * -- contributes no run ids; an empty result set means the commit
 * touched no run, so the per-run walks legitimately validate nothing.
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
 * Build the (runId → events[]) map by walking the prospective tree.
 * The substrate's listDir yields names directly under the given
 * directory, so the walk is `runs/` → run-id subdirs → `events/` →
 * event filenames. Filenames outside the `<seq>.json` shape fail the
 * push. When `scopeRunIds` is supplied, only those runs are walked --
 * see the substrate's `changedPathPrefixes` contract.
 */
async function enumerateEventBlobs(
  listDir: (path: string) => Promise<string[]>,
  scopeRunIds?: ReadonlySet<string>,
): Promise<
  | { ok: true; runs: Map<string, RunEventBlob[]> }
  | { ok: false; reason: string }
> {
  const runs = new Map<string, RunEventBlob[]>();
  // When the substrate bounds the commit's change set to a specific set
  // of runs, walk only those `runs/<runId>/` directories instead of
  // listing every run. An untouched run is carried forward
  // byte-identical by the substrate's prefix-preserving commit, so its
  // per-run invariants -- already validated when it was written --
  // cannot change. `scopeRunIds` may name a run absent from the tree
  // (e.g. a prior-tree walk for a run the prospective tree dropped);
  // `listDir` on a missing directory returns `[]`, which the
  // empty-children guards below handle.
  const runIds =
    scopeRunIds === undefined
      ? await listDir(WORKFLOW_RUN_RUNS_PREFIX)
      : Array.from(scopeRunIds);
  for (const runId of runIds) {
    const runDirPath = `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}`;
    const runChildren = await listDir(runDirPath);
    // A scoped run id can name a run that is absent from the tree being
    // walked -- the substrate's change set is the union of prospective
    // and prior touched runs, so the prospective walk may receive a run
    // that exists only in the prior tree (and vice versa). An absent run
    // directory lists as empty; skip it here so only runs actually
    // present in this tree are validated. The unscoped walk never
    // reaches this branch because its run ids come from listing the
    // present `runs/` directory.
    if (scopeRunIds !== undefined && runChildren.length === 0) continue;
    const offender = runChildren.find((c) => !RUN_DIR_ALLOWED_CHILDREN.has(c));
    if (offender !== undefined) {
      return {
        ok: false,
        reason: `run directory ${runDirPath} contains unexpected entry ${JSON.stringify(offender)}; only "${WORKFLOW_RUN_EVENTS_DIR}", "${WORKFLOW_RUN_BLOBS_DIR}", and "${WORKFLOW_RUN_EVENTS_FILE}" are allowed`,
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
 * Validate the prospective tree's combined-form (sealed) runs and return
 * the set of run ids that legitimately carry a combined `events.jsonl`.
 * The deletion-direction guard uses that set to allow a run's per-event
 * files to disappear when (and only when) they were folded into the
 * combined file under this same validation.
 *
 * Three prior states are accepted:
 *   - prior already combined  -> the sealed file is immutable; prospective
 *     bytes must equal prior bytes.
 *   - prior per-event         -> the compaction transition; the combined
 *     file must be the byte-for-byte fold of the prior per-event blobs in
 *     seq order. This is the audit-integrity boundary: a loose check here
 *     would let compaction silently rewrite history.
 *   - prior absent            -> a freshly-delivered sealed run (e.g. a
 *     pack receive); its own structure is validated.
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
 * The audit-integrity bridge. A compaction commit replaces a run's prior
 * `events/<seq>.json` files with one combined file; this asserts the
 * combined file reproduces those prior blobs' bytes verbatim, in seq
 * order, with nothing added, dropped, reordered, or mutated. It rebuilds
 * the expected combined bytes from the prior tree through the same encoder
 * the writer uses, so the two cannot drift, and compares for exact
 * equality.
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
 * Validate a combined event log's own structure: every line a valid event
 * envelope, contiguous seqs, exactly one terminal event and it is last
 * (so a sealed run is genuinely terminal). Used for a sealed run with no
 * prior per-event form to bridge against.
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
 * Walk every `runs/<runId>/blobs/` directory and validate each blob
 * filename matches the sha256-hex shape the production `BlobSubstrate`
 * adapter writes. The `blobs/` subdirectory itself is optional: a run
 * that has not yet spilled an output to a blob never produces a
 * `blobs/` directory, and a run with only inline-encoded outputs never
 * will. Returns the flat list of blob entries so the caller can apply
 * immutability checks against the prior tree.
 */
async function enumerateRunBlobs(
  listDir: (path: string) => Promise<string[]>,
  scopeRunIds?: ReadonlySet<string>,
): Promise<
  { ok: true; blobs: RunBlobEntry[] } | { ok: false; reason: string }
> {
  const out: RunBlobEntry[] = [];
  // See enumerateEventBlobs: a defined `scopeRunIds` walks only the
  // commit's touched runs; an untouched run's blobs are carried forward
  // byte-identical and were validated when written.
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

/**
 * Enforce blob immutability via prior-tree byte equality. The blob
 * value itself is opaque bytes (no JSON envelope, no arktype
 * validation); the only structural rule beyond filename shape is that
 * a blob entry present in the prior tree must carry byte-identical
 * contents in the prospective tree. Mirrors the consumed-entry
 * discipline in the claim-check subtree.
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
 * Compare the prospective bytes of `blobPath` against the bytes at
 * the same path in the prior tree. Returns `{ ok: true }` when the
 * blob is newly added (no prior entry) or when the prior and
 * prospective bytes are byte-identical; returns a rejection otherwise.
 * Surfaces append-only at the handler scope: the event log invariant
 * lives here rather than relying on caller-layer discipline at
 * `writeTreePreservingPrefix`.
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
 * Round-trip an `<urlEncoded(address)>` segment through decode then
 * encode. A divergence means the segment is not the canonical
 * encoding of any address, which would leave consumers guessing
 * which encoding to use when reading the subtree. Surface as a
 * concrete rejection at push time.
 */
function checkAddressSegmentRoundTrip(segment: string):
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
      reason: `address segment ${JSON.stringify(segment)} is not a valid URL-encoded string: ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    };
  }
  const reencoded = encodeURIComponent(decoded);
  if (reencoded !== segment) {
    return {
      ok: false,
      reason: `address segment ${JSON.stringify(segment)} does not round-trip URL-encoding (re-encoded as ${JSON.stringify(reencoded)})`,
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
 * FIFO comparator for inbox/processing entries. Sorts by the parsed
 * numeric `receivedAt` (filename prefix); ties break on the
 * messageId tail. The numeric compare is the load-bearing piece —
 * lexicographic compare on `<receivedAt>-…` filenames with
 * non-uniform digit widths disagrees with chronological order
 * (e.g. `"100-…"` < `"99-…"` because `'1' < '9'`).
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
    const roundTrip = checkAddressSegmentRoundTrip(segment);
    if (!roundTrip.ok) return roundTrip;
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
 * Read + validate the per-address `watermark.json` value from a blob
 * reader. `null` means the tree has no watermark blob (treated as
 * watermark 0 -- no entry pruned, nothing refused). The reader may be
 * the prospective `readBlob` or the `priorReadBlob` (the latter
 * returns `null` for an absent path, which is the legitimate
 * never-pruned genesis state).
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
 * Read + validate a consumed entry's envelope from a blob reader that
 * may return `null` for an absent path (the `priorReadBlob` shape).
 * `null` is treated as a read failure: the caller only passes a path
 * the prior tree is known to carry, so a `null` is structural damage.
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

/**
 * Compute the git blob OID of a consumed entry from a byte reader,
 * used only when the delta-scoped path lacks a substrate-provided prior
 * OID listing (e.g. a hand-built test validatePush). `git.hashBlob`
 * reproduces the same content-addressed OID a `git.readTree` listing
 * carries, so the delta path's intersection compare is identical
 * whether the OID came from the tree listing or from hashing the bytes.
 */
async function hashConsumedBlobOid(bytes: Uint8Array): Promise<string> {
  const { oid } = await git.hashBlob({ object: bytes });
  return oid;
}

/**
 * Resolve each consumed entry's git blob OID for the delta-scoped path.
 * When the substrate supplies a directory OID listing (`listDirOids`) the
 * OID comes straight from the tree — one `readTree` per consumed
 * directory, cached — so that side is not re-read blob-by-blob. When the
 * listing is absent (a hand-built validatePush in a unit test) each OID
 * falls back to hashing the entry's bytes, which preserves identical
 * semantics at O(retained) cost. Both the prior and prospective sides use
 * this; `sideLabel` distinguishes them in the missing-OID error.
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
 * Validate the `addresses/<urlEncoded>/{inbox,processing,consumed}`
 * subtree as a whole. The walk enforces filename shape, JSON envelope
 * structure, address round-trip, per-messageId atomicity across the
 * three queue states, consumed-blob immutability, and the
 * inbox→processing / processing→consumed transition invariants against
 * the prior tree.
 *
 * The consumed dedup index is validated by its per-commit DELTA against
 * the prior tree rather than by re-walking the whole retained set:
 * retained entries (same filename, same blob OID) are skipped as
 * already-validated-and-immutable, added entries are parsed and
 * validated, and removed entries are checked against the retention
 * watermark. `priorListDirOids` and `listDirOids`, when supplied by the
 * substrate, surface the prior and prospective consumed OIDs straight
 * from their tree listings so neither side is re-read blob-by-blob.
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
  // straight from the tree listing on both sides when the substrate
  // provides it, falling back to hashing the bytes otherwise.
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
  // Iterate the UNION of prospective and prior address segments so a
  // prospective tree that wipes an address subtree entirely still
  // runs the prior-retention checks against that segment's
  // prior-tree consumed/processing entries.
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
    // Per-messageId atomicity: each messageId may appear at most
    // once across inbox/processing/consumed combined. The check
    // keys on (messageId, kind, filename) so two inbox entries with
    // the same messageId at different `receivedAt` values surface
    // as a same-state collision (the Set-of-kinds shape would
    // collapse both into a single "inbox" member and miss the
    // case).
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
      // Cross-state atomicity needs each consumed messageId in the map;
      // the messageId is the filename stem, so this needs no blob read.
      // Retained consumed entries are not re-parsed (their envelope was
      // validated when first written and their bytes are proven
      // immutable by the OID compare below); added consumed entries are
      // parsed and validated by the transition check further down.
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

    // Consumed entries are immutable. Compare the git blob OID the
    // enumeration surfaced: a consumed entry present in the prior tree
    // at the same path must carry the same OID (git trees are
    // content-addressed, so equal OID proves byte-equality without
    // reading either blob). A diverging OID is an immutability
    // violation. Immutability is load-bearing for exactly-once: a
    // mutated `receivedAt` on a retained consumed entry could fake it
    // below the watermark, get it pruned, and let a re-submission miss
    // dedup -- so this compare is not optional.
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

    // Deletion-direction guards: walk every entry the prior tree
    // carried under `consumed/`, `processing/`, and `inbox/` and reject
    // any prior path that vanishes from the prospective tree except via
    // a permitted transition (or, for consumed, a watermark-passed
    // retention prune). Without this walk a prospective tree that
    // simply omits a prior entry would slip past the prospective-tree
    // by-presence checks above.
    //
    // Retention-watermark contract for the consumed dedup index. The
    // watermark is a monotonic `receivedAt` horizon; a `markConsumed`
    // commit may drop the oldest consumed tail (entries strictly below
    // the watermark) and the watermark may only advance. Resolve both
    // the prospective and prior watermark up front so the consumed
    // deletion check below can bind every drop to the watermark.
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
      // The consumed dedup index may shrink only by a watermark-passed
      // prune: a consumed entry dropped from the prior tree must have a
      // receivedAt strictly below the prospective watermark (you may
      // prune only what the watermark passed). Combined with the
      // already-verified watermark monotonicity, this is the whole of
      // the exactly-once retention contract: pruning is bound to the
      // watermark and the watermark only advances.
      //
      // The suffix relation (dropped entries older than every retained
      // entry) is deliberately NOT enforced. A RETAINED entry is NOT
      // required to sit at or above the watermark: a message consumed
      // long after receipt (or replayed back in-flight) may
      // legitimately carry a below-watermark receivedAt and survive
      // until a later commit prunes it. Holding it gives EXTRA dedup (a
      // re-submission at or above the watermark still hits the retained
      // entry; one below is stale-rejected at enqueue), so a hole left
      // by an out-of-order prune weakens nothing.
      //
      // Only the dropped entries are read. A retained entry (present in
      // both trees) is proven byte-identical by the OID compare above,
      // so its receivedAt is unchanged and need not be read. The
      // receivedAt lives in the body; read it from the prior tree
      // (retained bytes are immutable, so prior and prospective agree).
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
        // A processing entry may legitimately disappear in two
        // shapes: (1) markConsumed wrote a matching consumed entry
        // keyed by the same messageId, or (2) replayProcessingToInbox
        // moved the entry back to inbox preserving the
        // `<receivedAt>-<messageId>.json` filename. Anything else is
        // an in-flight loss.
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
        // A prior inbox entry may legitimately disappear when it
        // transitions to processing (same `<receivedAt>-<messageId>`
        // filename) or directly to consumed (matching messageId).
        // Anything else is an inbound-mail loss — the FIFO claim-check
        // contract requires the entry to reappear somewhere.
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

    // Newly-added processing entries must match an inbox entry that
    // existed in the prior tree at the same `<receivedAt>-<messageId>`
    // filename. This makes inbox→processing the only legal way to
    // grow processing/.
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

    // Newly-added consumed entries must match a processing entry that
    // existed in the prior tree at the same address+messageId, and
    // the receivedAt carried in the consumed envelope must equal the
    // receivedAt the processing entry's filename carried.
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
 * Enforce the Q3 principal-vs-origin map for a parsed
 * `CancelRequested` event. The principal kind is matched against the
 * origin's required-signer kind; a mismatch rejects with both the
 * declared origin and the actual principal kind in the message so a
 * misconfigured writer surfaces concretely at the push boundary.
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
 * Path-scoping for the `workflow-process` principal. A workflow-process
 * proxies writes for the workflow-run repo's `runs/<runId>/` subtree
 * only; the supervisor owns the `addresses/...` claim-check subtree.
 * If the principal carries a `runId`, every prospective `runs/<X>/`
 * subtree must use `X === principal.runId`. A workflow-process that
 * touches the `addresses/...` subtree is rejected outright so the
 * single-writer contract on inbox/processing/consumed holds at the
 * substrate boundary.
 *
 * The check only fires for `workflow-process` principals; `hub` and
 * `supervisor` have broader write authority by design.
 */
async function enforceWorkflowProcessPathScope(
  principal: Principal,
  topLevelTreePaths: readonly string[],
  listDir: (path: string) => Promise<string[]>,
): Promise<ValidatePushResult> {
  if (principal.kind !== "workflow-process") return { ok: true };
  const parsed = WorkflowProcessPrincipal(principal);
  if (parsed instanceof type.errors) {
    // `workflowRunAuthorize` already rejects malformed
    // `workflow-process` principals at `gateAccess`, so this branch is
    // unreachable when the substrate is wired against the real
    // authorize callback. Fail closed so a future wiring that supplies
    // a permissive authorize (e.g. test substrates using `allowAll`)
    // cannot silently bypass the path-scope enforcement below.
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
 * Validate the `agent-state/` subtree shape (design §3c). The subtree
 * holds one MUTABLE per-agent conversation snapshot directory per agent
 * below the prefix; each entry directly under `agent-state/` must be a
 * `<agentKey>/` DIRECTORY (not a dangling blob), and each `<agentKey>`
 * segment must round-trip URL-encoding so a reader can recover the
 * agent's identity from the path. The conversation blobs inside a
 * `<agentKey>/` directory are opaque to the substrate (the warm agent's
 * ContextStore owns their shape), so no file-level shape is enforced
 * here.
 *
 * A blob written DIRECTLY at `agent-state/<name>` (with no `<agentKey>/`
 * layer) is rejected: it would not be keyed by an agent and would not be
 * recoverable by any reader walking the per-agent layout.
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
    const roundTrip = checkAddressSegmentRoundTrip(segment);
    if (!roundTrip.ok) {
      return {
        ok: false,
        reason: `agent-state segment ${JSON.stringify(segment)} does not round-trip URL-encoding; ${roundTrip.reason}`,
      };
    }
    // Reject a blob dangling directly at `agent-state/<segment>`: every
    // entry under the prefix must be a `<agentKey>/` directory carrying
    // the agent's snapshot files. A directory has children under
    // `agent-state/<segment>/`; a direct blob has none.
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
    // Bound the per-run event/blob walks to the runs this commit could
    // have touched. The substrate guarantees a prefix-preserving commit
    // mutates only paths under `changedPathPrefixes`; every run outside
    // them is carried forward byte-identical, so its per-run invariants
    // (seq-contiguity, terminal-lock, append-only, blob-immutability)
    // cannot change and were already validated when the run was last
    // written. `scopeRunIds` is the set of run ids under a
    // `runs/<runId>/` change prefix. It stays `undefined` -- validate
    // every run -- whenever the substrate could not bound the change set
    // (`changedPathPrefixes` is undefined) OR a change prefix touches the
    // `runs/` subtree at a coarser-than-per-run granularity (a bare
    // `runs/` prefix, which cannot identify which run changed), so the
    // scoping never narrows below what the substrate can prove.
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
          reason: `unexpected top-level entry ${JSON.stringify(entry)}; allowed: "${WORKFLOW_RUN_RUNS_PREFIX}", "${WORKFLOW_RUN_ADDRESSES_PREFIX}", "${WORKFLOW_RUN_AGENT_STATE_PREFIX}", "${WORKFLOW_RUN_GITIGNORE_PATH}"`,
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
      // Enter claim-check validation when the prospective OR prior
      // tree carries an `addresses/` subtree. A prospective tree that
      // omits `addresses/` while the prior tree had consumed or
      // processing entries must still go through the subtree walk so
      // those prior entries' deletion-direction invariants fire.
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

    const runsPresent =
      topLevelTreePaths.includes(WORKFLOW_RUN_RUNS_PREFIX) ||
      priorTopLevels.includes(WORKFLOW_RUN_RUNS_PREFIX);
    if (!runsPresent) {
      // A workflow-run repo without any `runs/` directory in either
      // the prior or the prospective tree is a genesis state for the
      // events subtree — `.gitignore`-only or claim-check-only trees
      // are accepted so the asset routes' init can land before any
      // run has produced an event.
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
      // Sequence contiguity: per-run events must run contiguously
      // through the tip from whatever seq the first entry uses. Without
      // this, a downstream consumer that iterates the log by seq would
      // skip past a gap silently. `entries` is sorted by filenameSeq
      // above. The first seq is not pinned to 0 because the runtime
      // body's emptyState carries `lastSeq = 0` and emits its first
      // event at `seq = lastSeq + 1 = 1`, while the supervisor's
      // self-signed CancelRequested path lands seq=0 against an empty
      // events tree.
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
        if (terminalSeq !== null) {
          return {
            ok: false,
            reason: `run ${runId} has event at seq ${String(entry.filenameSeq)} after terminal ${terminalType} at seq ${String(terminalSeq)}`,
          };
        }
        if (TERMINAL_EVENT_TYPES.has(parsed.parsed.body.type)) {
          terminalSeq = entry.filenameSeq;
          terminalType = parsed.parsed.body.type;
          // Surface the run as newly terminal only when this commit is
          // the one that ADDS the terminal event -- i.e. the terminal
          // blob is absent from the prior tree. A commit that carries an
          // already-terminal run forward unchanged (a later compaction
          // commit folding the per-event files into one) finds the
          // terminal blob already present in the prior tree and emits no
          // signal, so a downstream consumer keyed on the signal does
          // not double-fire.
          if ((await priorReadBlob(entry.blobPath)) === null) {
            const terminalBytes = await readBlob(entry.blobPath);
            newlyTerminalRuns.push({
              runId,
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

    // Append-only / immutability extended to the deletion direction
    // for the runs subtree. The prospective-tree walks above only
    // see paths PRESENT in the prospective tree; a prospective tree
    // that omits a prior `runs/<runId>/events/<seq>.json` or
    // `runs/<runId>/blobs/<sha>` slips past those iterations
    // entirely. Enumerate the prior tree's runs subtree under the
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
        // A run sealed into its combined events.jsonl by this commit
        // legitimately drops its per-event files; the fold was validated
        // byte-for-byte against these same prior blobs above.
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
    if (parsed.deploymentId !== repoId.id) {
      return {
        allowed: false,
        reason: `workflow-process deployment ${parsed.deploymentId} cannot access workflow-run ${repoId.id}`,
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
    if (parsed.deploymentId !== repoId.id) {
      return {
        allowed: false,
        reason: `supervisor deployment ${parsed.deploymentId} cannot access workflow-run ${repoId.id}`,
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
    // The route layer has already pre-resolved the grant verdict and
    // attached it as `authz`. The substrate does NOT re-query the
    // grant store here; it (a) checks the bearer-token's claims
    // bound the requested (ref, action) and have not expired, and
    // (b) sanity-checks that the pre-resolved verdict targets this
    // exact resource and grant verb. Both gates must pass before the
    // verdict's `effect` is honoured.
    const parsed = UserPrincipal(principal);
    if (parsed instanceof type.errors) {
      return {
        allowed: false,
        reason: `user principal is malformed: ${parsed.summary}`,
      };
    }
    if (!parsed.tokenClaims.actions.includes(action)) {
      return {
        allowed: false,
        reason: `token does not grant action ${action}`,
      };
    }
    // `ref === "*"` is the substrate's sentinel for the bulk read
    // performed by `listRefs`. Per-ref filtering is the advertise-refs
    // layer's responsibility, so the bulk read is gated on action and
    // expiry alone.
    if (ref !== "*" && !glob.match(parsed.tokenClaims.refPattern, ref)) {
      return {
        allowed: false,
        reason: `token refPattern ${parsed.tokenClaims.refPattern} does not match ${ref}`,
      };
    }
    if (Date.now() >= parsed.tokenClaims.expiresAt) {
      return {
        allowed: false,
        reason: `token expired at ${parsed.tokenClaims.expiresAt}`,
      };
    }
    const expectedResource = `workflow-run:${repoId.id}`;
    if (parsed.authz.resource !== expectedResource) {
      return {
        allowed: false,
        reason: `authz verdict resource ${parsed.authz.resource} does not match ${expectedResource}`,
      };
    }
    const expectedGrantVerb = repoActionToGrantVerb(action);
    if (parsed.authz.grantVerb !== expectedGrantVerb) {
      return {
        allowed: false,
        reason: `authz verdict grantVerb ${parsed.authz.grantVerb} does not match ${expectedGrantVerb}`,
      };
    }
    if (parsed.authz.effect === "allow") {
      return { allowed: true };
    }
    return {
      allowed: false,
      reason: `authz verdict denied for ${expectedResource} ${expectedGrantVerb}`,
    };
  }

  return {
    allowed: false,
    reason: `unknown principal kind: ${principal.kind}`,
  };
};

// ---------------------------------------------------------------------
// Claim-check API.
//
// Four operations layer on top of `RepoStore.writeTreeDelta` to give
// the workflow runtime a FIFO claim-check queue per address:
//
//   enqueueInbox          — append a new inbox entry for an inbound
//                           message.
//   dequeueToProcessing   — pick the lexicographically-first inbox
//                           entry and atomically move it to
//                           processing.
//   markConsumed          — atomically remove the processing entry
//                           and write the canonical
//                           consumed/<messageId>.json dedup index
//                           entry.
//   replayProcessingToInbox — recovery path that moves every
//                           processing entry back to inbox preserving
//                           its `<receivedAt>-<messageId>` filename
//                           key so FIFO ordering survives a crash.
//
// All four route through `writeTreeDelta`, scoped to the per-address
// subtree via `changedPathPrefixes`. The substrate serializes concurrent
// claim-check operations on the per-repo lock and invokes each
// operation's `computeDelta` callback with a `prior` view of the
// committed tree. The callback reads only what it needs directly --
// `prior.listDirOids` for a directory's names and OIDs, and
// `prior.readBlobByOid` for a specific entry's bytes -- and returns a
// TARGETED delta (the `puts` and `deletes` for the paths that change),
// not the full subtree. The substrate applies that delta atomically over
// the prior tree, carrying every untouched entry forward by OID and
// landing the whole delta in a single commit, which is the atomic-commit
// guarantee these operations require.

function claimCheckCommitRef(): string {
  // Every claim-check operation targets the same canonical ref used by
  // the workflow-run kind handler's event log so subscribers see a
  // single coherent commit stream.
  return "refs/heads/events";
}

function addressSegmentFor(address: string): string {
  // The substrate boundary is the only place URL-encoding happens.
  // `validatePush` rejects non-round-trip segments; mirroring the same
  // encoder here is the only legitimate way to produce one.
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
 * Read one address's claim-check listing from the parent commit: the
 * filenames and blob OIDs directly under
 * `addresses/<addressSegment>/{inbox,processing,consumed}/` (NOT their
 * bytes), plus the retention watermark. The bytes of the single entry a
 * leg actually moves are read separately by OID via
 * `prior.readBlobByOid`, so the unbounded consumed/ dedup index is
 * enumerated (one `listDirOids` per bucket, names and OIDs only) but
 * never read blob-by-blob. Every read goes through the store's
 * cache-backed `prior` closures under the write lock. An empty listing
 * covers the repo/ref/address-absent first-write states -- all
 * legitimate for a brand-new operation.
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

/**
 * Decode the per-address retention watermark from its blob bytes. The
 * caller treats an absent watermark blob as 0 (the address has never
 * pruned; nothing refused).
 */
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
 * Append a new inbox entry for `address`. The merge callback reads
 * the address subtree under the per-repo lock, augments the inbox
 * with the new entry, and returns the full set of address files. The
 * substrate replaces the address subtree wholesale.
 *
 * Rejects if a same-messageId entry already exists in any queue
 * state at the address — including a prior inbox entry at a
 * different `receivedAt`. The caller is expected to consult the
 * dedup index (consumed/) before calling, but enforcing the
 * invariant here also catches the concurrent-enqueue race that the
 * per-repo lock alone cannot surface.
 */
export async function enqueueInbox(
  store: RepoStore,
  principal: Principal,
  repoId: RepoId,
  args: EnqueueInboxArgs,
): Promise<EnqueueInboxResult> {
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
  const { commitSha } = await store.writeTreeDelta(principal, repoId, ref, {
    changedPathPrefixes: new Set([addressPrefix(addressSegment)]),
    message: `enqueue inbox ${args.address} ${args.messageId}`,
    computeDelta: async (_parentCommitSha, prior) => {
      const listing = await readAddressListing(prior, addressSegment);
      // Refuse a definitively-stale enqueue: a message whose receivedAt
      // is strictly below the retention watermark could have had its
      // consumed/ dedup entry pruned, so a duplicate can no longer be
      // ruled out. Reject it LOUDLY rather than risk reprocessing. This
      // is the second half of the exactly-once guarantee: above the
      // watermark the consumed/ index is authoritative; below it, refuse.
      if (args.receivedAt < listing.watermark) {
        throw new Error(
          `claim_check_stale_enqueue: address ${args.address} message ${args.messageId} receivedAt ${String(args.receivedAt)} is below the retention watermark ${String(listing.watermark)}; its dedup entry may have been pruned, so it is refused as definitively-stale`,
        );
      }
      if (listing.inbox.some((e) => e.name === inboxFname)) {
        throw new Error(
          `claim_check_duplicate_inbox: ${newInboxPath} already exists`,
        );
      }
      // consumed/ is keyed by messageId alone, so this is an exact
      // filename lookup against the dedup index.
      if (listing.consumed.some((e) => e.name === consumedFname)) {
        throw new Error(
          `claim_check_already_consumed: address ${args.address} message ${args.messageId} is already in the consumed dedup index`,
        );
      }
      if (listing.processing.some((e) => e.name.endsWith(messageIdSuffix))) {
        throw new Error(
          `claim_check_already_processing: address ${args.address} message ${args.messageId} is currently in processing`,
        );
      }
      // Reject a second inbox entry for the same messageId at a
      // different receivedAt. The validatePush atomicity check also
      // catches this on the commit path, but surfacing it here gives the
      // caller a precise error and keeps the bad tree off the substrate.
      const inboxDup = listing.inbox.find((e) =>
        e.name.endsWith(messageIdSuffix),
      );
      if (inboxDup !== undefined) {
        throw new Error(
          `claim_check_already_inbox: address ${args.address} message ${args.messageId} is already in the inbox at ${inboxPath(addressSegment, inboxDup.name.slice(0, -".json".length))}`,
        );
      }
      return {
        puts: { [newInboxPath]: utf8(JSON.stringify(envelope)) },
        deletes: [],
      };
    },
  });
  return { commitSha, inboxKey, envelope };
}

export type DequeueToProcessingResult = {
  commitSha: string;
  key: string;
  envelope: ClaimCheckEnvelope;
} | null;

/**
 * Move the FIFO-first inbox entry for `address` to processing.
 * Returns `null` when the inbox is empty so the caller can
 * distinguish "nothing to do" from "operation failed".
 *
 * FIFO is keyed on the parsed numeric `receivedAt` prefix of the
 * inbox filename, with a lexicographic messageId tiebreak. The
 * substrate does NOT rely on uniform digit widths — sorting raw
 * filenames would put `"100-…"` ahead of `"99-…"` since `'1' < '9'`,
 * which violates the FIFO invariant.
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
      // Sort by numeric receivedAt with a messageId tiebreak. A raw
      // string sort would not agree with chronological order when
      // receivedAt values have non-uniform digit widths.
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
        // Empty inbox: nothing to move. The commit is a no-op rewrite of
        // the same tree; the caller reads `dequeued === null`.
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
 * Read the processing-queue entry for `messageId` at `address` without
 * mutating the tree. Returns the decoded claim-check envelope (carrying
 * `mailAuditRef` and, when the enqueuer inlined them, the base64
 * `rawMessage` bytes) or `null` when no processing entry exists for the
 * messageId.
 *
 * This is the read half of mailbox ownership (§3a): the supervisor's
 * dispatch loop moves an inbox entry to processing and forwards a
 * `trigger.fired{messageId}` to the workflow-process child; the child
 * calls this to recover the inbound message bytes that become its step
 * input.
 *
 * The read is a flat working-tree read of
 * `addresses/<seg>/processing/`. The substrate materializes each
 * claim-check commit's touched paths into the repo's working tree (the
 * delta write removes each deleted path and writes each put after
 * validation passes), so a read issued after `dequeueToProcessing`
 * committed -- which is exactly when the supervisor forwards
 * `trigger.fired` -- observes the processing entry. Reading the working tree (rather than walking the
 * committed git tree) matches the workflow-process child's sibling
 * reads of `workflow.json` and `runs/<runId>/events/`. Because the
 * read issues no commit it cannot race the supervisor's `markConsumed`
 * write; it returns a point-in-time snapshot of the directory.
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
 * Atomically remove the processing entry for `messageId` at `address`,
 * write the canonical `consumed/<messageId>.json` dedup index entry,
 * advance the per-address retention watermark, and prune consumed
 * entries the watermark has passed. The caller is expected to have
 * called `dequeueToProcessing` for this messageId; calling
 * `markConsumed` without a matching processing entry throws.
 *
 * The consumed envelope preserves the original `receivedAt` and
 * `mailAuditRef` from the processing entry so the dedup index doubles
 * as an audit record.
 *
 * Retention (the bounded-`consumed/` contract): the watermark advances
 * to `max(priorWatermark, min(consumedAt - retentionHorizonMs,
 * thisEntry.receivedAt))` -- monotonic, and never past the entry being
 * written so the new entry is always retained. Every consumed entry
 * whose `receivedAt` is strictly below the new watermark is dropped
 * (the oldest age-ordered tail). `consumed/` therefore reaches a
 * bounded steady state of roughly one horizon's worth of entries
 * instead of growing one entry per message forever.
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
      };
      consumedEnvelope = envelope;

      // The watermark may only advance, and never past the entry this
      // commit writes (so the new entry is always retained -- a message
      // consumed long after receipt may legitimately sit below
      // `consumedAt - horizon`, and it is pruned on a later commit once
      // the watermark passes ITS receivedAt).
      const horizonBoundary = args.consumedAt - retentionHorizonMs;
      const newWatermark = Math.max(
        listing.watermark,
        Math.min(horizonBoundary, envelope.receivedAt),
      );
      advancedWatermark = newWatermark;

      // Prune the oldest consumed tail: read each retained consumed
      // entry's receivedAt and drop any that has fallen strictly below
      // the new watermark. This is the one leg that must scan the
      // consumed index — its filenames carry only the messageId, so the
      // receivedAt lives in the bytes — and is the residual the
      // consumed-shard lever removes. The new entry (added via puts) is
      // never below the watermark by construction.
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

/**
 * Recovery path: move every processing entry at `address` back to
 * inbox preserving the original `<receivedAt>-<messageId>` filename
 * key so FIFO ordering survives a workflow-process crash. Returns
 * the set of keys that were moved; when nothing was in processing
 * the returned `replayedKeys` is empty (and the commit is a no-op
 * rewrite of the same tree).
 *
 * The replay is atomic across all processing entries — a partial
 * replay that left some entries in processing would corrupt the
 * FIFO discipline (the next dequeue would pull the wrong entry).
 *
 * Watermark carve-out (load-bearing — do NOT "tighten" this): the
 * replay deliberately does NOT apply the `receivedAt < watermark`
 * stale-reject that `enqueueInbox` applies. A `processing/` entry was
 * already dequeued past the dedup index, so re-admitting it to
 * `inbox/` even when its `receivedAt` has fallen below an advanced
 * watermark is correct — the message is a legitimately in-flight one
 * recovered after a crash, not a fresh inbound that could be a
 * duplicate. Applying the stale-reject here would silently LOSE that
 * message. The watermark only ever gates fresh inbound at the enqueue
 * boundary; the recovery replay is exempt by design.
 */
export async function replayProcessingToInbox(
  store: RepoStore,
  principal: Principal,
  repoId: RepoId,
  address: string,
): Promise<ReplayProcessingToInboxResult> {
  const addressSegment = addressSegmentFor(address);
  const ref = claimCheckCommitRef();
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
        const inboxFull = `${inboxDir}${entry.name}`;
        if (inboxNames.has(entry.name)) {
          throw new Error(
            `claim_check_replay_collision: ${inboxFull} already exists; cannot replay processing entry`,
          );
        }
        // Re-admit the in-flight entry WITHOUT the watermark stale-reject
        // enqueueInbox applies: it was already past dedup, so a
        // below-watermark receivedAt is no reason to refuse it. Applying
        // the stale-check here would lose a legitimately in-flight
        // message after a crash. Do not tighten this.
        const bytes = await prior.readBlobByOid(entry.oid);
        puts[inboxFull] = bytes;
        deletes.push(`${processingDir}${entry.name}`);
        replayedKeys.push(entry.name.slice(0, -".json".length));
      }
      return { puts, deletes };
    },
  });
  return { commitSha, replayedKeys };
}
