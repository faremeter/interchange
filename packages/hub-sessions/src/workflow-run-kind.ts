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
//     event log. The principal carries `{ anchorRunId, runId? }`;
//     this handler verifies `repoId.id === anchorRunId`.
//   - `supervisor` principal: read/write its own deployment's event
//     log. The principal carries `{ anchorRunId }`; this handler
//     verifies `repoId.id === anchorRunId`.
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
 * Per-run inbound mail-part subtree. Non-text inbound mail content
 * (image/audio/video/document mail parts) is committed here as real
 * files rather than inlined into the JSON event log, whose serialization
 * boundary would corrupt binary bytes. The layout is
 * `runs/<runId>/parts/<urlEncoded(messageId)>/<index>-<name>`: one
 * directory per inbound message (so a long-lived run's successive turns
 * never collide), and one file per mail part carrying its verbatim
 * bytes. The workflow-host ingest writes the bytes and records a
 * lightweight `{ name, contentType, ref }` reference into the run's
 * trigger / signal payload; the step invoker reads the bytes back at
 * `agent.send` time. Files are immutable once written, like `blobs/`.
 */
export const WORKFLOW_RUN_PARTS_DIR = "parts";

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
 * INVARIANT (operator-owned): the horizon must be >= the longest window in
 * which the same `messageId` could legitimately be re-submitted and still must
 * be caught as a duplicate. The hub now redelivers un-acked inbound mail
 * (connected-window retry and reconnect-redelivery), so an at-least-once
 * internal source DOES exist -- but the dedup guarantee against it does not
 * rest on window arithmetic. It rests on a STRUCTURAL fact: `enqueueInbox` is
 * only ever called with a freshly stamped `receivedAt` (a redelivery
 * re-enters `onMailMessage` and re-stamps `Date.now()`, never carrying the
 * original), and the watermark only ever advances to at most
 * `consumedAt - retentionHorizonMs`, which is <= now, so a fresh `receivedAt`
 * always sits a full horizon above the watermark and can never be stale-
 * refused. A redelivery instead hits the `consumed/`/`processing/`/`inbox/`
 * dedup index and is deduped there. The sole path that carries an original
 * (old) `receivedAt` back into the queue is `replayProcessingToInbox`, which
 * writes straight to `inbox/` and bypasses the stale gate entirely. So
 * `claim_check_stale_enqueue` is unreachable via redelivery today. If any
 * redelivery source is ever changed to carry the ORIGINAL `receivedAt` into
 * `enqueueInbox`, stale becomes reachable, the horizon must then be >= that
 * source's maximum redelivery window, and `StaleInboxEnqueueError`'s
 * withhold-not-ack handling becomes load-bearing; a breach surfaces LOUDLY (an
 * old-`receivedAt` re-submission is refused at enqueue) rather than as silent
 * double-processing.
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
 * Conversational-mailbox subtree for the warm single-step agent. The
 * substrate mailbox backing commits the agent's durable inbox under
 * `mailbox/INBOX/` so the full message history replicates to the hub
 * alongside the run state. The layout is:
 *
 *   - `mailbox/INBOX/index.json` — the mailbox index. MUTABLE: the
 *     backing rewrites it on every flush, so it is exempt from the
 *     retained-blob byte-equality walk the `<uid>.eml` blobs are subject
 *     to. It must nonetheless PERSIST once it existed: dropping it drops
 *     the whole mailbox and resets uidValidity/uidNext on the next open.
 *   - `mailbox/INBOX/<uid>.eml` — one message per file, carrying the
 *     raw signed message bytes. `<uid>` is a decimal integer >= 1. A
 *     RETAINED `<uid>.eml` (present in both prior and prospective) is
 *     opaque and IMMUTABLE: it must reappear byte-identically, exactly
 *     like `runs/<runId>/blobs/`. A prior `<uid>.eml` may be ABSENT from
 *     the prospective tree — that is the warm agent expunging a message.
 *     The raw bytes are not lost: they stay reachable through the parent
 *     commit, and a `workflow-run` repo's objects are never GC'd (its
 *     kind is off the GC allow-list), so an expunged message survives in
 *     history for the life of the run repo. The audit trail rests on
 *     "these objects are never pruned", not on the live tree being
 *     monotonic.
 *
 * The only entries permitted under `mailbox/` are the `INBOX/`
 * directory; the only entries permitted under `mailbox/INBOX/` are
 * `index.json` and `<uid>.eml` message files. Anything else fails the
 * push.
 */
export const WORKFLOW_RUN_MAILBOX_PREFIX = "mailbox";
export const WORKFLOW_RUN_MAILBOX_INBOX_DIR = "INBOX";
export const WORKFLOW_RUN_MAILBOX_INDEX_FILE = "index.json";

/**
 * Allowed top-level entries in the prospective tree. Anything else
 * fails the push. `control/` has no v1 use and stays absent.
 */
const ALLOWED_TOP_LEVEL = new Set<string>([
  WORKFLOW_RUN_RUNS_PREFIX,
  WORKFLOW_RUN_ADDRESSES_PREFIX,
  WORKFLOW_RUN_AGENT_STATE_PREFIX,
  WORKFLOW_RUN_MAILBOX_PREFIX,
  WORKFLOW_RUN_GITIGNORE_PATH,
]);

/**
 * Per-message filename shape for the `mailbox/INBOX/` subtree:
 * `<uid>.eml`, where `<uid>` is a decimal integer >= 1 (no leading zero,
 * never `0`). Pins the shape so a malformed message name fails the push
 * at the boundary rather than landing silently.
 */
const MAILBOX_EML_FILENAME_RE = /^[1-9][0-9]*\.eml$/;

const CLAIM_CHECK_SUBDIRS = new Set<string>([
  WORKFLOW_RUN_INBOX_DIR,
  WORKFLOW_RUN_PROCESSING_DIR,
  WORKFLOW_RUN_CONSUMED_DIR,
]);

/** Per-event filename shape: a decimal integer followed by `.json`. */
const EVENT_FILENAME_RE = /^(0|[1-9][0-9]*)\.json$/;

/**
 * Parse the seq from a per-event log filename `<seq>.json` under
 * `runs/<runId>/events/`. Returns the non-negative integer seq, or
 * `null` when the name is not a legal per-event filename. This is the
 * one place the filename shape is defined; every reader of the event log
 * narrows names through it rather than re-encoding the regex. Callers
 * decide what an illegal name means -- a foreign entry to skip, or a
 * substrate-invariant violation to surface -- since `validatePush` is
 * the authority that keeps illegal names from landing in the first place.
 */
export function parseEventSeq(filename: string): number | null {
  const match = EVENT_FILENAME_RE.exec(filename);
  if (match === null) return null;
  const seqStr = match[1];
  if (seqStr === undefined) return null;
  return Number.parseInt(seqStr, 10);
}

/**
 * Narrow a per-event filename to its seq, throwing when it is illegal.
 * A reader that enumerates the committed event log to act on its entries
 * uses this rather than `parseEventSeq`: `validatePush` is the authority
 * that keeps an illegal name from ever landing under
 * `runs/<runId>/events/`, so a name that reaches a reader is corruption,
 * and silently skipping it would drop an event from processing. `context`
 * is the repo-root-relative blob path, surfaced in the error so the
 * offending entry is identifiable.
 */
export function requireEventSeq(filename: string, context: string): number {
  const seq = parseEventSeq(filename);
  if (seq === null) {
    throw new Error(`event_filename_invalid: ${context}`);
  }
  return seq;
}

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
 * Per-mail-part filename shape for the
 * `runs/<runId>/parts/<urlEncoded(messageId)>/` subtree:
 * `<index>-<name>`, where `index` is the decimal position of the
 * mail part within its inbound message and `name` is a non-empty
 * (possibly sanitized) filename. The workflow-host ingest owns the exact
 * encoding and sanitizes untrusted names to satisfy this shape; this regex
 * pins the shape so a malformed name fails the push rather than landing
 * silently. The bytes themselves are opaque and immutable, exactly like
 * `blobs/`.
 */
const PART_FILENAME_RE = /^(0|[1-9][0-9]*)-(.+)$/;

/**
 * Maximum byte length of a single mail part path component (the
 * URL-encoded message segment, and each `<index>-<name>` filename).
 * messageIds and mail part names arrive from untrusted inbound mail;
 * an over-long RFC 5322 message-id URL-encodes past the filesystem's
 * 255-byte component limit and would otherwise fail at disk-write time,
 * downstream of validation. Reject it at the boundary instead.
 */
export const MAX_MAIL_PART_PATH_COMPONENT_BYTES = 255;

function mailPartComponentByteLength(component: string): number {
  return new TextEncoder().encode(component).length;
}

/**
 * Entries the kind handler accepts under `runs/<runId>/`. The `events/`
 * subtree carries the append-only event log; the `blobs/` subtree carries
 * opaque, content-addressed step outputs the `BlobSubstrate` adapter spills
 * there when a value exceeds the inline-encoding threshold; `grants.json`
 * carries the run's authorization grants, delivered by the hub's
 * `run.grants` frame ahead of the trigger and read back by the sidecar's
 * `onRunStart` barrier. The grants file is a run-dir sibling of `events/`,
 * not part of the event log, so the event-shape and blob-immutability walks
 * treat it as inert.
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
  "rejection?": {
    code: "string > 0",
    message: "string > 0",
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
 * Terminal event discriminators mapped to the `workflow_run.status` value
 * each settles the run into. A run whose log contains an entry with one of
 * these `type` values must not receive any event with a strictly greater
 * seq.
 *
 * This map is a hand-rolled copy of the runtime's terminal-run vocabulary
 * (`isTerminalRunPhase` in `@intx/workflow` state-machine `state.ts`,
 * re-exported from the state-machine index and consumed by `transition.ts`),
 * duplicated here because `@intx/hub-sessions` must not depend on
 * `@intx/workflow`. It is the sole authority for that vocabulary and MUST
 * stay in sync with the canonical runtime definition:
 * if the runtime adds or removes a terminal run phase, update this map too.
 * Drift silently reopens the restore-time double-driver collision that
 * `scanRunsForBoot` (below) exists to prevent.
 */
type TerminalRunStatus = "completed" | "failed" | "cancelled";

const TERMINAL_EVENT_STATUS: ReadonlyMap<string, TerminalRunStatus> = new Map([
  ["RunCompleted", "completed"],
  ["RunFailed", "failed"],
  ["RunCancelled", "cancelled"],
]);

/**
 * Membership set of terminal event types, derived from
 * `TERMINAL_EVENT_STATUS` so it always covers exactly the mapped types and
 * the two cannot drift apart.
 */
const TERMINAL_EVENT_TYPES = new Set<string>(TERMINAL_EVENT_STATUS.keys());

/**
 * Classify a workflow-run event type against the terminal-status vocabulary.
 * `TERMINAL_EVENT_STATUS` is the sole authority (see above), so a type absent
 * from it is by definition not terminal: no separate membership set is
 * consulted and no "unmapped terminal type" case can arise.
 */
export function classifyTerminalEvent(
  eventType: string,
): { terminal: true; status: TerminalRunStatus } | { terminal: false } {
  const status = TERMINAL_EVENT_STATUS.get(eventType);
  return status === undefined
    ? { terminal: false }
    : { terminal: true, status };
}

/**
 * True when a blob is absent from the prior tree -- i.e. this commit is the
 * one that authored it. Consumers act only on a newly-added blob; a blob
 * already present in the prior tree was carried forward unchanged by a
 * compaction commit and must not be re-acted upon.
 */
async function blobIsNewlyAdded(
  blobPath: string,
  priorReadBlob: (path: string) => Promise<Uint8Array | null>,
): Promise<boolean> {
  return (await priorReadBlob(blobPath)) === null;
}

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
      // The pre-first-event window: `grants.json` (the hub's `run.grants`
      // frame writes grants ahead of the trigger) and `parts/` (the
      // supervisor commits inbound-mail mail part bytes before firing the
      // trigger) may both land before the child emits its first event. Carry
      // such a run forward untouched -- there is no event log to enumerate
      // yet, and the mail parts subtree is validated by its own walk. Any
      // other events-less shape (e.g. a bare `blobs/` with no events) remains
      // rejected below.
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

type RunPartEntry = {
  runId: string;
  messageSegment: string;
  filename: string;
  blobPath: string;
};

/**
 * Walk every `runs/<runId>/parts/<messageSegment>/` directory and
 * validate each entry: the `<messageSegment>` is a URL-encoded messageId
 * that must round-trip cleanly (the same canonical-encoding discipline the
 * claim-check `addresses/` subtree enforces) and stay within the path-
 * component byte cap, must be a directory rather than a dangling blob, and
 * each filename matches the `<index>-<name>` shape within the same cap. The
 * `parts/` subdirectory is optional: a run that never received a
 * non-text inbound message never produces one. Returns the flat entry list
 * so the caller can apply immutability checks against the prior tree,
 * exactly as it does for blobs.
 */
async function enumerateRunParts(
  listDir: (path: string) => Promise<string[]>,
  scopeRunIds?: ReadonlySet<string>,
): Promise<
  { ok: true; parts: RunPartEntry[] } | { ok: false; reason: string }
> {
  const out: RunPartEntry[] = [];
  // See enumerateRunBlobs: a defined `scopeRunIds` walks only the commit's
  // touched runs; an untouched run's mail parts are carried forward
  // byte-identical and were validated when written.
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
      // A message segment must be a directory carrying at least one
      // mail part file. An empty listing means either a dangling blob
      // committed directly at `parts/<segment>` (the substrate lists a
      // blob path as empty, exactly as the agent-state walk detects) or an
      // empty directory; both are rejected so untrusted inbound content has
      // no silent-accept path.
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
        // Each mail part entry must be a leaf blob, not a nested directory.
        // The `<index>-<name>` shape is permissive (`.+`), so a directory
        // named e.g. `0-foo` would otherwise pass and admit an arbitrarily
        // nested subtree, breaking the one-file-per-mail-part invariant and
        // -- on a pack-receive validation with no `listDirOids` -- driving the
        // immutability resolver to `readBlob` a tree path and throw. A blob
        // lists as empty; a directory lists its children.
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
 * Validate the per-run `parts/` subtree's immutability against the
 * prior tree, both directions, mirroring the blobs walk. Mail part files
 * are write-once. A path present in the prior tree must carry the same git
 * blob OID in the prospective tree: unlike `blobs/`, a mail part filename
 * is `<index>-<name>` (not a content hash), so the same path can carry
 * different bytes -- the OID compare is the load-bearing immutability guard,
 * and comparing the OID git already computed avoids re-reading tens of MB of
 * mail part bytes on every commit that merely touches the run. A prior path
 * must reappear (no deletion): run reclaim drops the whole `runs/<runId>/`
 * subtree outside `validatePush`, so a partial mail part deletion is always
 * a violation. Structural shape is enforced by `enumerateRunParts`.
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
 * Round-trip a URL-encoded path segment through decode then encode. A
 * divergence means the segment is not the canonical encoding of any
 * value, which would leave consumers guessing which encoding to use
 * when reading the subtree. Surface as a concrete rejection at push
 * time. Shared by every subtree that keys a directory by an
 * `encodeURIComponent`-encoded identity (`addresses/`, `agent-state/`,
 * and the per-run `parts/` message segment); callers prepend
 * their own subtree context to the returned reason.
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
    const roundTrip = checkUrlSegmentRoundTrip(segment);
    if (!roundTrip.ok) {
      return {
        ok: false,
        reason: `agent-state ${roundTrip.reason}`,
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

/**
 * Enforce mailbox `<uid>.eml` immutability via prior-tree byte equality.
 * A message blob RETAINED from the prior tree (present in both) must carry
 * byte-identical contents in the prospective tree. A prior blob absent
 * from the prospective tree is a legal expunge and never reaches here
 * (the caller only checks retained paths). Mirrors the blob-immutability
 * discipline (`checkBlobPriorByteEquality`) with mailbox-specific wording.
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
 * Walk the `mailbox/INBOX/` subtree and validate its shape. The only
 * entry permitted directly under `mailbox/` is the `INBOX/` directory;
 * the only entries permitted under `mailbox/INBOX/` are the mutable
 * `index.json` file and `<uid>.eml` message files. Each entry must be a
 * leaf blob rather than a nested directory (a blob lists as empty, a
 * directory lists its children -- the same discrimination the agent-state
 * and mail-parts walks use). Returns the flat set of `<uid>.eml` blob
 * paths so the caller can hold retained messages immutable against the
 * prior tree, plus `indexPresent` -- whether `index.json` exists under
 * the INBOX -- so the caller can enforce index continuity. An absent
 * `mailbox/` subtree lists as empty and contributes no message paths.
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
  // A `mailbox/` top-level whose `INBOX` child carries no entries is a
  // dangling blob committed directly at `mailbox/INBOX` (a blob lists as
  // empty), not the required directory. Git never records an empty
  // directory, so a present-but-empty listing is always the blob case.
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
 * Validate the `mailbox/INBOX/` subtree (design conversational-mailbox).
 * Enforces the subtree shape via `enumerateMailboxInbox`, then holds a
 * RETAINED `<uid>.eml` message blob byte-identical against the prior tree.
 * A prior `<uid>.eml` absent from the prospective tree is a legal expunge:
 * the warm agent physically removes a message from the live INBOX. The raw
 * bytes are not lost -- they stay reachable through the parent commit, and
 * a `workflow-run` repo's objects are never GC'd (its kind is excluded
 * from the GC allow-list; see `DEFAULT_GC_KINDS` in `agent-repo`), so the
 * expunged message survives in history for the life of the run repo. The
 * audit trail therefore rests on "these objects are never pruned", not on
 * the live tree being monotonic.
 *
 * The `index.json` entry is mutable, but must PERSIST once it existed: if
 * the prior tree carried an index and the prospective tree drops it, the
 * whole mailbox has vanished, which would reset `uidValidity` / `uidNext`
 * on the next open and force uid reuse from 1. That is rejected. A
 * well-behaved backing always rewrites `index.json` on flush, so the guard
 * fails no legitimate push. Mirrors the blob-immutability discipline the
 * `runs/<runId>/blobs/` subtree uses, minus the deletion direction.
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

    const mailboxPresent =
      topLevelTreePaths.includes(WORKFLOW_RUN_MAILBOX_PREFIX) ||
      priorTopLevels.includes(WORKFLOW_RUN_MAILBOX_PREFIX);
    if (mailboxPresent) {
      // Enter mailbox validation when the prospective OR prior tree
      // carries a `mailbox/` subtree. A prospective tree that drops the
      // subtree while the prior tree held one must still go through the
      // walk so the index-continuity guard fires on the vanished
      // `index.json` (a message-only expunge is legal; dropping the whole
      // mailbox is not).
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
          // Enforce the cancel-origin principal only for a NEWLY-ADDED blob.
          // A CancelRequested's origin-vs-signer rule is a write-time
          // authorization: it belongs to the commit that authors the event. A
          // later commit that merely carries the event forward -- e.g. the
          // run's own workflow-process cascade write of CancelPropagated /
          // RunCancelled, which re-lists the whole events prefix -- must not be
          // rejected because the carried-forward cancel was authored under a
          // different (supervisor) signer. Re-checking it protects nothing: the
          // byte-equality check above already proves a carried-forward blob is
          // unchanged, and the deletion-direction check proves it cannot be
          // dropped. A tampered (byte-diverged) blob never reaches here --
          // checkPriorByteEquality rejects it first. Mirrors the newly-terminal
          // gate below, which likewise acts only on a blob absent from the
          // prior tree.
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
          // Surface the run as newly terminal only when this commit is
          // the one that ADDS the terminal event -- i.e. the terminal
          // blob is absent from the prior tree. A commit that carries an
          // already-terminal run forward unchanged (a later compaction
          // commit folding the per-event files into one) finds the
          // terminal blob already present in the prior tree and emits no
          // signal, so a downstream consumer keyed on the signal does
          // not double-fire.
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

  // Fail closed on any kind not handled above. The tenant-level
  // `workflow` principal kind (`@intx/types` principalKinds) is a
  // grant owner, not a workflow-run repo bearer, and never carries a
  // workflow-run push here -- so it is intentionally left denied.
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
 * Which already-present state an `enqueueInbox` call found the messageId
 * in. Every value is POSITIVE evidence the message's bytes are durably on
 * disk (inbox/processing) or were already consumed -- so a caller gating a
 * receipt on the enqueue may safely acknowledge on any of them.
 */
export type EnqueueAlreadyPresentReason =
  | "duplicate"
  | "already_inbox"
  | "processing"
  | "consumed";

/**
 * Outcome of an `enqueueInbox` call. Modeled as a value (not an exception)
 * precisely because the return/throw boundary is the ack/withhold boundary
 * for a caller gating a durable-receipt ack: a returned outcome is safe to
 * acknowledge (the bytes are on disk -- freshly written or already present),
 * a throw is not (the write could not complete or its disposition cannot be
 * decided). `enqueued` is the only outcome that added a new inbox entry, so
 * it is the only one a dispatch-driving caller wakes its loop on.
 */
export type EnqueueInboxOutcome =
  | ({ outcome: "enqueued" } & EnqueueInboxResult)
  | { outcome: "already-present"; reason: EnqueueAlreadyPresentReason };

/**
 * Internal signal thrown from the `enqueueInbox` merge callback when the
 * messageId is already present in a queue state. Caught at the `enqueueInbox`
 * boundary and turned into an `already-present` outcome; never escapes. It
 * carries the specific `reason` so the boundary maps it without re-deriving.
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
 * Thrown by `enqueueInbox` when the inbound's `receivedAt` is strictly below
 * the address's retention watermark. This is refusal under UNCERTAINTY, not
 * proof of prior receipt: the consumed dedup entry that would rule out a
 * duplicate may have been pruned, so the substrate can no longer tell a
 * duplicate from a never-processed message and refuses rather than risk
 * reprocessing. A caller gating a durable-receipt ack MUST NOT acknowledge on
 * this -- acking an "I cannot tell" would terminally drop a message that was
 * never written. It is its own type (not a generic Error) so that a caller,
 * and monitoring, can surface it as a distinct loud signal rather than
 * blending it into ordinary I/O-failure noise.
 *
 * Structurally unreachable on the mail-inbound path today: `enqueueInbox` is
 * only ever called with a freshly stamped `receivedAt` (a redelivery
 * re-stamps `Date.now()` rather than carrying the original), and the watermark
 * only ever advances to at most `consumedAt - retentionHorizonMs <= now`, so a
 * fresh `receivedAt` sits a full horizon above it. The sole path that carries
 * an original (old) `receivedAt` back into the queue is
 * `replayProcessingToInbox`, which writes straight to `inbox/` and bypasses
 * this gate entirely. If any redelivery source is ever changed to carry the
 * original `receivedAt` into `enqueueInbox`, this becomes reachable and its
 * withhold-not-ack handling becomes load-bearing.
 */
export class StaleInboxEnqueueError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StaleInboxEnqueueError";
  }
}

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
  // The already-present cases throw `InboxEntryAlreadyPresent` from the merge
  // callback and are caught here into an `already-present` outcome; the
  // stale-refusal throws `StaleInboxEnqueueError`, and a substrate/I/O failure
  // throws a generic error -- both of those propagate. The return/throw split
  // is deliberate and load-bearing: it is the ack/withhold boundary for a
  // caller gating a durable-receipt ack (return = safe to ack, throw =
  // withhold), so `stale` sits with I/O on the throw side, NOT with the
  // already-present cases (see `StaleInboxEnqueueError`).
  let commitSha: string;
  try {
    ({ commitSha } = await store.writeTreeDelta(principal, repoId, ref, {
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
        // consumed/ is keyed by messageId alone, so this is an exact
        // filename lookup against the dedup index.
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
        // different receivedAt. The validatePush atomicity check also
        // catches this on the commit path, but surfacing it here gives the
        // caller a precise error and keeps the bad tree off the substrate.
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
 * read of `runs/<runId>/events/`. Because the
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
   * Present when the supervisor deliberately refused the message instead of
   * delivering it to the run. The consumed entry remains the durable dedup
   * record, while Hub projection uses this detail to fail (rather than settle)
   * an exclusive-dispatch row.
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
        ...(args.rejection !== undefined ? { rejection: args.rejection } : {}),
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

export type ScanRunsForBootResult = {
  ownedMessageIds: Set<string>;
  pendingSealRunIds: string[];
};

/**
 * Walk `runs/` once and return the two boot-recovery inputs the supervisor's
 * spawn needs, from a single traversal of the working tree via `getRepoDir`:
 *
 * - `ownedMessageIds`: the `consumedMessageId`s of NON-terminal runs -- the
 *   messages a live run still owns. Spawn feeds this into
 *   `replayProcessingToInbox`'s `ownedMessageIds` so a parked run's message is
 *   not re-admitted to inbox and dispatched a second time while the run is
 *   recovered by re-driving its durable log. Without this, the re-drive AND the
 *   re-triggered fresh run both re-park the same awaitSignal gate on the same
 *   runId, and the two concurrent runtime bodies race to a corrupt terminal.
 * - `pendingSealRunIds`: runs that are terminal but still in per-event form --
 *   an interrupted fold left them unsealed. Spawn hands these to the recovery
 *   sweep, which re-runs the idempotent fold. A terminal event is a *proposal*:
 *   the authoritative decision is `compactRunEvents`, which independently
 *   re-checks the run's max-seq event and no-ops a run that is not actually
 *   terminal, so this scan may be loose.
 *
 * The working tree tracks the run-event ref (`refs/heads/main`); the
 * claim-check ref (`refs/heads/events`) cannot see it, which is why this lives
 * at the caller rather than inside `replayProcessingToInbox`'s single-ref
 * delta. A run whose log is sealed (combined `events.jsonl`, only permitted for
 * a terminated run) contributes to neither set; an absent `runs/` directory
 * yields empty results.
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
    // A sealed run (combined events file) is terminal by the handler's
    // own invariant -- only a terminated run is sealed -- so it owns
    // nothing and is already folded. Its presence also means the per-event
    // directory is absent.
    let sealed = false;
    try {
      await fs.access(path.join(runDir, WORKFLOW_RUN_EVENTS_FILE));
      sealed = true;
    } catch (cause) {
      // ENOENT is the normal "not sealed" case. Any other stat error leaves
      // the run to fall through to the events-dir read below, which resolves
      // it, so this catch is benign; warn so the anomaly is still visible.
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
      // ENOENT means the run has neither a sealed log nor a per-event
      // directory (grants may be staged before the first event); skip it.
      // A non-ENOENT error drops the run from BOTH result sets, and a live
      // run dropped from ownedMessageIds gets its message re-admitted and
      // dispatched a second time on the same runId -- the double-driver
      // corruption this scan exists to prevent. Surface it, but still skip:
      // aborting the whole boot scan over one run is worse.
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
        // A corrupt or unreadable event file drops this run's
        // classification: a missed RunStarted re-admits its message (a
        // second run on the same runId), a missed terminal event skips a
        // needed seal. Surface it, but skip the file rather than abort the
        // scan. ENOENT here is a benign race (the file vanished mid-scan).
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
 * Classify a run's lifecycle from a read surface. The committed (git-object)
 * and working-tree (node:fs) readers share this core: a sealed combined log is
 * terminal; otherwise the latest per-event file decides terminal-vs-live, and a
 * run with no events is absent. The surface owns every read detail -- the
 * absent/ENOENT discrimination, the per-surface entry filter, and wrapping an
 * unreadable latest event as `workflow_run_event_unreadable` -- so this core
 * never sees a raw read error.
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
 * Read the durable lifecycle of one run from the workflow-run working tree.
 * `grants.json` alone is still an absent run: grants are staged before the
 * first trigger, while the first event is the durable proof that the run was
 * fired. A sealed event log is terminal by the kind handler's compaction
 * invariant.
 *
 * The supervisor uses this when in-memory cohort membership is empty. That
 * happens both for a genuinely new deployment and briefly during recovery,
 * so treating both states as "fire" would start a second driver for a live
 * log or reuse a terminal run.
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
   * MessageIds whose run is still LIVE (non-terminal) and therefore owns
   * its inbound message: recovery re-drives that run against the durable
   * log, so re-admitting the message to `inbox/` would dispatch a SECOND
   * run for it, colliding with the re-drive on the shared runId. Entries
   * in this set are left in `processing/` untouched; the run's eventual
   * `markConsumed` clears them. The caller computes this by reading the
   * run event logs, which live on a DIFFERENT ref (`refs/heads/main`)
   * than the claim-check subtree this operation commits to
   * (`refs/heads/events`). Empty/absent means replay every processing
   * entry (the pre-existing behaviour: recover all orphans).
   */
  ownedMessageIds?: ReadonlySet<string>;
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
        // A processing entry whose run is still live (non-terminal durable
        // log) is owned by the recovery re-drive of that same run. Re-
        // admitting the message to inbox would dispatch a SECOND run for
        // it, colliding with the re-drive on the shared runId. Leave such
        // an entry in processing untouched; the run's eventual
        // `markConsumed` clears it. Only genuinely orphaned entries (no
        // run, or a terminal run) are replayed. The run logs live on a
        // different ref, so the caller precomputes the owned set.
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
        // Re-admit the in-flight entry WITHOUT the watermark stale-reject
        // enqueueInbox applies: it was already past dedup, so a
        // below-watermark receivedAt is no reason to refuse it. Applying
        // the stale-check here would lose a legitimately in-flight
        // message after a crash. Do not tighten this.
        puts[inboxFull] = bytes;
        deletes.push(`${processingDir}${entry.name}`);
        replayedKeys.push(entry.name.slice(0, -".json".length));
      }
      return { puts, deletes };
    },
  });
  return { commitSha, replayedKeys };
}
