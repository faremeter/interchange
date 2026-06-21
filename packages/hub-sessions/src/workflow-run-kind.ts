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
//     this dedup entry.
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
//     `consumed`. Other top-level names under an address fail the
//     push.
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
//   - `consumed/<messageId>.json` is immutable: a prospective commit
//     that mutates the bytes of an existing consumed entry is
//     rejected by the same prior-tree byte-equality guard used for
//     run events.
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
  type Principal,
  type RepoId,
  type RepoStore,
  type ValidatePushResult,
} from "./repo-store";

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

export type ClaimCheckEnvelope = typeof ClaimCheckEnvelope.infer;
export type ConsumedEnvelope = typeof ConsumedEnvelope.infer;

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
 * Build the (runId → events[]) map by walking the prospective tree.
 * The substrate's listDir yields names directly under the given
 * directory, so the walk is `runs/` → run-id subdirs → `events/` →
 * event filenames. Filenames outside the `<seq>.json` shape fail the
 * push.
 */
async function enumerateEventBlobs(
  listDir: (path: string) => Promise<string[]>,
): Promise<
  | { ok: true; runs: Map<string, RunEventBlob[]> }
  | { ok: false; reason: string }
> {
  const runs = new Map<string, RunEventBlob[]>();
  const runIds = await listDir(WORKFLOW_RUN_RUNS_PREFIX);
  for (const runId of runIds) {
    const runDirPath = `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}`;
    const runChildren = await listDir(runDirPath);
    const offender = runChildren.find((c) => !RUN_DIR_ALLOWED_CHILDREN.has(c));
    if (offender !== undefined) {
      return {
        ok: false,
        reason: `run directory ${runDirPath} contains unexpected entry ${JSON.stringify(offender)}; only "${WORKFLOW_RUN_EVENTS_DIR}" and "${WORKFLOW_RUN_BLOBS_DIR}" are allowed`,
      };
    }
    if (!runChildren.includes(WORKFLOW_RUN_EVENTS_DIR)) {
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
): Promise<
  { ok: true; blobs: RunBlobEntry[] } | { ok: false; reason: string }
> {
  const out: RunBlobEntry[] = [];
  const runIds = await listDir(WORKFLOW_RUN_RUNS_PREFIX);
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

async function enumerateClaimCheckBlobs(
  listDir: (path: string) => Promise<string[]>,
): Promise<
  | {
      ok: true;
      /** `addressSegment` → kind → entries */
      perAddress: Map<
        string,
        {
          decodedAddress: string;
          inbox: ClaimCheckBlob[];
          processing: ClaimCheckBlob[];
          consumed: ClaimCheckBlob[];
        }
      >;
    }
  | { ok: false; reason: string }
> {
  const perAddress = new Map<
    string,
    {
      decodedAddress: string;
      inbox: ClaimCheckBlob[];
      processing: ClaimCheckBlob[];
      consumed: ClaimCheckBlob[];
    }
  >();
  const segments = await listDir(WORKFLOW_RUN_ADDRESSES_PREFIX);
  for (const segment of segments) {
    const roundTrip = checkAddressSegmentRoundTrip(segment);
    if (!roundTrip.ok) return roundTrip;
    const addrDir = `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${segment}`;
    const children = await listDir(addrDir);
    for (const child of children) {
      if (!CLAIM_CHECK_SUBDIRS.has(child)) {
        return {
          ok: false,
          reason: `address directory ${addrDir} contains unexpected entry ${JSON.stringify(child)}; allowed: "${WORKFLOW_RUN_INBOX_DIR}", "${WORKFLOW_RUN_PROCESSING_DIR}", "${WORKFLOW_RUN_CONSUMED_DIR}"`,
        };
      }
    }
    const bucket = perAddress.get(segment) ?? {
      decodedAddress: roundTrip.decoded,
      inbox: [],
      processing: [],
      consumed: [],
    };
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
          bucket.consumed.push({
            kind: "consumed",
            addressSegment: segment,
            decodedAddress: roundTrip.decoded,
            filename,
            receivedAtFromFilename: null,
            messageIdFromFilename: messageId,
            blobPath: `${dirPath}/${filename}`,
          });
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

async function parseConsumedBlob(
  entry: ClaimCheckBlob,
  readBlob: (path: string) => Promise<Uint8Array>,
): Promise<
  { ok: true; body: ConsumedEnvelope } | { ok: false; reason: string }
> {
  let raw: Uint8Array;
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

async function checkPriorBytesImmutable(
  blobPath: string,
  readBlob: (path: string) => Promise<Uint8Array>,
  priorReadBlob: (path: string) => Promise<Uint8Array | null>,
  label: string,
): Promise<ValidatePushResult> {
  const prior = await priorReadBlob(blobPath);
  if (prior === null) return { ok: true };
  const prospective = await readBlob(blobPath);
  if (prior.byteLength !== prospective.byteLength) {
    return {
      ok: false,
      reason: `${label} ${blobPath} bytes diverge from the prior tree (lengths ${String(prior.byteLength)} vs ${String(prospective.byteLength)}); ${label} entries are immutable once written`,
    };
  }
  for (let i = 0; i < prior.byteLength; i++) {
    if (prior[i] !== prospective[i]) {
      return {
        ok: false,
        reason: `${label} ${blobPath} bytes diverge from the prior tree at offset ${String(i)}; ${label} entries are immutable once written`,
      };
    }
  }
  return { ok: true };
}

/**
 * Validate the `addresses/<urlEncoded>/{inbox,processing,consumed}`
 * subtree as a whole. The walk enforces filename shape, JSON envelope
 * structure, address round-trip, per-messageId atomicity across the
 * three queue states, consumed-blob immutability via prior-bytes
 * equality, and the inbox→processing / processing→consumed
 * transition invariants against the prior tree.
 */
async function validateClaimCheckSubtree(
  listDir: (path: string) => Promise<string[]>,
  readBlob: (path: string) => Promise<Uint8Array>,
  priorReadBlob: (path: string) => Promise<Uint8Array | null>,
  priorListDir: (path: string) => Promise<string[]>,
): Promise<ValidatePushResult> {
  const enumerated = await enumerateClaimCheckBlobs(listDir);
  if (!enumerated.ok) return enumerated;
  const priorEnumerated = await enumerateClaimCheckBlobs(priorListDir);
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

  const emptyBucket = (decodedAddress: string) => ({
    decodedAddress,
    inbox: [] as ClaimCheckBlob[],
    processing: [] as ClaimCheckBlob[],
    consumed: [] as ClaimCheckBlob[],
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
      const parsed = await parseConsumedBlob(entry, readBlob);
      if (!parsed.ok) return parsed;
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

    // Consumed entries are immutable: enforce byte-equality against
    // the prior tree for every consumed blob that already existed.
    for (const entry of bucket.consumed) {
      const immutability = await checkPriorBytesImmutable(
        entry.blobPath,
        readBlob,
        priorReadBlob,
        "consumed",
      );
      if (!immutability.ok) return immutability;
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

    // Append-only / immutability extended to the deletion direction:
    // walk every entry the prior tree carried under
    // `consumed/` and `processing/` and reject any prior path that
    // does not reappear in the prospective tree, with a per-kind
    // allowance for legitimate transitions (processing entries may
    // drop in the same commit they transition to consumed or replay
    // back to inbox). Without this walk, a prospective tree that
    // simply omits a prior entry slips past the prospective-tree
    // iteration the by-presence checks above perform.
    if (priorBucket !== undefined) {
      for (const e of priorBucket.consumed) {
        if (prospectiveConsumedPaths.has(e.blobPath)) continue;
        return {
          ok: false,
          reason: `consumed ${e.blobPath} present in the prior tree is missing from the prospective tree; consumed entries are immutable once written`,
        };
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
    priorReadBlob,
    priorListDir,
  }): Promise<ValidatePushResult> {
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

    const enumerated = await enumerateEventBlobs(listDir);
    if (!enumerated.ok) {
      logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${enumerated.reason}`;
      return { ok: false, reason: enumerated.reason };
    }

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
        }
      }
    }

    const blobsEnumerated = await enumerateRunBlobs(listDir);
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
    const priorEnumerated = await enumerateEventBlobs(priorListDir);
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
        return {
          ok: false,
          reason: `event ${e.blobPath} present in the prior tree is missing from the prospective tree; event blobs are append-only`,
        };
      }
    }
    const priorBlobsEnumerated = await enumerateRunBlobs(priorListDir);
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

    return { ok: true };
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
// Four operations layer on top of `RepoStore.writeTreePreservingPrefix`
// to give the workflow runtime a FIFO claim-check queue per address:
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
// All four route through `writeTreePreservingPrefix` with the
// per-address subtree as `preservePrefix`. The substrate serializes
// concurrent claim-check operations on the per-repo lock; the merge
// callback reads the prior address subtree directly via
// `isomorphic-git` (the substrate's `existing` parameter is a
// direct-children-only view, which does not see entries nested under
// inbox/processing/consumed), computes the new state, and returns the
// full set of files. The substrate's `clearPrefix` semantics replace
// the address subtree wholesale with the returned set, which is the
// atomic-commit guarantee these operations require.

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

function inboxPath(addressSegment: string, key: string): string {
  return `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/${WORKFLOW_RUN_INBOX_DIR}/${key}.json`;
}

function processingPath(addressSegment: string, key: string): string {
  return `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/${WORKFLOW_RUN_PROCESSING_DIR}/${key}.json`;
}

function filenameKey(receivedAt: number, messageId: string): string {
  return `${String(receivedAt)}-${messageId}`;
}

/**
 * Read the full state of one address subtree from the ref's tip via
 * `isomorphic-git`. The substrate's `writeTreePreservingPrefix` only
 * surfaces direct-child blobs; the address subtree's blobs live one
 * level deeper, so the merge callback uses this helper to assemble
 * the full pre-image inside the per-repo lock.
 *
 * Returns the bytes of every entry under
 * `addresses/<addressSegment>/{inbox,processing,consumed}/` keyed by
 * repo-root-relative path. An empty map covers the cases where the
 * repo, ref, or address subtree do not yet exist — all legitimate
 * first-write states for a brand-new claim-check operation.
 */
async function readAddressSubtree(
  repoDir: string,
  ref: string,
  addressSegment: string,
): Promise<Map<string, Uint8Array>> {
  const out = new Map<string, Uint8Array>();
  let commitSha: string;
  try {
    commitSha = await git.resolveRef({ fs, dir: repoDir, ref });
  } catch {
    return out;
  }
  const commit = await git.readCommit({ fs, dir: repoDir, oid: commitSha });
  const addrTreeOid = await resolveSubtreeOid(repoDir, commit.commit.tree, [
    WORKFLOW_RUN_ADDRESSES_PREFIX,
    addressSegment,
  ]);
  if (addrTreeOid === null) return out;
  const { tree: addrChildren } = await git.readTree({
    fs,
    dir: repoDir,
    oid: addrTreeOid,
  });
  for (const child of addrChildren) {
    if (child.type !== "tree") continue;
    if (!CLAIM_CHECK_SUBDIRS.has(child.path)) continue;
    const { tree: children } = await git.readTree({
      fs,
      dir: repoDir,
      oid: child.oid,
    });
    for (const blobEntry of children) {
      if (blobEntry.type !== "blob") continue;
      const { blob } = await git.readBlob({
        fs,
        dir: repoDir,
        oid: blobEntry.oid,
      });
      const blobPath = `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/${child.path}/${blobEntry.path}`;
      out.set(blobPath, blob);
    }
  }
  return out;
}

async function resolveSubtreeOid(
  repoDir: string,
  rootTreeOid: string,
  segments: readonly string[],
): Promise<string | null> {
  let current = rootTreeOid;
  for (const segment of segments) {
    const { tree } = await git.readTree({ fs, dir: repoDir, oid: current });
    const entry = tree.find((e) => e.path === segment);
    if (entry === undefined) return null;
    if (entry.type !== "tree") return null;
    current = entry.oid;
  }
  return current;
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
  const repoDir = store.getRepoDir(repoId);
  const inboxKey = filenameKey(args.receivedAt, args.messageId);
  const envelope: ClaimCheckEnvelope = {
    messageId: args.messageId,
    receivedAt: args.receivedAt,
    address: args.address,
    mailAuditRef: args.mailAuditRef,
    ...(args.rawMessage !== undefined ? { rawMessage: args.rawMessage } : {}),
  };
  const newInboxPath = inboxPath(addressSegment, inboxKey);
  const { commitSha } = await store.writeTreePreservingPrefix(
    principal,
    repoId,
    ref,
    {
      preservePrefix: `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/`,
      merge: async () => {
        const existing = await readAddressSubtree(repoDir, ref, addressSegment);
        const inboxPrefix = `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/${WORKFLOW_RUN_INBOX_DIR}/`;
        const processingPrefix = `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/${WORKFLOW_RUN_PROCESSING_DIR}/`;
        const consumedPrefix = `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/${WORKFLOW_RUN_CONSUMED_DIR}/`;
        const messageIdSuffix = `-${args.messageId}.json`;
        for (const [blobPath] of existing) {
          if (blobPath === newInboxPath) {
            throw new Error(
              `claim_check_duplicate_inbox: ${newInboxPath} already exists`,
            );
          }
          if (blobPath.startsWith(consumedPrefix)) {
            const fname = blobPath.slice(consumedPrefix.length);
            if (fname === `${args.messageId}.json`) {
              throw new Error(
                `claim_check_already_consumed: address ${args.address} message ${args.messageId} is already in the consumed dedup index`,
              );
            }
          }
          if (blobPath.startsWith(processingPrefix)) {
            const fname = blobPath.slice(processingPrefix.length);
            if (fname.endsWith(messageIdSuffix)) {
              throw new Error(
                `claim_check_already_processing: address ${args.address} message ${args.messageId} is currently in processing`,
              );
            }
          }
          // Reject a second inbox entry for the same messageId at a
          // different receivedAt. The validatePush atomicity check
          // also catches this on the commit path, but the inbox
          // scan here surfaces the rejection at the API boundary so
          // the caller sees a precise error (rather than a generic
          // tree-validation failure) and the bad tree never reaches
          // the substrate.
          if (blobPath.startsWith(inboxPrefix)) {
            const fname = blobPath.slice(inboxPrefix.length);
            if (fname.endsWith(messageIdSuffix)) {
              throw new Error(
                `claim_check_already_inbox: address ${args.address} message ${args.messageId} is already in the inbox at ${blobPath}`,
              );
            }
          }
        }
        const files: Record<string, string | Uint8Array> = {};
        for (const [blobPath, bytes] of existing) {
          files[blobPath] = bytes;
        }
        files[newInboxPath] = utf8(JSON.stringify(envelope));
        return files;
      },
      message: `enqueue inbox ${args.address} ${args.messageId}`,
    },
  );
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
  const repoDir = store.getRepoDir(repoId);
  let dequeued: { key: string; envelope: ClaimCheckEnvelope } | null = null;
  const { commitSha } = await store.writeTreePreservingPrefix(
    principal,
    repoId,
    ref,
    {
      preservePrefix: `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/`,
      merge: async () => {
        const existing = await readAddressSubtree(repoDir, ref, addressSegment);
        const inboxPrefix = `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/${WORKFLOW_RUN_INBOX_DIR}/`;
        // Parse each inbox path's filename and sort by numeric
        // receivedAt with a messageId tiebreak. A raw string sort
        // would not agree with chronological order when receivedAt
        // values have non-uniform digit widths.
        type InboxCandidate = {
          path: string;
          receivedAt: number;
          messageId: string;
        };
        const candidates: InboxCandidate[] = [];
        for (const p of existing.keys()) {
          if (!p.startsWith(inboxPrefix)) continue;
          const fname = p.slice(inboxPrefix.length);
          const m = QUEUE_FILENAME_RE.exec(fname);
          if (m === null || m[1] === undefined || m[2] === undefined) {
            throw new Error(`claim_check_invalid_inbox_filename: ${p}`);
          }
          candidates.push({
            path: p,
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
        if (candidates.length === 0) {
          dequeued = null;
          return Object.fromEntries(existing);
        }
        const first = candidates[0];
        if (first === undefined) throw new Error("unreachable");
        const firstPath = first.path;
        const bytes = existing.get(firstPath);
        if (bytes === undefined) throw new Error("unreachable");
        const fname = firstPath.slice(inboxPrefix.length);
        const key = fname.slice(0, -".json".length);
        const envelope = decodeQueueEnvelopeOrThrow(bytes, firstPath);
        dequeued = { key, envelope };
        const files: Record<string, string | Uint8Array> = {};
        for (const [blobPath, blobBytes] of existing) {
          if (blobPath === firstPath) continue;
          files[blobPath] = blobBytes;
        }
        files[processingPath(addressSegment, key)] = bytes;
        return files;
      },
      message: `dequeue ${address}`,
    },
  );
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
 * `addresses/<seg>/processing/`. The substrate materializes every
 * claim-check commit into the repo's working tree (each
 * `writeTreePreservingPrefix` resets the working tree to the ref tip),
 * so a read issued after `dequeueToProcessing` committed -- which is
 * exactly when the supervisor forwards `trigger.fired` -- observes the
 * processing entry. Reading the working tree (rather than walking the
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
};

export type MarkConsumedResult = {
  commitSha: string;
  envelope: ConsumedEnvelope;
};

/**
 * Atomically remove the processing entry for `messageId` at `address`
 * and write the canonical `consumed/<messageId>.json` dedup index
 * entry. The caller is expected to have called
 * `dequeueToProcessing` for this messageId; calling `markConsumed`
 * without a matching processing entry throws.
 *
 * The consumed envelope preserves the original `receivedAt` and
 * `mailAuditRef` from the processing entry so the dedup index
 * doubles as an audit record.
 */
export async function markConsumed(
  store: RepoStore,
  principal: Principal,
  repoId: RepoId,
  args: MarkConsumedArgs,
): Promise<MarkConsumedResult> {
  const addressSegment = addressSegmentFor(args.address);
  const ref = claimCheckCommitRef();
  const repoDir = store.getRepoDir(repoId);
  let consumedEnvelope: ConsumedEnvelope | null = null;
  const { commitSha } = await store.writeTreePreservingPrefix(
    principal,
    repoId,
    ref,
    {
      preservePrefix: `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/`,
      merge: async () => {
        const existing = await readAddressSubtree(repoDir, ref, addressSegment);
        const processingPrefix = `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/${WORKFLOW_RUN_PROCESSING_DIR}/`;
        const consumedPrefix = `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/${WORKFLOW_RUN_CONSUMED_DIR}/`;
        const consumedFname = `${args.messageId}.json`;
        const consumedFull = `${consumedPrefix}${consumedFname}`;
        if (existing.has(consumedFull)) {
          throw new Error(
            `claim_check_already_consumed: ${consumedFull} already in the dedup index`,
          );
        }
        let processingFull: string | null = null;
        let processingBytes: Uint8Array | null = null;
        for (const [blobPath, bytes] of existing) {
          if (!blobPath.startsWith(processingPrefix)) continue;
          const fname = blobPath.slice(processingPrefix.length);
          if (fname.endsWith(`-${args.messageId}.json`)) {
            processingFull = blobPath;
            processingBytes = bytes;
            break;
          }
        }
        if (processingFull === null || processingBytes === null) {
          throw new Error(
            `claim_check_processing_not_found: address ${args.address} message ${args.messageId} has no processing entry`,
          );
        }
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
        const files: Record<string, string | Uint8Array> = {};
        for (const [blobPath, blobBytes] of existing) {
          if (blobPath === processingFull) continue;
          files[blobPath] = blobBytes;
        }
        files[consumedFull] = utf8(JSON.stringify(envelope));
        return files;
      },
      message: `consume ${args.address} ${args.messageId}`,
    },
  );
  if (consumedEnvelope === null) throw new Error("unreachable");
  const captured: ConsumedEnvelope = consumedEnvelope;
  return { commitSha, envelope: captured };
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
 */
export async function replayProcessingToInbox(
  store: RepoStore,
  principal: Principal,
  repoId: RepoId,
  address: string,
): Promise<ReplayProcessingToInboxResult> {
  const addressSegment = addressSegmentFor(address);
  const ref = claimCheckCommitRef();
  const repoDir = store.getRepoDir(repoId);
  const replayedKeys: string[] = [];
  const { commitSha } = await store.writeTreePreservingPrefix(
    principal,
    repoId,
    ref,
    {
      preservePrefix: `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/`,
      merge: async () => {
        const existing = await readAddressSubtree(repoDir, ref, addressSegment);
        const processingPrefix = `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/${WORKFLOW_RUN_PROCESSING_DIR}/`;
        const inboxPrefix = `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegment}/${WORKFLOW_RUN_INBOX_DIR}/`;
        const files: Record<string, string | Uint8Array> = {};
        for (const [blobPath, bytes] of existing) {
          if (blobPath.startsWith(processingPrefix)) {
            const fname = blobPath.slice(processingPrefix.length);
            const key = fname.slice(0, -".json".length);
            const inboxFull = `${inboxPrefix}${fname}`;
            if (existing.has(inboxFull)) {
              throw new Error(
                `claim_check_replay_collision: ${inboxFull} already exists; cannot replay processing entry`,
              );
            }
            files[inboxFull] = bytes;
            replayedKeys.push(key);
            continue;
          }
          files[blobPath] = bytes;
        }
        return files;
      },
      message: `replay processing ${address}`,
    },
  );
  return { commitSha, replayedKeys };
}
