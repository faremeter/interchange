// `CancelRequested` signing path for the per-deployment supervisor.
//
// Every CancelRequested origin flows through the supervisor's
// signing identity -- including the
// `self`-origin case where the workflow-process passes its stated
// reason to the supervisor via the control IPC and the supervisor
// wraps it into a signed event. The child has no asymmetric keypair
// of its own; routing all four origins through the same supervisor-
// signed path keeps the trust anchor inventory at one signing key
// per deployment (plus the hub's, for `hub-admin`).
//
// The supervisor owns the Ed25519 signing key (held in closure by
// the `signAsPrincipal` callback the host injects); this module
// composes the call sequence:
//   1. Build the CancelRequested event payload from the requested
//      origin and reason.
//   2. Serialize a canonical byte representation the signing
//      callback signs.
//   3. Attach the signature to the on-the-wire event and append it
//      to the workflow-run repo via the substrate handle.
// The substrate-side workflow-run kind handler enforces the
// principal-vs-origin map at push validation (a `self`/`supervisor-
// drain`/`supervisor-operator` origin must arrive carried by a
// `supervisor`-kind principal), which is the cross-check the
// supervisor's runtime-side signing keeps coherent.

import { type } from "arktype";

import type {
  RepoId,
  RepoStore as SubstrateRepoStore,
  WorkflowRunSupervisorPrincipal,
} from "@intx/hub-sessions";
import type { CancelOrigin } from "@intx/workflow";
import { hexEncode } from "@intx/types";

import type {
  PrincipalSigner,
  SignedPayload,
  WorkflowSupervisorPrincipalKind,
} from "./types";

/**
 * Path inside the workflow-run repo each `CancelRequested` event
 * lands under. Matches the layout the workflow-run kind handler
 * validates: `runs/<runId>/events/<seq>.json`.
 */
const RUNS_PREFIX = "runs";
const EVENTS_DIR = "events";

/**
 * The supervisor's stable principal kind used for every supervisor-
 * authored commit (CancelRequested for `self`/`supervisor-drain`/
 * `supervisor-operator` origins, plus drain audit frames in later
 * commits). The kind handler reads this off the substrate's per-
 * push principal and enforces the principal-vs-origin map.
 */
export const SUPERVISOR_PRINCIPAL_KIND: WorkflowSupervisorPrincipalKind =
  "supervisor";

export type CommitCancelRequestedOpts = {
  /** Substrate handle the supervisor writes through. */
  substrate: SubstrateRepoStore;
  /** Workflow-run repo for this deployment. */
  repoId: RepoId;
  /** Events ref the workflow-run repo writes to. */
  ref: string;
  /** Deployment id used to construct the supervisor principal. */
  deploymentId: string;
  /** Run id whose event log receives the CancelRequested entry. */
  runId: string;
  /** Cancellation origin from the Q3 map. */
  origin: CancelOrigin;
  /**
   * Human-readable reason. For `self`-origin requests the supervisor
   * forwards the workflow-process's stated reason verbatim; the
   * other origins carry the supervisor's own source of truth.
   */
  reason: string;
  /**
   * ISO-8601 commit timestamp the event carries. The supervisor
   * controls this so tests can pin to a deterministic value.
   */
  at: string;
  /**
   * Host-supplied per-principal signing callback. Invoked here with
   * `"supervisor"` and the canonical event-payload bytes; never with
   * the supervisor's private key visible to the supervisor module.
   */
  signAsPrincipal: PrincipalSigner;
};

/**
 * Result of a successful CancelRequested commit. The committed
 * payload (including the attached signature envelope) is surfaced so
 * callers that need to audit the exact on-disk bytes have them
 * without re-reading the repo.
 */
export type CommitCancelRequestedResult = {
  /** Substrate-assigned commit SHA the append produced. */
  commitSha: string;
  /** Per-run sequence number the append landed at. */
  seq: number;
  /** Signature the supervisor attached to the event. */
  signature: SignedPayload;
};

const EVENT_FILENAME_RE = /^(0|[1-9][0-9]*)\.json$/;

const OnDiskEnvelope = type({
  seq: "number >= 0",
  type: "string",
  "+": "ignore",
});

/**
 * Build the canonical bytes the supervisor signs for a
 * CancelRequested event. The on-wire shape carries the same fields
 * the workflow-run kind handler validates plus a `signature`
 * sub-object the supervisor populates after this byte string is
 * signed. Signing the payload *without* the signature field keeps
 * the verifier's reconstruction trivial: strip `signature` from the
 * blob, canonicalize, verify against `signature.sig`.
 */
function buildPayloadBytes(args: {
  seq: number;
  runId: string;
  reason: string;
  origin: CancelOrigin;
  at: string;
}): Uint8Array {
  const canonical = {
    type: "CancelRequested",
    seq: args.seq,
    runId: args.runId,
    at: args.at,
    reason: args.reason,
    origin: args.origin,
  };
  return new TextEncoder().encode(JSON.stringify(canonical));
}

/**
 * Commit a CancelRequested event signed by the supervisor on behalf
 * of the named origin. The `self`-origin path is identical to the
 * other supervisor origins from the substrate's perspective; the
 * caller passes the workflow-process's stated reason through.
 */
export async function commitCancelRequested(
  opts: CommitCancelRequestedOpts,
): Promise<CommitCancelRequestedResult> {
  const prefix = `${RUNS_PREFIX}/${opts.runId}/${EVENTS_DIR}/`;
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
          runId: opts.runId,
          reason: opts.reason,
          origin: opts.origin,
          at: opts.at,
        });
        const signature = await opts.signAsPrincipal(
          SUPERVISOR_PRINCIPAL_KIND,
          payloadBytes,
        );
        resolved = { seq: nextSeq, signature };
        const onDisk = {
          type: "CancelRequested",
          seq: nextSeq,
          runId: opts.runId,
          at: opts.at,
          reason: opts.reason,
          origin: opts.origin,
          signature: serializeSignedPayload(signature),
        };
        const files: Record<string, string | Uint8Array> = {};
        for (const [k, v] of existing) files[k] = v;
        files[`${prefix}${String(nextSeq)}.json`] = JSON.stringify(onDisk);
        return files;
      },
      message: `append CancelRequested ${opts.origin} for run ${opts.runId}`,
    },
  );
  if (resolved === null) {
    throw new Error(
      `supervisor cancel-signing: merge callback did not assign a sequence number for run ${opts.runId}`,
    );
  }
  const final: { seq: number; signature: SignedPayload } = resolved;
  return { commitSha, seq: final.seq, signature: final.signature };
}

/**
 * Wire shape for a SignedPayload inside a CancelRequested event
 * blob. The signature bytes are hex-encoded for JSON-safety; the
 * principal kind rides alongside so an audit-log walker can verify
 * the signature without consulting a sidecar manifest for which key
 * to load.
 */
function serializeSignedPayload(signed: SignedPayload): {
  principalKind: string;
  sig: string;
} {
  return {
    principalKind: signed.principalKind,
    sig: hexEncode(signed.sig),
  };
}

export { OnDiskEnvelope as CancelRequestedOnDiskEnvelopeForTest };
