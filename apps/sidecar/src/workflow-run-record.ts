// Sidecar-local persistence of the per-run record needed to re-establish a
// workflow run across a sidecar PROCESS restart. The record is co-located
// with the run's workflow-run substrate at
// `${dataDir}/workflow-runs/<runId>/deployment.json`, so a single
// teardown reclaims both and a boot scan can enumerate the active runs
// beside the run state they resume.
//
// It carries the inputs that are otherwise frame/in-memory only: `sources`
// (each step's ordered inference-source failover chain, threaded to the child
// via the spawn env and durable nowhere else), `sessionId` (inference-event
// correlation), `hubPublicKey` (the head's deploy-pack / inbound verification
// key), `approvedWireHash` + `referencedDefinitionHashes` (the hub-approved
// hashes the deploy frame carried, so a restore re-spawn feeds the child the
// same re-verify anchors without recomputing them), `lineage`, and -- for a
// source-ref deployment -- the `sourceRef` pin (source + frozen closure) a
// restore needs to re-materialize and re-evaluate the pinned code. The
// definition itself lives
// in `assets/workflow/<definitionId>/workflow.json`, referenced by
// `definitionId`, and each step's grants live in its agent-state repo, so
// neither is duplicated here.

import { mkdir, readdir, readFile, rm } from "node:fs/promises";
import { dirname, join as pathJoin } from "node:path";

import { type } from "arktype";

import { getLogger } from "@intx/log";
import { InferenceSource } from "@intx/types/runtime";
import { SourceRefPin } from "@intx/types/sidecar";

import { writeFileAtomicDurable } from "./atomic-write";

const logger = getLogger(["interchange", "sidecar", "workflow-run-record"]);

const RECORD_FILENAME = "deployment.json";

/** True for a `node:fs` rejection whose `code` is `ENOENT`. */
function isENOENT(cause: unknown): boolean {
  return (
    cause instanceof Error &&
    "code" in cause &&
    (cause as { code: unknown }).code === "ENOENT"
  );
}

// The base fields every run record carries, spread into the full schema below.
const workflowRunRecordBase = {
  version: "1",
  agentAddress: "string > 0",
  definitionId: "string > 0",
  sources: {
    "[string]": InferenceSource.array().atLeastLength(1),
  },
  "sessionId?": "string > 0",
  "hubPublicKey?": "string > 0",
  // The hub-approved wire hash per referenced onTrigger body id, from the
  // deploy frame's `referencedDefinitions`. A boot-time restore re-threads
  // these into the child's spawn env so the onTrigger-body re-verify barrier
  // holds across a restart, rather than the body spawn failing closed for want
  // of a hash. The hash originated out-of-band on the signed deploy frame (not
  // from the on-disk body bytes), matching how the top-level `approvedWireHash`
  // is carried across restart. Omitted when the deployment has no bodies.
  "referencedDefinitionHashes?": {
    "[string]": "string > 0",
  },
} as const;

/**
 * The on-disk deployment record. `version` guards the schema shape so a future
 * reader can reject or migrate a stale record rather than parse it blindly.
 * Validated at read time (the boot scan) at the trust boundary.
 *
 * Source-ref is the only deploy lineage, so the record REQUIRES a `sourceRef`
 * pin (its co-required source + frozen closure, so a restore can re-materialize
 * the pinned closure) AND `approvedWireHash` (so the restored child re-verifies
 * the evaluated closure against the hub-approved pin rather than against a
 * sidecar recompute of the inert projection -- the latter would collapse the
 * out-of-band-pin property the barrier exists for). A record missing either --
 * including a legacy live-authored record written before this collapse -- fails
 * validation at the scan boundary and is soft-skipped as corruption, so the
 * restore loop needs no bespoke source-ref guard.
 */
export const WorkflowRunRecord = type({
  ...workflowRunRecordBase,
  lineage: "'source-ref'",
  // The hub-approved wire hash the restored child re-verifies the evaluated
  // closure against.
  approvedWireHash: "string > 0",
  // The source-ref pin a restore re-runs `applyFrozenWorkflowClosure` with:
  // `source` names the registry the definition package is published to (its
  // token is resolved from the sidecar's env at apply time, so no secret is
  // persisted here) and `closure` is the frozen dependency tree (concrete
  // versions + integrity SRIs) -- plain strings, no secrets. Both rode the
  // signed frame and co-travel (see `SourceRefPin`).
  sourceRef: SourceRefPin,
});
export type WorkflowRunRecord = typeof WorkflowRunRecord.infer;

function recordPath(dataDir: string, runId: string): string {
  return pathJoin(dataDir, "workflow-runs", runId, RECORD_FILENAME);
}

/**
 * Persist a run record. Written after the run's slug is claimed and before
 * the child is spawned, so a crash mid-spawn leaves a record the boot scan
 * re-drives. Idempotent: it overwrites any existing record for the same run.
 */
export async function writeWorkflowRunRecord(
  dataDir: string,
  runId: string,
  record: WorkflowRunRecord,
): Promise<void> {
  const path = recordPath(dataDir, runId);
  await mkdir(dirname(path), { recursive: true });
  // Atomic + durable: this is the sole restore source for the
  // run's `sources`/`hubPublicKey`, and a rotation overwrites the
  // existing record in place, so an interrupted write must never expose a
  // torn record the boot scan would then skip. Owner-only (0o600): the
  // record embeds each source's `apiKey`, so it must not be world-readable
  // on a shared host, matching the private-key writes elsewhere on the
  // sidecar.
  await writeFileAtomicDurable(path, JSON.stringify(record, null, 2), {
    mode: 0o600,
  });
}

/**
 * Remove a run record. Called on undeploy and on a soft-failed deploy so a
 * torn-down or never-completed run is not restored on the next boot. A
 * missing record is not an error (`force`).
 */
export async function deleteWorkflowRunRecord(
  dataDir: string,
  runId: string,
): Promise<void> {
  await rm(recordPath(dataDir, runId), { force: true });
}

/** A restorable run: its directory-derived id plus the validated record. */
export interface ScannedWorkflowRun {
  /** The `workflow-runs/<runId>` directory name the record was found under. */
  runId: string;
  record: WorkflowRunRecord;
}

/**
 * Enumerate the persisted run records under `workflow-runs/` so a boot-time
 * restore can re-establish each run. Soft-fails per record: a missing
 * `deployment.json`, unparseable JSON, or a record that fails schema
 * validation is logged and skipped rather than wedging the whole boot -- one
 * corrupt record must not strand every other run. An absent `workflow-runs/`
 * directory is the legitimate first-boot case and yields an empty list, not
 * an error.
 *
 * The returned `runId` is the directory name; the caller cross-checks it
 * against the record's own address before trusting it.
 */
export async function scanWorkflowRunRecords(
  dataDir: string,
): Promise<ScannedWorkflowRun[]> {
  const runsDir = pathJoin(dataDir, "workflow-runs");
  let entries;
  try {
    entries = await readdir(runsDir, { withFileTypes: true });
  } catch (cause) {
    if (isENOENT(cause)) return [];
    throw cause;
  }

  const scanned: ScannedWorkflowRun[] = [];
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const runId = entry.name;
    const path = recordPath(dataDir, runId);

    let raw: string;
    try {
      raw = await readFile(path, "utf8");
    } catch (cause) {
      // A run directory with no record: a crash between mkdir and the record
      // write, or a run whose record was already reclaimed. Nothing to
      // restore from -- skip.
      if (isENOENT(cause)) {
        logger.warn`skipping workflow-runs/${runId}: no ${RECORD_FILENAME} to restore from`;
        continue;
      }
      throw cause;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch (cause) {
      const reason = cause instanceof Error ? cause.message : String(cause);
      logger.warn`skipping workflow-runs/${runId}: ${RECORD_FILENAME} is not valid JSON: ${reason}`;
      continue;
    }

    const record = WorkflowRunRecord(parsed);
    if (record instanceof type.errors) {
      logger.warn`skipping workflow-runs/${runId}: ${RECORD_FILENAME} failed validation: ${record.summary}`;
      continue;
    }
    scanned.push({ runId, record });
  }
  return scanned;
}
