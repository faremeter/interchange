// Sidecar-local persistence of the per-run record needed to re-establish a
// workflow run across a sidecar PROCESS restart. Co-located with the run's
// workflow-run substrate at `${dataDir}/workflow-runs/<runId>/deployment.json`,
// so a single teardown reclaims both and a boot scan enumerates the active
// runs beside the run state they resume.
//
// It carries the inputs that are otherwise frame/in-memory only: `sources`
// (each step's inference-source failover chain, durable nowhere else),
// `sessionId`, `hubPublicKey`, `approvedWireHash` (so a restore re-spawn feeds
// the child the same re-verify anchor), and -- for a source-ref deployment --
// the `sourceRef` pin a restore needs to re-materialize the pinned code. The
// definition is re-materialized from that closure and each step's grants live
// in its agent-state repo, so neither is duplicated here.

import { mkdir, readdir, readFile, rm } from "node:fs/promises";
import { dirname, join as pathJoin } from "node:path";

import { type } from "arktype";

import { getLogger } from "@intx/log";
import { credentialAad, type CredentialCipher } from "@intx/types";
import { InferenceSource } from "@intx/types/runtime";
import { CredentialDelivery, SourceRefPin } from "@intx/types/sidecar";

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
// `version` is 2 for the unified credential format: `sources`/`bodySources`
// are non-secret config carrying a `credentialId` per source, and the secret
// material lives once in `credentials`, each material's secret sealed under
// the sidecar cipher. A pre-unification record (inline per-source secrets)
// fails this schema and is soft-skipped at the scan boundary.
const workflowRunRecordBase = {
  version: "1 | 2",
  agentAddress: "string > 0",
  definitionId: "string > 0",
  // Top-level per-step inference sources. Non-secret: each source references a
  // credential by id, with no inline secret.
  sources: {
    "[string]": InferenceSource.array().atLeastLength(1),
  },
  // Per spawned-body inference-source config, keyed by the body's definition id
  // (onTrigger or childWorkflow; a nested loop body folds its sources into its
  // container's table). Non-secret, like `sources`; absent for a deployment
  // with no bodies.
  "bodySources?": {
    "[string]": {
      "[string]": InferenceSource.array().atLeastLength(1),
    },
  },
  // The run's credential-material cell: the ONE at-rest home for every secret,
  // inference and tool alike. Only each material's `secret` is sealed under the
  // sidecar cipher; the rest rides in the clear. Restored and re-delivered to
  // the child on the pre-trigger barrier.
  "credentials?": CredentialDelivery,
  "sessionId?": "string > 0",
  "hubPublicKey?": "string > 0",
} as const;

/**
 * The on-disk deployment record. `version` guards the schema shape so a future
 * reader can reject or migrate a stale record rather than parse it blindly.
 * Validated at read time (the boot scan) at the trust boundary.
 *
 * Source-ref is the only deploy lineage, so the record REQUIRES a `sourceRef`
 * pin (so a restore can re-materialize the pinned closure) AND
 * `approvedWireHash` (so the restored child re-verifies the evaluated closure
 * against the hub-approved pin, not a sidecar recompute). A record missing
 * either fails validation at the scan boundary and is soft-skipped as
 * corruption.
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
  // persisted here) and `closure` is the frozen dependency tree. Plain
  // strings, no secrets.
  sourceRef: SourceRefPin,
});
export type WorkflowRunRecord = typeof WorkflowRunRecord.infer;

function recordPath(dataDir: string, runId: string): string {
  return pathJoin(dataDir, "workflow-runs", runId, RECORD_FILENAME);
}

// The AAD column binding a credential's sealed secret to its id, so ciphertext
// cannot be swapped between credentials or runs and still decrypt.
function credentialSecretColumn(credentialId: string): string {
  return `credential:${credentialId}:secret`;
}

// Transform each credential material's `secret` under `transform` (encrypt on
// write, decrypt on scan), binding each to its run and credential; the
// non-secret fields ride in the clear. Returns a fresh delivery so the
// caller's in-memory record is not mutated. A decrypt failure throws, which
// the scan caller treats as corruption.
async function transformDeliveryMaterials(
  delivery: CredentialDelivery,
  runId: string,
  transform: (secret: string, aad: string) => Promise<string>,
): Promise<CredentialDelivery> {
  return {
    bindings: delivery.bindings,
    materials: await Promise.all(
      delivery.materials.map(async (material) => ({
        ...material,
        secret: await transform(
          material.secret,
          credentialAad(runId, credentialSecretColumn(material.credentialId)),
        ),
      })),
    ),
  };
}

/**
 * Persist a run record. Written after the run's slug is claimed and before
 * the child is spawned, so a crash mid-spawn leaves a record the boot scan
 * re-drives. Idempotent: overwrites any existing record for the same run.
 *
 * Every credential secret is sealed under the sidecar `cipher` before it
 * touches disk (version 2); the `sources`/`bodySources` config is non-secret
 * and rides in the clear.
 */
export async function writeWorkflowRunRecord(
  dataDir: string,
  runId: string,
  record: WorkflowRunRecord,
  cipher: CredentialCipher,
): Promise<void> {
  const path = recordPath(dataDir, runId);
  await mkdir(dirname(path), { recursive: true });
  const sealed: WorkflowRunRecord = {
    ...record,
    version: 2,
    ...(record.credentials !== undefined
      ? {
          credentials: await transformDeliveryMaterials(
            record.credentials,
            runId,
            (secret, aad) => cipher.encrypt(secret, aad),
          ),
        }
      : {}),
  };
  // Atomic + durable: this is the sole restore source for the run's credential
  // material, and a rotation overwrites the record in place, so an interrupted
  // write must never expose a torn record. Owner-only (0o600): the record names
  // each source's provider and the deployment's identity, so it stays off a
  // shared host's world-readable set.
  await writeFileAtomicDurable(path, JSON.stringify(sealed, null, 2), {
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
 * `deployment.json`, unparseable JSON, or a schema-invalid record is logged
 * and skipped rather than wedging the whole boot. An absent `workflow-runs/`
 * directory is the first-boot case and yields an empty list, not an error.
 *
 * The returned `runId` is the directory name; the caller cross-checks it
 * against the record's own address before trusting it.
 */
export async function scanWorkflowRunRecords(
  dataDir: string,
  cipher: CredentialCipher,
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
      // A run directory with no record: a crash between mkdir and the write,
      // or a record already reclaimed. Nothing to restore -- skip.
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
    // Unseal the version-2 record's `credentials` cell under the sidecar
    // cipher. A decrypt failure -- a rotated/wrong key or a tampered blob --
    // is treated as corruption: soft-skip the WHOLE run so one undecryptable
    // run does not wedge the boot scan. A record with no `credentials` cell
    // needs no unseal.
    let restored = record;
    if (record.version === 2 && record.credentials !== undefined) {
      try {
        restored = {
          ...record,
          credentials: await transformDeliveryMaterials(
            record.credentials,
            runId,
            (secret, aad) => cipher.decrypt(secret, aad),
          ),
        };
      } catch (cause) {
        const reason = cause instanceof Error ? cause.message : String(cause);
        logger.warn`skipping workflow-runs/${runId}: sealed credential decrypt failed: ${reason}`;
        continue;
      }
    }
    scanned.push({ runId, record: restored });
  }
  return scanned;
}
