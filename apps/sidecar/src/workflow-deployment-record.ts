// Sidecar-local persistence of the per-deployment record needed to
// re-establish a workflow deployment across a sidecar PROCESS restart. The
// record is co-located with the deployment's workflow-run substrate at
// `${dataDir}/workflow-runs/<deploymentId>/deployment.json`, so a single
// teardown reclaims both and a boot scan can enumerate the active
// deployments beside the run state they resume.
//
// It carries only the inputs that are otherwise frame/in-memory only:
// `sources` (each step's ordered inference-source failover chain, threaded to
// the child via the spawn env and durable nowhere else), `sessionId`
// (inference-event
// correlation), and `hubPublicKey` (the head's deploy-pack / inbound
// verification key, recorded only in memory today). The definition itself
// lives in `assets/workflow/<definitionId>/workflow.json`, referenced by
// `definitionId`, and each step's grants live in its agent-state repo, so
// neither is duplicated here.

import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join as pathJoin } from "node:path";

import { type } from "arktype";

import { getLogger } from "@intx/log";
import { InferenceSource } from "@intx/types/runtime";

const logger = getLogger([
  "interchange",
  "sidecar",
  "workflow-deployment-record",
]);

const RECORD_FILENAME = "deployment.json";

/** True for a `node:fs` rejection whose `code` is `ENOENT`. */
function isENOENT(cause: unknown): boolean {
  return (
    cause instanceof Error &&
    "code" in cause &&
    (cause as { code: unknown }).code === "ENOENT"
  );
}

/**
 * The on-disk deployment record. `version` guards the schema shape so a
 * future reader can reject or migrate a stale record rather than parse it
 * blindly. Validated at read time (the boot scan) at the trust boundary.
 */
export const WorkflowDeploymentRecord = type({
  version: "1",
  agentAddress: "string > 0",
  definitionId: "string > 0",
  sources: {
    "[string]": InferenceSource.array().atLeastLength(1),
  },
  "sessionId?": "string > 0",
  "hubPublicKey?": "string > 0",
});
export type WorkflowDeploymentRecord = typeof WorkflowDeploymentRecord.infer;

function recordPath(dataDir: string, deploymentId: string): string {
  return pathJoin(dataDir, "workflow-runs", deploymentId, RECORD_FILENAME);
}

/**
 * Persist a deployment record. Written after the deployment's slug is
 * claimed and before the child is spawned, so a crash mid-spawn leaves a
 * record the boot scan re-drives. Idempotent: it overwrites any existing
 * record for the same deployment.
 */
export async function writeWorkflowDeploymentRecord(
  dataDir: string,
  deploymentId: string,
  record: WorkflowDeploymentRecord,
): Promise<void> {
  const path = recordPath(dataDir, deploymentId);
  await mkdir(dirname(path), { recursive: true });
  // Owner-only (0o600): the record embeds each source's `apiKey`, so it must
  // not be world-readable on a shared host. This matches the private-key
  // writes elsewhere on the sidecar.
  await writeFile(path, JSON.stringify(record, null, 2), {
    encoding: "utf8",
    mode: 0o600,
  });
}

/**
 * Remove a deployment record. Called on undeploy and on a soft-failed
 * deploy so a torn-down or never-completed deployment is not restored on
 * the next boot. A missing record is not an error (`force`).
 */
export async function deleteWorkflowDeploymentRecord(
  dataDir: string,
  deploymentId: string,
): Promise<void> {
  await rm(recordPath(dataDir, deploymentId), { force: true });
}

/** A restorable deployment: its directory-derived id plus the validated record. */
export interface ScannedWorkflowDeployment {
  /** The `workflow-runs/<deploymentId>` directory name the record was found under. */
  deploymentId: string;
  record: WorkflowDeploymentRecord;
}

/**
 * Enumerate the persisted deployment records under `workflow-runs/` so a
 * boot-time restore can re-establish each deployment. Soft-fails per record:
 * a missing `deployment.json`, unparseable JSON, or a record that fails schema
 * validation is logged and skipped rather than wedging the whole boot -- one
 * corrupt record must not strand every other deployment. An absent
 * `workflow-runs/` directory is the legitimate first-boot case and yields an
 * empty list, not an error.
 *
 * The returned `deploymentId` is the directory name; the caller cross-checks it
 * against the record's own address before trusting it.
 */
export async function scanWorkflowDeploymentRecords(
  dataDir: string,
): Promise<ScannedWorkflowDeployment[]> {
  const runsDir = pathJoin(dataDir, "workflow-runs");
  let entries;
  try {
    entries = await readdir(runsDir, { withFileTypes: true });
  } catch (cause) {
    if (isENOENT(cause)) return [];
    throw cause;
  }

  const scanned: ScannedWorkflowDeployment[] = [];
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const deploymentId = entry.name;
    const path = recordPath(dataDir, deploymentId);

    let raw: string;
    try {
      raw = await readFile(path, "utf8");
    } catch (cause) {
      // A run directory with no record: a crash between mkdir and the record
      // write, or a run whose record was already reclaimed. Nothing to
      // restore from -- skip.
      if (isENOENT(cause)) {
        logger.warn`skipping workflow-runs/${deploymentId}: no ${RECORD_FILENAME} to restore from`;
        continue;
      }
      throw cause;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch (cause) {
      const reason = cause instanceof Error ? cause.message : String(cause);
      logger.warn`skipping workflow-runs/${deploymentId}: ${RECORD_FILENAME} is not valid JSON: ${reason}`;
      continue;
    }

    const record = WorkflowDeploymentRecord(parsed);
    if (record instanceof type.errors) {
      logger.warn`skipping workflow-runs/${deploymentId}: ${RECORD_FILENAME} failed validation: ${record.summary}`;
      continue;
    }
    scanned.push({ deploymentId, record });
  }
  return scanned;
}
