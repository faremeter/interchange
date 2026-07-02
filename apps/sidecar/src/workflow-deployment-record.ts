// Sidecar-local persistence of the per-deployment record needed to
// re-establish a workflow deployment across a sidecar PROCESS restart. The
// record is co-located with the deployment's workflow-run substrate at
// `${dataDir}/workflow-runs/<deploymentId>/deployment.json`, so a single
// teardown reclaims both and a boot scan can enumerate the active
// deployments beside the run state they resume.
//
// It carries only the inputs that are otherwise frame/in-memory only:
// `sources` (pinned per-step inference sources, threaded to the child via
// the spawn env and durable nowhere else), `sessionId` (inference-event
// correlation), and `hubPublicKey` (the head's deploy-pack / inbound
// verification key, recorded only in memory today). The definition itself
// lives in `assets/workflow/<definitionId>/workflow.json`, referenced by
// `definitionId`, and each step's grants live in its agent-state repo, so
// neither is duplicated here.

import { mkdir, rm, writeFile } from "node:fs/promises";
import { dirname, join as pathJoin } from "node:path";

import { type } from "arktype";

import { InferenceSource } from "@intx/types/runtime";

const RECORD_FILENAME = "deployment.json";

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
    "[string]": InferenceSource,
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
  // writes elsewhere on the sidecar and is stricter than the legacy
  // `agent.json`, which persists the same credentials at the default mode.
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
