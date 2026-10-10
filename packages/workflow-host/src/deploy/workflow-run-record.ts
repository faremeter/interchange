// Sidecar-local persistence of the per-run record needed to re-establish a
// workflow run across a sidecar PROCESS restart. The record is co-located
// with the run's workflow-run substrate at
// `${dataDir}/workflow-runs/<runId>/deployment.json`, so a single
// teardown reclaims both and a boot scan can enumerate the active runs
// beside the run state they resume.
//
// It carries the inputs that are otherwise frame/in-memory only: `generation`
// (the allocation generation of the incarnation the record restores, which
// the sidecar reports to the Hub and stamps on what the deployment sends),
// `sources` (each step's ordered inference-source failover chain, threaded to
// the child via the spawn env and durable nowhere else), `sessionId` (inference-event
// correlation), `hubPublicKey` (the head's deploy-pack / inbound verification
// key), `approvedWireHash` (the hub-approved wire hash the deploy frame
// carried, so a restore re-spawn feeds the child the same re-verify anchor
// without recomputing it), `lineage`, and -- for a source-ref deployment --
// the `sourceRef` pin (source + frozen closure) a restore needs to
// re-materialize and re-evaluate the pinned code. The definition itself is
// re-materialized from that `sourceRef` closure on restore, and each step's
// grants live in its agent-state repo, so neither is duplicated here.

import { mkdir, readdir, readFile, rm } from "node:fs/promises";
import { dirname, join as pathJoin, resolve } from "node:path";

import { type } from "arktype";

import { getLogger } from "@intx/log";
import { credentialAad, type CredentialCipher } from "@intx/types";
import { InferenceSource } from "@intx/types/runtime";
import { CredentialDelivery } from "@intx/types/credential-delivery";
import { Generation, SourceRefPin } from "@intx/types/sidecar";

import { writeFileAtomicDurable } from "./atomic-write";

const logger = getLogger(["interchange", "sidecar", "workflow-run-record"]);

export const WORKFLOW_RUN_RECORD_FILENAME = "deployment.json";

/** True for a `node:fs` rejection whose `code` is `ENOENT`. */
function isENOENT(cause: unknown): boolean {
  return (
    cause instanceof Error &&
    "code" in cause &&
    (cause as { code: unknown }).code === "ENOENT"
  );
}

// The base fields every run record carries, spread into the full schema below.
// `version` is 2 for the unified credential format: `sources`/`bodySources` are
// non-secret config carrying a `credentialId` per source, and the run's secret
// material lives once in `credentials` (the delivery cell) with each material's
// secret sealed under the sidecar cipher. A pre-unification record (inline
// per-source secrets) does not satisfy this schema and is soft-skipped at the
// scan boundary, so the sidecar no longer reports its deployment and the Hub
// fails it.
const workflowRunRecordBase = {
  version: "1 | 2",
  agentAddress: "string > 0",
  // A record without it cannot say which incarnation it restores, so it
  // fails validation and is skipped rather than guessed.
  generation: Generation,
  definitionId: "string > 0",
  // Top-level per-step inference sources. Non-secret config only: each source
  // carries a `credentialId` referencing an entry in `credentials`, no inline
  // secret, so this rides in the clear.
  sources: {
    "[string]": InferenceSource.array().atLeastLength(1),
  },
  // Per spawned-body inference-source config, keyed by the body's definition id
  // (an onTrigger section or a childWorkflow child; a nested loop body folds its
  // step sources into its container's table, so it is not a separate entry).
  // Non-secret, like `sources`: each carries a `credentialId` into `credentials`.
  // Optional: a deployment with no bodies carries none.
  "bodySources?": {
    "[string]": {
      "[string]": InferenceSource.array().atLeastLength(1),
    },
  },
  // The run's credential-material cell: the ONE at-rest home for every secret,
  // inference and tool alike. `bindings` and each material's `credentialId`/
  // `providerKey`/`origin` ride in the clear; only each material's `secret` is
  // sealed under the sidecar cipher (see `transformDeliveryMaterials`). Optional:
  // a deployment that binds no credentials and whose sources need none carries
  // none. Restored + re-delivered to the child on the pre-trigger barrier.
  "credentials?": CredentialDelivery,
  "sessionId?": "string > 0",
  "hubPublicKey?": "string > 0",
  // A deployment that no longer runs keeps its record until the Hub undeploys
  // it, marked so a restart reports it instead of spawning it: `stopped`, with
  // the error that ended it unless the Hub stopped it, or `tearing-down` when
  // its undeploy did not finish.
  "state?": "'stopped' | 'tearing-down'",
  "error?": "string > 0",
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
  return pathJoin(
    dataDir,
    "workflow-runs",
    runId,
    WORKFLOW_RUN_RECORD_FILENAME,
  );
}

// Source updates, lifecycle marks and deletion share the record's read/write
// boundary. Atomic rename alone cannot protect a read-modify-write from a
// concurrent writer. Different deployments keep independent queues.
const recordOperations = new Map<string, Promise<void>>();

/**
 * Serialize an operation with this process's mutations of a deployment record.
 * The callback receives the record's absolute path and must not acquire the
 * same record queue again.
 */
export async function withWorkflowRunRecord<T>(
  dataDir: string,
  runId: string,
  operation: (path: string) => Promise<T>,
): Promise<T> {
  const path = resolve(recordPath(dataDir, runId));
  const previous = recordOperations.get(path) ?? Promise.resolve();
  const result = previous.then(() => operation(path));
  const tail = result.then(
    () => undefined,
    () => undefined,
  );
  recordOperations.set(path, tail);
  try {
    return await result;
  } finally {
    if (recordOperations.get(path) === tail) recordOperations.delete(path);
  }
}

// The AAD column binding a credential's sealed secret to its id, so a ciphertext
// cannot be swapped between credentials (or between runs) and still decrypt.
function credentialSecretColumn(credentialId: string): string {
  return `credential:${credentialId}:secret`;
}

// Transform each credential material's `secret` in the delivery under
// `transform` (encrypt on write, decrypt on scan), binding each to its run and
// credential. The non-secret fields -- each material's `credentialId`/
// `providerKey`/`origin` and the whole `bindings` array -- ride in the clear.
// Returns a fresh delivery so the caller's in-memory record is not mutated. A
// decrypt failure (a rotated/wrong key or a tampered blob) throws, which the
// scan caller treats as record corruption and soft-skips the whole run.
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
 * Persist the initial run record after its slug is claimed and before the
 * child is spawned, so a crash mid-spawn leaves a record the boot scan
 * re-drives. Idempotent: it overwrites any existing record for the same run.
 *
 * Every credential secret in the run's `credentials` cell -- inference and tool
 * alike -- is sealed under the sidecar `cipher` before it touches disk, so the
 * record carries ciphertext (version 2). The `sources`/`bodySources` config is
 * non-secret (each references a credential by id) and rides in the clear.
 */
export async function writeWorkflowRunRecord(
  dataDir: string,
  runId: string,
  record: WorkflowRunRecord,
  cipher: CredentialCipher,
): Promise<void> {
  return withWorkflowRunRecord(dataDir, runId, async (path) => {
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
    // material, so an interrupted write must never expose a torn record the
    // boot scan would then skip. Owner-only (0o600): the sealed secrets are
    // ciphertext, but the record still names each source's provider/baseURL and
    // the deployment's identity, so it stays off a shared host's world-readable
    // set, matching the private-key writes elsewhere on the sidecar.
    await writeFileAtomicDurable(path, JSON.stringify(sealed, null, 2), {
      mode: 0o600,
    });
  });
}

async function readWorkflowRunRecord(
  path: string,
  runId: string,
): Promise<WorkflowRunRecord | null> {
  let raw: string;
  try {
    raw = await readFile(path, "utf8");
  } catch (cause) {
    if (isENOENT(cause)) return null;
    throw cause;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    logger.warn`not updating workflow-runs/${runId}: ${WORKFLOW_RUN_RECORD_FILENAME} is not valid JSON: ${reason}`;
    return null;
  }
  const checked = WorkflowRunRecord(parsed);
  if (checked instanceof type.errors) {
    logger.warn`not updating workflow-runs/${runId}: ${WORKFLOW_RUN_RECORD_FILENAME} failed validation: ${checked.summary}`;
    return null;
  }
  return checked;
}

/** Update sources without replacing lifecycle fields or resealing credentials. */
export async function updateWorkflowRunRecordSources(
  dataDir: string,
  runId: string,
  generation: number,
  sources: WorkflowRunRecord["sources"],
): Promise<void> {
  return withWorkflowRunRecord(dataDir, runId, async (path) => {
    const record = await readWorkflowRunRecord(path, runId);
    if (record === null) {
      throw new Error(
        `Cannot update sources for ${runId}: its deployment record is missing or invalid`,
      );
    }
    if (record.generation !== generation) {
      throw new Error(
        `Cannot update sources for ${runId}: its deployment generation changed`,
      );
    }
    await writeFileAtomicDurable(
      path,
      JSON.stringify({ ...record, sources }, null, 2),
      { mode: 0o600 },
    );
  });
}

/**
 * Mark a run's record with what became of its deployment, keeping the rest of
 * the record as it is, so a restart reports the deployment instead of spawning
 * it. Returns false when the run has no record the boot scan would accept: a
 * restart neither spawns nor reports such a record, so there is nothing to
 * mark, and refusing it would fail the undeploy that removes it.
 */
export async function markWorkflowRunRecord(
  dataDir: string,
  runId: string,
  mark: { readonly state: "stopped" | "tearing-down"; readonly error?: string },
): Promise<boolean> {
  return withWorkflowRunRecord(dataDir, runId, async (path) => {
    const checked = await readWorkflowRunRecord(path, runId);
    if (checked === null) return false;
    const { error: _replaced, ...record } = checked;
    await writeFileAtomicDurable(
      path,
      JSON.stringify(
        {
          ...record,
          state: mark.state,
          ...(mark.error !== undefined ? { error: mark.error } : {}),
        },
        null,
        2,
      ),
      { mode: 0o600 },
    );
    return true;
  });
}

/**
 * Remove a run record. Called on a soft-failed deploy so a never-completed
 * run is not restored on the next boot; undeploy removes the record together
 * with the run repository. A missing record is not an error (`force`).
 */
export async function deleteWorkflowRunRecord(
  dataDir: string,
  runId: string,
): Promise<void> {
  return withWorkflowRunRecord(dataDir, runId, async (path) => {
    await rm(path, { force: true });
  });
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
  for (const entry of entries.sort((a, b) =>
    a.name.localeCompare(b.name, "en"),
  )) {
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
        logger.warn`skipping workflow-runs/${runId}: no ${WORKFLOW_RUN_RECORD_FILENAME} to restore from`;
        continue;
      }
      throw cause;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch (cause) {
      const reason = cause instanceof Error ? cause.message : String(cause);
      logger.warn`skipping workflow-runs/${runId}: ${WORKFLOW_RUN_RECORD_FILENAME} is not valid JSON: ${reason}`;
      continue;
    }

    const record = WorkflowRunRecord(parsed);
    if (record instanceof type.errors) {
      logger.warn`skipping workflow-runs/${runId}: ${WORKFLOW_RUN_RECORD_FILENAME} failed validation: ${record.summary}`;
      continue;
    }
    // A version-2 record seals every credential secret in its `credentials`
    // cell; unseal them under the sidecar cipher for the restored run. A decrypt
    // failure -- a rotated/wrong key or a tampered blob -- is treated as
    // corruption: log and soft-skip the WHOLE run so one undecryptable run does
    // not wedge the boot scan (the deployment is left unrestored and
    // unreported, so the Hub fails it, as it does for any other corrupt
    // record). A record with no `credentials` cell (a deployment that binds
    // none) needs no unseal.
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
