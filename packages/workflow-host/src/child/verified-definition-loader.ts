// Shared definition read + optional re-verify for the workflow-process child.
//
// The child reaches a workflow definition through structurally identical
// read paths that read `workflow.json` from the deploy working tree, parse
// it, and validate the envelope. `readWorkflowDefinitionEnvelope` owns that
// read+validate step in one layer. `loadVerifiedWorkflowDefinition` wraps it
// with the re-verify barrier: it recomputes the wire hash over the validated
// projection and refuses to return a definition whose recompute does not
// match the hub-approved hash. A mismatch throws -- fail closed, no fallback
// and no coercion.
//
// The gate is a THIN WRAPPER, not baked into the read, precisely because the
// re-verify barrier is load-bearing only where the caller holds an approved
// hash that arrived OUT-OF-BAND from the bytes being checked (a signed spawn
// env / deploy frame the file-writer cannot forge). Callers with such a hash
// -- the top-level run child (`run-child.ts`, `SpawnTimeEnv.definitionHash`)
// and the onTrigger-body spawn path (`adapters/spawn-child.ts`, the parent's
// frame-carried `referencedDefinitionHashes[bodyId]`) -- use the gated
// wrapper. A caller with no out-of-band pin -- a `childWorkflow` spawn, which
// resolves a SEPARATELY-approved, hub-authored, sidecar-read-only asset the
// parent's frame has no authority over -- uses `readWorkflowDefinitionEnvelope`
// directly. Gating that path could only fail-closed-always, since there is no
// approved hash to check against; its integrity is the asset repo's
// hub-writes/sidecar-reads authorization plus push-time envelope validation.
//
// The barrier lives at the LOAD boundary, not at run start. A resumed run
// reuses the definition this loader returned at child boot, so gating the
// load covers fresh runs, resumed runs, and referenced onTrigger bodies
// with a single check. `RunStarted.definitionHash` is deliberately NOT the
// barrier: it hashes a different projection and never fires when the run
// log already carries a `RunStarted`, so it is skipped on resume.

import { type } from "arktype";

import type { RepoId, RepoStore } from "@intx/hub-sessions/substrate";
import { workflowDefinitionEnvelopeSchema } from "@intx/hub-sessions/substrate";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import { computeLiveDefinitionHash } from "@intx/workflow";
import type { WorkflowDefinition } from "@intx/workflow";

import { loadWorkflowDefinitionFromClosure } from "../workflow-definition-loader";

export interface ReadWorkflowDefinitionEnvelopeOpts {
  /** Substrate the deploy orchestrator wrote the workflow asset into. */
  substrate: RepoStore;
  /**
   * Workflow-asset repo whose deploy working tree holds the definition.
   * The read composes `substrate.getRepoDir(repoId)` with `workflowPath`.
   */
  repoId: RepoId;
  /**
   * Repo-relative path to the workflow JSON within the deploy working
   * tree. Production callers pass `workflow.json`.
   */
  workflowPath: string;
}

export interface LoadVerifiedWorkflowDefinitionOpts
  extends ReadWorkflowDefinitionEnvelopeOpts {
  /**
   * Hub-approved wire hash the recompute must match. Sourced from the hub
   * authority: `SpawnTimeEnv.definitionHash` for the top-level run, or the
   * per-body entry of `SpawnTimeEnv.referencedDefinitionHashes` for a
   * referenced onTrigger body. A recompute that differs throws.
   */
  approvedHash: string;
}

/**
 * Read and envelope-validate a workflow definition from the deploy working
 * tree. Returns the validated `WorkflowDefinition` WITHOUT a re-verify gate:
 * this is the read step callers that hold no out-of-band approved hash use
 * directly (a `childWorkflow` spawn resolving a separately-approved,
 * hub-authored, sidecar-read-only asset). Callers that DO hold an out-of-band
 * pin wrap this with `loadVerifiedWorkflowDefinition`.
 */
export async function readWorkflowDefinitionEnvelope(
  opts: ReadWorkflowDefinitionEnvelopeOpts,
): Promise<WorkflowDefinition> {
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const dir = opts.substrate.getRepoDir(opts.repoId);
  const filePath = path.join(dir, opts.workflowPath);
  const label = `${opts.repoId.kind}/${opts.repoId.id}`;

  // Neutral "definition read" prefix, NOT "verified": this envelope read is
  // shared by the gated loaders below AND the deliberately-ungated childWorkflow
  // spawn, so labeling its errors "verified" would misdescribe the ungated path.
  // The re-verify errors that DO gate stay labeled "verified" in the loaders.
  let raw: string;
  try {
    raw = await fs.readFile(filePath, "utf8");
  } catch (cause) {
    if (isErrnoNotFound(cause)) {
      throw new Error(
        `workflow-host definition read: ${opts.workflowPath} not present under ${label}`,
        { cause },
      );
    }
    throw new Error(
      `workflow-host definition read: cannot read ${opts.workflowPath} for ${label}`,
      { cause },
    );
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new Error(
      `workflow-host definition read: ${opts.workflowPath} for ${label} is not valid JSON`,
      { cause },
    );
  }

  const validated = workflowDefinitionEnvelopeSchema(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `workflow-host definition read: ${opts.workflowPath} for ${label} failed envelope validation: ${validated.summary}`,
    );
  }
  // The envelope schema enforces the structural shape the runtime body and
  // state machine consume; the discriminated narrow over every primitive
  // variant lives downstream in the runtime body. `.onUndeclaredKey("ignore")`
  // is passthrough, not stripping, so the validated object carries the same
  // fields the on-disk bytes did -- a hash recompute over it therefore hashes
  // a faithful projection of exactly what was read.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- envelope schema enforces structural shape; primitive narrows live downstream in the runtime body
  return validated as unknown as WorkflowDefinition;
}

/**
 * Read, envelope-validate, and re-verify a workflow definition from the
 * deploy working tree. Returns the validated `WorkflowDefinition` only
 * when its recomputed wire hash matches `approvedHash`; otherwise throws.
 * The read+validate is delegated to `readWorkflowDefinitionEnvelope`; this
 * function adds only the out-of-band-pin re-verify barrier.
 */
export async function loadVerifiedWorkflowDefinition(
  opts: LoadVerifiedWorkflowDefinitionOpts,
): Promise<WorkflowDefinition> {
  const definition = await readWorkflowDefinitionEnvelope({
    substrate: opts.substrate,
    repoId: opts.repoId,
    workflowPath: opts.workflowPath,
  });
  const label = `${opts.repoId.kind}/${opts.repoId.id}`;

  const recomputed = await computeWireDefinitionHash(definition);
  if (recomputed !== opts.approvedHash) {
    throw new Error(
      `workflow-host verified-definition loader: recomputed wire hash ${recomputed} for ${label} does not match the approved hash ${opts.approvedHash}; refusing to load a definition tampered after approval`,
    );
  }
  return definition;
}

export interface LoadVerifiedWorkflowDefinitionFromClosureOpts {
  /**
   * Sidecar-local directory of the materialized workflow-definition closure:
   * the package dir holding `package.json` (with `interchange.workflow`) and
   * its laid-out `node_modules/`. The sidecar computes this when it applies
   * the frozen closure and threads it through the child's spawn env.
   */
  packageDir: string;
  /**
   * Hub-approved wire hash the re-verify must match. Sourced from the hub
   * authority (`SpawnTimeEnv.definitionHash`). The recompute projects the
   * evaluated LIVE definition back to its inert form and hashes that; a value
   * that differs throws.
   */
  approvedHash: string;
  /**
   * Test seam forwarded to `loadWorkflowDefinitionFromClosure` for the
   * workflow entry's dynamic import. Production omits it and the entry is
   * imported natively.
   */
  importModule?: (importUrl: string) => Promise<unknown>;
}

/**
 * Evaluate a source-ref deployment's pinned code closure to a live
 * `WorkflowDefinition` and re-verify it by project-then-hash before returning
 * it. This is the source-ref counterpart to `loadVerifiedWorkflowDefinition`:
 * the inert projection is a non-executable approval surface, so the runtime
 * needs the live definition the closure evaluates to. The re-verify projects
 * that live definition back to its inert form and hashes it
 * (`computeLiveDefinitionHash`), matching the hub-approved wire hash by byte
 * equality; a divergent closure fails closed here and never runs.
 */
export async function loadVerifiedWorkflowDefinitionFromClosure(
  opts: LoadVerifiedWorkflowDefinitionFromClosureOpts,
): Promise<WorkflowDefinition> {
  const definition = await loadWorkflowDefinitionFromClosure({
    packageDir: opts.packageDir,
    ...(opts.importModule !== undefined
      ? { importModule: opts.importModule }
      : {}),
  });
  const recomputed = await computeLiveDefinitionHash(definition);
  if (recomputed !== opts.approvedHash) {
    throw new Error(
      `workflow-host verified-definition loader: recomputed wire hash ${recomputed} for the source-ref closure at ${opts.packageDir} does not match the approved hash ${opts.approvedHash}; refusing to run a definition that no longer projects to the hub-approved content`,
    );
  }
  return definition;
}

function isErrnoNotFound(cause: unknown): boolean {
  return (
    cause !== null &&
    typeof cause === "object" &&
    "code" in cause &&
    cause.code === "ENOENT"
  );
}
