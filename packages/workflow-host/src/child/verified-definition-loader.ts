// Source-ref definition load + re-verify barrier for the workflow-process child.
//
// A source-ref deployment's runnable definition is the evaluated pinned code
// closure, not an on-disk `workflow.json`. `loadVerifiedWorkflowDefinitionFromClosure`
// evaluates that closure to a live `WorkflowDefinition` and re-verifies it by
// project-then-hash: it projects the live definition back to its inert form,
// hashes it (`computeLiveDefinitionHash`), and refuses to return a definition
// whose recompute does not match the hub-approved hash. A mismatch throws --
// fail closed, no fallback and no coercion.
//
// The re-verify barrier is load-bearing because the approved hash arrives
// OUT-OF-BAND from the bytes being checked (a signed spawn env the closure
// materializer cannot forge). It lives at the LOAD boundary, not at run start:
// a resumed run reuses the definition this loader returned at child boot, so
// gating the load covers fresh runs, resumed runs, and referenced onTrigger
// bodies (which the child extracts from the same re-verified closure) with a
// single check. `RunStarted.definitionHash` is deliberately NOT the barrier: it
// hashes a different projection and never fires when the run log already carries
// a `RunStarted`, so it is skipped on resume.

import { computeLiveDefinitionHash } from "@intx/workflow";
import type { WorkflowDefinition } from "@intx/workflow";

import { loadWorkflowDefinitionFromClosure } from "../workflow-definition-loader";

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
 * it. The inert projection is a non-executable approval surface, so the runtime
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
