// Hub-side install/approve orchestration and gate for a code-sourced workflow
// definition.
//
// This is the production glue that turns a code-sourced workflow install into
// an approved, frozen definition WITHOUT executing any author code on the hub.
// Everything here operates over the inert projection and grant set the sidecar
// returns from a probe:
//
//   1. Resolve the frozen dependency closure for the definition's pin.
//   2. Probe the sidecar for the definition's inert needs-surface projection,
//      its advisory grant set, and the content hash the sidecar shipped.
//   3. RECOMPUTE the wire hash over the RECEIVED projection as tamper-evidence:
//      a shipped hash that differs from the hub recompute is rejected, fail
//      closed, no coercion.
//   4. Gate the advisory grant set against the operator's `ApprovalSet`: every
//      grant the probe surfaced must be operator-approved or the gate fails.
//   5. Freeze the approved wire hash onto the definition version row, keyed by
//      the definition's selector, and return the frozen approved grant set.
//
// The frozen approved set is the single source of truth for the definition's
// grants: the deploy path materializes deploy grants as a SUBSET of it (never a
// fresh walk), so a workflow can never acquire at deploy or run time a grant it
// did not have frozen at approval. The wire hash is that freeze's anchor -- the
// grant set is a deterministic projection of the exact content the hash
// addresses, so pinning the hash pins the set.

import { and, eq } from "drizzle-orm";

import type { DBExecutor } from "@intx/db";
import { workflowDefinitionVersion } from "@intx/db/schema";
import type { PackumentFetcher, RegistryConfig } from "@intx/tool-packaging";
import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import type { ToolPackageManifest } from "@intx/types/tool-packages";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";
import type { ApprovalSet } from "@intx/workflow-deploy";

import { resolveWorkflowClosure } from "./workflow-closure-resolution";
import { ensureWorkflowDefinitionForAsset } from "./workflow-definition-ensure";
import type { SidecarRouter, WorkflowProbeResult } from "./ws/sidecar-handler";

// The version `ensureWorkflowDefinitionForAsset` projects for a fresh
// definition, and therefore the row the approval freeze targets. Kept in step
// with the ensure helper: if that helper ever projects a different initial
// version, this must follow.
const FROZEN_VERSION = "1";

/**
 * The frozen record an approval writes: the definition's asset selector, the
 * approved wire hash (the freeze anchor), and the approved grant set. The grant
 * set is a deterministic projection of the content the hash addresses; it rides
 * the deploy hand-off in memory rather than a version-row column.
 */
export type FrozenApproval = {
  readonly assetId: string;
  readonly approvedWireHash: string;
  readonly approvedGrants: readonly string[];
};

/**
 * Persists a frozen approval and returns the definition it was recorded
 * against. Bound to a `DBExecutor` in production via
 * `createDbFrozenApprovalWriter`; a test double records the call.
 */
export type PersistFrozenApprovalFn = (
  approval: FrozenApproval,
) => Promise<{ definitionId: string }>;

/**
 * The outcome of gating and freezing a probe result. `ok: true` is the frozen
 * approval the deploy hand-off consumes. The `ok: false` arms name the two
 * fail-closed paths: a shipped hash that does not match the hub recompute
 * (tamper-evidence), and advisory grants the operator did not approve.
 */
export type ProbeGateResult =
  | {
      readonly ok: true;
      readonly definitionId: string;
      readonly approvedWireHash: string;
      readonly approvedGrants: ReadonlySet<string>;
      /**
       * The inert wire projection the freeze hashed. Rides the ok-arm so the
       * deploy hand-off carries the exact content the frozen hash addresses,
       * never a re-projection of a registry that may have moved since approval.
       */
      readonly projection: WorkflowProjectionDefinition;
    }
  | {
      readonly ok: false;
      readonly reason: "wire_hash_mismatch";
      readonly shippedWireHash: string;
      readonly recomputedWireHash: string;
    }
  | {
      readonly ok: false;
      readonly reason: "grants_not_approved";
      readonly unapprovedGrants: readonly string[];
    };

/**
 * Build the production persistence step of the freeze. Records identity through
 * the selector-keyed ensure helper (a definition keyed by `(assetId,
 * wireHash)`) and writes the approved wire hash onto that definition's version
 * row. The grant set is not written to a version-row column -- none exists, and
 * the approved wire hash already pins the exact content the grants project
 * from -- so it travels with the returned frozen approval, not the row.
 */
export function createDbFrozenApprovalWriter(
  db: DBExecutor,
): PersistFrozenApprovalFn {
  return async ({ assetId, approvedWireHash }) => {
    const { definitionId } = await ensureWorkflowDefinitionForAsset(db, {
      assetId,
      wireHash: approvedWireHash,
    });
    await db
      .update(workflowDefinitionVersion)
      .set({ approvedWireHash })
      .where(
        and(
          eq(workflowDefinitionVersion.definitionId, definitionId),
          eq(workflowDefinitionVersion.version, FROZEN_VERSION),
        ),
      );
    return { definitionId };
  };
}

export type GateAndFreezeArgs = {
  /** The `workflow`-kind asset the frozen definition projects over. */
  readonly assetId: string;
  /** The sidecar's inert probe answer: projection, advisory grants, shipped hash. */
  readonly probeResult: WorkflowProbeResult;
  /** The operator-approved grant-shape strings. The advisory set is gated against this. */
  readonly approvals: ApprovalSet;
  /** Persistence step for the freeze; `createDbFrozenApprovalWriter` in production. */
  readonly persist: PersistFrozenApprovalFn;
};

/**
 * Gate a probe result and, on approval, freeze it. Operates purely over the
 * inert projection and grant set -- no author code runs here and the capability
 * walk is never re-run.
 *
 * Fails closed on the two security-load-bearing checks before it writes
 * anything: the recomputed wire hash must match the hash the sidecar shipped
 * (tamper-evidence), and every advisory grant must be operator-approved. Only
 * then does it freeze the recomputed hash onto the version row and return the
 * approved grant set.
 */
export async function gateAndFreezeProbeResult(
  args: GateAndFreezeArgs,
): Promise<ProbeGateResult> {
  const { assetId, probeResult, approvals, persist } = args;

  // Tamper-evidence: recompute over the RECEIVED projection and compare to the
  // shipped hash. A mismatch means the projection the hub is approving is not
  // the one the sidecar hashed, so reject rather than freeze a hash that does
  // not describe the approved content.
  const recomputedWireHash = await computeWireDefinitionHash(
    probeResult.projection,
  );
  if (recomputedWireHash !== probeResult.wireHash) {
    return {
      ok: false,
      reason: "wire_hash_mismatch",
      shippedWireHash: probeResult.wireHash,
      recomputedWireHash,
    };
  }

  // Gate the advisory grant set: every grant the probe surfaced must appear in
  // the operator's approved set. Any miss fails the gate closed.
  const unapprovedGrants = probeResult.grants.filter(
    (grant) => !approvals.has(grant),
  );
  if (unapprovedGrants.length > 0) {
    return { ok: false, reason: "grants_not_approved", unapprovedGrants };
  }

  // Freeze: the approved set is exactly what the workflow advertised (all of it
  // now operator-approved), pinned to the recomputed hash. Persisting the hash
  // is the freeze; the grant set is returned for the deploy hand-off.
  const approvedGrants: readonly string[] = [...probeResult.grants];
  const { definitionId } = await persist({
    assetId,
    approvedWireHash: recomputedWireHash,
    approvedGrants,
  });

  return {
    ok: true,
    definitionId,
    approvedWireHash: recomputedWireHash,
    approvedGrants: new Set(approvedGrants),
    projection: probeResult.projection,
  };
}

export type InstallAndApproveArgs = {
  /** Where the definition's bytes come from -- names the npm registry that publishes it. */
  readonly source: WorkflowDefinitionSource;
  /** A `name@range` spec for the workflow definition package. */
  readonly pin: string;
  /** URL and credentials for the registry `source` names. */
  readonly registryConfig: RegistryConfig;
  /** The `interchange.workflow` entry-module path the sidecar evaluates to project the definition. */
  readonly entry: string;
  /** The `workflow`-kind asset the frozen definition projects over. */
  readonly assetId: string;
  /** The operator-approved grant-shape strings. */
  readonly approvals: ApprovalSet;
  /** The sidecar router carrying the probe transport. */
  readonly router: Pick<SidecarRouter, "sendProbe">;
  /** Executor the freeze writes through. */
  readonly db: DBExecutor;
  /** Test seam for packument fetches, threaded to closure resolution. Omitted in production. */
  readonly fetchPackument?: PackumentFetcher;
};

/**
 * The frozen hand-off `installAndApproveWorkflowDefinition` produces. It carries
 * the gate outcome plus the two values the source-ref deploy frame needs and
 * must NOT recompute at deploy: the inert `projection` the freeze hashed and the
 * frozen dependency `closure` the pin resolved to. Re-resolving either at deploy
 * would reintroduce the non-determinism the freeze eliminates -- a registry that
 * moved between approve and deploy would pin different bytes and project
 * differently, failing the child re-verify -- so both ride from approve verbatim.
 */
export type InstallAndApproveResult = {
  readonly approval: ProbeGateResult;
  readonly projection: WorkflowProjectionDefinition;
  readonly closure: ToolPackageManifest;
};

/**
 * The install/approve orchestration entrypoint the end-to-end flow drives:
 * resolve the frozen closure, probe the sidecar, then gate and freeze the
 * result. This is production glue, not test-only wiring.
 *
 * The operator-approval decision is an input (`approvals`): the caller supplies
 * the set the operator approved, and the gate holds the advisory set to it.
 *
 * Returns the gate outcome alongside the inert projection and the frozen
 * closure so the deploy hand-off consumes them verbatim rather than re-probing
 * or re-resolving.
 */
export async function installAndApproveWorkflowDefinition(
  args: InstallAndApproveArgs,
): Promise<InstallAndApproveResult> {
  const closure = await resolveWorkflowClosure({
    source: args.source,
    pin: args.pin,
    registryConfig: args.registryConfig,
    ...(args.fetchPackument !== undefined
      ? { fetchPackument: args.fetchPackument }
      : {}),
  });

  const { sendProbe } = args.router;
  if (sendProbe === undefined) {
    throw new Error(
      "installAndApproveWorkflowDefinition: router does not support sendProbe",
    );
  }
  const probeResult = await sendProbe({
    source: args.source,
    closure,
    entry: args.entry,
  });

  const approval = await gateAndFreezeProbeResult({
    assetId: args.assetId,
    probeResult,
    approvals: args.approvals,
    persist: createDbFrozenApprovalWriter(args.db),
  });

  return { approval, projection: probeResult.projection, closure };
}

/**
 * Materialize deploy grants as a SUBSET of the frozen approved set. The deploy
 * path may present a candidate grant set from any source -- including a naive
 * live re-walk of the (possibly mutated) asset, which can surface MORE grants
 * than were approved. Only grants in the frozen approved set are granted; every
 * extra is refused. The result is therefore always a subset of
 * `frozenApprovedGrants`, so a workflow can never acquire at deploy time a grant
 * it did not have frozen at approval.
 */
export function materializeDeployGrantsFromFrozen(
  frozenApprovedGrants: ReadonlySet<string>,
  candidateGrants: Iterable<string>,
): string[] {
  const granted: string[] = [];
  for (const grant of candidateGrants) {
    if (frozenApprovedGrants.has(grant)) granted.push(grant);
  }
  return granted;
}
