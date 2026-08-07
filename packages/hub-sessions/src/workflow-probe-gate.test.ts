// Mutation tests for the security-load-bearing probe gate + freeze.
//
// The gate is the single point where a code-sourced workflow's inert probe
// answer becomes an approved, frozen definition. These tests pin the three
// invariants a mutation must not be able to break:
//
//   1. Tamper-evidence: a probe whose SHIPPED hash disagrees with the hub
//      recompute over the RECEIVED projection is rejected, and nothing is
//      frozen.
//   2. Freeze fidelity: on approval the RECOMPUTED hash (not the shipped one)
//      and exactly the workflow's advertised grant set (not the operator's
//      wider approval set) are frozen.
//   3. Subset invariant: deploy grants materialize as a subset of the frozen
//      approved set, so a naive live re-walk that surfaces MORE grants than
//      were approved cannot grant beyond the freeze.

import { describe, test, expect } from "bun:test";

import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import type { ApprovalSet } from "@intx/workflow-deploy";

import {
  gateAndFreezeProbeResult,
  type FrozenApproval,
  type PersistFrozenApprovalFn,
} from "./workflow-probe-gate";
import type { WorkflowProbeResult } from "./ws/sidecar-handler";

const PROJECTION: WorkflowProjectionDefinition = {
  id: "wf-under-test",
  triggers: [],
  stepOrder: [],
  steps: {},
};

// A probe result whose shipped hash matches the hub recompute over its
// projection. `shippedWireHash` overrides the shipped value to model a tampered
// answer; `grants` overrides the advisory set.
async function makeProbeResult(overrides?: {
  shippedWireHash?: string;
  grants?: string[];
  projection?: WorkflowProjectionDefinition;
}): Promise<WorkflowProbeResult> {
  const projection = overrides?.projection ?? PROJECTION;
  const wireHash =
    overrides?.shippedWireHash ?? (await computeWireDefinitionHash(projection));
  return {
    projection,
    grants: overrides?.grants ?? ["tool:fetch", "effect:log"],
    wireHash,
  };
}

// A persist double that records every freeze it is asked to write and hands
// back a fixed definition id.
function recordingPersist(definitionId: string): {
  persist: PersistFrozenApprovalFn;
  calls: FrozenApproval[];
} {
  const calls: FrozenApproval[] = [];
  const persist: PersistFrozenApprovalFn = async (approval) => {
    calls.push(approval);
    return { definitionId };
  };
  return { persist, calls };
}

// A persist double that fails the test if the gate ever tries to freeze.
const persistMustNotRun: PersistFrozenApprovalFn = async () => {
  throw new Error("freeze must not run when the gate rejects");
};

describe("gateAndFreezeProbeResult", () => {
  test("rejects a probe whose shipped hash differs from the hub recompute", async () => {
    const probeResult = await makeProbeResult({
      shippedWireHash: "sha256:not-the-real-hash",
    });
    const approvals: ApprovalSet = new Set(probeResult.grants);

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist: persistMustNotRun,
    });

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected rejection");
    expect(result.reason).toBe("wire_hash_mismatch");
    if (result.reason !== "wire_hash_mismatch") throw new Error("wrong reason");
    expect(result.shippedWireHash).toBe("sha256:not-the-real-hash");
    expect(result.recomputedWireHash).toBe(
      await computeWireDefinitionHash(probeResult.projection),
    );
    expect(result.recomputedWireHash).not.toBe(result.shippedWireHash);
  });

  test("freezes the recomputed hash and the advertised grant set on approval", async () => {
    const probeResult = await makeProbeResult({
      grants: ["tool:fetch", "effect:log"],
    });
    // The operator approved MORE than the workflow asked for. The freeze must
    // capture the workflow's advertised set, not the wider approval set.
    const approvals: ApprovalSet = new Set([
      "tool:fetch",
      "effect:log",
      "tool:unused-extra",
    ]);
    const { persist, calls } = recordingPersist("def-123");

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist,
    });

    const expectedHash = await computeWireDefinitionHash(
      probeResult.projection,
    );

    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error("expected approval");
    expect(result.definitionId).toBe("def-123");
    expect(result.approvedWireHash).toBe(expectedHash);
    expect([...result.approvedGrants].sort()).toEqual([
      "effect:log",
      "tool:fetch",
    ]);

    // The freeze was written exactly once, carrying the recomputed hash and the
    // advertised set -- not the shipped hash blindly and not the wider approval
    // set.
    expect(calls).toHaveLength(1);
    const frozen = calls[0];
    if (frozen === undefined) throw new Error("no freeze recorded");
    expect(frozen.assetId).toBe("asset-1");
    expect(frozen.approvedWireHash).toBe(expectedHash);
    expect([...frozen.approvedGrants].sort()).toEqual([
      "effect:log",
      "tool:fetch",
    ]);
    expect(frozen.approvedGrants).not.toContain("tool:unused-extra");
  });

  test("surfaces the inert projection on the ok-arm so the deploy hand-off carries the hashed content", async () => {
    const probeResult = await makeProbeResult({
      grants: ["tool:fetch", "effect:log"],
    });
    const approvals: ApprovalSet = new Set(probeResult.grants);
    const { persist } = recordingPersist("def-projection");

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist,
    });

    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error("expected approval");
    // The freeze hashed exactly this projection; the ok-arm carries it verbatim
    // so the deploy frame binds to the hashed content rather than a re-probe.
    expect(result.projection).toBe(probeResult.projection);
    expect(await computeWireDefinitionHash(result.projection)).toBe(
      result.approvedWireHash,
    );
  });

  test("rejects and freezes nothing when an advisory grant is unapproved", async () => {
    const probeResult = await makeProbeResult({
      grants: ["tool:fetch", "effect:log", "tool:escalate"],
    });
    // The operator did not approve `tool:escalate`.
    const approvals: ApprovalSet = new Set(["tool:fetch", "effect:log"]);

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist: persistMustNotRun,
    });

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected rejection");
    expect(result.reason).toBe("grants_not_approved");
    if (result.reason !== "grants_not_approved")
      throw new Error("wrong reason");
    expect(result.unapprovedGrants).toEqual(["tool:escalate"]);
  });
});
