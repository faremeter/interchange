// Gate tests for trigger types the runtime does not implement.
//
// `defineWorkflow` is bundled INTO the pinned workflow closure, so an
// authoring-time rejection only ever reaches an author who rebuilds and
// republishes. A closure that predates the authoring check keeps its own frozen
// copy of `defineWorkflow` and sails through every re-evaluation point. The
// inert projection is the one surface a stale closure cannot carry: its
// `triggers` array is produced by the hub's own live->inert projector, not by
// the closure. So the gate is where an already-deployed schedule-triggered
// workflow is stopped.

import { describe, test, expect } from "bun:test";

import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import { createApprovalSet, type ApprovalSet } from "@intx/workflow-deploy";

import {
  gateAndFreezeProbeResult,
  type FrozenApproval,
  type PersistFrozenApprovalFn,
} from "./workflow-probe-gate";
import type { WorkflowProbeResult } from "./ws/sidecar-handler";

function projectionWithTriggers(
  triggers: unknown[],
): WorkflowProjectionDefinition {
  return { id: "wf-under-test", triggers, stepOrder: [], steps: {} };
}

// A probe answer whose shipped hash matches the hub recompute, so the
// tamper-evidence check passes and the trigger check is what decides.
async function makeProbeResult(
  projection: WorkflowProjectionDefinition,
): Promise<WorkflowProbeResult> {
  return {
    projection,
    grants: ["tool:fetch"],
    grantWalkSnapshot: { perStep: [], grantRequirements: [] },
    wireHash: await computeWireDefinitionHash(projection),
  };
}

function recordingPersist(): {
  persist: PersistFrozenApprovalFn;
  calls: FrozenApproval[];
} {
  const calls: FrozenApproval[] = [];
  const persist: PersistFrozenApprovalFn = async (approval) => {
    calls.push(approval);
    return { definitionId: "def-1" };
  };
  return { persist, calls };
}

const persistMustNotRun: PersistFrozenApprovalFn = async () => {
  throw new Error("freeze must not run when the gate rejects");
};

describe("gateAndFreezeProbeResult trigger admission", () => {
  test("rejects a projection carrying a schedule trigger", async () => {
    const probeResult = await makeProbeResult(
      projectionWithTriggers([{ type: "schedule", cron: "0 9 * * *" }]),
    );
    const approvals: ApprovalSet = createApprovalSet(probeResult.grants);

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist: persistMustNotRun,
    });

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected rejection");
    expect(result.reason).toBe("unimplemented_trigger");
    if (result.reason !== "unimplemented_trigger") {
      throw new Error("wrong reason");
    }
    expect(result.unimplementedTriggerTypes).toEqual(["schedule"]);
  });

  test("rejects a schedule trigger mixed in with implemented triggers", async () => {
    const probeResult = await makeProbeResult(
      projectionWithTriggers([
        { type: "mail", to: "s@x.example" },
        { type: "schedule", cron: "*/5 * * * *" },
      ]),
    );
    const approvals: ApprovalSet = createApprovalSet(probeResult.grants);

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist: persistMustNotRun,
    });

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected rejection");
    expect(result.reason).toBe("unimplemented_trigger");
  });

  test("rejects before the grant gate approves anything", async () => {
    // The advisory grants are all approved, so only the trigger check can
    // reject here -- and nothing may be frozen when it does.
    const probeResult = await makeProbeResult(
      projectionWithTriggers([{ type: "schedule", cron: "0 0 * * *" }]),
    );
    const { persist, calls } = recordingPersist();

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals: { kind: "approve-probed" },
      persist,
    });

    expect(result.ok).toBe(false);
    expect(calls).toEqual([]);
  });

  test("approves a projection whose triggers are all implemented", async () => {
    const probeResult = await makeProbeResult(
      projectionWithTriggers([
        { type: "mail", to: "s@x.example" },
        { type: "manual" },
      ]),
    );
    const { persist, calls } = recordingPersist();

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals: { kind: "approve-probed" },
      persist,
    });

    expect(result.ok).toBe(true);
    expect(calls).toHaveLength(1);
  });

  test("approves a projection that declares no triggers", async () => {
    const probeResult = await makeProbeResult(projectionWithTriggers([]));
    const { persist, calls } = recordingPersist();

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals: { kind: "approve-probed" },
      persist,
    });

    expect(result.ok).toBe(true);
    expect(calls).toHaveLength(1);
  });
});
