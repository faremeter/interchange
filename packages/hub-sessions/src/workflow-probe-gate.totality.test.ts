// Gate tests for grant-record totality over a deployment's executable closure.
//
// A probe answer arrives in two independently produced halves: the inert
// projection, built by the hub's live->inert projector, and the grant-walk
// snapshot, built by the sidecar's capability walk over the live definition.
// Until the gate compared them, nothing did. A step the walk skipped still
// projects, still deploys, and is still scheduled -- and then every tool call
// from it is refused for lack of any grant, which the tool runner turns into an
// error tool result rather than a failure. The run completes having done none
// of the work.
//
// The walk folds a nested body's grants into the record of the top-level step
// that carries the body, so a top-level record covers that step's whole
// executable subtree. These tests exercise both directions: a deployment whose
// snapshot is missing a record is refused and names the affected steps, and
// well-formed deployments carrying tool-bearing loop and onTrigger bodies still
// pass.

import { describe, test, expect } from "bun:test";

import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type { GrantWalkSnapshot } from "@intx/types";
import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import type { ApprovalSet } from "@intx/workflow-deploy";

import {
  gateAndFreezeProbeResult,
  type FrozenApproval,
  type PersistFrozenApprovalFn,
} from "./workflow-probe-gate";
import type { WorkflowProbeResult } from "./ws/sidecar-handler";

const TOOL_GRANT = "tool:mail_send";

function agentStep(): Record<string, unknown> {
  return {
    kind: "step",
    agent: {
      id: "ag",
      modelSources: [{ provider: "anthropic", model: "claude-3" }],
    },
  };
}

function projection(args: {
  id: string;
  steps: Record<string, unknown>;
  stepOrder: string[];
}): WorkflowProjectionDefinition {
  return {
    id: args.id,
    triggers: [{ type: "manual" }],
    stepOrder: args.stepOrder,
    steps: args.steps,
  };
}

/** A snapshot record for each named top-level step, all carrying one tool grant. */
function snapshotFor(stepIds: readonly string[]): GrantWalkSnapshot {
  return {
    perStep: stepIds.map((stepId) => ({
      stepId,
      grants: [TOOL_GRANT],
      grantEffects: { [TOOL_GRANT]: "allow" as const },
    })),
    grantRequirements: [],
  };
}

// A probe answer whose shipped hash matches the hub recompute, so tamper-
// evidence passes and the totality check is what decides.
async function makeProbeResult(
  definition: WorkflowProjectionDefinition,
  snapshot: GrantWalkSnapshot,
): Promise<WorkflowProbeResult> {
  return {
    projection: definition,
    grants: [TOOL_GRANT],
    grantWalkSnapshot: snapshot,
    wireHash: await computeWireDefinitionHash(definition),
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

// A loop whose body carries the only tool-bearing step in the deployment. The
// body step's grants are folded into the `spin` record by the capability walk,
// so `spin` is the key its approved-grant record is under.
function loopBearingProjection(): WorkflowProjectionDefinition {
  const body = projection({
    id: "body",
    steps: { bodyStep: agentStep() },
    stepOrder: ["bodyStep"],
  });
  return projection({
    id: "wf-loop",
    steps: {
      lead: agentStep(),
      spin: { kind: "loop", body },
    },
    stepOrder: ["lead", "spin"],
  });
}

describe("gateAndFreezeProbeResult grant-record totality", () => {
  test("refuses a deployment whose loop body has no record behind it", async () => {
    // The snapshot records `lead` only. `spin` and everything its body can run
    // therefore deploy with no approved grants at all.
    const probeResult = await makeProbeResult(
      loopBearingProjection(),
      snapshotFor(["lead"]),
    );
    const approvals: ApprovalSet = new Set(probeResult.grants);

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist: persistMustNotRun,
    });

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected rejection");
    expect(result.reason).toBe("steps_without_grant_record");
    if (result.reason !== "steps_without_grant_record") {
      throw new Error("wrong reason");
    }
    expect(result.stepsWithoutGrantRecord).toEqual([
      { stepId: "spin", recordStepId: "spin", reachedThrough: ["spin"] },
      {
        stepId: "bodyStep",
        recordStepId: "spin",
        reachedThrough: ["spin", "bodyStep"],
      },
    ]);
    // The message must be actionable on its own: the step inside the body, the
    // position it was reached through, and the record that was absent.
    expect(result.message).toContain("bodyStep");
    expect(result.message).toContain("spin > bodyStep");
    expect(result.message).toContain("grant-walk record");
  });

  test("names a step inside a childWorkflow body nested in a loop body", async () => {
    // Two rungs down and across a lifted-body boundary. The capability walk
    // folds this grandchild's grants into the top-level `spin` record too, so
    // the missing record is reported against `spin` and the chain is what tells
    // the reader where the step actually lives.
    const grandchild = projection({
      id: "grandchild",
      steps: { work: agentStep() },
      stepOrder: ["work"],
    });
    const body = projection({
      id: "body",
      steps: {
        spawn: { kind: "childWorkflow", definition: { inline: grandchild } },
      },
      stepOrder: ["spawn"],
    });
    const definition = projection({
      id: "wf-nested",
      steps: { spin: { kind: "loop", body } },
      stepOrder: ["spin"],
    });
    const probeResult = await makeProbeResult(definition, snapshotFor([]));

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals: { mode: "approve-probed" },
      persist: persistMustNotRun,
    });

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected rejection");
    if (result.reason !== "steps_without_grant_record") {
      throw new Error(`wrong reason: ${result.reason}`);
    }
    expect(
      result.stepsWithoutGrantRecord.map((step) => step.reachedThrough),
    ).toEqual([["spin"], ["spin", "spawn"], ["spin", "spawn", "work"]]);
    expect(result.message).toContain("spin > spawn > work");
  });

  test("refuses before the freeze writes anything", async () => {
    // Every advertised grant is approved, so only the totality check can
    // reject here -- and nothing may be frozen when it does.
    const probeResult = await makeProbeResult(
      loopBearingProjection(),
      snapshotFor(["lead"]),
    );
    const { persist, calls } = recordingPersist();

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals: { mode: "approve-probed" },
      persist,
    });

    expect(result.ok).toBe(false);
    expect(calls).toEqual([]);
  });

  test("refuses ahead of the operator-policy checks", async () => {
    // Nothing is approved, so the advisory-grant gate would also reject. A
    // deploy-path defect must not be reported behind a message an operator
    // would act on instead.
    const probeResult = await makeProbeResult(
      loopBearingProjection(),
      snapshotFor(["lead"]),
    );

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals: new Set<string>(),
      persist: persistMustNotRun,
    });

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected rejection");
    expect(result.reason).toBe("steps_without_grant_record");
  });

  test("admits a tool-bearing loop body once every top-level step is recorded", async () => {
    const probeResult = await makeProbeResult(
      loopBearingProjection(),
      snapshotFor(["lead", "spin"]),
    );
    const approvals: ApprovalSet = new Set(probeResult.grants);
    const { persist, calls } = recordingPersist();

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist,
    });

    expect(result.ok).toBe(true);
    expect(calls).toHaveLength(1);
  });

  test("admits a tool-bearing onTrigger section body", async () => {
    const section = projection({
      id: "section-body",
      steps: { handle: agentStep() },
      stepOrder: ["handle"],
    });
    const definition = projection({
      id: "wf-section",
      steps: { watch: { kind: "onTrigger", body: { inline: section } } },
      stepOrder: ["watch"],
    });
    const probeResult = await makeProbeResult(
      definition,
      snapshotFor(["watch"]),
    );
    const { persist, calls } = recordingPersist();

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals: { mode: "approve-probed" },
      persist,
    });

    expect(result.ok).toBe(true);
    expect(calls).toHaveLength(1);
  });

  test("admits a deployment whose bodies are refs rather than inline", async () => {
    // A `{ ref }` body is a separately deployed asset walked on its own, so the
    // executable closure stops at it and no record is expected for anything
    // behind it.
    const definition = projection({
      id: "wf-ref",
      steps: {
        watch: { kind: "onTrigger", body: { ref: "other-workflow" } },
        spawn: { kind: "childWorkflow", definition: { ref: "other-child" } },
      },
      stepOrder: ["watch", "spawn"],
    });
    const probeResult = await makeProbeResult(
      definition,
      snapshotFor(["watch", "spawn"]),
    );
    const { persist, calls } = recordingPersist();

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals: { mode: "approve-probed" },
      persist,
    });

    expect(result.ok).toBe(true);
    expect(calls).toHaveLength(1);
  });

  test("admits a deployment with no steps at all", async () => {
    const definition = projection({ id: "wf-empty", steps: {}, stepOrder: [] });
    const probeResult = await makeProbeResult(definition, snapshotFor([]));
    const { persist, calls } = recordingPersist();

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals: { mode: "approve-probed" },
      persist,
    });

    expect(result.ok).toBe(true);
    expect(calls).toHaveLength(1);
  });
});
