// Gate tests for the `mail.send` namespace in a probe answer. A grant
// requirement inside it is refused; the per-step grants and approvals are not
// checked, so a stored bundle whose walk reports a `mail.send:<domain>`
// approval string freezes.

import { describe, test, expect } from "bun:test";

import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type { GrantRequirement } from "@intx/types";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";
import { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import { action, defineWorkflow, projectLiveToInert } from "@intx/workflow";

import {
  describeProbeGateRefusal,
  gateAndFreezeProbeResult,
  type PersistFrozenApprovalFn,
} from "./workflow-probe-gate";
import type { WorkflowProbeResult } from "./ws/sidecar-handler";

const DEFINITION_SOURCE: WorkflowDefinitionSource = {
  kind: "asset",
  assetId: "asset-1",
  package: {
    format: "source",
    commitSha: "0123456789abcdef0123456789abcdef01234567",
  },
};

// A probe answer for a one-action workflow whose shipped hash matches the hub
// recompute. The step grants and the requirements ride the grant-walk
// snapshot directly, as an untrusted sidecar could send them.
async function probeWith(args: {
  grants: readonly string[];
  grantRequirements: readonly GrantRequirement[];
}): Promise<WorkflowProbeResult> {
  const projection = WorkflowProjectionDefinition.assert(
    projectLiveToInert(
      defineWorkflow({
        id: "wf-under-test",
        trigger: { type: "manual" },
        steps: { push: action({ handler: "push" }) },
      }),
    ),
  );
  return {
    projection,
    grants: [...args.grants],
    grantWalkSnapshot: {
      perStep: [{ stepId: "push", grants: [...args.grants], grantEffects: {} }],
      grantRequirements: [...args.grantRequirements],
    },
    wireHash: await computeWireDefinitionHash(projection),
  };
}

describe("gateAndFreezeProbeResult mail.send namespace", () => {
  test("refuses a requirement inside the namespace and freezes nothing", async () => {
    const persist: PersistFrozenApprovalFn = async () => {
      throw new Error("freeze must not run when the gate rejects");
    };
    const requirement: GrantRequirement = {
      resource: "mail.send:*:*",
      action: "send",
      source: "creator",
    };

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      source: DEFINITION_SOURCE,
      probeResult: await probeWith({
        grants: [],
        grantRequirements: [requirement],
      }),
      approvals: { kind: "approve-probed" },
      persist,
    });

    expect(result).toEqual({
      ok: false,
      reason: "reserved_namespace_grant_requirements",
      reservedNamespaceGrantRequirements: [requirement],
    });
    if (result.ok) throw new Error("expected a refusal");
    expect(describeProbeGateRefusal(result)).toContain(
      "send grants come only from the hub at a run's first trigger",
    );
  });

  test("freezes a bundle whose step grants carry a domain approval string", async () => {
    let persisted = 0;
    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      source: DEFINITION_SOURCE,
      probeResult: await probeWith({
        grants: ["mail.send:example.com"],
        grantRequirements: [],
      }),
      approvals: { kind: "approve-probed" },
      persist: async () => {
        persisted += 1;
        return { definitionId: "def-1" };
      },
    });

    expect(result.ok).toBe(true);
    expect(persisted).toBe(1);
  });
});
