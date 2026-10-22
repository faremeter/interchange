// Gate tests for the `mail.accept` namespace in a probe answer: grant
// requirements inside it, and walk markers the projection's `mailAccept` does
// not produce. The gate checks both over the probe's grant-walk snapshot, so a
// stale or untrusted sidecar cannot carry either past it.

import { describe, test, expect } from "bun:test";

import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type { GrantRequirement } from "@intx/types";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";
import { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import { action, defineWorkflow, projectLiveToInert } from "@intx/workflow";

import {
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
// recompute and whose grant walk records the step. The requirements ride the
// grant-walk snapshot directly, as an untrusted sidecar could send them.
async function probeWith(
  grantRequirements: readonly GrantRequirement[],
): Promise<WorkflowProbeResult> {
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
    grants: [],
    grantWalkSnapshot: {
      perStep: [{ stepId: "push", grants: [], grantEffects: {} }],
      grantRequirements: [...grantRequirements],
    },
    wireHash: await computeWireDefinitionHash(projection),
  };
}

describe("gateAndFreezeProbeResult mail.accept grant requirements", () => {
  test("refuses a requirement inside the namespace and freezes nothing", async () => {
    const persist: PersistFrozenApprovalFn = async () => {
      throw new Error("freeze must not run when the gate rejects");
    };
    const requirement: GrantRequirement = {
      resource: "mail.accept:*",
      action: "accept",
      source: "creator",
    };

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      source: DEFINITION_SOURCE,
      probeResult: await probeWith([requirement]),
      approvals: { kind: "approve-probed" },
      persist,
    });

    expect(result).toEqual({
      ok: false,
      reason: "mail_accept_grant_requirements",
      mailAcceptGrantRequirements: [requirement],
    });
  });

  test("approves a broad requirement the admission gate ignores", async () => {
    let persisted = 0;
    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      source: DEFINITION_SOURCE,
      probeResult: await probeWith([
        { resource: "*", action: "*", source: "creator" },
      ]),
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

// A probe answer whose walk reports `markers` in the advisory grants and in the
// one step's record, as an untrusted sidecar could send them.
async function probeReporting(
  workflow: ReturnType<typeof defineWorkflow>,
  markers: readonly string[],
): Promise<WorkflowProbeResult> {
  const projection = WorkflowProjectionDefinition.assert(
    projectLiveToInert(workflow),
  );
  return {
    projection,
    grants: [...markers],
    grantWalkSnapshot: {
      perStep: [{ stepId: "push", grants: [...markers], grantEffects: {} }],
      grantRequirements: [],
    },
    wireHash: await computeWireDefinitionHash(projection),
  };
}

const MAIL_WORKFLOW = defineWorkflow({
  id: "wf-under-test",
  trigger: { type: "mail", to: "wf@tenant.example" },
  mailAccept: { invoker: true, principals: ["prn_friend"] },
  steps: { push: action({ handler: "push" }) },
});

describe("gateAndFreezeProbeResult mail.accept walk markers", () => {
  test("refuses markers on a workflow that declares no mailAccept", async () => {
    const persist: PersistFrozenApprovalFn = async () => {
      throw new Error("freeze must not run when the gate rejects");
    };
    const markers = [
      "mail.accept:*",
      "mail.accept:principal:*",
      "mail.accept:tenant:tnt_other",
    ];

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      source: DEFINITION_SOURCE,
      probeResult: await probeReporting(
        defineWorkflow({
          id: "wf-under-test",
          trigger: { type: "manual" },
          steps: { push: action({ handler: "push" }) },
        }),
        markers,
      ),
      approvals: { kind: "approve-probed" },
      persist,
    });

    expect(result).toEqual({
      ok: false,
      reason: "mail_accept_markers_undeclared",
      undeclaredMailAcceptMarkers: markers,
    });
  });

  test("refuses a marker the declared mailAccept does not produce", async () => {
    const persist: PersistFrozenApprovalFn = async () => {
      throw new Error("freeze must not run when the gate rejects");
    };

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      source: DEFINITION_SOURCE,
      probeResult: await probeReporting(MAIL_WORKFLOW, [
        "mail.accept:invoker",
        "mail.accept:principal:prn_friend",
        "mail.accept:tenant",
      ]),
      approvals: { kind: "approve-probed" },
      persist,
    });

    expect(result).toEqual({
      ok: false,
      reason: "mail_accept_markers_undeclared",
      undeclaredMailAcceptMarkers: ["mail.accept:tenant"],
    });
  });

  test("refuses an undeclared marker that only the grant-walk snapshot reports", async () => {
    // Launch materializes accept-grants from the snapshot alone, so a sidecar
    // that keeps the advisory grants clean must not slip a marker past the
    // gate in a step record.
    const persist: PersistFrozenApprovalFn = async () => {
      throw new Error("freeze must not run when the gate rejects");
    };
    const probe = await probeReporting(MAIL_WORKFLOW, [
      "mail.accept:invoker",
      "mail.accept:tenant",
    ]);

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      source: DEFINITION_SOURCE,
      probeResult: { ...probe, grants: ["mail.accept:invoker"] },
      approvals: { kind: "approve-probed" },
      persist,
    });

    expect(result).toEqual({
      ok: false,
      reason: "mail_accept_markers_undeclared",
      undeclaredMailAcceptMarkers: ["mail.accept:tenant"],
    });
  });

  test("freezes the markers of a workflow with no mail trigger", async () => {
    // A run of any trigger has an address, so an HTTP-only workflow's
    // authored rules are approvable like any other.
    let persisted = 0;
    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      source: DEFINITION_SOURCE,
      probeResult: await probeReporting(
        defineWorkflow({
          id: "wf-under-test",
          trigger: { type: "manual" },
          mailAccept: { invoker: true, correspondent: true },
          steps: { push: action({ handler: "push" }) },
        }),
        ["mail.accept:invoker", "mail.accept:correspondent"],
      ),
      approvals: { kind: "approve-probed" },
      persist: async () => {
        persisted += 1;
        return { definitionId: "def-1" };
      },
    });

    expect(result.ok).toBe(true);
    expect(persisted).toBe(1);
  });

  test("freezes the markers the declared mailAccept produces", async () => {
    let persisted = 0;
    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      source: DEFINITION_SOURCE,
      probeResult: await probeReporting(MAIL_WORKFLOW, [
        "mail.accept:invoker",
        "mail.accept:principal:prn_friend",
      ]),
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
