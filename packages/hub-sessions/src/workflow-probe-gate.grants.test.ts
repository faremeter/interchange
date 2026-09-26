// The approval gate's coverage of DECLARED GRANT REQUIREMENTS.
//
// A definition's declared requirements ride
// `probeResult.grantWalkSnapshot.grantRequirements`, a sibling of `projection`
// on the probe frame. They are not represented in `probeResult.grants`: the
// capability walk (`walkCapabilities`) never reads them, so the flattened walk
// surface cannot carry them. They are also outside the wire-hash preimage (the
// hash covers `projection` alone), so tamper-evidence says nothing about them
// either. The operator's decision is therefore the only thing standing between
// a declared requirement and the freeze.
//
// That matters because a frozen requirement is not an inert record. Both
// run-time materialization sites (the external trigger route and the
// mail-triggered run path) read the frozen list back and mint real grant rows
// onto the run principal from it, which ship to the child as the run's
// `stepGrants`.
//
// These cases assert the fail-closed property: a requirement the operator did
// not approve must be named in the rejection and must freeze nothing, a
// requirement the operator DID approve must pass, and an `approve-probed`
// approval must account for the requirement in the approval it returns.

import { describe, test, expect } from "bun:test";

import type { GrantRequirement } from "@intx/types";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import { createApprovalSet, type ApprovalSet } from "@intx/workflow-deploy";

import {
  gateAndFreezeProbeResult,
  type FrozenApproval,
  type PersistFrozenApprovalFn,
} from "./workflow-probe-gate";
import type { WorkflowProbeResult } from "./ws/sidecar-handler";

const PROJECTION: WorkflowProjectionDefinition = {
  id: "wf-forged-requirements",
  triggers: [],
  stepOrder: [],
  steps: {},
};

// A requirement the operator never saw. The resource and action strings are
// deliberately distinctive so `mentions` cannot match them incidentally against
// a projection or an unrelated grant string.
const FORGED_WILDCARD: GrantRequirement = {
  resource: "*",
  action: "*",
  source: "creator",
};

const FORGED_NAMED: GrantRequirement = {
  resource: "wallet:wal_forged_by_sidecar",
  action: "spend",
  source: "creator",
};

// A probe answer whose shipped hash matches the hub recompute over its
// projection, so the tamper-evidence check passes and the cases isolate the
// approval check. `grants` is the flattened walk surface the gate filters;
// `grantRequirements` is the declared list it does not.
async function makeProbeResult(overrides: {
  grants: string[];
  grantRequirements: GrantRequirement[];
}): Promise<WorkflowProbeResult> {
  return {
    projection: PROJECTION,
    grants: overrides.grants,
    grantWalkSnapshot: {
      perStep: overrides.grants.map((grant, index) => ({
        stepId: `s${String(index)}`,
        grants: [grant],
        grantEffects: grant.startsWith("tool:")
          ? { [grant]: "allow" as const }
          : {},
      })),
      grantRequirements: overrides.grantRequirements,
    },
    wireHash: await computeWireDefinitionHash(PROJECTION),
  };
}

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

// Does the gate's answer account for this requirement anywhere a caller can
// see it? A correct gate must name an unapproved requirement in its rejection,
// and must record an approved one in the approval it returns. The two arms
// carry it in different fields (`unapprovedGrantRequirements` on the rejection,
// `approvedSurface.requirements` on the approval), so this scans the serialized
// answer for the requirement's resource and action and lets each case assert
// the exact field where that field is the point.
function mentions(value: unknown, requirement: GrantRequirement): boolean {
  const serialized = JSON.stringify(value, (_key, v: unknown) =>
    v instanceof Set ? [...v] : v,
  );
  return (
    serialized.includes(requirement.resource) &&
    serialized.includes(requirement.action)
  );
}

describe("gateAndFreezeProbeResult declared grant requirements", () => {
  test("rejects creator requirements from an asset tarball before freezing", async () => {
    const probeResult = await makeProbeResult({
      grants: [],
      grantRequirements: [FORGED_NAMED],
    });
    const { persist, calls } = recordingPersist("def-registry-creator");

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      source: {
        kind: "asset",
        assetId: "registry-1",
        package: { format: "tarball" },
      },
      probeResult,
      approvals: { kind: "approve-probed" },
      persist,
    });

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected rejection");
    expect(result.reason).toBe(
      "creator_requirements_unsupported_for_package_registry_tarball",
    );
    if (
      result.reason !==
      "creator_requirements_unsupported_for_package_registry_tarball"
    ) {
      throw new Error("wrong reason");
    }
    expect(result.creatorGrantRequirements).toEqual([FORGED_NAMED]);
    expect(calls).toEqual([]);
  });

  test("allows an asset tarball without creator requirements", async () => {
    const invokerRequirement: GrantRequirement = {
      resource: "wallet:wal_invoker",
      action: "spend",
      source: "invoker",
    };
    const probeResult = await makeProbeResult({
      grants: [],
      grantRequirements: [invokerRequirement],
    });
    const { persist, calls } = recordingPersist("def-registry-invoker");

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      source: {
        kind: "asset",
        assetId: "registry-1",
        package: { format: "tarball" },
      },
      probeResult,
      approvals: { kind: "approve-probed" },
      persist,
    });

    expect(result.ok).toBe(true);
    expect(calls).toHaveLength(1);
  });

  test("allows creator requirements from an asset source tree", async () => {
    const probeResult = await makeProbeResult({
      grants: [],
      grantRequirements: [FORGED_NAMED],
    });
    const { persist, calls } = recordingPersist("def-source-creator");

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      source: {
        kind: "asset",
        assetId: "workflow-source-1",
        package: {
          format: "source",
          commitSha: "0123456789abcdef0123456789abcdef01234567",
        },
      },
      probeResult,
      approvals: { kind: "approve-probed" },
      persist,
    });

    expect(result.ok).toBe(true);
    expect(calls).toHaveLength(1);
  });

  test("rejects a declared requirement the operator's ApprovalSet does not cover", async () => {
    // The operator approved nothing at all. The probe advertises no walk
    // grants, so the gate's `grants` filter finds nothing to object to -- but
    // the answer declares a wildcard creator requirement.
    const probeResult = await makeProbeResult({
      grants: [],
      grantRequirements: [FORGED_WILDCARD],
    });
    const approvals: ApprovalSet = createApprovalSet([]);
    const { persist, calls } = recordingPersist("def-forged-wildcard");

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist,
    });

    expect(result.ok).toBe(false);
    expect(mentions(result, FORGED_WILDCARD)).toBe(true);
    // Fail closed: nothing reaches the version row.
    expect(calls).toEqual([]);
  });

  test("rejects a declared requirement even when every advertised grant is approved", async () => {
    // The operator approved the full advertised walk surface. That decision
    // says nothing about the declared requirement riding alongside it, so the
    // gate must still fail closed.
    const probeResult = await makeProbeResult({
      grants: ["tool:fetch", "effect:log"],
      grantRequirements: [FORGED_NAMED],
    });
    const approvals: ApprovalSet = createApprovalSet([
      "tool:fetch",
      "effect:log",
    ]);
    const { persist, calls } = recordingPersist("def-forged-named");

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist,
    });

    expect(result.ok).toBe(false);
    expect(mentions(result, FORGED_NAMED)).toBe(true);
    expect(calls).toEqual([]);
  });

  test("never freezes a grant requirement absent from the ApprovalSet", async () => {
    // The freeze is what the run path reads back to mint grant rows, so the
    // requirement must not reach `grantSnapshot` on any rejected approval.
    const probeResult = await makeProbeResult({
      grants: ["tool:fetch"],
      grantRequirements: [FORGED_WILDCARD, FORGED_NAMED],
    });
    const approvals: ApprovalSet = createApprovalSet(["tool:fetch"]);
    const { persist, calls } = recordingPersist("def-no-freeze");

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist,
    });

    // Assert the rejection itself, not only that nothing was frozen. A gate
    // that approved both forged requirements and merely dropped them from the
    // snapshot would satisfy an is-the-freeze-empty check while failing open,
    // which is the direction this suite exists to close.
    expect(result.ok).toBe(false);
    expect(mentions(result, FORGED_WILDCARD)).toBe(true);
    expect(mentions(result, FORGED_NAMED)).toBe(true);
    expect(calls).toEqual([]);
  });

  test("approve-probed accounts for the declared requirements in the approval it returns", async () => {
    // `approve-probed` has no set to gate against: the probe's surface IS the
    // approved surface, so freezing a declared requirement is consistent with
    // the mode. What would not be consistent is returning an approval record
    // that omits it -- the ok-arm is the deploy hand-off's and the audit
    // trail's account of what was approved, so it must name every kind of
    // authority the freeze will mint, not just the flattened walk grants.
    const probeResult = await makeProbeResult({
      grants: ["tool:fetch"],
      grantRequirements: [FORGED_NAMED],
    });
    const { persist, calls } = recordingPersist("def-approve-probed");

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals: { kind: "approve-probed" },
      persist,
    });

    expect(result.ok).toBe(true);
    // The requirement WAS frozen -- that half already holds.
    expect(calls).toHaveLength(1);
    const frozen = calls[0];
    if (frozen === undefined) throw new Error("no freeze recorded");
    expect(frozen.grantSnapshot.grantRequirements).toEqual([FORGED_NAMED]);
    // The approval the gate hands back must say so too, in the field the
    // vocabulary reserves for requirements.
    expect(mentions(result, FORGED_NAMED)).toBe(true);
    if (!result.ok) throw new Error("expected approval");
    expect(result.approvedSurface.requirements).toEqual([FORGED_NAMED]);
  });

  test("accepts and freezes a declared requirement the ApprovalSet covers", async () => {
    // The counterweight to the rejection cases above: a gate that refused every
    // requirement would satisfy them and break every deploy that declares one.
    // The approved item is a distinct object with the same fields, so this also
    // pins that the membership test is structural rather than by identity.
    const approved: GrantRequirement = {
      resource: "wallet:wal_forged_by_sidecar",
      action: "spend",
      source: "creator",
    };
    const probeResult = await makeProbeResult({
      grants: ["tool:fetch"],
      grantRequirements: [FORGED_NAMED],
    });
    const approvals: ApprovalSet = createApprovalSet(
      ["tool:fetch"],
      [approved],
    );
    const { persist, calls } = recordingPersist("def-covered");

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist,
    });

    expect(result.ok).toBe(true);
    expect(calls).toHaveLength(1);
    const frozen = calls[0];
    if (frozen === undefined) throw new Error("no freeze recorded");
    expect(frozen.grantSnapshot.grantRequirements).toEqual([FORGED_NAMED]);
    // The approved surface names the requirement alongside the walk grant, each
    // in its own field.
    expect(mentions(result, FORGED_NAMED)).toBe(true);
    if (!result.ok) throw new Error("expected approval");
    expect(result.approvedSurface.requirements).toEqual([FORGED_NAMED]);
    expect([...result.approvedSurface.grants]).toEqual(["tool:fetch"]);
  });

  test("rejects a requirement that differs from the approved one only in source", async () => {
    // `source` decides WHOSE authority the run path delegates, so an approval
    // of the creator-sourced form says nothing about the invoker-sourced one.
    const probeResult = await makeProbeResult({
      grants: [],
      grantRequirements: [
        {
          resource: "wallet:wal_forged_by_sidecar",
          action: "spend",
          source: "invoker",
        },
      ],
    });
    const approvals: ApprovalSet = createApprovalSet([], [FORGED_NAMED]);
    const { persist, calls } = recordingPersist("def-source-mismatch");

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist,
    });

    expect(result.ok).toBe(false);
    expect(calls).toEqual([]);
  });

  test("rejects a requirement whose conditions the approval does not carry", async () => {
    // `conditions` narrows when the materialized row applies, so dropping them
    // widens the authority beyond what the operator agreed to.
    const conditioned: GrantRequirement = {
      resource: "wallet:wal_forged_by_sidecar",
      action: "spend",
      source: "creator",
      conditions: { tool_consumer: "wf-forged-requirements" },
    };
    const probeResult = await makeProbeResult({
      grants: [],
      grantRequirements: [FORGED_NAMED],
    });
    const approvals: ApprovalSet = createApprovalSet([], [conditioned]);
    const { persist, calls } = recordingPersist("def-conditions-mismatch");

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist,
    });

    expect(result.ok).toBe(false);
    expect(calls).toEqual([]);
  });
});
