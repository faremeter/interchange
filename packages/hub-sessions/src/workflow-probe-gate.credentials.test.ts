// Gate tests for credential bindings that cannot be delivered. The gate runs
// the check over the projection it freezes, before any credential is resolved,
// and refuses with every violation named.

import { describe, test, expect } from "bun:test";

import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type { CredentialBinding } from "@intx/types";
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

const persistMustNotRun: PersistFrozenApprovalFn = async () => {
  throw new Error("freeze must not run when the gate rejects");
};

// A probe answer for a one-action workflow whose shipped hash matches the hub
// recompute and whose grant walk records the step, so only the binding check
// can refuse it.
async function probeFor(
  credentialBindings: readonly CredentialBinding[],
): Promise<WorkflowProbeResult> {
  const projection = WorkflowProjectionDefinition.assert(
    projectLiveToInert(
      defineWorkflow({
        id: "wf-under-test",
        trigger: { type: "manual" },
        steps: {
          push: action({
            handler: "push",
            credentials: ["gh"],
            effect: { requires: ["call"] },
          }),
        },
        ...(credentialBindings.length > 0 ? { credentialBindings } : {}),
      }),
    ),
  );
  return {
    projection,
    grants: [],
    grantWalkSnapshot: {
      perStep: [{ stepId: "push", grants: [], grantEffects: {} }],
      grantRequirements: [],
    },
    wireHash: await computeWireDefinitionHash(projection),
  };
}

const pushBinding: CredentialBinding = {
  action: "push",
  handle: "gh",
  provider: "github",
  locator: "tenant",
};

describe("gateAndFreezeProbeResult credential bindings", () => {
  test("refuses undeliverable bindings, names every violation, and freezes nothing", async () => {
    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      source: DEFINITION_SOURCE,
      probeResult: await probeFor([{ ...pushBinding, handle: "npm" }]),
      approvals: { kind: "approve-probed" },
      persist: persistMustNotRun,
    });

    if (result.ok) throw new Error("expected a refusal");
    if (result.reason !== "credential_bindings_invalid") {
      throw new Error(`wrong reason: ${result.reason}`);
    }
    expect(result.violations).toEqual([
      { kind: "unbound_declared_handle", handler: "push", handle: "gh" },
      { kind: "undeclared_binding_handle", action: "push", handle: "npm" },
    ]);
    expect(result.message).toBe(
      'action push declares credential handle "gh", but no root binding names it for that handler; the binding for handle "npm" names action handler push, which does not declare that handle',
    );
  });

  test("approves deliverable bindings", async () => {
    let persisted = 0;
    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      source: DEFINITION_SOURCE,
      probeResult: await probeFor([pushBinding]),
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
