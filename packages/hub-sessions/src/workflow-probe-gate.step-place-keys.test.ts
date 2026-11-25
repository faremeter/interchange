// Gate tests for the length of an agent step's state key.
//
// A step's state lives in a directory named for its URL-encoded place key:
// its path of step ids joined with ".", plus "[i]" for a map item. A nested
// step's key joins its whole path, so the gate refuses a deployment whose
// executable closure holds a key the repo cannot hold, rather than let the
// step fail when it runs.

import { describe, expect, test } from "bun:test";

import type { GrantWalkSnapshot } from "@intx/types";
import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";

import {
  gateAndFreezeProbeResult,
  type PersistFrozenApprovalFn,
} from "./workflow-probe-gate";

const DEFINITION_SOURCE: WorkflowDefinitionSource = {
  kind: "asset",
  assetId: "asset-1",
  package: {
    format: "source",
    commitSha: "0123456789abcdef0123456789abcdef01234567",
  },
};

const AGENT = {
  id: "ag",
  toolFactories: [],
  modelSources: [{ provider: "anthropic", model: "claude-3" }],
};

function agentStep(): Record<string, unknown> {
  return { kind: "step", agent: AGENT };
}

function projection(
  id: string,
  steps: Record<string, unknown>,
): WorkflowProjectionDefinition {
  return {
    id,
    triggers: [{ type: "manual" }],
    stepOrder: Object.keys(steps),
    steps,
  };
}

/** A childWorkflow step whose inline child runs `steps`. */
function child(id: string, steps: Record<string, unknown>) {
  return {
    kind: "childWorkflow",
    definition: { inline: projection(id, steps) },
  };
}

const persist: PersistFrozenApprovalFn = async () => ({
  definitionId: "def-1",
});

async function gate(definition: WorkflowProjectionDefinition) {
  const grantWalkSnapshot: GrantWalkSnapshot = {
    perStep: definition.stepOrder.map((stepId) => ({
      stepId,
      grants: [],
      grantEffects: {},
    })),
    grantRequirements: [],
  };
  return gateAndFreezeProbeResult({
    assetId: "asset-1",
    source: DEFINITION_SOURCE,
    probeResult: {
      projection: definition,
      grants: [],
      grantWalkSnapshot,
      wireHash: await computeWireDefinitionHash(definition),
    },
    approvals: { kind: "approve-probed" },
    persist,
  });
}

describe("gateAndFreezeProbeResult step place key length", () => {
  test("refuses a childWorkflow child step whose key is too long", async () => {
    // 20 + 1 + 240 = 261 bytes; each id alone fits in a path segment.
    const parent = "p".repeat(20);
    const inner = "c".repeat(240);
    const result = await gate(
      projection("wf", {
        [parent]: child("kid", { [inner]: agentStep() }),
      }),
    );

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected rejection");
    expect(result.reason).toBe("step_place_keys_too_long");
    if (result.reason !== "step_place_keys_too_long") {
      throw new Error("wrong reason");
    }
    expect(result.overlongStepPlaceKeys).toEqual([
      {
        reachedThrough: [parent, inner],
        key: `${parent}.${inner}`,
        segmentBytes: 261,
      },
    ]);
    expect(result.message).toContain("limited to 255 bytes");
    expect(result.message).toContain(`${parent} > ${inner}`);
  });

  test("accepts a nested key exactly at the limit", async () => {
    // 20 + 1 + 234 = 255 bytes.
    const result = await gate(
      projection("wf", {
        ["p".repeat(20)]: child("kid", { ["c".repeat(234)]: agentStep() }),
      }),
    );

    expect(result.ok).toBe(true);
  });

  test("measures a map at the largest index its items can have", async () => {
    // `m[4294967294]` encodes the brackets and 10 digits in 16 bytes.
    const fits = "m".repeat(255 - 16);
    const accepted = await gate(
      projection("wf", { [fits]: { kind: "map", step: { agent: AGENT } } }),
    );
    expect(accepted.ok).toBe(true);

    const tooLong = `${fits}m`;
    const refused = await gate(
      projection("wf", { [tooLong]: { kind: "map", step: { agent: AGENT } } }),
    );
    expect(refused.ok).toBe(false);
    if (refused.ok || refused.reason !== "step_place_keys_too_long") {
      throw new Error("expected a step place key refusal");
    }
    expect(refused.overlongStepPlaceKeys.map((s) => s.key)).toEqual([
      `${tooLong}[4294967294]`,
    ]);
  });

  test("refuses a chain of children whose key is too long only at a deep level", async () => {
    // Each level's id is 30 bytes, so the key grows by 31 bytes per level:
    // 7 levels and a 30-byte agent step id make 7 * 31 + 30 = 247 bytes, and
    // an eighth level makes 278. The shallow agent steps all fit.
    const level = (n: number) => `${String(n)}`.padStart(30, "x");
    let steps: Record<string, unknown> = { [level(99)]: agentStep() };
    for (let depth = 8; depth >= 1; depth -= 1) {
      steps = {
        [`a${level(depth).slice(1)}`]: agentStep(),
        [level(depth)]: child(`kid-${String(depth)}`, steps),
      };
    }
    const result = await gate(projection("wf", steps));

    expect(result.ok).toBe(false);
    if (result.ok || result.reason !== "step_place_keys_too_long") {
      throw new Error("expected a step place key refusal");
    }
    expect(result.overlongStepPlaceKeys).toHaveLength(1);
    const [overlong] = result.overlongStepPlaceKeys;
    expect(overlong?.reachedThrough).toHaveLength(9);
    expect(overlong?.segmentBytes).toBe(278);
  });

  test("accepts the same chain one level shallower", async () => {
    const level = (n: number) => `${String(n)}`.padStart(30, "x");
    let steps: Record<string, unknown> = { [level(99)]: agentStep() };
    for (let depth = 7; depth >= 1; depth -= 1) {
      steps = { [level(depth)]: child(`kid-${String(depth)}`, steps) };
    }

    expect((await gate(projection("wf", steps))).ok).toBe(true);
  });
});
