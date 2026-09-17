// Durability-boundary compatibility checks for the approval vocabulary.
//
// The branch widened `FrozenApprovalBundle.approvedGrants` from `string[]` to
// an `ApprovalItem[]` union and split the in-memory `ApprovalSet` from a bare
// `ReadonlySet<string>` into a struct carrying a `kind` discriminant. Both
// halves touch a persisted jsonb column
// (`workflow_run_launch_spec.frozen_approval_bundle`), so these tests exercise
// the real row-parsing boundary rather than the type alone.

import { describe, expect, test } from "bun:test";
import { type } from "arktype";

import { parseWorkflowRunLaunchSpecRow, schema } from "@intx/db";

import { FrozenApprovalBundle } from "@intx/types/sidecar";
import {
  approvalItemsFromSet,
  approvalSetFromItems,
  createApprovalSet,
} from "@intx/workflow-deploy";

type LaunchSpecRow = typeof schema.workflowRunLaunchSpec.$inferSelect;

const projection = {
  id: "w",
  stepOrder: ["a"],
  steps: { a: { kind: "step", id: "a" } },
  triggers: [],
};

// Exactly the JSON an `origin/main` build wrote: `approvedGrants` is a flat
// list of grant-shape strings and nothing else.
function legacyRow(approvedGrants: unknown): LaunchSpecRow {
  return {
    anchorRunId: "run_legacy",
    schemaVersion: 1,
    sessionId: "sess_1",
    deploymentDomain: "deploy.example",
    sourceAuthorityPrincipalId: "prin_1",
    frozenApprovalBundle: {
      source: {
        kind: "registry",
        registry: "npm",
        package: { packageName: "p", version: "1.0.0" },
      },
      entry: "./src/workflow.ts",
      projection,
      closure: { schemaVersion: "1", topLevel: [], entries: [] },
      approvedWireHash: "deadbeef",
      approvedGrants,
    },
    sourceOfferingIds: ["off_1"],
    defaultSourceOfferingId: "off_1",
    deployContent: {},
    toolPackagePins: null,
    createdAt: new Date("2026-01-01T00:00:00Z"),
  };
}

describe("workflow_run_launch_spec rows written by an older build", () => {
  test("a string-only approvedGrants list still parses at the row boundary", () => {
    const parsed = parseWorkflowRunLaunchSpecRow(
      legacyRow(["tool:x", "inference.source:anthropic:m", "director:d"]),
    );
    expect(parsed.frozenApprovalBundle.approvedGrants).toEqual([
      "tool:x",
      "inference.source:anthropic:m",
      "director:d",
    ]);
  });

  test("the legacy list rehydrates into an ApprovalSet with no requirements", () => {
    const parsed = parseWorkflowRunLaunchSpecRow(
      legacyRow(["tool:x", "inference.source:anthropic:m"]),
    );
    const surface = approvalSetFromItems(
      parsed.frozenApprovalBundle.approvedGrants,
    );
    expect(surface.kind).toBe("approval-set");
    expect([...surface.grants]).toEqual([
      "tool:x",
      "inference.source:anthropic:m",
    ]);
    expect(surface.requirements).toEqual([]);
    // This is what `orchestrator.isSourceApproved` consults on the redeploy
    // path; a legacy row must still answer it.
    expect(surface.grants.has("inference.source:anthropic:m")).toBe(true);
  });

  test("a mixed list written by this build parses and partitions", () => {
    const requirement = {
      resource: "tool:*",
      action: "invoke",
      source: "creator",
    } as const;
    const parsed = parseWorkflowRunLaunchSpecRow(
      legacyRow(["tool:x", requirement]),
    );
    const surface = approvalSetFromItems(
      parsed.frozenApprovalBundle.approvedGrants,
    );
    expect([...surface.grants]).toEqual(["tool:x"]);
    expect(surface.requirements).toEqual([requirement]);
  });
});

describe("the ApprovalSet discriminant never reaches the persisted form", () => {
  test("approvalItemsFromSet emits only grant strings and requirements", () => {
    const set = createApprovalSet(
      ["tool:x"],
      [{ resource: "tool:*", action: "invoke", source: "creator" }],
    );
    const items = approvalItemsFromSet(set);
    expect(items).toEqual([
      "tool:x",
      { resource: "tool:*", action: "invoke", source: "creator" },
    ]);
    expect(items).not.toContain("approval-set");
    for (const item of items) {
      if (typeof item !== "string") {
        expect(Object.keys(item)).not.toContain("kind");
      }
    }
  });

  test("a bundle built from approvalItemsFromSet parses as FrozenApprovalBundle", () => {
    const set = createApprovalSet(
      ["tool:x"],
      [{ resource: "tool:*", action: "invoke", source: "creator" }],
    );
    const bundle = FrozenApprovalBundle({
      source: {
        kind: "registry",
        registry: "npm",
        package: { packageName: "p", version: "1.0.0" },
      },
      entry: "./src/workflow.ts",
      projection,
      closure: { schemaVersion: "1", topLevel: [], entries: [] },
      approvedWireHash: "deadbeef",
      approvedGrants: [...approvalItemsFromSet(set)],
    });
    if (bundle instanceof type.errors) {
      throw new Error(`round-tripped bundle rejected: ${bundle.summary}`);
    }
    // Survives the JSON round trip a jsonb column imposes. The column stores
    // the whole bundle, so re-validating the bundle is the honest boundary
    // check -- and it needs no assertion, which asserting a shape onto
    // `JSON.parse`'s `any` would.
    const raw: unknown = JSON.parse(JSON.stringify(bundle));
    const reparsed = FrozenApprovalBundle(raw);
    if (reparsed instanceof type.errors) {
      throw new Error(
        `bundle rejected after a JSON round trip: ${reparsed.summary}`,
      );
    }
    const rehydrated = approvalSetFromItems(reparsed.approvedGrants);
    expect([...rehydrated.grants]).toEqual([...set.grants]);
    expect(rehydrated.requirements).toEqual(set.requirements);
  });
});

describe("the sidecar.test.ts compatibility fixture is not vacuous", () => {
  // The branch's own compatibility block asserts that a malformed entry is
  // refused. That assertion would pass for free if the surrounding bundle were
  // invalid for an unrelated reason, so pin the base fixture as valid here.
  const base = {
    source: {
      kind: "registry",
      registry: "npm",
      package: { packageName: "p", version: "1.0.0" },
    },
    entry: "./src/workflow.ts",
    projection,
    closure: { schemaVersion: "1", topLevel: [], entries: [] },
    approvedWireHash: "deadbeef",
  };

  test("the base fixture with an empty approvedGrants list is valid", () => {
    const parsed = FrozenApprovalBundle({ ...base, approvedGrants: [] });
    expect(parsed instanceof type.errors).toBe(false);
  });

  test("a requirement missing action/source is refused, not silently accepted", () => {
    const parsed = FrozenApprovalBundle({
      ...base,
      approvedGrants: [{ resource: "tool:*" }],
    });
    expect(parsed instanceof type.errors).toBe(true);
  });
});
