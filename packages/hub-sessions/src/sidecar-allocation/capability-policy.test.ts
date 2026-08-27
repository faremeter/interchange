import { describe, expect, test } from "bun:test";

import { matchSidecarCapabilityPolicy } from "./capability-policy";

describe("matchSidecarCapabilityPolicy", () => {
  test("uses grant specificity for workflow exceptions", () => {
    expect(
      matchSidecarCapabilityPolicy(
        {
          tenantPolicies: [],
          workflowRules: [
            { capability: "runtime:*", effect: "block" },
            { capability: "runtime:browser", effect: "require" },
          ],
        },
        [
          { capability: "runtime:*", state: "blocked" },
          { capability: "runtime:browser", state: "available" },
        ],
      ),
    ).toEqual({ ok: true });
  });

  test("requires every tenant layer and the workflow to be satisfied", () => {
    const match = matchSidecarCapabilityPolicy(
      {
        tenantPolicies: [
          {
            tenantId: "parent",
            rules: [{ capability: "runtime:*", effect: "block" }],
          },
        ],
        workflowRules: [{ capability: "runtime:browser", effect: "require" }],
      },
      [
        { capability: "runtime:*", state: "blocked" },
        { capability: "runtime:browser", state: "available" },
      ],
    );

    expect(match.ok).toBe(false);
    if (!match.ok) {
      expect(match.mismatches).toContainEqual({
        capability: "runtime:browser",
        expected: "blocked",
        actual: "available",
        rule: { capability: "runtime:*", effect: "block" },
        source: { kind: "tenant", tenantId: "parent" },
      });
    }
  });

  test("treats an omitted provisioner capability as unknown", () => {
    expect(
      matchSidecarCapabilityPolicy(
        {
          tenantPolicies: [],
          workflowRules: [{ capability: "runtime:posix", effect: "block" }],
        },
        [],
      ),
    ).toEqual({
      ok: false,
      mismatches: [
        {
          capability: "runtime:posix",
          expected: "blocked",
          actual: "unknown",
          rule: { capability: "runtime:posix", effect: "block" },
          source: { kind: "workflow" },
        },
      ],
    });
  });

  test("detects provisioner exceptions to broad workflow rules", () => {
    const match = matchSidecarCapabilityPolicy(
      {
        tenantPolicies: [],
        workflowRules: [{ capability: "runtime:*", effect: "block" }],
      },
      [
        { capability: "runtime:*", state: "blocked" },
        { capability: "runtime:posix", state: "available" },
      ],
    );

    expect(match.ok).toBe(false);
    if (!match.ok) {
      expect(match.mismatches).toContainEqual({
        capability: "runtime:posix",
        expected: "blocked",
        actual: "available",
        rule: { capability: "runtime:*", effect: "block" },
        source: { kind: "workflow" },
      });
    }
  });

  test("detects nested namespace exceptions to broad workflow rules", () => {
    const match = matchSidecarCapabilityPolicy(
      {
        tenantPolicies: [],
        workflowRules: [{ capability: "network:*", effect: "block" }],
      },
      [
        { capability: "network:*", state: "blocked" },
        { capability: "network:production:*", state: "available" },
      ],
    );

    expect(match.ok).toBe(false);
    if (!match.ok) {
      expect(match.mismatches).toContainEqual({
        capability: "network:production:*",
        expected: "blocked",
        actual: "available",
        rule: { capability: "network:*", effect: "block" },
        source: { kind: "workflow" },
      });
    }
  });

  test("fails closed when called directly with an invalid selector", () => {
    expect(() =>
      matchSidecarCapabilityPolicy(
        {
          tenantPolicies: [],
          workflowRules: [
            { capability: "network:*:external", effect: "block" },
          ],
        },
        [],
      ),
    ).toThrow(/Invalid sidecar capability selector/);
  });

  test("lets blocked win at equal specificity", () => {
    expect(
      matchSidecarCapabilityPolicy(
        {
          tenantPolicies: [],
          workflowRules: [
            { capability: "runtime:browser", effect: "require" },
            { capability: "runtime:browser", effect: "block" },
          ],
        },
        [{ capability: "runtime:browser", state: "blocked" }],
      ),
    ).toEqual({ ok: true });
  });
});
