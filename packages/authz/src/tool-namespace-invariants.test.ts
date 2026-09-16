// How a wildcard grant row and a bare `tool:<name>` row each fare against a
// PINNED tool's runtime gate.
//
// A pinned tool package is loaded through the namespacing loader, which
// rewrites every definition name to `<bundleId>:<name>`. The workflow child
// gates each call on `tool:<call.name>`, so the query it issues for a pinned
// tool is `tool:<bundleId>:<name>`. The deploy-time capability walk, by
// contrast, reads inline `agent.toolFactories`, which carry no bundle context,
// so the rows it contributes are bare `tool:<name>`.
//
// That asymmetry is what makes the namespaced form a step-scoping mechanism:
// a bare row cannot address a pinned tool's gate, and the authority a pinned
// tool actually needs is supplied per step by the sidecar's tool-mark floor,
// derived from the factories that step loaded. These cases pin what a wildcard
// row does to that scoping.

import { describe, test, expect } from "bun:test";

import { evaluateGrants } from "./evaluate";
import type { GrantRule } from "./types";

const BUNDLE_ID = "tp_pinned_bundle";
const TOOL_NAME = "search";
const NAMESPACED_GATE = `tool:${BUNDLE_ID}:${TOOL_NAME}`;
const BARE_GATE = `tool:${TOOL_NAME}`;
const INVOKE = "invoke";

function grant(
  overrides: Partial<GrantRule> &
    Pick<GrantRule, "resource" | "action" | "effect">,
): GrantRule {
  return {
    id: `grt_${Math.random().toString(36).slice(2, 10)}`,
    origin: "creator",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: null,
    ...overrides,
  };
}

describe("a wildcard row reaches a pinned tool's namespaced gate", () => {
  test("*/* resolves to allow for tool:<bundleId>:<name>/invoke", async () => {
    const result = await evaluateGrants(
      [grant({ resource: "*", action: "*", effect: "allow" })],
      NAMESPACED_GATE,
      INVOKE,
    );
    expect(result.effect).toBe("allow");
  });

  test("the walk's bare tool:<name> row does NOT reach that gate", async () => {
    const result = await evaluateGrants(
      [grant({ resource: BARE_GATE, action: INVOKE, effect: "allow" })],
      NAMESPACED_GATE,
      INVOKE,
    );
    expect(result.effect).toBe(null);
  });

  test("the bare row is operative against a bare, unpinned gate", async () => {
    const result = await evaluateGrants(
      [grant({ resource: BARE_GATE, action: INVOKE, effect: "allow" })],
      BARE_GATE,
      INVOKE,
    );
    expect(result.effect).toBe("allow");
  });

  test("a wildcard row reaches a second bundle's tool the step never loaded", async () => {
    const result = await evaluateGrants(
      [grant({ resource: "*", action: "*", effect: "allow" })],
      "tool:tp_other_bundle:transfer",
      INVOKE,
    );
    expect(result.effect).toBe("allow");
  });
});

describe("the tool-mark floor still outranks a wildcard row", () => {
  test("an ask floor beats */* allow at higher specificity", async () => {
    const floor = grant({
      resource: NAMESPACED_GATE,
      action: INVOKE,
      effect: "ask",
    });
    const wildcard = grant({ resource: "*", action: "*", effect: "allow" });
    const result = await evaluateGrants(
      [wildcard, floor],
      NAMESPACED_GATE,
      INVOKE,
    );
    expect(result.effect).toBe("ask");
  });
});
