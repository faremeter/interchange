import { describe, expect, test } from "bun:test";
import { type } from "arktype";

import {
  parseSidecarCapabilitySelector,
  SidecarCapabilityDeclaration,
  SidecarCapabilityRule,
} from "./sidecar-capabilities";
import { UpdateTenant } from "./tenants";

describe("sidecar capabilities", () => {
  test("accepts exact, namespace-prefix, and global selectors", () => {
    expect(parseSidecarCapabilitySelector("runtime:browser")).toEqual({
      kind: "exact",
      segments: ["runtime", "browser"],
    });
    expect(parseSidecarCapabilitySelector("network:production:*")).toEqual({
      kind: "prefix",
      segments: ["network", "production"],
    });
    expect(parseSidecarCapabilitySelector("*")).toEqual({
      kind: "prefix",
      segments: [],
    });
  });

  test("rejects malformed selectors", () => {
    for (const capability of [
      "network:*:external",
      "network:production*",
      "network:**",
      "runtime:",
      "a::b",
      ":*",
    ]) {
      expect(
        SidecarCapabilityRule({ capability, effect: "block" }) instanceof
          type.errors,
      ).toBe(true);
      expect(
        SidecarCapabilityDeclaration({
          capability,
          state: "blocked",
        }) instanceof type.errors,
      ).toBe(true);
    }
  });

  test("rejects undeclared tenant policy fields", () => {
    expect(
      UpdateTenant({
        config: {
          sidecarPlacement: {
            capabilites: [{ capability: "network:outbound", effect: "block" }],
          },
        },
      }) instanceof type.errors,
    ).toBe(true);
    expect(
      UpdateTenant({
        config: {
          sidecarPlacement: {
            capabilities: [{ capability: "network:outbound", effect: "block" }],
          },
        },
      }) instanceof type.errors,
    ).toBe(false);
  });
});
