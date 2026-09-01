import { describe, expect, test } from "bun:test";

import type { SidecarProvisioner } from "./contracts";
import { createSidecarPluginRegistry } from "./plugin-registry";

function provisioner(
  id: string,
  bindingFingerprint = "test-backend:v1",
  capabilities: SidecarProvisioner["capabilities"] = [],
): SidecarProvisioner {
  return {
    id,
    apiVersion: 1,
    bindingFingerprint,
    capabilities,
    async ensure() {
      return { kind: "accepted" };
    },
    async destroy() {
      return { kind: "destroyed" };
    },
  };
}

describe("createSidecarPluginRegistry", () => {
  test("resolves registered provisioners", () => {
    const containers = provisioner("containers");
    const virtualMachines = provisioner("virtual-machines");
    const registry = createSidecarPluginRegistry({
      provisioners: [containers, virtualMachines],
    });

    expect(registry.getProvisioner("containers")).toBe(containers);
    expect(registry.getProvisioner("virtual-machines")).toBe(virtualMachines);
    expect(registry.getProvisioner("missing")).toBeNull();
  });

  test("rejects duplicate provisioner ids", () => {
    expect(() =>
      createSidecarPluginRegistry({
        provisioners: [provisioner("same"), provisioner("same")],
      }),
    ).toThrow(/Duplicate sidecar provisioner id/);
  });

  test("rejects blank ids and binding fingerprints", () => {
    expect(() =>
      createSidecarPluginRegistry({
        provisioners: [provisioner(" ")],
      }),
    ).toThrow(/id must be non-empty/);
    expect(() =>
      createSidecarPluginRegistry({
        provisioners: [provisioner("containers", " ")],
      }),
    ).toThrow(/requires a binding fingerprint/);
  });

  test("rejects unsupported provisioner API versions at runtime", () => {
    expect(() =>
      createSidecarPluginRegistry({
        provisioners: [
          {
            ...provisioner("containers"),
            // @ts-expect-error Exercises validation for JavaScript plugins.
            apiVersion: 2,
          },
        ],
      }),
    ).toThrow(/Unsupported sidecar provisioner API version/);
  });

  test("rejects invalid capability declarations at runtime", () => {
    expect(() =>
      createSidecarPluginRegistry({
        provisioners: [
          {
            ...provisioner("containers"),
            capabilities: [
              {
                capability: "runtime:browser",
                // @ts-expect-error Exercises validation for JavaScript plugins.
                state: "sometimes",
              },
            ],
          },
        ],
      }),
    ).toThrow(/Invalid capability declarations/);
  });

  test("rejects capability declarations with non-hierarchical wildcards", () => {
    expect(() =>
      createSidecarPluginRegistry({
        provisioners: [
          provisioner("containers", "test-backend:v1", [
            { capability: "network:*:external", state: "blocked" },
          ]),
        ],
      }),
    ).toThrow(/Invalid capability declarations/);
  });

  test("selects the first matching provisioner", async () => {
    const containers = provisioner("containers", "containers:v1", [
      { capability: "runtime:browser", state: "available" },
    ]);
    const virtualMachines = provisioner("virtual-machines", "vms:v1", [
      { capability: "runtime:browser", state: "available" },
    ]);
    const registry = createSidecarPluginRegistry({
      provisioners: [containers, virtualMachines],
    });

    expect(
      await registry.selectProvisioner({
        tenantPolicies: [],
        workflowRules: [{ capability: "runtime:browser", effect: "require" }],
      }),
    ).toEqual({ ok: true, provisioner: containers });
  });

  test("lets a custom chooser select from every match", async () => {
    const containers = provisioner("containers", "containers:v1", [
      { capability: "runtime:browser", state: "available" },
    ]);
    const virtualMachines = provisioner("virtual-machines", "vms:v1", [
      { capability: "runtime:browser", state: "available" },
    ]);
    const seen: (readonly SidecarProvisioner[])[] = [];
    const registry = createSidecarPluginRegistry({
      provisioners: [containers, virtualMachines],
      chooser: async (candidates) => {
        seen.push(candidates);
        return virtualMachines;
      },
    });

    expect(
      await registry.selectProvisioner({
        tenantPolicies: [],
        workflowRules: [{ capability: "runtime:browser", effect: "require" }],
      }),
    ).toEqual({ ok: true, provisioner: virtualMachines });
    expect(seen).toEqual([[containers, virtualMachines]]);
  });

  test("rejects a chooser result outside the matching candidates", async () => {
    const containers = provisioner("containers");
    const registry = createSidecarPluginRegistry({
      provisioners: [containers],
      chooser: () => provisioner("unregistered"),
    });

    await expect(
      registry.selectProvisioner({ tenantPolicies: [], workflowRules: [] }),
    ).rejects.toThrow(/outside the matching candidates/);
  });

  test("skips earlier provisioners that do not match", async () => {
    const containers = provisioner("containers", "containers:v1", [
      { capability: "platform:ios", state: "blocked" },
    ]);
    const ios = provisioner("ios", "ios:v1", [
      { capability: "platform:ios", state: "available" },
    ]);
    const registry = createSidecarPluginRegistry({
      provisioners: [containers, ios],
    });

    expect(
      await registry.selectProvisioner({
        tenantPolicies: [],
        workflowRules: [{ capability: "platform:ios", effect: "require" }],
      }),
    ).toEqual({ ok: true, provisioner: ios });
  });

  test("reports each provisioner's capability mismatches", async () => {
    const registry = createSidecarPluginRegistry({
      provisioners: [provisioner("containers")],
    });

    const selection = await registry.selectProvisioner({
      tenantPolicies: [],
      workflowRules: [{ capability: "platform:ios", effect: "require" }],
    });
    expect(selection.ok).toBe(false);
    if (!selection.ok && selection.reason === "no_match") {
      expect(selection.mismatches["containers"]?.[0]).toEqual({
        capability: "platform:ios",
        expected: "available",
        actual: "unknown",
        rule: { capability: "platform:ios", effect: "require" },
        source: { kind: "workflow" },
      });
    }
  });
});
