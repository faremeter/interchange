import { describe, expect, test } from "bun:test";

import type { SidecarProvisioner } from "./contracts";
import { createSidecarPluginRegistry } from "./plugin-registry";

function provisioner(
  id: string,
  bindingFingerprint = "test-backend:v1",
): SidecarProvisioner {
  return {
    id,
    apiVersion: 1,
    bindingFingerprint,
    async ensure() {
      return { kind: "accepted" };
    },
    async destroy() {
      return { kind: "destroyed" };
    },
  };
}

describe("createSidecarPluginRegistry", () => {
  test("resolves the explicit default and registered provisioners", () => {
    const containers = provisioner("containers");
    const virtualMachines = provisioner("virtual-machines");
    const registry = createSidecarPluginRegistry({
      provisioners: [containers, virtualMachines],
      defaultProvisionerId: "virtual-machines",
    });

    expect(registry.getDefaultProvisioner()).toBe(virtualMachines);
    expect(registry.getProvisioner("containers")).toBe(containers);
    expect(registry.getProvisioner("missing")).toBeNull();
  });

  test("does not infer a default from registration order", () => {
    const registry = createSidecarPluginRegistry({
      provisioners: [provisioner("containers")],
    });

    expect(registry.getDefaultProvisioner()).toBeNull();
  });

  test("rejects an unregistered default provisioner", () => {
    expect(() =>
      createSidecarPluginRegistry({
        provisioners: [provisioner("containers")],
        defaultProvisionerId: "missing",
      }),
    ).toThrow(/Default sidecar provisioner missing is not registered/);
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
    expect(() =>
      createSidecarPluginRegistry({
        provisioners: [],
        defaultProvisionerId: " ",
      }),
    ).toThrow(/id must be non-empty/);
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
});
