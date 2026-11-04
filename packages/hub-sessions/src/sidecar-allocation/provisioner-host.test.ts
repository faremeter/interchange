import { describe, expect, test } from "bun:test";

import type { SidecarProvisioner, SidecarProvisionerHost } from "./contracts";
import { attachSidecarProvisioners } from "./provisioner-host";

function provisioner(
  id: string,
  hosts?: SidecarProvisionerHost[],
): SidecarProvisioner {
  return {
    id,
    apiVersion: 2,
    bindingFingerprint: `${id}:v1`,
    capabilities: [],
    ensure: async () => ({ kind: "accepted" }),
    destroy: async () => ({ kind: "destroyed", cleanup: "confirmed" }),
    ...(hosts !== undefined
      ? {
          attach(host: SidecarProvisionerHost) {
            hosts.push(host);
          },
        }
      : {}),
  };
}

const isConnected = () => false;

const unusedHostCalls = {
  retireIdleProvisionerSidecar: async () => ({ kind: "not_owned" as const }),
  listProvisionerSidecars: async () => [],
};

describe("attachSidecarProvisioners", () => {
  test("attaches a provisioner in both registries once and retires under its binding", async () => {
    const hosts: SidecarProvisionerHost[] = [];
    const shared = provisioner("sandbox", hosts);
    const retired: unknown[] = [];
    attachSidecarProvisioners({
      provisioners: [shared, provisioner("legacy"), shared],
      retireProvisionerSidecar: async (args) => {
        retired.push(args);
        return { connectedAllocationIds: [] };
      },
      ...unusedHostCalls,
      isConnected,
    });

    expect(hosts).toHaveLength(1);
    expect(await hosts[0]?.sidecarStopped("sc-idle")).toEqual({
      kind: "retired",
    });
    expect(
      await hosts[0]?.sidecarStopped("sc-crashed", {
        endReconnectWindow: true,
      }),
    ).toEqual({ kind: "retired" });
    const binding = {
      provisionerId: "sandbox",
      provisionerApiVersion: 2,
      provisionerBindingFingerprint: "sandbox:v1",
    };
    expect(retired).toEqual([
      { sidecarId: "sc-idle", binding, endReconnectWindow: false, isConnected },
      {
        sidecarId: "sc-crashed",
        binding,
        endReconnectWindow: true,
        isConnected,
      },
    ]);
  });

  test("answers not_owned for a sidecar the provisioner does not own", async () => {
    const hosts: SidecarProvisionerHost[] = [];
    attachSidecarProvisioners({
      provisioners: [provisioner("sandbox", hosts)],
      retireProvisionerSidecar: async () => null,
      ...unusedHostCalls,
      isConnected,
    });

    expect(await hosts[0]?.sidecarStopped("sc-other")).toEqual({
      kind: "not_owned",
    });
  });

  test("passes a transient store failure through as a throw", async () => {
    const hosts: SidecarProvisionerHost[] = [];
    attachSidecarProvisioners({
      provisioners: [provisioner("sandbox", hosts)],
      retireProvisionerSidecar: async () => {
        throw new Error("database unavailable");
      },
      ...unusedHostCalls,
      isConnected,
    });

    await expect(hosts[0]?.sidecarStopped("sc-idle")).rejects.toThrow(
      "database unavailable",
    );
  });

  test("rejects a report with invalid arguments without retiring anything", async () => {
    const hosts: SidecarProvisionerHost[] = [];
    let retired = 0;
    attachSidecarProvisioners({
      provisioners: [provisioner("sandbox", hosts)],
      retireProvisionerSidecar: async () => {
        retired += 1;
        return { connectedAllocationIds: [] };
      },
      ...unusedHostCalls,
      isConnected,
    });
    const host = hosts[0];
    if (host === undefined) throw new Error("Provisioner was not attached");

    await expect(
      // @ts-expect-error Exercises validation for JavaScript provisioners.
      host.sidecarStopped("sc-idle", { endReconnectWindow: "false" }),
    ).rejects.toThrow("invalid sidecarStopped report");
    await expect(host.sidecarStopped("")).rejects.toThrow(
      "invalid sidecarStopped report",
    );
    expect(retired).toBe(0);
  });

  test("retires an idle sidecar under the provisioner's own binding", async () => {
    const hosts: SidecarProvisionerHost[] = [];
    const retired: unknown[] = [];
    attachSidecarProvisioners({
      provisioners: [provisioner("sandbox", hosts)],
      retireProvisionerSidecar: async () => null,
      ...unusedHostCalls,
      retireIdleProvisionerSidecar: async (args) => {
        retired.push(args);
        return { kind: "in_use" };
      },
      isConnected,
    });

    expect(await hosts[0]?.retireIfIdle("sc-busy")).toEqual({ kind: "in_use" });
    expect(retired).toEqual([
      {
        sidecarId: "sc-busy",
        binding: {
          provisionerId: "sandbox",
          provisionerApiVersion: 2,
          provisionerBindingFingerprint: "sandbox:v1",
        },
      },
    ]);
  });

  test("rejects retireIfIdle with an invalid id without retiring anything", async () => {
    const hosts: SidecarProvisionerHost[] = [];
    let retired = 0;
    attachSidecarProvisioners({
      provisioners: [provisioner("sandbox", hosts)],
      retireProvisionerSidecar: async () => null,
      ...unusedHostCalls,
      retireIdleProvisionerSidecar: async () => {
        retired += 1;
        return { kind: "retired" };
      },
      isConnected,
    });

    await expect(hosts[0]?.retireIfIdle("")).rejects.toThrow(
      "invalid retireIfIdle call",
    );
    expect(retired).toBe(0);
  });

  test("lists the sidecars of the provisioner's own binding", async () => {
    const hosts: SidecarProvisionerHost[] = [];
    const listed: unknown[] = [];
    const sidecars = [
      {
        sidecarId: "sc-idle",
        retired: false,
        adapterManifestDigest: "digest",
        deployments: [],
        probes: [],
      },
    ];
    attachSidecarProvisioners({
      provisioners: [provisioner("sandbox", hosts)],
      retireProvisionerSidecar: async () => null,
      ...unusedHostCalls,
      listProvisionerSidecars: async (binding) => {
        listed.push(binding);
        return sidecars;
      },
      isConnected,
    });

    expect(await hosts[0]?.listSidecars()).toEqual(sidecars);
    expect(listed).toEqual([
      {
        provisionerId: "sandbox",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "sandbox:v1",
      },
    ]);
  });
});
