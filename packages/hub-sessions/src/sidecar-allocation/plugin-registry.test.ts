import { describe, expect, test } from "bun:test";

import type { SidecarProvisioner } from "./contracts";
import {
  createSidecarPluginRegistry,
  type SidecarProvisionerChoiceContext,
} from "./plugin-registry";

function provisioner(
  id: string,
  bindingFingerprint = "test-backend:v1",
  capabilities: SidecarProvisioner["capabilities"] = [],
): SidecarProvisioner {
  return {
    id,
    apiVersion: 2,
    bindingFingerprint,
    capabilities,
    async ensure() {
      return { kind: "accepted" };
    },
    async destroy() {
      return { kind: "destroyed", cleanup: "confirmed" };
    },
  };
}

function choiceContext(
  overrides: Partial<SidecarProvisionerChoiceContext> = {},
): SidecarProvisionerChoiceContext {
  return {
    purpose: "deployment",
    tenantId: "tnt-1",
    anchorRunId: "run-1",
    signal: new AbortController().signal,
    attempts: [],
    ...overrides,
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

  // Version 1 provisioners do not report whether destroy confirmed cleanup.
  test.each([1, 3])(
    "rejects provisioner API version %d at runtime",
    (apiVersion) => {
      expect(() =>
        createSidecarPluginRegistry({
          provisioners: [
            {
              ...provisioner("containers"),
              // @ts-expect-error Exercises validation for JavaScript plugins.
              apiVersion,
            },
          ],
        }),
      ).toThrow(/Unsupported sidecar provisioner API version/);
    },
  );

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
      await registry.selectProvisioner(
        {
          tenantPolicies: [],
          workflowRules: [{ capability: "runtime:browser", effect: "require" }],
        },
        choiceContext(),
      ),
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
      await registry.selectProvisioner(
        {
          tenantPolicies: [],
          workflowRules: [{ capability: "runtime:browser", effect: "require" }],
        },
        choiceContext(),
      ),
    ).toEqual({ ok: true, provisioner: virtualMachines });
    expect(seen).toEqual([[containers, virtualMachines]]);
  });

  test("passes the choice context to the chooser", async () => {
    const containers = provisioner("containers");
    const contexts: SidecarProvisionerChoiceContext[] = [];
    const registry = createSidecarPluginRegistry({
      provisioners: [containers],
      chooser: (_candidates, context) => {
        contexts.push(context);
        return containers;
      },
    });
    const context = choiceContext({
      purpose: "probe",
      attempts: [
        {
          provisionerId: "containers",
          code: "capacity_exhausted",
          message: "Full",
          retryable: false,
        },
      ],
    });

    await registry.selectProvisioner(
      { tenantPolicies: [], workflowRules: [] },
      context,
    );

    expect(contexts).toEqual([context]);
  });

  test("reports a chooser that selects nothing", async () => {
    const registry = createSidecarPluginRegistry({
      provisioners: [provisioner("containers")],
      chooser: () => null,
    });

    expect(
      await registry.selectProvisioner(
        { tenantPolicies: [], workflowRules: [] },
        choiceContext(),
      ),
    ).toEqual({ ok: false, reason: "declined" });
  });

  describe("default chooser after a failed ensure", () => {
    const containers = provisioner("containers");
    const virtualMachines = provisioner("virtual-machines");
    const select = (
      provisioners: readonly SidecarProvisioner[],
      context: Partial<SidecarProvisionerChoiceContext>,
    ) =>
      createSidecarPluginRegistry({ provisioners }).selectProvisioner(
        { tenantPolicies: [], workflowRules: [] },
        choiceContext(context),
      );
    const failed = (provisionerId: string, retryable: boolean) => ({
      provisionerId,
      code: "capacity_exhausted",
      message: "Full",
      retryable,
    });

    test("retries the provisioner of a retryable deployment failure", async () => {
      expect(
        await select([containers, virtualMachines], {
          attempts: [failed("virtual-machines", true)],
        }),
      ).toEqual({ ok: true, provisioner: virtualMachines });
    });

    test("selects nothing after a non-retryable deployment failure", async () => {
      expect(
        await select([containers, virtualMachines], {
          attempts: [failed("containers", true), failed("containers", false)],
        }),
      ).toEqual({ ok: false, reason: "declined" });
    });

    test("selects nothing when the failed provisioner no longer matches", async () => {
      expect(
        await select([containers], {
          attempts: [failed("virtual-machines", true)],
        }),
      ).toEqual({ ok: false, reason: "declined" });
    });

    test("selects nothing after a probe failure", async () => {
      expect(
        await select([containers, virtualMachines], {
          purpose: "probe",
          attempts: [failed("containers", true)],
        }),
      ).toEqual({ ok: false, reason: "declined" });
    });
  });

  test("rejects a chooser result outside the matching candidates", async () => {
    const containers = provisioner("containers");
    const registry = createSidecarPluginRegistry({
      provisioners: [containers],
      chooser: () => provisioner("unregistered"),
    });

    await expect(
      registry.selectProvisioner(
        { tenantPolicies: [], workflowRules: [] },
        choiceContext(),
      ),
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
      await registry.selectProvisioner(
        {
          tenantPolicies: [],
          workflowRules: [{ capability: "platform:ios", effect: "require" }],
        },
        choiceContext(),
      ),
    ).toEqual({ ok: true, provisioner: ios });
  });

  test("reports each provisioner's capability mismatches", async () => {
    const registry = createSidecarPluginRegistry({
      provisioners: [provisioner("containers")],
    });

    const selection = await registry.selectProvisioner(
      {
        tenantPolicies: [],
        workflowRules: [{ capability: "platform:ios", effect: "require" }],
      },
      choiceContext(),
    );
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
