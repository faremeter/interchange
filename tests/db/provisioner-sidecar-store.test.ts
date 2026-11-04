import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq } from "drizzle-orm";

import {
  createSidecarAllocationStore,
  type SidecarInventory,
  type SidecarProvisionerBinding,
} from "@intx/db";
import { sidecar, sidecarAllocation, workflowProbe } from "@intx/db/schema";
import type {
  HostedSidecarAllocationStatus,
  HostedWorkflowProbeStatus,
  SidecarAllocationStatus,
} from "@intx/types";
import { DEFAULT_SIDECAR_CAPACITY } from "@intx/types/sidecar";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const TENANT_ID = "tenant-sidecar-store";
const ASSET_ID = "asset-sidecar-store";
const MANIFEST_DIGEST = "1".repeat(64);
const BINDING: SidecarProvisionerBinding = {
  provisionerId: "test",
  provisionerApiVersion: 2,
  provisionerBindingFingerprint: "test:v1",
};
const OTHER_BINDING: SidecarProvisionerBinding = {
  ...BINDING,
  provisionerBindingFingerprint: "test:v2",
};

const HOSTED_ALLOCATION_STATUSES: readonly HostedSidecarAllocationStatus[] = [
  "provisioning",
  "allocated",
  "replacing",
  "releasing",
  "destroy_failed",
];
describe.skipIf(!harnessDbEnvAvailable())("provisioner sidecar store", () => {
  let h: TestDb;

  beforeAll(async () => {
    h = await createTestDb();
  });

  afterAll(async () => {
    await h.close();
  });

  beforeEach(async () => {
    await h.reset();
    await seedTenants(h.db, [{ id: TENANT_ID }]);
    await seedAsset(h.db, {
      id: ASSET_ID,
      tenantId: TENANT_ID,
      kind: "workflow",
      name: "sidecar-store",
    });
  });

  function storeWith(
    getSidecarInventory: (
      sidecarId: string,
    ) => SidecarInventory | undefined = () => undefined,
  ) {
    return createSidecarAllocationStore(h.db, { getSidecarInventory });
  }

  async function seedSidecar(
    id: string,
    args: {
      readonly binding?: SidecarProvisionerBinding;
      readonly retired?: boolean;
    } = {},
  ) {
    await h.db.insert(sidecar).values({
      id,
      url: null,
      tokenHashSha256: new TextEncoder().encode(id),
      ...(args.binding ?? BINDING),
      adapterManifestDigest: MANIFEST_DIGEST,
      ...(args.retired === true ? { retiredAt: new Date() } : {}),
    });
  }

  async function seedAllocation(
    id: string,
    sidecarId: string,
    status: SidecarAllocationStatus,
    args: {
      readonly externalRef?: string;
      readonly cleanupConfirmed?: boolean;
      readonly address?: string | null;
    } = {},
  ) {
    const address =
      args.address === undefined ? `${id}@example.test` : args.address;
    await seedWorkflowRun(h.db, {
      id: `run-${id}`,
      anchorRunId: `run-${id}`,
      tenantId: TENANT_ID,
      ...(address !== null ? { address } : {}),
    });
    await h.db.insert(sidecarAllocation).values({
      id,
      anchorRunId: `run-${id}`,
      tenantId: TENANT_ID,
      sidecarId,
      status,
      generation: 1,
      ensureAcceptedGeneration: 1,
      deploymentCleanupConfirmed: args.cleanupConfirmed ?? false,
      maxDisconnectedMs: 900_000,
      ...BINDING,
      adapterManifestDigest: MANIFEST_DIGEST,
      ...(args.externalRef !== undefined
        ? { externalRef: args.externalRef }
        : {}),
    });
  }

  async function seedProbe(
    id: string,
    sidecarId: string,
    status: HostedWorkflowProbeStatus | "succeeded" | "failed",
  ) {
    await h.db.insert(workflowProbe).values({
      id,
      tenantId: TENANT_ID,
      definitionAssetId: ASSET_ID,
      sidecarId,
      source: { kind: "registry", registry: "npmjs" },
      entry: "./workflow.js",
      status,
      generation: 1,
      result: {
        projection: {
          id: "workflow",
          triggers: [],
          stepOrder: [],
          steps: {},
        },
        grants: [],
        grantWalkSnapshot: { perStep: [], grantRequirements: [] },
        wireHash: "a".repeat(64),
      },
      ...BINDING,
      adapterManifestDigest: MANIFEST_DIGEST,
    });
  }

  async function retiredAt(sidecarId: string) {
    const [row] = await h.db
      .select({ retiredAt: sidecar.retiredAt })
      .from(sidecar)
      .where(eq(sidecar.id, sidecarId));
    return row?.retiredAt;
  }

  describe("listProvisionerSidecars", () => {
    test("lists the binding's idle and busy sidecars with what is on them", async () => {
      await seedSidecar("sc-idle");
      await seedSidecar("sc-busy");
      await seedSidecar("sc-other", { binding: OTHER_BINDING });
      await seedAllocation("alloc-busy", "sc-busy", "allocated", {
        externalRef: "vm-1",
      });
      await seedAllocation("alloc-other", "sc-other", "allocated");
      await seedProbe("probe-busy", "sc-busy", "probing");

      const listed = await storeWith().listProvisionerSidecars(BINDING);

      expect(
        listed.toSorted((a, b) => a.sidecarId.localeCompare(b.sidecarId)),
      ).toEqual([
        {
          sidecarId: "sc-busy",
          retired: false,
          adapterManifestDigest: MANIFEST_DIGEST,
          deployments: [
            {
              allocationId: "alloc-busy",
              generation: 1,
              tenantId: TENANT_ID,
              anchorRunId: "run-alloc-busy",
              address: "alloc-busy@example.test",
              definitionId: expect.any(String),
              status: "allocated",
              runStatus: "running",
              cleanupConfirmed: false,
              externalRef: "vm-1",
            },
          ],
          probes: [
            {
              allocationId: "probe-busy",
              generation: 1,
              tenantId: TENANT_ID,
              status: "probing",
            },
          ],
        },
        {
          sidecarId: "sc-idle",
          retired: false,
          adapterManifestDigest: MANIFEST_DIGEST,
          deployments: [],
          probes: [],
        },
      ]);
    });

    test("lists a retired sidecar only while work is left on it", async () => {
      await seedSidecar("sc-retired-idle", { retired: true });
      await seedSidecar("sc-retired-deployment", { retired: true });
      await seedSidecar("sc-retired-probe", { retired: true });
      await seedAllocation("alloc", "sc-retired-deployment", "allocated");
      await seedProbe("probe", "sc-retired-probe", "probing");

      const listed = await storeWith().listProvisionerSidecars(BINDING);

      expect(listed.map((view) => view.sidecarId).toSorted()).toEqual([
        "sc-retired-deployment",
        "sc-retired-probe",
      ]);
      expect(listed.every((view) => view.retired)).toBe(true);
    });

    test("leaves out finished work", async () => {
      await seedSidecar("sc-finished");
      await seedAllocation("alloc-released", "sc-finished", "released");
      await seedAllocation("alloc-failed", "sc-finished", "failed");
      await seedProbe("probe-succeeded", "sc-finished", "succeeded");
      await seedProbe("probe-failed", "sc-finished", "failed");

      expect(await storeWith().listProvisionerSidecars(BINDING)).toEqual([
        {
          sidecarId: "sc-finished",
          retired: false,
          adapterManifestDigest: MANIFEST_DIGEST,
          deployments: [],
          probes: [],
        },
      ]);
    });

    test("keeps a deployment whose anchor has no address", async () => {
      await seedSidecar("sc-busy");
      await seedAllocation("alloc", "sc-busy", "allocated", {
        address: null,
      });

      const [view] = await storeWith().listProvisionerSidecars(BINDING);

      expect(view?.deployments).toMatchObject([
        { allocationId: "alloc", address: null },
      ]);
    });

    test("reports capacity only for a sidecar this Hub has inventory for", async () => {
      await seedSidecar("sc-connected");
      await seedSidecar("sc-elsewhere");
      await seedAllocation("placing", "sc-connected", "provisioning");
      await seedAllocation("running", "sc-connected", "allocated");
      await seedAllocation("removing", "sc-connected", "releasing");
      await seedAllocation("removed", "sc-connected", "releasing", {
        cleanupConfirmed: true,
      });
      await seedAllocation("retained", "sc-connected", "allocated");

      const listed = await storeWith((sidecarId) =>
        sidecarId === "sc-connected"
          ? {
              capacity: DEFAULT_SIDECAR_CAPACITY,
              retainedIncarnations: [
                { allocationId: "retained", generation: 1 },
              ],
            }
          : undefined,
      ).listProvisionerSidecars(BINDING);

      expect(
        listed.find((view) => view.sidecarId === "sc-connected")?.capacity,
      ).toEqual({
        maxActiveDeployments: DEFAULT_SIDECAR_CAPACITY.maxActiveDeployments,
        activeDeployments: 3,
      });
      expect(
        listed.find((view) => view.sidecarId === "sc-elsewhere"),
      ).not.toHaveProperty("capacity");
    });
  });

  describe("retireIdleProvisionerSidecar", () => {
    const retire = (sidecarId: string, binding = BINDING) =>
      createSidecarAllocationStore(h.db).retireIdleProvisionerSidecar({
        sidecarId,
        binding,
      });

    test("retires an idle sidecar, and answers the same when repeated", async () => {
      await seedSidecar("sc-idle");

      expect(await retire("sc-idle")).toEqual({ kind: "retired" });
      const first = await retiredAt("sc-idle");
      expect(first).toBeInstanceOf(Date);
      expect(await retire("sc-idle")).toEqual({ kind: "retired" });
      expect(await retiredAt("sc-idle")).toEqual(first);
    });

    for (const status of HOSTED_ALLOCATION_STATUSES) {
      test(`leaves a sidecar with a ${status} deployment running`, async () => {
        await seedSidecar("sc-busy");
        await seedAllocation("alloc", "sc-busy", status);

        expect(await retire("sc-busy")).toEqual({ kind: "in_use" });
        expect(await retiredAt("sc-busy")).toBeNull();
      });
    }

    for (const status of ["provisioning", "allocated"] as const) {
      test(`leaves a sidecar with a ${status} deployment running even if its cleanup is confirmed`, async () => {
        await seedSidecar("sc-busy");
        await seedAllocation("alloc", "sc-busy", status, {
          cleanupConfirmed: true,
        });

        expect(await retire("sc-busy")).toEqual({ kind: "in_use" });
      });
    }

    for (const status of [
      "releasing",
      "replacing",
      "destroy_failed",
    ] as const) {
      test(`retires a sidecar whose only ${status} deployment is confirmed removed`, async () => {
        await seedSidecar("sc-last");
        await seedAllocation("alloc", "sc-last", status, {
          cleanupConfirmed: true,
        });

        expect(await retire("sc-last")).toEqual({ kind: "retired" });
        expect(await retiredAt("sc-last")).toBeInstanceOf(Date);
      });
    }

    test("retires for each of several final destroys at once", async () => {
      await seedSidecar("sc-last");
      await seedAllocation("alloc-a", "sc-last", "releasing", {
        cleanupConfirmed: true,
      });
      await seedAllocation("alloc-b", "sc-last", "releasing", {
        cleanupConfirmed: true,
      });

      expect(await Promise.all([retire("sc-last"), retire("sc-last")])).toEqual(
        [{ kind: "retired" }, { kind: "retired" }],
      );
    });

    test("a confirmed deployment does not hide other work on the sidecar", async () => {
      await seedSidecar("sc-busy");
      await seedAllocation("alloc-last", "sc-busy", "releasing", {
        cleanupConfirmed: true,
      });
      await seedAllocation("alloc-owed", "sc-busy", "releasing");

      expect(await retire("sc-busy")).toEqual({ kind: "in_use" });
    });

    for (const status of ["provisioning", "probing"] as const) {
      test(`leaves a sidecar with a ${status} probe running`, async () => {
        await seedSidecar("sc-busy");
        await seedProbe("probe", "sc-busy", status);

        expect(await retire("sc-busy")).toEqual({ kind: "in_use" });
        expect(await retiredAt("sc-busy")).toBeNull();
      });
    }

    test("does not count a probe being released", async () => {
      await seedSidecar("sc-last");
      await seedProbe("probe", "sc-last", "releasing");

      expect(await retire("sc-last")).toEqual({ kind: "retired" });
    });

    test("leaves a sidecar whose deployment has no address running", async () => {
      await seedSidecar("sc-busy");
      await seedAllocation("alloc", "sc-busy", "allocated", { address: null });

      expect(await retire("sc-busy")).toEqual({ kind: "in_use" });
    });

    test("does not count finished work", async () => {
      await seedSidecar("sc-finished");
      await seedAllocation("alloc-released", "sc-finished", "released");
      await seedAllocation("alloc-failed", "sc-finished", "failed");
      await seedProbe("probe-succeeded", "sc-finished", "succeeded");
      await seedProbe("probe-failed", "sc-finished", "failed");

      expect(await retire("sc-finished")).toEqual({ kind: "retired" });
    });

    test("answers in_use for a retired sidecar with work left", async () => {
      await seedSidecar("sc-retired", { retired: true });
      await seedAllocation("alloc", "sc-retired", "allocated");

      expect(await retire("sc-retired")).toEqual({ kind: "in_use" });
    });

    test("retires nothing under another binding or an unknown id", async () => {
      await seedSidecar("sc-other", { binding: OTHER_BINDING });

      expect(await retire("sc-other")).toEqual({ kind: "not_owned" });
      expect(await retiredAt("sc-other")).toBeNull();
      expect(await retire("sc-unknown")).toEqual({ kind: "not_owned" });
    });
  });
});
