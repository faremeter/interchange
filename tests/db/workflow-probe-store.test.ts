import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq } from "drizzle-orm";

import { createWorkflowProbeStore, SidecarReuseRejectedError } from "@intx/db";
import { sidecar, sidecarAllocation } from "@intx/db/schema";
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

const TENANT_ID = "tnt-probe-store";
const ASSET_ID = "asset-probe-store";
const MANIFEST_DIGEST = "1".repeat(64);
const OTHER_MANIFEST_DIGEST = "2".repeat(64);

describe.skipIf(!harnessDbEnvAvailable())(
  "workflowProbeStore (real DB)",
  () => {
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
        name: "probe-store-workflow",
      });
    });

    // Creates a probe and binds the identity minted for it, as the
    // allocation service does before calling ensure.
    async function bindProbe(
      probeId: string,
      adapterManifestDigest = MANIFEST_DIGEST,
    ) {
      const store = createWorkflowProbeStore(h.db);
      await store.create({
        id: probeId,
        tenantId: TENANT_ID,
        definitionAssetId: ASSET_ID,
        source: { kind: "registry", registry: "npmjs" },
        entry: "./workflow.js",
        provisionerId: "sandbox",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "sandbox:test",
        adapterManifestDigest,
      });
      await store.bindSidecar({
        probeId,
        sidecarId: `${probeId}-minted`,
        tokenHashSha256: new TextEncoder().encode(probeId),
      });
      return store;
    }

    // Seeds an allocated deployment of the probes' provisioner binding as the
    // only work on a sidecar.
    async function seedAllocationHost(
      sidecarId: string,
      adapterManifestDigest: string,
    ) {
      await h.db.insert(sidecar).values({
        id: sidecarId,
        url: null,
        tokenHashSha256: new TextEncoder().encode(sidecarId),
        provisionerId: "sandbox",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "sandbox:test",
        adapterManifestDigest,
      });
      await seedWorkflowRun(h.db, {
        id: `run-${sidecarId}`,
        anchorRunId: `run-${sidecarId}`,
        tenantId: TENANT_ID,
      });
      await h.db.insert(sidecarAllocation).values({
        id: `alloc-${sidecarId}`,
        anchorRunId: `run-${sidecarId}`,
        tenantId: TENANT_ID,
        sidecarId,
        status: "allocated",
        generation: 1,
        maxDisconnectedMs: 900_000,
        provisionerId: "sandbox",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "sandbox:test",
        adapterManifestDigest,
      });
    }

    test("places a probe on a sidecar that hosts another probe", async () => {
      const store = await bindProbe("probe-first");
      await store.markProbing({ probeId: "probe-first" });
      await bindProbe("probe-second");

      const probing = await store.markProbing({
        probeId: "probe-second",
        sidecarId: "probe-first-minted",
      });

      expect(probing).toMatchObject({
        status: "probing",
        sidecarId: "probe-first-minted",
      });
      expect(
        await h.db.query.sidecar.findFirst({
          where: eq(sidecar.id, "probe-second-minted"),
        }),
      ).toBeUndefined();
    });

    test("places a probe on a sidecar whose probe is still releasing", async () => {
      const store = await bindProbe("probe-first");
      await store.markProbing({ probeId: "probe-first" });
      await store.transition("probe-first", ["probing"], "releasing");
      await bindProbe("probe-second");

      const probing = await store.markProbing({
        probeId: "probe-second",
        sidecarId: "probe-first-minted",
      });

      expect(probing).toMatchObject({
        status: "probing",
        sidecarId: "probe-first-minted",
      });
    });

    test("places a probe on a sidecar whose probe already finished", async () => {
      const store = await bindProbe("probe-first");
      await store.markProbing({ probeId: "probe-first" });
      await store.transition("probe-first", ["probing"], "failed");
      await bindProbe("probe-second");

      expect(
        await store.markProbing({
          probeId: "probe-second",
          sidecarId: "probe-first-minted",
        }),
      ).toMatchObject({
        status: "probing",
        sidecarId: "probe-first-minted",
      });
    });

    test("rejects a sidecar its finished probe's destroy confirmed gone", async () => {
      const store = await bindProbe("probe-first");
      await store.markProbing({ probeId: "probe-first" });
      await store.transition("probe-first", ["probing"], "failed");
      await store.retireSidecar({ sidecarId: "probe-first-minted" });
      await bindProbe("probe-second");

      await expect(
        store.markProbing({
          probeId: "probe-second",
          sidecarId: "probe-first-minted",
        }),
      ).rejects.toThrow("it was retired");
    });

    test("rejects a sidecar whose probe booted with another adapter manifest", async () => {
      const store = await bindProbe("probe-first", OTHER_MANIFEST_DIGEST);
      await store.markProbing({ probeId: "probe-first" });
      await bindProbe("probe-second");

      await expect(
        store.markProbing({
          probeId: "probe-second",
          sidecarId: "probe-first-minted",
        }),
      ).rejects.toBeInstanceOf(SidecarReuseRejectedError);
      expect(await store.get("probe-second")).toMatchObject({
        status: "provisioning",
        sidecarId: "probe-second-minted",
      });
    });

    test("rejects a sidecar recorded before its adapter manifest was", async () => {
      const store = await bindProbe("probe-first");
      await store.markProbing({ probeId: "probe-first" });
      await h.db
        .update(sidecar)
        .set({ adapterManifestDigest: "unrecorded" })
        .where(eq(sidecar.id, "probe-first-minted"));
      await bindProbe("probe-second");

      await expect(
        store.markProbing({
          probeId: "probe-second",
          sidecarId: "probe-first-minted",
        }),
      ).rejects.toBeInstanceOf(SidecarReuseRejectedError);
    });

    test("places a probe on a sidecar whose allocation booted with the same adapter manifest", async () => {
      await seedAllocationHost("sidecar-deployment", MANIFEST_DIGEST);
      const store = await bindProbe("probe-second");

      expect(
        await store.markProbing({
          probeId: "probe-second",
          sidecarId: "sidecar-deployment",
        }),
      ).toMatchObject({
        status: "probing",
        sidecarId: "sidecar-deployment",
      });
    });

    test("rejects a sidecar whose allocation booted with another adapter manifest", async () => {
      await seedAllocationHost("sidecar-deployment", OTHER_MANIFEST_DIGEST);
      const store = await bindProbe("probe-second");

      await expect(
        store.markProbing({
          probeId: "probe-second",
          sidecarId: "sidecar-deployment",
        }),
      ).rejects.toBeInstanceOf(SidecarReuseRejectedError);
      expect(await store.get("probe-second")).toMatchObject({
        status: "provisioning",
        sidecarId: "probe-second-minted",
      });
    });
  },
);
