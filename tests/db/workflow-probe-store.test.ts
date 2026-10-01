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
import { sidecar } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedTenants } from "@intx/test-harness/seed";

const TENANT_ID = "tnt-probe-store";
const ASSET_ID = "asset-probe-store";

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
    async function bindProbe(probeId: string) {
      const store = createWorkflowProbeStore(h.db);
      await store.create({
        id: probeId,
        tenantId: TENANT_ID,
        definitionAssetId: ASSET_ID,
        source: { kind: "registry", registry: "npmjs" },
        entry: "./workflow.js",
        provisionerId: "sandbox",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "sandbox:test",
      });
      await store.bindSidecar({
        probeId,
        sidecarId: `${probeId}-minted`,
        tokenHashSha256: new TextEncoder().encode(probeId),
      });
      return store;
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

    test("rejects a sidecar whose probe already finished", async () => {
      const store = await bindProbe("probe-first");
      await store.markProbing({ probeId: "probe-first" });
      await store.transition("probe-first", ["probing"], "failed");
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
  },
);
