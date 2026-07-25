import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";
import {
  asset,
  workflowDefinition,
  workflowDefinitionVersion,
} from "@intx/db/schema";
import {
  createWorkflowDefinitionStore,
  resolveDefinitionIdForAsset,
} from "@intx/db";
import { and, eq } from "drizzle-orm";

describe.skipIf(!harnessDbEnvAvailable())(
  "createWorkflowDefinitionStore.rollback (real DB)",
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
    });

    async function seedDefinition(): Promise<void> {
      await seedTenants(h.db, [{ id: "tnt_root" }]);
      await h.db.insert(workflowDefinition).values({
        id: "wfd_1",
        tenantId: "tnt_root",
        name: "def",
        currentVersion: "2",
      });
      await h.db.insert(workflowDefinitionVersion).values([
        {
          id: "wdv_1",
          definitionId: "wfd_1",
          version: "1",
          status: "inactive",
        },
        {
          id: "wdv_2",
          definitionId: "wfd_1",
          version: "2",
          status: "active",
        },
      ]);
    }

    async function versionStatus(version: string): Promise<string | undefined> {
      const row = await h.db.query.workflowDefinitionVersion.findFirst({
        where: and(
          eq(workflowDefinitionVersion.definitionId, "wfd_1"),
          eq(workflowDefinitionVersion.version, version),
        ),
      });
      return row?.status;
    }

    test("rolls back: activates the target, deactivates current, repoints currentVersion", async () => {
      await seedDefinition();
      const store = createWorkflowDefinitionStore(h.db);

      const result = await store.rollback("tnt_root", "wfd_1", "1");

      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.definition.currentVersion).toBe("1");
      }
      expect(await versionStatus("1")).toBe("active");
      expect(await versionStatus("2")).toBe("inactive");

      const def = await h.db.query.workflowDefinition.findFirst({
        where: eq(workflowDefinition.id, "wfd_1"),
      });
      expect(def?.currentVersion).toBe("1");
    });

    test("returns definition_not_found for an unknown definition", async () => {
      await seedDefinition();
      const store = createWorkflowDefinitionStore(h.db);
      const result = await store.rollback("tnt_root", "wfd_missing", "1");
      expect(result).toEqual({ ok: false, reason: "definition_not_found" });
    });

    test("returns definition_not_found across a tenant boundary", async () => {
      await seedDefinition();
      await seedTenants(h.db, [{ id: "tnt_other" }]);
      const store = createWorkflowDefinitionStore(h.db);
      const result = await store.rollback("tnt_other", "wfd_1", "1");
      expect(result).toEqual({ ok: false, reason: "definition_not_found" });
      // The cross-tenant miss changed nothing.
      expect(await versionStatus("2")).toBe("active");
    });

    test("returns version_not_found for an unknown version", async () => {
      await seedDefinition();
      const store = createWorkflowDefinitionStore(h.db);
      const result = await store.rollback("tnt_root", "wfd_1", "99");
      expect(result).toEqual({ ok: false, reason: "version_not_found" });
      // No partial write: the active version is unchanged.
      expect(await versionStatus("2")).toBe("active");
    });

    test("resolveDefinitionIdForAsset resolves a folded asset and nulls a miss", async () => {
      await seedTenants(h.db, [{ id: "tnt_root" }]);
      await seedPrincipal(h.db, { id: "prn_creator", tenantId: "tnt_root" });
      await h.db.insert(asset).values({
        id: "ast_1",
        tenantId: "tnt_root",
        kind: "workflow",
        name: "wf",
        creatorPrincipalId: "prn_creator",
      });
      await h.db.insert(workflowDefinition).values({
        id: "wfd_asset",
        tenantId: "tnt_root",
        name: "asset-backed",
        assetId: "ast_1",
      });

      expect(await resolveDefinitionIdForAsset(h.db, "ast_1")).toBe(
        "wfd_asset",
      );
      // An asset the fold never covered has no definition: null, not an error.
      expect(await resolveDefinitionIdForAsset(h.db, "ast_missing")).toBeNull();
    });
  },
);
