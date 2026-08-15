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
  readApprovedGrantSurface,
  resolveDefinitionIdForAsset,
  writeApprovedGrantSurface,
} from "@intx/db";
import type { ApprovedGrantSurface } from "@intx/types";
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

    test("approved grant surface round-trips through write and read", async () => {
      await seedDefinition();
      const surface: ApprovedGrantSurface = {
        grants: ["tool:search", "effect:mail.send:example.com"],
        grantEffects: { "tool:search": "ask" },
      };

      await writeApprovedGrantSurface(h.db, "wfd_1", "2", surface);

      expect(await readApprovedGrantSurface(h.db, "wfd_1", "2")).toEqual(
        surface,
      );
      // The sibling version was never stamped: null, not the other version's
      // surface.
      expect(await readApprovedGrantSurface(h.db, "wfd_1", "1")).toBeNull();
    });

    test("reading an unstamped or absent version yields null", async () => {
      await seedDefinition();
      // Version exists but carries no surface yet.
      expect(await readApprovedGrantSurface(h.db, "wfd_1", "2")).toBeNull();
      // Version row absent entirely.
      expect(await readApprovedGrantSurface(h.db, "wfd_1", "99")).toBeNull();
    });

    test("writing to a nonexistent version throws rather than no-oping", async () => {
      await seedDefinition();
      const surface: ApprovedGrantSurface = { grants: [], grantEffects: {} };
      await expect(
        writeApprovedGrantSurface(h.db, "wfd_1", "99", surface),
      ).rejects.toThrow(/expected exactly one version row/);
    });

    test("resolveDefinitionIdForAsset resolves a folded selector and nulls a miss", async () => {
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
        wireHash: "hash_a",
      });

      expect(
        await resolveDefinitionIdForAsset(h.db, {
          assetId: "ast_1",
          wireHash: "hash_a",
        }),
      ).toBe("wfd_asset");
      // A selector the fold never covered has no definition: null, not an
      // error. Same asset, a different wire hash, resolves to nothing.
      expect(
        await resolveDefinitionIdForAsset(h.db, {
          assetId: "ast_1",
          wireHash: "hash_b",
        }),
      ).toBeNull();
      expect(
        await resolveDefinitionIdForAsset(h.db, {
          assetId: "ast_missing",
          wireHash: "hash_a",
        }),
      ).toBeNull();
    });
  },
);
