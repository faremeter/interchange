import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { eq } from "drizzle-orm";

import { ensureWorkflowDefinitionForAsset } from "@intx/hub-sessions";
import { resolveDefinitionIdForAsset } from "@intx/db";
import { workflowDefinition, workflowDefinitionVersion } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal, seedTenants } from "@intx/test-harness/seed";

const TENANT = "tnt";
const CREATOR = "prn_creator";
const ASSET = "ast_wf";
const WIRE_A = "wirehash_a";
const WIRE_B = "wirehash_b";

describe.skipIf(!harnessDbEnvAvailable())(
  "ensureWorkflowDefinitionForAsset (real DB)",
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
      await seedTenants(h.db, [{ id: TENANT }]);
      await seedPrincipal(h.db, {
        id: CREATOR,
        tenantId: TENANT,
        kind: "user",
        refId: "creator",
      });
      await seedAsset(h.db, {
        id: ASSET,
        tenantId: TENANT,
        kind: "workflow",
        name: "wf-name",
        displayName: "WF Display",
        creatorPrincipalId: CREATOR,
      });
    });

    test("projects a definition and version 1 over the selector", async () => {
      const { definitionId, created } = await ensureWorkflowDefinitionForAsset(
        h.db,
        { assetId: ASSET, wireHash: WIRE_A },
      );
      expect(created).toBe(true);

      const defs = await h.db
        .select()
        .from(workflowDefinition)
        .where(eq(workflowDefinition.assetId, ASSET));
      expect(defs).toHaveLength(1);
      const def = defs[0];
      expect(def?.id).toBe(definitionId);
      expect(def?.tenantId).toBe(TENANT);
      expect(def?.creatorPrincipalId).toBe(CREATOR);
      expect(def?.wireHash).toBe(WIRE_A);
      expect(def?.name).toBe("wf-name");
      expect(def?.description).toBe("WF Display");

      const versions = await h.db
        .select()
        .from(workflowDefinitionVersion)
        .where(eq(workflowDefinitionVersion.definitionId, definitionId));
      expect(versions).toHaveLength(1);
      expect(versions[0]?.version).toBe("1");
    });

    test("is idempotent: a second call with the same selector returns the same id and adds no rows", async () => {
      const first = await ensureWorkflowDefinitionForAsset(h.db, {
        assetId: ASSET,
        wireHash: WIRE_A,
      });
      expect(first.created).toBe(true);

      const second = await ensureWorkflowDefinitionForAsset(h.db, {
        assetId: ASSET,
        wireHash: WIRE_A,
      });
      expect(second.created).toBe(false);
      expect(second.definitionId).toBe(first.definitionId);

      const defs = await h.db
        .select()
        .from(workflowDefinition)
        .where(eq(workflowDefinition.assetId, ASSET));
      expect(defs).toHaveLength(1);
      const versions = await h.db
        .select()
        .from(workflowDefinitionVersion)
        .where(eq(workflowDefinitionVersion.definitionId, first.definitionId));
      expect(versions).toHaveLength(1);
    });

    test("two definitions share one asset under distinct wire hashes and resolve independently", async () => {
      const a = await ensureWorkflowDefinitionForAsset(h.db, {
        assetId: ASSET,
        wireHash: WIRE_A,
      });
      const b = await ensureWorkflowDefinitionForAsset(h.db, {
        assetId: ASSET,
        wireHash: WIRE_B,
      });
      expect(a.created).toBe(true);
      expect(b.created).toBe(true);
      expect(a.definitionId).not.toBe(b.definitionId);

      // Both persist against the one asset.
      const defs = await h.db
        .select()
        .from(workflowDefinition)
        .where(eq(workflowDefinition.assetId, ASSET));
      expect(defs).toHaveLength(2);

      // Each selector resolves to its own definition id, not the other's.
      expect(
        await resolveDefinitionIdForAsset(h.db, {
          assetId: ASSET,
          wireHash: WIRE_A,
        }),
      ).toBe(a.definitionId);
      expect(
        await resolveDefinitionIdForAsset(h.db, {
          assetId: ASSET,
          wireHash: WIRE_B,
        }),
      ).toBe(b.definitionId);

      // Each definition carries its own version "1".
      for (const definitionId of [a.definitionId, b.definitionId]) {
        const versions = await h.db
          .select()
          .from(workflowDefinitionVersion)
          .where(eq(workflowDefinitionVersion.definitionId, definitionId));
        expect(versions).toHaveLength(1);
        expect(versions[0]?.version).toBe("1");
      }
    });

    test("throws when the asset does not exist", async () => {
      await expect(
        ensureWorkflowDefinitionForAsset(h.db, {
          assetId: "ast_missing",
          wireHash: WIRE_A,
        }),
      ).rejects.toThrow(/no asset found/);
    });

    test("concurrent calls for one selector yield a single definition and version", async () => {
      // Concurrency safety is the whole reason for the conflict-and-read-back
      // path: several deploys of the same selector can race. Fire many at once
      // and assert exactly one definition, one version, and one winner.
      const results = await Promise.all(
        Array.from({ length: 8 }, () =>
          h.db.transaction((tx) =>
            ensureWorkflowDefinitionForAsset(tx, {
              assetId: ASSET,
              wireHash: WIRE_A,
            }),
          ),
        ),
      );

      const ids = new Set(results.map((r) => r.definitionId));
      expect(ids.size).toBe(1);
      expect(results.filter((r) => r.created)).toHaveLength(1);

      const defs = await h.db
        .select()
        .from(workflowDefinition)
        .where(eq(workflowDefinition.assetId, ASSET));
      expect(defs).toHaveLength(1);
      const versions = await h.db
        .select()
        .from(workflowDefinitionVersion)
        .where(eq(workflowDefinitionVersion.definitionId, [...ids][0] ?? ""));
      expect(versions).toHaveLength(1);
    });
  },
);
