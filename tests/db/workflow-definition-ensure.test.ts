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

    test("projects a definition and version 1 over the asset", async () => {
      const { definitionId, created } = await ensureWorkflowDefinitionForAsset(
        h.db,
        ASSET,
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
      expect(def?.name).toBe("wf-name");
      expect(def?.description).toBe("WF Display");
      // Native: no origin agent.
      expect(def?.originAgentId).toBeNull();

      const versions = await h.db
        .select()
        .from(workflowDefinitionVersion)
        .where(eq(workflowDefinitionVersion.definitionId, definitionId));
      expect(versions).toHaveLength(1);
      expect(versions[0]?.version).toBe("1");
    });

    test("is idempotent: a second call returns the same id and adds no rows", async () => {
      const first = await ensureWorkflowDefinitionForAsset(h.db, ASSET);
      expect(first.created).toBe(true);

      const second = await ensureWorkflowDefinitionForAsset(h.db, ASSET);
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

    test("throws when the asset does not exist", async () => {
      await expect(
        ensureWorkflowDefinitionForAsset(h.db, "ast_missing"),
      ).rejects.toThrow(/no asset found/);
    });

    test("concurrent calls for one asset yield a single definition and version", async () => {
      // Concurrency safety is the whole reason for the conflict-and-read-back
      // path: several deploys of the same asset can race. Fire many at once and
      // assert exactly one definition, one version, and one winner.
      const results = await Promise.all(
        Array.from({ length: 8 }, () =>
          h.db.transaction((tx) => ensureWorkflowDefinitionForAsset(tx, ASSET)),
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
