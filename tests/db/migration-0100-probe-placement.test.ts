import { readFileSync } from "node:fs";
import { join } from "node:path";

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { sql } from "drizzle-orm";

import { sha256 } from "@intx/crypto";
import { rewriteSchemaQualifiedReferences } from "@intx/db";
import { sidecar, type WorkflowProbeStatus } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedTenants } from "@intx/test-harness/seed";

const TENANT_ID = "tnt-probe-migration";
const ASSET_ID = "ast-probe-migration";

describe.skipIf(!harnessDbEnvAvailable())(
  "migration 0100 probe placement (real DB)",
  () => {
    let h: TestDb;

    beforeAll(async () => {
      h = await createTestDb();
    });

    afterAll(async () => {
      await h.close();
    });

    async function applyMigration(name: string): Promise<void> {
      const raw = readFileSync(
        join(import.meta.dir, "../../packages/db/migrations", name),
        "utf8",
      );
      const rendered = rewriteSchemaQualifiedReferences(raw, `"${h.schema}"`);
      for (const statement of rendered.split("--> statement-breakpoint")) {
        if (statement.trim() !== "") {
          await h.db.execute(sql.raw(statement));
        }
      }
    }

    beforeEach(async () => {
      await h.reset();
      // Restore the pre-0100 tables in this isolated schema so the complete
      // migration runs against retained data, including its foreign keys.
      await h.db.execute(sql`DROP TABLE IF EXISTS "execution_host_assignment"`);
      await h.db.execute(sql`DROP TABLE IF EXISTS "sidecar_operation" CASCADE`);
      await h.db.execute(sql`
        ALTER TABLE "workflow_probe"
        DROP COLUMN IF EXISTS "placement_principal_id",
        DROP COLUMN IF EXISTS "placement_policy"
      `);
      await applyMigration("0097_funny_wong.sql");
      await applyMigration("0098_lush_rumiko_fujikawa.sql");
      await applyMigration("0099_spotty_husk.sql");
      await seedTenants(h.db, [{ id: TENANT_ID }]);
      await seedAsset(h.db, {
        id: ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "probe-migration",
      });
    });

    async function seedLegacyProbe(
      status: WorkflowProbeStatus,
      hasCapacity: boolean,
    ) {
      const sidecarId = hasCapacity ? `sc-${status}` : null;
      if (sidecarId !== null) {
        await h.db.insert(sidecar).values({
          id: sidecarId,
          tokenHashSha256: await sha256(sidecarId),
        });
      }
      await h.db.execute(sql`
        INSERT INTO "workflow_probe" (
          "id", "tenant_id", "definition_asset_id", "source", "entry",
          "status", "provisioner_id", "provisioner_api_version",
          "provisioner_binding_fingerprint", "sidecar_id", "external_ref", "result"
        ) VALUES (
          ${`legacy-${status}`}, ${TENANT_ID}, ${ASSET_ID},
          '{"kind":"registry","registry":"test"}'::jsonb, './workflow.mjs', ${status},
          'legacy-provider', 1, 'legacy-provider:v1', ${sidecarId},
          ${hasCapacity ? `capacity-${status}` : null},
          ${status === "succeeded" ? "{}" : null}::jsonb
        )
      `);
    }

    test("discards existing probes before adding required placement fields", async () => {
      await seedLegacyProbe("succeeded", true);
      await seedLegacyProbe("failed", true);
      await seedLegacyProbe("pending", false);
      await seedLegacyProbe("provisioning", true);
      await seedLegacyProbe("probing", true);
      await seedLegacyProbe("releasing", true);

      await applyMigration("0100_sidecar_operation.sql");

      expect(await h.db.query.workflowProbe.findMany()).toEqual([]);
      expect(await h.db.query.sidecarOperation.findMany()).toEqual([]);
      expect(await h.db.query.sidecar.findMany()).toHaveLength(5);
      const columns = await h.db.execute(sql`
        SELECT column_name, is_nullable FROM information_schema.columns
        WHERE table_schema = ${h.schema} AND table_name = 'workflow_probe'
          AND column_name IN ('placement_principal_id', 'placement_policy')
        ORDER BY column_name
      `);
      expect([...columns]).toEqual([
        { column_name: "placement_policy", is_nullable: "NO" },
        { column_name: "placement_principal_id", is_nullable: "NO" },
      ]);
    });
  },
);
