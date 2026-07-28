import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { readFileSync } from "node:fs";
import { eq, sql } from "drizzle-orm";

import { workflowDefinition } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// Migration 0067 adds workflow_definition.kind (default 'workflow') and
// backfills folded definitions -- those with a non-null origin_agent_id -- to
// 'instance', so the launch route can gate on kind instead of origin_agent_id.
// This drives the real migration (lifted from the file) against seeded
// pre-migration rows to prove the default and the backfill. Strip the
// `"public".` qualifier: the harness pins search_path to a per-test schema.
const MIGRATION_SQL = readFileSync(
  new URL(
    "../../packages/db/migrations/0067_add_workflow_definition_kind.sql",
    import.meta.url,
  ),
  "utf-8",
).replaceAll('"public".', "");
const MIGRATION = sql.raw(MIGRATION_SQL);

const TENANT = "tnt";
const CREATOR = "prn_creator";

// Restore the pre-0067 shape: drop the column the migration adds, so ADD COLUMN
// + backfill can replay against seeded rows. IF EXISTS because the harness
// schema is shared across this file's tests.
async function restorePreMigrationColumn(h: TestDb): Promise<void> {
  await h.db.execute(
    sql.raw(`ALTER TABLE "workflow_definition" DROP COLUMN IF EXISTS "kind"`),
  );
}

// Seed a definition via raw SQL: a drizzle insert emits every schema column
// (including `kind` as DEFAULT), which references the column this test dropped.
// Raw SQL names only the columns it sets.
async function seedDefinition(
  h: TestDb,
  opts: { id: string; name: string; originAgentId: string | null },
): Promise<void> {
  await h.db.execute(
    sql`INSERT INTO workflow_definition (id, tenant_id, name, origin_agent_id)
        VALUES (${opts.id}, ${TENANT}, ${opts.name}, ${opts.originAgentId})`,
  );
}

describe.skipIf(!harnessDbEnvAvailable())(
  "0067 workflow_definition.kind backfill (migration)",
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
    });

    test("backfills a folded definition to instance", async () => {
      await restorePreMigrationColumn(h);
      await seedDefinition(h, {
        id: "wfd_folded",
        name: "folded",
        originAgentId: "agt_1",
      });

      await h.db.execute(MIGRATION);

      const row = await h.db.query.workflowDefinition.findFirst({
        where: eq(workflowDefinition.id, "wfd_folded"),
      });
      expect(row?.kind).toBe("instance");
    });

    test("leaves a native definition as workflow", async () => {
      await restorePreMigrationColumn(h);
      await seedDefinition(h, {
        id: "wfd_native",
        name: "native",
        originAgentId: null,
      });

      await h.db.execute(MIGRATION);

      const row = await h.db.query.workflowDefinition.findFirst({
        where: eq(workflowDefinition.id, "wfd_native"),
      });
      expect(row?.kind).toBe("workflow");
    });
  },
);
