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
import { seedAgent, seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// Migration 0065 adds workflow_definition.model_requirements and backfills it
// from each folded definition's agent (joined over origin_agent_id), so a
// folded launch resolves its inference sources from the definition rather than
// the agent row. A guard aborts up front if any folded definition names an
// agent that does not exist, since that join would otherwise silently leave the
// column null. This drives the real migration (lifted from the file) against
// seeded pre-migration rows to prove both the backfill and the guard. This test
// owns its own harness schema, so its temporary column drop never leaks to
// another file. Strip the `"public".` qualifier: the harness pins search_path
// to a per-test schema, so unqualified references resolve to that schema.
const MIGRATION_SQL = readFileSync(
  new URL(
    "../../packages/db/migrations/0065_add_workflow_definition_model_requirements.sql",
    import.meta.url,
  ),
  "utf-8",
).replaceAll('"public".', "");
const MIGRATION = sql.raw(MIGRATION_SQL);

const TENANT = "tnt";
const CREATOR = "prn_creator";

// Restore the pre-0065 shape: drop the column the migration adds, so the
// migration's ADD COLUMN + backfill can replay against seeded rows. IF EXISTS
// because the harness schema is shared across this file's tests and a prior
// test's migration re-adds the column (reset truncates data, not schema).
async function restorePreMigrationColumn(h: TestDb): Promise<void> {
  await h.db.execute(
    sql.raw(
      `ALTER TABLE "workflow_definition" DROP COLUMN IF EXISTS "model_requirements"`,
    ),
  );
}

// Seed a folded definition via raw SQL: a drizzle insert emits every schema
// column (including `model_requirements` as DEFAULT), which references the
// column this test has dropped. Raw SQL names only the columns it sets.
async function seedFoldedDefinition(
  h: TestDb,
  opts: { id: string; name: string; originAgentId: string },
): Promise<void> {
  await h.db.execute(
    sql`INSERT INTO workflow_definition (id, tenant_id, name, origin_agent_id)
        VALUES (${opts.id}, ${TENANT}, ${opts.name}, ${opts.originAgentId})`,
  );
}

describe.skipIf(!harnessDbEnvAvailable())(
  "0065 model_requirements backfill (migration)",
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

    test("mirrors each folded definition's agent model requirements", async () => {
      await restorePreMigrationColumn(h);

      await seedAgent(h.db, {
        id: "agt_x",
        tenantId: TENANT,
        creatorPrincipalId: CREATOR,
        name: "agent-x",
        modelRequirements: [{ model: "opus" }],
      });
      // The folded definition, as it stood before the column existed (no
      // model_requirements written).
      await seedFoldedDefinition(h, {
        id: "wfd_x",
        name: "agent-x",
        originAgentId: "agt_x",
      });

      await h.db.execute(MIGRATION);

      const row = await h.db.query.workflowDefinition.findFirst({
        where: eq(workflowDefinition.id, "wfd_x"),
      });
      expect(row?.modelRequirements).toEqual([{ model: "opus" }]);
    });

    test("preserves a null manifest verbatim", async () => {
      await restorePreMigrationColumn(h);

      // An agent with no model requirements is legitimate; the backfill copies
      // the null through rather than inventing an empty manifest.
      await seedAgent(h.db, {
        id: "agt_null",
        tenantId: TENANT,
        creatorPrincipalId: CREATOR,
        name: "agent-null",
      });
      await seedFoldedDefinition(h, {
        id: "wfd_null",
        name: "agent-null",
        originAgentId: "agt_null",
      });

      await h.db.execute(MIGRATION);

      const row = await h.db.query.workflowDefinition.findFirst({
        where: eq(workflowDefinition.id, "wfd_null"),
      });
      expect(row?.modelRequirements).toBeNull();
    });

    test("aborts when a folded definition references a missing agent", async () => {
      await restorePreMigrationColumn(h);

      // A folded definition whose origin agent does not exist -- a broken
      // back-reference. The join would silently skip it, so the guard aborts.
      await seedFoldedDefinition(h, {
        id: "wfd_ghost",
        name: "ghost",
        originAgentId: "agt_ghost",
      });

      let err: unknown;
      try {
        await h.db.execute(MIGRATION);
      } catch (e) {
        err = e;
      }
      expect(err).toBeDefined();
      // The RAISE message surfaces on the driver error's `cause`.
      const message =
        err instanceof Error
          ? `${err.message} ${err.cause instanceof Error ? err.cause.message : ""}`
          : String(err);
      expect(message).toMatch(/missing agent/);
    });
  },
);
