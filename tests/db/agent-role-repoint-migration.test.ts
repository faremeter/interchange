import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { readFileSync } from "node:fs";
import { and, eq, sql } from "drizzle-orm";

import { agentRole, role, workflowDefinition } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAgent, seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// Migration 0063 re-points agent_role.agent_id from agent to
// workflow_definition, rewriting each value from an agent id to its folded
// definition id across the constraint swap. The rewrite must run AFTER the old
// agent FK is dropped (it sets agent_id to a definition id, which is not an
// agent id), and a guard aborts up front if any agent_role row references an
// agent with no folded definition. This drives the real migration (lifted from
// the file) against seeded pre-migration rows to prove both. This test owns its
// own harness schema, so its temporary FK swap never leaks to another file.
// Strip the `"public".` qualifier drizzle emits: the harness pins search_path
// to a per-test schema (the production runner rewrites the qualifier the same
// way), so unqualified references resolve to that schema.
const MIGRATION_SQL = readFileSync(
  new URL(
    "../../packages/db/migrations/0063_repoint_agent_role_fk_to_definition.sql",
    import.meta.url,
  ),
  "utf-8",
).replaceAll('"public".', "");
const MIGRATION = sql.raw(MIGRATION_SQL);

const TENANT = "tnt";
const CREATOR = "prn_creator";

// Restore the pre-0063 shape: agent_role.agent_id references agent under the
// original constraint the migration drops.
async function restorePreMigrationFk(h: TestDb): Promise<void> {
  await h.db.execute(
    sql.raw(
      `ALTER TABLE "agent_role" DROP CONSTRAINT "agent_role_agent_id_workflow_definition_id_fk"`,
    ),
  );
  await h.db.execute(
    sql.raw(
      `ALTER TABLE "agent_role" ADD CONSTRAINT "agent_role_agent_id_agent_id_fk" FOREIGN KEY ("agent_id") REFERENCES "agent"("id") ON DELETE cascade`,
    ),
  );
}

describe.skipIf(!harnessDbEnvAvailable())(
  "0063 agent_role re-point (migration)",
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

    test("rewrites an existing agent_role's agent id to its definition id", async () => {
      await restorePreMigrationFk(h);

      await seedAgent(h.db, {
        id: "agt_x",
        tenantId: TENANT,
        creatorPrincipalId: CREATOR,
        name: "agent-x",
      });
      await h.db.insert(workflowDefinition).values({
        id: "wfd_x",
        tenantId: TENANT,
        name: "agent-x",
        originAgentId: "agt_x",
      });
      await h.db
        .insert(role)
        .values({ id: "rol_x", tenantId: TENANT, name: "reviewer" });
      // A role assignment pointing at the agent, as it stood before the fold.
      await h.db
        .insert(agentRole)
        .values({ agentId: "agt_x", roleId: "rol_x" });

      await h.db.execute(MIGRATION);

      // The assignment's agent_id now holds the folded definition's id; the FK
      // references workflow_definition (the migration's ADD CONSTRAINT validates
      // every row against it, so completing at all proves the rewrite).
      const row = await h.db.query.agentRole.findFirst({
        where: and(
          eq(agentRole.agentId, "wfd_x"),
          eq(agentRole.roleId, "rol_x"),
        ),
      });
      expect(row).toBeDefined();
    });

    test("aborts when an agent_role references an agent with no folded definition", async () => {
      await restorePreMigrationFk(h);

      // An agent that was never folded -- no workflow_definition names it as an
      // origin -- so its role row cannot be re-pointed.
      await seedAgent(h.db, {
        id: "agt_y",
        tenantId: TENANT,
        creatorPrincipalId: CREATOR,
        name: "agent-y",
      });
      await h.db
        .insert(role)
        .values({ id: "rol_y", tenantId: TENANT, name: "editor" });
      await h.db
        .insert(agentRole)
        .values({ agentId: "agt_y", roleId: "rol_y" });

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
      expect(message).toMatch(/no folded definition/);
    });
  },
);
