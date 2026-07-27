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

import { agentSession, workflowDefinition } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAgent, seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// Migration 0064 re-points agent_session.agent_id from agent to
// workflow_definition, rewriting each value from an agent id to its folded
// definition id across the constraint swap. Two things distinguish it from the
// offering/agent_role re-points: (1) the dropped constraint is the inline
// auto-name agent_session_agent_id_fkey (from 0008), not the drizzle-style
// name; (2) the guard is on the AGENT table, since the launch handler writes
// fresh session rows at runtime for agents with no existing sessions. This
// drives the real migration (lifted from the file) against seeded
// pre-migration rows to prove both the rewrite and the guard. This test owns
// its own harness schema, so its temporary FK swap never leaks to another file.
// Strip the `"public".` qualifier drizzle emits: the harness pins search_path
// to a per-test schema (the production runner rewrites the qualifier the same
// way), so unqualified references resolve to that schema.
const MIGRATION_SQL = readFileSync(
  new URL(
    "../../packages/db/migrations/0064_repoint_agent_session_fk_to_definition.sql",
    import.meta.url,
  ),
  "utf-8",
).replaceAll('"public".', "");
const MIGRATION = sql.raw(MIGRATION_SQL);

const TENANT = "tnt";
const CREATOR = "prn_creator";

// Restore the pre-0064 shape: agent_session.agent_id references agent under the
// inline auto-name the migration drops.
async function restorePreMigrationFk(h: TestDb): Promise<void> {
  await h.db.execute(
    sql.raw(
      `ALTER TABLE "agent_session" DROP CONSTRAINT "agent_session_agent_id_workflow_definition_id_fk"`,
    ),
  );
  await h.db.execute(
    sql.raw(
      `ALTER TABLE "agent_session" ADD CONSTRAINT "agent_session_agent_id_fkey" FOREIGN KEY ("agent_id") REFERENCES "agent"("id") ON DELETE cascade`,
    ),
  );
}

describe.skipIf(!harnessDbEnvAvailable())(
  "0064 agent_session re-point (migration)",
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

    test("rewrites an existing session's agent id to its definition id", async () => {
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
      // A session pointing at the agent, as it stood before the fold.
      await h.db.insert(agentSession).values({
        id: "ses_x",
        tenantId: TENANT,
        agentId: "agt_x",
        principalId: CREATOR,
        status: "active",
      });

      await h.db.execute(MIGRATION);

      // The session's agent_id now holds the folded definition's id; the FK
      // references workflow_definition (the migration's ADD CONSTRAINT validates
      // every row against it, so completing at all proves the rewrite).
      const row = await h.db.query.agentSession.findFirst({
        where: eq(agentSession.id, "ses_x"),
      });
      expect(row?.agentId).toBe("wfd_x");
    });

    test("aborts when an agent has no folded definition", async () => {
      await restorePreMigrationFk(h);

      // An agent that was never folded -- no workflow_definition names it as an
      // origin. The guard is on the agent table (not agent_session), so it fires
      // even though this agent has no session row yet: the launch writer would
      // otherwise explode on its undefined definition id.
      await seedAgent(h.db, {
        id: "agt_y",
        tenantId: TENANT,
        creatorPrincipalId: CREATOR,
        name: "agent-y",
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
      expect(message).toMatch(/no folded definition/);
    });
  },
);
