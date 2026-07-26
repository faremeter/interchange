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

import { offering, workflowDefinition } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAgent, seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// Migration 0058 re-points offering.agent_id from agent to workflow_definition,
// rewriting each value from an agent id to its folded definition id across the
// constraint swap. The rewrite must run AFTER the old agent FK is dropped: it
// sets agent_id to a definition id, which is not an agent id, so it would
// violate the still-live agent FK otherwise. This drives the real migration
// (lifted from the file) against a seeded pre-migration offering to prove the
// ordering. This test owns its own harness schema, so its temporary FK swap
// never leaks to another test file.
// Strip the `"public".` qualifier drizzle emits on the ADD CONSTRAINT: the
// harness runs each test in its own schema and pins search_path to it (the
// production runner rewrites the qualifier the same way), so unqualified
// references resolve to that schema.
const MIGRATION_SQL = readFileSync(
  new URL(
    "../../packages/db/migrations/0058_repoint_offering_fk_to_definition.sql",
    import.meta.url,
  ),
  "utf-8",
).replaceAll('"public".', "");
const MIGRATION = sql.raw(MIGRATION_SQL);

const TENANT = "tnt";
const CREATOR = "prn_creator";

describe.skipIf(!harnessDbEnvAvailable())(
  "0058 offering re-point (migration)",
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

    test("rewrites an existing offering's agent id to its definition id", async () => {
      // Restore the pre-0058 shape: offering.agent_id references the agent under
      // the original `capability_`-named constraint the migration drops.
      await h.db.execute(
        sql.raw(
          `ALTER TABLE "offering" DROP CONSTRAINT "offering_agent_id_workflow_definition_id_fk"`,
        ),
      );
      await h.db.execute(
        sql.raw(
          `ALTER TABLE "offering" ADD CONSTRAINT "capability_agent_id_agent_id_fk" FOREIGN KEY ("agent_id") REFERENCES "agent"("id") ON DELETE cascade`,
        ),
      );

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
      // An offering pointing at the agent, as it stood before the fold.
      await h.db.insert(offering).values({
        id: "off_x",
        tenantId: TENANT,
        agentId: "agt_x",
        name: "Translation",
      });

      await h.db.execute(MIGRATION);

      // The offering's agent_id now holds the folded definition's id, and the
      // FK references workflow_definition (a subsequent insert of a bare agent
      // id would be rejected -- proven by the migration completing at all, since
      // its ADD CONSTRAINT validates every row against workflow_definition).
      const row = await h.db.query.offering.findFirst({
        where: eq(offering.id, "off_x"),
      });
      expect(row?.agentId).toBe("wfd_x");
    });
  },
);
