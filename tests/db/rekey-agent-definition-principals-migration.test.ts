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

import { principal, workflowDefinition } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// Migration 0066 re-keys each folded agent's definition-level principal onto the
// workflow model: kind agent -> workflow, ref_id agent.id -> definition.id,
// joined over origin_agent_id within a tenant. An instance-level agent principal
// (ref_id = agent_instance.id) matches no origin_agent_id, so it stays agent-
// kind. This drives the real migration (lifted from the file) against seeded
// pre-migration principals to prove the re-key, the instance-principal
// exclusion, and idempotency. Strip the `"public".` qualifier: the harness pins
// search_path to a per-test schema, so unqualified references resolve there.
const MIGRATION_SQL = readFileSync(
  new URL(
    "../../packages/db/migrations/0066_rekey_agent_definition_principals_to_workflow.sql",
    import.meta.url,
  ),
  "utf-8",
).replaceAll('"public".', "");
const MIGRATION = sql.raw(MIGRATION_SQL);

const TENANT = "tnt";

async function readPrincipal(h: TestDb, id: string) {
  return h.db.query.principal.findFirst({ where: eq(principal.id, id) });
}

describe.skipIf(!harnessDbEnvAvailable())(
  "0066 re-key agent definition principals (migration)",
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
    });

    test("re-keys a definition principal onto its folded definition", async () => {
      await h.db.insert(workflowDefinition).values({
        id: "wfd_x",
        tenantId: TENANT,
        name: "agent-x",
        originAgentId: "agt_x",
      });
      // A definition-level agent principal: ref_id is the legacy agent id.
      await seedPrincipal(h.db, {
        id: "prn_def",
        tenantId: TENANT,
        kind: "agent",
        refId: "agt_x",
      });

      await h.db.execute(MIGRATION);

      const row = await readPrincipal(h, "prn_def");
      expect(row?.kind).toBe("workflow");
      expect(row?.refId).toBe("wfd_x");
    });

    test("leaves an instance principal agent-kind", async () => {
      await h.db.insert(workflowDefinition).values({
        id: "wfd_x",
        tenantId: TENANT,
        name: "agent-x",
        originAgentId: "agt_x",
      });
      // An instance-level agent principal: ref_id is an agent_instance id, which
      // matches no origin_agent_id, so the join skips it.
      await seedPrincipal(h.db, {
        id: "prn_inst",
        tenantId: TENANT,
        kind: "agent",
        refId: "ins_1",
      });

      await h.db.execute(MIGRATION);

      const row = await readPrincipal(h, "prn_inst");
      expect(row?.kind).toBe("agent");
      expect(row?.refId).toBe("ins_1");
    });

    test("leaves an existing workflow run principal untouched", async () => {
      // A folded launch's run principal already keys on a run id; the re-key
      // rewrites only agent-kind rows, so this is left alone.
      await seedPrincipal(h.db, {
        id: "prn_run",
        tenantId: TENANT,
        kind: "workflow",
        refId: "run_1",
      });

      await h.db.execute(MIGRATION);

      const row = await readPrincipal(h, "prn_run");
      expect(row?.kind).toBe("workflow");
      expect(row?.refId).toBe("run_1");
    });

    test("is idempotent -- a second run is a no-op", async () => {
      await h.db.insert(workflowDefinition).values({
        id: "wfd_x",
        tenantId: TENANT,
        name: "agent-x",
        originAgentId: "agt_x",
      });
      await seedPrincipal(h.db, {
        id: "prn_def",
        tenantId: TENANT,
        kind: "agent",
        refId: "agt_x",
      });

      await h.db.execute(MIGRATION);
      await h.db.execute(MIGRATION);

      const row = await readPrincipal(h, "prn_def");
      expect(row?.kind).toBe("workflow");
      expect(row?.refId).toBe("wfd_x");
    });
  },
);
