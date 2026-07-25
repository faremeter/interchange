import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { readFileSync } from "node:fs";
import { sql } from "drizzle-orm";

import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedTenants } from "@intx/test-harness/seed";
import { grant } from "@intx/db/schema";

// Load migration 0054 itself rather than a copy of its SQL, so the test and the
// migration cannot drift. The migration applies to an empty test schema at
// harness setup (nothing to re-key), so this replays it against seeded rows.
const MIGRATION_SQL = readFileSync(
  new URL(
    "../../packages/db/migrations/0054_rekey_instance_grants_to_workflow_run.sql",
    import.meta.url,
  ),
  "utf-8",
);
const REKEY = sql.raw(MIGRATION_SQL);

describe.skipIf(!harnessDbEnvAvailable())(
  "re-key instance grants to workflow-run (migration 0054)",
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

    async function seedGrants(): Promise<void> {
      await seedTenants(h.db, [{ id: "tnt_root" }]);
      await h.db.insert(grant).values([
        {
          id: "g_wild",
          tenantId: "tnt_root",
          resource: "instance:*",
          action: "create",
          effect: "allow",
          origin: "system",
        },
        {
          id: "g_id",
          tenantId: "tnt_root",
          resource: "instance:ins_abc",
          action: "read",
          effect: "allow",
          origin: "creator",
        },
        {
          id: "g_state",
          tenantId: "tnt_root",
          resource: "agent-state:ins_abc",
          action: "read",
          effect: "allow",
          origin: "creator",
        },
        {
          id: "g_run",
          tenantId: "tnt_root",
          resource: "workflow-run:dep_1",
          action: "read",
          effect: "allow",
          origin: "creator",
        },
      ]);
    }

    async function resourceOf(id: string): Promise<string | undefined> {
      const row = await h.db.query.grant.findFirst({
        where: (g, { eq }) => eq(g.id, id),
      });
      return row?.resource;
    }

    test("re-keys instance grants prefix-identity, leaving agent-state and workflow-run intact", async () => {
      await seedGrants();

      await h.db.execute(REKEY);

      // instance: -> workflow-run:, id after the colon unchanged.
      expect(await resourceOf("g_wild")).toBe("workflow-run:*");
      expect(await resourceOf("g_id")).toBe("workflow-run:ins_abc");
      // Colon-exact match never touches the agent-state sibling.
      expect(await resourceOf("g_state")).toBe("agent-state:ins_abc");
      // An already-migrated workflow-run grant is untouched.
      expect(await resourceOf("g_run")).toBe("workflow-run:dep_1");
    });

    test("is idempotent: a second run changes nothing", async () => {
      await seedGrants();
      await h.db.execute(REKEY);
      await h.db.execute(REKEY);

      expect(await resourceOf("g_wild")).toBe("workflow-run:*");
      expect(await resourceOf("g_id")).toBe("workflow-run:ins_abc");
      expect(await resourceOf("g_state")).toBe("agent-state:ins_abc");
    });
  },
);
