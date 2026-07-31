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

import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";

const MIGRATION_PATH = join(
  import.meta.dir,
  "../../packages/db/migrations/0068_drop_agent_tables_and_origin_agent_id.sql",
);

// The migrated test schema already applied 0068, so agent_instance is gone.
// Recreate a minimal stand-in (drop-first so each test starts from a known
// empty table regardless of what a prior test left behind) to give the guard
// something to inspect.
async function recreateAgentInstance(h: TestDb): Promise<void> {
  await h.db.execute(sql.raw(`DROP TABLE IF EXISTS "agent_instance"`));
  await h.db.execute(
    sql.raw(`CREATE TABLE "agent_instance" (id text primary key)`),
  );
}

// Apply the real 0068 statements in file order, the way the non-transactional
// runner does. The guard is the first statement, so a populated agent_instance
// makes this reject before any DROP runs.
async function applyMigration0068(h: TestDb): Promise<void> {
  const file = readFileSync(MIGRATION_PATH, "utf8");
  for (const statement of file.split("--> statement-breakpoint")) {
    if (statement.trim() === "") continue;
    await h.db.execute(sql.raw(statement));
  }
}

describe.skipIf(!harnessDbEnvAvailable())(
  "migration 0068 agent-table drop guard (real DB)",
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

    test("aborts before dropping when agent_instance holds rows", async () => {
      await recreateAgentInstance(h);
      await h.db.execute(
        sql.raw(`INSERT INTO "agent_instance" (id) VALUES ('ins_live')`),
      );

      await expect(applyMigration0068(h)).rejects.toThrow(
        /agent_instance still holds rows/,
      );

      // The guard ran before the DROP, so the table and its row survive.
      // `db.execute` returns a lazy thenable, so coerce it to a real promise
      // before asserting on its settlement.
      await expect(
        Promise.resolve(
          h.db.execute(sql.raw(`SELECT 1 FROM "agent_instance"`)),
        ),
      ).resolves.toBeDefined();
    });

    test("applies and drops the table when agent_instance is empty", async () => {
      await recreateAgentInstance(h);

      await applyMigration0068(h);

      // The guard passed, so the DROP ran and the table is gone.
      await expect(
        Promise.resolve(
          h.db.execute(sql.raw(`SELECT 1 FROM "agent_instance"`)),
        ),
      ).rejects.toThrow();
    });
  },
);
