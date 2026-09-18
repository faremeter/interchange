import { describe, expect, test } from "bun:test";
import { sql } from "drizzle-orm";

import { createDB } from "@intx/db";
import {
  harnessDbEnvAvailable,
  loadHarnessDbConfig,
} from "@intx/test-harness/db-harness";

describe.skipIf(!harnessDbEnvAvailable())("database statement timeout", () => {
  test("sets a default deadline on application connections", async () => {
    const handle = createDB({ ...loadHarnessDbConfig(), max: 1 });
    try {
      const rows = await handle.db.execute(sql`SHOW statement_timeout`);
      expect(rows[0]?.["statement_timeout"]).toBe("1min");
    } finally {
      await handle.close();
    }
  });

  test("an overridden deadline cancels SQL, rolls back, and releases the connection", async () => {
    const handle = createDB({
      ...loadHarnessDbConfig(),
      max: 1,
      statementTimeoutMs: 100,
    });
    try {
      const rows = await handle.db.execute(sql`SHOW statement_timeout`);
      expect(rows[0]?.["statement_timeout"]).toBe("100ms");
      await handle.db.execute(
        sql`CREATE TEMP TABLE timeout_rollback (id integer)`,
      );
      await expect(
        handle.db.transaction(async (tx) => {
          await tx.execute(sql`INSERT INTO timeout_rollback VALUES (1)`);
          await tx.execute(sql`SELECT pg_sleep(1)`);
        }),
      ).rejects.toMatchObject({ cause: { code: "57014" } });

      // With one connection, this can finish only after the timed-out
      // transaction has rolled back and returned its connection to the pool.
      const remaining = await handle.db.execute(
        sql`SELECT id FROM timeout_rollback`,
      );
      expect(remaining).toHaveLength(0);
    } finally {
      await handle.close();
    }
  });

  test("cancels a lock wait while its holder stays connected", async () => {
    const config = loadHarnessDbConfig();
    const holder = createDB({ ...config, max: 1 });
    const waiter = createDB({
      ...config,
      max: 1,
      statementTimeoutMs: 100,
    });
    const lockId = Math.floor(Math.random() * 2_147_483_647);
    const locked = Promise.withResolvers<boolean>();
    const release = Promise.withResolvers<boolean>();
    const holding = holder.db.transaction(async (tx) => {
      await tx.execute(sql`SELECT pg_advisory_xact_lock(81037, ${lockId})`);
      locked.resolve(true);
      await release.promise;
    });
    try {
      await Promise.race([locked.promise, holding]);
      await expect(
        waiter.db
          .execute(sql`SELECT pg_advisory_xact_lock(81037, ${lockId})`)
          .execute(),
      ).rejects.toMatchObject({ cause: { code: "57014" } });
      release.resolve(true);
      await holding;
      await waiter.db.execute(
        sql`SELECT pg_advisory_xact_lock(81037, ${lockId})`,
      );
    } finally {
      release.resolve(true);
      try {
        await holding;
      } finally {
        await Promise.all([holder.close(), waiter.close()]);
      }
    }
  });
});
