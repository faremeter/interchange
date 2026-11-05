// Migration 0122 drops the deployment anchor's copy of its key once it agrees
// with the run principal, so this replays it over seeded rows.

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
import { asc, eq, sql } from "drizzle-orm";

import {
  loadRunPrincipalActiveKey,
  recordPublicKey,
  recordRunPrincipalKey,
  retireRunPrincipalKey,
} from "@intx/db";
import { principalKey } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const MIGRATION_SQL = readFileSync(
  join(
    import.meta.dir,
    "../../packages/db/migrations/0122_drop_workflow_run_public_key.sql",
  ),
  "utf8",
);
const TENANT = "tnt-migration-0122";
const KEY_A = "aa".repeat(32);
const KEY_B = "bb".repeat(32);

describe.skipIf(!harnessDbEnvAvailable())(
  "migration 0122 drop workflow_run public_key",
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
      // Restore the column this migration drops.
      await h.db.execute(
        sql`alter table workflow_run add column if not exists public_key text`,
      );
      await seedTenants(h.db, [{ id: TENANT }]);
    });

    // An anchor run and its run principal, with `anchorKey` on the anchor's
    // column and `principalKey` recorded on the run principal.
    async function seedAnchor(
      runId: string,
      keys: { anchorKey: string | null; principalKey: string | null },
    ): Promise<string> {
      await seedWorkflowRun(h.db, {
        id: runId,
        anchorRunId: runId,
        tenantId: TENANT,
      });
      await h.db.execute(
        sql`update workflow_run set public_key = ${keys.anchorKey} where id = ${runId}`,
      );
      const principalId = `prn_${runId}`;
      await seedPrincipal(h.db, {
        id: principalId,
        tenantId: TENANT,
        kind: "workflow",
        refId: runId,
        status: "active",
      });
      if (keys.principalKey !== null) {
        await recordPublicKey(h.db, principalId, keys.principalKey);
      }
      return principalId;
    }

    async function keysOf(principalId: string) {
      return h.db
        .select({
          publicKey: principalKey.publicKey,
          status: principalKey.status,
        })
        .from(principalKey)
        .where(eq(principalKey.principalId, principalId))
        .orderBy(asc(principalKey.createdAt));
    }

    async function columnExists(): Promise<boolean> {
      const rows = await h.db.execute(
        sql`select 1 from information_schema.columns where table_schema = current_schema() and table_name = 'workflow_run' and column_name = 'public_key'`,
      );
      return rows.length > 0;
    }

    // The query builder returns a thenable; adopt it so a rejection reaches
    // the assertion.
    function replay(): Promise<unknown> {
      return Promise.resolve(h.db.execute(sql.raw(MIGRATION_SQL)));
    }

    async function waitForBlocked(blockerPid: number) {
      // Postgres exposes lock waits through its activity view only.
      for (;;) {
        const waiting = await h.db.execute(
          sql`select pid from pg_stat_activity where ${blockerPid} = any(pg_blocking_pids(pid))`,
        );
        if (waiting.length > 0) return;
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
    }

    test("waits for a hub transaction that read a run and then records a key", async () => {
      await seedAnchor("run_live", { anchorKey: null, principalKey: null });
      const readDone = Promise.withResolvers<number>();
      const write = Promise.withResolvers<undefined>();
      // A hub transaction that reads a run and then records its key, as an
      // initialization completion does.
      const hub = h.db.transaction(async (tx) => {
        await tx.execute(sql`select 1 from workflow_run`);
        const [backend] = await tx.execute(sql`select pg_backend_pid() as pid`);
        const pid = backend?.["pid"];
        if (typeof pid !== "number") throw new Error("Missing backend PID");
        readDone.resolve(pid);
        await write.promise;
        await tx.execute(
          sql`update workflow_run set public_key = ${KEY_A} where id = 'run_live'`,
        );
        return recordRunPrincipalKey(tx, TENANT, "run_live", KEY_A);
      });
      void hub.catch(readDone.reject);
      const hubPid = await readDone.promise;

      const migration = replay();
      await waitForBlocked(hubPid);
      write.resolve(undefined);

      expect(await hub).toBe(true);
      await migration;
      expect(await columnExists()).toBe(false);
      expect(await loadRunPrincipalActiveKey(h.db, TENANT, "run_live")).toBe(
        KEY_A,
      );
    });

    test("does not wait for a hub transaction that changed a principal and then reads a run", async () => {
      await seedAnchor("run_live", { anchorKey: null, principalKey: null });
      const changed = Promise.withResolvers<undefined>();
      const read = Promise.withResolvers<undefined>();
      // A hub transaction that changes a principal and then reads runs, as a
      // principal status change re-derives the grants of the runs it delegated.
      const hub = h.db.transaction(async (tx) => {
        await tx.execute(
          sql`update principal set status = 'active' where id = 'prn_run_live'`,
        );
        changed.resolve(undefined);
        await read.promise;
        await tx.execute(sql`select 1 from workflow_run`);
      });
      void hub.catch(changed.reject);
      await changed.promise;

      await replay();
      read.resolve(undefined);
      await hub;
      expect(await columnExists()).toBe(false);
    });

    test("drops the column when every anchor agrees with its run principal", async () => {
      const keyed = await seedAnchor("run_keyed", {
        anchorKey: KEY_A,
        principalKey: KEY_A,
      });
      const unkeyed = await seedAnchor("run_unkeyed", {
        anchorKey: null,
        principalKey: null,
      });
      // A replaced sidecar's key, retired on both.
      const replaced = await seedAnchor("run_replaced", {
        anchorKey: null,
        principalKey: KEY_B,
      });
      await retireRunPrincipalKey(h.db, TENANT, "run_replaced");

      await replay();

      expect(await columnExists()).toBe(false);
      expect(await keysOf(keyed)).toEqual([
        { publicKey: KEY_A, status: "active" },
      ]);
      expect(await keysOf(unkeyed)).toEqual([]);
      expect(await keysOf(replaced)).toEqual([
        { publicKey: KEY_B, status: "retired" },
      ]);
    });

    test("refuses an anchor key its run principal does not hold as active", async () => {
      await seedAnchor("run_drifted", {
        anchorKey: KEY_A,
        principalKey: KEY_B,
      });

      await expect(replay()).rejects.toThrow(/does not hold as active/);
      expect(await columnExists()).toBe(true);
    });

    test("refuses an anchor key its run principal holds only as retired", async () => {
      await seedAnchor("run_retired", {
        anchorKey: KEY_A,
        principalKey: KEY_A,
      });
      await retireRunPrincipalKey(h.db, TENANT, "run_retired");

      await expect(replay()).rejects.toThrow(/does not hold as active/);
      expect(await columnExists()).toBe(true);
    });

    test("refuses an anchor with no key whose run principal holds an active key", async () => {
      await seedAnchor("run_cleared", { anchorKey: null, principalKey: KEY_A });

      await expect(replay()).rejects.toThrow(
        /without a key has a run principal/,
      );
      expect(await columnExists()).toBe(true);
    });
  },
);
