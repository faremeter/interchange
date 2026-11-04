// Migration 0120 gives existing deployments their run principals once,
// against real data, so this replays it over seeded rows.

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

import { principal, principalKey } from "@intx/db/schema";
import { deriveRunPrincipalId } from "@intx/hub-common";
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
    "../../packages/db/migrations/0120_create_run_principals.sql",
  ),
  "utf8",
);
const TENANT = "tnt-migration-0120";

describe.skipIf(!harnessDbEnvAvailable())(
  "migration 0120 run principals",
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

    async function runPrincipalOf(runId: string) {
      return h.db
        .select({
          id: principal.id,
          kind: principal.kind,
          status: principal.status,
          createdAt: principal.createdAt,
          updatedAt: principal.updatedAt,
        })
        .from(principal)
        .where(eq(principal.refId, runId))
        .orderBy(asc(principal.id));
    }

    test("gives each anchor without one a keyless principal with the derived id", async () => {
      const liveAt = new Date("2026-01-02T03:04:05.000Z");
      const deployedAt = new Date("2026-01-03T03:04:05.000Z");
      const endedAt = new Date("2026-01-04T03:04:05.000Z");
      // A non-ASCII run id checks that the SQL hashes the same UTF-8 bytes.
      const unicodeRun = "run_migration_ünïcode";
      await seedWorkflowRun(h.db, {
        id: "run_migration_live",
        anchorRunId: "run_migration_live",
        tenantId: TENANT,
        status: "deployed",
        createdAt: liveAt,
      });
      await seedWorkflowRun(h.db, {
        id: unicodeRun,
        anchorRunId: unicodeRun,
        tenantId: TENANT,
        status: "running",
        createdAt: liveAt,
      });
      await seedWorkflowRun(h.db, {
        id: "run_migration_ended",
        anchorRunId: "run_migration_ended",
        tenantId: TENANT,
        status: "cancelled",
        createdAt: deployedAt,
        endedAt,
      });

      await h.db.execute(sql.raw(MIGRATION_SQL));

      expect(await runPrincipalOf("run_migration_live")).toEqual([
        {
          id: await deriveRunPrincipalId(TENANT, "run_migration_live"),
          kind: "workflow",
          status: "active",
          createdAt: liveAt,
          updatedAt: liveAt,
        },
      ]);
      expect(await runPrincipalOf(unicodeRun)).toEqual([
        {
          id: await deriveRunPrincipalId(TENANT, unicodeRun),
          kind: "workflow",
          status: "active",
          createdAt: liveAt,
          updatedAt: liveAt,
        },
      ]);
      expect(await runPrincipalOf("run_migration_ended")).toEqual([
        {
          id: await deriveRunPrincipalId(TENANT, "run_migration_ended"),
          kind: "workflow",
          status: "deactivated",
          createdAt: deployedAt,
          updatedAt: endedAt,
        },
      ]);
      expect(await h.db.select().from(principalKey)).toEqual([]);
    });

    test("keeps an anchor's existing principal and skips child runs", async () => {
      await seedPrincipal(h.db, {
        id: "prn_migration_fired",
        tenantId: TENANT,
        kind: "workflow",
        refId: "run_migration_fired",
        status: "suspended",
      });
      await seedWorkflowRun(h.db, {
        id: "run_migration_fired",
        anchorRunId: "run_migration_fired",
        tenantId: TENANT,
        principalId: "prn_migration_fired",
        status: "running",
      });
      await seedWorkflowRun(h.db, {
        id: "run_migration_child",
        anchorRunId: "run_migration_fired",
        tenantId: TENANT,
        status: "running",
      });

      await h.db.execute(sql.raw(MIGRATION_SQL));
      // A second replay changes nothing.
      await h.db.execute(sql.raw(MIGRATION_SQL));

      expect(await runPrincipalOf("run_migration_fired")).toEqual([
        expect.objectContaining({
          id: "prn_migration_fired",
          status: "suspended",
        }),
      ]);
      expect(await runPrincipalOf("run_migration_child")).toEqual([]);
    });
  },
);
