import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { asc, eq } from "drizzle-orm";

import {
  recordRunPrincipalKey,
  retireRunPrincipalKey,
  runPrincipalActiveKeySql,
  runPrincipalHasActiveKey,
} from "@intx/db";
import { principalKey, workflowDefinition, workflowRun } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedPrincipal,
  seedRunPrincipalKey,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const TENANT = "tnt_rpk";
const RUN_ID = "run_rpk";
const RUN_PRINCIPAL = "prn_run_rpk";
const FIRST_KEY = "aa".repeat(32);
const REPLACEMENT_KEY = "bb".repeat(32);

// A deployment's sidecar key moves through the run principal: the first
// sidecar records it, a replacement retires it, and the replacement records
// its own key -- or the first key again when it reuses the same disk.
describe.skipIf(!harnessDbEnvAvailable())(
  "run principal keys (real DB)",
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
        id: RUN_PRINCIPAL,
        tenantId: TENANT,
        kind: "workflow",
        refId: RUN_ID,
        status: "active",
      });
    });

    async function keys() {
      return h.db
        .select({
          publicKey: principalKey.publicKey,
          status: principalKey.status,
        })
        .from(principalKey)
        .where(eq(principalKey.principalId, RUN_PRINCIPAL))
        .orderBy(asc(principalKey.publicKey));
    }

    test("a replacement retires the key and records its own", async () => {
      await recordRunPrincipalKey(h.db, TENANT, RUN_ID, FIRST_KEY);
      await retireRunPrincipalKey(h.db, TENANT, RUN_ID);
      await recordRunPrincipalKey(h.db, TENANT, RUN_ID, REPLACEMENT_KEY);

      expect(await keys()).toEqual([
        { publicKey: FIRST_KEY, status: "retired" },
        { publicKey: REPLACEMENT_KEY, status: "active" },
      ]);
    });

    test("a replacement that reports the retired key makes it active again", async () => {
      await recordRunPrincipalKey(h.db, TENANT, RUN_ID, FIRST_KEY);
      await retireRunPrincipalKey(h.db, TENANT, RUN_ID);
      await recordRunPrincipalKey(h.db, TENANT, RUN_ID, FIRST_KEY);

      expect(await keys()).toEqual([
        { publicKey: FIRST_KEY, status: "active" },
      ]);
    });

    test("a query over many runs reads each run's own active key", async () => {
      const OTHER_TENANT = "tnt_rpk_other";
      await seedTenants(h.db, [{ id: OTHER_TENANT }]);
      for (const id of [RUN_ID, "run_retired", "run_unkeyed", "run_foreign"]) {
        await seedWorkflowRun(h.db, { id, tenantId: TENANT, anchorRunId: id });
      }
      await recordRunPrincipalKey(h.db, TENANT, RUN_ID, FIRST_KEY);
      await seedRunPrincipalKey(h.db, TENANT, "run_retired", REPLACEMENT_KEY);
      await retireRunPrincipalKey(h.db, TENANT, "run_retired");
      // A run principal with the same run id in another tenant is not the
      // run's principal.
      await seedRunPrincipalKey(
        h.db,
        OTHER_TENANT,
        "run_foreign",
        "cc".repeat(32),
      );

      // Joined, as the run list reads runs, so the subqueries' outer columns
      // are qualified against more than one table in scope.
      const rows = await h.db
        .select({
          id: workflowRun.id,
          publicKey: runPrincipalActiveKeySql(
            workflowRun.tenantId,
            workflowRun.id,
          ),
          initialized: runPrincipalHasActiveKey(
            workflowRun.tenantId,
            workflowRun.id,
          ),
        })
        .from(workflowRun)
        .innerJoin(
          workflowDefinition,
          eq(workflowDefinition.id, workflowRun.definitionId),
        )
        .where(eq(workflowRun.tenantId, TENANT))
        .orderBy(asc(workflowRun.id));

      expect(rows).toEqual([
        { id: "run_foreign", publicKey: null, initialized: false },
        { id: "run_retired", publicKey: null, initialized: false },
        { id: RUN_ID, publicKey: FIRST_KEY, initialized: true },
        { id: "run_unkeyed", publicKey: null, initialized: false },
      ]);
    });

    test("a run with no run principal cannot record a key and has none to retire", async () => {
      await expect(
        recordRunPrincipalKey(h.db, TENANT, "run_missing", FIRST_KEY),
      ).rejects.toThrow(/has no run principal/);
      await retireRunPrincipalKey(h.db, TENANT, "run_missing");
      expect(await keys()).toEqual([]);
    });
  },
);
