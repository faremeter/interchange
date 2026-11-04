import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { asc, eq } from "drizzle-orm";

import { recordRunPrincipalKey, retireRunPrincipalKey } from "@intx/db";
import { principalKey } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";

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

    test("a run with no run principal cannot record a key and has none to retire", async () => {
      await expect(
        recordRunPrincipalKey(h.db, TENANT, "run_missing", FIRST_KEY),
      ).rejects.toThrow(/has no run principal/);
      await retireRunPrincipalKey(h.db, TENANT, "run_missing");
      expect(await keys()).toEqual([]);
    });
  },
);
