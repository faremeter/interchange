// A standing (`scope: "always"`) resolution mutates the run's committed grant
// for one tool in place: `setRunToolGrantEffect` sets its effect to `allow`
// (approve-always) or `deny` (reject-always). The mutation is guarded to only
// change a grant currently gated `ask`, so it resolves the checkpoint in the
// operator's chosen direction and can never override an existing `allow`/`deny`
// or touch a tool the run does not already hold.
//
// Real DB: the mutation is a targeted UPDATE on the run principal's grant rows,
// so it is exercised against a migrated Postgres schema rather than a mock.

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { and, eq } from "drizzle-orm";

import { grant as grantTable } from "@intx/db/schema";
import { setRunToolGrantEffect } from "@intx/hub-api";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedGrant, seedPrincipal, seedTenants } from "@intx/test-harness/seed";

const TENANT = "tnt_grant_mut";
const RUN_ID = "run_grant_mut";
const RUN_PRINCIPAL = "prn_run_grant_mut";

describe.skipIf(!harnessDbEnvAvailable())(
  "setRunToolGrantEffect (real DB)",
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
      // The run's authorization principal: kind "workflow", refId the runId --
      // exactly what setRunToolGrantEffect resolves the run's grants by.
      await seedPrincipal(h.db, {
        id: RUN_PRINCIPAL,
        tenantId: TENANT,
        kind: "workflow",
        refId: RUN_ID,
      });
    });

    async function seedRunGrant(
      id: string,
      resource: string,
      effect: "allow" | "deny" | "ask",
    ): Promise<void> {
      await seedGrant(h.db, {
        id,
        tenantId: TENANT,
        resource,
        action: "invoke",
        effect,
        origin: "creator",
        principalId: RUN_PRINCIPAL,
      });
    }

    async function effectOf(resource: string): Promise<string | undefined> {
      const [row] = await h.db
        .select({ effect: grantTable.effect })
        .from(grantTable)
        .where(
          and(
            eq(grantTable.principalId, RUN_PRINCIPAL),
            eq(grantTable.resource, resource),
          ),
        );
      return row?.effect;
    }

    test("approve-always flips an ask tool to allow", async () => {
      await seedRunGrant("g_send", "tool:send_email", "ask");
      await setRunToolGrantEffect(h.db, TENANT, RUN_ID, "send_email", "allow");
      expect(await effectOf("tool:send_email")).toBe("allow");
    });

    test("reject-always flips an ask tool to deny", async () => {
      await seedRunGrant("g_charge", "tool:charge_card", "ask");
      await setRunToolGrantEffect(h.db, TENANT, RUN_ID, "charge_card", "deny");
      expect(await effectOf("tool:charge_card")).toBe("deny");
    });

    test("only changes an ask gate: an existing allow or deny is untouched", async () => {
      await seedRunGrant("g_read", "tool:read_file", "allow");
      await seedRunGrant("g_rm", "tool:rm_rf", "deny");
      // Neither is gated `ask`, so the guard leaves both as they are -- a
      // standing resolution can never override a settled allow or lift a deny.
      await setRunToolGrantEffect(h.db, TENANT, RUN_ID, "read_file", "deny");
      await setRunToolGrantEffect(h.db, TENANT, RUN_ID, "rm_rf", "allow");
      expect(await effectOf("tool:read_file")).toBe("allow");
      expect(await effectOf("tool:rm_rf")).toBe("deny");
    });

    test("no-op for a run principal that does not exist", async () => {
      await seedRunGrant("g_send2", "tool:send_email", "ask");
      await setRunToolGrantEffect(
        h.db,
        TENANT,
        "run_other",
        "send_email",
        "allow",
      );
      expect(await effectOf("tool:send_email")).toBe("ask");
    });

    test("no-op across a tenant boundary", async () => {
      await seedRunGrant("g_send3", "tool:send_email", "ask");
      // The run principal is resolved by (tenantId, kind, refId): a foreign
      // tenant with the same runId matches no principal, so the checkpoint is
      // left untouched -- one tenant can never resolve another tenant's grant.
      await setRunToolGrantEffect(
        h.db,
        "tnt_other",
        RUN_ID,
        "send_email",
        "allow",
      );
      expect(await effectOf("tool:send_email")).toBe("ask");
    });

    test("no-op for a tool the run does not hold: no row is created", async () => {
      // The run principal exists but holds no `tool:unheld` grant. The guarded
      // UPDATE matches nothing, so the run does not gain a grant it never had.
      await setRunToolGrantEffect(h.db, TENANT, RUN_ID, "unheld", "allow");
      expect(await effectOf("tool:unheld")).toBeUndefined();
    });
  },
);
