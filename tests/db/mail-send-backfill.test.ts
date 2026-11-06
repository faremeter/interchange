import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { readFileSync } from "node:fs";
import path from "node:path";

import { and, eq, like, sql } from "drizzle-orm";

import { matchPattern } from "@intx/authz";
import { grant, principalRole, role } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { REPO_ROOT } from "@intx/test-harness/env";
import {
  seedGrant,
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const BACKFILL_STATEMENTS = readFileSync(
  path.join(REPO_ROOT, "packages/db/migrations/0124_mail_send_backfill.sql"),
  "utf-8",
)
  .split("--> statement-breakpoint")
  .map((statement) => statement.trim());

const TENANT = "tnt";
const RUN_SEND = "mail.send:*:*";
const TENANT_SEND = `mail.send:tenant:${TENANT}`;

describe.skipIf(!harnessDbEnvAvailable())(
  "mail.send backfill migration (real DB)",
  () => {
    let h: TestDb;
    let grantCounter = 0;

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

    async function runBackfill(): Promise<void> {
      for (const statement of BACKFILL_STATEMENTS) {
        await h.db.execute(sql.raw(statement));
      }
    }

    async function seedRunPrincipal(
      id: string,
      status: "active" | "suspended" = "active",
    ) {
      await seedPrincipal(h.db, {
        id,
        tenantId: TENANT,
        kind: "workflow",
        refId: id,
        status,
      });
    }

    async function seedUser(id: string) {
      await seedPrincipal(h.db, {
        id,
        tenantId: TENANT,
        kind: "user",
        refId: `usr_${id}`,
        status: "active",
      });
    }

    async function grantTo(
      holder: { principalId: string } | { roleId: string },
      resource: string,
      action: string,
      extra: {
        effect?: "allow" | "deny" | "ask";
        origin?: "system" | "role" | "creator" | "invoker";
        expiresAt?: Date;
        conditions?: Record<string, unknown>;
      } = {},
    ) {
      grantCounter += 1;
      await seedGrant(h.db, {
        id: `grt_src_${String(grantCounter)}`,
        tenantId: TENANT,
        resource,
        action,
        ...holder,
        ...extra,
      });
    }

    async function sendRows(resource: string) {
      return h.db
        .select()
        .from(grant)
        .where(and(eq(grant.resource, resource), eq(grant.action, "send")));
    }

    describe("run principals", () => {
      test("a linked anchor of any status or principal state gets one row", async () => {
        await seedRunPrincipal("prn_running");
        await seedRunPrincipal("prn_done");
        await seedRunPrincipal("prn_suspended", "suspended");
        for (const [id, principalId, status] of [
          ["run_running", "prn_running", "running"],
          ["run_done", "prn_done", "completed"],
          ["run_suspended", "prn_suspended", "running"],
        ] as const) {
          await seedWorkflowRun(h.db, {
            id,
            tenantId: TENANT,
            anchorRunId: id,
            principalId,
            status,
          });
        }

        await runBackfill();

        const rows = await sendRows(RUN_SEND);
        expect(rows.map((row) => row.principalId).sort()).toEqual([
          "prn_done",
          "prn_running",
          "prn_suspended",
        ]);
        for (const row of rows) {
          expect(row).toMatchObject({
            effect: "allow",
            origin: "creator",
            conditions: null,
            expiresAt: null,
            roleId: null,
          });
        }
      });

      test("an anchor that has not fired and a child run get no row", async () => {
        await seedRunPrincipal("prn_parent");
        await seedWorkflowRun(h.db, {
          id: "run_fresh",
          tenantId: TENANT,
          anchorRunId: "run_fresh",
          status: "deployed",
        });
        await seedWorkflowRun(h.db, {
          id: "run_parent",
          tenantId: TENANT,
          anchorRunId: "run_parent",
          principalId: "prn_parent",
        });
        await seedWorkflowRun(h.db, {
          id: "run_child",
          tenantId: TENANT,
          anchorRunId: "run_parent",
          principalId: "prn_parent",
        });

        await runBackfill();

        expect(
          (await sendRows(RUN_SEND)).map((row) => row.principalId),
        ).toEqual(["prn_parent"]);
      });

      test("a run principal that holds workflow-run manage gets no tenant row", async () => {
        await seedRunPrincipal("prn_deployer_run");
        await seedWorkflowRun(h.db, {
          id: "run_deployer",
          tenantId: TENANT,
          anchorRunId: "run_deployer",
          principalId: "prn_deployer_run",
        });
        for (const origin of ["creator", "invoker"] as const) {
          await grantTo(
            { principalId: "prn_deployer_run" },
            "workflow-run:*",
            "manage",
            { origin },
          );
        }

        await runBackfill();

        expect(await sendRows(TENANT_SEND)).toEqual([]);
        expect(await sendRows(RUN_SEND)).toHaveLength(1);
      });
    });

    describe("users and roles", () => {
      test("a holder of a manage grant that can match a run gets a tenant row", async () => {
        for (const [user, resource, action] of [
          ["prn_exact", "workflow-run:run_abc", "manage"],
          ["prn_wildcard", "workflow-run:*", "manage"],
          ["prn_star", "*", "*"],
          ["prn_prefix", "workflow*", "manage"],
          ["prn_inner", "*-run:abc", "manage"],
        ] as const) {
          await seedUser(user);
          await grantTo({ principalId: user }, resource, action);
        }

        await runBackfill();

        expect(
          (await sendRows(TENANT_SEND)).map((row) => row.principalId).sort(),
        ).toEqual([
          "prn_exact",
          "prn_inner",
          "prn_prefix",
          "prn_star",
          "prn_wildcard",
        ]);
      });

      test("a holder of no matching manage allow gets no row", async () => {
        for (const [user, resource, action, effect] of [
          ["prn_other_ns", "wallet:*", "manage", "allow"],
          ["prn_no_colon", "workflow-run", "manage", "allow"],
          ["prn_read", "workflow-run:*", "read", "allow"],
          ["prn_deny", "workflow-run:*", "manage", "deny"],
          ["prn_ask", "workflow-run:*", "manage", "ask"],
        ] as const) {
          await seedUser(user);
          await grantTo({ principalId: user }, resource, action, { effect });
        }
        await seedUser("prn_expired");
        await grantTo({ principalId: "prn_expired" }, "*", "*", {
          expiresAt: new Date("2000-01-01T00:00:00.000Z"),
        });

        await runBackfill();

        expect(await sendRows(TENANT_SEND)).toEqual([]);
      });

      test("a role gets one row and its members none of their own", async () => {
        await h.db.insert(role).values({
          id: "rol_admin",
          tenantId: TENANT,
          name: "admin",
          isSystem: true,
        });
        await seedUser("prn_member");
        await h.db
          .insert(principalRole)
          .values({ principalId: "prn_member", roleId: "rol_admin" });
        for (const action of ["read", "create", "manage"]) {
          await grantTo({ roleId: "rol_admin" }, "*", action, {
            origin: "system",
          });
        }

        await runBackfill();

        const rows = await sendRows(TENANT_SEND);
        expect(rows).toHaveLength(1);
        expect(rows[0]).toMatchObject({
          roleId: "rol_admin",
          principalId: null,
          origin: "system",
        });
      });

      test("each source row keeps its origin, conditions and expiry", async () => {
        await seedUser("prn_two");
        const later = new Date("2999-01-01T00:00:00.000Z");
        const conditions = {
          time_window: { after: "09:00", before: "17:00", timezone: "UTC" },
        };
        await grantTo({ principalId: "prn_two" }, "*", "manage", {
          origin: "role",
        });
        await grantTo({ principalId: "prn_two" }, "workflow-run:*", "manage", {
          origin: "creator",
          expiresAt: later,
          conditions,
        });

        await runBackfill();

        const rows = await sendRows(TENANT_SEND);
        expect(
          rows
            .map((row) => ({
              origin: row.origin,
              conditions: row.conditions,
              expiresAt: row.expiresAt,
            }))
            .sort((a, b) => a.origin.localeCompare(b.origin)),
        ).toEqual([
          { origin: "creator", conditions, expiresAt: later },
          { origin: "role", conditions: null, expiresAt: null },
        ]);
      });

      // The send check cannot evaluate a `tool` condition, so a copy would
      // make the holder's sends fail.
      test("a source row with a tool condition gives no row", async () => {
        await seedUser("prn_tool");
        await grantTo({ principalId: "prn_tool" }, "workflow-run:*", "manage", {
          conditions: { tool: "tool:pkg" },
        });
        await grantTo({ principalId: "prn_tool" }, "workflow-run:*", "manage", {
          conditions: {
            tool: "tool:pkg",
            time_window: { after: "09:00", before: "17:00", timezone: "UTC" },
          },
        });

        await runBackfill();

        expect(await sendRows(TENANT_SEND)).toEqual([]);
      });

      // The API stores only an object or SQL NULL, but a direct write can
      // leave a jsonb scalar. Such a row must not abort the migration.
      test("a source row whose conditions are not an object does not abort it", async () => {
        await seedUser("prn_scalar");
        await grantTo({ principalId: "prn_scalar" }, "api:unrelated", "read");
        await grantTo(
          { principalId: "prn_scalar" },
          "workflow-run:*",
          "manage",
        );
        await grantTo(
          { principalId: "prn_scalar" },
          "workflow-run:a*",
          "manage",
        );
        await h.db.execute(
          sql`UPDATE "grant" SET "conditions" = '5'::jsonb WHERE "resource" IN ('api:unrelated', 'workflow-run:a*')`,
        );
        await h.db.execute(
          sql`UPDATE "grant" SET "conditions" = 'null'::jsonb WHERE "resource" = 'workflow-run:*'`,
        );

        await runBackfill();

        // The JSON-null row holds no condition and gives a row; the scalar
        // row is skipped.
        expect(await sendRows(TENANT_SEND)).toHaveLength(1);
      });
    });

    describe("rows that already exist", () => {
      test("a send row with empty conditions counts as the row", async () => {
        await seedRunPrincipal("prn_empty_conditions");
        await seedWorkflowRun(h.db, {
          id: "run_empty_conditions",
          tenantId: TENANT,
          anchorRunId: "run_empty_conditions",
          principalId: "prn_empty_conditions",
        });
        await grantTo(
          { principalId: "prn_empty_conditions" },
          RUN_SEND,
          "send",
          { conditions: {} },
        );

        await runBackfill();

        expect(await sendRows(RUN_SEND)).toHaveLength(1);
      });

      test("a run principal with only an expired or conditioned send row gets the row", async () => {
        await seedRunPrincipal("prn_expired_row");
        await seedRunPrincipal("prn_conditioned_row");
        for (const id of ["prn_expired_row", "prn_conditioned_row"]) {
          await seedWorkflowRun(h.db, {
            id: `run_${id}`,
            tenantId: TENANT,
            anchorRunId: `run_${id}`,
            principalId: id,
          });
        }
        await grantTo({ principalId: "prn_expired_row" }, RUN_SEND, "send", {
          expiresAt: new Date("2000-01-01T00:00:00.000Z"),
        });
        await grantTo(
          { principalId: "prn_conditioned_row" },
          RUN_SEND,
          "send",
          {
            conditions: {
              time_window: { after: "09:00", before: "17:00", timezone: "UTC" },
            },
          },
        );

        await runBackfill();

        const usable = (await sendRows(RUN_SEND)).filter(
          (row) => row.conditions === null && row.expiresAt === null,
        );
        expect(usable.map((row) => row.principalId).sort()).toEqual([
          "prn_conditioned_row",
          "prn_expired_row",
        ]);
      });

      test("a tenant whose system roles got their send rows at creation gets none", async () => {
        for (const id of ["rol_owner", "rol_admin"]) {
          await h.db
            .insert(role)
            .values({ id, tenantId: TENANT, name: id, isSystem: true });
        }
        await grantTo({ roleId: "rol_owner" }, "*", "*", { origin: "system" });
        await grantTo({ roleId: "rol_admin" }, "*", "manage", {
          origin: "system",
        });
        for (const roleId of ["rol_owner", "rol_admin"]) {
          await grantTo({ roleId }, TENANT_SEND, "send", { origin: "system" });
        }
        const before = (await sendRows(TENANT_SEND)).map((row) => row.id);

        await runBackfill();

        expect(
          (await sendRows(TENANT_SEND)).map((row) => row.id).sort(),
        ).toEqual(before.sort());
      });
    });

    // Each action is a grant on `workflow-run:*`; the statement gives its holder
    // a row exactly when the hub's own matcher says the action matches
    // `manage`, including LIKE metacharacters and several `*`.
    test("the action rule agrees with matchPattern", async () => {
      const actions = [
        "*",
        "manage",
        "man*",
        "*age",
        "m*e",
        "*an*",
        "**",
        "m_n*",
        "%*",
        "ma%*",
        "\\*",
        "m\\*",
        "manag_*",
        "*manage*",
        "manage*",
        "m*x",
        "_anage",
        "%",
        "Manage",
        "*Manage",
        "m*a*n*a*g*e",
        "m*ee",
        "ma*age*",
        "man*age*e",
      ];
      for (const [index, action] of actions.entries()) {
        await seedUser(`prn_action_${String(index)}`);
        await grantTo(
          { principalId: `prn_action_${String(index)}` },
          "workflow-run:*",
          action,
        );
      }

      await runBackfill();

      const holders = new Set(
        (await sendRows(TENANT_SEND)).map((row) => row.principalId),
      );
      expect(
        actions.filter((_action, index) =>
          holders.has(`prn_action_${String(index)}`),
        ),
      ).toEqual(actions.filter((action) => matchPattern(action, "manage")));
    });

    test("a user whose manage grant has origin invoker gets a row", async () => {
      await seedUser("prn_invoker_origin");
      await grantTo(
        { principalId: "prn_invoker_origin" },
        "workflow-run:*",
        "manage",
        { origin: "invoker" },
      );

      await runBackfill();

      expect(await sendRows(TENANT_SEND)).toMatchObject([
        { principalId: "prn_invoker_origin", origin: "invoker" },
      ]);
    });

    test("an action glob that matches manage gives a row", async () => {
      await seedUser("prn_glob");
      await seedUser("prn_other_glob");
      await grantTo({ principalId: "prn_glob" }, "workflow-run:*", "man*");
      await grantTo({ principalId: "prn_other_glob" }, "workflow-run:*", "re*");

      await runBackfill();

      expect(
        (await sendRows(TENANT_SEND)).map((row) => row.principalId),
      ).toEqual(["prn_glob"]);
    });

    test("source rows that agree on holder fields give one row", async () => {
      await seedUser("prn_two_runs");
      await grantTo(
        { principalId: "prn_two_runs" },
        "workflow-run:a",
        "manage",
      );
      await grantTo(
        { principalId: "prn_two_runs" },
        "workflow-run:b",
        "manage",
      );

      await runBackfill();

      expect(await sendRows(TENANT_SEND)).toHaveLength(1);
    });

    test("a second run adds no row for a source with conditions and expiry", async () => {
      await seedUser("prn_timed");
      await grantTo({ principalId: "prn_timed" }, "workflow-run:*", "manage", {
        conditions: {
          time_window: { after: "09:00", before: "17:00", timezone: "UTC" },
        },
        expiresAt: new Date("2999-01-01T00:00:00.123Z"),
      });

      await runBackfill();
      await runBackfill();

      expect(await sendRows(TENANT_SEND)).toHaveLength(1);
    });

    test("a second run adds no rows", async () => {
      await seedRunPrincipal("prn_run");
      await seedWorkflowRun(h.db, {
        id: "run_once",
        tenantId: TENANT,
        anchorRunId: "run_once",
        principalId: "prn_run",
      });
      await seedUser("prn_owner");
      await grantTo({ principalId: "prn_owner" }, "*", "*", {
        origin: "system",
      });

      await runBackfill();
      const first = await h.db
        .select()
        .from(grant)
        .where(like(grant.resource, "mail.send:%"));
      await runBackfill();
      const second = await h.db
        .select()
        .from(grant)
        .where(like(grant.resource, "mail.send:%"));

      expect(first).toHaveLength(2);
      expect(second.map((row) => row.id).sort()).toEqual(
        first.map((row) => row.id).sort(),
      );
    });
  },
);
