// Verifies the mail.accept backfill migration against a real,
// schema-isolated database.
//
// The migration runs once during `createTestDb` when the tables are empty, so
// its effect on seeded data is exercised by re-executing the exact data
// statements the migration ships. Its column and check are already in place
// from that first run, so the two schema statements are not re-executed.
// Re-running the data statements is also the idempotency scenario: a second
// application must change nothing and must not error.

import { readFileSync } from "node:fs";
import path from "node:path";

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { and, eq, sql } from "drizzle-orm";

import {
  createGrantStore,
  createPrincipalKeyStore,
  createRunTokenStore,
} from "@intx/db";
import {
  grant,
  workflowDefinition,
  workflowDefinitionVersion,
  workflowRun,
  workflowRunLaunchSpec,
} from "@intx/db/schema";
import type { GrantWalkSnapshot } from "@intx/types";
import type { MailAcceptCoordinate } from "@intx/authz";
import {
  createCorrespondentGrantMinter,
  createMailTriggeredRunGrantsMaterializer,
} from "@intx/hub-api";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import { REPO_ROOT } from "@intx/test-harness/env";
import {
  seedAsset,
  seedPrincipal,
  seedTenants,
  seedWorkflowRunLaunchSpec,
} from "@intx/test-harness/seed";

const BACKFILL_SQL = readFileSync(
  path.join(REPO_ROOT, "packages/db/migrations/0113_mail_accept_backfill.sql"),
  "utf-8",
);

// The data statements of the migration, without its two schema statements.
const BACKFILL_DATA_STATEMENTS = BACKFILL_SQL.split("--> statement-breakpoint")
  .map((statement) => statement.trim())
  .filter((statement) => !/^ALTER TABLE /m.test(statement));

const TENANT = "tnt";
const ASSET = "ast";
const CREATOR = "prn_creator";
const MAIL_DEFINITION = "wfd_mail";
const MANUAL_DEFINITION = "wfd_manual";
const SENDING_DEFINITION = "wfd_sending";
const DEPLOYMENT = "run_mail";
const WORKFLOW_ADDRESS = "run_mail@tenant.example";
const SENDING_DEPLOYMENT = "run_sending";
const SENDING_ADDRESS = "run_sending@tenant.example";
const MANUAL_DEPLOYMENT = "run_manual";
const LIVE_RUN = "<mail-run-live@tenant.example>";
const LIVE_RUN_PRINCIPAL = "prn_live_run";
const TENANT_ROW = `mail.accept:tenant:${TENANT}`;

// A snapshot that a mail-trigger definition froze before the sender gate: the
// trigger-derived `mail.address:` / `mail.send:` grants and no accept markers.
function preGateMailSnapshot(): GrantWalkSnapshot {
  return {
    perStep: [
      {
        stepId: "draft",
        grants: [
          "tool:read_file",
          `mail.address:${WORKFLOW_ADDRESS}`,
          "mail.send:tenant.example",
        ],
        grantEffects: { "tool:read_file": "allow" },
      },
      {
        stepId: "send",
        grants: [
          `mail.address:${WORKFLOW_ADDRESS}`,
          "mail.send:tenant.example",
        ],
        grantEffects: {},
      },
    ],
    grantRequirements: [],
  };
}

function manualSnapshot(): GrantWalkSnapshot {
  return {
    perStep: [
      {
        stepId: "work",
        grants: ["tool:read_file"],
        grantEffects: { "tool:read_file": "allow" },
      },
    ],
    grantRequirements: [],
  };
}

// A snapshot of a definition with no mail trigger whose agent sends mail.
function sendingSnapshot(): GrantWalkSnapshot {
  return {
    perStep: [
      {
        stepId: "work",
        grants: ["tool:read_file", "tool:mail_send"],
        grantEffects: { "tool:read_file": "allow", "tool:mail_send": "allow" },
      },
    ],
    grantRequirements: [],
  };
}

const SAME_TENANT_SENDER = {
  senderCoordinates: [
    { coordType: "principal", id: "prn_colleague" },
    { coordType: "tenant", id: TENANT },
  ] satisfies MailAcceptCoordinate[],
};

// The creator deployed every deployment here, so it is their invoker.
const DEPLOYER_SENDER = {
  senderCoordinates: [
    { coordType: "principal", id: CREATOR },
    { coordType: "tenant", id: TENANT },
  ] satisfies MailAcceptCoordinate[],
};

const OTHER_TENANT_SENDER = {
  senderCoordinates: [
    { coordType: "principal", id: "prn_outsider" },
    { coordType: "tenant", id: "tnt_other" },
  ] satisfies MailAcceptCoordinate[],
};

describe.skipIf(!harnessDbEnvAvailable())(
  "mail.accept backfill migration (real DB)",
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
      await seedTenants(h.db, [{ id: TENANT }, { id: "tnt_other" }]);
      await seedPrincipal(h.db, {
        id: CREATOR,
        tenantId: TENANT,
        kind: "user",
        refId: "creator-user",
      });
      await seedPrincipal(h.db, {
        id: "prn_colleague",
        tenantId: TENANT,
        kind: "user",
        refId: "colleague-user",
      });
      await seedPrincipal(h.db, {
        id: "prn_outsider",
        tenantId: "tnt_other",
        kind: "user",
        refId: "outsider-user",
      });
      await seedAsset(h.db, {
        id: ASSET,
        tenantId: TENANT,
        kind: "workflow",
        name: ASSET,
        creatorPrincipalId: CREATOR,
      });
      await h.db.insert(workflowDefinition).values([
        {
          id: MAIL_DEFINITION,
          tenantId: TENANT,
          name: MAIL_DEFINITION,
          assetId: ASSET,
        },
        {
          id: MANUAL_DEFINITION,
          tenantId: TENANT,
          name: MANUAL_DEFINITION,
          assetId: ASSET,
        },
        {
          id: SENDING_DEFINITION,
          tenantId: TENANT,
          name: SENDING_DEFINITION,
          assetId: ASSET,
        },
      ]);
      await h.db.insert(workflowDefinitionVersion).values([
        {
          id: `wdv_${MAIL_DEFINITION}`,
          definitionId: MAIL_DEFINITION,
          version: "1",
          status: "active",
          approvedWireHash: "a".repeat(64),
          grantSnapshot: preGateMailSnapshot(),
        },
        {
          id: `wdv_${MANUAL_DEFINITION}`,
          definitionId: MANUAL_DEFINITION,
          version: "1",
          status: "active",
          approvedWireHash: "b".repeat(64),
          grantSnapshot: manualSnapshot(),
        },
        {
          id: `wdv_${SENDING_DEFINITION}`,
          definitionId: SENDING_DEFINITION,
          version: "1",
          status: "active",
          approvedWireHash: "c".repeat(64),
          grantSnapshot: sendingSnapshot(),
        },
      ]);
      await h.db.insert(workflowRun).values([
        {
          id: DEPLOYMENT,
          tenantId: TENANT,
          anchorRunId: DEPLOYMENT,
          definitionId: MAIL_DEFINITION,
          address: WORKFLOW_ADDRESS,
          status: "running",
          publicKey: "test-public-key",
        },
        {
          id: SENDING_DEPLOYMENT,
          tenantId: TENANT,
          anchorRunId: SENDING_DEPLOYMENT,
          definitionId: SENDING_DEFINITION,
          address: SENDING_ADDRESS,
          status: "deployed",
          publicKey: "test-public-key",
        },
        {
          id: MANUAL_DEPLOYMENT,
          tenantId: TENANT,
          anchorRunId: MANUAL_DEPLOYMENT,
          definitionId: MANUAL_DEFINITION,
          address: "run_manual@tenant.example",
          status: "deployed",
          publicKey: "test-public-key",
        },
      ]);
      for (const [anchorRunId, grantSnapshot] of [
        [DEPLOYMENT, preGateMailSnapshot()],
        [SENDING_DEPLOYMENT, sendingSnapshot()],
        [MANUAL_DEPLOYMENT, manualSnapshot()],
      ] as const) {
        await seedWorkflowRunLaunchSpec(h.db, {
          anchorRunId,
          sourceAuthorityPrincipalId: CREATOR,
          grantSnapshot,
        });
      }
    });

    async function runBackfill(): Promise<void> {
      for (const statement of BACKFILL_DATA_STATEMENTS) {
        await h.db.execute(sql.raw(statement));
      }
    }

    async function snapshotOf(anchorRunId: string): Promise<unknown> {
      const [row] = await h.db
        .select({ grantSnapshot: workflowRunLaunchSpec.grantSnapshot })
        .from(workflowRunLaunchSpec)
        .where(eq(workflowRunLaunchSpec.anchorRunId, anchorRunId));
      return row?.grantSnapshot;
    }

    async function setSnapshot(anchorRunId: string, grantSnapshot: unknown) {
      await h.db
        .update(workflowRunLaunchSpec)
        .set({ grantSnapshot })
        .where(eq(workflowRunLaunchSpec.anchorRunId, anchorRunId));
    }

    async function legacyRuleOf(runId: string): Promise<string | null> {
      const [row] = await h.db
        .select({ legacyMailAccept: workflowRun.legacyMailAccept })
        .from(workflowRun)
        .where(eq(workflowRun.id, runId));
      if (row === undefined) throw new Error(`no run ${runId}`);
      return row.legacyMailAccept;
    }

    // A run committed before the gate: a run principal with its runtime
    // grants and no mail.accept row.
    async function seedPreGateRun(args: {
      runId: string;
      principalId: string | null;
      status: "running" | "completed";
      definitionId?: string;
      anchorRunId?: string;
    }): Promise<void> {
      if (args.principalId !== null) {
        await seedPrincipal(h.db, {
          id: args.principalId,
          tenantId: TENANT,
          kind: "workflow",
          refId: args.runId,
        });
        await h.db.insert(grant).values({
          id: `grt_tool_${args.principalId}`,
          tenantId: TENANT,
          principalId: args.principalId,
          resource: "tool:read_file",
          action: "invoke",
          effect: "allow",
          origin: "creator",
        });
      }
      await h.db.insert(workflowRun).values({
        id: args.runId,
        tenantId: TENANT,
        anchorRunId: args.anchorRunId ?? DEPLOYMENT,
        definitionId: args.definitionId ?? MAIL_DEFINITION,
        status: args.status,
        principalId: args.principalId,
        publicKey: "test-public-key",
      });
    }

    async function tenantRowsFor(
      principalId: string,
    ): Promise<(typeof grant.$inferSelect)[]> {
      return h.db
        .select()
        .from(grant)
        .where(
          and(
            eq(grant.principalId, principalId),
            eq(grant.resource, TENANT_ROW),
          ),
        );
    }

    function materialize(
      runId: string,
      sender: typeof SAME_TENANT_SENDER,
      agentAddress: string = WORKFLOW_ADDRESS,
    ): ReturnType<ReturnType<typeof createMailTriggeredRunGrantsMaterializer>> {
      const materializer = createMailTriggeredRunGrantsMaterializer({
        runTokenStore: createRunTokenStore({
          db: h.db,
          cipher: createTestCredentialCipher(),
        }),
        db: h.db,
        principalKeyStore: createPrincipalKeyStore({
          db: h.db,
          cipher: createTestCredentialCipher(),
        }),
        grantStore: createGrantStore(h.db),
      });
      return materializer({ agentAddress, runId, ...sender });
    }

    // The write a new approval of the same definition makes: the snapshot the
    // probe derives from a projection with no `mailAccept`.
    async function approveAgain(definitionId: string, grantSnapshot: unknown) {
      await h.db
        .update(workflowDefinitionVersion)
        .set({ grantSnapshot })
        .where(eq(workflowDefinitionVersion.definitionId, definitionId));
    }

    test("leaves every frozen snapshot untouched", async () => {
      await runBackfill();

      expect(await snapshotOf(DEPLOYMENT)).toEqual(preGateMailSnapshot());
      expect(await snapshotOf(SENDING_DEPLOYMENT)).toEqual(sendingSnapshot());
      expect(await snapshotOf(MANUAL_DEPLOYMENT)).toEqual(manualSnapshot());
    });

    test("gives a live mail-trigger deployment the invoker and correspondent rule", async () => {
      await runBackfill();

      expect(await legacyRuleOf(DEPLOYMENT)).toBe("invoker-correspondent");
    });

    test("gives a live sending deployment only the correspondent rule", async () => {
      await runBackfill();

      expect(await legacyRuleOf(SENDING_DEPLOYMENT)).toBe("correspondent");
    });

    test("gives a mail-trigger deployment that also sends the mail-trigger rule", async () => {
      const withSend = preGateMailSnapshot();
      withSend.perStep[0]?.grants.push("tool:mail_send");
      await setSnapshot(DEPLOYMENT, withSend);

      await runBackfill();

      expect(await legacyRuleOf(DEPLOYMENT)).toBe("invoker-correspondent");
    });

    test("gives no rule to a deployment that cannot receive mail, a terminal deployment, or a child run", async () => {
      await h.db.insert(workflowRun).values({
        id: "run_mail_done",
        tenantId: TENANT,
        anchorRunId: "run_mail_done",
        definitionId: MAIL_DEFINITION,
        address: "run_mail_done@tenant.example",
        status: "completed",
      });
      await seedPreGateRun({
        runId: LIVE_RUN,
        principalId: LIVE_RUN_PRINCIPAL,
        status: "running",
      });

      await runBackfill();

      expect(await legacyRuleOf(MANUAL_DEPLOYMENT)).toBeNull();
      expect(await legacyRuleOf("run_mail_done")).toBeNull();
      expect(await legacyRuleOf(LIVE_RUN)).toBeNull();
    });

    test("classifies a deployment by its launch spec, not the shared version row", async () => {
      // A later approval rewrote the shared row with a snapshot that has a
      // mail trigger and sends mail, so each of the three data statements
      // would act on it if it read that row.
      const rewritten = preGateMailSnapshot();
      rewritten.perStep[0]?.grants.push("tool:mail_send");
      await approveAgain(MANUAL_DEFINITION, rewritten);
      await seedPreGateRun({
        runId: "<manual-run@tenant.example>",
        principalId: "prn_manual_run",
        status: "running",
        definitionId: MANUAL_DEFINITION,
        anchorRunId: MANUAL_DEPLOYMENT,
      });

      await runBackfill();

      expect(await legacyRuleOf(MANUAL_DEPLOYMENT)).toBeNull();
      expect(await tenantRowsFor("prn_manual_run")).toHaveLength(0);
    });

    test("leaves an anchor that already holds a rule as it is", async () => {
      await h.db
        .update(workflowRun)
        .set({ legacyMailAccept: "correspondent" })
        .where(eq(workflowRun.id, DEPLOYMENT));

      await runBackfill();

      expect(await legacyRuleOf(DEPLOYMENT)).toBe("correspondent");
    });

    test("gives a live run that only reads mail an accept row for its tenant and no anchor rule", async () => {
      const WAITING_DEFINITION = "wfd_waiting";
      const WAITING_DEPLOYMENT = "run_waiting";
      await h.db.insert(workflowDefinition).values({
        id: WAITING_DEFINITION,
        tenantId: TENANT,
        name: WAITING_DEFINITION,
        assetId: ASSET,
      });
      await h.db.insert(workflowRun).values({
        id: WAITING_DEPLOYMENT,
        tenantId: TENANT,
        anchorRunId: WAITING_DEPLOYMENT,
        definitionId: WAITING_DEFINITION,
        address: "run_waiting@tenant.example",
        status: "deployed",
        publicKey: "test-public-key",
      });
      await seedWorkflowRunLaunchSpec(h.db, {
        anchorRunId: WAITING_DEPLOYMENT,
        sourceAuthorityPrincipalId: CREATOR,
        grantSnapshot: {
          perStep: [
            {
              stepId: "work",
              grants: ["tool:mail_wait", "tool:mail_flag", "tool:mail_expunge"],
              grantEffects: {
                "tool:mail_wait": "allow",
                "tool:mail_flag": "allow",
                "tool:mail_expunge": "allow",
              },
            },
          ],
          grantRequirements: [],
        },
      });
      await seedPreGateRun({
        runId: LIVE_RUN,
        principalId: LIVE_RUN_PRINCIPAL,
        status: "running",
        definitionId: WAITING_DEFINITION,
        anchorRunId: WAITING_DEPLOYMENT,
      });

      await runBackfill();

      expect(await tenantRowsFor(LIVE_RUN_PRINCIPAL)).toHaveLength(1);
      expect(await legacyRuleOf(WAITING_DEPLOYMENT)).toBeNull();
    });

    test("gives a live run of a mail-trigger definition an accept row for its tenant", async () => {
      await seedPreGateRun({
        runId: LIVE_RUN,
        principalId: LIVE_RUN_PRINCIPAL,
        status: "running",
      });

      await runBackfill();

      const rows = await tenantRowsFor(LIVE_RUN_PRINCIPAL);
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({
        tenantId: TENANT,
        action: "accept",
        effect: "allow",
        origin: "creator",
        conditions: null,
        expiresAt: null,
        roleId: null,
      });
      expect(rows[0]?.id).toMatch(/^grt_[0-9a-f]{32}$/);
    });

    test("gives a live run of a sending definition an accept row for its tenant", async () => {
      await seedPreGateRun({
        runId: "<sending-run@tenant.example>",
        principalId: "prn_sending_run",
        status: "running",
        definitionId: SENDING_DEFINITION,
        anchorRunId: SENDING_DEPLOYMENT,
      });

      await runBackfill();

      expect(await tenantRowsFor("prn_sending_run")).toHaveLength(1);
    });

    test("leaves a terminal run, a run without a principal, and a run that cannot receive mail without an accept row", async () => {
      await seedPreGateRun({
        runId: "<mail-run-done@tenant.example>",
        principalId: "prn_done_run",
        status: "completed",
      });
      await seedPreGateRun({
        runId: "<mail-run-internal@tenant.example>",
        principalId: null,
        status: "running",
      });
      await seedPreGateRun({
        runId: "<manual-run@tenant.example>",
        principalId: "prn_manual_run",
        status: "running",
        definitionId: MANUAL_DEFINITION,
        anchorRunId: MANUAL_DEPLOYMENT,
      });

      await runBackfill();

      const acceptRows = await h.db
        .select()
        .from(grant)
        .where(sql`${grant.resource} like 'mail.accept:%'`);
      expect(acceptRows).toHaveLength(0);
    });

    test("re-applying the backfill changes nothing", async () => {
      await seedPreGateRun({
        runId: LIVE_RUN,
        principalId: LIVE_RUN_PRINCIPAL,
        status: "running",
      });

      await runBackfill();
      const rowsAfterFirst = await tenantRowsFor(LIVE_RUN_PRINCIPAL);
      expect(rowsAfterFirst).toHaveLength(1);

      await runBackfill();

      expect(await legacyRuleOf(DEPLOYMENT)).toBe("invoker-correspondent");
      expect(await legacyRuleOf(SENDING_DEPLOYMENT)).toBe("correspondent");
      expect(await snapshotOf(DEPLOYMENT)).toEqual(preGateMailSnapshot());
      const rowsAfterSecond = await tenantRowsFor(LIVE_RUN_PRINCIPAL);
      expect(rowsAfterSecond).toHaveLength(1);
      expect(rowsAfterSecond[0]?.id).toBe(rowsAfterFirst[0]?.id);
    });

    test("after the backfill a deployment that has not fired admits its first mail from its deployer", async () => {
      const firstRun = "<mail-run-first@tenant.example>";
      const before = await materialize(firstRun, DEPLOYER_SENDER);
      expect(before.outcome).toBe("rejected");

      await runBackfill();

      const outsider = await materialize(firstRun, OTHER_TENANT_SENDER);
      expect(outsider.outcome).toBe("rejected");
      const after = await materialize(firstRun, DEPLOYER_SENDER);
      expect(after.outcome).toBe("materialized");
    });

    test("after the backfill a live run admits a same-tenant sender and refuses another tenant", async () => {
      await seedPreGateRun({
        runId: LIVE_RUN,
        principalId: LIVE_RUN_PRINCIPAL,
        status: "running",
      });
      const before = await materialize(LIVE_RUN, SAME_TENANT_SENDER);
      expect(before.outcome).toBe("rejected");

      await runBackfill();

      const sameTenant = await materialize(LIVE_RUN, SAME_TENANT_SENDER);
      expect(sameTenant.outcome).toBe("materialized");
      const otherTenant = await materialize(LIVE_RUN, OTHER_TENANT_SENDER);
      expect(otherTenant.outcome).toBe("rejected");
      if (otherTenant.outcome === "rejected") {
        expect(otherTenant.code).toBe("mail_admission_denied");
      }
    });

    test("a new deployment of the same definition keeps the older deployment's rule and refuses by default", async () => {
      await runBackfill();

      // A new deployment of the unchanged definition: a new anchor, a new
      // approval that writes the shared snapshot again, and the new
      // deployment's own launch spec.
      await approveAgain(MAIL_DEFINITION, preGateMailSnapshot());
      await h.db.insert(workflowRun).values({
        id: "run_mail_new",
        tenantId: TENANT,
        anchorRunId: "run_mail_new",
        definitionId: MAIL_DEFINITION,
        address: "run_mail_new@tenant.example",
        status: "deployed",
        publicKey: "test-public-key",
      });
      await seedWorkflowRunLaunchSpec(h.db, {
        anchorRunId: "run_mail_new",
        sourceAuthorityPrincipalId: CREATOR,
        grantSnapshot: preGateMailSnapshot(),
      });

      const older = await materialize(
        "<mail-run-older@tenant.example>",
        DEPLOYER_SENDER,
      );
      expect(older.outcome).toBe("materialized");

      const newer = await materialize(
        "<mail-run-newer@tenant.example>",
        DEPLOYER_SENDER,
        "run_mail_new@tenant.example",
      );
      expect(newer.outcome).toBe("rejected");
      if (newer.outcome === "rejected") {
        expect(newer.code).toBe("mail_admission_denied");
      }
      expect(await legacyRuleOf("run_mail_new")).toBeNull();
    });

    describe("correspondent grants of a sending deployment", () => {
      const SENDER_PRINCIPAL = "prn_sending_deployment";
      const RECIPIENT_RUN_ID = "run_recipient";
      const RECIPIENT_ADDRESS = "run_recipient@tenant.example";
      const RECIPIENT_PRINCIPAL = "prn_recipient";

      beforeEach(async () => {
        // The sending deployment has fired, so its anchor holds a principal.
        await seedPrincipal(h.db, {
          id: SENDER_PRINCIPAL,
          tenantId: TENANT,
          kind: "workflow",
          refId: SENDING_DEPLOYMENT,
        });
        await h.db
          .update(workflowRun)
          .set({ principalId: SENDER_PRINCIPAL, status: "running" })
          .where(eq(workflowRun.id, SENDING_DEPLOYMENT));
        // A recipient that resolves to a durable principal.
        await seedPrincipal(h.db, {
          id: RECIPIENT_PRINCIPAL,
          tenantId: TENANT,
          kind: "workflow",
          refId: RECIPIENT_RUN_ID,
        });
        await h.db.insert(workflowRun).values({
          id: RECIPIENT_RUN_ID,
          tenantId: TENANT,
          anchorRunId: RECIPIENT_RUN_ID,
          definitionId: MANUAL_DEFINITION,
          address: RECIPIENT_ADDRESS,
          status: "running",
          principalId: RECIPIENT_PRINCIPAL,
          publicKey: "ab".repeat(32),
        });
      });

      async function mintAndCount(): Promise<number> {
        await createCorrespondentGrantMinter({
          db: h.db,
          principalKeyStore: createPrincipalKeyStore({
            db: h.db,
            cipher: createTestCredentialCipher(),
          }),
        })({
          senderAddress: SENDING_ADDRESS,
          recipientAddresses: [RECIPIENT_ADDRESS],
        });
        const rows = await h.db
          .select()
          .from(grant)
          .where(
            and(
              eq(grant.principalId, SENDER_PRINCIPAL),
              eq(
                grant.resource,
                `mail.accept:principal:${RECIPIENT_PRINCIPAL}`,
              ),
            ),
          );
        return rows.length;
      }

      test("mints nothing before the backfill", async () => {
        expect(await mintAndCount()).toBe(0);
      });

      test("mints after the backfill, also after a new approval of the definition", async () => {
        await runBackfill();
        await approveAgain(SENDING_DEFINITION, sendingSnapshot());

        expect(await mintAndCount()).toBe(1);
      });
    });
  },
);
