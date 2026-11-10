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

import { eq, sql } from "drizzle-orm";

import { workflowDefinition, workflowRunDispatch } from "@intx/db/schema";
import { deriveDeliveryKey } from "@intx/types/message-id";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { REPO_ROOT } from "@intx/test-harness/env";
import { seedTenants, seedWorkflowRun } from "@intx/test-harness/seed";

const MIGRATION_STATEMENTS = readFileSync(
  path.join(
    REPO_ROOT,
    "packages/db/migrations/0125_workflow_run_dispatch_delivery_key.sql",
  ),
  "utf-8",
)
  .split("--> statement-breakpoint")
  .map((statement) => statement.trim());

const BACKFILL = MIGRATION_STATEMENTS.find((statement) =>
  statement.includes('UPDATE "workflow_run_dispatch"'),
);

const TENANT_ID = "tnt-backfill";
const DEFINITION_ID = "wfd-backfill";
const ANCHOR_RUN_ID = "dep-backfill";
const SENDER = "run_sender@tnt-backfill.localhost";

describe.skipIf(!harnessDbEnvAvailable())(
  "workflow_run_dispatch delivery key migration (real DB)",
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
      await seedTenants(h.db, [{ id: TENANT_ID }]);
      await h.db.insert(workflowDefinition).values({
        id: DEFINITION_ID,
        tenantId: TENANT_ID,
        name: DEFINITION_ID,
      });
      await seedWorkflowRun(h.db, {
        id: ANCHOR_RUN_ID,
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        definitionId: DEFINITION_ID,
      });
    });

    async function insertRow(row: {
      id: string;
      messageId: string;
      kind: "mail" | "signal";
      senderAddress: string | null;
      status: "pending" | "failed";
    }): Promise<void> {
      await h.db.insert(workflowRunDispatch).values({
        ...row,
        anchorRunId: ANCHOR_RUN_ID,
        deliveryKey: `before-backfill-${row.id}`,
        rawMessage: new Uint8Array([1]),
        stepGrants: [],
        nextAttemptAt: row.status === "pending" ? new Date() : null,
      });
    }

    async function keyOf(id: string): Promise<string | undefined> {
      const [row] = await h.db
        .select({ deliveryKey: workflowRunDispatch.deliveryKey })
        .from(workflowRunDispatch)
        .where(eq(workflowRunDispatch.id, id));
      return row?.deliveryKey;
    }

    test("derives each mail row's key exactly as deriveDeliveryKey does", async () => {
      if (BACKFILL === undefined) throw new Error("backfill statement missing");
      await insertRow({
        id: "mail-ascii",
        messageId: "<ascii@example.test>",
        kind: "mail",
        senderAddress: SENDER,
        status: "pending",
      });
      await insertRow({
        id: "mail-utf8",
        messageId: "<café@example.test>",
        kind: "mail",
        senderAddress: SENDER,
        status: "pending",
      });

      await h.db.execute(sql.raw(BACKFILL));

      expect(await keyOf("mail-ascii")).toBe(
        await deriveDeliveryKey(SENDER, "<ascii@example.test>"),
      );
      expect(await keyOf("mail-utf8")).toBe(
        await deriveDeliveryKey(SENDER, "<café@example.test>"),
      );
    });

    test("keeps the id of a signal row and of a mail row with no sender", async () => {
      if (BACKFILL === undefined) throw new Error("backfill statement missing");
      await insertRow({
        id: "signal",
        messageId: "sig-1",
        kind: "signal",
        senderAddress: null,
        status: "pending",
      });
      await insertRow({
        id: "unsent",
        messageId: "<failed@example.test>",
        kind: "mail",
        senderAddress: null,
        status: "failed",
      });

      await h.db.execute(sql.raw(BACKFILL));

      expect(await keyOf("signal")).toBe("sig-1");
      expect(await keyOf("unsent")).toBe("<failed@example.test>");
    });

    test("leaves the key required and unique per anchor, with no Message-ID index", async () => {
      const [column] = [
        ...(await h.db.execute(
          sql`SELECT is_nullable FROM information_schema.columns WHERE table_schema = ${h.schema} AND table_name = 'workflow_run_dispatch' AND column_name = 'delivery_key'`,
        )),
      ];
      expect(column?.["is_nullable"]).toBe("NO");

      const indexes = [
        ...(await h.db.execute(
          sql`SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = ${h.schema} AND tablename = 'workflow_run_dispatch'`,
        )),
      ];
      const names = indexes.map((index) => index["indexname"]);
      expect(names).toContain("workflow_run_dispatch_anchor_delivery_key_idx");
      expect(names).not.toContain("workflow_run_dispatch_anchor_message_idx");
      const keyIndex = indexes.find(
        (index) =>
          index["indexname"] ===
          "workflow_run_dispatch_anchor_delivery_key_idx",
      );
      expect(String(keyIndex?.["indexdef"])).toMatch(
        /UNIQUE INDEX .*\(anchor_run_id, delivery_key\)/,
      );
    });
  },
);
