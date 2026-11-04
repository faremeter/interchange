// Migration 0121 moves each deployment's sidecar key onto its run principal
// once, against real data, so this replays it over seeded rows.

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

import { createPrincipalKeyStore, recordPublicKey } from "@intx/db";
import { principalKey, workflowRun } from "@intx/db/schema";
import { createNoopCredentialCipher } from "@intx/crypto";
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
    "../../packages/db/migrations/0121_run_principal_sidecar_keys.sql",
  ),
  "utf8",
);
const TENANT = "tnt-migration-0121";
const KEY_A = "aa".repeat(32);
const KEY_B = "bb".repeat(32);

describe.skipIf(!harnessDbEnvAvailable())(
  "migration 0121 run principal sidecar keys",
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

    // An anchor run and the run principal deploy created, with the key the
    // deploy ack recorded only on the anchor, as before the migration.
    async function seedAnchor(
      runId: string,
      publicKey: string | null,
      status: "deployed" | "running" | "failed" = "running",
    ): Promise<string> {
      await seedWorkflowRun(h.db, {
        id: runId,
        anchorRunId: runId,
        tenantId: TENANT,
        status,
      });
      await h.db
        .update(workflowRun)
        .set({ publicKey })
        .where(eq(workflowRun.id, runId));
      const principalId = `prn_${runId}`;
      await seedPrincipal(h.db, {
        id: principalId,
        tenantId: TENANT,
        kind: "workflow",
        refId: runId,
        status: "active",
      });
      return principalId;
    }

    async function keysOf(principalId: string) {
      return h.db
        .select({
          publicKey: principalKey.publicKey,
          privateKey: principalKey.privateKey,
          status: principalKey.status,
        })
        .from(principalKey)
        .where(eq(principalKey.principalId, principalId))
        .orderBy(asc(principalKey.createdAt));
    }

    // The query builder returns a thenable; adopt it so a rejection reaches
    // the assertion.
    function replay(): Promise<unknown> {
      return Promise.resolve(h.db.execute(sql.raw(MIGRATION_SQL)));
    }

    function hubKeys() {
      return createPrincipalKeyStore({
        db: h.db,
        cipher: createNoopCredentialCipher(),
      });
    }

    test("replaces a run principal's hub key with its deployment's sidecar key", async () => {
      const fired = await seedAnchor("run_fired", KEY_A);
      const ended = await seedAnchor("run_ended", KEY_B, "failed");
      const unacked = await seedAnchor("run_unacked", null, "deployed");
      // A run principal that fired before deploy created it got a hub key.
      await hubKeys().generate(fired);
      // A definition-level workflow principal keeps its key.
      await seedPrincipal(h.db, {
        id: "prn_definition",
        tenantId: TENANT,
        kind: "workflow",
        refId: "wfd_definition",
        status: "active",
      });
      const definitionKey = await hubKeys().generate("prn_definition");

      await h.db.execute(sql.raw(MIGRATION_SQL));
      // A second replay changes nothing.
      await h.db.execute(sql.raw(MIGRATION_SQL));

      expect(await keysOf(fired)).toEqual([
        { publicKey: KEY_A, privateKey: null, status: "active" },
      ]);
      expect(await keysOf(ended)).toEqual([
        { publicKey: KEY_B, privateKey: null, status: "active" },
      ]);
      expect(await keysOf(unacked)).toEqual([]);
      expect(await keysOf("prn_definition")).toEqual([
        expect.objectContaining({ publicKey: definitionKey, status: "active" }),
      ]);
    });

    test("refuses a key that is not 32 bytes of lowercase hex", async () => {
      await seedAnchor("run_malformed", "not-a-key");

      await expect(replay()).rejects.toThrow(/not 32 bytes of lowercase hex/);
    });

    test("refuses two deployments that hold the same key", async () => {
      await seedAnchor("run_first", KEY_A);
      await seedAnchor("run_second", KEY_A);

      await expect(replay()).rejects.toThrow(/hold the same public key/);
    });

    test("refuses a deployment key another principal already holds", async () => {
      await seedAnchor("run_keyed", KEY_A);
      await seedPrincipal(h.db, {
        id: "prn_other",
        tenantId: TENANT,
        kind: "user",
        refId: "usr_other",
        status: "active",
      });
      await recordPublicKey(h.db, "prn_other", KEY_A);

      await expect(replay()).rejects.toThrow(
        /already held by another principal/,
      );
    });

    test("refuses when a run principal already holds a different key", async () => {
      const principalId = await seedAnchor("run_keyed", KEY_A);
      await recordPublicKey(h.db, principalId, KEY_B);

      await expect(replay()).rejects.toThrow(
        /was not copied to its run principal/,
      );
    });

    test("refuses a keyed deployment with no run principal", async () => {
      await seedWorkflowRun(h.db, {
        id: "run_orphan",
        anchorRunId: "run_orphan",
        tenantId: TENANT,
      });
      await h.db
        .update(workflowRun)
        .set({ publicKey: KEY_A })
        .where(eq(workflowRun.id, "run_orphan"));

      await expect(replay()).rejects.toThrow(/has no run principal/);
    });

    test("refuses a key on a run that is not a deployment anchor", async () => {
      await seedAnchor("run_parent", null);
      await seedWorkflowRun(h.db, {
        id: "run_child",
        anchorRunId: "run_parent",
        tenantId: TENANT,
      });
      await h.db
        .update(workflowRun)
        .set({ publicKey: KEY_A })
        .where(eq(workflowRun.id, "run_child"));

      await expect(replay()).rejects.toThrow(/not a deployment anchor/);
    });
  },
);
