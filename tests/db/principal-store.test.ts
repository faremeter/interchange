import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { and, eq } from "drizzle-orm";

import {
  createPrincipalStore,
  createPrincipalKeyStore,
  deactivateRunPrincipals,
  insertRunPrincipal,
  type PrincipalKeyStore,
} from "@intx/db";
import { principal, principalKey, runToken } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import {
  seedPrincipal,
  seedRunToken,
  seedTenants,
} from "@intx/test-harness/seed";

const TENANT = "tnt_ps";
const OTHER_TENANT = "tnt_ps_other";
const cipher = createTestCredentialCipher();

describe.skipIf(!harnessDbEnvAvailable())(
  "createPrincipalStore (real DB)",
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

    function newStore() {
      return createPrincipalStore(
        h.db,
        createPrincipalKeyStore({ db: h.db, cipher }),
      );
    }

    async function activeKeyCount(principalId: string): Promise<number> {
      const rows = await h.db
        .select()
        .from(principalKey)
        .where(
          and(
            eq(principalKey.principalId, principalId),
            eq(principalKey.status, "active"),
          ),
        );
      return rows.length;
    }

    test("insertRunPrincipal inserts a keyless run principal once per run", async () => {
      const createdAt = new Date("2026-01-02T03:04:05Z");
      const row = {
        id: "prn_run_first",
        tenantId: TENANT,
        runId: "run_shared",
        createdAt,
      };

      expect(await insertRunPrincipal(h.db, row)).toBe(true);
      // A second insert for the same run -- even with a different surrogate
      // id -- inserts no row.
      expect(
        await insertRunPrincipal(h.db, { ...row, id: "prn_run_second" }),
      ).toBe(false);

      const rows = await h.db
        .select()
        .from(principal)
        .where(
          and(
            eq(principal.tenantId, TENANT),
            eq(principal.kind, "workflow"),
            eq(principal.refId, "run_shared"),
          ),
        );
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({
        id: "prn_run_first",
        status: "active",
        createdAt,
      });
      const keys = await h.db
        .select()
        .from(principalKey)
        .where(eq(principalKey.principalId, "prn_run_first"));
      expect(keys).toHaveLength(0);
    });

    test("create mints an active signing key for the new principal", async () => {
      const store = newStore();
      const now = new Date();

      const created = await store.create({
        id: "prn_keyed",
        tenantId: TENANT,
        kind: "user",
        refId: "usr_keyed",
        status: "active",
        createdAt: now,
        updatedAt: now,
      });
      expect(created.id).toBe("prn_keyed");
      expect(await activeKeyCount("prn_keyed")).toBe(1);
    });

    test("create fails loudly on a natural-key conflict", async () => {
      const store = newStore();
      const now = new Date();
      const row = {
        id: "prn_a",
        tenantId: TENANT,
        kind: "user" as const,
        refId: "usr_dup",
        status: "active" as const,
        createdAt: now,
        updatedAt: now,
      };

      await store.create(row);
      await expect(store.create({ ...row, id: "prn_b" })).rejects.toThrow();
    });

    test("create preserves the caller-supplied status", async () => {
      const store = newStore();
      const now = new Date();

      const created = await store.create({
        id: "prn_invited",
        tenantId: TENANT,
        kind: "user",
        refId: "usr_invited",
        status: "invited",
        createdAt: now,
        updatedAt: now,
      });
      expect(created.status).toBe("invited");

      const [persisted] = await h.db
        .select()
        .from(principal)
        .where(eq(principal.id, "prn_invited"));
      expect(persisted?.status).toBe("invited");
    });

    async function principalExists(id: string): Promise<boolean> {
      const rows = await h.db
        .select()
        .from(principal)
        .where(eq(principal.id, id));
      return rows.length > 0;
    }

    test("a failed key mint rolls back the principal insert", async () => {
      // The key store throws on mint, so the whole transaction -- the principal
      // insert included -- must roll back and leave no principal row. This pins
      // the store's invariant: a principal it creates never exists without a
      // key.
      const throwingKeyStore: PrincipalKeyStore = {
        generate: async () => {
          throw new Error("mint boom");
        },
        sign: async () => new Uint8Array(64),
        getPublicKey: async () => "pky_unused",
        recordPublicKey: async () => undefined,
        retireActive: async () => undefined,
      };
      const store = createPrincipalStore(h.db, throwingKeyStore);
      const now = new Date();

      await expect(
        store.create({
          id: "prn_create_rollback",
          tenantId: TENANT,
          kind: "user",
          refId: "usr_create_rollback",
          status: "active",
          createdAt: now,
          updatedAt: now,
        }),
      ).rejects.toThrow(/mint boom/);
      expect(await principalExists("prn_create_rollback")).toBe(false);
    });
  },
);

describe.skipIf(!harnessDbEnvAvailable())(
  "deactivateRunPrincipals (real DB)",
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
      await seedTenants(h.db, [{ id: TENANT }, { id: OTHER_TENANT }]);
    });

    async function statusOf(principalId: string): Promise<string | undefined> {
      const [row] = await h.db
        .select({ status: principal.status })
        .from(principal)
        .where(eq(principal.id, principalId));
      return row?.status;
    }

    async function tokenCount(principalId: string): Promise<number> {
      const rows = await h.db
        .select({ id: runToken.id })
        .from(runToken)
        .where(eq(runToken.principalId, principalId));
      return rows.length;
    }

    test("deactivates the run principals of the given runs and deletes their tokens", async () => {
      await seedPrincipal(h.db, {
        id: "prn_run_a",
        tenantId: TENANT,
        kind: "workflow",
        refId: "run_a",
      });
      await seedRunToken(h.db, { id: "rtk_run_a", principalId: "prn_run_a" });

      // `run_child` has no principal of its own and is skipped.
      await deactivateRunPrincipals(
        h.db,
        TENANT,
        ["run_a", "run_child"],
        new Date(),
      );

      expect(await statusOf("prn_run_a")).toBe("deactivated");
      expect(await tokenCount("prn_run_a")).toBe(0);
    });

    test("leaves other runs, other tenants, and other kinds alone", async () => {
      await seedPrincipal(h.db, {
        id: "prn_run_b",
        tenantId: TENANT,
        kind: "workflow",
        refId: "run_b",
      });
      await seedPrincipal(h.db, {
        id: "prn_other_tenant_a",
        tenantId: OTHER_TENANT,
        kind: "workflow",
        refId: "run_a",
      });
      await seedPrincipal(h.db, {
        id: "prn_user_a",
        tenantId: TENANT,
        kind: "user",
        refId: "run_a",
      });
      await seedRunToken(h.db, { id: "rtk_run_b", principalId: "prn_run_b" });

      await deactivateRunPrincipals(h.db, TENANT, ["run_a"], new Date());

      expect(await statusOf("prn_run_b")).toBe("active");
      expect(await tokenCount("prn_run_b")).toBe(1);
      expect(await statusOf("prn_other_tenant_a")).toBe("active");
      expect(await statusOf("prn_user_a")).toBe("active");
    });
  },
);
