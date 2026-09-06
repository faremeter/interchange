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
  type PrincipalKeyStore,
} from "@intx/db";
import { principal, principalKey } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import { seedTenants } from "@intx/test-harness/seed";

const TENANT = "tnt_ps";
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

    test("createIfAbsent inserts, mints a key, and is idempotent", async () => {
      const store = newStore();
      const now = new Date();
      const row = {
        id: "prn_first",
        tenantId: TENANT,
        kind: "user" as const,
        refId: "usr_shared",
        status: "active" as const,
        createdAt: now,
        updatedAt: now,
      };

      const first = await store.createIfAbsent(row);
      expect(first?.id).toBe("prn_first");
      expect(await activeKeyCount("prn_first")).toBe(1);

      // A second reservation of the same (tenantId, kind, refId) -- even with a
      // different surrogate id -- inserts no row and mints no second key: the
      // winning transaction already minted the one active key.
      const second = await store.createIfAbsent({ ...row, id: "prn_second" });
      expect(second).toBeNull();

      const rows = await h.db
        .select()
        .from(principal)
        .where(
          and(
            eq(principal.tenantId, TENANT),
            eq(principal.kind, "user"),
            eq(principal.refId, "usr_shared"),
          ),
        );
      expect(rows).toHaveLength(1);
      expect(rows[0]?.id).toBe("prn_first");
      expect(await activeKeyCount("prn_first")).toBe(1);
      expect(await activeKeyCount("prn_second")).toBe(0);
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
      // the feature's headline invariant: a principal never exists without a key.
      const throwingKeyStore: PrincipalKeyStore = {
        generate: async () => {
          throw new Error("mint boom");
        },
        sign: async () => new Uint8Array(64),
        getPublicKey: async () => "pky_unused",
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

      await expect(
        store.createIfAbsent({
          id: "prn_ifabsent_rollback",
          tenantId: TENANT,
          kind: "user",
          refId: "usr_ifabsent_rollback",
          status: "active",
          createdAt: now,
          updatedAt: now,
        }),
      ).rejects.toThrow(/mint boom/);
      expect(await principalExists("prn_ifabsent_rollback")).toBe(false);
    });
  },
);
