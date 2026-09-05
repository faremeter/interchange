import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { and, eq } from "drizzle-orm";

import { createPrincipalStore } from "@intx/db";
import { principal } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedTenants } from "@intx/test-harness/seed";

const TENANT = "tnt_ps";

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

    test("createIfAbsent is idempotent on the natural key", async () => {
      const store = createPrincipalStore(h.db);
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

      // A second reservation of the same (tenantId, kind, refId) -- even with a
      // different surrogate id -- must not insert a new row.
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
    });

    test("create fails loudly on a natural-key conflict", async () => {
      const store = createPrincipalStore(h.db);
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
      const store = createPrincipalStore(h.db);
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
  },
);
