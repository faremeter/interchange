// Verifies the `credential:{id}` / `use` grant backfill migration against a
// real, schema-isolated database.
//
// The migration runs once during `createTestDb` when the tables are empty, so
// its effect on seeded data is exercised by re-executing the exact SQL the
// migration ships. Re-running the same statement is also the idempotency
// scenario: a second and third application must insert nothing and must not
// error.

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

import { grant } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { REPO_ROOT } from "@intx/test-harness/env";
import {
  seedCredential,
  seedPrincipal,
  seedProvider,
  seedTenants,
} from "@intx/test-harness/seed";

const BACKFILL_SQL = readFileSync(
  path.join(
    REPO_ROOT,
    "packages/db/migrations/0037_credential_use_backfill.sql",
  ),
  "utf-8",
);

describe.skipIf(!harnessDbEnvAvailable())(
  "credential-use backfill migration (real DB)",
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
    });

    async function runBackfill(): Promise<void> {
      await h.db.execute(sql.raw(BACKFILL_SQL));
    }

    async function useGrantsFor(
      principalId: string,
      resource: string,
    ): Promise<(typeof grant.$inferSelect)[]> {
      return h.db
        .select()
        .from(grant)
        .where(
          and(
            eq(grant.principalId, principalId),
            eq(grant.resource, resource),
            eq(grant.action, "use"),
          ),
        );
    }

    test("grants credential use to each personal credential owner", async () => {
      await seedTenants(h.db, [{ id: "tnt_leaf" }]);
      await seedProvider(h.db, {
        id: "prv_1",
        tenantId: "tnt_leaf",
        name: "github",
      });
      await seedPrincipal(h.db, { id: "prn_owner", tenantId: "tnt_leaf" });
      await seedCredential(h.db, {
        id: "crd_personal",
        tenantId: "tnt_leaf",
        providerId: "prv_1",
        name: "personal-cred",
        principalId: "prn_owner",
      });

      await runBackfill();

      const grants = await useGrantsFor("prn_owner", "credential:crd_personal");
      expect(grants).toHaveLength(1);
      const [g] = grants;
      expect(g?.tenantId).toBe("tnt_leaf");
      expect(g?.effect).toBe("allow");
      expect(g?.origin).toBe("creator");
      expect(g?.expiresAt).toBeNull();
      expect(g?.roleId).toBeNull();
      expect(g?.id).toMatch(/^grt_[0-9a-f]{32}$/);
    });

    test("does not grant use for organizational credentials", async () => {
      await seedTenants(h.db, [{ id: "tnt_leaf" }]);
      await seedProvider(h.db, {
        id: "prv_1",
        tenantId: "tnt_leaf",
        name: "github",
      });
      await seedCredential(h.db, {
        id: "crd_org",
        tenantId: "tnt_leaf",
        providerId: "prv_1",
        name: "org-cred",
        principalId: null,
      });

      await runBackfill();

      const orgResourceGrants = await h.db
        .select()
        .from(grant)
        .where(
          and(
            eq(grant.resource, "credential:crd_org"),
            eq(grant.action, "use"),
          ),
        );
      expect(orgResourceGrants).toHaveLength(0);
    });

    test("re-applying the backfill is a no-op", async () => {
      await seedTenants(h.db, [{ id: "tnt_leaf" }]);
      await seedProvider(h.db, {
        id: "prv_1",
        tenantId: "tnt_leaf",
        name: "github",
      });
      await seedPrincipal(h.db, { id: "prn_owner", tenantId: "tnt_leaf" });
      await seedCredential(h.db, {
        id: "crd_personal",
        tenantId: "tnt_leaf",
        providerId: "prv_1",
        name: "personal-cred",
        principalId: "prn_owner",
      });

      await runBackfill();
      const first = await useGrantsFor("prn_owner", "credential:crd_personal");
      expect(first).toHaveLength(1);
      const originalId = first[0]?.id;

      await runBackfill();
      await runBackfill();

      const after = await useGrantsFor("prn_owner", "credential:crd_personal");
      expect(after).toHaveLength(1);
      expect(after[0]?.id).toBe(originalId);
    });

    test("preserves a pre-existing use grant and adds none", async () => {
      await seedTenants(h.db, [{ id: "tnt_leaf" }]);
      await seedProvider(h.db, {
        id: "prv_1",
        tenantId: "tnt_leaf",
        name: "github",
      });
      await seedPrincipal(h.db, { id: "prn_owner", tenantId: "tnt_leaf" });
      await seedCredential(h.db, {
        id: "crd_personal",
        tenantId: "tnt_leaf",
        providerId: "prv_1",
        name: "personal-cred",
        principalId: "prn_owner",
      });
      await h.db.insert(grant).values({
        id: "grt_preexisting",
        tenantId: "tnt_leaf",
        principalId: "prn_owner",
        resource: "credential:crd_personal",
        action: "use",
        effect: "allow",
        origin: "creator",
      });

      await runBackfill();

      const grants = await useGrantsFor("prn_owner", "credential:crd_personal");
      expect(grants).toHaveLength(1);
      expect(grants[0]?.id).toBe("grt_preexisting");
    });
  },
);
