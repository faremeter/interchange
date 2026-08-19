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
import { sql } from "drizzle-orm";

import { credential, grant } from "@intx/db/schema";
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

// Exercise the shipped cleanup migration against the real migrated schema.
// The migration already ran on the empty schema at creation; re-executing its
// own SQL after seeding is legitimate because the DELETE is idempotent.
const MIGRATION_SQL = readFileSync(
  path.join(
    REPO_ROOT,
    "packages/db/migrations/0084_delete_orphaned_credential_grants.sql",
  ),
  "utf8",
);

const TENANT_ID = "tnt_cleanup";
const PRINCIPAL_ID = "prn_owner";
const PROVIDER_ID = "prv_test";
const LIVE_CREDENTIAL_ID = "crd_live";

let h: TestDb;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  h = await createTestDb();
});

afterAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  await h.close();
});

beforeEach(async () => {
  if (!harnessDbEnvAvailable()) return;
  await h.reset();
});

function grantRow(id: string, resource: string, action: string) {
  return {
    id,
    tenantId: TENANT_ID,
    principalId: PRINCIPAL_ID,
    resource,
    action,
    effect: "allow" as const,
    origin: "creator" as const,
  };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "0082 orphaned credential grant cleanup",
  () => {
    test("removes only grants naming a credential that no longer exists", async () => {
      await seedTenants(h.db, [{ id: TENANT_ID }]);
      await seedPrincipal(h.db, {
        id: PRINCIPAL_ID,
        tenantId: TENANT_ID,
        refId: "usr_owner",
      });
      await seedProvider(h.db, {
        id: PROVIDER_ID,
        tenantId: TENANT_ID,
        name: "openai",
      });
      await seedCredential(h.db, {
        id: LIVE_CREDENTIAL_ID,
        tenantId: TENANT_ID,
        providerId: PROVIDER_ID,
        principalId: PRINCIPAL_ID,
        name: "live",
        type: "api_key",
        secret: "sk",
      });

      await h.db.insert(grant).values([
        // Orphan: no credential has the id `crd_dead` -> removed.
        grantRow("grn_orphan", "credential:crd_dead", "use"),
        // Names a live credential -> survives.
        grantRow("grn_live", `credential:${LIVE_CREDENTIAL_ID}`, "use"),
        // Coarse wildcard role resource -> survives (guards the denylist).
        grantRow("grn_wildcard", "credential:*", "use"),
        // Unrelated resource sharing no prefix -> survives (guards LIKE).
        grantRow("grn_unrelated", "workflow-run:*", "read"),
      ]);

      await h.db.execute(sql.raw(MIGRATION_SQL));

      const remaining = await h.db
        .select({ id: grant.id })
        .from(grant)
        .orderBy(grant.id);
      const remainingIds = remaining.map((r) => r.id);

      expect(remainingIds).toEqual([
        "grn_live",
        "grn_unrelated",
        "grn_wildcard",
      ]);
    });

    test("is a no-op when there are no orphaned grants", async () => {
      await seedTenants(h.db, [{ id: TENANT_ID }]);
      await seedPrincipal(h.db, {
        id: PRINCIPAL_ID,
        tenantId: TENANT_ID,
        refId: "usr_owner",
      });
      await seedProvider(h.db, {
        id: PROVIDER_ID,
        tenantId: TENANT_ID,
        name: "openai",
      });
      await seedCredential(h.db, {
        id: LIVE_CREDENTIAL_ID,
        tenantId: TENANT_ID,
        providerId: PROVIDER_ID,
        principalId: PRINCIPAL_ID,
        name: "live",
        type: "api_key",
        secret: "sk",
      });
      await h.db
        .insert(grant)
        .values(
          grantRow("grn_live", `credential:${LIVE_CREDENTIAL_ID}`, "use"),
        );

      await h.db.execute(sql.raw(MIGRATION_SQL));
      await h.db.execute(sql.raw(MIGRATION_SQL));

      const remaining = await h.db.select({ id: grant.id }).from(grant);
      expect(remaining.map((r) => r.id)).toEqual(["grn_live"]);

      // The live credential itself is untouched by the grant cleanup.
      const creds = await h.db.select({ id: credential.id }).from(credential);
      expect(creds.map((r) => r.id)).toEqual([LIVE_CREDENTIAL_ID]);
    });
  },
);
