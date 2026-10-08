import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { eq } from "drizzle-orm";

import { pgErrorCode } from "@intx/db";
import { credential, principal, tenant } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedCredential,
  seedPrincipal,
  seedProvider,
  seedTenants,
} from "@intx/test-harness/seed";

const TENANT_ID = "tnt_cred_owner";
const PRINCIPAL_ID = "prn_owner";
const PROVIDER_ID = "prv_cred_owner";
const CREDENTIAL_ID = "crd_personal";

describe.skipIf(!harnessDbEnvAvailable())("credential principal delete", () => {
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
    await seedPrincipal(h.db, {
      id: PRINCIPAL_ID,
      tenantId: TENANT_ID,
    });
    await seedProvider(h.db, {
      id: PROVIDER_ID,
      tenantId: TENANT_ID,
      name: "openai",
    });
  });

  test("refuses to delete a principal who owns a credential", async () => {
    await seedCredential(h.db, {
      id: CREDENTIAL_ID,
      tenantId: TENANT_ID,
      providerId: PROVIDER_ID,
      name: "personal",
      principalId: PRINCIPAL_ID,
    });

    let code: string | undefined;
    try {
      await h.db.delete(principal).where(eq(principal.id, PRINCIPAL_ID));
    } catch (err) {
      code = pgErrorCode(err);
    }
    if (code === undefined) {
      throw new Error("expected a SQLSTATE on the caught error");
    }
    // Postgres 18 reports ON DELETE RESTRICT as 23001. Postgres 15, which
    // CI runs, reports it as 23503. Both are the referenced-row violation.
    expect(["23001", "23503"]).toContain(code);

    const [credentialRow] = await h.db
      .select()
      .from(credential)
      .where(eq(credential.id, CREDENTIAL_ID));
    expect(credentialRow?.principalId).toBe(PRINCIPAL_ID);

    const [principalRow] = await h.db
      .select()
      .from(principal)
      .where(eq(principal.id, PRINCIPAL_ID));
    expect(principalRow?.id).toBe(PRINCIPAL_ID);
  });

  test("removes a principal who owns no credential", async () => {
    await seedCredential(h.db, {
      id: "crd_org",
      tenantId: TENANT_ID,
      providerId: PROVIDER_ID,
      name: "org",
      principalId: null,
    });

    await h.db.delete(principal).where(eq(principal.id, PRINCIPAL_ID));

    const principals = await h.db
      .select()
      .from(principal)
      .where(eq(principal.id, PRINCIPAL_ID));
    expect(principals).toHaveLength(0);

    const [orgCredential] = await h.db
      .select()
      .from(credential)
      .where(eq(credential.id, "crd_org"));
    expect(orgCredential?.principalId).toBeNull();
  });

  test("deleting the tenant removes the principal and their credential", async () => {
    await seedCredential(h.db, {
      id: CREDENTIAL_ID,
      tenantId: TENANT_ID,
      providerId: PROVIDER_ID,
      name: "personal",
      principalId: PRINCIPAL_ID,
    });

    await h.db.delete(tenant).where(eq(tenant.id, TENANT_ID));

    const tenants = await h.db.select().from(tenant);
    const principals = await h.db.select().from(principal);
    const credentials = await h.db.select().from(credential);
    expect(tenants).toHaveLength(0);
    expect(principals).toHaveLength(0);
    expect(credentials).toHaveLength(0);
  });
});
