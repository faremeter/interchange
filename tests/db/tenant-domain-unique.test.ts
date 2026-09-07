import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";

describe.skipIf(!harnessDbEnvAvailable())("tenant domain uniqueness", () => {
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

  async function insertTenant(id: string, domain: string): Promise<void> {
    await h.db.insert(tenantTable).values({
      id,
      name: id,
      slug: id,
      domain,
      parentId: null,
    });
  }

  test("rejects a second tenant whose domain differs only in case", async () => {
    // An inbound mail `From` is lowercased when parsed, so two case-variant
    // domains would collapse to one sender address. `tenant_domain_lower_idx`
    // makes `lower(domain)` unique so they cannot both exist -- otherwise a
    // case-variant registration could shadow another tenant's senders.
    await insertTenant("tnt_a", "acme.localhost");
    await expect(insertTenant("tnt_b", "ACME.localhost")).rejects.toThrow();
  });

  test("still rejects an exact duplicate domain", async () => {
    await insertTenant("tnt_a", "acme.localhost");
    await expect(insertTenant("tnt_b", "acme.localhost")).rejects.toThrow();
  });

  test("allows distinct domains", async () => {
    await insertTenant("tnt_a", "acme.localhost");
    await insertTenant("tnt_b", "beta.localhost");
    const rows = await h.db.select().from(tenantTable);
    expect(rows).toHaveLength(2);
  });
});
