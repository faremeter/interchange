import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { grant, role } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// The grant table's `grant_target_exactly_one` CHECK enforces that a grant
// hangs off exactly one target -- a role OR a principal, never both and never
// neither. This invariant is documented in AUTH.md and enforced in the DB so
// a malformed grant row is rejected at write time.

describe.skipIf(!harnessDbEnvAvailable())(
  "grant target CHECK constraint (real DB)",
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
      await seedTenants(h.db, [{ id: "tnt_root" }]);
      await seedPrincipal(h.db, { id: "prn_x", tenantId: "tnt_root" });
      await h.db.insert(role).values({
        id: "rol_x",
        tenantId: "tnt_root",
        name: "member",
        isSystem: true,
      });
    });

    async function insertGrant(fields: {
      principalId: string | null;
      roleId: string | null;
    }): Promise<void> {
      await h.db.insert(grant).values({
        id: `grt_${Math.random().toString(36).slice(2, 10)}`,
        tenantId: "tnt_root",
        resource: "credential:*",
        action: "use",
        effect: "allow",
        origin: "system",
        principalId: fields.principalId,
        roleId: fields.roleId,
      });
    }

    test("rejects a grant with neither principal_id nor role_id", async () => {
      await expect(
        insertGrant({ principalId: null, roleId: null }),
      ).rejects.toThrow();
    });

    test("rejects a grant with both principal_id and role_id", async () => {
      await expect(
        insertGrant({ principalId: "prn_x", roleId: "rol_x" }),
      ).rejects.toThrow();
    });

    test("accepts a principal-scoped grant", async () => {
      await insertGrant({ principalId: "prn_x", roleId: null });
    });

    test("accepts a role-scoped grant", async () => {
      await insertGrant({ principalId: null, roleId: "rol_x" });
    });
  },
);
