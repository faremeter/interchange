// Updating a tenant needs `manage` on it, held through an active membership.
// Driven against a real spawned hub through the production HTTP route; the
// memberships beyond the tenant's creator are seeded into the hub's schema.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { type } from "arktype";

import {
  harnessHubEnvAvailable,
  startHub,
  type HubHandle,
} from "./lib/git-harness";
import {
  apiCall,
  createTenant,
  signUpUser,
  type CreatedTenant,
  type SignedUpUser,
} from "./lib/git-asset-fixtures";
import {
  addMembership,
  type MembershipStatus,
} from "./lib/membership-fixtures";

const TenantName = type({ name: "string" });

describe.skipIf(!harnessHubEnvAvailable())(
  "tenant update authorization",
  () => {
    let hub: HubHandle;
    let owner: SignedUpUser;
    let tenant: CreatedTenant;

    beforeAll(async () => {
      hub = await startHub();
      owner = await signUpUser(hub.url, { emailPrefix: "owner" });
      tenant = await createTenant(hub.url, owner);
    });

    afterAll(async () => {
      await hub.stop();
    });

    async function userWithMembership(
      roleName: "admin" | "member",
      status: MembershipStatus = "active",
    ): Promise<SignedUpUser> {
      const user = await signUpUser(hub.url);
      await addMembership(hub.schema, tenant.tenantId, user, roleName, status);
      return user;
    }

    function patchTenant(user: SignedUpUser, body: unknown) {
      return apiCall(
        hub.url,
        "PATCH",
        `/api/tenants/${tenant.tenantId}`,
        body,
        user.cookies,
      );
    }

    async function tenantName(): Promise<string> {
      const res = await apiCall(
        hub.url,
        "GET",
        `/api/tenants/${tenant.tenantId}`,
        undefined,
        owner.cookies,
      );
      expect(res.status).toBe(200);
      return TenantName.assert(res.data).name;
    }

    test("a member with only read access cannot rename the tenant or change its config", async () => {
      const member = await userWithMembership("member");
      const before = await tenantName();
      expect((await patchTenant(member, { name: "Renamed" })).status).toBe(403);
      expect(
        (await patchTenant(member, { config: { note: "changed" } })).status,
      ).toBe(403);
      expect(await tenantName()).toBe(before);
    });

    test("an admin can rename the tenant", async () => {
      const admin = await userWithMembership("admin");
      expect(
        (await patchTenant(admin, { name: "Renamed by admin" })).status,
      ).toBe(200);
      expect(await tenantName()).toBe("Renamed by admin");
    });

    test.each(["suspended", "invited", "deactivated"] as const)(
      "a %s admin cannot update the tenant",
      async (status) => {
        const admin = await userWithMembership("admin", status);
        const before = await tenantName();
        expect((await patchTenant(admin, { name: "Renamed" })).status).toBe(
          403,
        );
        expect(await tenantName()).toBe(before);
      },
    );

    test("a user outside the tenant cannot update it", async () => {
      const outsider = await signUpUser(hub.url);
      expect((await patchTenant(outsider, { name: "Renamed" })).status).toBe(
        403,
      );
    });
  },
);
