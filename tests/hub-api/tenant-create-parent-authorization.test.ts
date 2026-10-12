// Creating a tenant under a parent needs `create` on `tenant:*` in the parent,
// held through an active membership, because the child inherits the parent's
// credentials, providers, catalogs, assets and OAuth clients. Every refusal is
// the same 403, so a caller cannot learn whether a tenant id exists or who
// belongs to it. Driven against a real spawned hub through the production HTTP
// route.

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
import { addMembership, addPrincipal } from "./lib/membership-fixtures";

const Created = type({ id: "string", parentId: "string | null" });

const REFUSAL = {
  status: 403,
  data: {
    error: {
      code: "forbidden",
      message:
        "You do not have permission to create a tenant under this parent",
    },
  },
};

let slugCounter = 0;
function uniqueSlug(): string {
  slugCounter += 1;
  return `child${String(Date.now())}${String(slugCounter)}`;
}

describe.skipIf(!harnessHubEnvAvailable())(
  "tenant creation under a parent",
  () => {
    let hub: HubHandle;
    let owner: SignedUpUser;
    let parent: CreatedTenant;

    beforeAll(async () => {
      hub = await startHub();
      owner = await signUpUser(hub.url, { emailPrefix: "owner" });
      parent = await createTenant(hub.url, owner);
    });

    afterAll(async () => {
      await hub.stop();
    });

    async function createUnder(
      user: SignedUpUser,
      parentId: string,
      slug = uniqueSlug(),
    ) {
      const res = await apiCall(
        hub.url,
        "POST",
        "/api/tenants",
        { name: `Child ${slug}`, slug, parentId },
        user.cookies,
      );
      return { status: res.status, data: res.data };
    }

    async function expectRefusedWithoutRow(
      user: SignedUpUser,
      parentId: string,
    ): Promise<void> {
      const slug = uniqueSlug();
      expect(await createUnder(user, parentId, slug)).toEqual(REFUSAL);
      // A refused create leaves no row, so the slug is still free.
      const retry = await apiCall(
        hub.url,
        "POST",
        "/api/tenants",
        { name: `Retry ${slug}`, slug },
        owner.cookies,
      );
      expect(retry.status).toBe(201);
    }

    test("a user outside the parent is refused", async () => {
      const outsider = await signUpUser(hub.url);
      await expectRefusedWithoutRow(outsider, parent.tenantId);
    });

    test("a parent that does not exist is refused the same way", async () => {
      const user = await signUpUser(hub.url);
      await expectRefusedWithoutRow(user, "tnt_does_not_exist");
    });

    test("a member with only read access is refused the same way", async () => {
      const member = await signUpUser(hub.url);
      await addMembership(
        hub.schema,
        parent.tenantId,
        member,
        "member",
        "active",
      );
      await expectRefusedWithoutRow(member, parent.tenantId);
    });

    test.each(["suspended", "invited", "deactivated"] as const)(
      "an admin with status %s is refused the same way",
      async (status) => {
        const admin = await signUpUser(hub.url);
        await addMembership(
          hub.schema,
          parent.tenantId,
          admin,
          "admin",
          status,
        );
        await expectRefusedWithoutRow(admin, parent.tenantId);
      },
    );

    test("a taken slug under a parent the caller cannot use gets the parent refusal before its slug is checked", async () => {
      const outsider = await signUpUser(hub.url);
      expect(await createUnder(outsider, parent.tenantId, parent.slug)).toEqual(
        REFUSAL,
      );
    });

    test("an admin with a deny on creating tenants is refused the same way", async () => {
      const admin = await signUpUser(hub.url);
      const principalId = await addMembership(
        hub.schema,
        parent.tenantId,
        admin,
        "admin",
        "active",
      );
      const deny = await apiCall(
        hub.url,
        "POST",
        `/api/tenants/${parent.tenantId}/grants`,
        {
          principalId,
          resource: "tenant:*",
          action: "create",
          effect: "deny",
          origin: "system",
        },
        owner.cookies,
      );
      expect(deny.status).toBe(201);
      await expectRefusedWithoutRow(admin, parent.tenantId);
    });

    test("a refused outsider holds no tenant that inherits the parent's providers", async () => {
      const created = await apiCall(
        hub.url,
        "POST",
        `/api/tenants/${parent.tenantId}/providers`,
        { name: `inherited-${uniqueSlug()}`, plugin: "openai" },
        owner.cookies,
      );
      expect(created.status).toBe(201);
      const providerId = type({ id: "string" }).assert(created.data).id;

      // A child the parent allows does inherit the provider, so a tenant under
      // the parent is what the outsider must not come to hold.
      const child = await createUnder(owner, parent.tenantId);
      expect(child.status).toBe(201);
      const childId = Created.assert(child.data).id;
      const inherited = await apiCall(
        hub.url,
        "GET",
        `/api/tenants/${childId}/providers/${providerId}`,
        undefined,
        owner.cookies,
      );
      expect(inherited.status).toBe(200);

      const outsider = await signUpUser(hub.url);
      const own = await createTenant(hub.url, outsider);
      expect(await createUnder(outsider, parent.tenantId)).toEqual(REFUSAL);

      // The outsider's only membership is its own top-level tenant, so no
      // tenant it can act in has the parent in its ancestor chain.
      const memberships = await apiCall(
        hub.url,
        "GET",
        "/api/me/principals",
        undefined,
        outsider.cookies,
      );
      expect(memberships.status).toBe(200);
      expect(
        type({ data: type({ tenantId: "string" }).array() })
          .assert(memberships.data)
          .data.map((p) => p.tenantId),
      ).toEqual([own.tenantId]);
      const ownRow = await apiCall(
        hub.url,
        "GET",
        `/api/tenants/${own.tenantId}`,
        undefined,
        outsider.cookies,
      );
      expect(Created.assert(ownRow.data).parentId).toBeNull();
    });

    test("the parent's owner creates a child of the parent", async () => {
      const res = await createUnder(owner, parent.tenantId);
      expect(res.status).toBe(201);
      expect(Created.assert(res.data).parentId).toBe(parent.tenantId);
    });

    test("an active admin creates a child of the parent", async () => {
      const admin = await signUpUser(hub.url);
      await addMembership(
        hub.schema,
        parent.tenantId,
        admin,
        "admin",
        "active",
      );
      const res = await createUnder(admin, parent.tenantId);
      expect(res.status).toBe(201);
      expect(Created.assert(res.data).parentId).toBe(parent.tenantId);
    });

    test("a custom role holding only create on tenant:* creates a child", async () => {
      const role = await apiCall(
        hub.url,
        "POST",
        `/api/tenants/${parent.tenantId}/roles`,
        { name: "child-creator" },
        owner.cookies,
      );
      expect(role.status).toBe(201);
      const roleId = type({ id: "string" }).assert(role.data).id;
      const grant = await apiCall(
        hub.url,
        "POST",
        `/api/tenants/${parent.tenantId}/grants`,
        {
          roleId,
          resource: "tenant:*",
          action: "create",
          effect: "allow",
          origin: "role",
        },
        owner.cookies,
      );
      expect(grant.status).toBe(201);

      const creator = await signUpUser(hub.url);
      const principalId = await addPrincipal(
        hub.schema,
        parent.tenantId,
        creator,
        "active",
      );
      const assigned = await apiCall(
        hub.url,
        "POST",
        `/api/tenants/${parent.tenantId}/principals/${principalId}/roles/${roleId}`,
        undefined,
        owner.cookies,
      );
      expect(assigned.status).toBe(204);

      const res = await createUnder(creator, parent.tenantId);
      expect(res.status).toBe(201);
      expect(Created.assert(res.data).parentId).toBe(parent.tenantId);
    });
  },
);
