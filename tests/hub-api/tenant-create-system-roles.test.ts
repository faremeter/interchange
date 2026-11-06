// Tenant creation gives the owner and admin system roles a send grant for
// every recipient in the new tenant. The owner's `*` grant does not reach the
// `mail.send` namespace, so it needs its own row; the member role gets none.
// Driven against a real spawned hub through the production HTTP route.

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
  type SignedUpUser,
} from "./lib/git-asset-fixtures";
import { listRoleGrants } from "./lib/membership-fixtures";

describe.skipIf(!harnessHubEnvAvailable())(
  "tenant creation system role send grants",
  () => {
    let hub: HubHandle;
    let owner: SignedUpUser;

    beforeAll(async () => {
      hub = await startHub();
      owner = await signUpUser(hub.url, { emailPrefix: "owner" });
    });

    afterAll(async () => {
      await hub.stop();
    });

    async function sendGrantsOf(tenantId: string) {
      const rows = await listRoleGrants(hub.schema, tenantId);
      return rows.filter((row) => row.resource.startsWith("mail.send:"));
    }

    function expectedSendGrants(tenantId: string) {
      return ["admin", "owner"].map((roleName) => ({
        roleName,
        resource: `mail.send:tenant:${tenantId}`,
        action: "send",
        effect: "allow",
        origin: "system",
      }));
    }

    test("owner and admin get one tenant send grant and member gets none", async () => {
      const tenant = await createTenant(hub.url, owner);

      expect(await sendGrantsOf(tenant.tenantId)).toEqual(
        expectedSendGrants(tenant.tenantId),
      );
    });

    test("a child tenant's send grants name the child, not the parent", async () => {
      const parent = await createTenant(hub.url, owner);
      const slug = `child${String(Date.now())}`;
      const created = await apiCall(
        hub.url,
        "POST",
        "/api/tenants",
        { name: `Child ${slug}`, slug, parentId: parent.tenantId },
        owner.cookies,
      );
      expect(created.status).toBe(201);
      const childId = type({ id: "string" }).assert(created.data).id;

      expect(await sendGrantsOf(childId)).toEqual(expectedSendGrants(childId));
    });
  },
);
