import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { Hono } from "hono";

import { createNoopCredentialCipher, sha256 } from "@intx/crypto";
import { createPrincipalKeyStore } from "@intx/db";
import { executionHost } from "@intx/db/schema";
import {
  createExecutionHostRoutes,
  type RequireGrant,
  type TenantEnv,
} from "@intx/hub-api";
import {
  ExecutionHostEnrollmentResponse,
  type ExecutionHostEnrollmentResponse as ExecutionHostEnrollment,
} from "@intx/types";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";

const TENANT_ID = "tnt-host-enrollment";
const OWNER_PRINCIPAL_ID = "prn-host-owner";

const allowAll: RequireGrant = () => async (_c, next) => {
  await next();
};

describe.skipIf(!harnessDbEnvAvailable())(
  "execution host enrollment (real DB)",
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
      await seedTenants(h.db, [{ id: TENANT_ID }]);
      await seedPrincipal(h.db, {
        id: OWNER_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "user",
        refId: "user-host-owner",
      });
    });

    function createApp() {
      const app = new Hono<TenantEnv>();
      app.use("*", async (c, next) => {
        c.set("tenant", {
          id: TENANT_ID,
          name: "Host tenant",
          slug: "host-tenant",
          domain: "host.test",
          parentId: null,
          config: {},
          createdAt: new Date(),
          updatedAt: new Date(),
        });
        c.set("principal", {
          id: OWNER_PRINCIPAL_ID,
          tenantId: TENANT_ID,
          kind: "user",
          refId: "user-host-owner",
          status: "active",
          createdAt: new Date(),
          updatedAt: new Date(),
        });
        await next();
      });
      app.route(
        "/",
        createExecutionHostRoutes({
          db: h.db,
          principalKeyStore: createPrincipalKeyStore({
            db: h.db,
            cipher: createNoopCredentialCipher(),
          }),
          requireGrant: allowAll,
        }),
      );
      return app;
    }

    test("creates the host and principal atomically and returns its secret once", async () => {
      const response = await createApp().request("/", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ displayName: "Farin's iPhone" }),
      });

      expect(response.status).toBe(201);
      const body: unknown = await response.json();
      const enrollment: ExecutionHostEnrollment =
        ExecutionHostEnrollmentResponse.assert(body);
      expect(enrollment.secret).toStartWith("intx_host_");
      expect(enrollment.host.ownerPrincipalId).toBe(OWNER_PRINCIPAL_ID);
      expect(enrollment.host.displayName).toBe("Farin's iPhone");

      const storedHost = await h.db.query.executionHost.findFirst({
        where: (host, { eq }) => eq(host.id, enrollment.host.id),
      });
      expect(storedHost).toMatchObject({
        principalId: enrollment.host.principalId,
        ownerPrincipalId: OWNER_PRINCIPAL_ID,
      });
      expect(storedHost?.tokenHashSha256).toEqual(
        await sha256(enrollment.secret),
      );

      expect(
        await h.db.query.principal.findFirst({
          where: (row, { eq }) => eq(row.id, enrollment.host.principalId),
        }),
      ).toMatchObject({
        tenantId: TENANT_ID,
        kind: "host",
        refId: enrollment.host.id,
        status: "active",
      });
      expect(
        await h.db.query.principalKey.findFirst({
          where: (row, { eq }) =>
            eq(row.principalId, enrollment.host.principalId),
        }),
      ).toMatchObject({
        principalId: enrollment.host.principalId,
        status: "active",
      });
    });

    test("rejects a blank display name without persisting a host", async () => {
      const response = await createApp().request("/", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ displayName: "   " }),
      });

      expect(response.status).toBe(400);
      expect(await h.db.select().from(executionHost)).toEqual([]);
      const principals = await h.db.query.principal.findMany();
      expect(principals).toHaveLength(1);
      expect(principals[0]?.id).toBe(OWNER_PRINCIPAL_ID);
    });
  },
);
