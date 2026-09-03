import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { sha256 } from "@intx/crypto";
import { executionHost } from "@intx/db/schema";
import {
  createPrincipalStore,
  type DB,
  type PrincipalKeyStore,
} from "@intx/db";
import { generateId, HOST_TOKEN_PREFIX } from "@intx/hub-common";
import {
  CreateExecutionHost,
  ErrorResponse,
  ExecutionHostEnrollmentResponse,
  hexEncode,
} from "@intx/types";

import type { TenantEnv } from "../context";
import type { RequireGrant } from "../middleware/grant";
import { ts } from "../format";

export type CreateExecutionHostRoutesDeps = {
  db: DB["db"];
  principalKeyStore: PrincipalKeyStore;
  requireGrant: RequireGrant;
};

export function createExecutionHostRoutes({
  db,
  principalKeyStore,
  requireGrant,
}: CreateExecutionHostRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();
  const principalStore = createPrincipalStore(db, principalKeyStore);

  app.post(
    "/",
    requireGrant("host:*", "create"),
    describeRoute({
      tags: ["Execution Hosts"],
      summary: "Enroll an execution host",
      description:
        "Creates a tenant-scoped host principal and returns its connection secret exactly once.",
      responses: {
        400: {
          description: "Invalid execution host enrollment request",
          content: {
            "application/json": { schema: {} },
          },
        },
        403: {
          description: "Insufficient grants",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        201: {
          description: "Execution host enrolled",
          content: {
            "application/json": {
              schema: resolver(ExecutionHostEnrollmentResponse),
            },
          },
        },
      },
    }),
    validator("json", CreateExecutionHost),
    async (c) => {
      const tenant = c.get("tenant");
      const owner = c.get("principal");
      const body = c.req.valid("json");
      const hostId = generateId("executionHost");
      const principalId = generateId("principal");
      const secret = `${HOST_TOKEN_PREFIX}${hexEncode(crypto.getRandomValues(new Uint8Array(32)))}`;
      const now = new Date();

      const host = await db.transaction(async (tx) => {
        await principalStore.create(
          {
            id: principalId,
            tenantId: tenant.id,
            kind: "host",
            refId: hostId,
            status: "active",
            createdAt: now,
            updatedAt: now,
          },
          tx,
        );
        const [created] = await tx
          .insert(executionHost)
          .values({
            id: hostId,
            tenantId: tenant.id,
            principalId,
            ownerPrincipalId: owner.id,
            displayName: body.displayName.trim(),
            tokenHashSha256: await sha256(secret),
            createdAt: now,
            updatedAt: now,
          })
          .returning();
        if (created === undefined) {
          throw new Error("Execution host insert returned no row");
        }
        return created;
      });

      return c.json(
        {
          host: {
            id: host.id,
            tenantId: host.tenantId,
            principalId: host.principalId,
            ownerPrincipalId: host.ownerPrincipalId,
            displayName: host.displayName,
            createdAt: ts(host.createdAt),
            updatedAt: ts(host.updatedAt),
          },
          secret,
        },
        201,
      );
    },
  );

  return app;
}
