import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, validator } from "hono-openapi";

import { federationTrust, tenant } from "@intx/db/schema";
import type { DB } from "@intx/db";
import {
  FederationTrust,
  ErrorResponse,
  CreateFederationTrust,
  paginatedSchema,
} from "@intx/types";

import type { TenantEnv } from "../context";
import {
  denyUnlessBodyGrant,
  idResource,
  type AuthorizeGrant,
  type RequireGrant,
} from "../middleware/grant";
import { errorResponse } from "../error-response";
import { ts } from "../format";
import { generateId } from "@intx/hub-common";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";
import { jsonResponse } from "../openapi";

export type CreateTenantFederationRoutesDeps = {
  db: DB["db"];
  requireGrant: RequireGrant;
  authorizeGrant: AuthorizeGrant;
};

export function createTenantFederationRoutes({
  db,
  requireGrant,
  authorizeGrant,
}: CreateTenantFederationRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  app.get(
    "/",
    describeRoute({
      tags: ["Tenants"],
      summary: "List federation trust relationships",
      parameters: [...pageParameters],
      responses: {
        200: jsonResponse(
          "Federation trusts",
          paginatedSchema(FederationTrust),
        ),
        403: jsonResponse("Insufficient grants", ErrorResponse),
      },
    }),
    requireGrant("federation:*", "read"),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      const conditions = [eq(federationTrust.tenantId, tenantCtx.id)];
      if (cursor) {
        conditions.push(
          cursorCondition(
            federationTrust.createdAt,
            federationTrust.id,
            cursor,
          ),
        );
      }

      const rows = await db.query.federationTrust.findMany({
        where: and(...conditions),
        orderBy: pageOrder(federationTrust.createdAt, federationTrust.id),
        limit,
      });

      const targetIds = rows.map((t) => t.targetTenantId);
      const tenants =
        targetIds.length > 0
          ? await db.query.tenant.findMany({
              where: (t, { inArray }) => inArray(t.id, targetIds),
            })
          : [];
      const tenantMap = new Map(tenants.map((t) => [t.id, t]));

      const items = rows.map((trust) => {
        const target = tenantMap.get(trust.targetTenantId);
        return {
          tenantId: trust.targetTenantId,
          tenantName: target?.name ?? "Unknown",
          tenantDomain: target?.domain ?? "unknown",
          direction: trust.direction,
          createdAt: ts(trust.createdAt),
        };
      });

      return c.json(paginatedResponse(items, rows, limit));
    },
  );

  app.post(
    "/",
    describeRoute({
      tags: ["Tenants"],
      summary: "Establish federation trust",
      description:
        "Creates a trust relationship with another tenant for cross-tenant agent discovery and interaction.",
      responses: {
        201: jsonResponse("Trust established", FederationTrust),
        400: jsonResponse("Validation error", ErrorResponse),
        403: jsonResponse("Insufficient grants", ErrorResponse),
        404: jsonResponse("Target tenant not found", ErrorResponse),
        409: jsonResponse("Trust relationship already exists", ErrorResponse),
      },
    }),
    validator("json", CreateFederationTrust),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const body = c.req.valid("json");

      // Keyed on the target, as revoke is, so a grant on one target's trust
      // decides that trust and a `federation:*` grant covers every target.
      const denied = await denyUnlessBodyGrant({
        c,
        authorizeGrant,
        resource: `federation:${body.targetTenantId}`,
        action: "create",
      });
      if (denied !== null) return denied;

      const target = await db.query.tenant.findFirst({
        where: eq(tenant.id, body.targetTenantId),
      });
      if (!target) {
        return errorResponse(c, "not_found", "Target tenant not found");
      }

      const existing = await db.query.federationTrust.findFirst({
        where: and(
          eq(federationTrust.tenantId, tenantCtx.id),
          eq(federationTrust.targetTenantId, body.targetTenantId),
        ),
      });
      if (existing) {
        return errorResponse(
          c,
          "conflict",
          "Trust relationship already exists",
        );
      }

      await db.insert(federationTrust).values({
        id: generateId("federationTrust"),
        tenantId: tenantCtx.id,
        targetTenantId: body.targetTenantId,
        direction: body.direction,
        createdAt: new Date(),
      });

      return c.json(
        {
          tenantId: body.targetTenantId,
          tenantName: target.name,
          tenantDomain: target.domain,
          direction: body.direction,
          createdAt: ts(new Date()),
        },
        201,
      );
    },
  );

  app.delete(
    "/:targetTenantId",
    describeRoute({
      tags: ["Tenants"],
      summary: "Revoke federation trust",
      responses: {
        204: {
          description: "Trust revoked",
        },
        403: jsonResponse("Insufficient grants", ErrorResponse),
        404: jsonResponse("Trust not found", ErrorResponse),
      },
    }),
    requireGrant(idResource("federation", "targetTenantId"), "manage"),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const targetTenantId = c.req.param("targetTenantId");

      const deleted = await db
        .delete(federationTrust)
        .where(
          and(
            eq(federationTrust.tenantId, tenantCtx.id),
            eq(federationTrust.targetTenantId, targetTenantId),
          ),
        )
        .returning();

      if (deleted.length === 0) {
        return errorResponse(c, "not_found", "Trust not found");
      }

      return c.body(null, 204);
    },
  );

  return app;
}
