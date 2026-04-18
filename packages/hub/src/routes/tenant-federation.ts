import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { federationTrust, tenant } from "@interchange/db/schema";
import {
  FederationTrust,
  CreateFederationTrust,
  ErrorResponse,
  paginatedSchema,
} from "@interchange/types";

import type { TenantEnv } from "../context";
import { ts } from "../format";
import { generateId } from "../ids";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";

const app = new Hono<TenantEnv>();

app.get(
  "/federation",
  describeRoute({
    tags: ["Tenants"],
    summary: "List federation trust relationships",
    parameters: [...pageParameters],
    responses: {
      200: {
        description: "Federation trusts",
        content: {
          "application/json": {
            schema: resolver(paginatedSchema(FederationTrust)),
          },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const db = c.get("db");
    const { limit, cursor } = parsePageParams({
      cursor: c.req.query("cursor"),
      limit: c.req.query("limit"),
    });

    const conditions = [eq(federationTrust.tenantId, tenantCtx.id)];
    if (cursor) {
      conditions.push(
        cursorCondition(federationTrust.createdAt, federationTrust.id, cursor),
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
  "/federation",
  describeRoute({
    tags: ["Tenants"],
    summary: "Establish federation trust",
    description:
      "Creates a trust relationship with another tenant for cross-tenant agent discovery and interaction.",
    responses: {
      201: {
        description: "Trust established",
        content: {
          "application/json": { schema: resolver(FederationTrust) },
        },
      },
      400: {
        description: "Validation error",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", CreateFederationTrust),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const body = c.req.valid("json");
    const db = c.get("db");

    const target = await db.query.tenant.findFirst({
      where: eq(tenant.id, body.targetTenantId),
    });
    if (!target) {
      return c.json(
        {
          error: {
            code: "not_found",
            message: "Target tenant not found",
          },
        },
        404,
      );
    }

    const existing = await db.query.federationTrust.findFirst({
      where: and(
        eq(federationTrust.tenantId, tenantCtx.id),
        eq(federationTrust.targetTenantId, body.targetTenantId),
      ),
    });
    if (existing) {
      return c.json(
        {
          error: {
            code: "conflict",
            message: "Trust relationship already exists",
          },
        },
        409,
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
  "/federation/:targetTenantId",
  describeRoute({
    tags: ["Tenants"],
    summary: "Revoke federation trust",
    responses: {
      204: {
        description: "Trust revoked",
      },
      404: {
        description: "Trust not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const targetTenantId = c.req.param("targetTenantId");
    const db = c.get("db");

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
      return c.json(
        {
          error: { code: "not_found", message: "Trust not found" },
        },
        404,
      );
    }

    return c.body(null, 204);
  },
);

export { app as tenantFederationRoutes };
