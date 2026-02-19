import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import {
  tenant,
  principal,
  role,
  principalRole,
  grant,
  federationTrust,
} from "@interchange/db/schema";
import {
  CreateTenant,
  UpdateTenant,
  TenantResponse,
  FederationTrust,
  CreateFederationTrust,
  ErrorResponse,
} from "@interchange/types";

import type { AppEnv, TenantRow } from "../context";
import { first, ts } from "../format";
import { generateId } from "../ids";

const SYSTEM_ROLES = ["owner", "admin", "member"] as const;

function formatTenant(row: typeof tenant.$inferSelect) {
  return {
    id: row.id,
    name: row.name,
    slug: row.slug,
    domain: row.domain,
    parentId: row.parentId ?? null,
    config: (row.config as Record<string, unknown>) ?? undefined,
    createdAt: ts(row.createdAt),
    updatedAt: ts(row.updatedAt),
  };
}

const app = new Hono<AppEnv>();

app.post(
  "/",
  describeRoute({
    tags: ["Tenants"],
    summary: "Create a tenant",
    description:
      "Creates a new tenant. The authenticated user becomes the owner with a principal and default owner role.",
    responses: {
      201: {
        description: "Tenant created",
        content: {
          "application/json": { schema: resolver(TenantResponse) },
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
  validator("json", CreateTenant),
  async (c) => {
    const user = c.get("user");
    if (!user) {
      return c.json(
        {
          error: { code: "unauthorized", message: "Authentication required" },
        },
        401,
      );
    }

    const body = c.req.valid("json" as never) as typeof CreateTenant.infer;
    const db = c.get("db");

    const tenantId = generateId("tenant");
    const domain = `${body.slug}.localhost`;

    const existing = await db.query.tenant.findFirst({
      where: eq(tenant.slug, body.slug),
    });
    if (existing) {
      return c.json(
        {
          error: { code: "conflict", message: "Slug already taken" },
        },
        409,
      );
    }

    const now = new Date();

    const tenantRow = first(
      await db
        .insert(tenant)
        .values({
          id: tenantId,
          name: body.name,
          slug: body.slug,
          domain,
          parentId: body.parentId ?? null,
          createdAt: now,
          updatedAt: now,
        })
        .returning(),
    );

    const roleIds: Record<string, string> = {};
    for (const roleName of SYSTEM_ROLES) {
      const roleId = generateId("role");
      roleIds[roleName] = roleId;
      await db.insert(role).values({
        id: roleId,
        tenantId,
        name: roleName,
        description: `System ${roleName} role`,
        isSystem: true,
        createdAt: now,
        updatedAt: now,
      });
    }

    const ownerRoleId = roleIds["owner"];
    if (!ownerRoleId) throw new Error("Owner role was not created");

    const principalId = generateId("principal");
    await db.insert(principal).values({
      id: principalId,
      tenantId,
      kind: "user",
      refId: user.id,
      status: "active",
      createdAt: now,
      updatedAt: now,
    });

    await db.insert(principalRole).values({
      principalId,
      roleId: ownerRoleId,
      createdAt: now,
    });

    // Grant owner role full access
    await db.insert(grant).values({
      id: generateId("grant"),
      tenantId,
      roleId: ownerRoleId,
      resource: "*",
      action: "*",
      effect: "allow",
      source: "system",
      createdAt: now,
      updatedAt: now,
    });

    // Grant admin role broad management access
    const adminRoleId = roleIds["admin"];
    if (adminRoleId) {
      await db.insert(grant).values([
        {
          id: generateId("grant"),
          tenantId,
          roleId: adminRoleId,
          resource: "*",
          action: "read",
          effect: "allow",
          source: "system",
          createdAt: now,
          updatedAt: now,
        },
        {
          id: generateId("grant"),
          tenantId,
          roleId: adminRoleId,
          resource: "*",
          action: "create",
          effect: "allow",
          source: "system",
          createdAt: now,
          updatedAt: now,
        },
        {
          id: generateId("grant"),
          tenantId,
          roleId: adminRoleId,
          resource: "*",
          action: "manage",
          effect: "allow",
          source: "system",
          createdAt: now,
          updatedAt: now,
        },
      ]);
    }

    // Grant member role read-only access
    const memberRoleId = roleIds["member"];
    if (memberRoleId) {
      await db.insert(grant).values({
        id: generateId("grant"),
        tenantId,
        roleId: memberRoleId,
        resource: "*",
        action: "read",
        effect: "allow",
        source: "system",
        createdAt: now,
        updatedAt: now,
      });
    }

    return c.json(formatTenant(tenantRow), 201);
  },
);

app.get(
  "/:tenantId",
  describeRoute({
    tags: ["Tenants"],
    summary: "Get tenant details",
    responses: {
      200: {
        description: "Tenant details",
        content: {
          "application/json": { schema: resolver(TenantResponse) },
        },
      },
      403: {
        description: "Not a member of this tenant",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      404: {
        description: "Tenant not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const user = c.get("user");
    if (!user) {
      return c.json(
        {
          error: { code: "unauthorized", message: "Authentication required" },
        },
        401,
      );
    }

    const tenantId = c.req.param("tenantId");
    const db = c.get("db");

    const tenantRow = await db.query.tenant.findFirst({
      where: eq(tenant.id, tenantId),
    });
    if (!tenantRow) {
      return c.json(
        { error: { code: "not_found", message: "Tenant not found" } },
        404,
      );
    }

    const membership = await db.query.principal.findFirst({
      where: and(
        eq(principal.tenantId, tenantId),
        eq(principal.kind, "user"),
        eq(principal.refId, user.id),
      ),
    });
    if (!membership) {
      return c.json(
        {
          error: {
            code: "forbidden",
            message: "Not a member of this tenant",
          },
        },
        403,
      );
    }

    return c.json(formatTenant(tenantRow));
  },
);

app.patch(
  "/:tenantId",
  describeRoute({
    tags: ["Tenants"],
    summary: "Update tenant config",
    description: "Requires admin or higher grant within the tenant.",
    responses: {
      200: {
        description: "Tenant updated",
        content: {
          "application/json": { schema: resolver(TenantResponse) },
        },
      },
      403: {
        description: "Insufficient grants",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", UpdateTenant),
  async (c) => {
    const user = c.get("user");
    if (!user) {
      return c.json(
        {
          error: { code: "unauthorized", message: "Authentication required" },
        },
        401,
      );
    }

    const tenantId = c.req.param("tenantId");
    const body = c.req.valid("json" as never) as typeof UpdateTenant.infer;
    const db = c.get("db");

    const membership = await db.query.principal.findFirst({
      where: and(
        eq(principal.tenantId, tenantId),
        eq(principal.kind, "user"),
        eq(principal.refId, user.id),
      ),
    });
    if (!membership) {
      return c.json(
        {
          error: {
            code: "forbidden",
            message: "Not a member of this tenant",
          },
        },
        403,
      );
    }

    const updates: Record<string, unknown> = { updatedAt: new Date() };
    if (body.name !== undefined) updates["name"] = body.name;
    if (body.config !== undefined) updates["config"] = body.config;

    const [updated] = await db
      .update(tenant)
      .set(updates)
      .where(eq(tenant.id, tenantId))
      .returning();

    if (!updated) {
      return c.json(
        { error: { code: "not_found", message: "Tenant not found" } },
        404,
      );
    }

    return c.json(formatTenant(updated));
  },
);

// Federation routes go through resolveTenant middleware via the
// /api/tenants/:tenantId/* wildcard in app.ts.

app.get(
  "/:tenantId/federation",
  describeRoute({
    tags: ["Tenants"],
    summary: "List federation trust relationships",
    responses: {
      200: {
        description: "Federation trusts",
        content: {
          "application/json": {
            schema: resolver(FederationTrust.array()),
          },
        },
      },
    },
  }),
  async (c) => {
    // Set by resolveTenant middleware via /api/tenants/:tenantId/* wildcard
    const tenantCtx = c.get("tenant" as never) as TenantRow;
    const db = c.get("db");

    const trusts = await db.query.federationTrust.findMany({
      where: eq(federationTrust.tenantId, tenantCtx.id),
    });

    const targetIds = trusts.map((t) => t.targetTenantId);
    const tenants =
      targetIds.length > 0
        ? await db.query.tenant.findMany({
            where: (t, { inArray }) => inArray(t.id, targetIds),
          })
        : [];
    const tenantMap = new Map(tenants.map((t) => [t.id, t]));

    const results = trusts.map((trust) => {
      const target = tenantMap.get(trust.targetTenantId);
      return {
        tenantId: trust.targetTenantId,
        tenantName: target?.name ?? "Unknown",
        tenantDomain: target?.domain ?? "unknown",
        direction: trust.direction,
        createdAt: ts(trust.createdAt),
      };
    });

    return c.json(results);
  },
);

app.post(
  "/:tenantId/federation",
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
    const tenantCtx = c.get("tenant" as never) as TenantRow;
    const body = c.req.valid(
      "json" as never,
    ) as typeof CreateFederationTrust.infer;
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
  "/:tenantId/federation/:targetTenantId",
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
    const tenantCtx = c.get("tenant" as never) as TenantRow;
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

export { app as tenantRoutes };
