import { type } from "arktype";
import { eq, and, sql } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, validator } from "hono-openapi";
import { authorize } from "@intx/authz";

import { tenant, principal, role, principalRole, grant } from "@intx/db/schema";
import {
  createPrincipalStore,
  parseTenantRow,
  TenantConfigInvalidError,
  validateLifecyclePolicyEdit,
} from "@intx/db";
import type { DB, PrincipalKeyStore } from "@intx/db";
import type { GrantStore, ConditionRegistry } from "@intx/types/authz";
import {
  CreateTenant,
  ErrorResponse,
  TenantConfig,
  UpdateTenant,
  TenantResponse,
  type TenantConfigPatch,
} from "@intx/types";

import { unauthorizedResponse, type AppEnv } from "../context";
import { errorResponse, tenantConfigErrorResponse } from "../error-response";
import { first, ts } from "../format";
import { jsonResponse } from "../openapi";
import { generateId } from "@intx/hub-common";

const SYSTEM_ROLES = ["owner", "admin", "member"] as const;

function formatTenant(row: typeof tenant.$inferSelect) {
  const parsed = parseTenantRow(row);
  return {
    id: parsed.id,
    name: parsed.name,
    slug: parsed.slug,
    domain: parsed.domain,
    parentId: parsed.parentId ?? null,
    config: parsed.config ?? undefined,
    createdAt: ts(parsed.createdAt),
    updatedAt: ts(parsed.updatedAt),
  };
}

// Separate policies share `config`, so an update must keep the keys it omits.
function mergeTenantConfig(
  stored: unknown,
  patch: TenantConfigPatch,
): Record<string, unknown> {
  const merged = new Map<string, unknown>(
    typeof stored === "object" && stored !== null ? Object.entries(stored) : [],
  );
  for (const [key, value] of Object.entries(patch)) {
    if (value === null) merged.delete(key);
    else merged.set(key, value);
  }
  return Object.fromEntries(merged);
}

export type CreateTenantRoutesDeps = {
  db: DB["db"];
  principalKeyStore: PrincipalKeyStore;
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
};

export function createTenantRoutes({
  db,
  principalKeyStore,
  grantStore,
  conditionRegistry,
}: CreateTenantRoutesDeps): Hono<AppEnv> {
  const app = new Hono<AppEnv>();
  const principalStore = createPrincipalStore(db, principalKeyStore);

  app.post(
    "/",
    describeRoute({
      tags: ["Tenants"],
      summary: "Create a tenant",
      description:
        "Creates a new tenant. The authenticated user becomes the owner with a principal and default owner role.",
      responses: {
        201: jsonResponse("Tenant created", TenantResponse),
        400: jsonResponse("Validation error", ErrorResponse),
      },
    }),
    validator("json", CreateTenant),
    async (c) => {
      const user = c.get("user");
      if (!user) {
        return unauthorizedResponse(c);
      }

      const body = c.req.valid("json");

      const tenantId = generateId("tenant");
      // Derive the domain from a lowercased slug and check the slug conflict
      // case-insensitively. An inbound mail `From` is lowercased when parsed, so
      // a sender address must map to exactly one tenant; `tenant_domain_lower_idx`
      // enforces a unique `lower(domain)`. Normalizing here keeps a case-variant
      // slug a clean 409 instead of a unique-violation 500, and stops a
      // case-variant registration from shadowing another tenant's senders.
      const domain = `${body.slug.toLowerCase()}.localhost`;

      const existing = await db.query.tenant.findFirst({
        where: eq(sql`lower(${tenant.slug})`, body.slug.toLowerCase()),
      });
      if (existing) {
        return errorResponse(c, "conflict", "Slug already taken");
      }

      const now = new Date();

      const tenantRow = await db.transaction(async (tx) => {
        const inserted = first(
          await tx
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
          await tx.insert(role).values({
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

        const ownerPrincipal = await principalStore.create(
          {
            id: generateId("principal"),
            tenantId,
            kind: "user",
            refId: user.id,
            status: "active",
            createdAt: now,
            updatedAt: now,
          },
          tx,
        );

        await tx.insert(principalRole).values({
          principalId: ownerPrincipal.id,
          roleId: ownerRoleId,
          createdAt: now,
        });

        // Grant owner role full access
        await tx.insert(grant).values({
          id: generateId("grant"),
          tenantId,
          roleId: ownerRoleId,
          resource: "*",
          action: "*",
          effect: "allow",
          origin: "system",
          createdAt: now,
          updatedAt: now,
        });

        // Grant admin role broad management access
        const adminRoleId = roleIds["admin"];
        if (adminRoleId) {
          await tx.insert(grant).values([
            {
              id: generateId("grant"),
              tenantId,
              roleId: adminRoleId,
              resource: "*",
              action: "read",
              effect: "allow",
              origin: "system",
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
              origin: "system",
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
              origin: "system",
              createdAt: now,
              updatedAt: now,
            },
          ]);
        }

        // Grant member role read-only access
        const memberRoleId = roleIds["member"];
        if (memberRoleId) {
          await tx.insert(grant).values({
            id: generateId("grant"),
            tenantId,
            roleId: memberRoleId,
            resource: "*",
            action: "read",
            effect: "allow",
            origin: "system",
            createdAt: now,
            updatedAt: now,
          });
        }

        return inserted;
      });

      return c.json(formatTenant(tenantRow), 201);
    },
  );

  app.get(
    "/:tenantId",
    describeRoute({
      tags: ["Tenants"],
      summary: "Get tenant details",
      responses: {
        200: jsonResponse("Tenant details", TenantResponse),
        403: jsonResponse("Not a member of this tenant", ErrorResponse),
        404: jsonResponse("Tenant not found", ErrorResponse),
        409: jsonResponse("Stored tenant config is invalid", ErrorResponse),
      },
    }),
    async (c) => {
      const user = c.get("user");
      if (!user) {
        return unauthorizedResponse(c);
      }

      const tenantId = c.req.param("tenantId");

      const tenantRow = await db.query.tenant.findFirst({
        where: eq(tenant.id, tenantId),
      });
      if (!tenantRow) {
        return errorResponse(c, "not_found", "Tenant not found");
      }

      const membership = await db.query.principal.findFirst({
        where: and(
          eq(principal.tenantId, tenantId),
          eq(principal.kind, "user"),
          eq(principal.refId, user.id),
        ),
      });
      if (!membership) {
        return errorResponse(c, "forbidden", "Not a member of this tenant");
      }

      try {
        return c.json(formatTenant(tenantRow));
      } catch (err) {
        if (!(err instanceof TenantConfigInvalidError)) throw err;
        return errorResponse(c, "invalid_tenant_config", err.message);
      }
    },
  );

  app.patch(
    "/:tenantId",
    describeRoute({
      tags: ["Tenants"],
      summary: "Update tenant config",
      description:
        "Merges `config` by top-level key: keys the request omits keep their stored values, and `null` removes a key. Requires admin or higher grant within the tenant.",
      responses: {
        200: jsonResponse("Tenant updated", TenantResponse),
        400: jsonResponse(
          "Invalid config or inherited limit exceeded",
          ErrorResponse,
        ),
        403: jsonResponse("Insufficient grants", ErrorResponse),
        404: jsonResponse("Tenant not found", ErrorResponse),
        409: jsonResponse(
          "A stored config the tenant inherits is invalid",
          ErrorResponse,
        ),
      },
    }),
    validator("json", UpdateTenant),
    async (c) => {
      const user = c.get("user");
      if (!user) {
        return unauthorizedResponse(c);
      }

      const tenantId = c.req.param("tenantId");
      const body = c.req.valid("json");

      const membership = await db.query.principal.findFirst({
        where: and(
          eq(principal.tenantId, tenantId),
          eq(principal.kind, "user"),
          eq(principal.refId, user.id),
          eq(principal.status, "active"),
        ),
      });
      if (!membership) {
        return errorResponse(c, "forbidden", "Not a member of this tenant");
      }

      const authorization = await authorize(
        grantStore,
        membership.id,
        tenantId,
        `tenant:${tenantId}`,
        "manage",
        conditionRegistry,
      );
      if (authorization.effect !== "allow") {
        return errorResponse(
          c,
          "forbidden",
          "You do not have permission to manage this tenant",
        );
      }
      const result = await db.transaction(async (tx) => {
        // A concurrent update waits here, then merges into the committed result.
        const [current] = await tx
          .select({ parentId: tenant.parentId, config: tenant.config })
          .from(tenant)
          .where(eq(tenant.id, tenantId))
          .for("no key update");
        if (current === undefined) return { kind: "not_found" } as const;

        const updates: Record<string, unknown> = { updatedAt: new Date() };
        if (body.name !== undefined) updates["name"] = body.name;
        const lifecycle = body.config?.lifecycle;
        if (lifecycle != null) {
          let resolved;
          try {
            resolved = await validateLifecyclePolicyEdit(
              tx,
              current.parentId,
              lifecycle,
            );
          } catch (err) {
            if (!(err instanceof TenantConfigInvalidError)) throw err;
            return { kind: "inherited_invalid", error: err } as const;
          }
          if (!resolved.ok)
            return {
              kind: "invalid",
              message: `${resolved.field} exceeds inherited limit`,
            } as const;
        }
        // A stored key this update leaves in place can predate validation,
        // and the response returns the whole config.
        const config = TenantConfig(
          mergeTenantConfig(current.config, body.config ?? {}),
        );
        if (config instanceof type.errors)
          return {
            kind: "invalid",
            message: `Tenant config would be invalid after this update: ${config.summary}`,
          } as const;
        if (body.config !== undefined) updates["config"] = config;

        const [updated] = await tx
          .update(tenant)
          .set(updates)
          .where(eq(tenant.id, tenantId))
          .returning();
        return updated === undefined
          ? ({ kind: "not_found" } as const)
          : ({ kind: "updated", row: updated } as const);
      });

      if (result.kind === "not_found")
        return errorResponse(c, "not_found", "Tenant not found");
      if (result.kind === "invalid")
        return errorResponse(c, "bad_request", result.message);
      if (result.kind === "inherited_invalid")
        return tenantConfigErrorResponse(c, result.error);
      return c.json(formatTenant(result.row));
    },
  );

  return app;
}
