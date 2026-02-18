import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { grant, principal, principalRole } from "@interchange/db/schema";
import {
  CreateGrant,
  UpdateGrant,
  GrantResponse,
  EvaluateRequest,
  EvaluateResult,
  ErrorResponse,
} from "@interchange/types";

import type { TenantEnv } from "../context";
import { first, ts } from "../format";
import { generateId } from "../ids";

function formatGrant(row: typeof grant.$inferSelect) {
  return {
    id: row.id,
    tenantId: row.tenantId,
    roleId: row.roleId ?? null,
    principalId: row.principalId ?? null,
    resource: row.resource,
    action: row.action,
    effect: row.effect as "allow" | "deny" | "ask",
    conditions: (row.conditions as Record<string, unknown> | null) ?? null,
    source: row.source as "system" | "role" | "creator" | "invoker",
    expiresAt: row.expiresAt ? ts(row.expiresAt) : null,
    createdAt: ts(row.createdAt),
    updatedAt: ts(row.updatedAt),
  };
}

const app = new Hono<TenantEnv>();

app.get(
  "/",
  describeRoute({
    tags: ["Grants"],
    summary: "List capability grants in the tenant",
    description:
      "Lists all capability grants. Filterable by principalId, roleId, resource pattern, and effect.",
    parameters: [
      { name: "principalId", in: "query", schema: { type: "string" } },
      { name: "roleId", in: "query", schema: { type: "string" } },
      { name: "resource", in: "query", schema: { type: "string" } },
      {
        name: "effect",
        in: "query",
        schema: { type: "string", enum: ["allow", "deny", "ask"] },
      },
    ],
    responses: {
      200: {
        description: "List of grants",
        content: {
          "application/json": {
            schema: resolver(GrantResponse.array()),
          },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const db = c.get("db");

    const principalId = c.req.query("principalId");
    const roleId = c.req.query("roleId");
    const resource = c.req.query("resource");
    const effect = c.req.query("effect");

    const conditions = [eq(grant.tenantId, tenantCtx.id)];
    if (principalId) conditions.push(eq(grant.principalId, principalId));
    if (roleId) conditions.push(eq(grant.roleId, roleId));
    if (resource) conditions.push(eq(grant.resource, resource));
    if (effect === "allow" || effect === "deny" || effect === "ask") {
      conditions.push(eq(grant.effect, effect));
    }

    const grants = await db.query.grant.findMany({
      where: and(...conditions),
    });

    return c.json(grants.map(formatGrant));
  },
);

app.post(
  "/",
  describeRoute({
    tags: ["Grants"],
    summary: "Create a capability grant",
    description:
      "Creates a grant targeting either a role or a principal directly. Exactly one of roleId or principalId must be provided.",
    responses: {
      201: {
        description: "Grant created",
        content: {
          "application/json": { schema: resolver(GrantResponse) },
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
  validator("json", CreateGrant),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const body = c.req.valid("json" as never) as typeof CreateGrant.infer;
    const db = c.get("db");

    if (!body.roleId && !body.principalId) {
      return c.json(
        {
          error: {
            code: "bad_request",
            message: "Either roleId or principalId must be provided",
          },
        },
        400,
      );
    }

    const now = new Date();
    const row = first(
      await db
        .insert(grant)
        .values({
          id: generateId("grant"),
          tenantId: tenantCtx.id,
          roleId: body.roleId ?? null,
          principalId: body.principalId ?? null,
          resource: body.resource,
          action: body.action,
          effect: body.effect,
          conditions: body.conditions ?? null,
          source: body.source,
          expiresAt: body.expiresAt ? new Date(body.expiresAt) : null,
          createdAt: now,
          updatedAt: now,
        })
        .returning(),
    );

    return c.json(formatGrant(row), 201);
  },
);

app.get(
  "/:grantId",
  describeRoute({
    tags: ["Grants"],
    summary: "Get grant details",
    responses: {
      200: {
        description: "Grant details",
        content: {
          "application/json": { schema: resolver(GrantResponse) },
        },
      },
      404: {
        description: "Grant not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const grantId = c.req.param("grantId");
    const db = c.get("db");

    const row = await db.query.grant.findFirst({
      where: and(eq(grant.id, grantId), eq(grant.tenantId, tenantCtx.id)),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Grant not found" } },
        404,
      );
    }

    return c.json(formatGrant(row));
  },
);

app.patch(
  "/:grantId",
  describeRoute({
    tags: ["Grants"],
    summary: "Update a grant",
    description: "Update effect, conditions, or expiry on an existing grant.",
    responses: {
      200: {
        description: "Grant updated",
        content: {
          "application/json": { schema: resolver(GrantResponse) },
        },
      },
      404: {
        description: "Grant not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", UpdateGrant),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const grantId = c.req.param("grantId");
    const body = c.req.valid("json" as never) as typeof UpdateGrant.infer;
    const db = c.get("db");

    const updates: Record<string, unknown> = { updatedAt: new Date() };
    if (body.effect !== undefined) updates["effect"] = body.effect;
    if (body.conditions !== undefined) updates["conditions"] = body.conditions;
    if (body.expiresAt !== undefined) {
      updates["expiresAt"] = body.expiresAt ? new Date(body.expiresAt) : null;
    }

    const [updated] = await db
      .update(grant)
      .set(updates)
      .where(and(eq(grant.id, grantId), eq(grant.tenantId, tenantCtx.id)))
      .returning();

    if (!updated) {
      return c.json(
        { error: { code: "not_found", message: "Grant not found" } },
        404,
      );
    }

    return c.json(formatGrant(updated));
  },
);

app.delete(
  "/:grantId",
  describeRoute({
    tags: ["Grants"],
    summary: "Revoke a grant",
    responses: {
      204: {
        description: "Grant revoked",
      },
      404: {
        description: "Grant not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const grantId = c.req.param("grantId");
    const db = c.get("db");

    const deleted = await db
      .delete(grant)
      .where(and(eq(grant.id, grantId), eq(grant.tenantId, tenantCtx.id)))
      .returning();

    if (deleted.length === 0) {
      return c.json(
        { error: { code: "not_found", message: "Grant not found" } },
        404,
      );
    }

    return c.body(null, 204);
  },
);

export { app as grantRoutes };

// Evaluate endpoint is mounted under principals
const evaluateApp = new Hono<TenantEnv>();

evaluateApp.post(
  "/",
  describeRoute({
    tags: ["Grants"],
    summary: "Evaluate grants for a principal",
    description:
      "Evaluates what would happen if a principal attempted an operation. Returns the resolved effect and all matching grants. Useful for debugging authorization.",
    responses: {
      200: {
        description: "Evaluation result",
        content: {
          "application/json": { schema: resolver(EvaluateResult) },
        },
      },
      404: {
        description: "Principal not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", EvaluateRequest),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const principalId = c.req.param("principalId") ?? "";
    const body = c.req.valid("json" as never) as typeof EvaluateRequest.infer;
    const db = c.get("db");

    const principalRow = await db.query.principal.findFirst({
      where: and(
        eq(principal.id, principalId),
        eq(principal.tenantId, tenantCtx.id),
      ),
    });

    if (!principalRow) {
      return c.json(
        { error: { code: "not_found", message: "Principal not found" } },
        404,
      );
    }

    // Collect role IDs for the principal
    const roleAssignments = await db.query.principalRole.findMany({
      where: eq(principalRole.principalId, principalId),
    });
    const roleIds = roleAssignments.map((a) => a.roleId);

    // Find all matching grants: direct principal grants + role-based grants
    const allGrants = await db.query.grant.findMany({
      where: eq(grant.tenantId, tenantCtx.id),
    });

    const now = new Date();
    const matching = allGrants.filter((g) => {
      // Skip expired grants
      if (g.expiresAt && g.expiresAt < now) return false;

      // Must be relevant to this principal (direct or via role)
      const isDirectGrant = g.principalId === principalId;
      const isRoleGrant = g.roleId !== null && roleIds.includes(g.roleId);
      if (!isDirectGrant && !isRoleGrant) return false;

      // Match resource (wildcard or exact)
      if (g.resource !== "*" && g.resource !== body.resource) return false;

      // Match action (wildcard or exact)
      if (g.action !== "*" && g.action !== body.action) return false;

      return true;
    });

    // Resolve effect: deny > ask > allow
    let resolved: "allow" | "deny" | "ask" = "deny";
    if (matching.some((g) => g.effect === "deny")) {
      resolved = "deny";
    } else if (matching.some((g) => g.effect === "ask")) {
      resolved = "ask";
    } else if (matching.some((g) => g.effect === "allow")) {
      resolved = "allow";
    }

    return c.json({
      effect: resolved,
      matchingGrants: matching.map((g) => ({
        id: g.id,
        resource: g.resource,
        action: g.action,
        effect: g.effect as "allow" | "deny" | "ask",
        source: g.source as "system" | "role" | "creator" | "invoker",
      })),
    });
  },
);

export { evaluateApp as evaluateRoutes };
