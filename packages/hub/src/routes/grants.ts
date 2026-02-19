import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { authorize } from "@interchange/authz";
import { grant, principal } from "@interchange/db/schema";
import {
  CreateGrant,
  UpdateGrant,
  GrantResponse,
  EvaluateRequest,
  EvaluateResult,
  ErrorResponse,
  paginatedSchema,
} from "@interchange/types";

import type { TenantEnv } from "../context";
import { first, ts } from "../format";
import { generateId } from "../ids";
import { requireGrant, idResource } from "../middleware/grant";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";

type ResolvedNames = {
  roleNames: Map<string, string>;
  principalNames: Map<string, string>;
};

function formatGrant(row: typeof grant.$inferSelect, names?: ResolvedNames) {
  return {
    id: row.id,
    tenantId: row.tenantId,
    roleId: row.roleId ?? null,
    roleName: (row.roleId && names?.roleNames.get(row.roleId)) ?? null,
    principalId: row.principalId ?? null,
    principalName:
      (row.principalId && names?.principalNames.get(row.principalId)) ?? null,
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

async function resolveGrantNames(
  db: TenantEnv["Variables"]["db"],
  grants: (typeof grant.$inferSelect)[],
): Promise<ResolvedNames> {
  const roleIds = [
    ...new Set(
      grants.map((g) => g.roleId).filter((id): id is string => id !== null),
    ),
  ];
  const principalIds = [
    ...new Set(
      grants
        .map((g) => g.principalId)
        .filter((id): id is string => id !== null),
    ),
  ];

  const roleNames = new Map<string, string>();
  if (roleIds.length > 0) {
    const roles = await db.query.role.findMany({
      where: (r, { inArray }) => inArray(r.id, roleIds),
    });
    for (const r of roles) {
      roleNames.set(r.id, r.name);
    }
  }

  const principalNames = new Map<string, string>();
  if (principalIds.length > 0) {
    const principals = await db.query.principal.findMany({
      where: (p, { inArray }) => inArray(p.id, principalIds),
    });

    const userRefIds = principals
      .filter((p) => p.kind === "user")
      .map((p) => p.refId);
    const agentRefIds = principals
      .filter((p) => p.kind === "agent")
      .map((p) => p.refId);

    const refToName = new Map<string, string>();

    if (userRefIds.length > 0) {
      const users = await db.query.user.findMany({
        where: (u, { inArray }) => inArray(u.id, userRefIds),
      });
      for (const u of users) {
        refToName.set(u.id, u.name);
      }
    }

    if (agentRefIds.length > 0) {
      const agents = await db.query.agent.findMany({
        where: (a, { inArray }) => inArray(a.id, agentRefIds),
      });
      for (const a of agents) {
        refToName.set(a.id, a.name);
      }
    }

    for (const p of principals) {
      const name = refToName.get(p.refId);
      if (name) principalNames.set(p.id, name);
    }
  }

  return { roleNames, principalNames };
}

const app = new Hono<TenantEnv>();

app.get(
  "/",
  requireGrant("grant:*", "read"),
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
      ...pageParameters,
    ],
    responses: {
      200: {
        description: "List of grants",
        content: {
          "application/json": {
            schema: resolver(paginatedSchema(GrantResponse)),
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
    const { limit, cursor } = parsePageParams({
      cursor: c.req.query("cursor"),
      limit: c.req.query("limit"),
    });

    const conditions = [eq(grant.tenantId, tenantCtx.id)];
    if (principalId) conditions.push(eq(grant.principalId, principalId));
    if (roleId) conditions.push(eq(grant.roleId, roleId));
    if (resource) conditions.push(eq(grant.resource, resource));
    if (effect === "allow" || effect === "deny" || effect === "ask") {
      conditions.push(eq(grant.effect, effect));
    }
    if (cursor) {
      conditions.push(cursorCondition(grant.createdAt, grant.id, cursor));
    }

    const rows = await db.query.grant.findMany({
      where: and(...conditions),
      orderBy: pageOrder(grant.createdAt, grant.id),
      limit,
    });

    const names = await resolveGrantNames(db, rows);
    return c.json(
      paginatedResponse(
        rows.map((g) => formatGrant(g, names)),
        rows,
        limit,
      ),
    );
  },
);

app.post(
  "/",
  requireGrant("grant:*", "create"),
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
  requireGrant(idResource("grant", "grantId"), "read"),
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
  requireGrant(idResource("grant", "grantId"), "manage"),
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
  requireGrant(idResource("grant", "grantId"), "manage"),
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
    const grantStore = c.get("grantStore");
    const conditionRegistry = c.get("conditionRegistry");

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

    const result = await authorize(
      grantStore,
      principalId,
      tenantCtx.id,
      body.resource,
      body.action,
      conditionRegistry,
    );

    return c.json({
      effect: result.effect ?? "deny",
      matchingGrants: result.matchingGrants.map((g) => ({
        id: g.id,
        resource: g.resource,
        action: g.action,
        effect: g.effect,
        source: g.source,
      })),
    });
  },
);

export { evaluateApp as evaluateRoutes };
