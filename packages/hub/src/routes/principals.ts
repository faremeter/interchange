import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { principal, principalRole, role, user } from "@interchange/db/schema";
import {
  PrincipalResponse,
  UpdatePrincipal,
  InviteMember,
  ErrorResponse,
} from "@interchange/types";

import type { TenantEnv } from "../context";
import { first, ts } from "../format";
import { generateId } from "../ids";
import { requireGrant, idResource } from "../middleware/grant";

type ResolvedIdentity = { displayName: string; email?: string };

function formatPrincipal(
  row: typeof principal.$inferSelect,
  roles: { id: string; name: string }[],
  identity?: ResolvedIdentity,
) {
  return {
    id: row.id,
    tenantId: row.tenantId,
    kind: row.kind as "user" | "agent",
    refId: row.refId,
    displayName: identity?.displayName ?? row.refId,
    ...(identity?.email ? { email: identity.email } : {}),
    status: row.status as "active" | "suspended" | "invited" | "deactivated",
    roles,
    createdAt: ts(row.createdAt),
    updatedAt: ts(row.updatedAt),
  };
}

async function resolveIdentities(
  db: TenantEnv["Variables"]["db"],
  principals: (typeof principal.$inferSelect)[],
): Promise<Map<string, ResolvedIdentity>> {
  const identities = new Map<string, ResolvedIdentity>();

  const userRefIds = principals
    .filter((p) => p.kind === "user")
    .map((p) => p.refId);
  const agentRefIds = principals
    .filter((p) => p.kind === "agent")
    .map((p) => p.refId);

  if (userRefIds.length > 0) {
    const users = await db.query.user.findMany({
      where: (u, { inArray }) => inArray(u.id, userRefIds),
    });
    for (const u of users) {
      identities.set(u.id, { displayName: u.name, email: u.email });
    }
  }

  if (agentRefIds.length > 0) {
    const agents = await db.query.agent.findMany({
      where: (a, { inArray }) => inArray(a.id, agentRefIds),
    });
    for (const a of agents) {
      identities.set(a.id, { displayName: a.name });
    }
  }

  return identities;
}

async function loadRolesForPrincipal(
  db: TenantEnv["Variables"]["db"],
  principalId: string,
) {
  const assignments = await db.query.principalRole.findMany({
    where: eq(principalRole.principalId, principalId),
  });
  if (assignments.length === 0) return [];

  const roleIds = assignments.map((a) => a.roleId);
  const roles = await db.query.role.findMany({
    where: (r, { inArray }) => inArray(r.id, roleIds),
  });
  return roles.map((r) => ({ id: r.id, name: r.name }));
}

const app = new Hono<TenantEnv>();

app.get(
  "/",
  requireGrant("principal:*", "read"),
  describeRoute({
    tags: ["Principals"],
    summary: "List principals in the tenant",
    description:
      "Lists all principals (users and agents) in the tenant. Filterable by kind and status.",
    parameters: [
      {
        name: "kind",
        in: "query",
        schema: { type: "string", enum: ["user", "agent"] },
      },
      {
        name: "status",
        in: "query",
        schema: {
          type: "string",
          enum: ["active", "suspended", "invited", "deactivated"],
        },
      },
    ],
    responses: {
      200: {
        description: "List of principals",
        content: {
          "application/json": {
            schema: resolver(PrincipalResponse.array()),
          },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const db = c.get("db");
    const kind = c.req.query("kind");
    const status = c.req.query("status");

    const conditions = [eq(principal.tenantId, tenantCtx.id)];
    if (kind === "user" || kind === "agent") {
      conditions.push(eq(principal.kind, kind));
    }
    if (
      status === "active" ||
      status === "suspended" ||
      status === "invited" ||
      status === "deactivated"
    ) {
      conditions.push(eq(principal.status, status));
    }

    const principals = await db.query.principal.findMany({
      where: and(...conditions),
    });

    const allAssignments =
      principals.length > 0
        ? await db.query.principalRole.findMany({
            where: (pr, { inArray }) =>
              inArray(
                pr.principalId,
                principals.map((p) => p.id),
              ),
          })
        : [];

    const roleIds = [...new Set(allAssignments.map((a) => a.roleId))];
    const roles =
      roleIds.length > 0
        ? await db.query.role.findMany({
            where: (r, { inArray }) => inArray(r.id, roleIds),
          })
        : [];
    const roleMap = new Map(roles.map((r) => [r.id, r]));

    const rolesByPrincipal = new Map<string, { id: string; name: string }[]>();
    for (const a of allAssignments) {
      const r = roleMap.get(a.roleId);
      if (!r) continue;
      const list = rolesByPrincipal.get(a.principalId) ?? [];
      list.push({ id: r.id, name: r.name });
      rolesByPrincipal.set(a.principalId, list);
    }

    const identities = await resolveIdentities(db, principals);

    const results = principals.map((p) =>
      formatPrincipal(
        p,
        rolesByPrincipal.get(p.id) ?? [],
        identities.get(p.refId),
      ),
    );

    return c.json(results);
  },
);

app.get(
  "/:principalId",
  requireGrant(idResource("principal", "principalId"), "read"),
  describeRoute({
    tags: ["Principals"],
    summary: "Get principal details",
    description:
      "Returns principal details including kind, status, assigned roles, and effective grants.",
    responses: {
      200: {
        description: "Principal details",
        content: {
          "application/json": { schema: resolver(PrincipalResponse) },
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
  async (c) => {
    const tenantCtx = c.get("tenant");
    const principalId = c.req.param("principalId");
    const db = c.get("db");

    const row = await db.query.principal.findFirst({
      where: and(
        eq(principal.id, principalId),
        eq(principal.tenantId, tenantCtx.id),
      ),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Principal not found" } },
        404,
      );
    }

    const roles = await loadRolesForPrincipal(db, principalId);
    const identities = await resolveIdentities(db, [row]);
    return c.json(formatPrincipal(row, roles, identities.get(row.refId)));
  },
);

app.patch(
  "/:principalId",
  requireGrant(idResource("principal", "principalId"), "manage"),
  describeRoute({
    tags: ["Principals"],
    summary: "Update principal status",
    description: "Activate, suspend, or deactivate a principal.",
    responses: {
      200: {
        description: "Principal updated",
        content: {
          "application/json": { schema: resolver(PrincipalResponse) },
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
  validator("json", UpdatePrincipal),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const principalId = c.req.param("principalId");
    const body = c.req.valid("json" as never) as typeof UpdatePrincipal.infer;
    const db = c.get("db");

    const [updated] = await db
      .update(principal)
      .set({ status: body.status, updatedAt: new Date() })
      .where(
        and(
          eq(principal.id, principalId),
          eq(principal.tenantId, tenantCtx.id),
        ),
      )
      .returning();

    if (!updated) {
      return c.json(
        { error: { code: "not_found", message: "Principal not found" } },
        404,
      );
    }

    const roles = await loadRolesForPrincipal(db, principalId);
    const identities = await resolveIdentities(db, [updated]);
    return c.json(
      formatPrincipal(updated, roles, identities.get(updated.refId)),
    );
  },
);

app.delete(
  "/:principalId",
  requireGrant(idResource("principal", "principalId"), "manage"),
  describeRoute({
    tags: ["Principals"],
    summary: "Remove principal from tenant",
    description:
      "Removes a user or agent principal from the tenant. For agents, use agent deletion instead.",
    responses: {
      204: {
        description: "Principal removed",
      },
      403: {
        description: "Insufficient grants",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const principalId = c.req.param("principalId");
    const db = c.get("db");

    const deleted = await db
      .delete(principal)
      .where(
        and(
          eq(principal.id, principalId),
          eq(principal.tenantId, tenantCtx.id),
        ),
      )
      .returning();

    if (deleted.length === 0) {
      return c.json(
        { error: { code: "not_found", message: "Principal not found" } },
        404,
      );
    }

    return c.body(null, 204);
  },
);

export { app as principalRoutes };

// Invite is mounted separately at ../members/invite in app.ts
const inviteApp = new Hono<TenantEnv>();

inviteApp.post(
  "/",
  requireGrant("principal:*", "create"),
  describeRoute({
    tags: ["Principals"],
    summary: "Invite a user to the tenant",
    description:
      "Invites a user by email. Creates a principal with invited status and optionally assigns a role.",
    responses: {
      201: {
        description: "Invitation sent",
        content: {
          "application/json": { schema: resolver(PrincipalResponse) },
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
  validator("json", InviteMember),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const body = c.req.valid("json" as never) as typeof InviteMember.infer;
    const db = c.get("db");

    const invitedUser = await db.query.user.findFirst({
      where: eq(user.email, body.email),
    });

    if (!invitedUser) {
      return c.json(
        {
          error: {
            code: "not_found",
            message: "No user found with that email",
          },
        },
        404,
      );
    }

    const existing = await db.query.principal.findFirst({
      where: and(
        eq(principal.tenantId, tenantCtx.id),
        eq(principal.kind, "user"),
        eq(principal.refId, invitedUser.id),
      ),
    });

    if (existing) {
      return c.json(
        {
          error: {
            code: "conflict",
            message: "User is already a member of this tenant",
          },
        },
        409,
      );
    }

    const now = new Date();
    const principalId = generateId("principal");

    const row = first(
      await db
        .insert(principal)
        .values({
          id: principalId,
          tenantId: tenantCtx.id,
          kind: "user",
          refId: invitedUser.id,
          status: "invited",
          createdAt: now,
          updatedAt: now,
        })
        .returning(),
    );

    let roles: { id: string; name: string }[] = [];

    if (body.roleId) {
      const roleRow = await db.query.role.findFirst({
        where: and(eq(role.id, body.roleId), eq(role.tenantId, tenantCtx.id)),
      });
      if (roleRow) {
        await db.insert(principalRole).values({
          principalId,
          roleId: roleRow.id,
          createdAt: now,
        });
        roles = [{ id: roleRow.id, name: roleRow.name }];
      }
    }

    const identity: ResolvedIdentity = {
      displayName: invitedUser.name,
      email: invitedUser.email,
    };
    return c.json(formatPrincipal(row, roles, identity), 201);
  },
);

export { inviteApp as inviteRoutes };
