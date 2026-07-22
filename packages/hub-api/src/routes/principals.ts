import { eq, ne, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { principal, principalRole, role, user } from "@intx/db/schema";
import { parsePrincipalRow } from "@intx/db";
import type { DB } from "@intx/db";
import {
  PrincipalResponse,
  UpdatePrincipal,
  InviteMember,
  ErrorResponse,
  paginatedSchema,
} from "@intx/types";

import type { TenantEnv } from "../context";
import { first, ts } from "../format";
import { generateId } from "@intx/hub-common";
import { idResource } from "../middleware/grant";
import type { RequireGrant } from "../middleware/grant";
import { resolveWorkflowPrincipalNames } from "./workflow-principal-name";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";

type ResolvedIdentity = { displayName: string; email?: string };

function formatPrincipal(
  row: typeof principal.$inferSelect,
  roles: { id: string; name: string }[],
  identity?: ResolvedIdentity,
) {
  const parsed = parsePrincipalRow(row);
  return {
    id: parsed.id,
    tenantId: parsed.tenantId,
    kind: parsed.kind,
    refId: parsed.refId,
    displayName: identity?.displayName ?? parsed.refId,
    ...(identity?.email ? { email: identity.email } : {}),
    status: parsed.status,
    roles,
    createdAt: ts(parsed.createdAt),
    updatedAt: ts(parsed.updatedAt),
  };
}

async function resolveIdentities(
  db: DB["db"],
  principals: (typeof principal.$inferSelect)[],
): Promise<Map<string, ResolvedIdentity>> {
  const identities = new Map<string, ResolvedIdentity>();

  const userRefIds = principals
    .filter((p) => p.kind === "user")
    .map((p) => p.refId);
  const agentRefIds = principals
    .filter((p) => p.kind === "agent")
    .map((p) => p.refId);
  const workflowRefIds = principals
    .filter((p) => p.kind === "workflow")
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
    // First pass: resolve definition principals (refId = agent.id)
    const agents = await db.query.agent.findMany({
      where: (a, { inArray }) => inArray(a.id, agentRefIds),
    });
    for (const a of agents) {
      identities.set(a.id, { displayName: a.name });
    }

    // Second pass: resolve instance principals (refId = agentInstance.id)
    const unresolvedRefIds = agentRefIds.filter((id) => !identities.has(id));
    if (unresolvedRefIds.length > 0) {
      const instances = await db.query.agentInstance.findMany({
        where: (i, { inArray }) => inArray(i.id, unresolvedRefIds),
      });
      const definitionIds = [...new Set(instances.map((i) => i.agentId))];
      const definitions =
        definitionIds.length > 0
          ? await db.query.agent.findMany({
              where: (a, { inArray }) => inArray(a.id, definitionIds),
            })
          : [];
      const defNames = new Map(definitions.map((d) => [d.id, d.name]));
      for (const inst of instances) {
        const name = defNames.get(inst.agentId);
        if (name) {
          identities.set(inst.id, { displayName: `${name} (instance)` });
        }
      }
    }
  }

  if (workflowRefIds.length > 0) {
    // A workflow principal's refId is its run id; the run's deployment address
    // is the only human-facing label it has, reached by joining the runId
    // through workflow_run to workflow_deployment.
    const workflowNames = await resolveWorkflowPrincipalNames(
      db,
      workflowRefIds,
    );
    for (const [runId, displayName] of workflowNames) {
      identities.set(runId, { displayName });
    }
  }

  return identities;
}

async function loadRolesForPrincipal(db: DB["db"], principalId: string) {
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

export type CreatePrincipalRoutesDeps = {
  db: DB["db"];
  requireGrant: RequireGrant;
};

export function createPrincipalRoutes({
  db,
  requireGrant,
}: CreatePrincipalRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  app.get(
    "/",
    requireGrant("principal:*", "read"),
    describeRoute({
      tags: ["Principals"],
      summary: "List principals in the tenant",
      description:
        "Lists all principals (users, agents, and workflow deployments) in the tenant. Filterable by kind and status.",
      parameters: [
        {
          name: "kind",
          in: "query",
          schema: { type: "string", enum: ["user", "agent", "workflow"] },
        },
        {
          name: "status",
          in: "query",
          schema: {
            type: "string",
            enum: ["active", "suspended", "invited", "deactivated"],
          },
        },
        ...pageParameters,
      ],
      responses: {
        200: {
          description: "List of principals",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(PrincipalResponse)),
            },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const kind = c.req.query("kind");
      const status = c.req.query("status");
      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      const conditions = [eq(principal.tenantId, tenantCtx.id)];
      if (kind === "user" || kind === "agent" || kind === "workflow") {
        conditions.push(eq(principal.kind, kind));
      }
      if (
        status === "active" ||
        status === "suspended" ||
        status === "invited" ||
        status === "deactivated"
      ) {
        conditions.push(eq(principal.status, status));
      } else {
        // Exclude deactivated principals by default
        conditions.push(ne(principal.status, "deactivated"));
      }
      if (cursor) {
        conditions.push(
          cursorCondition(principal.createdAt, principal.id, cursor),
        );
      }

      const rows = await db.query.principal.findMany({
        where: and(...conditions),
        orderBy: pageOrder(principal.createdAt, principal.id),
        limit,
      });

      const allAssignments =
        rows.length > 0
          ? await db.query.principalRole.findMany({
              where: (pr, { inArray }) =>
                inArray(
                  pr.principalId,
                  rows.map((p) => p.id),
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

      const rolesByPrincipal = new Map<
        string,
        { id: string; name: string }[]
      >();
      for (const a of allAssignments) {
        const r = roleMap.get(a.roleId);
        if (!r) continue;
        const list = rolesByPrincipal.get(a.principalId) ?? [];
        list.push({ id: r.id, name: r.name });
        rolesByPrincipal.set(a.principalId, list);
      }

      const identities = await resolveIdentities(db, rows);

      const items = rows.map((p) =>
        formatPrincipal(
          p,
          rolesByPrincipal.get(p.id) ?? [],
          identities.get(p.refId),
        ),
      );

      return c.json(paginatedResponse(items, rows, limit));
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
      const body = c.req.valid("json");

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

  return app;
}

// Invite is mounted separately at ../members/invite in app.ts
export type CreateInviteRoutesDeps = {
  db: DB["db"];
  requireGrant: RequireGrant;
};

export function createInviteRoutes({
  db,
  requireGrant,
}: CreateInviteRoutesDeps): Hono<TenantEnv> {
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
      const body = c.req.valid("json");

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

  return inviteApp;
}
