import { type SQL, eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver } from "hono-openapi";

import { agentInstance, principal } from "@intx/db/schema";
import { parsePrincipalRow } from "@intx/db";
import type { DB } from "@intx/db";
import {
  UserProfile,
  PrincipalSummary,
  InstanceSummary,
  SessionSummary,
  ApprovalSummary,
  ErrorResponse,
  paginatedSchema,
} from "@intx/types";

import type { AppEnv } from "../context";
import { ts } from "../format";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";

export type CreateMeRoutesDeps = {
  db: DB["db"];
};

export function createMeRoutes({ db }: CreateMeRoutesDeps): Hono<AppEnv> {
  const app = new Hono<AppEnv>();

  app.get(
    "/",
    describeRoute({
      tags: ["User"],
      summary: "Get current user profile",
      responses: {
        200: {
          description: "User profile",
          content: {
            "application/json": { schema: resolver(UserProfile) },
          },
        },
        401: {
          description: "Not authenticated",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    (c) => {
      const user = c.get("user");
      if (!user) {
        return c.json(
          {
            error: { code: "unauthorized", message: "Authentication required" },
          },
          401,
        );
      }
      return c.json({
        id: user.id,
        name: user.name,
        email: user.email,
        emailVerified: user.emailVerified,
        image: user.image ?? null,
        createdAt: ts(user.createdAt),
        updatedAt: ts(user.updatedAt),
      });
    },
  );

  app.get(
    "/principals",
    describeRoute({
      tags: ["User"],
      summary: "List principals across all tenants",
      description:
        "Returns all of the authenticated user's principals across tenants, with tenant name, roles, and status in each.",
      parameters: [...pageParameters],
      responses: {
        200: {
          description: "List of principals across tenants",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(PrincipalSummary)),
            },
          },
        },
        401: {
          description: "Not authenticated",
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
      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      const conditions = [
        eq(principal.kind, "user"),
        eq(principal.refId, user.id),
      ];
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

      const tenantIds = rows.map((p) => p.tenantId);
      const tenants =
        tenantIds.length > 0
          ? await db.query.tenant.findMany({
              where: (t, { inArray }) => inArray(t.id, tenantIds),
            })
          : [];
      const tenantMap = new Map(tenants.map((t) => [t.id, t]));

      const principalIds = rows.map((p) => p.id);
      const assignments =
        principalIds.length > 0
          ? await db.query.principalRole.findMany({
              where: (pr, { inArray }) => inArray(pr.principalId, principalIds),
            })
          : [];

      const roleIds = [...new Set(assignments.map((a) => a.roleId))];
      const roles =
        roleIds.length > 0
          ? await db.query.role.findMany({
              where: (r, { inArray }) => inArray(r.id, roleIds),
            })
          : [];
      const roleMap = new Map(roles.map((r) => [r.id, r]));

      const assignmentsByPrincipal = new Map<
        string,
        { id: string; name: string }[]
      >();
      for (const a of assignments) {
        const r = roleMap.get(a.roleId);
        if (!r) continue;
        const list = assignmentsByPrincipal.get(a.principalId) ?? [];
        list.push({ id: r.id, name: r.name });
        assignmentsByPrincipal.set(a.principalId, list);
      }

      const items = rows.map((row) => {
        const p = parsePrincipalRow(row);
        const t = tenantMap.get(p.tenantId);
        return {
          principalId: p.id,
          tenantId: p.tenantId,
          tenantName: t?.name ?? "Unknown",
          tenantSlug: t?.slug ?? "unknown",
          kind: p.kind,
          status: p.status,
          roles: assignmentsByPrincipal.get(p.id) ?? [],
        };
      });

      return c.json(paginatedResponse(items, rows, limit));
    },
  );

  app.get(
    "/instances",
    describeRoute({
      tags: ["User"],
      summary: "List running agent instances across all tenants",
      description:
        "Aggregates running agent instances from all tenants the user belongs to. Each result is tagged with tenantId.",
      parameters: [...pageParameters],
      responses: {
        200: {
          description: "Instances across tenants",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(InstanceSummary)),
            },
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
      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      const principals = await db.query.principal.findMany({
        where: and(eq(principal.kind, "user"), eq(principal.refId, user.id)),
      });

      const tenantIds = principals.map((p) => p.tenantId);
      if (tenantIds.length === 0) {
        return c.json({ data: [], nextCursor: null });
      }

      const tenants = await db.query.tenant.findMany({
        where: (t, { inArray }) => inArray(t.id, tenantIds),
      });
      const tenantMap = new Map(tenants.map((t) => [t.id, t]));

      const conditions: SQL[] = [eq(agentInstance.status, "running")];
      if (cursor) {
        conditions.push(
          cursorCondition(agentInstance.createdAt, agentInstance.id, cursor),
        );
      }

      const rows = await db.query.agentInstance.findMany({
        where: (ai, { inArray }) =>
          and(inArray(ai.tenantId, tenantIds), ...conditions),
        orderBy: pageOrder(agentInstance.createdAt, agentInstance.id),
        limit,
      });

      // A running instance's agent_id is the legacy agent id; its folded
      // definition (id + display name) is keyed by origin_agent_id.
      const agentIds = [...new Set(rows.map((r) => r.agentId))];
      const definitions =
        agentIds.length > 0
          ? await db.query.workflowDefinition.findMany({
              where: (d, { inArray }) => inArray(d.originAgentId, agentIds),
            })
          : [];
      const defByAgentId = new Map(
        definitions.flatMap((d) =>
          d.originAgentId === null
            ? []
            : [[d.originAgentId, { id: d.id, name: d.name }] as const],
        ),
      );

      // Drop an instance whose agent has no folded definition, matching the
      // per-tenant instance surfaces (which inner-join the definition): every
      // agent is folded, so this never elides a real row, and it emits no
      // definition id it cannot resolve.
      const items = rows.flatMap((r) => {
        const def = defByAgentId.get(r.agentId);
        if (def === undefined) return [];
        return [
          {
            id: r.id,
            tenantId: r.tenantId,
            tenantName: tenantMap.get(r.tenantId)?.name ?? "Unknown",
            definitionId: def.id,
            agentName: def.name,
            address: r.address,
            status: r.status as
              | "deployed"
              | "running"
              | "updating"
              | "error"
              | "stopped",
            createdAt: ts(r.createdAt),
          },
        ];
      });

      return c.json(paginatedResponse(items, rows, limit));
    },
  );

  app.get(
    "/sessions",
    describeRoute({
      tags: ["User"],
      summary: "List sessions across all tenants",
      description:
        "Aggregates active sessions from all tenants the user belongs to. Each result is tagged with tenantId.",
      responses: {
        200: {
          description: "Sessions across tenants",
          content: {
            "application/json": {
              schema: resolver(SessionSummary.array()),
            },
          },
        },
      },
    }),
    (_c) => {
      // Sessions deferred to later phase
      return _c.json([]);
    },
  );

  app.get(
    "/approvals",
    describeRoute({
      tags: ["User"],
      summary: "List pending approvals across all tenants",
      description:
        "Aggregates pending approval requests from all tenants the user belongs to. Each result is tagged with tenantId.",
      responses: {
        200: {
          description: "Approvals across tenants",
          content: {
            "application/json": {
              schema: resolver(ApprovalSummary.array()),
            },
          },
        },
      },
    }),
    (_c) => {
      // Approvals deferred to later phase
      return _c.json([]);
    },
  );

  return app;
}
