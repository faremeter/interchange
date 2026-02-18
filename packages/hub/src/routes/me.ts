import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver } from "hono-openapi";

import { principal } from "@interchange/db/schema";
import {
  UserProfile,
  PrincipalSummary,
  AgentSummary,
  SessionSummary,
  ApprovalSummary,
  ErrorResponse,
} from "@interchange/types";

import type { AppEnv } from "../context";
import { ts } from "../format";

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
        { error: { code: "unauthorized", message: "Authentication required" } },
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
    responses: {
      200: {
        description: "List of principals across tenants",
        content: {
          "application/json": {
            schema: resolver(PrincipalSummary.array()),
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
        { error: { code: "unauthorized", message: "Authentication required" } },
        401,
      );
    }
    const db = c.get("db");

    const principals = await db.query.principal.findMany({
      where: and(eq(principal.kind, "user"), eq(principal.refId, user.id)),
    });

    const tenantIds = principals.map((p) => p.tenantId);
    const tenants =
      tenantIds.length > 0
        ? await db.query.tenant.findMany({
            where: (t, { inArray }) => inArray(t.id, tenantIds),
          })
        : [];
    const tenantMap = new Map(tenants.map((t) => [t.id, t]));

    const principalIds = principals.map((p) => p.id);
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

    const results = principals.map((p) => {
      const t = tenantMap.get(p.tenantId);
      return {
        principalId: p.id,
        tenantId: p.tenantId,
        tenantName: t?.name ?? "Unknown",
        tenantSlug: t?.slug ?? "unknown",
        kind: p.kind as "user" | "agent",
        status: p.status as "active" | "suspended" | "invited" | "deactivated",
        roles: assignmentsByPrincipal.get(p.id) ?? [],
      };
    });

    return c.json(results);
  },
);

app.get(
  "/agents",
  describeRoute({
    tags: ["User"],
    summary: "List agents across all tenants",
    description:
      "Aggregates agents from all tenants the user belongs to. Each result is tagged with tenantId.",
    responses: {
      200: {
        description: "Agents across tenants",
        content: {
          "application/json": {
            schema: resolver(AgentSummary.array()),
          },
        },
      },
    },
  }),
  async (c) => {
    const user = c.get("user");
    if (!user) {
      return c.json(
        { error: { code: "unauthorized", message: "Authentication required" } },
        401,
      );
    }
    const db = c.get("db");

    const principals = await db.query.principal.findMany({
      where: and(eq(principal.kind, "user"), eq(principal.refId, user.id)),
    });

    const tenantIds = principals.map((p) => p.tenantId);
    if (tenantIds.length === 0) return c.json([]);

    const tenants = await db.query.tenant.findMany({
      where: (t, { inArray }) => inArray(t.id, tenantIds),
    });
    const tenantMap = new Map(tenants.map((t) => [t.id, t]));

    const agents = await db.query.agent.findMany({
      where: (a, { inArray }) => inArray(a.tenantId, tenantIds),
    });

    const results = agents.map((a) => ({
      id: a.id,
      tenantId: a.tenantId,
      tenantName: tenantMap.get(a.tenantId)?.name ?? "Unknown",
      name: a.name,
      description: a.description ?? null,
      status: a.status as "deployed" | "stopped" | "updating" | "error",
    }));

    return c.json(results);
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

export { app as meRoutes };
