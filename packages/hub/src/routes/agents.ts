import { eq, and, isNull } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import {
  agent,
  agentInstance,
  agentRole,
  agentVersion,
} from "@interchange/db/schema";
import { parseAgentRow, parseAgentVersionRow } from "@interchange/db";
import type { DB } from "@interchange/db";
import {
  CreateAgent,
  UpdateAgent,
  AgentResponse,
  AgentVersion,
  RollbackRequest,
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

function formatAgent(
  row: typeof agent.$inferSelect,
  roles: { id: string; name: string }[],
) {
  const parsed = parseAgentRow(row);
  return {
    id: parsed.id,
    tenantId: parsed.tenantId,
    creatorPrincipalId: parsed.creatorPrincipalId ?? undefined,
    name: parsed.name,
    description: parsed.description ?? null,
    systemPrompt: parsed.systemPrompt ?? null,
    skills: parsed.skills ?? undefined,
    contextConfig: parsed.contextConfig ?? undefined,
    initialState: parsed.initialState ?? undefined,
    modelConfig: parsed.modelConfig ?? undefined,
    currentVersion: parsed.currentVersion,
    status: parsed.status,
    capabilities: parsed.capabilities ?? undefined,
    credentialRequirements: parsed.credentialRequirements ?? undefined,
    grantRequirements: parsed.grantRequirements ?? undefined,
    roles,
    createdAt: ts(parsed.createdAt),
    updatedAt: ts(parsed.updatedAt),
  };
}

async function loadAgentRoles(
  db: TenantEnv["Variables"]["db"],
  agentId: string,
  tenantId: string,
): Promise<{ id: string; name: string }[]> {
  const assignments = await db.query.agentRole.findMany({
    where: eq(agentRole.agentId, agentId),
  });
  if (assignments.length === 0) return [];
  const roleIds = assignments.map((a) => a.roleId);
  const roles = await db.query.role.findMany({
    where: (r, { inArray, and: a }) =>
      a(inArray(r.id, roleIds), eq(r.tenantId, tenantId)),
  });
  return roles.map((r) => ({ id: r.id, name: r.name }));
}

export type CreateAgentRoutesDeps = {
  db: DB["db"];
};

export function createAgentRoutes({
  db,
}: CreateAgentRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  app.get(
    "/",
    requireGrant("agent:*", "read"),
    describeRoute({
      tags: ["Agents"],
      summary: "List agents in the tenant",
      description: "Filterable by offering and status.",
      parameters: [
        { name: "offering", in: "query", schema: { type: "string" } },
        {
          name: "status",
          in: "query",
          schema: {
            type: "string",
            enum: ["deployed", "stopped"],
          },
        },
        ...pageParameters,
      ],
      responses: {
        200: {
          description: "List of agents",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(AgentResponse)),
            },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const status = c.req.query("status");
      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      const conditions = [eq(agent.tenantId, tenantCtx.id)];
      if (status === "deployed" || status === "stopped") {
        conditions.push(eq(agent.status, status));
      }
      if (cursor) {
        conditions.push(cursorCondition(agent.createdAt, agent.id, cursor));
      }

      const rows = await db.query.agent.findMany({
        where: and(...conditions),
        orderBy: pageOrder(agent.createdAt, agent.id),
        limit,
      });

      const allAssignments =
        rows.length > 0
          ? await db.query.agentRole.findMany({
              where: (ar, { inArray }) =>
                inArray(
                  ar.agentId,
                  rows.map((r) => r.id),
                ),
            })
          : [];

      const roleIds = [...new Set(allAssignments.map((a) => a.roleId))];
      const allRoles =
        roleIds.length > 0
          ? await db.query.role.findMany({
              where: (r, { inArray, and: a }) =>
                a(inArray(r.id, roleIds), eq(r.tenantId, tenantCtx.id)),
            })
          : [];
      const roleMap = new Map(allRoles.map((r) => [r.id, r]));

      const rolesByAgent = new Map<string, { id: string; name: string }[]>();
      for (const a of allAssignments) {
        const r = roleMap.get(a.roleId);
        if (!r) continue;
        const list = rolesByAgent.get(a.agentId) ?? [];
        list.push({ id: r.id, name: r.name });
        rolesByAgent.set(a.agentId, list);
      }

      return c.json(
        paginatedResponse(
          rows.map((r) => formatAgent(r, rolesByAgent.get(r.id) ?? [])),
          rows,
          limit,
        ),
      );
    },
  );

  app.post(
    "/",
    requireGrant("agent:*", "create"),
    describeRoute({
      tags: ["Agents"],
      summary: "Create an agent",
      description:
        "Creates an agent definition. Grant requirements are stored as a manifest and resolved at instance launch time.",
      responses: {
        201: {
          description: "Agent created",
          content: {
            "application/json": { schema: resolver(AgentResponse) },
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
    validator("json", CreateAgent),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const creatorPrincipal = c.get("principal");
      const body = c.req.valid("json");

      const now = new Date();
      const agentId = generateId("agent");

      // Validate role IDs before writing anything.
      const uniqueRoleIds = [...new Set(body.roleIds ?? [])];
      let validRoles: { id: string; name: string }[] = [];
      if (uniqueRoleIds.length > 0) {
        const found = await db.query.role.findMany({
          where: (r, { inArray, and: a }) =>
            a(inArray(r.id, uniqueRoleIds), eq(r.tenantId, tenantCtx.id)),
        });
        if (found.length !== uniqueRoleIds.length) {
          const validIds = new Set(found.map((r) => r.id));
          const invalid = uniqueRoleIds.filter((id) => !validIds.has(id));
          return c.json(
            {
              error: {
                code: "bad_request",
                message: `Roles not found in tenant: ${invalid.join(", ")}`,
              },
            },
            400,
          );
        }
        validRoles = found.map((r) => ({ id: r.id, name: r.name }));
      }

      const agentRow = await db.transaction(async (tx) => {
        const row = first(
          await tx
            .insert(agent)
            .values({
              id: agentId,
              tenantId: tenantCtx.id,
              creatorPrincipalId: creatorPrincipal.id,
              name: body.name,
              description: body.description ?? null,
              systemPrompt: body.systemPrompt ?? null,
              skills: body.skills ?? null,
              contextConfig: body.contextConfig ?? null,
              initialState: body.initialState ?? null,
              modelConfig: body.modelConfig ?? null,
              capabilities: body.capabilities ?? null,
              credentialRequirements: body.credentialRequirements ?? null,
              grantRequirements: body.grantRequirements ?? null,
              currentVersion: "1",
              status: "deployed",
              createdAt: now,
              updatedAt: now,
            })
            .returning(),
        );

        await tx.insert(agentVersion).values({
          id: generateId("agentVersion"),
          agentId,
          version: "1",
          status: "active",
          createdAt: now,
        });

        if (uniqueRoleIds.length > 0) {
          await tx.insert(agentRole).values(
            uniqueRoleIds.map((roleId) => ({
              agentId,
              roleId,
              createdAt: now,
            })),
          );
        }

        return row;
      });

      return c.json(formatAgent(agentRow, validRoles), 201);
    },
  );

  app.get(
    "/:agentId",
    requireGrant(idResource("agent", "agentId"), "read"),
    describeRoute({
      tags: ["Agents"],
      summary: "Get agent details",
      description:
        "Returns the agent definition, status, health, and capabilities.",
      responses: {
        200: {
          description: "Agent details",
          content: {
            "application/json": { schema: resolver(AgentResponse) },
          },
        },
        404: {
          description: "Agent not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const agentId = c.req.param("agentId");

      const row = await db.query.agent.findFirst({
        where: and(eq(agent.id, agentId), eq(agent.tenantId, tenantCtx.id)),
      });

      if (!row) {
        return c.json(
          { error: { code: "not_found", message: "Agent not found" } },
          404,
        );
      }

      const roles = await loadAgentRoles(db, agentId, tenantCtx.id);
      return c.json(formatAgent(row, roles));
    },
  );

  app.patch(
    "/:agentId",
    requireGrant(idResource("agent", "agentId"), "manage"),
    describeRoute({
      tags: ["Agents"],
      summary: "Update agent definition",
      description:
        "Updates the agent definition and creates a new version. The new version is deployed alongside the current version until health checks pass.",
      responses: {
        200: {
          description: "Agent updated",
          content: {
            "application/json": { schema: resolver(AgentResponse) },
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
    validator("json", UpdateAgent),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const agentId = c.req.param("agentId");
      const body = c.req.valid("json");

      const existing = await db.query.agent.findFirst({
        where: and(eq(agent.id, agentId), eq(agent.tenantId, tenantCtx.id)),
      });

      if (!existing) {
        return c.json(
          { error: { code: "not_found", message: "Agent not found" } },
          404,
        );
      }

      const now = new Date();
      const newVersion = String(Number(existing.currentVersion) + 1);

      // Validate role IDs before writing anything.
      let validRoles: { id: string; name: string }[] | undefined;
      if (body.roleIds !== undefined) {
        const uniqueRoleIds = [...new Set(body.roleIds)];
        if (uniqueRoleIds.length > 0) {
          const found = await db.query.role.findMany({
            where: (r, { inArray, and: a }) =>
              a(inArray(r.id, uniqueRoleIds), eq(r.tenantId, tenantCtx.id)),
          });
          if (found.length !== uniqueRoleIds.length) {
            const validIds = new Set(found.map((r) => r.id));
            const invalid = uniqueRoleIds.filter((id) => !validIds.has(id));
            return c.json(
              {
                error: {
                  code: "bad_request",
                  message: `Roles not found in tenant: ${invalid.join(", ")}`,
                },
              },
              400,
            );
          }
          validRoles = found.map((r) => ({ id: r.id, name: r.name }));
        } else {
          validRoles = [];
        }
      }

      const updates: Record<string, unknown> = {
        updatedAt: now,
        currentVersion: newVersion,
      };
      if (body.name !== undefined) updates["name"] = body.name;
      if (body.description !== undefined)
        updates["description"] = body.description;
      if (body.systemPrompt !== undefined)
        updates["systemPrompt"] = body.systemPrompt;
      if (body.skills !== undefined) updates["skills"] = body.skills;
      if (body.contextConfig !== undefined)
        updates["contextConfig"] = body.contextConfig;
      if (body.initialState !== undefined)
        updates["initialState"] = body.initialState;
      if (body.modelConfig !== undefined)
        updates["modelConfig"] = body.modelConfig;
      if (body.capabilities !== undefined)
        updates["capabilities"] = body.capabilities;
      if (body.credentialRequirements !== undefined)
        updates["credentialRequirements"] = body.credentialRequirements;
      if (body.grantRequirements !== undefined)
        updates["grantRequirements"] = body.grantRequirements;

      const updated = await db.transaction(async (tx) => {
        const row = first(
          await tx
            .update(agent)
            .set(updates)
            .where(eq(agent.id, agentId))
            .returning(),
        );

        await tx
          .update(agentVersion)
          .set({ status: "inactive" })
          .where(
            and(
              eq(agentVersion.agentId, agentId),
              eq(agentVersion.version, existing.currentVersion),
            ),
          );

        await tx.insert(agentVersion).values({
          id: generateId("agentVersion"),
          agentId,
          version: newVersion,
          status: "active",
          createdAt: now,
        });

        if (validRoles !== undefined) {
          await tx.delete(agentRole).where(eq(agentRole.agentId, agentId));
          if (validRoles.length > 0) {
            await tx.insert(agentRole).values(
              validRoles.map((r) => ({
                agentId,
                roleId: r.id,
                createdAt: now,
              })),
            );
          }
        }

        return row;
      });

      const roles =
        validRoles ?? (await loadAgentRoles(db, agentId, tenantCtx.id));

      return c.json(formatAgent(updated, roles));
    },
  );

  app.delete(
    "/:agentId",
    requireGrant(idResource("agent", "agentId"), "manage"),
    describeRoute({
      tags: ["Agents"],
      summary: "Retire an agent",
      description:
        "Retires the agent definition and marks all running instances as stopped. Does not signal running sidecars; in-flight sessions continue until the sidecar disconnects.",
      responses: {
        204: {
          description: "Agent retirement initiated",
        },
        404: {
          description: "Agent not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const agentId = c.req.param("agentId");

      const existing = await db.query.agent.findFirst({
        where: and(eq(agent.id, agentId), eq(agent.tenantId, tenantCtx.id)),
      });

      if (!existing) {
        return c.json(
          { error: { code: "not_found", message: "Agent not found" } },
          404,
        );
      }

      const retiredAt = new Date();

      // TODO: This is DB-only — it does not signal the sidecar to stop
      // or end the agentSession. A running sidecar will continue until
      // it disconnects naturally. Add sidecar teardown coordination.
      await db
        .update(agentInstance)
        .set({
          status: "stopped",
          sessionId: null,
          updatedAt: retiredAt,
          endedAt: retiredAt,
        })
        .where(
          and(
            eq(agentInstance.agentId, agentId),
            isNull(agentInstance.endedAt),
          ),
        );

      // Set agent status to stopped
      await db
        .update(agent)
        .set({ status: "stopped", updatedAt: retiredAt })
        .where(eq(agent.id, agentId));

      return c.body(null, 204);
    },
  );

  app.get(
    "/:agentId/versions",
    requireGrant(idResource("agent", "agentId"), "read"),
    describeRoute({
      tags: ["Agents"],
      summary: "List agent versions",
      description: "Lists all versions with their deployment status.",
      parameters: [...pageParameters],
      responses: {
        200: {
          description: "List of versions",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(AgentVersion)),
            },
          },
        },
      },
    }),
    async (c) => {
      const agentId = c.req.param("agentId");
      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      const conditions = [eq(agentVersion.agentId, agentId)];
      if (cursor) {
        conditions.push(
          cursorCondition(agentVersion.createdAt, agentVersion.id, cursor),
        );
      }

      const rows = await db.query.agentVersion.findMany({
        where: and(...conditions),
        orderBy: pageOrder(agentVersion.createdAt, agentVersion.id),
        limit,
      });

      const items = rows.map((v) => {
        const parsed = parseAgentVersionRow(v);
        return {
          version: parsed.version,
          status: parsed.status,
          createdAt: ts(parsed.createdAt),
        };
      });

      return c.json(paginatedResponse(items, rows, limit));
    },
  );

  app.post(
    "/:agentId/rollback",
    requireGrant(idResource("agent", "agentId"), "manage"),
    describeRoute({
      tags: ["Agents"],
      summary: "Rollback to a previous version",
      description:
        "Shifts traffic back to the specified version. The current version is stopped.",
      responses: {
        200: {
          description: "Rollback initiated",
          content: {
            "application/json": { schema: resolver(AgentResponse) },
          },
        },
        400: {
          description: "Invalid version",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", RollbackRequest),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const agentId = c.req.param("agentId");
      const body = c.req.valid("json");

      const existing = await db.query.agent.findFirst({
        where: and(eq(agent.id, agentId), eq(agent.tenantId, tenantCtx.id)),
      });
      if (!existing) {
        return c.json(
          { error: { code: "not_found", message: "Agent not found" } },
          404,
        );
      }

      const targetVersion = await db.query.agentVersion.findFirst({
        where: and(
          eq(agentVersion.agentId, agentId),
          eq(agentVersion.version, body.version),
        ),
      });
      if (!targetVersion) {
        return c.json(
          {
            error: {
              code: "bad_request",
              message: "Target version not found",
            },
          },
          400,
        );
      }

      const now = new Date();

      // Deactivate current version
      await db
        .update(agentVersion)
        .set({ status: "inactive" })
        .where(
          and(
            eq(agentVersion.agentId, agentId),
            eq(agentVersion.version, existing.currentVersion),
          ),
        );

      // Activate target version
      await db
        .update(agentVersion)
        .set({ status: "active" })
        .where(
          and(
            eq(agentVersion.agentId, agentId),
            eq(agentVersion.version, body.version),
          ),
        );

      // Update agent
      const updated = first(
        await db
          .update(agent)
          .set({ currentVersion: body.version, updatedAt: now })
          .where(eq(agent.id, agentId))
          .returning(),
      );

      const roles = await loadAgentRoles(db, updated.id, tenantCtx.id);
      return c.json(formatAgent(updated, roles));
    },
  );

  return app;
}
