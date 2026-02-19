import { eq, and, ilike } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { agent, capability } from "@interchange/db/schema";
import {
  CreateCapability,
  UpdateCapability,
  CapabilityDetail,
  ModelInfo,
  ErrorResponse,
  paginatedSchema,
} from "@interchange/types";

import type { TenantEnv, AppEnv } from "../context";
import { first } from "../format";
import { generateId } from "../ids";
import { requireGrant, idResource } from "../middleware/grant";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";

type Pricing = {
  base?: { amount: string; currency: string };
  methods?: string[];
  negotiable?: boolean;
  bounds?: { min?: string; max?: string };
};

function formatCapability(
  row: typeof capability.$inferSelect,
  agentName: string,
) {
  return {
    id: row.id,
    agentId: row.agentId,
    agentName,
    tenantId: row.tenantId,
    name: row.name,
    description: row.description ?? null,
    pricing: (row.pricing as Pricing | null) ?? undefined,
    schema: (row.schema as Record<string, unknown> | null) ?? null,
  };
}

const app = new Hono<TenantEnv>();

app.get(
  "/",
  requireGrant("capability:*", "read"),
  describeRoute({
    tags: ["Discovery"],
    summary: "Search capabilities",
    description:
      "Searches capabilities across discoverable agents in the tenant and federated tenants. Filterable by capability name, pricing range, and payment method.",
    parameters: [
      { name: "name", in: "query", schema: { type: "string" } },
      { name: "minPrice", in: "query", schema: { type: "string" } },
      { name: "maxPrice", in: "query", schema: { type: "string" } },
      { name: "paymentMethod", in: "query", schema: { type: "string" } },
      ...pageParameters,
    ],
    responses: {
      200: {
        description: "List of capabilities",
        content: {
          "application/json": {
            schema: resolver(paginatedSchema(CapabilityDetail)),
          },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const db = c.get("db");
    const name = c.req.query("name");
    const { limit, cursor } = parsePageParams({
      cursor: c.req.query("cursor"),
      limit: c.req.query("limit"),
    });

    const conditions = [eq(capability.tenantId, tenantCtx.id)];
    if (name) {
      conditions.push(ilike(capability.name, `%${name}%`));
    }
    if (cursor) {
      conditions.push(
        cursorCondition(capability.createdAt, capability.id, cursor),
      );
    }

    const rows = await db.query.capability.findMany({
      where: and(...conditions),
      orderBy: pageOrder(capability.createdAt, capability.id),
      limit,
    });

    const agentIds = [...new Set(rows.map((r) => r.agentId))];
    const agentNames = new Map<string, string>();
    if (agentIds.length > 0) {
      const agents = await db.query.agent.findMany({
        where: (a, { inArray }) => inArray(a.id, agentIds),
      });
      for (const a of agents) {
        agentNames.set(a.id, a.name);
      }
    }

    const items = rows.map((r) =>
      formatCapability(r, agentNames.get(r.agentId) ?? r.agentId),
    );

    return c.json(paginatedResponse(items, rows, limit));
  },
);

app.post(
  "/",
  requireGrant("capability:*", "create"),
  describeRoute({
    tags: ["Discovery"],
    summary: "Register a capability",
    description:
      "Registers a capability for an agent. The agent must belong to the tenant.",
    responses: {
      201: {
        description: "Capability registered",
        content: {
          "application/json": { schema: resolver(CapabilityDetail) },
        },
      },
      400: {
        description: "Validation error",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
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
  validator("json", CreateCapability),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const body = c.req.valid("json" as never) as typeof CreateCapability.infer;
    const db = c.get("db");

    const agentRow = await db.query.agent.findFirst({
      where: and(eq(agent.id, body.agentId), eq(agent.tenantId, tenantCtx.id)),
    });

    if (!agentRow) {
      return c.json(
        {
          error: {
            code: "not_found",
            message: "Agent not found in this tenant",
          },
        },
        404,
      );
    }

    const now = new Date();
    const row = first(
      await db
        .insert(capability)
        .values({
          id: generateId("capability"),
          agentId: body.agentId,
          tenantId: tenantCtx.id,
          name: body.name,
          description: body.description ?? null,
          pricing: body.pricing ?? null,
          schema: body.schema ?? null,
          createdAt: now,
          updatedAt: now,
        })
        .returning(),
    );

    return c.json(formatCapability(row, agentRow.name), 201);
  },
);

app.get(
  "/:capabilityId",
  requireGrant(idResource("capability", "capabilityId"), "read"),
  describeRoute({
    tags: ["Discovery"],
    summary: "Get capability details",
    description:
      "Returns pricing, agent info, and request/response type information.",
    responses: {
      200: {
        description: "Capability details",
        content: {
          "application/json": { schema: resolver(CapabilityDetail) },
        },
      },
      404: {
        description: "Capability not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const capabilityId = c.req.param("capabilityId");
    const db = c.get("db");

    const row = await db.query.capability.findFirst({
      where: and(
        eq(capability.id, capabilityId),
        eq(capability.tenantId, tenantCtx.id),
      ),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Capability not found" } },
        404,
      );
    }

    const agentRow = await db.query.agent.findFirst({
      where: eq(agent.id, row.agentId),
    });

    return c.json(formatCapability(row, agentRow?.name ?? row.agentId));
  },
);

app.patch(
  "/:capabilityId",
  requireGrant(idResource("capability", "capabilityId"), "manage"),
  describeRoute({
    tags: ["Discovery"],
    summary: "Update a capability",
    responses: {
      200: {
        description: "Capability updated",
        content: {
          "application/json": { schema: resolver(CapabilityDetail) },
        },
      },
      404: {
        description: "Capability not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", UpdateCapability),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const capabilityId = c.req.param("capabilityId");
    const body = c.req.valid("json" as never) as typeof UpdateCapability.infer;
    const db = c.get("db");

    const updates: Record<string, unknown> = { updatedAt: new Date() };
    if (body.name !== undefined) updates["name"] = body.name;
    if (body.description !== undefined)
      updates["description"] = body.description;
    if (body.pricing !== undefined) updates["pricing"] = body.pricing;
    if (body.schema !== undefined) updates["schema"] = body.schema;

    const [updated] = await db
      .update(capability)
      .set(updates)
      .where(
        and(
          eq(capability.id, capabilityId),
          eq(capability.tenantId, tenantCtx.id),
        ),
      )
      .returning();

    if (!updated) {
      return c.json(
        { error: { code: "not_found", message: "Capability not found" } },
        404,
      );
    }

    const agentRow = await db.query.agent.findFirst({
      where: eq(agent.id, updated.agentId),
    });

    return c.json(formatCapability(updated, agentRow?.name ?? updated.agentId));
  },
);

app.delete(
  "/:capabilityId",
  requireGrant(idResource("capability", "capabilityId"), "manage"),
  describeRoute({
    tags: ["Discovery"],
    summary: "Remove a capability",
    responses: {
      204: {
        description: "Capability removed",
      },
      404: {
        description: "Capability not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const capabilityId = c.req.param("capabilityId");
    const db = c.get("db");

    const deleted = await db
      .delete(capability)
      .where(
        and(
          eq(capability.id, capabilityId),
          eq(capability.tenantId, tenantCtx.id),
        ),
      )
      .returning();

    if (deleted.length === 0) {
      return c.json(
        { error: { code: "not_found", message: "Capability not found" } },
        404,
      );
    }

    return c.body(null, 204);
  },
);

export { app as capabilityRoutes };

// Models endpoint is global (not tenant-scoped) -- remains a stub for now
// since model discovery requires external provider integration
const modelsApp = new Hono<AppEnv>();

modelsApp.get(
  "/",
  describeRoute({
    tags: ["Discovery"],
    summary: "List available models",
    description:
      "Lists available models across configured providers with capabilities, pricing, and limits.",
    responses: {
      200: {
        description: "List of models",
        content: {
          "application/json": {
            schema: resolver(ModelInfo.array()),
          },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

export { modelsApp as modelRoutes };
