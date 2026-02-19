import { eq, and, ilike } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { agent, offering } from "@interchange/db/schema";
import {
  CreateOffering,
  UpdateOffering,
  OfferingDetail,
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

function formatOffering(row: typeof offering.$inferSelect, agentName: string) {
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
  requireGrant("offering:*", "read"),
  describeRoute({
    tags: ["Discovery"],
    summary: "Search offerings",
    description:
      "Searches offerings across discoverable agents in the tenant and federated tenants. Filterable by offering name, pricing range, and payment method.",
    parameters: [
      { name: "name", in: "query", schema: { type: "string" } },
      { name: "minPrice", in: "query", schema: { type: "string" } },
      { name: "maxPrice", in: "query", schema: { type: "string" } },
      { name: "paymentMethod", in: "query", schema: { type: "string" } },
      ...pageParameters,
    ],
    responses: {
      200: {
        description: "List of offerings",
        content: {
          "application/json": {
            schema: resolver(paginatedSchema(OfferingDetail)),
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

    const conditions = [eq(offering.tenantId, tenantCtx.id)];
    if (name) {
      conditions.push(ilike(offering.name, `%${name}%`));
    }
    if (cursor) {
      conditions.push(cursorCondition(offering.createdAt, offering.id, cursor));
    }

    const rows = await db.query.offering.findMany({
      where: and(...conditions),
      orderBy: pageOrder(offering.createdAt, offering.id),
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
      formatOffering(r, agentNames.get(r.agentId) ?? r.agentId),
    );

    return c.json(paginatedResponse(items, rows, limit));
  },
);

app.post(
  "/",
  requireGrant("offering:*", "create"),
  describeRoute({
    tags: ["Discovery"],
    summary: "Register an offering",
    description:
      "Registers an offering for an agent. The agent must belong to the tenant.",
    responses: {
      201: {
        description: "Offering registered",
        content: {
          "application/json": { schema: resolver(OfferingDetail) },
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
  validator("json", CreateOffering),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const body = c.req.valid("json" as never) as typeof CreateOffering.infer;
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
        .insert(offering)
        .values({
          id: generateId("offering"),
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

    return c.json(formatOffering(row, agentRow.name), 201);
  },
);

app.get(
  "/:offeringId",
  requireGrant(idResource("offering", "offeringId"), "read"),
  describeRoute({
    tags: ["Discovery"],
    summary: "Get offering details",
    description:
      "Returns pricing, agent info, and request/response type information.",
    responses: {
      200: {
        description: "Offering details",
        content: {
          "application/json": { schema: resolver(OfferingDetail) },
        },
      },
      404: {
        description: "Offering not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const offeringId = c.req.param("offeringId");
    const db = c.get("db");

    const row = await db.query.offering.findFirst({
      where: and(
        eq(offering.id, offeringId),
        eq(offering.tenantId, tenantCtx.id),
      ),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Offering not found" } },
        404,
      );
    }

    const agentRow = await db.query.agent.findFirst({
      where: eq(agent.id, row.agentId),
    });

    return c.json(formatOffering(row, agentRow?.name ?? row.agentId));
  },
);

app.patch(
  "/:offeringId",
  requireGrant(idResource("offering", "offeringId"), "manage"),
  describeRoute({
    tags: ["Discovery"],
    summary: "Update an offering",
    responses: {
      200: {
        description: "Offering updated",
        content: {
          "application/json": { schema: resolver(OfferingDetail) },
        },
      },
      404: {
        description: "Offering not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", UpdateOffering),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const offeringId = c.req.param("offeringId");
    const body = c.req.valid("json" as never) as typeof UpdateOffering.infer;
    const db = c.get("db");

    const updates: Record<string, unknown> = { updatedAt: new Date() };
    if (body.name !== undefined) updates["name"] = body.name;
    if (body.description !== undefined)
      updates["description"] = body.description;
    if (body.pricing !== undefined) updates["pricing"] = body.pricing;
    if (body.schema !== undefined) updates["schema"] = body.schema;

    const [updated] = await db
      .update(offering)
      .set(updates)
      .where(
        and(eq(offering.id, offeringId), eq(offering.tenantId, tenantCtx.id)),
      )
      .returning();

    if (!updated) {
      return c.json(
        { error: { code: "not_found", message: "Offering not found" } },
        404,
      );
    }

    const agentRow = await db.query.agent.findFirst({
      where: eq(agent.id, updated.agentId),
    });

    return c.json(formatOffering(updated, agentRow?.name ?? updated.agentId));
  },
);

app.delete(
  "/:offeringId",
  requireGrant(idResource("offering", "offeringId"), "manage"),
  describeRoute({
    tags: ["Discovery"],
    summary: "Remove an offering",
    responses: {
      204: {
        description: "Offering removed",
      },
      404: {
        description: "Offering not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const offeringId = c.req.param("offeringId");
    const db = c.get("db");

    const deleted = await db
      .delete(offering)
      .where(
        and(eq(offering.id, offeringId), eq(offering.tenantId, tenantCtx.id)),
      )
      .returning();

    if (deleted.length === 0) {
      return c.json(
        { error: { code: "not_found", message: "Offering not found" } },
        404,
      );
    }

    return c.body(null, 204);
  },
);

export { app as offeringRoutes };

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
