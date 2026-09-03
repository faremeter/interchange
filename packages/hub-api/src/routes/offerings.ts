import { eq, and, ilike } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { offering, workflowDefinition } from "@intx/db/schema";
import { parseOfferingRow } from "@intx/db";
import type { DB } from "@intx/db";
import {
  CreateOffering,
  UpdateOffering,
  OfferingDetail,
  ErrorResponse,
  paginatedSchema,
} from "@intx/types";

import type { TenantEnv } from "../context";
import { errorResponse } from "../error-response";
import { first } from "../format";
import { generateId } from "@intx/hub-common";
import { idResource } from "../middleware/grant";
import type { RequireGrant } from "../middleware/grant";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";

export function formatOffering(
  row: typeof offering.$inferSelect,
  agentName: string,
) {
  const parsed = parseOfferingRow(row);
  return {
    id: parsed.id,
    agentId: parsed.agentId,
    agentName,
    tenantId: parsed.tenantId,
    name: parsed.name,
    description: parsed.description ?? null,
    pricing: parsed.pricing ?? undefined,
    schema: parsed.schema,
  };
}

export type CreateOfferingRoutesDeps = {
  db: DB["db"];
  requireGrant: RequireGrant;
};

export function createOfferingRoutes({
  db,
  requireGrant,
}: CreateOfferingRoutesDeps): Hono<TenantEnv> {
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
        conditions.push(
          cursorCondition(offering.createdAt, offering.id, cursor),
        );
      }

      const rows = await db.query.offering.findMany({
        where: and(...conditions),
        orderBy: pageOrder(offering.createdAt, offering.id),
        limit,
      });

      // `agentId` holds the offering's workflow_definition id (contract-stable
      // name); the display name is the definition's name.
      const definitionIds = [...new Set(rows.map((r) => r.agentId))];
      const definitionNames = new Map<string, string>();
      if (definitionIds.length > 0) {
        const definitions = await db.query.workflowDefinition.findMany({
          where: (d, { inArray }) => inArray(d.id, definitionIds),
        });
        for (const d of definitions) {
          definitionNames.set(d.id, d.name);
        }
      }

      const items = rows.map((r) =>
        formatOffering(r, definitionNames.get(r.agentId) ?? r.agentId),
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
        "Registers an offering for a workflow definition. The definition must belong to the tenant.",
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
          description: "Workflow definition not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", CreateOffering),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const body = c.req.valid("json");

      // `agentId` carries a workflow_definition id (contract-stable field name).
      const definitionRow = await db.query.workflowDefinition.findFirst({
        where: and(
          eq(workflowDefinition.id, body.agentId),
          eq(workflowDefinition.tenantId, tenantCtx.id),
        ),
      });

      if (!definitionRow) {
        return errorResponse(
          c,
          "not_found",
          "Workflow definition not found in this tenant",
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

      return c.json(formatOffering(row, definitionRow.name), 201);
    },
  );

  app.get(
    "/:offeringId",
    requireGrant(idResource("offering", "offeringId"), "read"),
    describeRoute({
      tags: ["Discovery"],
      summary: "Get offering details",
      description:
        "Returns pricing, definition info, and request/response type information.",
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

      const row = await db.query.offering.findFirst({
        where: and(
          eq(offering.id, offeringId),
          eq(offering.tenantId, tenantCtx.id),
        ),
      });

      if (!row) {
        return errorResponse(c, "not_found", "Offering not found");
      }

      const definitionRow = await db.query.workflowDefinition.findFirst({
        where: eq(workflowDefinition.id, row.agentId),
      });

      return c.json(formatOffering(row, definitionRow?.name ?? row.agentId));
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
      const body = c.req.valid("json");

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
        return errorResponse(c, "not_found", "Offering not found");
      }

      const definitionRow = await db.query.workflowDefinition.findFirst({
        where: eq(workflowDefinition.id, updated.agentId),
      });

      return c.json(
        formatOffering(updated, definitionRow?.name ?? updated.agentId),
      );
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

      const deleted = await db
        .delete(offering)
        .where(
          and(eq(offering.id, offeringId), eq(offering.tenantId, tenantCtx.id)),
        )
        .returning();

      if (deleted.length === 0) {
        return errorResponse(c, "not_found", "Offering not found");
      }

      return c.body(null, 204);
    },
  );

  return app;
}
