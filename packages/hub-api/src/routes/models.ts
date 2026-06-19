import { eq, and, inArray } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { model, modelPricing } from "@intx/db/schema";
import { listVisibleOfferings, resolveActivePrice } from "@intx/db";
import type { DB, ModelPricingRow, ResolvedOffering } from "@intx/db";
import {
  CreateModel,
  UpdateModel,
  ModelResponse,
  ModelInfo,
  ErrorResponse,
  paginatedSchema,
} from "@intx/types";
import {
  pushSourceUpdatesSubtree,
  type SidecarRouter,
} from "@intx/hub-sessions";

import type { TenantEnv } from "../context";
import { first, ts } from "../format";
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
import { formatPricingRow } from "./model-offerings";

export function formatModel(row: typeof model.$inferSelect) {
  return {
    id: row.id,
    tenantId: row.tenantId,
    canonicalName: row.canonicalName,
    displayName: row.displayName,
    description: row.description,
    disabled: row.disabled,
    createdAt: ts(row.createdAt),
    updatedAt: ts(row.updatedAt),
  };
}

export type CreateModelCatalogRoutesDeps = {
  db: DB["db"];
  sidecarRouter: SidecarRouter;
  requireGrant: RequireGrant;
};

export function createModelCatalogRoutes({
  db,
  sidecarRouter,
  requireGrant,
}: CreateModelCatalogRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  app.get(
    "/",
    requireGrant("model:*", "read"),
    describeRoute({
      tags: ["Catalog"],
      summary: "List models owned by the tenant",
      description:
        "Lists the models created directly on this tenant. Models inherited from ancestor tenants are not included; use the model discovery endpoint to see the resolved catalog.",
      parameters: [...pageParameters],
      responses: {
        200: {
          description: "List of models",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(ModelResponse)),
            },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      const conditions = [eq(model.tenantId, tenantCtx.id)];
      if (cursor) {
        conditions.push(cursorCondition(model.createdAt, model.id, cursor));
      }

      const rows = await db.query.model.findMany({
        where: and(...conditions),
        orderBy: pageOrder(model.createdAt, model.id),
        limit,
      });
      return c.json(paginatedResponse(rows.map(formatModel), rows, limit));
    },
  );

  app.post(
    "/",
    requireGrant("model:*", "create"),
    describeRoute({
      tags: ["Catalog"],
      summary: "Create a model",
      description:
        "Creates a tenant-local model. Reusing the canonical name of an inherited model shadows that model for this tenant and its descendants.",
      responses: {
        201: {
          description: "Model created",
          content: {
            "application/json": { schema: resolver(ModelResponse) },
          },
        },
        409: {
          description: "A model with this canonical name already exists",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", CreateModel),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const body = c.req.valid("json");

      const existing = await db.query.model.findFirst({
        where: and(
          eq(model.tenantId, tenantCtx.id),
          eq(model.canonicalName, body.canonicalName),
        ),
      });
      if (existing) {
        return c.json(
          {
            error: {
              code: "conflict",
              message: "A model with this canonical name already exists",
            },
          },
          409,
        );
      }

      const now = new Date();
      const row = first(
        await db
          .insert(model)
          .values({
            id: generateId("model"),
            tenantId: tenantCtx.id,
            canonicalName: body.canonicalName,
            displayName: body.displayName ?? null,
            description: body.description ?? null,
            createdAt: now,
            updatedAt: now,
          })
          .returning(),
      );

      void pushSourceUpdatesSubtree(db, sidecarRouter, tenantCtx.id);
      return c.json(formatModel(row), 201);
    },
  );

  app.get(
    "/:modelId",
    requireGrant(idResource("model", "modelId"), "read"),
    describeRoute({
      tags: ["Catalog"],
      summary: "Get a model",
      responses: {
        200: {
          description: "Model details",
          content: {
            "application/json": { schema: resolver(ModelResponse) },
          },
        },
        404: {
          description: "Model not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const modelId = c.req.param("modelId");
      const row = await db.query.model.findFirst({
        where: and(eq(model.id, modelId), eq(model.tenantId, tenantCtx.id)),
      });
      if (!row) {
        return c.json(
          { error: { code: "not_found", message: "Model not found" } },
          404,
        );
      }
      return c.json(formatModel(row));
    },
  );

  app.patch(
    "/:modelId",
    requireGrant(idResource("model", "modelId"), "manage"),
    describeRoute({
      tags: ["Catalog"],
      summary: "Update a model",
      responses: {
        200: {
          description: "Model updated",
          content: {
            "application/json": { schema: resolver(ModelResponse) },
          },
        },
        404: {
          description: "Model not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", UpdateModel),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const modelId = c.req.param("modelId");
      const body = c.req.valid("json");

      const updates: Record<string, unknown> = { updatedAt: new Date() };
      if (body.displayName !== undefined)
        updates["displayName"] = body.displayName;
      if (body.description !== undefined)
        updates["description"] = body.description;
      if (body.disabled !== undefined) updates["disabled"] = body.disabled;

      const [updated] = await db
        .update(model)
        .set(updates)
        .where(and(eq(model.id, modelId), eq(model.tenantId, tenantCtx.id)))
        .returning();

      if (!updated) {
        return c.json(
          { error: { code: "not_found", message: "Model not found" } },
          404,
        );
      }

      void pushSourceUpdatesSubtree(db, sidecarRouter, tenantCtx.id);
      return c.json(formatModel(updated));
    },
  );

  app.delete(
    "/:modelId",
    requireGrant(idResource("model", "modelId"), "manage"),
    describeRoute({
      tags: ["Catalog"],
      summary: "Delete a model",
      description:
        "Removes the model and cascades to the offerings that reference it. Running instances resolved through those offerings fail over to the next eligible source.",
      responses: {
        204: { description: "Model removed" },
        404: {
          description: "Model not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const modelId = c.req.param("modelId");
      const deleted = await db
        .delete(model)
        .where(and(eq(model.id, modelId), eq(model.tenantId, tenantCtx.id)))
        .returning();
      if (deleted.length === 0) {
        return c.json(
          { error: { code: "not_found", message: "Model not found" } },
          404,
        );
      }
      void pushSourceUpdatesSubtree(db, sidecarRouter, tenantCtx.id);
      return c.body(null, 204);
    },
  );

  return app;
}

/**
 * Builds the discovery view from a tenant's resolved offerings and the flat
 * set of pricing rows across those offerings. Pure: it groups the rows by
 * offering, `asOf` selects each offering's active price per currency, and the
 * result groups offerings under their model, ordering each model's offerings
 * by resolution priority (ties broken by provider name for a stable
 * response).
 */
export function composeDiscoveredModels(
  offerings: ResolvedOffering[],
  priceRows: ModelPricingRow[],
  asOf: Date,
) {
  const pricingByOffering = new Map<string, ModelPricingRow[]>();
  for (const row of priceRows) {
    const list = pricingByOffering.get(row.offeringId);
    if (list === undefined) {
      pricingByOffering.set(row.offeringId, [row]);
    } else {
      list.push(row);
    }
  }

  function buildOfferingInfo(o: ResolvedOffering) {
    const active = resolveActivePrice(
      pricingByOffering.get(o.offering.id) ?? [],
      asOf,
    );
    return {
      offeringId: o.offering.id,
      providerId: o.provider.id,
      providerName: o.provider.name,
      plugin: o.provider.plugin,
      priority: o.offering.priority,
      deploymentTags: o.offering.deploymentTags,
      capabilities: o.offering.capabilities,
      pricing: active.map(formatPricingRow),
    };
  }

  type Accumulated = {
    id: string;
    canonicalName: string;
    displayName: string | null;
    description: string | null;
    offerings: ReturnType<typeof buildOfferingInfo>[];
  };
  const byModel = new Map<string, Accumulated>();

  for (const o of offerings) {
    let entry = byModel.get(o.model.id);
    if (entry === undefined) {
      entry = {
        id: o.model.id,
        canonicalName: o.model.canonicalName,
        displayName: o.model.displayName,
        description: o.model.description,
        offerings: [],
      };
      byModel.set(o.model.id, entry);
    }
    entry.offerings.push(buildOfferingInfo(o));
  }

  return [...byModel.values()].map((m) => ({
    ...m,
    offerings: m.offerings.sort(
      (a, b) =>
        a.priority - b.priority || a.providerName.localeCompare(b.providerName),
    ),
  }));
}

export type CreateModelDiscoveryRoutesDeps = {
  db: DB["db"];
  requireGrant: RequireGrant;
};

export function createModelDiscoveryRoutes({
  db,
  requireGrant,
}: CreateModelDiscoveryRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  app.get(
    "/",
    requireGrant("model:*", "read"),
    describeRoute({
      tags: ["Discovery"],
      summary: "List the models resolved for the tenant",
      description:
        "Returns the tenant's resolved catalog: every model visible after applying inheritance, shadowing, and disable suppression, broken down by the providers that offer it with each offering's active price per currency.",
      responses: {
        200: {
          description: "Resolved models",
          content: {
            "application/json": { schema: resolver(ModelInfo.array()) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const offerings = await listVisibleOfferings(db, tenantCtx.id);

      const offeringIds = offerings.map((o) => o.offering.id);
      const priceRows =
        offeringIds.length > 0
          ? await db.query.modelPricing.findMany({
              where: inArray(modelPricing.offeringId, offeringIds),
            })
          : [];

      const models = composeDiscoveredModels(offerings, priceRows, new Date());
      return c.json(models);
    },
  );

  return app;
}
