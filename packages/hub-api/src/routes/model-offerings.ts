import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import {
  model,
  modelProvider,
  modelOffering,
  modelPricing,
} from "@intx/db/schema";
import type { DB } from "@intx/db";
import {
  CreateModelOffering,
  UpdateModelOffering,
  CreatePricingRow,
  ModelOfferingResponse,
  PricingRowResponse,
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

export function formatModelOffering(row: typeof modelOffering.$inferSelect) {
  return {
    id: row.id,
    tenantId: row.tenantId,
    modelId: row.modelId,
    providerId: row.providerId,
    priority: row.priority,
    deploymentTags: row.deploymentTags,
    capabilities: row.capabilities,
    disabled: row.disabled,
    createdAt: ts(row.createdAt),
    updatedAt: ts(row.updatedAt),
  };
}

export function formatPricingRow(row: typeof modelPricing.$inferSelect) {
  return {
    id: row.id,
    tenantId: row.tenantId,
    offeringId: row.offeringId,
    currency: row.currency,
    inputTokenPrice: row.inputTokenPrice,
    outputTokenPrice: row.outputTokenPrice,
    cacheReadTokenPrice: row.cacheReadTokenPrice,
    cacheWriteTokenPrice: row.cacheWriteTokenPrice,
    thinkingTokenPrice: row.thinkingTokenPrice,
    perRequestFee: row.perRequestFee,
    perImageFee: row.perImageFee,
    perAudioFee: row.perAudioFee,
    effectiveFrom: ts(row.effectiveFrom),
    createdAt: ts(row.createdAt),
  };
}

export type CreateModelOfferingRoutesDeps = {
  db: DB["db"];
  sidecarRouter: SidecarRouter;
  requireGrant: RequireGrant;
};

export function createModelOfferingRoutes({
  db,
  sidecarRouter,
  requireGrant,
}: CreateModelOfferingRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  app.get(
    "/",
    requireGrant("model-offering:*", "read"),
    describeRoute({
      tags: ["Catalog"],
      summary: "List model offerings owned by the tenant",
      description:
        "Lists the model offerings created directly on this tenant. Offerings inherited from ancestor tenants are not included; use the model discovery endpoint to see the resolved catalog.",
      parameters: [...pageParameters],
      responses: {
        200: {
          description: "List of model offerings",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(ModelOfferingResponse)),
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

      const conditions = [eq(modelOffering.tenantId, tenantCtx.id)];
      if (cursor) {
        conditions.push(
          cursorCondition(modelOffering.createdAt, modelOffering.id, cursor),
        );
      }

      const rows = await db.query.modelOffering.findMany({
        where: and(...conditions),
        orderBy: pageOrder(modelOffering.createdAt, modelOffering.id),
        limit,
      });
      return c.json(
        paginatedResponse(rows.map(formatModelOffering), rows, limit),
      );
    },
  );

  app.post(
    "/",
    requireGrant("model-offering:*", "create"),
    describeRoute({
      tags: ["Catalog"],
      summary: "Create a model offering",
      description:
        "Pairs a tenant-owned model with a tenant-owned provider. To offer an inherited model or provider, first create a tenant-local copy of it (shadowing).",
      responses: {
        201: {
          description: "Offering created",
          content: {
            "application/json": { schema: resolver(ModelOfferingResponse) },
          },
        },
        404: {
          description: "Model or provider not found in this tenant",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        409: {
          description: "Offering already exists for this model and provider",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", CreateModelOffering),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const body = c.req.valid("json");

      const modelRow = await db.query.model.findFirst({
        where: and(
          eq(model.id, body.modelId),
          eq(model.tenantId, tenantCtx.id),
        ),
      });
      if (!modelRow) {
        return c.json(
          {
            error: {
              code: "not_found",
              message: "Model not found in this tenant",
            },
          },
          404,
        );
      }

      const providerRow = await db.query.modelProvider.findFirst({
        where: and(
          eq(modelProvider.id, body.providerId),
          eq(modelProvider.tenantId, tenantCtx.id),
        ),
      });
      if (!providerRow) {
        return c.json(
          {
            error: {
              code: "not_found",
              message: "Provider not found in this tenant",
            },
          },
          404,
        );
      }

      const existing = await db.query.modelOffering.findFirst({
        where: and(
          eq(modelOffering.tenantId, tenantCtx.id),
          eq(modelOffering.modelId, body.modelId),
          eq(modelOffering.providerId, body.providerId),
        ),
      });
      if (existing) {
        return c.json(
          {
            error: {
              code: "conflict",
              message: "An offering for this model and provider already exists",
            },
          },
          409,
        );
      }

      const now = new Date();
      const row = first(
        await db
          .insert(modelOffering)
          .values({
            id: generateId("modelOffering"),
            tenantId: tenantCtx.id,
            modelId: body.modelId,
            providerId: body.providerId,
            priority: body.priority ?? 0,
            deploymentTags: body.deploymentTags ?? [],
            capabilities: body.capabilities ?? [],
            createdAt: now,
            updatedAt: now,
          })
          .returning(),
      );

      void pushSourceUpdatesSubtree(db, sidecarRouter, tenantCtx.id);
      return c.json(formatModelOffering(row), 201);
    },
  );

  app.get(
    "/:offeringId",
    requireGrant(idResource("model-offering", "offeringId"), "read"),
    describeRoute({
      tags: ["Catalog"],
      summary: "Get a model offering",
      responses: {
        200: {
          description: "Offering details",
          content: {
            "application/json": { schema: resolver(ModelOfferingResponse) },
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
      const row = await db.query.modelOffering.findFirst({
        where: and(
          eq(modelOffering.id, offeringId),
          eq(modelOffering.tenantId, tenantCtx.id),
        ),
      });
      if (!row) {
        return c.json(
          { error: { code: "not_found", message: "Offering not found" } },
          404,
        );
      }
      return c.json(formatModelOffering(row));
    },
  );

  app.patch(
    "/:offeringId",
    requireGrant(idResource("model-offering", "offeringId"), "manage"),
    describeRoute({
      tags: ["Catalog"],
      summary: "Update a model offering",
      responses: {
        200: {
          description: "Offering updated",
          content: {
            "application/json": { schema: resolver(ModelOfferingResponse) },
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
    validator("json", UpdateModelOffering),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const offeringId = c.req.param("offeringId");
      const body = c.req.valid("json");

      const updates: Record<string, unknown> = { updatedAt: new Date() };
      if (body.priority !== undefined) updates["priority"] = body.priority;
      if (body.deploymentTags !== undefined)
        updates["deploymentTags"] = body.deploymentTags;
      if (body.capabilities !== undefined)
        updates["capabilities"] = body.capabilities;
      if (body.disabled !== undefined) updates["disabled"] = body.disabled;

      const [updated] = await db
        .update(modelOffering)
        .set(updates)
        .where(
          and(
            eq(modelOffering.id, offeringId),
            eq(modelOffering.tenantId, tenantCtx.id),
          ),
        )
        .returning();

      if (!updated) {
        return c.json(
          { error: { code: "not_found", message: "Offering not found" } },
          404,
        );
      }

      void pushSourceUpdatesSubtree(db, sidecarRouter, tenantCtx.id);
      return c.json(formatModelOffering(updated));
    },
  );

  app.delete(
    "/:offeringId",
    requireGrant(idResource("model-offering", "offeringId"), "manage"),
    describeRoute({
      tags: ["Catalog"],
      summary: "Delete a model offering",
      description:
        "Removes the offering and its pricing history. Running instances resolved through it fail over to the next eligible source.",
      responses: {
        204: { description: "Offering removed" },
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
        .delete(modelOffering)
        .where(
          and(
            eq(modelOffering.id, offeringId),
            eq(modelOffering.tenantId, tenantCtx.id),
          ),
        )
        .returning();
      if (deleted.length === 0) {
        return c.json(
          { error: { code: "not_found", message: "Offering not found" } },
          404,
        );
      }
      void pushSourceUpdatesSubtree(db, sidecarRouter, tenantCtx.id);
      return c.body(null, 204);
    },
  );

  app.get(
    "/:offeringId/pricing",
    requireGrant("model-pricing:*", "read"),
    describeRoute({
      tags: ["Catalog"],
      summary: "List an offering's pricing history",
      description:
        "Returns the full append-only pricing history for an offering, every currency and effective-from date, newest first.",
      responses: {
        200: {
          description: "Pricing rows",
          content: {
            "application/json": {
              schema: resolver(PricingRowResponse.array()),
            },
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

      const offeringRow = await db.query.modelOffering.findFirst({
        where: and(
          eq(modelOffering.id, offeringId),
          eq(modelOffering.tenantId, tenantCtx.id),
        ),
      });
      if (!offeringRow) {
        return c.json(
          { error: { code: "not_found", message: "Offering not found" } },
          404,
        );
      }

      const rows = await db.query.modelPricing.findMany({
        where: eq(modelPricing.offeringId, offeringId),
        orderBy: (p, { desc }) => desc(p.effectiveFrom),
      });
      return c.json(rows.map(formatPricingRow));
    },
  );

  app.post(
    "/:offeringId/pricing",
    requireGrant("model-pricing:*", "create"),
    describeRoute({
      tags: ["Catalog"],
      summary: "Add a pricing row to an offering",
      description:
        "Appends a pricing row. Pricing is append-only: a price change inserts a new row with a later effective-from rather than editing an existing one, so historical cost attribution stays accurate.",
      responses: {
        201: {
          description: "Pricing row created",
          content: {
            "application/json": { schema: resolver(PricingRowResponse) },
          },
        },
        400: {
          description: "effectiveFrom is not a valid timestamp",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        404: {
          description: "Offering not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        409: {
          description:
            "A pricing row already exists for this currency and effective-from",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", CreatePricingRow),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const offeringId = c.req.param("offeringId");
      const body = c.req.valid("json");

      const offeringRow = await db.query.modelOffering.findFirst({
        where: and(
          eq(modelOffering.id, offeringId),
          eq(modelOffering.tenantId, tenantCtx.id),
        ),
      });
      if (!offeringRow) {
        return c.json(
          { error: { code: "not_found", message: "Offering not found" } },
          404,
        );
      }

      let effectiveFrom = new Date();
      if (body.effectiveFrom !== undefined) {
        effectiveFrom = new Date(body.effectiveFrom);
        if (Number.isNaN(effectiveFrom.valueOf())) {
          return c.json(
            {
              error: {
                code: "invalid_request",
                message: "effectiveFrom must be a valid ISO-8601 timestamp",
              },
            },
            400,
          );
        }
      }

      const existing = await db.query.modelPricing.findFirst({
        where: and(
          eq(modelPricing.offeringId, offeringId),
          eq(modelPricing.currency, body.currency),
          eq(modelPricing.effectiveFrom, effectiveFrom),
        ),
      });
      if (existing) {
        return c.json(
          {
            error: {
              code: "conflict",
              message:
                "A pricing row already exists for this currency and effective-from",
            },
          },
          409,
        );
      }

      const row = first(
        await db
          .insert(modelPricing)
          .values({
            id: generateId("modelPricing"),
            tenantId: tenantCtx.id,
            offeringId,
            currency: body.currency,
            inputTokenPrice: body.inputTokenPrice ?? null,
            outputTokenPrice: body.outputTokenPrice ?? null,
            cacheReadTokenPrice: body.cacheReadTokenPrice ?? null,
            cacheWriteTokenPrice: body.cacheWriteTokenPrice ?? null,
            thinkingTokenPrice: body.thinkingTokenPrice ?? null,
            perRequestFee: body.perRequestFee ?? null,
            perImageFee: body.perImageFee ?? null,
            perAudioFee: body.perAudioFee ?? null,
            effectiveFrom,
            createdAt: new Date(),
          })
          .returning(),
      );

      // No source-update push: pricing is catalog metadata and does not
      // participate in inference-source resolution, so a price change cannot
      // alter any running instance's resolved sources.
      return c.json(formatPricingRow(row), 201);
    },
  );

  return app;
}
