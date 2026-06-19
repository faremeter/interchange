import { eq, and, ne } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { modelProvider } from "@intx/db/schema";
import type { DB } from "@intx/db";
import {
  CreateModelProvider,
  UpdateModelProvider,
  ModelProviderResponse,
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

export function formatModelProvider(row: typeof modelProvider.$inferSelect) {
  return {
    id: row.id,
    tenantId: row.tenantId,
    name: row.name,
    plugin: row.plugin,
    baseURL: row.baseURL,
    credentialId: row.credentialId,
    walletId: row.walletId,
    disabled: row.disabled,
    createdAt: ts(row.createdAt),
    updatedAt: ts(row.updatedAt),
  };
}

export type CreateModelProviderRoutesDeps = {
  db: DB["db"];
  sidecarRouter: SidecarRouter;
  requireGrant: RequireGrant;
};

export function createModelProviderRoutes({
  db,
  sidecarRouter,
  requireGrant,
}: CreateModelProviderRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  app.get(
    "/",
    requireGrant("model-provider:*", "read"),
    describeRoute({
      tags: ["Catalog"],
      summary: "List model providers owned by the tenant",
      description:
        "Lists the model providers created directly on this tenant. Providers inherited from ancestor tenants are not included.",
      parameters: [...pageParameters],
      responses: {
        200: {
          description: "List of model providers",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(ModelProviderResponse)),
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

      const conditions = [eq(modelProvider.tenantId, tenantCtx.id)];
      if (cursor) {
        conditions.push(
          cursorCondition(modelProvider.createdAt, modelProvider.id, cursor),
        );
      }

      const rows = await db.query.modelProvider.findMany({
        where: and(...conditions),
        orderBy: pageOrder(modelProvider.createdAt, modelProvider.id),
        limit,
      });
      return c.json(
        paginatedResponse(rows.map(formatModelProvider), rows, limit),
      );
    },
  );

  app.post(
    "/",
    requireGrant("model-provider:*", "create"),
    describeRoute({
      tags: ["Catalog"],
      summary: "Create a model provider",
      description:
        "Registers an inference endpoint authenticated by exactly one of a credential or a wallet. Supplying both or neither is rejected.",
      responses: {
        201: {
          description: "Provider created",
          content: {
            "application/json": { schema: resolver(ModelProviderResponse) },
          },
        },
        400: {
          description: "Exactly one of credentialId or walletId is required",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        409: {
          description: "Provider name already exists in this tenant",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", CreateModelProvider),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const body = c.req.valid("json");

      const hasCredential =
        body.credentialId !== undefined && body.credentialId !== null;
      const hasWallet = body.walletId !== undefined && body.walletId !== null;
      if (hasCredential === hasWallet) {
        return c.json(
          {
            error: {
              code: "invalid_request",
              message:
                "Exactly one of credentialId or walletId must be provided",
            },
          },
          400,
        );
      }

      const existing = await db.query.modelProvider.findFirst({
        where: and(
          eq(modelProvider.tenantId, tenantCtx.id),
          eq(modelProvider.name, body.name),
        ),
      });
      if (existing) {
        return c.json(
          {
            error: {
              code: "conflict",
              message: "Provider name already exists in this tenant",
            },
          },
          409,
        );
      }

      const now = new Date();
      const row = first(
        await db
          .insert(modelProvider)
          .values({
            id: generateId("modelProvider"),
            tenantId: tenantCtx.id,
            name: body.name,
            plugin: body.plugin,
            baseURL: body.baseURL,
            credentialId: hasCredential ? body.credentialId : null,
            walletId: hasWallet ? body.walletId : null,
            createdAt: now,
            updatedAt: now,
          })
          .returning(),
      );

      return c.json(formatModelProvider(row), 201);
    },
  );

  app.get(
    "/:providerId",
    requireGrant(idResource("model-provider", "providerId"), "read"),
    describeRoute({
      tags: ["Catalog"],
      summary: "Get a model provider",
      responses: {
        200: {
          description: "Provider details",
          content: {
            "application/json": { schema: resolver(ModelProviderResponse) },
          },
        },
        404: {
          description: "Provider not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const providerId = c.req.param("providerId");
      const row = await db.query.modelProvider.findFirst({
        where: and(
          eq(modelProvider.id, providerId),
          eq(modelProvider.tenantId, tenantCtx.id),
        ),
      });
      if (!row) {
        return c.json(
          { error: { code: "not_found", message: "Provider not found" } },
          404,
        );
      }
      return c.json(formatModelProvider(row));
    },
  );

  app.patch(
    "/:providerId",
    requireGrant(idResource("model-provider", "providerId"), "manage"),
    describeRoute({
      tags: ["Catalog"],
      summary: "Update a model provider",
      description:
        "Updates a provider's name, base URL, or disabled flag. Changing the authentication binding is not supported; delete and recreate the provider to repoint it.",
      responses: {
        200: {
          description: "Provider updated",
          content: {
            "application/json": { schema: resolver(ModelProviderResponse) },
          },
        },
        404: {
          description: "Provider not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        409: {
          description: "Provider name already exists in this tenant",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", UpdateModelProvider),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const providerId = c.req.param("providerId");
      const body = c.req.valid("json");

      if (body.name !== undefined) {
        const clash = await db.query.modelProvider.findFirst({
          where: and(
            eq(modelProvider.tenantId, tenantCtx.id),
            eq(modelProvider.name, body.name),
            ne(modelProvider.id, providerId),
          ),
        });
        if (clash) {
          return c.json(
            {
              error: {
                code: "conflict",
                message: "Provider name already exists in this tenant",
              },
            },
            409,
          );
        }
      }

      const updates: Record<string, unknown> = { updatedAt: new Date() };
      if (body.name !== undefined) updates["name"] = body.name;
      if (body.baseURL !== undefined) updates["baseURL"] = body.baseURL;
      if (body.disabled !== undefined) updates["disabled"] = body.disabled;

      const [updated] = await db
        .update(modelProvider)
        .set(updates)
        .where(
          and(
            eq(modelProvider.id, providerId),
            eq(modelProvider.tenantId, tenantCtx.id),
          ),
        )
        .returning();

      if (!updated) {
        return c.json(
          { error: { code: "not_found", message: "Provider not found" } },
          404,
        );
      }

      void pushSourceUpdatesSubtree(db, sidecarRouter, tenantCtx.id);
      return c.json(formatModelProvider(updated));
    },
  );

  app.delete(
    "/:providerId",
    requireGrant(idResource("model-provider", "providerId"), "manage"),
    describeRoute({
      tags: ["Catalog"],
      summary: "Delete a model provider",
      description:
        "Removes the provider and cascades to the offerings that reference it. Running instances resolved through those offerings fail over to the next eligible source.",
      responses: {
        204: { description: "Provider removed" },
        404: {
          description: "Provider not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const providerId = c.req.param("providerId");
      const deleted = await db
        .delete(modelProvider)
        .where(
          and(
            eq(modelProvider.id, providerId),
            eq(modelProvider.tenantId, tenantCtx.id),
          ),
        )
        .returning();
      if (deleted.length === 0) {
        return c.json(
          { error: { code: "not_found", message: "Provider not found" } },
          404,
        );
      }
      void pushSourceUpdatesSubtree(db, sidecarRouter, tenantCtx.id);
      return c.body(null, 204);
    },
  );

  return app;
}
