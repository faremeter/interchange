import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { provider } from "@interchange/db/schema";
import { getAncestorChain, parseProviderRow } from "@interchange/db";
import type { DB } from "@interchange/db";
import {
  CreateProvider,
  UpdateProvider,
  ProviderResponse,
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

function formatProvider(row: typeof provider.$inferSelect) {
  const parsed = parseProviderRow(row);
  return {
    id: parsed.id,
    tenantId: parsed.tenantId,
    name: parsed.name,
    plugin: parsed.plugin,
    authorizationUrl: parsed.authorizationUrl ?? null,
    tokenUrl: parsed.tokenUrl ?? null,
    userInfoUrl: parsed.userInfoUrl ?? null,
    scopes: parsed.scopes ?? null,
    metadata: parsed.metadata,
    createdAt: ts(parsed.createdAt),
    updatedAt: ts(parsed.updatedAt),
  };
}

export type CreateProviderRoutesDeps = {
  db: DB["db"];
};

export function createProviderRoutes({
  db,
}: CreateProviderRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  app.get(
    "/",
    requireGrant("provider:*", "read"),
    describeRoute({
      tags: ["Providers"],
      summary: "List providers",
      description:
        "Lists provider definitions for the tenant, including those inherited from ancestor tenants.",
      parameters: [
        {
          name: "inherited",
          in: "query",
          schema: { type: "string", enum: ["true", "false"] },
        },
        ...pageParameters,
      ],
      responses: {
        200: {
          description: "List of providers",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(ProviderResponse)),
            },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const inherited = c.req.query("inherited") !== "false";
      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      if (inherited) {
        const chain = await getAncestorChain(db, tenantCtx.id);
        const seen = new Set<string>();
        const items: ReturnType<typeof formatProvider>[] = [];

        for (const tid of chain) {
          const rows = await db.query.provider.findMany({
            where: eq(provider.tenantId, tid),
          });
          for (const row of rows) {
            if (!seen.has(row.name)) {
              seen.add(row.name);
              items.push(formatProvider(row));
            }
          }
        }

        return c.json({ data: items, nextCursor: null });
      }

      const conditions = [eq(provider.tenantId, tenantCtx.id)];
      if (cursor) {
        conditions.push(
          cursorCondition(provider.createdAt, provider.id, cursor),
        );
      }

      const rows = await db.query.provider.findMany({
        where: and(...conditions),
        orderBy: pageOrder(provider.createdAt, provider.id),
        limit,
      });

      return c.json(paginatedResponse(rows.map(formatProvider), rows, limit));
    },
  );

  app.post(
    "/",
    requireGrant("provider:*", "create"),
    describeRoute({
      tags: ["Providers"],
      summary: "Create a provider definition",
      description:
        "Defines a new service provider for the tenant. The plugin field determines how Interchange integrates with the service.",
      responses: {
        201: {
          description: "Provider created",
          content: {
            "application/json": { schema: resolver(ProviderResponse) },
          },
        },
        400: {
          description: "Validation error",
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
    validator("json", CreateProvider),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const body = c.req.valid("json");

      const existing = await db.query.provider.findFirst({
        where: and(
          eq(provider.tenantId, tenantCtx.id),
          eq(provider.name, body.name),
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
          .insert(provider)
          .values({
            id: generateId("provider"),
            tenantId: tenantCtx.id,
            name: body.name,
            plugin: body.plugin,
            authorizationUrl: body.authorizationUrl ?? null,
            tokenUrl: body.tokenUrl ?? null,
            userInfoUrl: body.userInfoUrl ?? null,
            scopes: body.scopes ?? null,
            metadata: body.metadata ?? null,
            createdAt: now,
            updatedAt: now,
          })
          .returning(),
      );

      return c.json(formatProvider(row), 201);
    },
  );

  app.get(
    "/:providerId",
    requireGrant(idResource("provider", "providerId"), "read"),
    describeRoute({
      tags: ["Providers"],
      summary: "Get provider details",
      responses: {
        200: {
          description: "Provider details",
          content: {
            "application/json": { schema: resolver(ProviderResponse) },
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

      const row = await db.query.provider.findFirst({
        where: eq(provider.id, providerId),
      });

      if (!row) {
        return c.json(
          { error: { code: "not_found", message: "Provider not found" } },
          404,
        );
      }

      const chain = await getAncestorChain(db, tenantCtx.id);
      if (!chain.includes(row.tenantId)) {
        return c.json(
          { error: { code: "not_found", message: "Provider not found" } },
          404,
        );
      }

      return c.json(formatProvider(row));
    },
  );

  app.patch(
    "/:providerId",
    requireGrant(idResource("provider", "providerId"), "manage"),
    describeRoute({
      tags: ["Providers"],
      summary: "Update a provider definition",
      description: "Only providers owned by this tenant can be updated.",
      responses: {
        200: {
          description: "Provider updated",
          content: {
            "application/json": { schema: resolver(ProviderResponse) },
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
    validator("json", UpdateProvider),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const providerId = c.req.param("providerId");
      const body = c.req.valid("json");

      const updates: Record<string, unknown> = { updatedAt: new Date() };
      if (body.name !== undefined) updates["name"] = body.name;
      if (body.plugin !== undefined) updates["plugin"] = body.plugin;
      if (body.authorizationUrl !== undefined)
        updates["authorizationUrl"] = body.authorizationUrl;
      if (body.tokenUrl !== undefined) updates["tokenUrl"] = body.tokenUrl;
      if (body.userInfoUrl !== undefined)
        updates["userInfoUrl"] = body.userInfoUrl;
      if (body.scopes !== undefined) updates["scopes"] = body.scopes;
      if (body.metadata !== undefined) updates["metadata"] = body.metadata;

      const [updated] = await db
        .update(provider)
        .set(updates)
        .where(
          and(eq(provider.id, providerId), eq(provider.tenantId, tenantCtx.id)),
        )
        .returning();

      if (!updated) {
        return c.json(
          { error: { code: "not_found", message: "Provider not found" } },
          404,
        );
      }

      return c.json(formatProvider(updated));
    },
  );

  app.delete(
    "/:providerId",
    requireGrant(idResource("provider", "providerId"), "manage"),
    describeRoute({
      tags: ["Providers"],
      summary: "Remove a provider definition",
      description: "Only providers owned by this tenant can be removed.",
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
        .delete(provider)
        .where(
          and(eq(provider.id, providerId), eq(provider.tenantId, tenantCtx.id)),
        )
        .returning();

      if (deleted.length === 0) {
        return c.json(
          { error: { code: "not_found", message: "Provider not found" } },
          404,
        );
      }

      return c.body(null, 204);
    },
  );

  return app;
}
