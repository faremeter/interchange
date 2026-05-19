import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { oauthClient, provider } from "@interchange/db/schema";
import { getAncestorChain, parseOAuthClientRow } from "@interchange/db";
import type { DB } from "@interchange/db";
import {
  CreateOAuthClient,
  UpdateOAuthClient,
  OAuthClientResponse,
  ErrorResponse,
  paginatedSchema,
} from "@interchange/types";

import type { TenantEnv } from "../context";
import { first, ts } from "../format";
import { generateId } from "@interchange/hub-common";
import { idResource } from "../middleware/grant";
import type { RequireGrant } from "../middleware/grant";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";

function formatOAuthClient(row: typeof oauthClient.$inferSelect) {
  const parsed = parseOAuthClientRow(row);
  return {
    id: parsed.id,
    tenantId: parsed.tenantId,
    providerId: parsed.providerId,
    name: parsed.name,
    redirectUris: parsed.redirectUris ?? null,
    defaultScopes: parsed.defaultScopes ?? null,
    metadata: parsed.metadata,
    createdAt: ts(parsed.createdAt),
    updatedAt: ts(parsed.updatedAt),
  };
}

export type CreateOAuthClientRoutesDeps = {
  db: DB["db"];
  requireGrant: RequireGrant;
};

export function createOAuthClientRoutes({
  db,
  requireGrant,
}: CreateOAuthClientRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  app.get(
    "/",
    requireGrant("oauth_client:*", "read"),
    describeRoute({
      tags: ["OAuth Clients"],
      summary: "List OAuth client registrations",
      description:
        "Lists OAuth client registrations for the tenant. Secrets are never returned.",
      parameters: [...pageParameters],
      responses: {
        200: {
          description: "List of OAuth clients",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(OAuthClientResponse)),
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

      const conditions = [eq(oauthClient.tenantId, tenantCtx.id)];
      if (cursor) {
        conditions.push(
          cursorCondition(oauthClient.createdAt, oauthClient.id, cursor),
        );
      }

      const rows = await db.query.oauthClient.findMany({
        where: and(...conditions),
        orderBy: pageOrder(oauthClient.createdAt, oauthClient.id),
        limit,
      });

      return c.json(
        paginatedResponse(rows.map(formatOAuthClient), rows, limit),
      );
    },
  );

  app.post(
    "/",
    requireGrant("oauth_client:*", "create"),
    describeRoute({
      tags: ["OAuth Clients"],
      summary: "Register an OAuth client",
      description:
        "Registers an OAuth client (client_id/client_secret) for a provider. The provider must exist in the tenant or its ancestors.",
      responses: {
        201: {
          description: "OAuth client registered",
          content: {
            "application/json": { schema: resolver(OAuthClientResponse) },
          },
        },
        400: {
          description: "Validation error",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        404: {
          description: "Provider not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        409: {
          description:
            "OAuth client already exists for this provider in this tenant",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", CreateOAuthClient),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const body = c.req.valid("json");

      const providerRow = await db.query.provider.findFirst({
        where: eq(provider.id, body.providerId),
      });
      if (!providerRow) {
        return c.json(
          { error: { code: "not_found", message: "Provider not found" } },
          404,
        );
      }

      const chain = await getAncestorChain(db, tenantCtx.id);
      if (!chain.includes(providerRow.tenantId)) {
        return c.json(
          { error: { code: "not_found", message: "Provider not found" } },
          404,
        );
      }

      const existing = await db.query.oauthClient.findFirst({
        where: and(
          eq(oauthClient.tenantId, tenantCtx.id),
          eq(oauthClient.providerId, body.providerId),
        ),
      });
      if (existing) {
        return c.json(
          {
            error: {
              code: "conflict",
              message:
                "OAuth client already exists for this provider in this tenant",
            },
          },
          409,
        );
      }

      const now = new Date();
      const row = first(
        await db
          .insert(oauthClient)
          .values({
            id: generateId("oauthClient"),
            tenantId: tenantCtx.id,
            providerId: body.providerId,
            name: body.name,
            clientId: body.clientId,
            clientSecret: body.clientSecret,
            redirectUris: body.redirectUris ?? null,
            defaultScopes: body.defaultScopes ?? null,
            metadata: body.metadata ?? null,
            createdAt: now,
            updatedAt: now,
          })
          .returning(),
      );

      return c.json(formatOAuthClient(row), 201);
    },
  );

  app.get(
    "/:oauthClientId",
    requireGrant(idResource("oauth_client", "oauthClientId"), "read"),
    describeRoute({
      tags: ["OAuth Clients"],
      summary: "Get OAuth client details",
      description: "Returns OAuth client metadata. Secrets are never included.",
      responses: {
        200: {
          description: "OAuth client details",
          content: {
            "application/json": { schema: resolver(OAuthClientResponse) },
          },
        },
        404: {
          description: "OAuth client not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const oauthClientId = c.req.param("oauthClientId");

      const row = await db.query.oauthClient.findFirst({
        where: and(
          eq(oauthClient.id, oauthClientId),
          eq(oauthClient.tenantId, tenantCtx.id),
        ),
      });

      if (!row) {
        return c.json(
          { error: { code: "not_found", message: "OAuth client not found" } },
          404,
        );
      }

      return c.json(formatOAuthClient(row));
    },
  );

  app.patch(
    "/:oauthClientId",
    requireGrant(idResource("oauth_client", "oauthClientId"), "manage"),
    describeRoute({
      tags: ["OAuth Clients"],
      summary: "Update an OAuth client registration",
      responses: {
        200: {
          description: "OAuth client updated",
          content: {
            "application/json": { schema: resolver(OAuthClientResponse) },
          },
        },
        404: {
          description: "OAuth client not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", UpdateOAuthClient),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const oauthClientId = c.req.param("oauthClientId");
      const body = c.req.valid("json");

      const updates: Record<string, unknown> = { updatedAt: new Date() };
      if (body.name !== undefined) updates["name"] = body.name;
      if (body.clientId !== undefined) updates["clientId"] = body.clientId;
      if (body.clientSecret !== undefined)
        updates["clientSecret"] = body.clientSecret;
      if (body.redirectUris !== undefined)
        updates["redirectUris"] = body.redirectUris;
      if (body.defaultScopes !== undefined)
        updates["defaultScopes"] = body.defaultScopes;
      if (body.metadata !== undefined) updates["metadata"] = body.metadata;

      const [updated] = await db
        .update(oauthClient)
        .set(updates)
        .where(
          and(
            eq(oauthClient.id, oauthClientId),
            eq(oauthClient.tenantId, tenantCtx.id),
          ),
        )
        .returning();

      if (!updated) {
        return c.json(
          { error: { code: "not_found", message: "OAuth client not found" } },
          404,
        );
      }

      return c.json(formatOAuthClient(updated));
    },
  );

  app.delete(
    "/:oauthClientId",
    requireGrant(idResource("oauth_client", "oauthClientId"), "manage"),
    describeRoute({
      tags: ["OAuth Clients"],
      summary: "Remove an OAuth client registration",
      responses: {
        204: { description: "OAuth client removed" },
        404: {
          description: "OAuth client not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const oauthClientId = c.req.param("oauthClientId");

      const deleted = await db
        .delete(oauthClient)
        .where(
          and(
            eq(oauthClient.id, oauthClientId),
            eq(oauthClient.tenantId, tenantCtx.id),
          ),
        )
        .returning();

      if (deleted.length === 0) {
        return c.json(
          { error: { code: "not_found", message: "OAuth client not found" } },
          404,
        );
      }

      return c.body(null, 204);
    },
  );

  return app;
}
