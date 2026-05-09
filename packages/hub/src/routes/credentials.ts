import { eq, and, isNull } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { credential, provider } from "@interchange/db/schema";
import {
  getAncestorChain,
  resolveCredentialByName,
  parseCredentialRow,
} from "@interchange/db";
import {
  CreateCredential,
  UpdateCredential,
  CredentialResponse,
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
import { pushProviderUpdates } from "../credential-push";

function formatCredential(row: typeof credential.$inferSelect) {
  const parsed = parseCredentialRow(row);
  return {
    id: parsed.id,
    tenantId: parsed.tenantId,
    providerId: parsed.providerId,
    principalId: parsed.principalId ?? null,
    oauthClientId: parsed.oauthClientId ?? null,
    name: parsed.name,
    type: parsed.type,
    description: parsed.description ?? null,
    scopes: parsed.scopes ?? null,
    expiresAt: parsed.expiresAt ? ts(parsed.expiresAt) : null,
    status: parsed.status,
    metadata: parsed.metadata,
    createdAt: ts(parsed.createdAt),
    updatedAt: ts(parsed.updatedAt),
  };
}

const app = new Hono<TenantEnv>();

app.get(
  "/",
  requireGrant("credential:*", "read"),
  describeRoute({
    tags: ["Credentials"],
    summary: "List credentials",
    description:
      "Lists credential metadata. Secrets are never returned. Filterable by owner type.",
    parameters: [
      {
        name: "owner",
        in: "query",
        schema: { type: "string", enum: ["me", "org", "all"] },
      },
      ...pageParameters,
    ],
    responses: {
      200: {
        description: "List of credentials",
        content: {
          "application/json": {
            schema: resolver(paginatedSchema(CredentialResponse)),
          },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const principalCtx = c.get("principal");
    const db = c.get("db");
    const owner = c.req.query("owner") ?? "all";
    const { limit, cursor } = parsePageParams({
      cursor: c.req.query("cursor"),
      limit: c.req.query("limit"),
    });

    const conditions = [eq(credential.tenantId, tenantCtx.id)];

    if (owner === "me") {
      conditions.push(eq(credential.principalId, principalCtx.id));
    } else if (owner === "org") {
      conditions.push(isNull(credential.principalId));
    }

    if (cursor) {
      conditions.push(
        cursorCondition(credential.createdAt, credential.id, cursor),
      );
    }

    const rows = await db.query.credential.findMany({
      where: and(...conditions),
      orderBy: pageOrder(credential.createdAt, credential.id),
      limit,
    });

    return c.json(paginatedResponse(rows.map(formatCredential), rows, limit));
  },
);

app.post(
  "/",
  requireGrant("credential:*", "create"),
  describeRoute({
    tags: ["Credentials"],
    summary: "Store a credential",
    description:
      "Stores a credential (API key, OAuth token, etc.). The secret is stored securely and never returned in subsequent reads. A provider must be specified.",
    responses: {
      201: {
        description: "Credential stored",
        content: {
          "application/json": { schema: resolver(CredentialResponse) },
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
        description: "Credential name already exists in this tenant",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", CreateCredential),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const body = c.req.valid("json");
    const db = c.get("db");

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

    const existing = await db.query.credential.findFirst({
      where: and(
        eq(credential.tenantId, tenantCtx.id),
        eq(credential.name, body.name),
      ),
    });
    if (existing) {
      return c.json(
        {
          error: {
            code: "conflict",
            message: "Credential name already exists in this tenant",
          },
        },
        409,
      );
    }

    const now = new Date();
    const row = first(
      await db
        .insert(credential)
        .values({
          id: generateId("credential"),
          tenantId: tenantCtx.id,
          providerId: body.providerId,
          principalId: body.principalId ?? null,
          oauthClientId: body.oauthClientId ?? null,
          name: body.name,
          type: body.type,
          description: body.description ?? null,
          secret: body.secret,
          refreshSecret: body.refreshSecret ?? null,
          scopes: body.scopes ?? null,
          expiresAt: body.expiresAt ? new Date(body.expiresAt) : null,
          metadata: body.metadata ?? null,
          createdAt: now,
          updatedAt: now,
        })
        .returning(),
    );

    return c.json(formatCredential(row), 201);
  },
);

app.get(
  "/resolve/:name",
  requireGrant("credential:*", "read"),
  describeRoute({
    tags: ["Credentials"],
    summary: "Resolve a credential by name",
    description:
      "Resolves a credential by name, walking the tenant hierarchy. Returns metadata only (no secret). Useful for discovering which credential an agent would get.",
    responses: {
      200: {
        description: "Credential metadata",
        content: {
          "application/json": { schema: resolver(CredentialResponse) },
        },
      },
      404: {
        description: "Credential not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const name = c.req.param("name");
    const db = c.get("db");

    const row = await resolveCredentialByName(db, tenantCtx.id, name);

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Credential not found" } },
        404,
      );
    }

    return c.json(formatCredential(row));
  },
);

app.get(
  "/:credentialId",
  requireGrant(idResource("credential", "credentialId"), "read"),
  describeRoute({
    tags: ["Credentials"],
    summary: "Get credential metadata",
    description:
      "Returns credential metadata. The secret is never included. Supports hierarchy-aware access.",
    responses: {
      200: {
        description: "Credential metadata",
        content: {
          "application/json": { schema: resolver(CredentialResponse) },
        },
      },
      404: {
        description: "Credential not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const credentialId = c.req.param("credentialId");
    const db = c.get("db");

    const row = await db.query.credential.findFirst({
      where: eq(credential.id, credentialId),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Credential not found" } },
        404,
      );
    }

    const chain = await getAncestorChain(db, tenantCtx.id);
    if (!chain.includes(row.tenantId)) {
      return c.json(
        { error: { code: "not_found", message: "Credential not found" } },
        404,
      );
    }

    return c.json(formatCredential(row));
  },
);

app.patch(
  "/:credentialId",
  requireGrant(idResource("credential", "credentialId"), "manage"),
  describeRoute({
    tags: ["Credentials"],
    summary: "Rotate or update a credential",
    description: "Only credentials owned by this tenant can be updated.",
    responses: {
      200: {
        description: "Credential updated",
        content: {
          "application/json": { schema: resolver(CredentialResponse) },
        },
      },
      404: {
        description: "Credential not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", UpdateCredential),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const credentialId = c.req.param("credentialId");
    const body = c.req.valid("json");
    const db = c.get("db");

    const updates: Record<string, unknown> = { updatedAt: new Date() };
    if (body.name !== undefined) updates["name"] = body.name;
    if (body.description !== undefined)
      updates["description"] = body.description;
    if (body.secret !== undefined) updates["secret"] = body.secret;
    if (body.refreshSecret !== undefined)
      updates["refreshSecret"] = body.refreshSecret;
    if (body.scopes !== undefined) updates["scopes"] = body.scopes;
    if (body.expiresAt !== undefined)
      updates["expiresAt"] = body.expiresAt ? new Date(body.expiresAt) : null;
    if (body.status !== undefined) updates["status"] = body.status;
    if (body.metadata !== undefined) updates["metadata"] = body.metadata;

    const [updated] = await db
      .update(credential)
      .set(updates)
      .where(
        and(
          eq(credential.id, credentialId),
          eq(credential.tenantId, tenantCtx.id),
        ),
      )
      .returning();

    if (!updated) {
      return c.json(
        { error: { code: "not_found", message: "Credential not found" } },
        404,
      );
    }

    // If the secret was updated, push new provider config to running instances.
    if (body.secret !== undefined) {
      const sidecarRouter = c.get("sidecarRouter");
      void pushProviderUpdates(db, sidecarRouter, updated.tenantId);
    }

    return c.json(formatCredential(updated));
  },
);

app.delete(
  "/:credentialId",
  requireGrant(idResource("credential", "credentialId"), "manage"),
  describeRoute({
    tags: ["Credentials"],
    summary: "Revoke a credential",
    description: "Only credentials owned by this tenant can be revoked.",
    responses: {
      204: {
        description: "Credential revoked",
      },
      404: {
        description: "Credential not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const credentialId = c.req.param("credentialId");
    const db = c.get("db");

    const deleted = await db
      .delete(credential)
      .where(
        and(
          eq(credential.id, credentialId),
          eq(credential.tenantId, tenantCtx.id),
        ),
      )
      .returning();

    if (deleted.length === 0) {
      return c.json(
        { error: { code: "not_found", message: "Credential not found" } },
        404,
      );
    }

    return c.body(null, 204);
  },
);

export { app as credentialRoutes };
