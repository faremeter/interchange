import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { credential } from "@interchange/db/schema";
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

function formatCredential(row: typeof credential.$inferSelect) {
  return {
    id: row.id,
    tenantId: row.tenantId,
    name: row.name,
    type: row.type as "api_key" | "oauth_token" | "certificate" | "other",
    description: row.description ?? null,
    metadata: (row.metadata as Record<string, unknown> | null) ?? null,
    createdAt: ts(row.createdAt),
    updatedAt: ts(row.updatedAt),
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
      "Lists credential metadata. Secrets are never returned. Access for agents is managed through capability grants.",
    parameters: [...pageParameters],
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
    const db = c.get("db");
    const { limit, cursor } = parsePageParams({
      cursor: c.req.query("cursor"),
      limit: c.req.query("limit"),
    });

    const conditions = [eq(credential.tenantId, tenantCtx.id)];
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
      "Stores a credential (API key, OAuth token, etc.). The secret is stored securely and never returned in subsequent reads.",
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
    },
  }),
  validator("json", CreateCredential),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const body = c.req.valid("json" as never) as typeof CreateCredential.infer;
    const db = c.get("db");

    const now = new Date();
    const row = first(
      await db
        .insert(credential)
        .values({
          id: generateId("credential"),
          tenantId: tenantCtx.id,
          name: body.name,
          type: body.type,
          description: body.description ?? null,
          secret: body.secret,
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
  "/:credentialId",
  requireGrant(idResource("credential", "credentialId"), "read"),
  describeRoute({
    tags: ["Credentials"],
    summary: "Get credential metadata",
    description: "Returns credential metadata. The secret is never included.",
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
      where: and(
        eq(credential.id, credentialId),
        eq(credential.tenantId, tenantCtx.id),
      ),
    });

    if (!row) {
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
    const body = c.req.valid("json" as never) as typeof UpdateCredential.infer;
    const db = c.get("db");

    const updates: Record<string, unknown> = { updatedAt: new Date() };
    if (body.name !== undefined) updates["name"] = body.name;
    if (body.description !== undefined)
      updates["description"] = body.description;
    if (body.secret !== undefined) updates["secret"] = body.secret;
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

    return c.json(formatCredential(updated));
  },
);

app.delete(
  "/:credentialId",
  requireGrant(idResource("credential", "credentialId"), "manage"),
  describeRoute({
    tags: ["Credentials"],
    summary: "Revoke a credential",
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
