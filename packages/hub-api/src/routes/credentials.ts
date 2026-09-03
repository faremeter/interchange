import { eq, and, isNull } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { credential, grant as grantTable, provider } from "@intx/db/schema";
import {
  getAncestorChain,
  resolveCredentialByName,
  parseCredentialRow,
} from "@intx/db";
import type { DB } from "@intx/db";
import {
  CreateCredential,
  UpdateCredential,
  CredentialResponse,
  ErrorResponse,
  paginatedSchema,
  credentialAad,
} from "@intx/types";
import type { CredentialCipher } from "@intx/types";

import type { TenantEnv } from "../context";
import { errorResponse } from "../error-response";
import { first, ts } from "../format";
import { generateId } from "@intx/hub-common";
import { isReferencedRowViolation } from "../pg-errors";
import { idResource } from "../middleware/grant";
import type { RequireGrant } from "../middleware/grant";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";
import {
  pushCredentialRevoke,
  pushSourceUpdates,
  type SidecarRouter,
} from "@intx/hub-sessions";

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

export type CreateCredentialRoutesDeps = {
  db: DB["db"];
  sidecarRouter: SidecarRouter;
  requireGrant: RequireGrant;
  credentialCipher: CredentialCipher;
};

export function createCredentialRoutes({
  db,
  sidecarRouter,
  requireGrant,
  credentialCipher,
}: CreateCredentialRoutesDeps): Hono<TenantEnv> {
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

      const providerRow = await db.query.provider.findFirst({
        where: eq(provider.id, body.providerId),
      });
      if (!providerRow) {
        return errorResponse(c, "not_found", "Provider not found");
      }

      const chain = await getAncestorChain(db, tenantCtx.id);
      if (!chain.includes(providerRow.tenantId)) {
        return errorResponse(c, "not_found", "Provider not found");
      }

      const existing = await db.query.credential.findFirst({
        where: and(
          eq(credential.tenantId, tenantCtx.id),
          eq(credential.name, body.name),
        ),
      });
      if (existing) {
        return errorResponse(
          c,
          "conflict",
          "Credential name already exists in this tenant",
        );
      }

      const now = new Date();
      const credentialId = generateId("credential");
      const ownerPrincipalId = body.principalId ?? null;

      // Encrypt the secrets at rest, bound to this row and column so a
      // ciphertext cannot be transplanted. refreshSecret stays null when absent.
      const encryptedSecret = await credentialCipher.encrypt(
        body.secret,
        credentialAad(credentialId, "secret"),
      );
      const encryptedRefreshSecret =
        body.refreshSecret === null || body.refreshSecret === undefined
          ? null
          : await credentialCipher.encrypt(
              body.refreshSecret,
              credentialAad(credentialId, "refreshSecret"),
            );

      const row = await db.transaction(async (tx) => {
        const inserted = first(
          await tx
            .insert(credential)
            .values({
              id: credentialId,
              tenantId: tenantCtx.id,
              providerId: body.providerId,
              principalId: ownerPrincipalId,
              oauthClientId: body.oauthClientId ?? null,
              name: body.name,
              type: body.type,
              description: body.description ?? null,
              secret: encryptedSecret,
              refreshSecret: encryptedRefreshSecret,
              scopes: body.scopes ?? null,
              expiresAt: body.expiresAt ? new Date(body.expiresAt) : null,
              metadata: body.metadata ?? null,
              createdAt: now,
              updatedAt: now,
            })
            .returning(),
        );

        // A personal credential (principalId set) grants its owner durable
        // `use` authority on creation so the owner can launch agents with
        // their own credential without a separate manual grant. Org
        // credentials (principalId null) are covered by tenant-owner role
        // inheritance and explicit administrative role grants, so they get
        // no auto-grant here.
        if (ownerPrincipalId !== null) {
          await tx.insert(grantTable).values({
            id: generateId("grant"),
            tenantId: tenantCtx.id,
            principalId: ownerPrincipalId,
            resource: `credential:${credentialId}`,
            action: "use",
            effect: "allow",
            origin: "creator",
            expiresAt: null,
            createdAt: now,
            updatedAt: now,
          });
        }

        return inserted;
      });

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

      const row = await resolveCredentialByName(db, tenantCtx.id, name);

      if (!row) {
        return errorResponse(c, "not_found", "Credential not found");
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

      const row = await db.query.credential.findFirst({
        where: eq(credential.id, credentialId),
      });

      if (!row) {
        return errorResponse(c, "not_found", "Credential not found");
      }

      const chain = await getAncestorChain(db, tenantCtx.id);
      if (!chain.includes(row.tenantId)) {
        return errorResponse(c, "not_found", "Credential not found");
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

      const updates: Record<string, unknown> = { updatedAt: new Date() };
      if (body.name !== undefined) updates["name"] = body.name;
      if (body.description !== undefined)
        updates["description"] = body.description;
      // Re-encrypt a rotated secret under the same row/column binding. A null
      // refreshSecret clears it; a present value is encrypted.
      if (body.secret !== undefined)
        updates["secret"] = await credentialCipher.encrypt(
          body.secret,
          credentialAad(credentialId, "secret"),
        );
      if (body.refreshSecret !== undefined)
        updates["refreshSecret"] =
          body.refreshSecret === null
            ? null
            : await credentialCipher.encrypt(
                body.refreshSecret,
                credentialAad(credentialId, "refreshSecret"),
              );
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
        return errorResponse(c, "not_found", "Credential not found");
      }

      // If the secret was updated, push new inference sources to running
      // instances -- UNLESS the same request also revokes the credential. A
      // revoke and a secret re-resolve target overlapping addresses with no
      // ordering guarantee (both fire-and-forget), and the source re-resolve
      // does not consult credential status, so it would re-deliver the rotated
      // material and could re-populate the revoked secret in a live child's
      // cell. The revoke wins.
      if (body.secret !== undefined && body.status !== "revoked") {
        void pushSourceUpdates(
          db,
          sidecarRouter,
          updated.tenantId,
          credentialCipher,
        );
      }

      // A deliberate revocation must evict the credential from any running
      // deployment that holds it -- a re-resolve alone would leave the revoked
      // secret in a live child's cell. Fire-and-forget, best effort.
      if (body.status === "revoked") {
        void pushCredentialRevoke(
          db,
          sidecarRouter,
          updated.tenantId,
          credentialId,
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
        409: {
          description: "Credential is in use by a model provider",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const credentialId = c.req.param("credentialId");

      // Delete the credential and its per-credential grants atomically. Two
      // invariants, each owned by exactly one layer:
      //   - Existence within the tenant is owned by the delete's WHERE clause
      //     (`id` AND `tenantId`). authz cannot own it: the resource string
      //     `credential:{id}` is opaque, so a wildcard grant matches an id in
      //     any tenant. A foreign or unknown id matches zero rows -> 404,
      //     disclosing nothing across the tenant boundary.
      //   - "Cannot delete a credential a model provider still references" is
      //     owned by the model_provider.credential_id foreign key, which is
      //     onDelete "restrict" (catalog.ts). The delete fires it only for a row
      //     actually being deleted (an in-tenant credential), so the catch maps
      //     the raw violation to a 409 instead of a 500.
      // The grant delete keys on the exact `credential:{id}` resource, which has
      // no foreign key to `credential` (nothing cascades) and never matches the
      // coarse `credential:*` role grant.
      let outcome: "not_found" | "deleted";
      try {
        outcome = await db.transaction(async (tx) => {
          const deleted = await tx
            .delete(credential)
            .where(
              and(
                eq(credential.id, credentialId),
                eq(credential.tenantId, tenantCtx.id),
              ),
            )
            .returning();

          if (deleted.length === 0) {
            return "not_found" as const;
          }

          await tx
            .delete(grantTable)
            .where(
              and(
                eq(grantTable.tenantId, tenantCtx.id),
                eq(grantTable.resource, `credential:${credentialId}`),
              ),
            );

          return "deleted" as const;
        });
      } catch (err) {
        if (!isReferencedRowViolation(err)) {
          throw err;
        }
        return errorResponse(
          c,
          "conflict",
          "Credential is in use by a model provider",
        );
      }

      if (outcome === "not_found") {
        return errorResponse(c, "not_found", "Credential not found");
      }

      // The credential row is gone; evict its material from any running
      // deployment that still holds it so a live child cannot keep using it.
      // Fire-and-forget, best effort.
      void pushCredentialRevoke(db, sidecarRouter, tenantCtx.id, credentialId);

      return c.body(null, 204);
    },
  );

  return app;
}
