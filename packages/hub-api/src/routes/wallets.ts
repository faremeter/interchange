import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { wallet, transaction } from "@intx/db/schema";
import { parseWalletRow, parseTransactionRow } from "@intx/db";
import type { DB } from "@intx/db";
import {
  CreateWallet,
  UpdateWallet,
  WalletResponse,
  TransactionResponse,
  ErrorResponse,
  paginatedSchema,
} from "@intx/types";

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

function formatWallet(row: typeof wallet.$inferSelect) {
  const parsed = parseWalletRow(row);
  return {
    id: parsed.id,
    tenantId: parsed.tenantId,
    name: parsed.name,
    backendType: parsed.backendType,
    currency: parsed.currency,
    balance: parsed.balance,
    config: parsed.config ?? undefined,
    createdAt: ts(parsed.createdAt),
    updatedAt: ts(parsed.updatedAt),
  };
}

function formatTransaction(row: typeof transaction.$inferSelect) {
  const parsed = parseTransactionRow(row);
  return {
    id: parsed.id,
    walletId: parsed.walletId,
    runId: parsed.runId ?? null,
    direction: parsed.direction,
    amount: parsed.amount,
    currency: parsed.currency,
    recipientId: parsed.recipientId ?? null,
    senderId: parsed.senderId ?? null,
    requestId: parsed.requestId ?? null,
    status: parsed.status,
    createdAt: ts(parsed.createdAt),
  };
}

export type CreateWalletRoutesDeps = {
  db: DB["db"];
  requireGrant: RequireGrant;
};

export function createWalletRoutes({
  db,
  requireGrant,
}: CreateWalletRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  app.get(
    "/",
    requireGrant("wallet:*", "read"),
    describeRoute({
      tags: ["Wallets"],
      summary: "List wallets in the tenant",
      parameters: [...pageParameters],
      responses: {
        200: {
          description: "List of wallets",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(WalletResponse)),
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

      const conditions = [eq(wallet.tenantId, tenantCtx.id)];
      if (cursor) {
        conditions.push(cursorCondition(wallet.createdAt, wallet.id, cursor));
      }

      const rows = await db.query.wallet.findMany({
        where: and(...conditions),
        orderBy: pageOrder(wallet.createdAt, wallet.id),
        limit,
      });

      return c.json(paginatedResponse(rows.map(formatWallet), rows, limit));
    },
  );

  app.post(
    "/",
    requireGrant("wallet:*", "create"),
    describeRoute({
      tags: ["Wallets"],
      summary: "Create a wallet",
      description:
        "Creates a wallet with the specified payment backend and currency. Access for agents is managed through grants.",
      responses: {
        201: {
          description: "Wallet created",
          content: {
            "application/json": { schema: resolver(WalletResponse) },
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
    validator("json", CreateWallet),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const body = c.req.valid("json");

      const now = new Date();
      const row = first(
        await db
          .insert(wallet)
          .values({
            id: generateId("wallet"),
            tenantId: tenantCtx.id,
            name: body.name,
            backendType: body.backendType,
            currency: body.currency,
            balance: "0",
            config: body.config ?? null,
            createdAt: now,
            updatedAt: now,
          })
          .returning(),
      );

      return c.json(formatWallet(row), 201);
    },
  );

  app.get(
    "/:walletId",
    requireGrant(idResource("wallet", "walletId"), "read"),
    describeRoute({
      tags: ["Wallets"],
      summary: "Get wallet details",
      description: "Returns wallet details including current balance.",
      responses: {
        200: {
          description: "Wallet details",
          content: {
            "application/json": { schema: resolver(WalletResponse) },
          },
        },
        404: {
          description: "Wallet not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const walletId = c.req.param("walletId");

      const row = await db.query.wallet.findFirst({
        where: and(eq(wallet.id, walletId), eq(wallet.tenantId, tenantCtx.id)),
      });

      if (!row) {
        return errorResponse(c, "not_found", "Wallet not found");
      }

      return c.json(formatWallet(row));
    },
  );

  app.patch(
    "/:walletId",
    requireGrant(idResource("wallet", "walletId"), "manage"),
    describeRoute({
      tags: ["Wallets"],
      summary: "Update wallet config",
      responses: {
        200: {
          description: "Wallet updated",
          content: {
            "application/json": { schema: resolver(WalletResponse) },
          },
        },
        404: {
          description: "Wallet not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", UpdateWallet),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const walletId = c.req.param("walletId");
      const body = c.req.valid("json");

      const updates: Record<string, unknown> = { updatedAt: new Date() };
      if (body.name !== undefined) updates["name"] = body.name;
      if (body.config !== undefined) updates["config"] = body.config;

      const [updated] = await db
        .update(wallet)
        .set(updates)
        .where(and(eq(wallet.id, walletId), eq(wallet.tenantId, tenantCtx.id)))
        .returning();

      if (!updated) {
        return errorResponse(c, "not_found", "Wallet not found");
      }

      return c.json(formatWallet(updated));
    },
  );

  app.delete(
    "/:walletId",
    requireGrant(idResource("wallet", "walletId"), "manage"),
    describeRoute({
      tags: ["Wallets"],
      summary: "Deactivate a wallet",
      responses: {
        204: {
          description: "Wallet deactivated",
        },
        404: {
          description: "Wallet not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        409: {
          description: "Wallet is in use by a model provider",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const walletId = c.req.param("walletId");

      // Two invariants, each owned by one layer:
      //   - Existence within the tenant is owned by the delete's WHERE clause
      //     (`id` AND `tenantId`). authz cannot own it: the resource string
      //     `wallet:{id}` is opaque, so a wildcard grant matches an id in any
      //     tenant. A foreign or unknown id matches zero rows -> 404, disclosing
      //     nothing across the tenant boundary.
      //   - "Cannot delete a wallet a model provider still references" is owned
      //     by the model_provider.wallet_id foreign key (onDelete "restrict",
      //     catalog.ts). The delete fires it only for a row actually being
      //     deleted, so the catch maps the raw violation to a 409 instead of a
      //     500.
      // Wallets mint no per-wallet grant, so there is no grant cleanup and the
      // single delete needs no transaction.
      let outcome: "not_found" | "deleted";
      try {
        const deleted = await db
          .delete(wallet)
          .where(
            and(eq(wallet.id, walletId), eq(wallet.tenantId, tenantCtx.id)),
          )
          .returning();
        outcome = deleted.length === 0 ? "not_found" : "deleted";
      } catch (err) {
        if (!isReferencedRowViolation(err)) {
          throw err;
        }
        return errorResponse(
          c,
          "conflict",
          "Wallet is in use by a model provider",
        );
      }

      if (outcome === "not_found") {
        return errorResponse(c, "not_found", "Wallet not found");
      }

      return c.body(null, 204);
    },
  );

  app.get(
    "/:walletId/transactions",
    requireGrant(idResource("wallet", "walletId"), "read"),
    describeRoute({
      tags: ["Wallets"],
      summary: "List transactions",
      description:
        "Transaction history for a wallet. Filterable by run, date range, and status.",
      parameters: [
        { name: "runId", in: "query", schema: { type: "string" } },
        { name: "startTime", in: "query", schema: { type: "string" } },
        { name: "endTime", in: "query", schema: { type: "string" } },
        {
          name: "status",
          in: "query",
          schema: { type: "string", enum: ["pending", "completed", "failed"] },
        },
        ...pageParameters,
      ],
      responses: {
        200: {
          description: "List of transactions",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(TransactionResponse)),
            },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const walletId = c.req.param("walletId");

      const walletRow = await db.query.wallet.findFirst({
        where: and(eq(wallet.id, walletId), eq(wallet.tenantId, tenantCtx.id)),
      });

      if (!walletRow) {
        return errorResponse(c, "not_found", "Wallet not found");
      }

      const runId = c.req.query("runId");
      const status = c.req.query("status");
      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      const conditions = [eq(transaction.walletId, walletId)];
      if (runId) conditions.push(eq(transaction.runId, runId));
      if (
        status === "pending" ||
        status === "completed" ||
        status === "failed"
      ) {
        conditions.push(eq(transaction.status, status));
      }
      if (cursor) {
        conditions.push(
          cursorCondition(transaction.createdAt, transaction.id, cursor),
        );
      }

      const rows = await db.query.transaction.findMany({
        where: and(...conditions),
        orderBy: pageOrder(transaction.createdAt, transaction.id),
        limit,
      });

      return c.json(
        paginatedResponse(rows.map(formatTransaction), rows, limit),
      );
    },
  );

  return app;
}
