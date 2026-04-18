import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { wallet, transaction } from "@interchange/db/schema";
import {
  CreateWallet,
  UpdateWallet,
  WalletResponse,
  TransactionResponse,
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

function formatWallet(row: typeof wallet.$inferSelect) {
  return {
    id: row.id,
    tenantId: row.tenantId,
    name: row.name,
    backendType: row.backendType as "crypto" | "fiat" | "credits",
    currency: row.currency,
    balance: row.balance,
    config: (row.config as Record<string, unknown> | null) ?? undefined,
    createdAt: ts(row.createdAt),
    updatedAt: ts(row.updatedAt),
  };
}

function formatTransaction(row: typeof transaction.$inferSelect) {
  return {
    id: row.id,
    walletId: row.walletId,
    agentId: row.agentId ?? null,
    direction: row.direction as "inbound" | "outbound",
    amount: row.amount,
    currency: row.currency,
    recipientId: row.recipientId ?? null,
    senderId: row.senderId ?? null,
    requestId: row.requestId ?? null,
    status: row.status as "pending" | "completed" | "failed",
    createdAt: ts(row.createdAt),
  };
}

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
    const db = c.get("db");
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
    const db = c.get("db");

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
    const db = c.get("db");

    const row = await db.query.wallet.findFirst({
      where: and(eq(wallet.id, walletId), eq(wallet.tenantId, tenantCtx.id)),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Wallet not found" } },
        404,
      );
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
    const db = c.get("db");

    const updates: Record<string, unknown> = { updatedAt: new Date() };
    if (body.name !== undefined) updates["name"] = body.name;
    if (body.config !== undefined) updates["config"] = body.config;

    const [updated] = await db
      .update(wallet)
      .set(updates)
      .where(and(eq(wallet.id, walletId), eq(wallet.tenantId, tenantCtx.id)))
      .returning();

    if (!updated) {
      return c.json(
        { error: { code: "not_found", message: "Wallet not found" } },
        404,
      );
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
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const walletId = c.req.param("walletId");
    const db = c.get("db");

    const deleted = await db
      .delete(wallet)
      .where(and(eq(wallet.id, walletId), eq(wallet.tenantId, tenantCtx.id)))
      .returning();

    if (deleted.length === 0) {
      return c.json(
        { error: { code: "not_found", message: "Wallet not found" } },
        404,
      );
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
      "Transaction history for a wallet. Filterable by agent, date range, and status.",
    parameters: [
      { name: "agentId", in: "query", schema: { type: "string" } },
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
    const db = c.get("db");

    const walletRow = await db.query.wallet.findFirst({
      where: and(eq(wallet.id, walletId), eq(wallet.tenantId, tenantCtx.id)),
    });

    if (!walletRow) {
      return c.json(
        { error: { code: "not_found", message: "Wallet not found" } },
        404,
      );
    }

    const agentId = c.req.query("agentId");
    const status = c.req.query("status");
    const { limit, cursor } = parsePageParams({
      cursor: c.req.query("cursor"),
      limit: c.req.query("limit"),
    });

    const conditions = [eq(transaction.walletId, walletId)];
    if (agentId) conditions.push(eq(transaction.agentId, agentId));
    if (status === "pending" || status === "completed" || status === "failed") {
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

    return c.json(paginatedResponse(rows.map(formatTransaction), rows, limit));
  },
);

export { app as walletRoutes };
