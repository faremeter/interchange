import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import {
  CreateWallet,
  UpdateWallet,
  WalletResponse,
  TransactionResponse,
  ErrorResponse,
} from "@interchange/types";

import type { AppEnv } from "../context";

const app = new Hono<AppEnv>();

app.get(
  "/",
  describeRoute({
    tags: ["Wallets"],
    summary: "List wallets in the tenant",
    responses: {
      200: {
        description: "List of wallets",
        content: {
          "application/json": {
            schema: resolver(WalletResponse.array()),
          },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.post(
  "/",
  describeRoute({
    tags: ["Wallets"],
    summary: "Create a wallet",
    description:
      "Creates a wallet with the specified payment backend and currency. Access for agents is managed through capability grants.",
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
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/:walletId",
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
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.patch(
  "/:walletId",
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
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.delete(
  "/:walletId",
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
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/:walletId/transactions",
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
    ],
    responses: {
      200: {
        description: "List of transactions",
        content: {
          "application/json": {
            schema: resolver(TransactionResponse.array()),
          },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

export { app as walletRoutes };
