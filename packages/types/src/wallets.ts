import { type } from "arktype";

export const walletBackendTypes = ["crypto", "fiat", "credits"] as const;
export type WalletBackendType = (typeof walletBackendTypes)[number];

const BackendType = type.enumerated(...walletBackendTypes);

const backendTypeDescription =
  "Settlement backend the wallet is denominated in: `crypto` (on-chain assets), `fiat` (national currency), or `credits` (internal accounting units). Determines how balances and transactions are settled.";

const walletConfigDescription =
  "Backend-specific configuration for the wallet (for example chain or account details for a `crypto` backend). Shape depends on `backendType`; not interpreted by the hub.";

const balanceDescription =
  "Current balance as a decimal string in the wallet's `currency`. Stored as a string to preserve precision for both crypto and fiat amounts.";

export const CreateWallet = type({
  name: "string",
  backendType: BackendType.describe(backendTypeDescription),
  currency: "string",
  "config?": type("Record<string, unknown>").describe(walletConfigDescription),
});

export const UpdateWallet = type({
  "name?": "string",
  "config?": type("Record<string, unknown>").describe(walletConfigDescription),
});

export const WalletResponse = type({
  id: "string",
  tenantId: "string",
  name: "string",
  backendType: BackendType.describe(backendTypeDescription),
  currency: "string",
  balance: type("string").describe(balanceDescription),
  "config?": type("Record<string, unknown>").describe(walletConfigDescription),
  createdAt: "string",
  updatedAt: "string",
});

export const TransactionResponse = type({
  id: "string",
  walletId: "string",
  "agentId?": "string | null",
  direction: type("'inbound' | 'outbound'").describe(
    "Whether funds moved into the wallet (`inbound`) or out of it (`outbound`).",
  ),
  amount: type("string").describe(
    "Transaction amount as a decimal string in `currency`, stored as a string to preserve precision.",
  ),
  currency: "string",
  "recipientId?": "string | null",
  "senderId?": "string | null",
  "requestId?": "string | null",
  status: type("'pending' | 'completed' | 'failed'").describe(
    "Settlement state of the transaction: `pending` (initiated, not yet settled), `completed`, or `failed`.",
  ),
  createdAt: "string",
});
