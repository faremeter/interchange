import { type } from "arktype";

export const CreateWallet = type({
  name: "string",
  backendType: "'crypto' | 'fiat' | 'credits'",
  currency: "string",
  "config?": "Record<string, unknown>",
});

export const UpdateWallet = type({
  "name?": "string",
  "config?": "Record<string, unknown>",
});

export const WalletResponse = type({
  id: "string",
  tenantId: "string",
  name: "string",
  backendType: "'crypto' | 'fiat' | 'credits'",
  currency: "string",
  balance: "string",
  "config?": "Record<string, unknown>",
  createdAt: "string",
  updatedAt: "string",
});

export const TransactionResponse = type({
  id: "string",
  walletId: "string",
  "agentId?": "string | null",
  direction: "'inbound' | 'outbound'",
  amount: "string",
  currency: "string",
  "recipientId?": "string | null",
  "senderId?": "string | null",
  "requestId?": "string | null",
  status: "'pending' | 'completed' | 'failed'",
  createdAt: "string",
});
