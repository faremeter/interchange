import { type } from "arktype";

export const CreateSession = type({
  agentId: "string",
  "invokerCapabilities?": type({
    resource: "string",
    action: "string",
    "conditions?": "Record<string, unknown> | null",
  }).array(),
});

export const SessionResponse = type({
  id: "string",
  tenantId: "string",
  agentId: "string",
  principalId: "string",
  status: "'idle' | 'ending' | 'ended'",
  createdAt: "string",
  updatedAt: "string",
  "lastActivityAt?": "string | null",
});

// Runtime operational status of an active session. The harness retries
// internally and does not surface retry state to the hub, so the retry
// variant is omitted until the event protocol supports it.
export const SessionStatus = type({
  status: "'idle' | 'busy' | 'waiting_approval'",
});
export type SessionStatus = typeof SessionStatus.infer;

export const SendMessage = type({
  content: "string",
  "attachments?": type({
    type: "string",
    url: "string",
    "mimeType?": "string",
  }).array(),
});

export const MessageResponse = type({
  id: "string",
  sessionId: "string",
  role: "'user' | 'assistant'",
  status: "'pending' | 'delivered' | 'failed'",
  createdAt: "string",
  from: "string",
  parts: type({
    id: "string",
    type: "'text' | 'reasoning' | 'tool' | 'file' | 'step-start' | 'step-finish' | 'snapshot' | 'patch'",
    "content?": "string | null",
    "metadata?": "Record<string, unknown> | null",
  }).array(),
});
