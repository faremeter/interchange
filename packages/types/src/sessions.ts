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
  status: "'idle' | 'busy' | 'retry' | 'waiting_approval'",
  "sessionToken?": "string",
  createdAt: "string",
  updatedAt: "string",
  "lastActivityAt?": "string | null",
});

export const SessionStatus = type({
  status: "'idle' | 'busy' | 'retry' | 'waiting_approval'",
  "retryAttempt?": "number",
  "retryMessage?": "string",
  "retryNextAt?": "string",
});

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
  createdAt: "string",
  parts: type({
    id: "string",
    type: "'text' | 'reasoning' | 'tool' | 'file' | 'step-start' | 'step-finish' | 'snapshot' | 'patch'",
    "content?": "string | null",
    "metadata?": "Record<string, unknown> | null",
  }).array(),
});
