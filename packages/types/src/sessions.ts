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

// The schema validates structure only: a required mimeType, a required
// string `data` carrying base64-encoded bytes, an optional name, and no
// other keys. base64 validity, the MIME allowlist, and size limits are
// enforced at the route boundary so it can emit ordered, per-index
// structured errors (malformed_base64, disallowed_mime_type, oversize_*)
// that an all-or-nothing schema validator cannot produce.
export const SendMessage = type({
  content: "string",
  "attachments?": type({
    mimeType: "string",
    data: "string",
    "name?": "string",
  })
    .onUndeclaredKey("reject")
    .array(),
});

export const MailResponse = type({
  id: "string",
  sessionId: type("string").describe(
    "Internal session channel identifier, not a user-facing session resource.",
  ),
  instanceId: "string | null",
  direction: "'inbound' | 'outbound'",
  status: "'pending' | 'delivered'",
  receivedAt: "string",
  from: type({
    name: "string | null",
    email: "string",
  }).array(),
  to: type({
    name: "string | null",
    email: "string",
  }).array(),
  subject: "string | null",
  sentAt: "string | null",
  bodyValues: "Record<string, unknown>",
  textBody: type({
    partId: "string",
    type: "string",
  }).array(),
  htmlBody: type({
    partId: "string",
    type: "string",
  }).array(),
  attachments: type({
    blobId: "string",
    name: "string | null",
    type: "string",
    size: "number",
  }).array(),
  headers: "Record<string, string>",
});
export type MailResponse = typeof MailResponse.infer;

// Structured attachment-rejection errors returned by POST /:instanceId/mail.
// Each variant carries a machine-actionable `code` plus the fields a client
// needs to locate and explain the rejection, alongside a human-readable
// `message`. This is the wire contract for the route's attachment 400s; the
// route handler is the single producer.
export const AttachmentError = type({
  code: "'oversize_attachment'",
  message: "string",
  attachmentIndex: "number",
  byteLength: "number",
  limitBytes: "number",
})
  .or({
    code: "'disallowed_mime_type'",
    message: "string",
    attachmentIndex: "number",
    mimeType: "string",
  })
  .or({
    code: "'invalid_attachment_name'",
    message: "string",
    attachmentIndex: "number",
  })
  .or({
    code: "'malformed_base64'",
    message: "string",
    attachmentIndex: "number",
  })
  .or({
    code: "'oversize_total'",
    message: "string",
    totalBytes: "number",
    limitBytes: "number",
  });
export type AttachmentError = typeof AttachmentError.infer;

export const AttachmentErrorResponse = type({ error: AttachmentError });
export type AttachmentErrorResponse = typeof AttachmentErrorResponse.infer;

export const InferenceTurnResponse = type({
  id: "string",
  sessionId: type("string").describe(
    "Internal session channel identifier, not a user-facing session resource.",
  ),
  instanceId: "string",
  model: "string",
  status: "'running' | 'completed' | 'failed'",
  startedAt: "string",
  endedAt: "string | null",
  parts: type({
    id: "string",
    type: "'text' | 'reasoning' | 'tool' | 'file' | 'error' | 'step-start' | 'step-finish' | 'snapshot' | 'patch'",
    "content?": "string | null",
    "metadata?": "Record<string, unknown> | null",
    ordinal: "number",
  }).array(),
});
export type InferenceTurnResponse = typeof InferenceTurnResponse.infer;
