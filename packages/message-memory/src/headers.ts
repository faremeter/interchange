import type {
  MessageHeaders,
  InterchangeType,
} from "@interchange/types/runtime";

/**
 * Build a MessageHeaders object from a parsed header map.
 *
 * Uses exactOptionalPropertyTypes-safe construction: optional fields are
 * only included in the returned object when they carry actual values.
 */
export function buildMessageHeaders(
  headers: Map<string, string>,
): MessageHeaders {
  const from = headers.get("from") ?? "";
  const toRaw = headers.get("to") ?? "";
  const to = toRaw
    ? toRaw
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    : [];

  const date = headers.get("date") ?? "";
  const messageId = headers.get("message-id") ?? "";

  const result: MessageHeaders = { from, to, date, messageId };

  const ccRaw = headers.get("cc");
  if (ccRaw !== undefined) {
    const cc = ccRaw
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    if (cc.length > 0) result.cc = cc;
  }

  const refsRaw = headers.get("references");
  if (refsRaw !== undefined) {
    const refs = refsRaw.split(/\s+/).filter(Boolean);
    if (refs.length > 0) result.references = refs;
  }

  const inReplyTo = headers.get("in-reply-to");
  if (inReplyTo !== undefined) result.inReplyTo = inReplyTo;

  const subject = headers.get("subject");
  if (subject !== undefined) result.subject = subject;

  const listId = headers.get("list-id");
  if (listId !== undefined) result.listId = listId;

  const rawType = headers.get("interchange-type");
  if (rawType !== undefined) {
    result.interchangeType = rawType as InterchangeType;
  }

  const corrId = headers.get("interchange-correlation-id");
  if (corrId !== undefined) result.interchangeCorrelationId = corrId;

  const tenantId = headers.get("interchange-tenant-id");
  if (tenantId !== undefined) result.interchangeTenantId = tenantId;

  const agentId = headers.get("interchange-agent-id");
  if (agentId !== undefined) result.interchangeAgentId = agentId;

  const sessionId = headers.get("interchange-session-id");
  if (sessionId !== undefined) result.interchangeSessionId = sessionId;

  const offeringId = headers.get("interchange-offering-id");
  if (offeringId !== undefined) result.interchangeOfferingId = offeringId;

  const schemaVersion = headers.get("interchange-schema-version");
  if (schemaVersion !== undefined)
    result.interchangeSchemaVersion = schemaVersion;

  const traceparent = headers.get("traceparent");
  if (traceparent !== undefined) result.traceparent = traceparent;

  const tracestate = headers.get("tracestate");
  if (tracestate !== undefined) result.tracestate = tracestate;

  return result;
}
