/**
 * Builders for InboundMessage and OutboundMessage shapes.
 *
 * Constructing these by hand requires assembling MessageRef, MessageHeaders,
 * payload envelopes, signature status, and other mail-shaped fields that the
 * transport normally produces after parsing wire bytes. These builders
 * collapse that boilerplate behind two factories with sensible defaults.
 *
 * The builders use the parsed-shape MessageHeaders from
 * @intx/types/runtime (where date is an ISO string), NOT the
 * wire-shape MessageHeaders local to this package (where date is a Date
 * object and headers are serialised to RFC 2822 bytes via assembleMessage).
 *
 * Consumers import the message types from @intx/types directly; the
 * @intx/mime barrel does not re-export them.
 */

import { type } from "arktype";
import type {
  InboundMessage,
  MessageAttachment,
  MessageHeaders,
  MessageRef,
  OutboundMessage,
} from "@intx/types/runtime";
import { InterchangeType, SignatureStatus } from "@intx/types/runtime";
import { generateMessageId } from "./mime";

/**
 * Default schema version for structured payloads. Matches
 * docs/MESSAGE.md § Payload Structure, which specifies "version": "1" as
 * the current schema version for every Interchange payload type. Audit
 * this default whenever the documented schema version increments.
 */
const DEFAULT_PAYLOAD_VERSION = "1";

const MESSAGE_ID_RE = /^<[^<>\s@]+@[^<>\s@]+>$/;
const ADDRESS_RE = /^[^@\s]+@[^@\s]+$/;

const CONVERSATION_TYPE_PREFIX = "conversation.";

// ---------------------------------------------------------------------------
// InboundMessage builder
// ---------------------------------------------------------------------------

/**
 * Structured payload envelope for an inbound message. `version` defaults to
 * the current schema version per docs/MESSAGE.md.
 */
export type InboundPayloadInput = {
  type: InterchangeType;
  body: Record<string, unknown>;
  version?: string;
};

export type CreateInboundMessageOpts = {
  from: string;
  to: string | string[];

  /** Plain-text body. Mutually exclusive with `payload`. */
  content?: string;

  /** Structured JSON envelope. Mutually exclusive with `content`. */
  payload?: InboundPayloadInput;

  cc?: string | string[];
  subject?: string;

  /**
   * Defaults to `new Date().toISOString()`. Accepts Date or any string
   * parseable by `new Date(...)`; stored as an ISO 8601 string.
   */
  date?: Date | string;

  /** Defaults to `generateMessageId(from)`. Must be of the form `<id@host>`. */
  messageId?: string;

  inReplyTo?: string;
  references?: string[];
  listId?: string;

  /**
   * Interchange-Type header value. Auto-derived from `payload.type` when a
   * payload is supplied; throws if explicitly set to a value that conflicts
   * with `payload.type`.
   */
  interchangeType?: InterchangeType;

  correlationId?: string;
  tenantId?: string;
  agentId?: string;
  sessionId?: string;
  offeringId?: string;
  schemaVersion?: string;
  traceparent?: string;
  tracestate?: string;

  attachments?: MessageAttachment[];

  /** Merged with `{ uid: 1, mailbox: "INBOX" }`. */
  ref?: Partial<MessageRef>;

  flags?: string[];

  /** Defaults to `"missing"`. */
  signatureStatus?: SignatureStatus;
};

export function createInboundMessage(
  opts: CreateInboundMessageOpts,
): InboundMessage {
  const fn = "createInboundMessage";

  requireAddress(opts.from, "from", fn);
  const to = normalizeAndValidateAddressArray(opts.to, "to", fn);

  validateBodyExclusivity(opts.content, opts.payload, fn);

  if (opts.payload !== undefined) {
    validateInterchangeType(opts.payload.type, "payload.type", fn);
    if (isConversationType(opts.payload.type)) {
      throw new Error(
        `${fn}: conversation types must use \`content\` instead of \`payload\`; got \`payload.type\`: ${opts.payload.type}`,
      );
    }
    validatePayloadBody(opts.payload.body, "payload.body", fn);
    if (opts.payload.version !== undefined) {
      if (
        typeof opts.payload.version !== "string" ||
        opts.payload.version.length === 0
      ) {
        throw new Error(
          `${fn}: \`payload.version\`, when provided, must be a non-empty string`,
        );
      }
    }
  }

  if (opts.interchangeType !== undefined) {
    validateInterchangeType(opts.interchangeType, "interchangeType", fn);
    if (
      opts.payload !== undefined &&
      opts.interchangeType !== opts.payload.type
    ) {
      throw new Error(
        `${fn}: \`interchangeType\` (${opts.interchangeType}) conflicts with \`payload.type\` (${opts.payload.type})`,
      );
    }
  }

  if (opts.messageId !== undefined) {
    validateMessageId(opts.messageId, "messageId", fn);
  }
  if (opts.inReplyTo !== undefined) {
    validateMessageId(opts.inReplyTo, "inReplyTo", fn);
  }
  if (opts.references !== undefined) {
    if (opts.references.length === 0) {
      throw new Error(
        `${fn}: \`references\`, when provided, must contain at least one entry`,
      );
    }
    opts.references.forEach((ref, i) => {
      validateMessageId(ref, `references[${i}]`, fn);
    });
  }

  const cc =
    opts.cc === undefined
      ? undefined
      : normalizeAndValidateAddressArray(opts.cc, "cc", fn);

  rejectEmptyStringIfPresent(opts.content, "content", fn);
  rejectEmptyStringIfPresent(opts.subject, "subject", fn);
  rejectEmptyStringIfPresent(opts.listId, "listId", fn);
  rejectEmptyStringIfPresent(opts.correlationId, "correlationId", fn);
  rejectEmptyStringIfPresent(opts.tenantId, "tenantId", fn);
  rejectEmptyStringIfPresent(opts.agentId, "agentId", fn);
  rejectEmptyStringIfPresent(opts.sessionId, "sessionId", fn);
  rejectEmptyStringIfPresent(opts.offeringId, "offeringId", fn);
  rejectEmptyStringIfPresent(opts.schemaVersion, "schemaVersion", fn);
  rejectEmptyStringIfPresent(opts.traceparent, "traceparent", fn);
  rejectEmptyStringIfPresent(opts.tracestate, "tracestate", fn);

  if (opts.flags !== undefined) {
    opts.flags.forEach((flag, i) => {
      if (typeof flag !== "string" || flag.length === 0) {
        throw new Error(`${fn}: \`flags[${i}]\` must be a non-empty string`);
      }
    });
  }

  const signatureStatus = opts.signatureStatus ?? "missing";
  const validatedStatus = SignatureStatus(signatureStatus);
  if (validatedStatus instanceof type.errors) {
    throw new Error(
      `${fn}: \`signatureStatus\` is not a recognised SignatureStatus: ${validatedStatus.summary}`,
    );
  }

  const date = normalizeDate(opts.date, "date", fn);
  const messageId = opts.messageId ?? generateMessageId(opts.from);
  const derivedInterchangeType = opts.interchangeType ?? opts.payload?.type;

  const headers: MessageHeaders = { from: opts.from, to, date, messageId };
  if (cc !== undefined) headers.cc = cc;
  if (opts.subject !== undefined) headers.subject = opts.subject;
  if (opts.inReplyTo !== undefined) headers.inReplyTo = opts.inReplyTo;
  if (opts.references !== undefined) headers.references = opts.references;
  if (opts.listId !== undefined) headers.listId = opts.listId;
  if (derivedInterchangeType !== undefined) {
    headers.interchangeType = derivedInterchangeType;
  }
  if (opts.correlationId !== undefined) {
    headers.interchangeCorrelationId = opts.correlationId;
  }
  if (opts.tenantId !== undefined) headers.interchangeTenantId = opts.tenantId;
  if (opts.agentId !== undefined) headers.interchangeAgentId = opts.agentId;
  if (opts.sessionId !== undefined) {
    headers.interchangeSessionId = opts.sessionId;
  }
  if (opts.offeringId !== undefined) {
    headers.interchangeOfferingId = opts.offeringId;
  }
  if (opts.schemaVersion !== undefined) {
    headers.interchangeSchemaVersion = opts.schemaVersion;
  }
  if (opts.traceparent !== undefined) headers.traceparent = opts.traceparent;
  if (opts.tracestate !== undefined) headers.tracestate = opts.tracestate;

  if (opts.ref?.uid !== undefined) {
    if (
      typeof opts.ref.uid !== "number" ||
      !Number.isInteger(opts.ref.uid) ||
      !Number.isFinite(opts.ref.uid) ||
      opts.ref.uid < 1
    ) {
      throw new Error(
        `${fn}: \`ref.uid\`, when provided, must be a positive integer (IMAP UID)`,
      );
    }
  }
  const ref: MessageRef = {
    uid: opts.ref?.uid ?? 1,
    mailbox: opts.ref?.mailbox ?? "INBOX",
  };
  if (typeof ref.mailbox !== "string" || ref.mailbox.length === 0) {
    throw new Error(
      `${fn}: \`ref.mailbox\`, when provided, must be a non-empty string`,
    );
  }

  const result: InboundMessage = {
    ref,
    headers,
    flags: opts.flags ?? [],
    signatureStatus,
  };
  if (opts.content !== undefined) result.content = opts.content;
  if (opts.payload !== undefined) {
    result.payload = {
      type: opts.payload.type,
      version: opts.payload.version ?? DEFAULT_PAYLOAD_VERSION,
      body: opts.payload.body,
    };
  }
  if (opts.attachments !== undefined && opts.attachments.length > 0) {
    result.attachments = opts.attachments;
  }

  return result;
}

// ---------------------------------------------------------------------------
// OutboundMessage builder
// ---------------------------------------------------------------------------

export type CreateOutboundMessageOpts = {
  to: string | string[];

  /** Interchange payload type. Determines content vs payload semantics. */
  type: InterchangeType;

  /** Plain-text body. Mutually exclusive with `payload`. */
  content?: string;

  /** Structured JSON envelope body. Mutually exclusive with `content`. */
  payload?: Record<string, unknown>;

  cc?: string | string[];
  subject?: string;

  /** Human-readable summary used as the text/plain part for structured types. */
  summary?: string;

  inReplyTo?: string;
  correlationId?: string;
  sessionId?: string;
  tenantId?: string;

  attachments?: MessageAttachment[];
};

export function createOutboundMessage(
  opts: CreateOutboundMessageOpts,
): OutboundMessage {
  const fn = "createOutboundMessage";

  validateInterchangeType(opts.type, "type", fn);
  // Validate addresses without mutating the source shape; the OutboundMessage
  // type preserves `string | string[]` and downstream consumers handle both.
  normalizeAndValidateAddressArray(opts.to, "to", fn);
  if (opts.cc !== undefined) {
    normalizeAndValidateAddressArray(opts.cc, "cc", fn);
  }

  validateBodyExclusivity(opts.content, opts.payload, fn);

  if (isConversationType(opts.type)) {
    if (opts.payload !== undefined) {
      throw new Error(
        `${fn}: conversation \`type\` ${opts.type} must use \`content\` instead of \`payload\``,
      );
    }
    if (opts.content === undefined) {
      throw new Error(
        `${fn}: conversation \`type\` ${opts.type} requires \`content\``,
      );
    }
  } else {
    if (opts.content !== undefined) {
      throw new Error(
        `${fn}: non-conversation \`type\` ${opts.type} must use \`payload\` instead of \`content\``,
      );
    }
    if (opts.payload === undefined) {
      throw new Error(
        `${fn}: non-conversation \`type\` ${opts.type} requires \`payload\``,
      );
    }
  }
  if (opts.payload !== undefined) {
    validatePayloadBody(opts.payload, "payload", fn);
  }

  if (opts.inReplyTo !== undefined) {
    validateMessageId(opts.inReplyTo, "inReplyTo", fn);
  }
  rejectEmptyStringIfPresent(opts.content, "content", fn);
  rejectEmptyStringIfPresent(opts.subject, "subject", fn);
  rejectEmptyStringIfPresent(opts.summary, "summary", fn);
  rejectEmptyStringIfPresent(opts.correlationId, "correlationId", fn);
  rejectEmptyStringIfPresent(opts.sessionId, "sessionId", fn);
  rejectEmptyStringIfPresent(opts.tenantId, "tenantId", fn);

  const result: OutboundMessage = { to: opts.to, type: opts.type };
  if (opts.cc !== undefined) result.cc = opts.cc;
  if (opts.subject !== undefined) result.subject = opts.subject;
  if (opts.content !== undefined) result.content = opts.content;
  if (opts.payload !== undefined) result.payload = opts.payload;
  if (opts.summary !== undefined) result.summary = opts.summary;
  if (opts.attachments !== undefined && opts.attachments.length > 0) {
    result.attachments = opts.attachments;
  }
  if (opts.inReplyTo !== undefined) result.inReplyTo = opts.inReplyTo;
  if (opts.correlationId !== undefined) {
    result.correlationId = opts.correlationId;
  }
  if (opts.sessionId !== undefined) result.sessionId = opts.sessionId;
  if (opts.tenantId !== undefined) result.tenantId = opts.tenantId;
  return result;
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

function rejectEmptyStringIfPresent(
  value: string | undefined,
  field: string,
  fn: string,
): void {
  if (value !== undefined && value.length === 0) {
    throw new Error(
      `${fn}: \`${field}\`, when provided, must be a non-empty string`,
    );
  }
}

function requireAddress(value: unknown, field: string, fn: string): void {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${fn}: \`${field}\` must be a non-empty string`);
  }
  if (!ADDRESS_RE.test(value)) {
    throw new Error(
      `${fn}: \`${field}\` must be an RFC 5322 address of the form \`local@domain\`; got: ${value}`,
    );
  }
}

function normalizeAndValidateAddressArray(
  input: string | string[],
  field: string,
  fn: string,
): string[] {
  if (typeof input === "string") {
    requireAddress(input, field, fn);
    return [input];
  }
  if (!Array.isArray(input) || input.length === 0) {
    throw new Error(
      `${fn}: \`${field}\` must contain at least one recipient address`,
    );
  }
  input.forEach((entry, i) => {
    requireAddress(entry, `${field}[${i}]`, fn);
  });
  return input;
}

function validatePayloadBody(value: unknown, field: string, fn: string): void {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(
      `${fn}: \`${field}\` must be a plain object (got ${
        value === null ? "null" : Array.isArray(value) ? "array" : typeof value
      })`,
    );
  }
}

function isConversationType(t: InterchangeType): boolean {
  return t.startsWith(CONVERSATION_TYPE_PREFIX);
}

function validateInterchangeType(
  value: unknown,
  field: string,
  fn: string,
): void {
  const validated = InterchangeType(value);
  if (validated instanceof type.errors) {
    throw new Error(
      `${fn}: \`${field}\` is not a valid InterchangeType: ${validated.summary}`,
    );
  }
}

function validateMessageId(value: string, field: string, fn: string): void {
  if (!MESSAGE_ID_RE.test(value)) {
    throw new Error(
      `${fn}: \`${field}\` must be an RFC 2822 message identifier of the form \`<id@host>\`; got: ${value}`,
    );
  }
}

function normalizeDate(
  input: Date | string | undefined,
  field: string,
  fn: string,
): string {
  if (input === undefined) return new Date().toISOString();
  if (input instanceof Date) {
    if (Number.isNaN(input.getTime())) {
      throw new Error(`${fn}: \`${field}\` is an Invalid Date`);
    }
    return input.toISOString();
  }
  if (typeof input !== "string" || input.length === 0) {
    throw new Error(
      `${fn}: \`${field}\`, when provided, must be a Date or a non-empty string`,
    );
  }
  const parsed = new Date(input);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(
      `${fn}: \`${field}\` is not a parseable date string: ${input}`,
    );
  }
  return parsed.toISOString();
}

function validateBodyExclusivity(
  content: unknown,
  payload: unknown,
  fn: string,
): void {
  if (content !== undefined && payload !== undefined) {
    throw new Error(
      `${fn}: \`content\` and \`payload\` are mutually exclusive; provide at most one`,
    );
  }
}
