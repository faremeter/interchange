// Per-tool handler factories for the mail tools. Each factory takes
// the bound MessageTransport and returns a closed-over ToolHandler.
//
// Keeping the factories at this granularity (one per tool, pure
// (MessageTransport) → ToolHandler) is deliberate: the package's
// public surface in index.ts resolves the transport once at handler-init
// and wires the handlers in a single place. The factories themselves
// carry no resolver vocabulary, so a future per-tool composition can
// reuse them unchanged.
//
// (MESSAGE.md § Mail Tools)

import { scope, type, type Type } from "arktype";
import { getLogger } from "@intx/log";
import type {
  MessageTransport,
  ToolCall,
  ToolResult,
  OutboundMessage,
} from "@intx/types/runtime";
import {
  InterchangeType,
  isConversationType,
  isMessageTransportError,
} from "@intx/types/runtime";

import type { MailToolName } from "./definitions";
import { errorResult, type MailToolErrorCode } from "./errors";

const logger = getLogger(["tools-mail", "handlers"]);

export type ToolHandler = (
  call: ToolCall,
  signal: AbortSignal,
) => Promise<ToolResult>;

// ---------------------------------------------------------------------------
// Argument schemas
// ---------------------------------------------------------------------------

// `Record<string, unknown>` admits a JSON array, whose own keys are strings.
const JSONObject = type("Record<string, unknown>").narrow(
  (value, ctx) =>
    !Array.isArray(value) || ctx.mustBe("an object, not an array"),
);

const MessageRefShape = {
  "+": "reject",
  uid: "number",
  mailbox: "string",
} as const;

// A subject, an in-reply-to and a correlation id each become a header field
// body, which RFC 5322 § 2.2 allows no CR and no LF outside folding. Stated as
// a pattern rather than a narrow so arktype can be asked about it:
// definitions.test.ts pairs this shape against the schema.
const HEADER_VALUE_PATTERN = "^[^\\r\\n]*$";

// RFC 5322 § 3.6.4 gives In-Reply-To as `1*msg-id`; `\S` matches no CR or LF.
const MESSAGE_ID_PATTERN = "^[^\\r\\n]*\\S[^\\r\\n]*$";

const HeaderValue = type("string").matching(HEADER_VALUE_PATTERN).configure({
  description: "a header value, so free of carriage return and line feed",
});

const MessageIdReference = type("string")
  .matching(MESSAGE_ID_PATTERN)
  .configure({
    description:
      "a message identifier, so free of carriage return and line feed, and not blank",
  });

const SendArgs = type({
  "+": "reject",
  to: "string | string[]",
  "type?": InterchangeType,
  "content?": "string",
  "payload?": JSONObject,
  "subject?": HeaderValue,
  "inReplyTo?": MessageIdReference,
  "correlationId?": HeaderValue,
});

const ReplyArgs = type({
  "+": "reject",
  ref: MessageRefShape,
  "type?": InterchangeType,
  "content?": "string",
  "payload?": JSONObject,
});

// A tool call arrives as JSON, which carries no Date, and the matcher behind
// MessageTransport.search compares the date filters as Date instances.
const QueryDate = type("Date").or(type("string.date.parse"));

// "+" governs the recursive references below as well, because and/or/not
// resolve to this same node.
export const SearchQueryArgs = scope({
  searchQuery: {
    "+": "reject",
    "from?": "string",
    "to?": "string",
    "cc?": "string",
    "bcc?": "string",
    "header?": {
      "+": "reject",
      field: "string",
      contains: "string",
    },
    "before?": QueryDate,
    "after?": QueryDate,
    "on?": QueryDate,
    "sentBefore?": QueryDate,
    "sentAfter?": QueryDate,
    "sentOn?": QueryDate,
    "hasFlags?": "string[]",
    "missingFlags?": "string[]",
    "body?": "string",
    "text?": "string",
    "largerThan?": "number",
    "smallerThan?": "number",
    "and?": "searchQuery[]",
    "or?": "searchQuery[]",
    "not?": "searchQuery",
  },
}).export().searchQuery;

const SearchLimit = type("number.integer").atLeast(1);

const SearchArgs = type({
  "+": "reject",
  "mailbox?": "string",
  "query?": JSONObject,
  "limit?": SearchLimit,
});

const ReadArgs = type({
  "+": "reject",
  ref: MessageRefShape,
  "parts?": "string",
});

// RFC 2177 § 3 advises terminating and re-issuing IDLE at least every 29
// minutes, so that is the longest span a wait may hold open.
// https://www.rfc-editor.org/rfc/rfc2177.txt
const MAX_WAIT_SECONDS = 29 * 60;

// `timeout * 1000` reaches setTimeout, whose delay is a signed 32-bit count: a
// larger delay is coerced to 1ms and the wait returns almost at once.
const WaitTimeoutSeconds = type("number.integer")
  .atLeast(1)
  .atMost(MAX_WAIT_SECONDS);

const WaitArgs = type({
  "+": "reject",
  "query?": JSONObject,
  "timeout?": WaitTimeoutSeconds,
  "mailbox?": "string",
});

const FlagArgs = type({
  "+": "reject",
  ref: MessageRefShape,
  "set?": "string[]",
  "clear?": "string[]",
});

const ExpungeArgs = type({ "+": "reject" });

// Exported so definitions.test.ts can pair each tool's advertised schema
// against the shape enforced here.
export const ARGUMENT_SHAPES = {
  mail_send: SendArgs,
  mail_reply: ReplyArgs,
  mail_search: SearchArgs,
  mail_read: ReadArgs,
  mail_wait: WaitArgs,
  mail_flag: FlagArgs,
  mail_expunge: ExpungeArgs,
} satisfies Record<MailToolName, Type<object>>;

// The names arktype's `"+": "reject"` cannot refuse. Its compiled check asks
// `k in propsByKey` over an ordinary object, so every name Object.prototype
// carries answers "declared" and is accepted. arktype 2.2 offers no setting
// for that, so the names are refused here, ahead of the shape.
const INHERITED_NAMES: ReadonlySet<string> = new Set(
  Object.getOwnPropertyNames(Object.prototype),
);

// The keys whose values no shape in this file closes: a 'payload' is the
// caller's own JSON, and a 'query' is closed by SearchQueryArgs instead.
const OPAQUE_ARGUMENT_KEYS: ReadonlySet<string> = new Set(["payload", "query"]);

const NO_OPAQUE_KEYS: ReadonlySet<string> = new Set();

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

/**
 * The dotted path of the first reserved name found anywhere in `value`, or
 * undefined when it carries none. `opaque` names the keys whose values are not
 * descended into.
 */
function findInheritedName(
  value: unknown,
  opaque: ReadonlySet<string>,
  path = "",
): string | undefined {
  if (Array.isArray(value)) {
    for (const [index, element] of value.entries()) {
      const found = findInheritedName(element, opaque, `${path}[${index}]`);
      if (found !== undefined) return found;
    }
    return undefined;
  }
  if (!isRecord(value)) return undefined;

  for (const key of Object.keys(value)) {
    const at = path === "" ? key : `${path}.${key}`;
    if (INHERITED_NAMES.has(key)) return at;
    if (opaque.has(key)) continue;
    const found = findInheritedName(value[key], opaque, at);
    if (found !== undefined) return found;
  }
  return undefined;
}

function inheritedNameResult(
  callId: string,
  value: unknown,
  opaque: ReadonlySet<string>,
  code: MailToolErrorCode,
): ToolResult | undefined {
  const found = findInheritedName(value, opaque);
  if (found === undefined) return undefined;
  return errorResult(callId, `${found} must be removed`, code);
}

// `NONEXISTENT` is RFC 5530's condition for a mailbox that is not there, and a
// transport raises it before it attempts the operation. Any other rejection
// carries `operationCode`, worded by `describe`.
function transportFailureResult(
  callId: string,
  cause: unknown,
  operationCode: MailToolErrorCode,
  describe: (message: string) => string,
): ToolResult {
  const message = cause instanceof Error ? cause.message : String(cause);
  const operationFailure = () =>
    errorResult(callId, describe(message), operationCode);

  if (!isMessageTransportError(cause)) return operationFailure();
  switch (cause.condition) {
    case "NONEXISTENT":
      return errorResult(callId, message, "invalid_mailbox");
    case "CANNOT":
    case "SERVERBUG":
      return operationFailure();
  }
}

function searchFailureResult(callId: string, cause: unknown): ToolResult {
  return transportFailureResult(
    callId,
    cause,
    "search_failed",
    (message) => message,
  );
}

// ---------------------------------------------------------------------------
// Individual tool handlers
// ---------------------------------------------------------------------------

export function makeMailSendHandler(transport: MessageTransport): ToolHandler {
  return async (call, signal) => {
    const reserved = inheritedNameResult(
      call.id,
      call.arguments,
      OPAQUE_ARGUMENT_KEYS,
      "invalid_arguments",
    );
    if (reserved !== undefined) return reserved;

    const args = SendArgs(call.arguments);
    if (args instanceof type.errors) {
      return errorResult(call.id, args.summary, "invalid_arguments");
    }

    // An empty list names no destination, so the call has nowhere to send.
    if (Array.isArray(args.to) && args.to.length === 0) {
      return errorResult(
        call.id,
        "'to' must name at least one recipient",
        "invalid_arguments",
      );
    }

    const { content, payload } = args;

    if (content !== undefined && payload !== undefined) {
      return errorResult(
        call.id,
        "provide either 'content' or 'payload', not both",
        "invalid_arguments",
      );
    }
    if (content === undefined && payload === undefined) {
      return errorResult(
        call.id,
        "provide a body in 'content' or 'payload'",
        "invalid_arguments",
      );
    }

    const messageType = args.type ?? "conversation.message";

    if (isConversationType(messageType) && payload !== undefined) {
      return errorResult(
        call.id,
        `'${messageType}' is a conversation type, so its body belongs in 'content'; 'payload' is for a structured type`,
        "invalid_arguments",
      );
    }
    if (!isConversationType(messageType) && content !== undefined) {
      return errorResult(
        call.id,
        `'${messageType}' is a structured type, so its body belongs in 'payload'; 'content' is for a conversation type`,
        "invalid_arguments",
      );
    }

    const outbound: OutboundMessage = {
      to: args.to,
      type: messageType,
    };

    if (args.subject !== undefined) {
      outbound.subject = args.subject;
    }
    if (content !== undefined) {
      outbound.content = content;
    }
    if (payload !== undefined) {
      outbound.payload = payload;
    }
    if (args.inReplyTo !== undefined) {
      outbound.inReplyTo = args.inReplyTo;
    }
    if (args.correlationId !== undefined) {
      outbound.correlationId = args.correlationId;
    }

    let receipt;
    try {
      receipt = await transport.send(outbound, signal);
    } catch (cause) {
      return errorResult(
        call.id,
        `send_failed: ${cause instanceof Error ? cause.message : String(cause)}`,
        "send_failed",
      );
    }

    return { callId: call.id, content: { messageId: receipt.messageId } };
  };
}

export function makeMailReplyHandler(transport: MessageTransport): ToolHandler {
  return async (call, signal) => {
    const reserved = inheritedNameResult(
      call.id,
      call.arguments,
      OPAQUE_ARGUMENT_KEYS,
      "invalid_arguments",
    );
    if (reserved !== undefined) return reserved;

    const args = ReplyArgs(call.arguments);
    if (args instanceof type.errors) {
      return errorResult(call.id, args.summary, "invalid_arguments");
    }

    const { content, payload } = args;

    if (content !== undefined && payload !== undefined) {
      return errorResult(
        call.id,
        "provide either 'content' or 'payload', not both",
        "invalid_arguments",
      );
    }
    if (content === undefined && payload === undefined) {
      return errorResult(
        call.id,
        "provide a body in 'content' or 'payload'",
        "invalid_arguments",
      );
    }

    const messageType = args.type ?? "conversation.message";

    if (isConversationType(messageType) && payload !== undefined) {
      return errorResult(
        call.id,
        `'${messageType}' is a conversation type, so its body belongs in 'content'; 'payload' is for a structured type`,
        "invalid_arguments",
      );
    }
    if (!isConversationType(messageType) && content !== undefined) {
      return errorResult(
        call.id,
        `'${messageType}' is a structured type, so its body belongs in 'payload'; 'content' is for a conversation type`,
        "invalid_arguments",
      );
    }

    const messageRef = args.ref;

    // Fetch the parent message to retrieve threading headers.
    let parentHeaders;
    try {
      parentHeaders = await transport.fetchHeaders(messageRef, signal);
    } catch (cause) {
      return transportFailureResult(
        call.id,
        cause,
        "not_found",
        (message) => `failed to fetch parent message: ${message}`,
      );
    }

    if (parentHeaders.from === undefined) {
      return errorResult(
        call.id,
        "the message being replied to carries no From header, so it has no reply address",
        "no_reply_address",
      );
    }

    const outbound: OutboundMessage = {
      to: parentHeaders.from,
      type: messageType,
    };

    if (parentHeaders.messageId !== undefined) {
      outbound.inReplyTo = parentHeaders.messageId;
      // The full RFC 5322 References chain for a reply is the parent's own
      // References plus the parent's Message-Id. The parent is in hand here
      // (fetched above for its threading headers), so build the complete
      // ancestry rather than leaving the transport to derive a single-element
      // chain from inReplyTo alone.
      outbound.references = [
        ...(parentHeaders.references ?? []),
        parentHeaders.messageId,
      ];
    }

    // Carry forward the subject if available.
    if (parentHeaders.subject !== undefined) {
      outbound.subject = parentHeaders.subject;
    }

    // The reactor's tryCorrelate keys on Interchange-Correlation-ID alone, so a
    // reply that drops it leaves the requester waiting.
    if (parentHeaders.interchangeCorrelationId !== undefined) {
      outbound.correlationId = parentHeaders.interchangeCorrelationId;
    }

    if (content !== undefined) {
      outbound.content = content;
    }
    if (payload !== undefined) {
      outbound.payload = payload;
    }

    let receipt;
    try {
      receipt = await transport.send(outbound, signal);
    } catch (cause) {
      return errorResult(
        call.id,
        `send_failed: ${cause instanceof Error ? cause.message : String(cause)}`,
        "send_failed",
      );
    }

    return { callId: call.id, content: { messageId: receipt.messageId } };
  };
}

export function makeMailSearchHandler(
  transport: MessageTransport,
): ToolHandler {
  return async (call, signal) => {
    const reserved = inheritedNameResult(
      call.id,
      call.arguments,
      OPAQUE_ARGUMENT_KEYS,
      "invalid_arguments",
    );
    if (reserved !== undefined) return reserved;

    const args = SearchArgs(call.arguments);
    if (args instanceof type.errors) {
      return errorResult(call.id, args.summary, "invalid_arguments");
    }

    const mailbox = args.mailbox ?? "INBOX";
    const limit = args.limit ?? 20;

    const reservedFilter = inheritedNameResult(
      call.id,
      args.query,
      NO_OPAQUE_KEYS,
      "invalid_query",
    );
    if (reservedFilter !== undefined) return reservedFilter;

    const query = SearchQueryArgs(args.query ?? {});
    if (query instanceof type.errors) {
      return errorResult(call.id, query.summary, "invalid_query");
    }

    let refs;
    try {
      refs = await transport.search(mailbox, query, signal);
    } catch (cause) {
      return searchFailureResult(call.id, cause);
    }

    const limited = refs.slice(0, limit);

    // Fetch summary headers for each result.
    const summaries = await Promise.all(
      limited.map(async (ref) => {
        try {
          const headers = await transport.fetchHeaders(ref, signal);
          return {
            ref,
            from: headers.from,
            subject: headers.subject,
            date: headers.date,
            interchangeType: headers.interchangeType,
            messageId: headers.messageId,
          };
        } catch {
          return { ref };
        }
      }),
    );

    return { callId: call.id, content: { results: summaries } };
  };
}

export function makeMailReadHandler(transport: MessageTransport): ToolHandler {
  return async (call, signal) => {
    const reserved = inheritedNameResult(
      call.id,
      call.arguments,
      OPAQUE_ARGUMENT_KEYS,
      "invalid_arguments",
    );
    if (reserved !== undefined) return reserved;

    const args = ReadArgs(call.arguments);
    if (args instanceof type.errors) {
      return errorResult(call.id, args.summary, "invalid_arguments");
    }

    const messageRef = args.ref;
    const parts = args.parts ?? "payload";

    // A rejection naming no condition leaves it open whether the message is
    // there at all, and `fetchFull` and `fetchPart` reject that way both for a
    // message that is gone and for one that is there but cannot be read back.
    // Re-read the headers to find out: a reference the headers still answer
    // for names a message that exists, so the failure is `presentCode`.
    const readFailure = async (
      cause: unknown,
      presentCode: MailToolErrorCode,
    ): Promise<ToolResult> => {
      const operationFailure = () =>
        transportFailureResult(
          call.id,
          cause,
          presentCode,
          (message) => `${presentCode}: ${message}`,
        );

      // A rejection that names its condition has already said what failed.
      if (isMessageTransportError(cause)) return operationFailure();
      try {
        await transport.fetchHeaders(messageRef, signal);
      } catch (probeCause) {
        return transportFailureResult(
          call.id,
          probeCause,
          "not_found",
          (message) => `not_found: ${message}`,
        );
      }
      return operationFailure();
    };

    if (parts === "headers") {
      let headers;
      try {
        headers = await transport.fetchHeaders(messageRef, signal);
      } catch (cause) {
        return transportFailureResult(
          call.id,
          cause,
          "not_found",
          (message) => `not_found: ${message}`,
        );
      }
      return { callId: call.id, content: { headers } };
    }

    if (parts === "full") {
      let message;
      try {
        message = await transport.fetchFull(messageRef, signal);
      } catch (cause) {
        return await readFailure(cause, "fetch_failed");
      }
      return {
        callId: call.id,
        content: {
          headers: message.headers,
          content: message.content,
          payload: message.payload,
          signatureStatus: message.signatureStatus,
          flags: message.flags,
        },
      };
    }

    if (parts === "payload") {
      let message;
      try {
        message = await transport.fetchFull(messageRef, signal);
      } catch (cause) {
        return await readFailure(cause, "fetch_failed");
      }

      if (message.payload !== undefined) {
        return { callId: call.id, content: { payload: message.payload } };
      }
      // Conversation message — return content field.
      return {
        callId: call.id,
        content: {
          content: message.content,
          interchangeType: message.headers.interchangeType,
        },
      };
    }

    // Specific MIME part path (e.g. "1.3").
    let part;
    try {
      part = await transport.fetchPart(messageRef, parts, signal);
    } catch (cause) {
      return await readFailure(cause, "invalid_part");
    }

    return {
      callId: call.id,
      content: {
        contentType: part.contentType,
        encoding: part.encoding,
        content: new TextDecoder().decode(part.content),
      },
    };
  };
}

// The deadline seam, shaped after the `Scheduler` of
// packages/inference/src/harness.ts: setTimeout returns its canceller.
export type WaitScheduler = {
  setTimeout(callback: () => void, delayMs: number): () => void;
};

function createDefaultWaitScheduler(): WaitScheduler {
  return {
    setTimeout(callback, delayMs) {
      const handle = setTimeout(callback, delayMs);
      return () => {
        clearTimeout(handle);
      };
    },
  };
}

export function makeMailWaitHandler(
  transport: MessageTransport,
  scheduler: WaitScheduler = createDefaultWaitScheduler(),
): ToolHandler {
  return async (call, signal) => {
    const reserved = inheritedNameResult(
      call.id,
      call.arguments,
      OPAQUE_ARGUMENT_KEYS,
      "invalid_arguments",
    );
    if (reserved !== undefined) return reserved;

    const args = WaitArgs(call.arguments);
    if (args instanceof type.errors) {
      return errorResult(call.id, args.summary, "invalid_arguments");
    }

    const timeoutSeconds = args.timeout ?? 120;
    const mailbox = args.mailbox ?? "INBOX";

    const reservedFilter = inheritedNameResult(
      call.id,
      args.query,
      NO_OPAQUE_KEYS,
      "invalid_query",
    );
    if (reservedFilter !== undefined) return reservedFilter;

    const query = SearchQueryArgs(args.query ?? {});
    if (query instanceof type.errors) {
      return errorResult(call.id, query.summary, "invalid_query");
    }

    // Returns undefined when nothing matched, and a result -- never a
    // rejection -- for every other outcome: a throw out of the watch callback
    // below has nowhere to go.
    const firstMatch = async (): Promise<ToolResult | undefined> => {
      let refs;
      try {
        refs = await transport.search(mailbox, query, signal);
      } catch (cause) {
        return searchFailureResult(call.id, cause);
      }

      const ref = refs[0];
      if (ref === undefined) return undefined;

      let message;
      try {
        message = await transport.fetchFull(ref, signal);
      } catch (cause) {
        return transportFailureResult(
          call.id,
          cause,
          "fetch_failed",
          (message) => `failed to fetch matching message: ${message}`,
        );
      }
      return {
        callId: call.id,
        content: {
          ref,
          from: message.headers.from,
          subject: message.headers.subject,
          content: message.content,
        },
      };
    };

    // The first read of the mailbox lives inside the promise too, so the
    // deadline and the abort listener armed below cover it.
    return new Promise<ToolResult>((resolve) => {
      let settled = false;
      const teardowns: (() => void)[] = [];

      const runTeardown = (teardown: () => void) => {
        try {
          teardown();
        } catch (cause) {
          logger.error`mail_wait could not dismantle a settled wait on ${mailbox}: ${cause instanceof Error ? cause.message : String(cause)}`;
        }
      };

      const settle = (result: ToolResult) => {
        if (settled) return;
        settled = true;
        resolve(result);
        for (const teardown of teardowns) {
          runTeardown(teardown);
        }
      };

      // A teardown registered after the promise settled runs at once: settle
      // walks only the list it already holds.
      const addTeardown = (teardown: () => void) => {
        if (settled) {
          runTeardown(teardown);
          return;
        }
        teardowns.push(teardown);
      };

      addTeardown(
        scheduler.setTimeout(() => {
          settle(
            errorResult(
              call.id,
              `Timed out after ${String(timeoutSeconds)}s waiting for a matching message`,
              "timeout",
            ),
          );
        }, timeoutSeconds * 1000),
      );

      const onAbort = () => {
        settle(errorResult(call.id, "aborted", "aborted"));
      };

      signal.addEventListener("abort", onAbort, { once: true });
      addTeardown(() => {
        signal.removeEventListener("abort", onAbort);
      });
      if (signal.aborted) onAbort();

      // Reads are chained rather than overlapped, so an arrival is never
      // checked against a mailbox the previous read has not finished with.
      let checks = Promise.resolve();

      const onCheckFailure = (cause: unknown) => {
        settle(
          errorResult(
            call.id,
            cause instanceof Error ? cause.message : String(cause),
            "internal_error",
          ),
        );
      };

      const watchArrivals = () => {
        const unsubscribe = transport.watch(mailbox, (event) => {
          if (settled) return;
          if (event.type !== "exists") return;

          checks = checks
            .then(async () => {
              if (settled) return;
              const match = await firstMatch();
              if (match !== undefined) settle(match);
            })
            .catch(onCheckFailure);
        });
        addTeardown(unsubscribe);
      };

      checks = checks
        .then(async () => {
          const existing = await firstMatch();
          if (existing !== undefined) {
            settle(existing);
            return;
          }
          if (settled) return;
          watchArrivals();
        })
        .catch(onCheckFailure);
    });
  };
}

export function makeMailFlagHandler(transport: MessageTransport): ToolHandler {
  return async (call, signal) => {
    const reserved = inheritedNameResult(
      call.id,
      call.arguments,
      OPAQUE_ARGUMENT_KEYS,
      "invalid_arguments",
    );
    if (reserved !== undefined) return reserved;

    const args = FlagArgs(call.arguments);
    if (args instanceof type.errors) {
      return errorResult(call.id, args.summary, "invalid_arguments");
    }

    const set = args.set ?? [];
    const clear = args.clear ?? [];
    // Reject an empty mutation at the boundary: a call with neither direction
    // does nothing, and firing an empty flag write would still round-trip to
    // the supervisor as a pointless commit.
    if (set.length === 0 && clear.length === 0) {
      return errorResult(
        call.id,
        "provide flags in 'set' or 'clear'",
        "invalid_arguments",
      );
    }
    // One direction per call. Adding and removing flags in one call would be
    // two separate supervisor round-trips; if the first landed and the second
    // failed, one error would have to cover a half-applied mutation.
    if (set.length > 0 && clear.length > 0) {
      return errorResult(
        call.id,
        "provide 'set' or 'clear', not both -- call mail_flag once per direction",
        "invalid_arguments",
      );
    }

    try {
      if (set.length > 0) {
        await transport.setFlags(args.ref, set, signal);
      } else {
        await transport.clearFlags(args.ref, clear, signal);
      }
    } catch (cause) {
      // A rejection has three readings: the uid names no message, the
      // supervisor refused the mutation, or it applied the mutation and lost
      // the reply when the control channel went down. Nothing here separates
      // the first from the other two, and IMAP does not report it either --
      // RFC 9051 section 6.4.8 makes a UID STORE against an absent uid a
      // silent no-op, so there is no condition for the transport to have
      // raised. The weakest of the three is therefore what this answers.
      return transportFailureResult(
        call.id,
        cause,
        "flag_failed",
        (message) => `flag_failed: ${message}`,
      );
    }

    return { callId: call.id, content: { ok: true } };
  };
}

export function makeMailExpungeHandler(
  transport: MessageTransport,
): ToolHandler {
  return async (call, signal) => {
    const reserved = inheritedNameResult(
      call.id,
      call.arguments,
      OPAQUE_ARGUMENT_KEYS,
      "invalid_arguments",
    );
    if (reserved !== undefined) return reserved;

    const args = ExpungeArgs(call.arguments);
    if (args instanceof type.errors) {
      return errorResult(call.id, args.summary, "invalid_arguments");
    }

    // The warm agent owns exactly one mailbox; expunge sweeps its INBOX.
    let outcome;
    try {
      outcome = await transport.expunge("INBOX", signal);
    } catch (cause) {
      // A rejection naming no condition leaves the outcome unknown: the sweep
      // may have been refused, or applied with the reply lost. NONEXISTENT is
      // raised before the sweep, so it reports the mailbox instead.
      return transportFailureResult(
        call.id,
        cause,
        "expunge_failed",
        (message) => `expunge_failed: ${message}`,
      );
    }

    return {
      callId: call.id,
      content: { ok: true, expungedUids: outcome.expungedUids },
    };
  };
}
