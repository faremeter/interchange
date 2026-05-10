// Mail tool implementations for the harness.
//
// Each tool wraps the MessageTransport interface, translating model-supplied
// arguments into transport calls and returning structured results the model
// can reason about.
//
// (MESSAGE.md § Mail Tools)

import { type } from "arktype";
import type {
  MessageTransport,
  ToolRunner,
  ToolCall,
  ToolResult,
  ToolDefinition,
  OutboundMessage,
  SearchQuery,
} from "@interchange/types/runtime";

type ToolHandler = (call: ToolCall, signal: AbortSignal) => Promise<ToolResult>;

// ---------------------------------------------------------------------------
// Argument schemas
// ---------------------------------------------------------------------------

const InterchangeType = type.enumerated(
  "conversation.message",
  "conversation.join",
  "conversation.leave",
  "offering.request",
  "offering.response",
  "offering.error",
  "offering.discover",
  "offering.catalog",
  "payment.required",
  "payment.receipt",
  "payment.verified",
  "approval.request",
  "approval.granted",
  "approval.denied",
  "system.health",
  "system.register",
  "system.deregister",
  "system.credential.refresh",
);
type InterchangeType = typeof InterchangeType.infer;

const SendArgs = type({
  to: "string | string[]",
  "type?": InterchangeType,
  "content?": "string",
  "payload?": "Record<string, unknown>",
  "subject?": "string",
  "inReplyTo?": "string",
});

const ReplyArgs = type({
  ref: { uid: "number", mailbox: "string" },
  "type?": InterchangeType,
  "content?": "string",
  "payload?": "Record<string, unknown>",
});

const SearchArgs = type({
  "mailbox?": "string",
  "query?": "Record<string, unknown>",
  "limit?": "number",
});

const ReadArgs = type({
  ref: { uid: "number", mailbox: "string" },
  "parts?": "string",
});

const WaitArgs = type({
  "query?": "Record<string, unknown>",
  "timeout?": "number",
  "mailbox?": "string",
});

// ---------------------------------------------------------------------------
// Individual tool handlers
// ---------------------------------------------------------------------------

function makeMailSendHandler(transport: MessageTransport): ToolHandler {
  return async (call, signal) => {
    const args = SendArgs(call.arguments);
    if (args instanceof type.errors) {
      return errorResult(call.id, args.summary);
    }

    const { content, payload } = args;

    if (content !== undefined && payload !== undefined) {
      return errorResult(
        call.id,
        "provide either 'content' or 'payload', not both",
      );
    }

    const outbound: OutboundMessage = {
      to: args.to,
      type: args.type ?? "conversation.message",
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

function makeMailReplyHandler(transport: MessageTransport): ToolHandler {
  return async (call, signal) => {
    const args = ReplyArgs(call.arguments);
    if (args instanceof type.errors) {
      return errorResult(call.id, args.summary);
    }

    const messageRef = args.ref;

    // Fetch the parent message to retrieve threading headers.
    let parentHeaders;
    try {
      parentHeaders = await transport.fetchHeaders(messageRef, signal);
    } catch (cause) {
      return errorResult(
        call.id,
        `failed to fetch parent message: ${cause instanceof Error ? cause.message : String(cause)}`,
      );
    }

    const { content, payload } = args;

    if (content !== undefined && payload !== undefined) {
      return errorResult(
        call.id,
        "provide either 'content' or 'payload', not both",
      );
    }

    // Build References: parent's References + parent's Message-ID
    const parentReferences = parentHeaders.references ?? [];
    const references = [...parentReferences, parentHeaders.messageId];

    const outbound: OutboundMessage = {
      to: parentHeaders.from,
      type: args.type ?? "conversation.message",
      inReplyTo: parentHeaders.messageId,
    };

    // Carry forward the subject if available.
    if (parentHeaders.subject !== undefined) {
      outbound.subject = parentHeaders.subject;
    }

    if (content !== undefined) {
      outbound.content = content;
    }
    if (payload !== undefined) {
      outbound.payload = payload;
    }

    // Include References in a summary field for the transport to set as a
    // standard header. The OutboundMessage type doesn't carry a 'references'
    // field directly, so we embed it in a custom field that the transport
    // may use. For now we record it in the payload for structured messages.
    // For conversation replies the transport uses inReplyTo to build the chain.
    void references; // acknowledged but transport builds chain from inReplyTo

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

function makeMailSearchHandler(transport: MessageTransport): ToolHandler {
  return async (call, signal) => {
    const args = SearchArgs(call.arguments);
    if (args instanceof type.errors) {
      return errorResult(call.id, args.summary);
    }

    const mailbox = args.mailbox ?? "INBOX";
    const query = args.query ?? {};
    const limit = args.limit ?? 20;

    let refs;
    try {
      refs = await transport.search(mailbox, query as SearchQuery, signal);
    } catch (cause) {
      const msg = cause instanceof Error ? cause.message : String(cause);
      const code = msg.includes("does not exist")
        ? "invalid_mailbox"
        : "invalid_query";
      return errorResult(call.id, msg, code);
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

function makeMailReadHandler(transport: MessageTransport): ToolHandler {
  return async (call, signal) => {
    const args = ReadArgs(call.arguments);
    if (args instanceof type.errors) {
      return errorResult(call.id, args.summary);
    }

    const messageRef = args.ref;
    const parts = args.parts ?? "payload";

    if (parts === "headers") {
      let headers;
      try {
        headers = await transport.fetchHeaders(messageRef, signal);
      } catch (cause) {
        return errorResult(
          call.id,
          `not_found: ${cause instanceof Error ? cause.message : String(cause)}`,
          "not_found",
        );
      }
      return { callId: call.id, content: { headers } };
    }

    if (parts === "full") {
      let message;
      try {
        message = await transport.fetchFull(messageRef, signal);
      } catch (cause) {
        return errorResult(
          call.id,
          `not_found: ${cause instanceof Error ? cause.message : String(cause)}`,
          "not_found",
        );
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
        return errorResult(
          call.id,
          `not_found: ${cause instanceof Error ? cause.message : String(cause)}`,
          "not_found",
        );
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
      return errorResult(
        call.id,
        `invalid_part: ${cause instanceof Error ? cause.message : String(cause)}`,
        "invalid_part",
      );
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

function makeMailWaitHandler(transport: MessageTransport): ToolHandler {
  return async (call, signal) => {
    const args = WaitArgs(call.arguments);
    if (args instanceof type.errors) {
      return errorResult(call.id, args.summary);
    }

    const query = args.query ?? {};
    const timeoutSeconds = args.timeout ?? 120;
    const mailbox = args.mailbox ?? "INBOX";

    // Check for an existing match first.
    const existing = await transport.search(
      mailbox,
      query as SearchQuery,
      signal,
    );
    const firstMatch = existing[0];
    if (firstMatch !== undefined) {
      const message = await transport.fetchFull(firstMatch, signal);
      return {
        callId: call.id,
        content: {
          ref: firstMatch,
          from: message.headers.from,
          subject: message.headers.subject,
          content: message.content,
        },
      };
    }

    // No match yet — watch for new arrivals.
    return new Promise<ToolResult>((resolve) => {
      let settled = false;

      const unsubscribe = transport.watch(mailbox, (event) => {
        if (settled) return;
        if (event.type !== "exists") return;

        // Match against the query's 'from' field (the primary use case).
        if (
          typeof query.from === "string" &&
          event.headers.from !== query.from
        ) {
          return;
        }

        settled = true;
        unsubscribe();
        clearTimeout(timer);

        void (async () => {
          const ref = { uid: event.uid, mailbox };
          const message = await transport.fetchFull(ref, signal);
          resolve({
            callId: call.id,
            content: {
              ref,
              from: message.headers.from,
              subject: message.headers.subject,
              content: message.content,
            },
          });
        })();
      });

      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        unsubscribe();
        resolve(
          errorResult(
            call.id,
            `Timed out after ${timeoutSeconds}s waiting for a matching message`,
            "timeout",
          ),
        );
      }, timeoutSeconds * 1000);

      // Respect the abort signal.
      signal.addEventListener(
        "abort",
        () => {
          if (settled) return;
          settled = true;
          unsubscribe();
          clearTimeout(timer);
          resolve(errorResult(call.id, "aborted", "aborted"));
        },
        { once: true },
      );
    });
  };
}

// ---------------------------------------------------------------------------
// Tool name registry
// ---------------------------------------------------------------------------

export type MailToolName =
  | "mail_send"
  | "mail_reply"
  | "mail_search"
  | "mail_read"
  | "mail_wait";

/**
 * Tool definitions for the built-in mail tools, suitable for passing to
 * the inference provider so the model knows these tools exist.
 */
export function getMailToolDefinitions(): ToolDefinition[] {
  return [
    {
      name: "mail_send",
      description:
        "Send mail to another agent or address. Use this to initiate conversations or send information to other agents.",
      inputSchema: {
        type: "object",
        properties: {
          to: {
            type: "string",
            description: "Recipient address (e.g. agent@local.interchange)",
          },
          content: {
            type: "string",
            description: "Message text content",
          },
          type: {
            type: "string",
            description: "Message type (default: conversation.message)",
            default: "conversation.message",
          },
          subject: {
            type: "string",
            description: "Optional subject line",
          },
          inReplyTo: {
            type: "string",
            description: "Message-ID of the message being replied to",
          },
        },
        required: ["to", "content"],
      },
    },
    {
      name: "mail_reply",
      description:
        "Reply to mail by reference. Addresses the reply to the original sender and sets inReplyTo for threading.",
      inputSchema: {
        type: "object",
        properties: {
          ref: {
            type: "object",
            description: "Message reference { uid, mailbox }",
            properties: {
              uid: { type: "number" },
              mailbox: { type: "string" },
            },
            required: ["uid", "mailbox"],
          },
          content: {
            type: "string",
            description: "Reply text content",
          },
          type: {
            type: "string",
            description: "Message type (default: conversation.message)",
            default: "conversation.message",
          },
        },
        required: ["ref", "content"],
      },
    },
    {
      name: "mail_search",
      description: "Search mail in a mailbox. Returns message summaries.",
      inputSchema: {
        type: "object",
        properties: {
          mailbox: {
            type: "string",
            description: "Mailbox to search (default: INBOX)",
            default: "INBOX",
          },
          query: {
            type: "object",
            description: "Search query (e.g. { from: 'agent@...' })",
          },
          limit: {
            type: "number",
            description: "Maximum results to return (default: 20)",
            default: 20,
          },
        },
      },
    },
    {
      name: "mail_read",
      description: "Read a specific mail message by reference.",
      inputSchema: {
        type: "object",
        properties: {
          ref: {
            type: "object",
            description: "Message reference { uid, mailbox }",
            properties: {
              uid: { type: "number" },
              mailbox: { type: "string" },
            },
            required: ["uid", "mailbox"],
          },
          parts: {
            type: "string",
            description:
              "What to fetch: 'full', 'headers', 'payload', or a MIME part path",
            default: "payload",
          },
        },
        required: ["ref"],
      },
    },
    {
      name: "mail_wait",
      description:
        "Wait for a message matching a query to arrive. Blocks until a matching message is delivered or the timeout expires. Use this instead of polling mail_search in a loop.",
      inputSchema: {
        type: "object",
        properties: {
          query: {
            type: "object",
            description:
              "Search criteria for the message to wait for (e.g. { from: 'agent@...' })",
          },
          timeout: {
            type: "number",
            description:
              "Maximum seconds to wait before returning a timeout error (default: 120)",
            default: 120,
          },
          mailbox: {
            type: "string",
            description: "Mailbox to watch (default: INBOX)",
            default: "INBOX",
          },
        },
        required: ["query"],
      },
    },
  ];
}

/**
 * Build a map of mail tool name → handler for the given transport.
 */
export function buildMailToolHandlers(
  transport: MessageTransport,
): Map<string, ToolHandler> {
  const handlers = new Map<string, ToolHandler>();
  handlers.set("mail_send", makeMailSendHandler(transport));
  handlers.set("mail_reply", makeMailReplyHandler(transport));
  handlers.set("mail_search", makeMailSearchHandler(transport));
  handlers.set("mail_read", makeMailReadHandler(transport));
  handlers.set("mail_wait", makeMailWaitHandler(transport));
  return handlers;
}

/**
 * Combine mail tool handlers with a caller-supplied ToolRunner into a
 * single unified ToolRunner. Throws at call time if a name collision exists
 * between mail tools and the caller-supplied tools.
 *
 * The collision check runs at construction time (startup), not at invocation
 * time, so it fails loudly before any inference happens.
 */
export function buildCombinedRunner(
  mailHandlers: Map<string, ToolHandler>,
  callerTools: ToolRunner,
  callerToolDefs: ToolDefinition[],
): ToolRunner {
  // Check for collisions at startup.
  for (const def of callerToolDefs) {
    if (mailHandlers.has(def.name)) {
      throw new Error(
        `Tool name collision: "${def.name}" is registered by both the mail tools and the caller-provided ToolRunner`,
      );
    }
  }

  return {
    async run(call: ToolCall, signal: AbortSignal): Promise<ToolResult> {
      const handler = mailHandlers.get(call.name);
      if (handler !== undefined) {
        return handler(call, signal);
      }
      return callerTools.run(call, signal);
    },
  };
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

function errorResult(
  callId: string,
  message: string,
  code?: string,
): ToolResult {
  const content: Record<string, unknown> = { error: message };
  if (code !== undefined) {
    content["code"] = code;
  }
  return { callId, content, isError: true };
}
