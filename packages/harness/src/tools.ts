// Message tool implementations for the harness.
//
// Each tool wraps the MessageTransport interface, translating model-supplied
// arguments into transport calls and returning structured results the model
// can reason about.
//
// (MESSAGE.md § Messaging Tools)

import type {
  MessageTransport,
  ToolRunner,
  ToolCall,
  ToolResult,
  SearchQuery,
  InterchangeType,
  OutboundMessage,
  MessageRef,
} from "@interchange/types/runtime";

type ToolHandler = (call: ToolCall, signal: AbortSignal) => Promise<ToolResult>;

// ---------------------------------------------------------------------------
// Individual tool handlers
// ---------------------------------------------------------------------------

function makeMessageSendHandler(transport: MessageTransport): ToolHandler {
  return async (call, signal) => {
    const args = call.arguments;

    const to = args["to"];
    if (to === undefined) {
      return errorResult(call.id, "missing required parameter: to");
    }
    if (typeof to !== "string" && !Array.isArray(to)) {
      return errorResult(call.id, "parameter 'to' must be a string or array");
    }

    const content = args["content"];
    const payload = args["payload"];

    if (content !== undefined && payload !== undefined) {
      return errorResult(
        call.id,
        "provide either 'content' or 'payload', not both",
      );
    }

    const type: InterchangeType =
      typeof args["type"] === "string"
        ? (args["type"] as InterchangeType)
        : "conversation.message";

    const outbound: OutboundMessage = {
      to: to as string | string[],
      type,
    };

    if (typeof args["subject"] === "string") {
      outbound.subject = args["subject"];
    }
    if (typeof content === "string") {
      outbound.content = content;
    }
    if (typeof payload === "object" && payload !== null) {
      outbound.payload = payload as Record<string, unknown>;
    }
    if (typeof args["inReplyTo"] === "string") {
      outbound.inReplyTo = args["inReplyTo"];
    }
    if (typeof args["correlationId"] === "string") {
      outbound.correlationId = args["correlationId"];
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

    const result: Record<string, unknown> = { messageId: receipt.messageId };

    if (typeof args["correlationId"] === "string") {
      result["status"] = "pending";
      result["correlationId"] = args["correlationId"];

      const firstRecipient =
        typeof to === "string"
          ? to
          : Array.isArray(to) && to.length > 0
            ? String(to[0])
            : undefined;

      const marker: ToolResult["pendingMarker"] =
        firstRecipient !== undefined
          ? {
              status: "pending",
              correlationId: args["correlationId"],
              expectedFrom: firstRecipient,
            }
          : {
              status: "pending",
              correlationId: args["correlationId"],
            };

      const toolResult: ToolResult = {
        callId: call.id,
        content: result,
        pendingMarker: marker,
      };
      return toolResult;
    }

    return { callId: call.id, content: result };
  };
}

function makeMessageReplyHandler(transport: MessageTransport): ToolHandler {
  return async (call, signal) => {
    const args = call.arguments;

    const ref = args["ref"];
    if (ref === undefined || typeof ref !== "object" || ref === null) {
      return errorResult(call.id, "missing required parameter: ref");
    }

    const messageRef = ref as MessageRef;

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

    const content = args["content"];
    const payload = args["payload"];

    if (content !== undefined && payload !== undefined) {
      return errorResult(
        call.id,
        "provide either 'content' or 'payload', not both",
      );
    }

    const type: InterchangeType =
      typeof args["type"] === "string"
        ? (args["type"] as InterchangeType)
        : "conversation.message";

    // Build References: parent's References + parent's Message-ID
    const parentReferences = parentHeaders.references ?? [];
    const references = [...parentReferences, parentHeaders.messageId];

    const outbound: OutboundMessage = {
      to: parentHeaders.from,
      type,
      inReplyTo: parentHeaders.messageId,
    };

    // Carry forward the subject if available.
    if (parentHeaders.subject !== undefined) {
      outbound.subject = parentHeaders.subject;
    }

    if (typeof content === "string") {
      outbound.content = content;
    }
    if (typeof payload === "object" && payload !== null) {
      outbound.payload = payload as Record<string, unknown>;
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

function makeMessageSearchHandler(transport: MessageTransport): ToolHandler {
  return async (call, signal) => {
    const args = call.arguments;

    const mailbox =
      typeof args["mailbox"] === "string" ? args["mailbox"] : "INBOX";

    const query =
      typeof args["query"] === "object" && args["query"] !== null
        ? (args["query"] as SearchQuery)
        : {};

    const limit = typeof args["limit"] === "number" ? args["limit"] : 20;

    let refs;
    try {
      refs = await transport.search(mailbox, query, signal);
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

function makeMessageReadHandler(transport: MessageTransport): ToolHandler {
  return async (call, signal) => {
    const args = call.arguments;

    const ref = args["ref"];
    if (ref === undefined || typeof ref !== "object" || ref === null) {
      return errorResult(call.id, "missing required parameter: ref");
    }

    const messageRef = ref as MessageRef;
    const parts = typeof args["parts"] === "string" ? args["parts"] : "payload";

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

// ---------------------------------------------------------------------------
// Tool name registry
// ---------------------------------------------------------------------------

export type MessageToolName =
  | "message.send"
  | "message.reply"
  | "message.search"
  | "message.read";

/**
 * Build a map of message tool name → handler for the given transport.
 */
export function buildMessageToolHandlers(
  transport: MessageTransport,
): Map<string, ToolHandler> {
  const handlers = new Map<string, ToolHandler>();
  handlers.set("message.send", makeMessageSendHandler(transport));
  handlers.set("message.reply", makeMessageReplyHandler(transport));
  handlers.set("message.search", makeMessageSearchHandler(transport));
  handlers.set("message.read", makeMessageReadHandler(transport));
  return handlers;
}

/**
 * Combine message tool handlers with a caller-supplied ToolRunner into a
 * single unified ToolRunner. Throws at call time if a name collision exists
 * between message tools and the caller-supplied tools.
 *
 * The collision check runs at construction time (startup), not at invocation
 * time, so it fails loudly before any inference happens.
 */
export function buildCombinedRunner(
  messageHandlers: Map<string, ToolHandler>,
  callerTools: ToolRunner,
  callerToolNames: string[],
): ToolRunner {
  // Check for collisions at startup.
  for (const name of callerToolNames) {
    if (messageHandlers.has(name)) {
      throw new Error(
        `Tool name collision: "${name}" is registered by both the message tools and the caller-provided ToolRunner`,
      );
    }
  }

  return {
    async run(call: ToolCall, signal: AbortSignal): Promise<ToolResult> {
      const handler = messageHandlers.get(call.name);
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
