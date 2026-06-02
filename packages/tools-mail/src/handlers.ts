// Per-tool handler factories for the five mail tools. Each factory takes
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

import { type } from "arktype";
import type {
  MessageTransport,
  ToolCall,
  ToolResult,
  OutboundMessage,
  SearchQuery,
} from "@intx/types/runtime";
import { InterchangeType } from "@intx/types/runtime";

export type ToolHandler = (
  call: ToolCall,
  signal: AbortSignal,
) => Promise<ToolResult>;

// ---------------------------------------------------------------------------
// Argument schemas
// ---------------------------------------------------------------------------

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

export function makeMailSendHandler(transport: MessageTransport): ToolHandler {
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

export function makeMailReplyHandler(transport: MessageTransport): ToolHandler {
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

    // OutboundMessage does not carry a References field; the transport
    // builds the threading chain from inReplyTo when delivering the
    // reply.

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

export function makeMailReadHandler(transport: MessageTransport): ToolHandler {
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

export function makeMailWaitHandler(transport: MessageTransport): ToolHandler {
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
