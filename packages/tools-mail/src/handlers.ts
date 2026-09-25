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

import { type } from "arktype";
import type {
  MessageTransport,
  ToolCall,
  ToolResult,
  OutboundMessage,
  InboundMessage,
  MessageAttachment,
  SearchQuery,
} from "@intx/types/runtime";
import { InterchangeType } from "@intx/types/runtime";
import {
  base64Encode,
  isTextLikeMimeType,
  mimeTypeAndSubtype,
  validateAttachments,
} from "@intx/types";

export type ToolHandler = (
  call: ToolCall,
  signal: AbortSignal,
) => Promise<ToolResult>;

// ---------------------------------------------------------------------------
// Argument schemas
// ---------------------------------------------------------------------------

// A tool-facing attachment: plain text unless `encoding` says otherwise. The
// model never has to hand-encode base64 for a text file -- `content` is the
// text itself, and `encoding` only exists to opt into base64 (or to force it
// for a text-like type the caller already has base64-encoded).
const AttachmentInput = type({
  name: "string",
  contentType: "string",
  content: "string",
  "encoding?": "'utf-8' | 'base64'",
});
type AttachmentInput = typeof AttachmentInput.infer;

const SendArgs = type({
  to: "string | string[]",
  "type?": InterchangeType,
  "content?": "string",
  "payload?": "Record<string, unknown>",
  "subject?": "string",
  "inReplyTo?": "string",
  "attachments?": AttachmentInput.array(),
});

const ReplyArgs = type({
  ref: { uid: "number", mailbox: "string" },
  "type?": InterchangeType,
  "content?": "string",
  "payload?": "Record<string, unknown>",
  "attachments?": AttachmentInput.array(),
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

const FlagArgs = type({
  ref: { uid: "number", mailbox: "string" },
  "set?": "string[]",
  "clear?": "string[]",
});

const ExpungeArgs = type({});

// ---------------------------------------------------------------------------
// Attachment decoding
// ---------------------------------------------------------------------------

/**
 * Validate tool-supplied attachments against the system attachment policy.
 * Text-like content types carry plain-text `content` by default and
 * everything else base64; an explicit `encoding` overrides the inference.
 */
function decodeAttachments(inputs: readonly AttachmentInput[]) {
  // Text under a binary content type is almost always base64 the model
  // mislabelled; sending it would deliver a corrupt file with no error.
  const mislabelled = inputs.findIndex(
    (input) =>
      input.encoding === "utf-8" && !isTextLikeMimeType(input.contentType),
  );
  if (mislabelled !== -1) {
    return {
      ok: false as const,
      error: {
        code: "invalid_encoding",
        message: `attachment ${String(mislabelled)} is not a text type, so its content must be base64`,
        attachmentIndex: mislabelled,
      },
    };
  }

  return validateAttachments(
    inputs.map((input) => {
      const encoding =
        input.encoding ??
        (isTextLikeMimeType(input.contentType) ? "utf-8" : "base64");
      return {
        name: input.name,
        mimeType: input.contentType,
        data:
          encoding === "base64"
            ? input.content
            : new TextEncoder().encode(input.content),
      };
    }),
  );
}

/**
 * Decode bytes as UTF-8 only when that loses nothing: a malformed sequence
 * yields `undefined` rather than replacement characters, and a leading BOM
 * is kept rather than dropped.
 */
function decodeStrictUTF8(bytes: Uint8Array): string | undefined {
  try {
    return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
      bytes,
    );
  } catch {
    return undefined;
  }
}

/**
 * The `attachments` field of a `mail_read` response, or nothing when the
 * message carries none: enough for the model to know an attachment arrived
 * and how to fetch it with a MIME part path, without inlining the
 * (possibly large) payload.
 *
 * Part numbering is the parsed IMAP path stamped on each attachment by
 * `@intx/mime` `extractAttachments` — the same sibling numbering
 * `extractPartByPath` uses, including skipped inline html/text siblings.
 * Do not recompute `1.${index+2}` here: that only matches writer-shaped
 * mail with no extra parts.
 */
function attachmentsField(message: InboundMessage) {
  if (message.attachments === undefined || message.attachments.length === 0) {
    return {};
  }
  return {
    attachments: message.attachments.map((att) => {
      const listed = {
        name: att.name,
        contentType: att.contentType,
        size: att.data.length,
      };
      if (att.part === undefined) return listed;
      return { ...listed, part: att.part };
    }),
  };
}

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

    let attachments: MessageAttachment[] | undefined;
    if (args.attachments !== undefined) {
      const decoded = decodeAttachments(args.attachments);
      if (!decoded.ok) {
        return errorResult(call.id, decoded.error.message, decoded.error.code);
      }
      attachments = decoded.attachments;
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
    if (attachments !== undefined) {
      outbound.attachments = attachments;
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

    let attachments: MessageAttachment[] | undefined;
    if (args.attachments !== undefined) {
      const decoded = decodeAttachments(args.attachments);
      if (!decoded.ok) {
        return errorResult(call.id, decoded.error.message, decoded.error.code);
      }
      attachments = decoded.attachments;
    }

    const outbound: OutboundMessage = {
      to: parentHeaders.from,
      type: args.type ?? "conversation.message",
      inReplyTo: parentHeaders.messageId,
      // The full RFC 5322 References chain for a reply is the parent's own
      // References plus the parent's Message-Id. The parent is in hand here
      // (fetched above for its threading headers), so build the complete
      // ancestry rather than leaving the transport to derive a single-element
      // chain from inReplyTo alone.
      references: [
        ...(parentHeaders.references ?? []),
        parentHeaders.messageId,
      ],
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
    if (attachments !== undefined) {
      outbound.attachments = attachments;
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
          ...attachmentsField(message),
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
        return {
          callId: call.id,
          content: { payload: message.payload, ...attachmentsField(message) },
        };
      }
      // Conversation message — return content field.
      return {
        callId: call.id,
        content: {
          content: message.content,
          interchangeType: message.headers.interchangeType,
          ...attachmentsField(message),
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

    // Composite parts are valid IMAP (fetchFull uses BODY[1]) but not a
    // leaf the model can attach or quote. Refuse after fetch by type, not
    // by guessing at the path string — `1.1` is a documented leaf.
    if (mimeTypeAndSubtype(part.contentType).startsWith("multipart/")) {
      return errorResult(
        call.id,
        `invalid_part: ${parts} is a composite MIME part`,
        "invalid_part",
      );
    }

    // Text comes back as text; anything else as base64, since decoding
    // arbitrary bytes as UTF-8 would corrupt them. Mirrors the `content` /
    // `encoding` pair the send tools accept.
    const text = isTextLikeMimeType(part.contentType)
      ? decodeStrictUTF8(part.content)
      : undefined;
    return {
      callId: call.id,
      content: {
        contentType: part.contentType,
        encoding: text === undefined ? "base64" : "utf-8",
        content: text ?? base64Encode(part.content),
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

export function makeMailFlagHandler(transport: MessageTransport): ToolHandler {
  return async (call, signal) => {
    const args = FlagArgs(call.arguments);
    if (args instanceof type.errors) {
      return errorResult(call.id, args.summary);
    }

    const set = args.set ?? [];
    const clear = args.clear ?? [];
    // Reject an empty mutation at the boundary: a call with neither direction
    // does nothing, and firing an empty flag write would still round-trip to
    // the supervisor as a pointless commit.
    if (set.length === 0 && clear.length === 0) {
      return errorResult(call.id, "provide flags in 'set' or 'clear'");
    }
    // One direction per call. Adding and removing flags in one call would be
    // two separate supervisor round-trips; if the first landed and the second
    // failed, the error's "mailbox unchanged" contract would be a lie. Keeping
    // each call a single mutation makes that contract unconditionally true.
    if (set.length > 0 && clear.length > 0) {
      return errorResult(
        call.id,
        "provide 'set' or 'clear', not both -- call mail_flag once per direction",
      );
    }

    try {
      if (set.length > 0) {
        await transport.setFlags(args.ref, set, signal);
      } else {
        await transport.clearFlags(args.ref, clear, signal);
      }
    } catch (cause) {
      // A rejection means the supervisor did not apply the mutation, so the
      // mailbox is unchanged. Surface that so the model does not assume the
      // flag stuck (and then expunge expecting the message gone).
      return errorResult(
        call.id,
        `flag not applied: ${cause instanceof Error ? cause.message : String(cause)}`,
        "flag_failed",
      );
    }

    return { callId: call.id, content: { ok: true } };
  };
}

export function makeMailExpungeHandler(
  transport: MessageTransport,
): ToolHandler {
  return async (call, signal) => {
    const args = ExpungeArgs(call.arguments);
    if (args instanceof type.errors) {
      return errorResult(call.id, args.summary);
    }

    // The warm agent owns exactly one mailbox; expunge sweeps its INBOX.
    let outcome;
    try {
      outcome = await transport.expunge("INBOX", signal);
    } catch (cause) {
      // A rejection means nothing was removed; the mailbox is unchanged.
      return errorResult(
        call.id,
        `expunge not applied: ${cause instanceof Error ? cause.message : String(cause)}`,
        "expunge_failed",
      );
    }

    return {
      callId: call.id,
      content: { ok: true, expungedUids: outcome.expungedUids },
    };
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
