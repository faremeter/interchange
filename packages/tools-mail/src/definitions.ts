// Static definitions for the mail tools. The catalog generator and the
// inference director both consume these as inert data — no factory calls,
// no runtime side effects.
//
// (MESSAGE.md § Mail Tools)

import type { ToolDefinition } from "@intx/types/runtime";

export type MailToolName =
  | "mail_send"
  | "mail_reply"
  | "mail_search"
  | "mail_read"
  | "mail_wait"
  | "mail_flag"
  | "mail_expunge";

// The values are written out because this module holds inert data — it calls
// nothing, so it cannot introspect the InterchangeType union it mirrors.
const MAIL_TYPE_SCHEMA = {
  type: "string",
  description: "Mail type (default: conversation.message)",
  default: "conversation.message",
  enum: [
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
  ],
};

// `additionalProperties: false` is left off because this schema is forwarded
// verbatim to providers whose accepted JSON Schema subsets differ; handlers.ts
// is what enforces the rejection.
const SEARCH_QUERY_SCHEMA = {
  type: "object",
  description:
    "Search criteria. Only the filters listed below are accepted; any other key is rejected as invalid_query rather than ignored. An absent or empty query matches every message.",
  properties: {
    from: { type: "string", description: "Substring of the From address" },
    to: { type: "string", description: "Substring of the To address" },
    cc: { type: "string", description: "Substring of the Cc address" },
    bcc: { type: "string", description: "Substring of the Bcc address" },
    header: {
      type: "object",
      description: "Match a named header against a substring",
      properties: {
        field: {
          type: "string",
          description: "Header name, e.g. Interchange-Type",
        },
        contains: {
          type: "string",
          description: "Substring the header value must contain",
        },
      },
      required: ["field", "contains"],
    },
    before: {
      type: "string",
      description: "Delivery date filter, as a date string",
    },
    after: {
      type: "string",
      description: "Delivery date filter, as a date string",
    },
    on: {
      type: "string",
      description: "Delivery date filter, as a date string",
    },
    sentBefore: {
      type: "string",
      description: "Origination date filter, as a date string",
    },
    sentAfter: {
      type: "string",
      description: "Origination date filter, as a date string",
    },
    sentOn: {
      type: "string",
      description: "Origination date filter, as a date string",
    },
    hasFlags: {
      type: "array",
      items: { type: "string" },
      description: 'Flags or keywords the message carries (e.g. ["\\\\Seen"])',
    },
    missingFlags: {
      type: "array",
      items: { type: "string" },
      description: "Flags or keywords the message does not carry",
    },
    body: { type: "string", description: "Substring of the body text" },
    text: { type: "string", description: "Substring of the headers and body" },
    largerThan: { type: "number", description: "Message size in octets" },
    smallerThan: { type: "number", description: "Message size in octets" },
    and: {
      type: "array",
      items: { type: "object" },
      description: "Sub-queries of this same shape that must all match",
    },
    or: {
      type: "array",
      items: { type: "object" },
      description: "Sub-queries of this same shape of which one must match",
    },
    not: {
      type: "object",
      description: "A sub-query of this same shape that must not match",
    },
  },
};

const ATTACHMENTS_SCHEMA = {
  type: "array",
  description:
    "Files to attach to a conversation mail (not allowed with a structured 'type'). Each attachment's 'content' is plain text by default (e.g. for text/plain, text/csv, text/markdown, application/json) -- pass base64 in 'content' and set 'encoding' to 'base64' for anything else (images, video, audio, PDF). Never pre-encode text as base64.",
  items: {
    type: "object",
    properties: {
      name: { type: "string", description: "Attachment filename" },
      contentType: {
        type: "string",
        description: "MIME type (e.g. image/png, text/plain)",
      },
      content: {
        type: "string",
        description:
          "The attachment's content: plain text unless 'encoding' is 'base64'",
      },
      encoding: {
        type: "string",
        enum: ["utf-8", "base64"],
        description:
          "How 'content' is encoded. Defaults to 'utf-8' for text-like types and 'base64' for everything else, which must be base64",
      },
    },
    required: ["name", "contentType", "content"],
  },
};

export const TOOL_DEFINITIONS: ToolDefinition[] = [
  {
    name: "mail_send",
    description:
      "Send mail to another agent or address. Use this to initiate conversations or send mail to other agents. Provide EXACTLY ONE body, and make it the one the 'type' takes: 'content' for a conversation type, 'payload' for every other type. Providing both, providing neither, or providing the one the 'type' does not take is rejected.",
    inputSchema: {
      type: "object",
      properties: {
        to: {
          anyOf: [
            { type: "string" },
            { type: "array", items: { type: "string" } },
          ],
          // JSON Schema applies 'pattern' to a string instance only, so the
          // per-address rule of the array form is stated in the description.
          pattern: "^[^\\r\\n]*\\S[^\\r\\n]*$",
          description:
            "Recipient address (e.g. agent@local.interchange), or an array of addresses to send to several recipients. Each address becomes part of a header value, so a line break in one is rejected, and a blank address names nobody. An empty array names no destination and is rejected",
        },
        content: {
          type: "string",
          description:
            "Mail text content, for a conversation type. Rejected for any other type, which takes 'payload' instead",
        },
        payload: {
          type: "object",
          description:
            "Structured payload object, sent instead of 'content' for a non-conversation type. Rejected for a conversation type, which takes 'content' instead",
        },
        type: MAIL_TYPE_SCHEMA,
        subject: {
          type: "string",
          pattern: "^[^\\r\\n]*$",
          description:
            "Optional subject line. It becomes a header value, so a line break in it is rejected",
        },
        inReplyTo: {
          type: "string",
          pattern: "^[^\\r\\n]*\\S[^\\r\\n]*$",
          description:
            "Message-ID of the mail being replied to. It becomes a header value, so a line break in it is rejected, and a blank value names no message",
        },
        correlationId: {
          type: "string",
          pattern: "^[^\\r\\n]*$",
          description:
            "Correlation ID stamped on the outgoing mail as Interchange-Correlation-ID. A responder carries it back to tie its reply to this request; this tool does not await that reply -- call mail_wait to wait for it. It becomes a header value, so a line break in it is rejected",
        },
        attachments: ATTACHMENTS_SCHEMA,
      },
      // Exactly one of 'content' and 'payload' is mandatory, which this
      // dialect cannot state; the description says so and handlers.ts enforces
      // it.
      required: ["to"],
    },
  },
  {
    name: "mail_reply",
    description:
      "Reply to a mail by reference. Addresses the reply to the original sender and sets inReplyTo for threading. Provide EXACTLY ONE body, and make it the one the 'type' takes: 'content' for a conversation type, 'payload' for every other type. Providing both, providing neither, or providing the one the 'type' does not take is rejected.",
    inputSchema: {
      type: "object",
      properties: {
        ref: {
          type: "object",
          description: "Mail reference { uid, mailbox }",
          properties: {
            uid: { type: "number" },
            mailbox: { type: "string" },
          },
          required: ["uid", "mailbox"],
        },
        content: {
          type: "string",
          description:
            "Reply mail text content, for a conversation type. Rejected for any other type, which takes 'payload' instead",
        },
        payload: {
          type: "object",
          description:
            "Structured payload object, sent instead of 'content' for a non-conversation reply type. Rejected for a conversation type, which takes 'content' instead",
        },
        type: MAIL_TYPE_SCHEMA,
        attachments: ATTACHMENTS_SCHEMA,
      },
      required: ["ref"],
    },
  },
  {
    name: "mail_search",
    description:
      "Search mail in a mailbox. Returns mail summaries in 'results', the number of matches in 'matched', and whether 'limit' cut the list short in 'truncated'. A summary whose headers could not be read carries 'headersError' instead of its header fields. Put every filter inside 'query': an argument other than 'mailbox', 'query' and 'limit' is rejected rather than ignored.",
    inputSchema: {
      type: "object",
      properties: {
        mailbox: {
          type: "string",
          description: "Mailbox to search",
          default: "INBOX",
        },
        query: SEARCH_QUERY_SCHEMA,
        limit: {
          type: "integer",
          minimum: 1,
          description:
            "Maximum results to return: a positive whole number. Zero, a negative number and a fraction are rejected. The tool returns the first 'limit' matches, so there is no value that means 'all' and no negative index that means 'the last one'. The result reports the match count and whether it was cut short, so raise this to read the matches left behind",
          default: 20,
        },
      },
    },
  },
  {
    name: "mail_read",
    description:
      "Read a specific mail by reference. 'full' and 'payload' responses include an 'attachments' array (name, contentType, size, and a MIME 'part' path) when the mail carries any -- fetch an attachment with a follow-up mail_read using that part path, which returns 'content' as text for text-like types holding valid UTF-8 ('encoding' is 'utf-8') and as base64 otherwise ('encoding' is 'base64').",
    inputSchema: {
      type: "object",
      properties: {
        ref: {
          type: "object",
          description: "Mail reference { uid, mailbox }",
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
      "Wait for mail matching a query to arrive. Blocks until matching mail is delivered or the timeout expires. Use this instead of polling mail_search in a loop. Put every filter inside 'query': an argument other than 'query', 'timeout' and 'mailbox' is rejected rather than ignored.",
    inputSchema: {
      type: "object",
      properties: {
        query: SEARCH_QUERY_SCHEMA,
        // 1740 is the MAX_WAIT_SECONDS of handlers.ts, written out because this
        // module holds inert data and cannot read it.
        timeout: {
          type: "integer",
          minimum: 1,
          maximum: 1740,
          description:
            "Maximum seconds to wait before returning a timeout error: a whole number from 1 to 1740 (29 minutes, the longest IDLE span RFC 2177 contemplates). A value outside that range is rejected",
          default: 120,
        },
        mailbox: {
          type: "string",
          description: "Mailbox to watch",
          default: "INBOX",
        },
      },
    },
  },
  {
    name: "mail_flag",
    description:
      "Set or clear IMAP flags on a message (system flags like \\Seen or \\Deleted, or custom keywords). Provide EITHER 'set' to add flags OR 'clear' to remove them -- one direction per call. To consume a message, flag it \\Deleted then call mail_expunge. A 'flag_failed' result means the outcome is unknown: the flag may or may not have been applied. An 'invalid_mailbox' result means the mailbox named in 'ref' does not exist, so nothing was changed and it is 'ref' that has to change.",
    inputSchema: {
      type: "object",
      properties: {
        ref: {
          type: "object",
          description: "Mail reference { uid, mailbox }",
          properties: {
            uid: { type: "number" },
            mailbox: { type: "string" },
          },
          required: ["uid", "mailbox"],
        },
        set: {
          type: "array",
          items: { type: "string" },
          description: 'Flags to add (e.g. ["\\\\Deleted"])',
        },
        clear: {
          type: "array",
          items: { type: "string" },
          description: "Flags to remove",
        },
      },
      required: ["ref"],
    },
  },
  {
    name: "mail_expunge",
    description:
      "Permanently remove every message flagged \\Deleted from the INBOX and return the uids removed. Flag a message \\Deleted with mail_flag first. An 'expunge_failed' result means the outcome is unknown: messages may or may not have been removed. An 'invalid_mailbox' result means the INBOX does not exist, so nothing was removed.",
    inputSchema: {
      type: "object",
      properties: {},
      required: [],
    },
  },
];
