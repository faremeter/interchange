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
      "Send mail to another agent or address. Use this to initiate conversations or send mail to other agents.",
    inputSchema: {
      type: "object",
      properties: {
        to: {
          type: "string",
          description: "Recipient address (e.g. agent@local.interchange)",
        },
        content: {
          type: "string",
          description: "Mail text content",
        },
        type: {
          type: "string",
          description: "Mail type (default: conversation.message)",
          default: "conversation.message",
        },
        subject: {
          type: "string",
          description: "Optional subject line",
        },
        inReplyTo: {
          type: "string",
          description: "Message-ID of the mail being replied to",
        },
        attachments: ATTACHMENTS_SCHEMA,
      },
      required: ["to", "content"],
    },
  },
  {
    name: "mail_reply",
    description:
      "Reply to a mail by reference. Addresses the reply to the original sender and sets inReplyTo for threading.",
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
          description: "Reply mail text content",
        },
        type: {
          type: "string",
          description: "Mail type (default: conversation.message)",
          default: "conversation.message",
        },
        attachments: ATTACHMENTS_SCHEMA,
      },
      required: ["ref", "content"],
    },
  },
  {
    name: "mail_search",
    description: "Search mail in a mailbox. Returns mail summaries.",
    inputSchema: {
      type: "object",
      properties: {
        mailbox: {
          type: "string",
          description: "Mailbox to search",
          default: "INBOX",
        },
        query: {
          type: "object",
          description: "Search query (e.g. { from: 'agent@...' })",
        },
        limit: {
          type: "number",
          description: "Maximum results to return",
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
      "Wait for mail matching a query to arrive. Blocks until matching mail is delivered or the timeout expires. Use this instead of polling mail_search in a loop.",
    inputSchema: {
      type: "object",
      properties: {
        query: {
          type: "object",
          description:
            "Search criteria for the mail to wait for (e.g. { from: 'agent@...' })",
        },
        timeout: {
          type: "number",
          description:
            "Maximum seconds to wait before returning a timeout error",
          default: 120,
        },
        mailbox: {
          type: "string",
          description: "Mailbox to watch",
          default: "INBOX",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "mail_flag",
    description:
      "Set or clear IMAP flags on a message (system flags like \\Seen or \\Deleted, or custom keywords). Provide EITHER 'set' to add flags OR 'clear' to remove them -- one direction per call. To consume a message, flag it \\Deleted then call mail_expunge. An error result means the mailbox was NOT changed.",
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
      "Permanently remove every message flagged \\Deleted from the INBOX and return the uids removed. Flag a message \\Deleted with mail_flag first. An error result means nothing was removed.",
    inputSchema: {
      type: "object",
      properties: {},
      required: [],
    },
  },
];
