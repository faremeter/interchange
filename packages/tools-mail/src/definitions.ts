// Static definitions for the five mail tools. The catalog generator and the
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
  | "mail_wait";

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
    description: "Read a specific mail by reference.",
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
];
