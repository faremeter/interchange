import { type } from "arktype";

export const sessionEndedEvent = type({ type: "'session.ended'" });

export const MailDeliveredEvent = type({
  type: "'mail.delivered'",
  data: {
    id: "string",
    direction: "'inbound' | 'outbound'",
    "from?": type({
      name: "string | null",
      email: "string",
    }).array(),
    "to?": type({
      name: "string | null",
      email: "string",
    }).array(),
    "subject?": "string | null",
    "sentAt?": "string | null",
    bodyValues: "Record<string, unknown>",
    textBody: type({
      partId: "string",
      type: "string",
    }).array(),
    "htmlBody?": type({
      partId: "string",
      type: "string",
    }).array(),
    "attachments?": type({
      blobId: "string",
      "name?": "string | null",
      type: "string",
      size: "number",
    }).array(),
    headers: "Record<string, string>",
    receivedAt: "string",
  },
});
export type MailDeliveredEvent = typeof MailDeliveredEvent.infer;

export const TurnCommittedEvent = type({
  type: "'turn.committed'",
  data: {
    turnId: "string",
    status: "'completed' | 'failed'",
    text: "string",
    hadError: "boolean",
    errors: type({
      category: "string",
      message: "string",
    }).array(),
    toolErrors: type({
      name: "string",
      content: "string",
    }).array(),
  },
});
export type TurnCommittedEvent = typeof TurnCommittedEvent.infer;
