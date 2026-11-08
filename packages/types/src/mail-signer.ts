// The run code's handle on its deployment's mail key.
//
// A workflow run's private key stays in the sidecar. Run code that sends
// mail through the hub REST API asks the sidecar to compose and sign the
// message, then posts the signed bytes itself; the hub verifies them against
// the run principal's key. The signer only composes: it sends nothing.

import type { AttachmentInput } from "./attachment-validation";
import { AttachmentError } from "./sessions";

/**
 * A conversation message to compose. The attachments take the shape the
 * hub's REST mail route accepts and pass the same checks. The composed
 * message is also held to the size of a mail request, which carries it
 * base64-encoded, so it carries fewer attachment bytes than a message the hub
 * signs.
 */
export type MailComposeInput = {
  to: string;
  content: string;
  attachments?: readonly AttachmentInput[];
};

/**
 * A signed RFC 2822 message and the Message-ID stamped into it. The message
 * stays base64-encoded: the hub's REST mail route takes it in that form, and
 * a string survives the JSON of an action's recorded output.
 */
export type ComposedMail = {
  messageId: string;
  rawMessage: string;
};

/**
 * Composes conversation messages signed as the deployment's mail address.
 * The From address is always the deployment's own; a caller cannot choose
 * it.
 */
export interface MailSigner {
  compose(message: MailComposeInput): Promise<ComposedMail>;
}

/**
 * The host refused to compose the message. `attachmentError` is set when an
 * attachment failed validation, with the same structured error the hub's
 * REST mail route reports for that input.
 */
export class MailComposeError extends Error {
  readonly attachmentError: AttachmentError | undefined;

  constructor(message: string, attachmentError?: AttachmentError) {
    super(message);
    this.name = "MailComposeError";
    this.attachmentError = attachmentError;
  }
}

/**
 * `instanceof` is not usable for this: a workflow bundle inlines its workspace
 * imports, so a consumer loaded from one holds its own copy of the class above
 * and answers false against an error the host's copy constructed.
 */
export function isMailComposeError(value: unknown): value is Error & {
  readonly attachmentError: AttachmentError | undefined;
} {
  if (!(value instanceof Error)) return false;
  if (value.name !== "MailComposeError") return false;
  if (!("attachmentError" in value)) return false;
  return (
    value.attachmentError === undefined ||
    AttachmentError.allows(value.attachmentError)
  );
}
