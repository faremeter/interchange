// Child-side mail-compose bridge.
//
// Run code that posts mail to the hub REST API needs the message signed
// with the deployment key, and the child holds no key. The bridge asks the
// supervisor to compose and sign a conversation message with
// `mail.compose.request` and resolves with the signed message, base64-encoded,
// from the matching `mail.compose.response`. Nothing is sent: the caller
// posts the message, and the hub verifies it against the run principal's key.
//
// The supervisor composes one message per child at a time and refuses a
// second request while one is in flight, so the bridge queues its
// requests and keeps at most one outstanding.

import { getLogger } from "@intx/log";

import { base64Encode } from "@intx/types/base64";
import type { AttachmentInput } from "@intx/types/attachment-validation";
import type { AttachmentError } from "@intx/types/sessions";

import type {
  ComposeAttachmentPayload,
  ControlChannelSender,
  ControlPayload,
} from "../ipc/control-channel";

import { createPendingRequestCore } from "./pending-request";

const logger = getLogger(["workflow-host", "child", "mail-compose-bridge"]);

type MailComposeResponse = Extract<
  ControlPayload,
  { type: "mail.compose.response" }
>["data"];

/**
 * A conversation message to compose. The attachments take the shape the
 * hub's REST mail route accepts, so a caller can pass the same input to
 * either.
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
 * The supervisor refused to compose the message. `attachmentError` is set
 * when an attachment failed validation, with the same structured error the
 * hub's REST mail route reports for that input.
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
 * Bridge surface for run code. `compose` resolves with the signed message.
 * `handleResult` is what the control loop calls when
 * `mail.compose.response` arrives. `cancelAll` rejects every pending and
 * queued compose when the control loop exits.
 */
export interface ChildMailComposeBridge {
  compose(input: MailComposeInput): Promise<ComposedMail>;
  handleResult(data: MailComposeResponse): void;
  cancelAll(reason: string): void;
  readonly pendingCount: number;
}

export interface CreateChildMailComposeBridgeOpts {
  upstreamSender: ControlChannelSender;
  /**
   * Optional `requestId` allocator. Production wires a per-instance
   * monotonic counter plus a random suffix; tests inject a deterministic
   * factory so the upstream frame's `requestId` is predictable.
   */
  allocateRequestId?: () => string;
}

export function createChildMailComposeBridge(
  opts: CreateChildMailComposeBridgeOpts,
): ChildMailComposeBridge {
  const pending = createPendingRequestCore<ComposedMail, undefined>({
    label: "workflow-child mail compose",
    allocatorPrefix: "mcp",
    allocateRequestId: opts.allocateRequestId,
  });
  // The tail of the request queue. Each compose waits for the one before
  // it to settle, whatever its outcome, before it sends its own request.
  let tail: Promise<unknown> = Promise.resolve();
  let cancelledReason: string | undefined;

  async function submit(input: MailComposeInput): Promise<ComposedMail> {
    // A plain `Error`, as the pending core rejects a sent compose on cancel:
    // `MailComposeError` means the supervisor refused, and it never saw this.
    if (cancelledReason !== undefined) {
      throw new Error(
        `workflow-child mail compose cancelled: ${cancelledReason}`,
      );
    }
    const { requestId, promise } = pending.register(undefined);
    try {
      await opts.upstreamSender.send({
        type: "mail.compose.request",
        data: {
          requestId,
          message: {
            to: input.to,
            content: input.content,
            ...(input.attachments !== undefined
              ? { attachments: input.attachments.map(projectAttachment) }
              : {}),
          },
        },
      });
    } catch (cause) {
      pending.discard(requestId);
      throw pending.sendFailedError(requestId, cause);
    }
    return promise;
  }

  return {
    get pendingCount() {
      return pending.pendingCount;
    },
    compose(input: MailComposeInput): Promise<ComposedMail> {
      const result = tail.then(() => submit(input));
      tail = result.catch(() => undefined);
      return result;
    },
    handleResult(data) {
      const entry = pending.settle(data.requestId);
      if (entry === undefined) {
        logger.warn`mail.compose.response landed with no pending entry; requestId=${data.requestId} dropped`;
        return;
      }
      if (!data.result.ok) {
        entry.reject(
          new MailComposeError(data.result.reason, data.result.attachmentError),
        );
        return;
      }
      entry.resolve({
        messageId: data.result.messageId,
        rawMessage: data.result.rawMessageBase64,
      });
    },
    cancelAll(reason: string) {
      cancelledReason = reason;
      pending.cancelAll(reason);
    },
  };
}

function projectAttachment(input: AttachmentInput): ComposeAttachmentPayload {
  return {
    contentType: input.mimeType,
    dataBase64:
      typeof input.data === "string" ? input.data : base64Encode(input.data),
    ...(input.name !== undefined ? { name: input.name } : {}),
  };
}
