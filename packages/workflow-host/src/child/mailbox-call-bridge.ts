// Child-side mailbox-call bridge.
//
// The supervisor owns the deployment mailbox. Every read, listing, append,
// move, copy, watch acceptance, and distribution-list call goes up through
// this bridge and comes back as the supervisor's answer, including a
// refusal; the child does not open the mailbox to produce that answer.
//
// Lifecycle of one call: `submit` mints a `requestId`, registers a pending
// awaiter, and emits `mailbox.call.request` upstream; the supervisor
// answers with `mailbox.call.response` echoing `op`; the bridge resolves
// the success or rejects. A response carrying `condition` rejects with
// that `MessageTransportError`; a failure with no condition stays a plain
// error (the supervisor had no IMAP condition to name).

import { getLogger } from "@intx/log";

import { MessageTransportError } from "@intx/types/runtime";

import type {
  ControlChannelSender,
  ControlPayload,
} from "../ipc/control-channel";

import { createPendingRequestCore } from "./pending-request";

const logger = getLogger(["workflow-host", "child", "mailbox-call-bridge"]);

type MailboxCallRequest = Extract<
  ControlPayload,
  { type: "mailbox.call.request" }
>["data"];

type MailboxCallResponse = Extract<
  ControlPayload,
  { type: "mailbox.call.response" }
>["data"];

export type MailboxCallSuccess = Extract<MailboxCallResponse, { ok: true }>;

/**
 * One mailbox call without the `requestId` the bridge mints. The operands
 * are the request frame's, so a field the wire does not carry cannot be
 * submitted.
 */
export type MailboxCall = MailboxCallRequest extends infer Request
  ? Request extends { requestId: string }
    ? Omit<Request, "requestId">
    : never
  : never;

/**
 * Bridge surface the child's supervisor-backed transport reaches into.
 * `submit` sends a `mailbox.call.request` and resolves with the supervisor's
 * success. `handleResult` is what the control loop calls when
 * `mailbox.call.response` arrives. `cancelAll` rejects every still-pending
 * call when the control loop exits.
 */
export interface ChildMailboxCallBridge {
  submit(call: MailboxCall): Promise<MailboxCallSuccess>;
  handleResult(data: MailboxCallResponse): void;
  cancelAll(reason: string): void;
  readonly pendingCount: number;
}

export interface CreateChildMailboxCallBridgeOpts {
  upstreamSender: ControlChannelSender;
  /**
   * Optional `requestId` allocator. Production wires a per-instance
   * monotonic counter plus a random suffix; tests inject a deterministic
   * factory so the upstream frame's `requestId` is predictable.
   */
  allocateRequestId?: () => string;
}

/**
 * Construct the child-side mailbox-call bridge. Pending calls live in the
 * shared pending-request core keyed by `requestId`.
 */
export function createChildMailboxCallBridge(
  opts: CreateChildMailboxCallBridgeOpts,
): ChildMailboxCallBridge {
  const pending = createPendingRequestCore<MailboxCallSuccess, undefined>({
    label: "workflow-child mailbox call",
    allocatorPrefix: "mc",
    allocateRequestId: opts.allocateRequestId,
  });

  return {
    get pendingCount() {
      return pending.pendingCount;
    },
    async submit(call: MailboxCall): Promise<MailboxCallSuccess> {
      const { requestId, promise } = pending.register(undefined);
      try {
        await opts.upstreamSender.send({
          type: "mailbox.call.request",
          data: { ...call, requestId },
        });
      } catch (cause) {
        pending.discard(requestId);
        throw pending.sendFailedError(requestId, cause);
      }
      return promise;
    },
    handleResult(data) {
      const entry = pending.settle(data.requestId);
      if (entry === undefined) {
        logger.warn`mailbox.call.response landed with no pending entry; requestId=${data.requestId} dropped`;
        return;
      }
      if (!data.ok) {
        if (data.condition !== undefined) {
          entry.reject(new MessageTransportError(data.condition, data.reason));
          return;
        }
        entry.reject(pending.rejectedError(data.requestId, data.reason));
        return;
      }
      entry.resolve(data);
    },
    cancelAll(reason: string) {
      pending.cancelAll(reason);
    },
  };
}
