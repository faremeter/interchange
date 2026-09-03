// Child-side outbound-mail bridge (OUTBOUND half of mailbox ownership,
// §3a).
//
// The workflow-process child holds no signing key for the agent's
// identity. The supervisor is the sole mail owner: it holds the host
// transport against which the agent's address is registered with its
// `CryptoProvider`, and it is the only process that can emit signed mail
// on the agent's behalf. So a step agent never calls `transport.send`
// directly; its mail tools are backed by a transport whose outbound side
// routes through this bridge, which forwards the structured
// `OutboundMessage` plus the sender (agent) address up over the control
// IPC. The supervisor performs the actual signed send and replies with
// the `SendReceipt`.
//
// Lifecycle of one outbound send:
//
//   1. The agent's mail tool (or the step reply path) calls the
//      supervisor-backed transport's `send`. The transport calls
//      `bridge.submit(senderAddress, message)`.
//   2. `submit` mints a `requestId`, registers a pending awaiter, and
//      emits `outbound.message` upstream carrying the JSON-projected
//      message (attachment bytes base64-encoded).
//   3. The supervisor receives the request, performs the signed send
//      through the host transport (`MailBusBindings.sendOutbound`), and
//      replies with `outbound.result` carrying the `SendReceipt` (or a
//      structured failure).
//   4. The bridge resolves / rejects the pending awaiter; the
//      transport's `send` returns the receipt to the mail tool. A
//      supervisor-side failure (unregistered sender, signing failure,
//      transport rejection) surfaces as a rejection so the agent's
//      mail-tool call fails loudly rather than silently dropping the
//      send.

import { getLogger } from "@intx/log";

import { base64Encode } from "@intx/types";
import type { OutboundMessage, SendReceipt } from "@intx/types/runtime";

import type {
  ControlChannelSender,
  ControlPayload,
  OutboundMessagePayload,
} from "../ipc/control-channel";

import { createPendingRequestCore } from "./pending-request";

const logger = getLogger(["workflow-host", "child", "outbound-mail-bridge"]);

/**
 * Bridge surface the child's supervisor-backed transport reaches into.
 * `submit` sends an `outbound.message` upstream and resolves once the
 * supervisor's matching `outbound.result` lands. `handleResult` is the
 * receiver-side entry point the child's control loop invokes when the
 * downstream `outbound.result` frame arrives. `cancelAll` is the
 * cleanup hook the control loop invokes on any exit path so a pending
 * send does not leak an awaiter when the supervisor has torn the IPC
 * down.
 */
export interface ChildOutboundMailBridge {
  submit(senderAddress: string, message: OutboundMessage): Promise<SendReceipt>;
  handleResult(
    data: Extract<ControlPayload, { type: "outbound.result" }>["data"],
  ): void;
  cancelAll(reason: string): void;
  readonly pendingCount: number;
}

export interface CreateChildOutboundMailBridgeOpts {
  upstreamSender: ControlChannelSender;
  /**
   * Optional `requestId` allocator. Production wires a per-instance
   * monotonic counter plus a random suffix; tests inject a
   * deterministic factory so the upstream frame's `requestId` is
   * predictable.
   */
  allocateRequestId?: () => string;
}

/**
 * Construct the child-side outbound-mail bridge. Pending sends live in
 * the shared pending-request core keyed by `requestId`; the bridge
 * resolves the awaiter when the supervisor's matching `outbound.result`
 * lands.
 */
export function createChildOutboundMailBridge(
  opts: CreateChildOutboundMailBridgeOpts,
): ChildOutboundMailBridge {
  const pending = createPendingRequestCore<SendReceipt, undefined>({
    label: "workflow-child outbound mail",
    allocatorPrefix: "om",
    allocateRequestId: opts.allocateRequestId,
  });

  return {
    get pendingCount() {
      return pending.pendingCount;
    },
    async submit(
      senderAddress: string,
      message: OutboundMessage,
    ): Promise<SendReceipt> {
      const { requestId, promise } = pending.register(undefined);
      try {
        await opts.upstreamSender.send({
          type: "outbound.message",
          data: {
            requestId,
            senderAddress,
            message: projectOutboundMessage(message),
          },
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
        logger.warn`outbound.result landed with no pending entry; requestId=${data.requestId} dropped`;
        return;
      }
      if (data.result.ok) {
        entry.resolve({
          messageId: data.result.messageId,
          status: data.result.status,
        });
        return;
      }
      entry.reject(pending.rejectedError(data.requestId, data.result.reason));
    },
    cancelAll(reason: string) {
      pending.cancelAll(reason);
    },
  };
}

/**
 * Project a runtime `OutboundMessage` into the IPC wire shape. Optional
 * fields are omitted when absent (the wire validator spells them
 * optional), and attachment bytes ride base64-encoded so the NDJSON
 * control channel stays text-safe.
 */
function projectOutboundMessage(
  message: OutboundMessage,
): OutboundMessagePayload {
  const payload: OutboundMessagePayload = {
    to: message.to,
    type: message.type,
  };
  if (message.cc !== undefined) payload.cc = message.cc;
  if (message.subject !== undefined) payload.subject = message.subject;
  if (message.content !== undefined) payload.content = message.content;
  if (message.payload !== undefined) payload.payload = message.payload;
  if (message.summary !== undefined) payload.summary = message.summary;
  if (message.inReplyTo !== undefined) payload.inReplyTo = message.inReplyTo;
  if (message.references !== undefined) payload.references = message.references;
  if (message.correlationId !== undefined) {
    payload.correlationId = message.correlationId;
  }
  if (message.sessionId !== undefined) payload.sessionId = message.sessionId;
  if (message.tenantId !== undefined) payload.tenantId = message.tenantId;
  if (message.attachments !== undefined) {
    payload.attachments = message.attachments.map((a) => ({
      name: a.name,
      contentType: a.contentType,
      dataBase64: base64Encode(a.data),
    }));
  }
  return payload;
}
