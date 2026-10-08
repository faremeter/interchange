// Aggregate control-channel API and combined payload schema for inspection.
// Process-specific callers import control-sender or control-receiver directly;
// importing this module constructs the full payload union.

import { createControlPayloadSchema } from "./control-payloads";

export const ControlPayload = createControlPayloadSchema();
export type ControlPayload = typeof ControlPayload.infer;

export {
  SourcesUpdatedData,
  OutboundAttachmentPayload,
  OutboundMessagePayload,
  MailboxNotifyHeaders,
} from "./control-payloads";
export {
  createControlChannelSender,
  type ControlChannelSender,
  type ControlChannelSenderOpts,
  type NdjsonReader,
  type NdjsonWriter,
} from "./control-sender";
export {
  receiveControlChannel,
  type ControlChannelReceiverOpts,
} from "./control-receiver";
