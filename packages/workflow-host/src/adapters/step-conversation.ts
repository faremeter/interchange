// The connector conversation one step turn drives, bound to one durable
// conversation store (design §3c). The step invoker runs each turn through
// it: the turn start, the inbound mail seed, the approval park, and the
// mirror to the workflow-run substrate after the turn. The reply drain reads
// the store to address each reply, sends it through the outbound bridge, and
// advances the connector thread from the receipt.

import {
  driveConnectorReplies,
  type AgentEventStream,
  type ConnectorReplyDrain,
  type ConnectorReplyParts,
} from "@intx/harness";
import type {
  InboundMessage,
  OutboundMessage,
  SendReceipt,
} from "@intx/types/runtime";

export interface StepConversation {
  /**
   * Start a connector turn. `turn.resume` is the correlation id of the gate
   * an approval resume resumes: that turn takes the turn mode its parked
   * turn filed. Any other turn answers no mail until a seed sets its mode,
   * so a turn that seeds no mail does not reply to an earlier passthrough
   * sender.
   */
  beginTurn(turn: { resume?: string }): Promise<void>;
  /**
   * Route a mail-derived inbound message onto the connector thread before
   * the send, so the reply path has thread state and the turn answers that
   * mail.
   */
  seedInbound(message: InboundMessage): Promise<void>;
  /**
   * File the turn mode of a mail turn that parked on an approval gate under
   * the gate's correlation id, so the mirror persists it for the approval
   * resume.
   */
  parkTurn(correlationId: string): Promise<void>;
  /**
   * Whether the current turn answers mail: its input was mail, or it
   * resumes a parked turn whose input was mail.
   */
  turnAnswersMail(): boolean;
  /** The reply drain's threading headers for the next reply. */
  composeReply(): ConnectorReplyParts | null;
  /** Advance the connector thread after the reply drain sent a reply. */
  onReplySent(
    receipt: SendReceipt,
    references: readonly string[],
  ): Promise<void>;
  /** Send one composed reply through the outbound bridge. */
  send(message: OutboundMessage): Promise<SendReceipt>;
  /** Mirror the conversation to the workflow-run substrate. */
  settle(): Promise<void>;
}

/**
 * Drive the `connector.reply` events on `stream` out through `conversation`:
 * compose each reply from the conversation, send it, then advance the
 * connector thread from the receipt.
 */
export function driveStepConversationReplies(
  conversation: StepConversation,
  stream: AgentEventStream,
): ConnectorReplyDrain {
  return driveConnectorReplies({
    stream,
    composeReply: () => conversation.composeReply(),
    send: (message) => conversation.send(message),
    onReplySent: (receipt, references) =>
      conversation.onReplySent(receipt, references),
  });
}
