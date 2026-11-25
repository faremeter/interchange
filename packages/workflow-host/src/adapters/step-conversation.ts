// The connector conversation one step turn drives, bound to one durable
// conversation store (design §3c). The step invoker runs each turn through
// it: the turn start, the inbound mail seed, the approval park, the reply,
// and the mirror to the workflow-run substrate after the turn. The step
// invoker sends a reply only from a turn that answers mail, and only when
// the step sets `replyByMail`. It sends the reply from the turn that
// produced it: it reads the store to address the reply, sends it through
// the outbound bridge, and advances the connector thread from the receipt.

import type { ConnectorReplyParts } from "@intx/harness";
import type {
  InboundMessage,
  OutboundMessage,
  SendReceipt,
} from "@intx/types/runtime";

/**
 * The connector conversation of one step. A turn that answers mail, of a
 * step that sets `replyByMail`, sends its reply through it: the step
 * invoker composes, sends, and records the reply before the turn completes.
 */
export interface StepConversation {
  /**
   * Start a connector turn. `turn.resume` is the correlation id of the gate
   * an approval resume resumes: that turn takes the turn mode its parked
   * turn filed. Any other turn answers no mail until a seed sets its mode,
   * so a turn that seeds no mail replies to nobody.
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
  /**
   * The threading headers for the next reply. Throws
   * `TurnAnswersNoMailError` when the turn answers no mail.
   */
  composeReply(): ConnectorReplyParts | null;
  /** Advance the connector thread after the step sent a reply. */
  onReplySent(
    receipt: SendReceipt,
    references: readonly string[],
  ): Promise<void>;
  /** Send one composed reply through the outbound bridge. */
  send(message: OutboundMessage): Promise<SendReceipt>;
  /** Mirror the conversation to the workflow-run substrate. */
  settle(): Promise<void>;
}
