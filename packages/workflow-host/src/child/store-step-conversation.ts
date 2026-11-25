// The connector conversation of one step, bound to the durable conversation
// store its env is built over (design §3c).

import type { OutboundMessage, SendReceipt } from "@intx/types/runtime";

import type { StepConversation } from "../adapters/step-conversation";
import type { DurableConversationStore } from "../conversation-state";

/**
 * Bind a step conversation to `store`. `send` sends one composed connector
 * reply and returns its receipt; `settle` mirrors the store to the
 * workflow-run substrate.
 */
export function createStoreStepConversation(
  store: DurableConversationStore,
  send: (message: OutboundMessage) => Promise<SendReceipt>,
): StepConversation {
  return {
    beginTurn: async (turn) => store.beginTurn(turn),
    seedInbound: async (message) => store.seedInbound(message),
    parkTurn: async (correlationId) => store.parkTurn(correlationId),
    turnAnswersMail: () => store.turnAnswersMail(),
    composeReply: () => store.composeReply(),
    onReplySent: async (receipt, references) =>
      store.onReplySent(receipt, references),
    send,
    settle: async () => store.mirrorToSubstrate(),
  };
}
