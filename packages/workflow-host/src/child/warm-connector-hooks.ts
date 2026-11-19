// The warm step invoker's connector hooks, bound to the durable
// conversation registry (design §3c). A warm single-step deployment drives
// its agent's connector thread through these hooks; a multi-step deployment
// has no durable registry and wires none of them.
//
// Every hook takes the step identity as its key, the same key the env
// builder filed the durable store under, so each hook resolves the right
// per-agent store.

import type { AgentEventStream, ConnectorReplyDrain } from "@intx/harness";
import type { OutboundMessage, SendReceipt } from "@intx/types/runtime";

import {
  driveStepConversationReplies,
  type StepConversation,
} from "../adapters/step-conversation";
import type { DurableConversationRegistry } from "../conversation-state";

export type WarmConnectorHooks = {
  warmConversation: (key: string) => StepConversation;
  driveReplies: (key: string, stream: AgentEventStream) => ConnectorReplyDrain;
};

/**
 * Bind the warm step invoker's connector hooks to `registry`. `sendReply`
 * sends one composed connector reply and returns its receipt.
 */
export function createWarmConnectorHooks(
  registry: DurableConversationRegistry,
  sendReply: (message: OutboundMessage) => Promise<SendReceipt>,
): WarmConnectorHooks {
  // Each member looks the store up when it runs, so binding a conversation
  // does not require the env builder to have acquired the store yet.
  const warmConversation = (key: string): StepConversation => ({
    beginTurn: async (turn) => registry.get(key).beginTurn(turn),
    seedInbound: async (message) => registry.get(key).seedInbound(message),
    parkTurn: async (correlationId) =>
      registry.get(key).parkTurn(correlationId),
    composeReply: () => registry.get(key).composeReply(),
    onReplySent: async (receipt, references) =>
      registry.get(key).onReplySent(receipt, references),
    send: sendReply,
    // Run-boundary durability flush: mirror the warm agent's conversation
    // snapshot to the workflow-run substrate after each message's send
    // settles.
    settle: async () => registry.get(key).mirrorToSubstrate(),
  });
  return {
    warmConversation,
    // Connector reply drain. Established once per warm agent over its
    // lifetime stream; the returned drain handle carries the lifetime `done`
    // promise the step invoker folds into the warm entry so eviction drains
    // it, plus the per-turn settle barrier the warm step gates each reply
    // turn on.
    driveReplies: (key, stream) =>
      driveStepConversationReplies(warmConversation(key), stream),
  };
}
