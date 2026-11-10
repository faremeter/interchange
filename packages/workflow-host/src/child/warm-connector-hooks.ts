// The warm step invoker's connector hooks, bound to the durable
// conversation registry (design §3c). A warm single-step deployment drives
// its agent's connector thread through these hooks; a multi-step deployment
// has no durable registry and wires none of them.
//
// Every hook takes the step identity as its key, the same key the env
// builder filed the durable store under, so each hook resolves the right
// per-agent store.

import {
  driveConnectorReplies,
  type AgentEventStream,
  type ConnectorReplyDrain,
} from "@intx/harness";
import type {
  InboundMessage,
  OutboundMessage,
  SendReceipt,
} from "@intx/types/runtime";

import type { DurableConversationRegistry } from "../conversation-state";

export type WarmConnectorHooks = {
  onRunBoundary: (key: string) => Promise<void>;
  seedInbound: (key: string, message: InboundMessage) => Promise<void>;
  beginTurn: (key: string, turn: { resume?: string }) => Promise<void>;
  parkTurn: (key: string, correlationId: string) => Promise<void>;
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
  return {
    // Run-boundary durability flush: mirror the warm agent's conversation
    // snapshot to the workflow-run substrate after each message's send
    // settles.
    onRunBoundary: async (key) => {
      await registry.get(key).mirrorToSubstrate();
    },
    // Connector-thread seed: route each mail-derived inbound message onto
    // the warm agent's connector thread before its send, so the reply path
    // has thread state.
    seedInbound: async (key, message) => {
      await registry.get(key).seedInbound(message);
    },
    // Connector turn start. An approval resume takes the reply target its
    // parked turn filed; any other warm turn clears the reply target a
    // passthrough seed set, so a warm turn that seeds no mail replies on the
    // connector thread.
    beginTurn: async (key, turn) => {
      await registry.get(key).beginTurn(turn);
    },
    // Connector turn park. Files the reply target of a warm turn that parked
    // on an approval gate under the gate's correlation id, so the
    // run-boundary mirror persists it for the approval resume.
    parkTurn: async (key, correlationId) => {
      await registry.get(key).parkTurn(correlationId);
    },
    // Connector reply drain: on each `connector.reply` the agent emits,
    // compose the reply from the durable store, send it, then advance the
    // thread from the receipt. Established once per warm agent over its
    // lifetime stream; the returned drain handle carries the lifetime `done`
    // promise the step invoker folds into the warm entry so eviction drains
    // it, plus the per-turn settle barrier the warm step gates each reply
    // turn on.
    driveReplies: (key, stream) =>
      driveConnectorReplies({
        stream,
        composeReply: () => registry.get(key).composeReply(),
        send: sendReply,
        onReplySent: (receipt, references) =>
          registry.get(key).onReplySent(receipt, references),
      }),
  };
}
