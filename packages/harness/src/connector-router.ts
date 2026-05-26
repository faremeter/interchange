// Connector-thread routing for the agent harness.
//
// Two-phase decision: route() is pure and returns a discriminated kind
// plus an opaque carrier of the next state; commit() advances router
// state from that carrier. Separating the decision from the mutation
// lets the harness sequence the side effects (deliver, INBOX
// expunge) around the state change however it needs to.

import type {
  ConnectorThreadState,
  InboundMessage,
  SendReceipt,
} from "@intx/types/runtime";

export type RouteDecision =
  | { kind: "start" }
  | { kind: "continue" }
  | { kind: "passthrough" };

export type ConnectorReplyParts = {
  to: string;
  inReplyTo: string;
  subject?: string;
};

export class NoActiveConnectorThreadError extends Error {
  constructor() {
    super("no active connector thread");
    this.name = "NoActiveConnectorThreadError";
  }
}

export interface ConnectorRouter {
  /**
   * Classify an inbound message against the current connector state. Pure:
   * does not mutate router state. The returned decision must be passed to
   * `commit()` to take effect.
   */
  route(message: InboundMessage): RouteDecision;

  /**
   * Advance router state per a decision produced by `route()`. No-op for
   * `passthrough`. For `start` and `continue`, throws if the decision was
   * not produced by this router instance.
   */
  commit(decision: RouteDecision): void;

  /**
   * Produce the threading headers needed to send a reply on the active
   * connector thread. The caller composes the full outbound message by
   * adding its own `content` and `type` fields. Throws
   * `NoActiveConnectorThreadError` when no thread is active.
   */
  composeReply(): ConnectorReplyParts;

  /**
   * Update `lastMessageId` after a successful outbound reply send.
   * Throws when called with no active thread — outbound state advance
   * has no meaning without a thread.
   */
  onReplySent(receipt: SendReceipt): void;

  /**
   * Return the current connector state as a serializable snapshot, or
   * `null` when no thread is active. Matches the
   * `ConnectorThreadState | null` shape used by the storage layer.
   */
  snapshot(): ConnectorThreadState | null;

  /**
   * Install a snapshot as the router's current state. Used at startup
   * to restore from the persisted context store, and in tests to set
   * up scenarios. Passing `null` clears the active thread.
   */
  restore(state: ConnectorThreadState | null): void;
}

export function createConnectorRouter(): ConnectorRouter {
  let state: ConnectorThreadState | null = null;

  // Pending state per decision is held off the decision object via a
  // WeakMap so callers see only `{ kind }` — no path to inspect or
  // mutate the next state, even via type assertions.
  const pendingStates = new WeakMap<RouteDecision, ConnectorThreadState>();

  function isContinuation(message: InboundMessage): boolean {
    if (state === null) return false;

    const { inReplyTo, references } = message.headers;

    if (references !== undefined && references.includes(state.threadRoot)) {
      return true;
    }

    if (inReplyTo !== undefined && inReplyTo === state.lastMessageId) {
      return true;
    }

    return false;
  }

  function route(message: InboundMessage): RouteDecision {
    if (state === null) {
      const nextState: ConnectorThreadState = {
        threadRoot: message.headers.messageId,
        lastMessageId: message.headers.messageId,
        replyTo: message.headers.from,
        ...(message.headers.subject !== undefined
          ? { subject: message.headers.subject }
          : {}),
      };
      const decision: RouteDecision = { kind: "start" };
      pendingStates.set(decision, nextState);
      return decision;
    }

    if (isContinuation(message)) {
      const nextState: ConnectorThreadState = {
        threadRoot: state.threadRoot,
        lastMessageId: message.headers.messageId,
        replyTo: message.headers.from,
        ...(state.subject !== undefined ? { subject: state.subject } : {}),
      };
      const decision: RouteDecision = { kind: "continue" };
      pendingStates.set(decision, nextState);
      return decision;
    }

    return { kind: "passthrough" };
  }

  function commit(decision: RouteDecision): void {
    if (decision.kind === "passthrough") return;

    const nextState = pendingStates.get(decision);
    if (nextState === undefined) {
      throw new Error(
        "commit() called with a decision from a different router instance",
      );
    }

    state = nextState;
    pendingStates.delete(decision);
  }

  function composeReply(): ConnectorReplyParts {
    if (state === null) {
      throw new NoActiveConnectorThreadError();
    }

    return {
      to: state.replyTo,
      inReplyTo: state.lastMessageId,
      ...(state.subject !== undefined ? { subject: state.subject } : {}),
    };
  }

  function onReplySent(receipt: SendReceipt): void {
    if (state === null) {
      throw new NoActiveConnectorThreadError();
    }
    state = {
      threadRoot: state.threadRoot,
      lastMessageId: receipt.messageId,
      replyTo: state.replyTo,
      ...(state.subject !== undefined ? { subject: state.subject } : {}),
    };
  }

  function snapshot(): ConnectorThreadState | null {
    if (state === null) return null;
    return {
      threadRoot: state.threadRoot,
      lastMessageId: state.lastMessageId,
      replyTo: state.replyTo,
      ...(state.subject !== undefined ? { subject: state.subject } : {}),
    };
  }

  function restore(next: ConnectorThreadState | null): void {
    if (next === null) {
      state = null;
      return;
    }
    state = {
      threadRoot: next.threadRoot,
      lastMessageId: next.lastMessageId,
      replyTo: next.replyTo,
      ...(next.subject !== undefined ? { subject: next.subject } : {}),
    };
  }

  return {
    route,
    commit,
    composeReply,
    onReplySent,
    snapshot,
    restore,
  };
}
