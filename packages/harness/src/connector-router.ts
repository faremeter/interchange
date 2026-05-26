// Connector-thread routing for the agent harness.
//
// The connector is one durable thread per agent. Participants accumulate
// as they speak; no one is displaced. `replyTo` tracks the most recent
// speaker (the primary recipient on the next outbound reply) and `cc`
// tracks every other participant who has spoken (carried on outbound so
// everyone stays in the loop).
//
// Two-phase decision: route() is pure and returns a discriminated kind
// plus an opaque carrier of the next state; commit() advances router
// state from that carrier. Separating the decision from the mutation
// lets the harness sequence the side effects (deliver, INBOX expunge)
// around the state change however it needs to.

import { getLogger } from "@intx/log";
import { extractAddrSpec } from "@intx/mime";
import type {
  ConnectorThreadState,
  InboundMessage,
  SendReceipt,
} from "@intx/types/runtime";

const logger = getLogger(["interchange", "harness", "connector-router"]);

export type RouteDecision =
  | { kind: "start" }
  | { kind: "continue" }
  | { kind: "passthrough" };

export type ConnectorReplyParts = {
  to: string;
  cc: string[];
  inReplyTo: string;
  subject?: string;
};

export class NoActiveConnectorThreadError extends Error {
  constructor() {
    super("no active connector thread");
    this.name = "NoActiveConnectorThreadError";
  }
}

export type ConnectorRouterOptions = {
  /**
   * Called synchronously after the router's internal state mutates and the
   * new state is committed to internal storage. Fires only when the new
   * state differs from the prior state — restore() into the same state,
   * passthrough commits, and other no-ops do not fire. Single subscriber:
   * the harness wiring that lifts state changes onto the hub-bound event
   * channel.
   *
   * The router catches and logs any error this callback throws. The cache
   * the callback feeds is a best-effort projection of router state, and
   * the authoritative state remains in the router and the persisted
   * context store. Dropping one notification means the projection stays
   * stale until the next state change rebuilds it; that is the right
   * trade-off versus aborting the call chain that invoked the
   * commit/onReplySent that produced the notification.
   */
  onStateChanged?(state: ConnectorThreadState | null): void;
};

export interface ConnectorRouter {
  /**
   * Classify an inbound message against the current connector state. Pure:
   * does not mutate router state. The returned decision must be passed to
   * `commit()` to take effect.
   *
   * Throws when `message.headers.from` is not a parseable bare addr-spec
   * (per `extractAddrSpec` from `@intx/mime`). The production fetch path
   * copies the wire `From:` header verbatim, so a malformed sender is a
   * normal-shape runtime concern, not a programmer error. Callers should
   * treat the throw as passthrough — deliver the message to the reactor
   * but do not advance router state or consume the message from the
   * INBOX.
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
   * connector thread. `to` is the most recent speaker; `cc` is everyone
   * else who has spoken on the thread (deduplicated). The caller composes
   * the full outbound message by adding its own `content` and `type`
   * fields. Throws `NoActiveConnectorThreadError` when no thread is
   * active.
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

function statesEqual(
  a: ConnectorThreadState | null,
  b: ConnectorThreadState | null,
): boolean {
  if (a === null || b === null) return a === b;
  return (
    a.threadRoot === b.threadRoot &&
    a.lastMessageId === b.lastMessageId &&
    a.replyTo === b.replyTo &&
    a.subject === b.subject &&
    a.cc.length === b.cc.length &&
    a.cc.every((v, i) => v === b.cc[i])
  );
}

export function createConnectorRouter(
  options?: ConnectorRouterOptions,
): ConnectorRouter {
  let state: ConnectorThreadState | null = null;
  const onStateChanged = options?.onStateChanged;

  // Pending state per decision is held off the decision object via a
  // WeakMap so callers see only `{ kind }` — no path to inspect or
  // mutate the next state, even via type assertions.
  const pendingStates = new WeakMap<RouteDecision, ConnectorThreadState>();

  function applyState(next: ConnectorThreadState | null): void {
    // The null → X transition is what drives bootstrap on restore() — a
    // future refactor that collapses null into a sentinel "no mutation"
    // case would silently break the hub-side cache's only fill path
    // outside live state mutations. Keep the equality check as-is; the
    // null state is a value, not a non-event.
    if (statesEqual(state, next)) return;
    state = next;
    if (onStateChanged !== undefined) {
      // The callback feeds a best-effort projection of router state. A
      // throwing subscriber would otherwise propagate out of commit() or
      // onReplySent() and abort the caller; catching here drops one
      // notification (cache stays stale until the next change) instead
      // of corrupting the call chain. The authoritative state is
      // already committed to the router by this point.
      try {
        onStateChanged(snapshot());
      } catch (cause) {
        logger.warn`onStateChanged subscriber threw: ${cause instanceof Error ? cause.message : String(cause)}`;
      }
    }
  }

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

  // Append `value` to `existing` only when it is not already present.
  // The thread's participant list is small enough that linear-scan dedup
  // is the right cost.
  function appendUnique(existing: readonly string[], value: string): string[] {
    if (existing.includes(value)) return [...existing];
    return [...existing, value];
  }

  function route(message: InboundMessage): RouteDecision {
    if (state === null) {
      const nextState: ConnectorThreadState = {
        threadRoot: message.headers.messageId,
        lastMessageId: message.headers.messageId,
        replyTo: extractAddrSpec(message.headers.from),
        cc: [],
        ...(message.headers.subject !== undefined
          ? { subject: message.headers.subject }
          : {}),
      };
      const decision: RouteDecision = { kind: "start" };
      pendingStates.set(decision, nextState);
      return decision;
    }

    if (isContinuation(message)) {
      const nextSpeaker = extractAddrSpec(message.headers.from);
      // The previous most-recent speaker moves into the cc list; the
      // new speaker becomes replyTo. Dedup so a sender returning after
      // others have spoken doesn't appear twice.
      const carriedCc = appendUnique(state.cc, state.replyTo).filter(
        (addr) => addr !== nextSpeaker,
      );
      const nextState: ConnectorThreadState = {
        threadRoot: state.threadRoot,
        lastMessageId: message.headers.messageId,
        replyTo: nextSpeaker,
        cc: carriedCc,
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

    pendingStates.delete(decision);
    applyState(nextState);
  }

  function composeReply(): ConnectorReplyParts {
    if (state === null) {
      throw new NoActiveConnectorThreadError();
    }

    return {
      to: state.replyTo,
      cc: [...state.cc],
      inReplyTo: state.lastMessageId,
      ...(state.subject !== undefined ? { subject: state.subject } : {}),
    };
  }

  function onReplySent(receipt: SendReceipt): void {
    if (state === null) {
      throw new NoActiveConnectorThreadError();
    }
    applyState({
      threadRoot: state.threadRoot,
      lastMessageId: receipt.messageId,
      replyTo: state.replyTo,
      cc: [...state.cc],
      ...(state.subject !== undefined ? { subject: state.subject } : {}),
    });
  }

  function snapshot(): ConnectorThreadState | null {
    if (state === null) return null;
    return {
      threadRoot: state.threadRoot,
      lastMessageId: state.lastMessageId,
      replyTo: state.replyTo,
      cc: [...state.cc],
      ...(state.subject !== undefined ? { subject: state.subject } : {}),
    };
  }

  function restore(next: ConnectorThreadState | null): void {
    applyState(
      next === null
        ? null
        : {
            threadRoot: next.threadRoot,
            lastMessageId: next.lastMessageId,
            replyTo: next.replyTo,
            cc: [...next.cc],
            ...(next.subject !== undefined ? { subject: next.subject } : {}),
          },
    );
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
