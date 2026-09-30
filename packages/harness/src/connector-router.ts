// Connector-thread routing for the agent harness.
//
// The connector is one durable thread per agent. On a thread holding an
// anchor -- a thread root or a last message id -- participants accumulate
// as they speak and no one is displaced: `replyTo` tracks the most recent
// speaker (the primary recipient on the next outbound reply) and `cc`
// tracks every other participant who has spoken (carried on outbound so
// everyone stays in the loop). A thread holding neither anchor offers
// nothing for a later message to continue, so the next arrival opens a
// fresh thread and the accumulated list starts over.
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
  /**
   * The nearest identified ancestor: the last message id, or the thread
   * root when the last message named no id. Absent when neither exists.
   */
  inReplyTo?: string;
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
   * Returns `passthrough` when `message.headers.from` is absent: the
   * message names nobody to reply to.
   *
   * Throws when it is present but is not a parseable bare addr-spec.
   * Callers should treat the throw as passthrough — deliver the message
   * but do not advance router state or consume it from the INBOX.
   *
   * Does not read `Interchange-Type`. A structured payload that names a
   * `From` starts or continues the thread exactly as a conversation
   * message does.
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

    if (
      state.threadRoot !== undefined &&
      references !== undefined &&
      references.includes(state.threadRoot)
    ) {
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

  function speakerOf(message: InboundMessage): string | null {
    const { from } = message.headers;
    if (from === undefined) return null;
    return extractAddrSpec(from);
  }

  function passthroughWithNoReplyAddress(
    message: InboundMessage,
  ): RouteDecision {
    logger.debug`Message ${message.headers.messageId} carries no From header, so it has no reply address; routing it as passthrough and leaving the connector thread unadvanced`;
    return { kind: "passthrough" };
  }

  // A thread opened by a message that named no id holds neither anchor, so
  // `isContinuation` can never match it again. The next message starts a
  // fresh thread rather than being absorbed by it forever.
  function isAnchored(s: ConnectorThreadState): boolean {
    return s.threadRoot !== undefined || s.lastMessageId !== undefined;
  }

  function route(message: InboundMessage): RouteDecision {
    if (state === null || !isAnchored(state)) {
      const replyTo = speakerOf(message);
      if (replyTo === null) return passthroughWithNoReplyAddress(message);

      const nextState: ConnectorThreadState = {
        ...(message.headers.messageId !== undefined
          ? {
              threadRoot: message.headers.messageId,
              lastMessageId: message.headers.messageId,
            }
          : {}),
        replyTo,
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
      const nextSpeaker = speakerOf(message);
      if (nextSpeaker === null) return passthroughWithNoReplyAddress(message);

      // The previous most-recent speaker moves into the cc list; the
      // new speaker becomes replyTo. Dedup so a sender returning after
      // others have spoken doesn't appear twice.
      const carriedCc = appendUnique(state.cc, state.replyTo).filter(
        (addr) => addr !== nextSpeaker,
      );
      const nextState: ConnectorThreadState = {
        ...(state.threadRoot !== undefined
          ? { threadRoot: state.threadRoot }
          : {}),
        ...(message.headers.messageId !== undefined
          ? { lastMessageId: message.headers.messageId }
          : {}),
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

    // RFC 5322 3.6.4: a parent that named no Message-ID leaves the root as
    // the nearest identified ancestor, and the References chain built from
    // it is what keeps the reply on the thread.
    const parent = state.lastMessageId ?? state.threadRoot;

    return {
      to: state.replyTo,
      cc: [...state.cc],
      ...(parent !== undefined ? { inReplyTo: parent } : {}),
      ...(state.subject !== undefined ? { subject: state.subject } : {}),
    };
  }

  function onReplySent(receipt: SendReceipt): void {
    if (state === null) {
      throw new NoActiveConnectorThreadError();
    }
    applyState({
      ...(state.threadRoot !== undefined
        ? { threadRoot: state.threadRoot }
        : {}),
      lastMessageId: receipt.messageId,
      replyTo: state.replyTo,
      cc: [...state.cc],
      ...(state.subject !== undefined ? { subject: state.subject } : {}),
    });
  }

  function snapshot(): ConnectorThreadState | null {
    if (state === null) return null;
    return {
      ...(state.threadRoot !== undefined
        ? { threadRoot: state.threadRoot }
        : {}),
      ...(state.lastMessageId !== undefined
        ? { lastMessageId: state.lastMessageId }
        : {}),
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
            ...(next.threadRoot !== undefined
              ? { threadRoot: next.threadRoot }
              : {}),
            ...(next.lastMessageId !== undefined
              ? { lastMessageId: next.lastMessageId }
              : {}),
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
