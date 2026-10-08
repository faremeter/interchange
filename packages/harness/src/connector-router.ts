// Connector-thread routing for the agent harness.
//
// One durable thread per agent. On a thread holding an anchor (a thread
// root or last message id), participants accumulate as they speak and no
// one is displaced: `replyTo` is the most recent speaker, `cc` every
// other participant. A thread holding neither anchor opens fresh on the
// next arrival.
//
// Two-phase: route() is pure and returns a kind plus an opaque carrier
// of the next state; commit() advances state from that carrier, so the
// harness can sequence side effects around the mutation.

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
   * Called synchronously after state mutates, only when the new state
   * differs. Single subscriber: the harness wiring that lifts changes
   * onto the hub-bound event channel. Throwing errors are caught and
   * logged -- the authoritative state stays in the router and the
   * persisted store, so a dropped notification only leaves the
   * projection stale until the next change.
   */
  onStateChanged?(state: ConnectorThreadState | null): void;
};

export interface ConnectorRouter {
  /**
   * Classify an inbound message; pure, so the returned decision must be
   * passed to `commit()`. Returns `passthrough` when `from` is absent
   * and throws when it is present but not a parseable bare addr-spec.
   * Does not read `Interchange-Type`: any message naming a `From` starts
   * or continues the thread.
   */
  route(message: InboundMessage): RouteDecision;

  /**
   * Advance router state per a decision produced by `route()`. No-op for
   * `passthrough`. For `start` and `continue`, throws if the decision was
   * not produced by this router instance.
   */
  commit(decision: RouteDecision): void;

  /**
   * Produce the threading headers for a reply on the active thread:
   * `to` is the most recent speaker, `cc` everyone else (deduplicated).
   * Throws `NoActiveConnectorThreadError` when no thread is active.
   */
  composeReply(): ConnectorReplyParts;

  /**
   * Update `lastMessageId` after a successful outbound reply send.
   * Throws when called with no active thread.
   */
  onReplySent(receipt: SendReceipt): void;

  /** Current state as a serializable snapshot, or `null` when no thread is active. */
  snapshot(): ConnectorThreadState | null;

  /** Install a snapshot as the current state (startup restore, test setup); `null` clears the thread. */
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

  // Hold pending state off the decision via a WeakMap so callers see
  // only `{ kind }` -- no path to inspect or mutate the next state.
  const pendingStates = new WeakMap<RouteDecision, ConnectorThreadState>();

  function applyState(next: ConnectorThreadState | null): void {
    // The null -> X transition drives bootstrap on restore(); a refactor
    // that collapsed null into a "no mutation" sentinel would silently
    // break the hub-side cache's only fill path. Null is a value, not a
    // non-event.
    if (statesEqual(state, next)) return;
    state = next;
    if (onStateChanged !== undefined) {
      // A throwing subscriber would abort commit()/onReplySent(); catch
      // here to drop one notification (projection stale until the next
      // change) instead of corrupting the call chain.
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

  // Append `value` unless already present; the list is small enough
  // that linear-scan dedup is the right cost.
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

  // A thread opened by a message with no id holds neither anchor, so
  // nothing can continue it; the next message starts fresh rather than
  // being absorbed forever.
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

      // The previous replyTo moves into cc; the new speaker becomes
      // replyTo. Dedup so a returning sender does not appear twice.
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
