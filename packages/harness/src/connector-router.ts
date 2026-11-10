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
// The speaker of a message is its reply address: the authenticated sender
// when the message carries one, else the addr-spec of its `From`. The
// authenticated sender is the address the hub verified the message under,
// a run address or the address of the principal that sent it, and the hub
// routes mail to either. The `From` is the sender's own claim. It is the
// speaker only for mail with no authenticated sender: mail that
// `createHarness` receives, and mail recorded before the hub stamped one.
//
// `In-Reply-To` and `References` are the sender's own claims too, so a
// message that names the thread continues it only when its speaker is a
// participant: a speaker on the thread (`replyTo` or `cc`), or a recipient
// that a participant addressed on the thread (`addressed`). Mail from any
// other speaker that names the thread is passthrough. An addressed
// recipient is not on outbound mail until it speaks on the thread.
//
// Addresses compare case-insensitively: both sides of a comparison are
// lowercased whole, so state restored with any spelling still matches.
// The router does not rewrite restored values. An address it reads from a
// message comes through `extractAddrSpec`, which lowercases it.
//
// Two-phase decision: route() is pure and returns a discriminated kind
// plus an opaque carrier of the next state; commit() advances router
// state from that carrier. Separating the decision from the mutation
// lets the harness sequence the side effects (deliver, INBOX expunge)
// around the state change however it needs to.
//
// A reply builds its threading headers from the exact message it answers,
// as RFC 5322 3.6.4 describes: `In-Reply-To` is the parent's Message-ID and
// `References` is the parent's References followed by that id. A parent with
// no Message-ID gives no `In-Reply-To`, and `References` is its References
// alone. On the thread, the parent is the most recent message:
// `lastMessageId` holds its Message-ID and `lastReferences` its References.
// An inbound start or continue makes that message the parent; a sent thread
// reply makes the agent's own reply the parent, with the References it went
// out with. The router never looks a parent up by Message-ID.
//
// A passthrough message does not join the thread, so the thread state does
// not answer it. Its commit sets a turn reply target instead: the reply
// goes to the passthrough sender, threaded under that message, and the
// thread state stays as it was. The next start or continue commit clears
// it, and so does `beginTurn()`.
//
// A turn that parks on an approval gate keeps its target across the park:
// `parkTurn()` files the target under the gate's correlation id, and the
// `beginTurn()` of the approval resume takes it back. The router holds the
// parked targets in memory; the caller persists them through
// `parkedReplyTargets()` and installs them with
// `restoreParkedReplyTargets()`.

import { getLogger } from "@intx/log";
import { extractAddrSpec, isMessageId } from "@intx/mime";
import type {
  ConnectorReplyParts,
  ConnectorThreadState,
  InboundMessage,
  ParkedReplyTarget,
  ParkedReplyTargets,
  SendReceipt,
} from "@intx/types/runtime";

export type { ConnectorReplyParts };

const logger = getLogger(["interchange", "harness", "connector-router"]);

export type RouteDecision =
  | { kind: "start" }
  | { kind: "continue" }
  | { kind: "passthrough" };

export class NoActiveConnectorThreadError extends Error {
  constructor() {
    super("no active connector thread");
    this.name = "NoActiveConnectorThreadError";
  }
}

export type ConnectorRouterOptions = {
  /**
   * The address the agent receives mail at. The router keeps it out of
   * `addressed`, so mail to the agent does not make the agent a
   * participant of its own thread.
   */
  ownAddress: string;
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
   * Returns `passthrough` when the message names no speaker, that is, it
   * carries neither `authenticatedSender` nor `headers.from`: the message
   * names nobody to reply to. Returns `passthrough` when the message names
   * the active anchored thread but its speaker is not a participant.
   *
   * Throws when the speaker's address is present but is not a parseable
   * addr-spec. Callers should treat the throw as passthrough — deliver
   * the message but do not advance router state or consume it from the
   * INBOX.
   *
   * Does not read `Interchange-Type`. A structured payload that names a
   * `From` starts or continues the thread exactly as a conversation
   * message does.
   */
  route(message: InboundMessage): RouteDecision;

  /**
   * Apply a decision produced by `route()`. For `start` and `continue`,
   * advance the thread state and clear the turn reply target; throws if
   * the decision was not produced by this router instance.
   *
   * For `passthrough`, leave the thread state unchanged and set the turn
   * reply target to the passthrough message's sender. A passthrough
   * decision that names no sender sets a target with no address, so
   * `composeReply()` returns `null` for the turn. That is the case when
   * the message had no `From`, or when the caller made the decision itself
   * after `route()` threw.
   */
  commit(decision: RouteDecision): void;

  /**
   * Mark the start of an agent turn. A turn that resumes the approval gate
   * `turn.resume` takes the reply target parked under that correlation id
   * and removes it from the parked targets. When no target is parked
   * under that id, the parked turn had no turn reply target, so the
   * resumed turn replies on the thread. Any other turn clears the turn
   * reply target, so a turn that commits no passthrough decision replies
   * on the thread. Returns `true` when the parked targets changed.
   */
  beginTurn(turn?: { resume?: string }): boolean;

  /**
   * Park the current turn on the approval gate `correlationId`: file the
   * turn reply target under that id, so the `beginTurn()` of the approval
   * resume takes it back. A target with no address is filed as such. When
   * the turn has no turn reply target, nothing is filed. The turn reply
   * target stays set until the next `beginTurn()`. Returns `true` when the
   * parked targets changed.
   */
  parkTurn(correlationId: string): boolean;

  /**
   * Return a copy of the parked reply targets, keyed by correlation id.
   */
  parkedReplyTargets(): ParkedReplyTargets;

  /**
   * Replace the parked reply targets. Used at startup to restore the
   * targets the caller persisted.
   */
  restoreParkedReplyTargets(targets: ParkedReplyTargets): void;

  /**
   * Produce the threading headers needed to send a reply. When a
   * passthrough commit set a turn reply target, `to` is the passthrough
   * sender, `cc` is empty, and the headers thread the reply under that
   * message; the return is `null` when that sender has no address.
   * Otherwise `to` is the most recent speaker on the active connector
   * thread and `cc` is everyone else who has spoken on the thread
   * (deduplicated). The thread reply answers the most recent message on
   * the thread: `inReplyTo` is `lastMessageId` and `references` is
   * `lastReferences` followed by `lastMessageId`. When that message named
   * no id, `inReplyTo` is absent and `references` is `lastReferences`
   * alone. Each is absent when it would be empty. The caller composes the
   * full outbound message by adding its own `content` and `type` fields.
   * Throws `NoActiveConnectorThreadError` when no turn reply target is set
   * and no thread is active.
   */
  composeReply(): ConnectorReplyParts | null;

  /**
   * Make a sent thread reply the parent of the next one: `lastMessageId`
   * becomes the receipt's Message-ID, and `lastReferences` becomes
   * `references`, the References chain the reply was sent with. A reply to
   * a turn reply target leaves the thread state unchanged. Throws when
   * called with no turn reply target and no active thread — outbound state
   * advance has no meaning without a thread.
   */
  onReplySent(receipt: SendReceipt, references: readonly string[]): void;

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
    listsEqual(a.lastReferences ?? [], b.lastReferences ?? []) &&
    a.replyTo === b.replyTo &&
    a.subject === b.subject &&
    listsEqual(a.cc, b.cc) &&
    listsEqual(a.addressed ?? [], b.addressed ?? [])
  );
}

function listsEqual(a: readonly string[], b: readonly string[]): boolean {
  return a.length === b.length && a.every((v, i) => v === b[i]);
}

function sameAddress(a: string, b: string): boolean {
  return a.toLowerCase() === b.toLowerCase();
}

function includesAddress(list: readonly string[], address: string): boolean {
  return list.some((entry) => sameAddress(entry, address));
}

// Append each of `values` to `existing` when it is not already present.
// The thread's address lists are small enough that linear-scan dedup is
// the right cost.
function appendUnique(
  existing: readonly string[],
  values: readonly string[],
): string[] {
  const result = [...existing];
  for (const value of values) {
    if (!includesAddress(result, value)) result.push(value);
  }
  return result;
}

function addressedField(addressed: readonly string[]): {
  addressed?: string[];
} {
  return addressed.length > 0 ? { addressed: [...addressed] } : {};
}

// The parent fields of a thread state. A parent that named no Message-ID
// still has a References chain, so each field stands alone: `lastMessageId`
// is set when the parent named an id, and `lastReferences` when its chain is
// not empty.
function parentFields(
  lastMessageId: string | undefined,
  lastReferences: readonly string[] | undefined,
): { lastMessageId?: string; lastReferences?: string[] } {
  return {
    ...(lastMessageId !== undefined ? { lastMessageId } : {}),
    ...(lastReferences !== undefined && lastReferences.length > 0
      ? { lastReferences: [...lastReferences] }
      : {}),
  };
}

// The References chain of an inbound message, as RFC 5322 3.6.4 has a
// replier read it: the message's References, or else its In-Reply-To when
// that names exactly one Message-ID. An In-Reply-To that names several ids
// or none gives no chain.
function referencesOf(message: InboundMessage): string[] {
  const { references, inReplyTo } = message.headers;
  if (references !== undefined && references.length > 0) {
    return [...references];
  }
  if (inReplyTo !== undefined && isMessageId(inReplyTo.trim())) {
    return [inReplyTo.trim()];
  }
  return [];
}

export function createConnectorRouter(
  options: ConnectorRouterOptions,
): ConnectorRouter {
  let state: ConnectorThreadState | null = null;
  const { ownAddress, onStateChanged } = options;

  // Pending state per decision is held off the decision object via a
  // WeakMap so callers see only `{ kind }` — no path to inspect or
  // mutate the next state, even via type assertions.
  const pendingStates = new WeakMap<RouteDecision, ConnectorThreadState>();
  const pendingReplyTargets = new WeakMap<RouteDecision, ConnectorReplyParts>();

  // `null` when no passthrough commit set a target for the current turn.
  // `parts: null` is a passthrough sender with no address.
  let turnReplyTarget: ParkedReplyTarget | null = null;
  const parked = new Map<string, ParkedReplyTarget>();

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

  function isContinuation(
    s: ConnectorThreadState,
    message: InboundMessage,
    speaker: string,
  ): boolean {
    const { inReplyTo, references } = message.headers;
    const namesThread =
      (s.threadRoot !== undefined &&
        references !== undefined &&
        references.includes(s.threadRoot)) ||
      (inReplyTo !== undefined && inReplyTo === s.lastMessageId);
    if (!namesThread) return false;

    return (
      sameAddress(s.replyTo, speaker) ||
      includesAddress(s.cc, speaker) ||
      includesAddress(s.addressed ?? [], speaker)
    );
  }

  function speakerOf(message: InboundMessage): string | null {
    if (message.authenticatedSender !== undefined) {
      return extractAddrSpec(message.authenticatedSender);
    }
    const { from } = message.headers;
    if (from === undefined) return null;
    return extractAddrSpec(from);
  }

  // A recipient entry that does not parse names nobody the thread could
  // admit, so the router leaves it out rather than fail the route.
  function recipientsOf(message: InboundMessage): string[] {
    const { to, cc } = message.headers;
    const recipients: string[] = [];
    for (const entry of [...to, ...(cc ?? [])]) {
      let address: string;
      try {
        address = extractAddrSpec(entry);
      } catch {
        continue;
      }
      if (!sameAddress(address, ownAddress)) recipients.push(address);
    }
    return recipients;
  }

  function passthroughWithNoReplyAddress(
    message: InboundMessage,
  ): RouteDecision {
    logger.debug`Message ${message.headers.messageId} names no sender, so it has no reply address; routing it as passthrough and leaving the connector thread unadvanced`;
    return { kind: "passthrough" };
  }

  // A thread opened by a message that named no id holds neither anchor, so
  // `isContinuation` can never match it again. The next message starts a
  // fresh thread rather than being absorbed by it forever.
  function isAnchored(s: ConnectorThreadState): boolean {
    return s.threadRoot !== undefined || s.lastMessageId !== undefined;
  }

  function route(message: InboundMessage): RouteDecision {
    const speaker = speakerOf(message);
    if (speaker === null) return passthroughWithNoReplyAddress(message);

    if (state === null || !isAnchored(state)) {
      const nextState: ConnectorThreadState = {
        ...(message.headers.messageId !== undefined
          ? { threadRoot: message.headers.messageId }
          : {}),
        ...parentFields(message.headers.messageId, referencesOf(message)),
        replyTo: speaker,
        cc: [],
        ...addressedField(appendUnique([], recipientsOf(message))),
        ...(message.headers.subject !== undefined
          ? { subject: message.headers.subject }
          : {}),
      };
      const decision: RouteDecision = { kind: "start" };
      pendingStates.set(decision, nextState);
      return decision;
    }

    if (isContinuation(state, message, speaker)) {
      // The previous most-recent speaker moves into the cc list; the
      // new speaker becomes replyTo. Dedup so a sender returning after
      // others have spoken doesn't appear twice.
      const carriedCc = appendUnique(state.cc, [state.replyTo]).filter(
        (addr) => !sameAddress(addr, speaker),
      );
      const nextState: ConnectorThreadState = {
        ...(state.threadRoot !== undefined
          ? { threadRoot: state.threadRoot }
          : {}),
        ...parentFields(message.headers.messageId, referencesOf(message)),
        replyTo: speaker,
        cc: carriedCc,
        ...addressedField(
          appendUnique(state.addressed ?? [], recipientsOf(message)),
        ),
        ...(state.subject !== undefined ? { subject: state.subject } : {}),
      };
      const decision: RouteDecision = { kind: "continue" };
      pendingStates.set(decision, nextState);
      return decision;
    }

    const decision: RouteDecision = { kind: "passthrough" };
    pendingReplyTargets.set(decision, passthroughReplyParts(message, speaker));
    return decision;
  }

  // Thread the reply under the passthrough message itself. The chain is the
  // message's References plus its Message-ID, as the message carried them.
  // The router does not check them: composing the outbound message
  // (`composeOutbound` in `@intx/mailbox`) drops every References entry that
  // `isMessageId` rejects. That layer appends `inReplyTo` unchecked.
  function passthroughReplyParts(
    message: InboundMessage,
    to: string,
  ): ConnectorReplyParts {
    const { messageId, references, subject } = message.headers;
    const chain = [
      ...(references ?? []),
      ...(messageId !== undefined ? [messageId] : []),
    ];
    return {
      to,
      cc: [],
      ...(messageId !== undefined ? { inReplyTo: messageId } : {}),
      ...(chain.length > 0 ? { references: chain } : {}),
      ...(subject !== undefined ? { subject } : {}),
    };
  }

  function commit(decision: RouteDecision): void {
    if (decision.kind === "passthrough") {
      const parts = pendingReplyTargets.get(decision);
      pendingReplyTargets.delete(decision);
      turnReplyTarget = { parts: parts ?? null };
      return;
    }

    const nextState = pendingStates.get(decision);
    if (nextState === undefined) {
      throw new Error(
        "commit() called with a decision from a different router instance",
      );
    }

    pendingStates.delete(decision);
    turnReplyTarget = null;
    applyState(nextState);
  }

  function beginTurn(turn?: { resume?: string }): boolean {
    const resume = turn?.resume;
    if (resume === undefined) {
      turnReplyTarget = null;
      return false;
    }
    turnReplyTarget = parked.get(resume) ?? null;
    return parked.delete(resume);
  }

  function parkTurn(correlationId: string): boolean {
    if (turnReplyTarget === null) return false;
    parked.set(correlationId, copyTarget(turnReplyTarget));
    return true;
  }

  function parkedReplyTargets(): ParkedReplyTargets {
    const targets: ParkedReplyTargets = {};
    for (const [correlationId, target] of parked) {
      targets[correlationId] = copyTarget(target);
    }
    return targets;
  }

  function restoreParkedReplyTargets(targets: ParkedReplyTargets): void {
    parked.clear();
    for (const [correlationId, target] of Object.entries(targets)) {
      parked.set(correlationId, copyTarget(target));
    }
  }

  function copyParts(parts: ConnectorReplyParts): ConnectorReplyParts {
    return {
      ...parts,
      cc: [...parts.cc],
      ...(parts.references !== undefined
        ? { references: [...parts.references] }
        : {}),
    };
  }

  function copyTarget(target: ParkedReplyTarget): ParkedReplyTarget {
    return { parts: target.parts === null ? null : copyParts(target.parts) };
  }

  function composeReply(): ConnectorReplyParts | null {
    if (turnReplyTarget !== null) {
      const { parts } = turnReplyTarget;
      if (parts === null) return null;
      return copyParts(parts);
    }

    if (state === null) {
      throw new NoActiveConnectorThreadError();
    }

    // RFC 5322 3.6.4: `In-Reply-To` is the parent's Message-ID, and the
    // chain is the parent's References followed by that id. A parent that
    // named no Message-ID gives no `In-Reply-To`, and the chain is its
    // References alone. `lastReferences` is absent when the parent had no
    // References, and in a state persisted before the field existed.
    // `threadRoot` is not a fallback parent: the RFC threads a reply under
    // the message it answers, and that message's own chain is what keeps
    // the reply on the thread.
    const { lastMessageId } = state;
    const references = [
      ...(state.lastReferences ?? []),
      ...(lastMessageId !== undefined ? [lastMessageId] : []),
    ];

    return {
      to: state.replyTo,
      cc: [...state.cc],
      ...(lastMessageId !== undefined ? { inReplyTo: lastMessageId } : {}),
      ...(references.length > 0 ? { references } : {}),
      ...(state.subject !== undefined ? { subject: state.subject } : {}),
    };
  }

  function onReplySent(
    receipt: SendReceipt,
    references: readonly string[],
  ): void {
    if (turnReplyTarget !== null) return;
    if (state === null) {
      throw new NoActiveConnectorThreadError();
    }
    applyState({
      ...(state.threadRoot !== undefined
        ? { threadRoot: state.threadRoot }
        : {}),
      ...parentFields(receipt.messageId, references),
      replyTo: state.replyTo,
      cc: [...state.cc],
      ...addressedField(state.addressed ?? []),
      ...(state.subject !== undefined ? { subject: state.subject } : {}),
    });
  }

  function snapshot(): ConnectorThreadState | null {
    if (state === null) return null;
    return {
      ...(state.threadRoot !== undefined
        ? { threadRoot: state.threadRoot }
        : {}),
      ...parentFields(state.lastMessageId, state.lastReferences),
      replyTo: state.replyTo,
      cc: [...state.cc],
      ...addressedField(state.addressed ?? []),
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
            ...parentFields(next.lastMessageId, next.lastReferences),
            replyTo: next.replyTo,
            cc: [...next.cc],
            ...addressedField(next.addressed ?? []),
            ...(next.subject !== undefined ? { subject: next.subject } : {}),
          },
    );
  }

  return {
    route,
    commit,
    beginTurn,
    parkTurn,
    parkedReplyTargets,
    restoreParkedReplyTargets,
    composeReply,
    onReplySent,
    snapshot,
    restore,
  };
}
