// Registry of active event collectors, keyed by run address.
//
// The hub creates a collector when an instance starts and removes it when the
// instance ends or the sidecar disconnects. The hub session orchestrator's
// `agent.event` listener looks up the collector by run address and
// dispatches the event.

import type { DB } from "@intx/db";
import type { InferenceEvent } from "@intx/types/runtime";
import type { SessionStatus } from "@intx/types";
import { getLogger } from "@intx/log";

import {
  createEventCollector,
  type EventCollector,
  type TurnFinalized,
  type TurnUsage,
} from "./event-collector";

const log = getLogger(["hub", "event-collector-registry"]);

export type EventCollectorRegistry = {
  create(
    agentAddress: string,
    tenantId: string,
    sessionId: string,
    runId: string,
  ): void;
  dispatch(agentAddress: string, event: InferenceEvent): void;
  abandon(agentAddress: string): void;
  has(agentAddress: string): boolean;
  getStatus(agentAddress: string): SessionStatus | undefined;
  getAccumulatedText(agentAddress: string): string | undefined;
  getCurrentTurnId(agentAddress: string): string | null | undefined;
  getLastTurnId(agentAddress: string): string | null | undefined;
};

export type EventCollectorRegistryConfig = {
  db: DB["db"];
  onTurnFinalized?: (agentAddress: string, turn: TurnFinalized) => void;
  onUsage?: (agentAddress: string, usage: TurnUsage) => void;
};

/**
 * The per-address state the session status is derived from: the turns that
 * run now, and the gates that are open, with their turn.
 */
type StatusTracker = {
  running: Set<string>;
  gates: Map<string, { turnId: string | undefined; approval: boolean }>;
};

function createStatusTracker(): StatusTracker {
  return { running: new Set(), gates: new Map() };
}

function dropGatesOf(tracker: StatusTracker, turnId: string): void {
  for (const [gateId, gate] of tracker.gates) {
    if (gate.turnId === turnId) tracker.gates.delete(gateId);
  }
}

function holdsApproval(tracker: StatusTracker, turnId: string): boolean {
  for (const gate of tracker.gates.values()) {
    if (gate.turnId === turnId && gate.approval) return true;
  }
  return false;
}

// The status of the tracker's state: `busy` while a running turn holds no
// open approval gate, else `waiting_approval` while any approval gate is
// open, else `idle`.
function statusOf(tracker: StatusTracker): SessionStatus {
  for (const turnId of tracker.running) {
    if (!holdsApproval(tracker, turnId)) return { status: "busy" };
  }
  for (const gate of tracker.gates.values()) {
    if (gate.approval) return { status: "waiting_approval" };
  }
  return { status: "idle" };
}

/**
 * Reduce one event into the tracker and return the session status it sets,
 * or `null` for an event that does not change the tracker. After each event
 * that changes it, the status is computed from the whole state, so one
 * turn's events cannot hide another turn's work or its open approval.
 *
 * State: the running turns, and the open gates (gate id to turn and whether
 * it is an approval).
 * - `reactor.turn.started` and `inference.start` make their turn run.
 * - `reactor.gate.blocked` records the gate. Its turn is parked, so it no
 *   longer runs.
 * - `reactor.gate.cleared` of a known gate drops the gate (any reason) and
 *   makes its turn run again, except for an abandoned turn's gate: nothing
 *   resumes. The event carries no turn id, so one for a gate the tracker
 *   does not know changes nothing.
 * - `reactor.turn.ended` stops the turn and drops its gates. A `joined` end
 *   changes nothing.
 * - `reactor.done` and a fatal `reactor.error` clear the state.
 * - `connector.reply` and other events change nothing.
 *
 * Status: `busy` while any running turn holds no open approval gate; else
 * `waiting_approval` while any approval gate is open; else `idle`. A turn
 * that holds an open approval gate shows `waiting_approval` also while it
 * infers: that turn cannot finish until a person decides, so
 * `waiting_approval` is the actionable status. Another running turn still
 * makes the session `busy`.
 *
 * Known gap: after a restart the tracker never sees `reactor.gate.blocked`
 * for a rehydrated approval gate, so the status reads `idle`, not
 * `waiting_approval`, until that gate clears.
 */
function deriveStatus(
  tracker: StatusTracker,
  event: InferenceEvent,
): SessionStatus | null {
  switch (event.type) {
    case "inference.start": {
      // The type allows an `inference.start` with no turn id: the inference
      // harness emits it before the reactor stamps the turn id. The hub
      // gets only stamped events. One with no turn id sets `busy` but
      // changes no state, so no turn runs, and the next event that changes
      // the state gives the status from it.
      const turnId = event.data.turnId;
      if (turnId === undefined) return { status: "busy" };
      tracker.running.add(turnId);
      return statusOf(tracker);
    }
    case "reactor.gate.blocked": {
      const { turnId } = event.data;
      const approval = event.data.reason === "approval";
      tracker.gates.set(event.data.gateId, { turnId, approval });
      if (turnId !== undefined) tracker.running.delete(turnId);
      return statusOf(tracker);
    }
    case "reactor.turn.started":
      tracker.running.add(event.data.turnId);
      return statusOf(tracker);
    case "reactor.gate.cleared": {
      const gate = tracker.gates.get(event.data.gateId);
      if (gate === undefined) return null;
      tracker.gates.delete(event.data.gateId);
      if (gate.turnId !== undefined && event.data.reason !== "abandoned") {
        tracker.running.add(gate.turnId);
      }
      return statusOf(tracker);
    }
    case "reactor.turn.ended": {
      const ended = event.data;
      if (ended.outcome === "joined") return null;
      tracker.running.delete(ended.turnId);
      dropGatesOf(tracker, ended.turnId);
      return statusOf(tracker);
    }
    case "reactor.done":
      tracker.running.clear();
      tracker.gates.clear();
      return statusOf(tracker);
    case "reactor.error":
      if (!event.data.fatal) return null;
      tracker.running.clear();
      tracker.gates.clear();
      return statusOf(tracker);
    default:
      return null;
  }
}

export function createEventCollectorRegistry(
  config: EventCollectorRegistryConfig,
): EventCollectorRegistry {
  const { db, onTurnFinalized, onUsage } = config;
  const collectors = new Map<string, EventCollector>();
  const statuses = new Map<string, SessionStatus>();
  const trackers = new Map<string, StatusTracker>();
  // Per-address tail promise: serializes onEvent/abandon work for one run
  // address so their DB writes cannot interleave. Reaped when it drains.
  const tails = new Map<string, Promise<void>>();

  function create(
    agentAddress: string,
    tenantId: string,
    sessionId: string,
    runId: string,
  ): void {
    if (collectors.has(agentAddress)) {
      log.warn`Collector already exists for ${agentAddress}, replacing`;
      abandon(agentAddress);
    }

    const collector = createEventCollector({
      db,
      sessionId,
      runId,
      tenantId,
      ...(onTurnFinalized
        ? {
            onTurnFinalized: (turn: TurnFinalized) =>
              onTurnFinalized(agentAddress, turn),
          }
        : {}),
      ...(onUsage
        ? {
            onUsage: (usage: TurnUsage) => onUsage(agentAddress, usage),
          }
        : {}),
    });
    collectors.set(agentAddress, collector);
    statuses.set(agentAddress, { status: "idle" });
    trackers.set(agentAddress, createStatusTracker());
  }

  function removeCollector(agentAddress: string): void {
    collectors.delete(agentAddress);
    statuses.delete(agentAddress);
    trackers.delete(agentAddress);
  }

  // Chain `work` onto the address's tail so per-address work runs in order and
  // never interleaves, while the caller stays non-blocking. `onError` swallows
  // a failure so one bad event cannot wedge the chain; `onSettled` runs after
  // the work settles. The tail entry is reaped once no later work is queued.
  function enqueue(
    agentAddress: string,
    work: () => Promise<void>,
    onError: (err: unknown) => void,
    onSettled?: () => void,
  ): void {
    const prev = tails.get(agentAddress) ?? Promise.resolve();
    const next = prev
      .then(work)
      .catch(onError)
      .finally(() => {
        if (onSettled !== undefined) {
          onSettled();
        }
        if (tails.get(agentAddress) === next) {
          tails.delete(agentAddress);
        }
      });
    tails.set(agentAddress, next);
  }

  function dispatch(agentAddress: string, event: InferenceEvent): void {
    const collector = collectors.get(agentAddress);
    const tracker = trackers.get(agentAddress);
    if (collector === undefined || tracker === undefined) {
      return;
    }

    const derived = deriveStatus(tracker, event);
    if (derived !== null) {
      statuses.set(agentAddress, derived);
    }

    const isTerminal =
      event.type === "reactor.done" ||
      (event.type === "reactor.error" && event.data.fatal);

    enqueue(
      agentAddress,
      () => collector.onEvent(event),
      (err: unknown) => {
        log.warn`Failed to persist event ${event.type} seq=${String(event.seq)} for ${agentAddress}: ${err instanceof Error ? err.message : String(err)}`;
      },
      () => {
        if (isTerminal && collectors.get(agentAddress) === collector) {
          removeCollector(agentAddress);
        }
      },
    );
  }

  function abandon(agentAddress: string): void {
    const collector = collectors.get(agentAddress);
    if (collector === undefined) return;

    // Stop NEW dispatches immediately; the queued closures keep their own
    // reference so already-queued events still drain before the abandon runs.
    removeCollector(agentAddress);

    // Chain the abandon onto the tail so it runs AFTER any queued onEvents
    // instead of racing them. Otherwise a queued beginTurn could create a
    // fresh `running` turn row after the collector was finalized, orphaning it.
    enqueue(
      agentAddress,
      () => collector.abandon(),
      (err: unknown) => {
        log.warn`Failed to abandon collector for ${agentAddress}: ${err instanceof Error ? err.message : String(err)}`;
      },
    );
  }

  function has(agentAddress: string): boolean {
    return collectors.has(agentAddress);
  }

  function getStatus(agentAddress: string): SessionStatus | undefined {
    return statuses.get(agentAddress);
  }

  function getAccumulatedText(agentAddress: string): string | undefined {
    return collectors.get(agentAddress)?.getAccumulatedText();
  }

  function getCurrentTurnId(agentAddress: string): string | null | undefined {
    return collectors.get(agentAddress)?.getCurrentTurnId();
  }

  function getLastTurnId(agentAddress: string): string | null | undefined {
    return collectors.get(agentAddress)?.getLastTurnId();
  }

  return {
    create,
    dispatch,
    abandon,
    has,
    getStatus,
    getAccumulatedText,
    getCurrentTurnId,
    getLastTurnId,
  };
}
