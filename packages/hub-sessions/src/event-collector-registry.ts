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
};

export function deriveStatus(event: InferenceEvent): SessionStatus | null {
  switch (event.type) {
    case "inference.start":
      return { status: "busy" };
    case "connector.reply":
      return { status: "idle" };
    case "reactor.gate.blocked":
      if (event.data.reason === "approval")
        return { status: "waiting_approval" };
      return null;
    case "reactor.gate.cleared":
      return { status: "busy" };
    case "reactor.done":
      return { status: "idle" };
    case "reactor.error":
      if (event.data.fatal) return { status: "idle" };
      return null;
    default:
      return null;
  }
}

export function createEventCollectorRegistry(
  config: EventCollectorRegistryConfig,
): EventCollectorRegistry {
  const { db, onTurnFinalized } = config;
  const collectors = new Map<string, EventCollector>();
  const statuses = new Map<string, SessionStatus>();
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
    });
    collectors.set(agentAddress, collector);
    statuses.set(agentAddress, { status: "idle" });
  }

  function removeCollector(agentAddress: string): void {
    collectors.delete(agentAddress);
    statuses.delete(agentAddress);
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
    if (collector === undefined) {
      return;
    }

    const derived = deriveStatus(event);
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
