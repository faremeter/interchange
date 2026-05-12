// Registry of active event collectors, keyed by agent address.
//
// The hub creates a collector when an instance starts and removes it when the
// instance ends or the sidecar disconnects. The onAgentEvent callback looks
// up the collector by agent address and dispatches the event.

import type { DB } from "@interchange/db";
import type { InferenceEvent } from "@interchange/types/runtime";
import type { SessionStatus } from "@interchange/types";
import { getLogger } from "@interchange/log";

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
    instanceId: string,
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

  function create(
    agentAddress: string,
    tenantId: string,
    sessionId: string,
    instanceId: string,
  ): void {
    if (collectors.has(agentAddress)) {
      log.warn`Collector already exists for ${agentAddress}, replacing`;
      abandon(agentAddress);
    }

    const collector = createEventCollector({
      db,
      sessionId,
      instanceId,
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

    collector
      .onEvent(event)
      .catch((err: unknown) => {
        log.warn`Failed to persist event ${event.type} seq=${String(event.seq)} for ${agentAddress}: ${err instanceof Error ? err.message : String(err)}`;
      })
      .finally(() => {
        if (isTerminal) {
          removeCollector(agentAddress);
        }
      });
  }

  function abandon(agentAddress: string): void {
    const collector = collectors.get(agentAddress);
    if (collector === undefined) return;

    collector.abandon().catch((err: unknown) => {
      log.warn`Failed to abandon collector for ${agentAddress}: ${err instanceof Error ? err.message : String(err)}`;
    });

    removeCollector(agentAddress);
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
