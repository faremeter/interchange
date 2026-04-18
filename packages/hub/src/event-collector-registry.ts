// Registry of active event collectors, keyed by sessionId.
//
// The hub creates a collector when a session starts and removes it when the
// session ends or the sidecar disconnects. The onAgentEvent callback looks
// up the collector by sessionId and dispatches the event.

import type { DB } from "@interchange/db";
import type { InferenceEvent } from "@interchange/types/runtime";
import { getLogger } from "@interchange/log";

import { createEventCollector, type EventCollector } from "./event-collector";

const log = getLogger(["hub", "event-collector-registry"]);

export type EventCollectorRegistry = {
  create(sessionId: string, tenantId: string, agentAddress: string): void;
  dispatch(sessionId: string, event: InferenceEvent): void;
  abandon(sessionId: string): void;
  abandonByAddress(agentAddress: string): void;
  has(sessionId: string): boolean;
};

export function createEventCollectorRegistry(
  db: DB["db"],
): EventCollectorRegistry {
  const collectors = new Map<string, EventCollector>();
  const addressToSession = new Map<string, string>();
  const sessionToAddress = new Map<string, string>();

  function create(
    sessionId: string,
    tenantId: string,
    agentAddress: string,
  ): void {
    if (collectors.has(sessionId)) {
      log.warn`Collector already exists for session ${sessionId}, replacing`;
      abandon(sessionId);
    }

    const collector = createEventCollector({ db, sessionId, tenantId });
    collectors.set(sessionId, collector);
    addressToSession.set(agentAddress, sessionId);
    sessionToAddress.set(sessionId, agentAddress);
  }

  function removeSession(sessionId: string): void {
    collectors.delete(sessionId);
    const addr = sessionToAddress.get(sessionId);
    if (addr !== undefined) {
      addressToSession.delete(addr);
      sessionToAddress.delete(sessionId);
    }
  }

  function dispatch(sessionId: string, event: InferenceEvent): void {
    const collector = collectors.get(sessionId);
    if (collector === undefined) {
      return;
    }

    const isTerminal =
      event.type === "reactor.done" ||
      (event.type === "reactor.error" && event.data.fatal);

    collector
      .onEvent(event)
      .catch((err: unknown) => {
        log.warn`Failed to persist event ${event.type} seq=${String(event.seq)} for session ${sessionId}: ${err instanceof Error ? err.message : String(err)}`;
      })
      .finally(() => {
        if (isTerminal) {
          removeSession(sessionId);
        }
      });
  }

  function abandon(sessionId: string): void {
    const collector = collectors.get(sessionId);
    if (collector === undefined) return;

    collector.abandon().catch((err: unknown) => {
      log.warn`Failed to abandon collector for session ${sessionId}: ${err instanceof Error ? err.message : String(err)}`;
    });

    removeSession(sessionId);
  }

  function abandonByAddress(agentAddress: string): void {
    const sessionId = addressToSession.get(agentAddress);
    if (sessionId === undefined) return;
    abandon(sessionId);
  }

  function has(sessionId: string): boolean {
    return collectors.has(sessionId);
  }

  return { create, dispatch, abandon, abandonByAddress, has };
}
