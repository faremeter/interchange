// Hub-side orchestrator for the sidecar event emitter.
//
// Subscribes to the sidecar router's events and runs the host-side
// reactions: dispatching inference events to the event collector,
// restoring the event collector on reconnect, re-deploying stale
// agents, and forwarding mail.delivered notifications back to
// subscribers.
//
// The orchestrator depends on a narrow `HubSessionRouterFacade` rather
// than the full SidecarRouter, so tests can drive subscriber behavior
// with a small stub and an isolated emitter.

import { eq } from "drizzle-orm";
import { type } from "arktype";
import type { DB } from "@intx/db";
import { workflowRun } from "@intx/db/schema";
import { parseMailToEmail } from "@intx/mime";
import { parseInferenceEvent } from "@intx/types/runtime";
import { getLogger } from "@intx/log";

import type { EventCollectorRegistry } from "./event-collector-registry";
import type { SidecarEventEmitter } from "./ws/sidecar-events";

const log = getLogger(["hub", "orchestrator"]);

/** Subset of `SidecarRouter` the orchestrator drives outbound. The
 * narrow surface keeps tests honest and decouples the orchestrator
 * from the rest of the router API. */
export type HubSessionRouterFacade = {
  dispatchAgentEvent(agentAddress: string, event: unknown): void;
};

export type HubSessionOrchestratorDeps = {
  events: SidecarEventEmitter;
  router: HubSessionRouterFacade;
  db: DB["db"];
  eventCollectors: EventCollectorRegistry;
};

export type HubSessionOrchestrator = {
  /** Unsubscribe all listeners. Tests use this between cases; the hub
   * application doesn't need to dispose because the orchestrator's
   * lifetime matches the process. */
  dispose(): void;
};

export function createHubSessionOrchestrator(
  deps: HubSessionOrchestratorDeps,
): HubSessionOrchestrator {
  const { events, router, db, eventCollectors } = deps;

  const unsubscribers: (() => void)[] = [];

  unsubscribers.push(
    events.on("agent.event", ({ agentAddress, event }) => {
      const validated = parseInferenceEvent(event);
      if (validated instanceof type.errors) {
        log.warn("Received invalid agent event for {agentAddress}: {summary}", {
          agentAddress,
          summary: validated.summary,
        });
        return;
      }
      eventCollectors.dispatch(agentAddress, validated);
    }),
  );

  unsubscribers.push(
    events.on("sidecar.disconnect", ({ ownedAddresses }) => {
      for (const addr of ownedAddresses) {
        eventCollectors.abandon(addr);
      }
    }),
  );

  unsubscribers.push(
    events.on("mail.outbound.undelivered", ({ recipients }) => {
      // The hub has no external mail transport today. Anything that
      // could not be delivered locally or queued for a disconnected
      // agent is dropped; log so operators can see it.
      log.warn("Dropping mail with no local recipient: {recipients}", {
        recipients: recipients.join(", "),
      });
    }),
  );

  unsubscribers.push(
    events.on("agent.deploy.ack", async (event) => {
      const { agentAddress, publicKey, allocated } = event;
      // Provisioned initialization publishes its key only after every deploy
      // and asset pack succeeds under the allocation generation fence.
      if (allocated !== undefined) return;

      // Every deploy address names a workflow run whose public key lives on its
      // single self-anchored workflow_run row, keyed by address. Persist it
      // there as the deployment's published identity. Reconnect routing is
      // allocation-authenticated, so this projection is not connection
      // authority. Only the deployment-level address owns a row, so a stray
      // per-step ack updates nothing.
      await db
        .update(workflowRun)
        .set({ publicKey })
        .where(eq(workflowRun.address, agentAddress));
    }),
  );

  unsubscribers.push(
    events.on("mail.persisted", (row) => {
      const parsed = parseMailToEmail(row.raw, row.id);
      router.dispatchAgentEvent(row.address, {
        type: "mail.delivered",
        data: {
          ...parsed,
          id: row.id,
          direction: row.direction,
          receivedAt: row.createdAt.toISOString(),
        },
      });
    }),
  );

  return {
    dispose() {
      for (const off of unsubscribers) off();
    },
  };
}
