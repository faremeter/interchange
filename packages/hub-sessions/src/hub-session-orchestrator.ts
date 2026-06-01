// Hub-side orchestrator for the sidecar event emitter.
//
// Subscribes to the sidecar router's events and runs the host-side
// reactions: dispatching inference events to the event collector,
// refreshing grants and credentials on reconnect, ingesting state
// packs, re-deploying stale agents, persisting mail, and forwarding
// mail.delivered notifications back to subscribers.
//
// The orchestrator depends on a narrow `HubSessionRouterFacade` rather
// than the full SidecarRouter, so tests can drive subscriber behavior
// with a small stub and an isolated emitter.

import { eq } from "drizzle-orm";
import { type } from "arktype";
import type { DB } from "@intx/db";
import { agentInstance } from "@intx/db/schema";
import { resolveInstanceSources } from "@intx/db";
import { parseMailToEmail } from "@intx/mime";
import { parseInferenceEvent } from "@intx/types/runtime";
import type { GrantRule, GrantStore } from "@intx/types/authz";
import type { InferenceSource } from "@intx/types/runtime";
import { getLogger } from "@intx/log";

import type { AgentRepoStore } from "./agent-repo";
import type { EventCollectorRegistry } from "./event-collector-registry";
import type { SidecarEventEmitter } from "./ws/sidecar-events";
import { parseAgentId, requireInstance } from "./hub-session-lookups";

const log = getLogger(["hub", "orchestrator"]);

/** Subset of `SidecarRouter` the orchestrator drives outbound. The
 * narrow surface keeps tests honest and decouples the orchestrator
 * from the rest of the router API. */
export type HubSessionRouterFacade = {
  sendGrantsUpdate(agentAddress: string, grants: GrantRule[]): Promise<void>;
  sendSourcesUpdate(
    agentAddress: string,
    sources: InferenceSource[],
    defaultSource: string,
  ): Promise<void>;
  sendPack(
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
  ): Promise<{ assetPackSha: string }>;
  dispatchAgentEvent(agentAddress: string, event: unknown): void;
};

export type HubSessionOrchestratorDeps = {
  events: SidecarEventEmitter;
  router: HubSessionRouterFacade;
  db: DB["db"];
  eventCollectors: EventCollectorRegistry;
  grantStore: GrantStore;
  agentRepoStore: AgentRepoStore;
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
  const { events, router, db, eventCollectors, grantStore, agentRepoStore } =
    deps;

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
    events.on("sidecar.disconnect", ({ agentAddresses }) => {
      for (const addr of agentAddresses) {
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
    events.on("agent.deploy.ack", async ({ agentAddress, publicKey }) => {
      const instance = await requireInstance(db, agentAddress);
      await db
        .update(agentInstance)
        .set({ publicKey })
        .where(eq(agentInstance.id, instance.id));
    }),
  );

  unsubscribers.push(
    events.on("agent.reconnected", async ({ agentAddress }) => {
      const instance = await requireInstance(db, agentAddress);

      if (!instance.sessionId) {
        throw new Error(
          `Agent "${agentAddress}" reconnected but has no active session`,
        );
      }
      const sessionId = instance.sessionId;

      // Refresh grants before creating any local state. If this
      // fails, the address is rejected and nothing needs cleanup.
      const grants = await grantStore.collectGrants(
        instance.principalId,
        instance.tenantId,
      );
      await router.sendGrantsUpdate(agentAddress, grants);

      // Re-resolve and push credentials so the agent picks up any
      // rotations that happened while the sidecar was disconnected.
      // Fail-open: a stale credential causes runtime 401s, not a
      // security escalation, so we log rather than reject the
      // reconnect.
      try {
        const sources = await resolveInstanceSources(
          db,
          instance.tenantId,
          instance,
        );
        if (sources.length > 0) {
          const [first] = sources;
          if (first !== undefined) {
            await router.sendSourcesUpdate(agentAddress, sources, first.id);
          }
        }
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err);
        log.warn(
          "Failed to push credentials on reconnect for {agentAddress}: {msg}",
          { agentAddress, msg },
        );
      }

      const now = new Date();
      if (instance.status !== "running") {
        await db
          .update(agentInstance)
          .set({ status: "running", updatedAt: now })
          .where(eq(agentInstance.id, instance.id));
      }
      if (!eventCollectors.has(agentAddress)) {
        eventCollectors.create(
          agentAddress,
          instance.tenantId,
          sessionId,
          instance.id,
        );
        log.info(
          "Restored event collector for reconnected agent {agentAddress}",
          { agentAddress },
        );
      }
    }),
  );

  unsubscribers.push(
    events.on("deploy.ref.stale", async ({ agentAddress }) => {
      const agentId = parseAgentId(agentAddress);
      const { pack, commitSha, ref } =
        await agentRepoStore.createDeployPack(agentId);
      await router.sendPack(agentAddress, pack, ref, commitSha);
      log.info("Re-deployed stale agent {agentAddress}", { agentAddress });
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
