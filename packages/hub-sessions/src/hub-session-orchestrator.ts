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
import { isWorkflowDerivedAddress } from "@intx/workflow-deploy";

import type { AgentRepoStore } from "./agent-repo";
import type { EventCollectorRegistry } from "./event-collector-registry";
import type { SidecarEventEmitter } from "./ws/sidecar-events";
import { parseAgentId, resolveRoutableAddress } from "./hub-session-lookups";

const log = getLogger(["hub", "orchestrator"]);

/** Subset of `SidecarRouter` the orchestrator drives outbound. The
 * narrow surface keeps tests honest and decouples the orchestrator
 * from the rest of the router API. */
export type HubSessionRouterFacade = {
  sendPack(
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
  ): Promise<void>;
  dispatchAgentEvent(agentAddress: string, event: unknown): void;
};

export type HubSessionOrchestratorDeps = {
  events: SidecarEventEmitter;
  router: HubSessionRouterFacade;
  db: DB["db"];
  eventCollectors: EventCollectorRegistry;
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
  const { events, router, db, eventCollectors, agentRepoStore } = deps;

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
    events.on("agent.deploy.ack", async ({ agentAddress, publicKey }) => {
      // Workflow-derived addresses (the deployment-level
      // `ins_<deploymentId>@<domain>` and the per-step
      // `ins_<deploymentId>-<stepId>@<domain>`) have no agent_instance row;
      // their public key lives on the deployment's anchor workflow_run row,
      // keyed by address. Persist it there so the reconnect ownership
      // challenge can verify the deployment address off the same row
      // `lookupPublicKey` reads. Only the deployment-level address has an
      // anchor run, so a stray per-step ack updates nothing.
      if (isWorkflowDerivedAddress(agentAddress)) {
        await db
          .update(workflowRun)
          .set({ publicKey })
          .where(eq(workflowRun.address, agentAddress));
        return;
      }
      // A plain address is backed by a folded workflow_run; persist the acked
      // key on it. A missing endpoint is a bug to surface, not to drop.
      const endpoint = await resolveRoutableAddress(db, agentAddress);
      if (endpoint === undefined) {
        throw new Error(
          `No active endpoint found for deploy ack on address "${agentAddress}"`,
        );
      }
      await db
        .update(workflowRun)
        .set({ publicKey })
        .where(eq(workflowRun.id, endpoint.id));
    }),
  );

  unsubscribers.push(
    events.on("agent.reconnected", async ({ agentAddress }) => {
      // A plain address is backed by a folded workflow_run; resolve it. A
      // missing endpoint is a bug to surface, not to drop.
      const endpoint = await resolveRoutableAddress(db, agentAddress);
      if (endpoint === undefined) {
        throw new Error(
          `No active endpoint found for reconnect on address "${agentAddress}"`,
        );
      }
      // A leaked or terminal folded run is deliberately kept routable (terminal
      // status, null endedAt) so it stays reachable to inspect or clean up. A
      // run resolves its session live-only, so once that session has ended it
      // has no session to collect into. Keep the address routable (return
      // rather than throw, which would roll the just-verified address back out
      // of routing) and restore no collector; no status flip either, since a
      // folded run is born running and a failed -> running flip would claim a
      // running session that does not exist.
      if (endpoint.kind === "run" && endpoint.status !== "running") {
        return;
      }
      if (endpoint.sessionId === null) {
        throw new Error(
          `Agent "${agentAddress}" reconnected but has no active session`,
        );
      }
      const sessionId = endpoint.sessionId;

      // A supervised deployment carries its grants and sources in the
      // deploy pack and refreshes them over the supervisor's IPC
      // credentials snapshot at spawn and recycle, so reconnect does not
      // re-push them over the wire.

      // Restore the inference-turn collector. It records turns under endpoint.id
      // -- a folded run id -- which inference_turn.instanceId stores without a
      // foreign key.
      if (!eventCollectors.has(agentAddress)) {
        eventCollectors.create(
          agentAddress,
          endpoint.tenantId,
          sessionId,
          endpoint.id,
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
