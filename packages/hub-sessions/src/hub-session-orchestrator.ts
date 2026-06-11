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

import { and, desc, eq, isNull, sql } from "drizzle-orm";
import { type } from "arktype";
import type { DB } from "@intx/db";
import { agentInstance, inferenceTurn, turnPart } from "@intx/db/schema";
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
  ): Promise<void>;
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
        // If the agent was mid-turn when the link dropped, the turn row is
        // still 'running'. Adopt it so the resumed inference.done / step-finish
        // parts land in that turn instead of being dropped for having no active
        // turn — the live reply survives the reconnect (CL-1654).
        const resumeTurn = await findOpenTurn(db, sessionId);
        eventCollectors.create(
          agentAddress,
          instance.tenantId,
          sessionId,
          instance.id,
          resumeTurn,
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

// A turn this old at reconnect is almost certainly orphaned — a harness that
// crashed mid-turn leaves the row 'running' forever (nothing finalizes it on
// the crash path), and `agent.reconnected` proves only that the sidecar owns
// the key, not that the inference is still alive. Adopting such a turn would
// re-adopt the same zombie on every future reconnect, so bound adoption to a
// turn that could plausibly still be streaming. Real in-flight turns at a hub
// redeploy are seconds old; this leaves generous headroom for long inferences.
const MAX_RESUMABLE_TURN_AGE_MS = 15 * 60 * 1000;

/**
 * Find the session's still-open inference turn, if any, so a collector rebuilt
 * on reconnect can resume it rather than dropping the trailing parts of a turn
 * that was in flight when the link dropped.
 *
 * Best-effort, not a guarantee: it preserves the tail only when the sidecar
 * actually redelivers it on reconnect (frames emitted to the dead socket, or
 * dropped under the hub-link send-queue cap, are gone regardless). It also
 * skips turns older than MAX_RESUMABLE_TURN_AGE_MS so a crashed-harness zombie
 * is not adopted indefinitely. Returns the turn id and the next ordinal (one
 * past the highest part already written) so the part sequence stays monotonic;
 * the caller is the sole writer to that turn, so the read-then-write is safe.
 * Returns undefined when no adoptable running turn exists.
 */
async function findOpenTurn(
  db: DB["db"],
  sessionId: string,
): Promise<{ id: string; nextOrdinal: number } | undefined> {
  const [openTurn] = await db
    .select({ id: inferenceTurn.id, startedAt: inferenceTurn.startedAt })
    .from(inferenceTurn)
    .where(
      and(
        eq(inferenceTurn.sessionId, sessionId),
        eq(inferenceTurn.status, "running"),
        isNull(inferenceTurn.endedAt),
      ),
    )
    .orderBy(desc(inferenceTurn.startedAt), desc(inferenceTurn.id))
    .limit(1);

  if (openTurn === undefined) return undefined;
  if (Date.now() - openTurn.startedAt.getTime() > MAX_RESUMABLE_TURN_AGE_MS) {
    return undefined;
  }

  const [maxRow] = await db
    .select({ max: sql<number | null>`max(${turnPart.ordinal})` })
    .from(turnPart)
    .where(eq(turnPart.turnId, openTurn.id));

  return { id: openTurn.id, nextOrdinal: (maxRow?.max ?? -1) + 1 };
}
