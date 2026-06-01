// Lookups that the sidecar wire layer issues against host state. These
// are queries (one answer per question) rather than events (broadcast
// notifications), so they live separately from the event emitter.
//
// Each lookup is a stateless DB or repo call. They are gathered into a
// single struct that the hub app passes to `createSidecarRouter` as
// `lookups`.

import { eq, and, isNull } from "drizzle-orm";
import type { DB } from "@intx/db";
import { agentInstance, sessionMail } from "@intx/db/schema";
import { getLogger } from "@intx/log";

import type { AgentRepoStore } from "./agent-repo";
import { generateId } from "@intx/hub-common";
import type { SidecarLookups } from "./ws/sidecar-events";

const logger = getLogger(["hub", "lookups"]);

export type HubSessionLookupsDeps = {
  db: DB["db"];
  agentRepoStore: AgentRepoStore;
};

export function createHubSessionLookups(
  deps: HubSessionLookupsDeps,
): Required<SidecarLookups> {
  const { db, agentRepoStore } = deps;

  return {
    async lookupPublicKey(agentAddress) {
      const row = await db
        .select({ publicKey: agentInstance.publicKey })
        .from(agentInstance)
        .where(
          and(
            eq(agentInstance.address, agentAddress),
            isNull(agentInstance.endedAt),
          ),
        )
        .limit(1)
        .then((rows) => rows[0]);
      if (!row) {
        return null;
      }
      return row.publicKey;
    },

    async lookupDeployRef(agentAddress) {
      const agentId = parseAgentId(agentAddress);
      return agentRepoStore.getDeployRef(agentId);
    },

    async persistMail({ senderAddress, recipients, raw }) {
      const senderInstance = await requireInstance(db, senderAddress);
      if (!senderInstance.sessionId) {
        throw new Error(
          `Instance ${senderInstance.id} has no session for address "${senderAddress}"`,
        );
      }
      const createdAt = new Date();

      // Outbound record on the sender's session.
      const outboundId = generateId("sessionMail");
      const outboundRecord = {
        id: outboundId,
        sessionId: senderInstance.sessionId,
        instanceId: senderInstance.id,
        tenantId: senderInstance.tenantId,
        direction: "outbound" as const,
        status: "delivered" as const,
        raw,
        createdAt,
      };

      // Inbound records for each recipient that has an active agent
      // instance. Recipients that are not agent instances (e.g. human
      // user addresses) are skipped.
      const recipientResults = await Promise.all(
        recipients.map(async (addr) => {
          const row = await db.query.agentInstance.findFirst({
            where: and(
              eq(agentInstance.address, addr),
              isNull(agentInstance.endedAt),
            ),
          });
          if (row === undefined) {
            return null;
          }
          if (row.sessionId === null) {
            logger.warn`Active instance ${row.id} for "${addr}" has no session; skipping inbound record`;
            return null;
          }
          return { addr, instance: row, sessionId: row.sessionId };
        }),
      );
      const recipientInstances = recipientResults.filter(
        (r): r is NonNullable<typeof r> => r !== null,
      );

      const inboundEntries = recipientInstances.map(
        ({ addr, instance, sessionId }) => {
          const id = generateId("sessionMail");
          return {
            record: {
              id,
              sessionId,
              instanceId: instance.id,
              tenantId: instance.tenantId,
              direction: "inbound" as const,
              status: "delivered" as const,
              raw,
              createdAt,
            },
            result: {
              id,
              direction: "inbound" as const,
              instanceId: instance.id,
              address: addr,
              createdAt,
            },
          };
        },
      );

      await db
        .insert(sessionMail)
        .values([outboundRecord, ...inboundEntries.map((e) => e.record)]);

      return [
        {
          id: outboundId,
          direction: "outbound" as const,
          instanceId: senderInstance.id,
          address: senderInstance.address,
          createdAt,
        },
        ...inboundEntries.map((e) => e.result),
      ];
    },

    async receiveStatePack(repoId, pack, ref, commitSha) {
      if (repoId.kind !== "agent-state") {
        throw new Error(
          `hub-session lookups received unsupported repo kind ${JSON.stringify(repoId.kind)}`,
        );
      }
      const agentAddress = repoId.id;
      const agentId = parseAgentId(agentAddress);
      try {
        await agentRepoStore.receiveStatePack(
          { kind: "agent-state", id: agentId },
          pack,
          ref,
          commitSha,
        );
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (msg.startsWith("path_violation")) {
          logger.warn`State pack rejected for ${agentAddress}: ${msg}`;
          return { accepted: false, reason: "path_violation" as const };
        }
        throw err;
      }
      return { accepted: true };
    },
  };
}

export function parseAgentId(agentAddress: string): string {
  const atIdx = agentAddress.indexOf("@");
  if (atIdx === -1) {
    throw new Error(`Invalid agent address: "${agentAddress}"`);
  }
  return agentAddress.substring(0, atIdx);
}

export async function requireInstance(
  db: DB["db"],
  agentAddress: string,
): Promise<
  NonNullable<Awaited<ReturnType<typeof db.query.agentInstance.findFirst>>>
> {
  const row = await db.query.agentInstance.findFirst({
    where: and(
      eq(agentInstance.address, agentAddress),
      isNull(agentInstance.endedAt),
    ),
  });
  if (!row) {
    throw new Error(`No active instance found for address "${agentAddress}"`);
  }
  return row;
}
