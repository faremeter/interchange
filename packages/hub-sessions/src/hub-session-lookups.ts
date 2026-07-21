// Lookups that the sidecar wire layer issues against host state. These
// are queries (one answer per question) rather than events (broadcast
// notifications), so they live separately from the event emitter.
//
// Each lookup is a stateless DB or repo call. They are gathered into a
// single struct that the hub app passes to `createSidecarRouter` as
// `lookups`.

import { eq, and, isNull } from "drizzle-orm";
import type { DB } from "@intx/db";
import { createApprovalStore, createSignalCorrelationStore } from "@intx/db";
import {
  agentInstance,
  sessionMail,
  workflowDeployment,
} from "@intx/db/schema";
import { getLogger } from "@intx/log";
import { parseAgentAddress, signalName } from "@intx/types";
import {
  deriveWorkflowRunRepoId,
  isWorkflowDerivedAddress,
} from "@intx/workflow-deploy";

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

  const signalCorrelationStore = createSignalCorrelationStore(db);
  const approvalStore = createApprovalStore(db);

  return {
    async lookupPublicKey(agentAddress) {
      // Route by address space, not a blind two-table fallback: a
      // workflow-derived address's key lives on its workflow_deployment
      // row, a launched agent's on its agent_instance row, and the two
      // spaces are disjoint. Routing (rather than falling back) means a
      // launched agent that is missing its instance row returns null and
      // fails its challenge visibly, instead of silently resolving against
      // the wrong table.
      if (isWorkflowDerivedAddress(agentAddress)) {
        // Filter to a live ("deployed") deployment so a torn-down
        // deployment's key can no longer satisfy a challenge. A null
        // publicKey (deployed but not yet acked, or pre-migration) or an
        // absent row returns null -- the challenge fails closed and the
        // address stays unrouted rather than routing without ownership
        // proof.
        const row = await db
          .select({ publicKey: workflowDeployment.publicKey })
          .from(workflowDeployment)
          .where(
            and(
              eq(workflowDeployment.address, agentAddress),
              eq(workflowDeployment.status, "deployed"),
            ),
          )
          .limit(1)
          .then((rows) => rows[0]);
        return row?.publicKey ?? null;
      }
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
      return row?.publicKey ?? null;
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
          const row = await findInstance(db, addr);
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

    async registerSignalCorrelation({
      correlationId,
      runId,
      deploymentId,
      agentAddress,
      kind,
      approvalSnapshot,
    }) {
      // Resolve tenancy and co-write both rows in one transaction so a resolver
      // never sees a correlation without its approval or vice versa. Both
      // inserts are idempotent on their dedup key (the signal_correlation
      // primary key and the approval's unique correlationId), so a redelivered
      // frame -- sidecar reconnect, workflow-log replay, supervisor restart
      // re-emitting -- is a no-op rather than a unique-violation. `timeoutAt` is
      // null: an agent-step suspend holds indefinitely (`parkOnSignal` is called
      // with no timeout), so no deadline reaches this co-write.
      await db.transaction(async (tx) => {
        // Resolve tenancy from the workflow deployment the address names. The
        // deployment is the origin of every approval (an approval has no
        // agent_instance/agent/principal referent), so its row is the only place
        // the tenant is recorded. Filter to a live ("deployed") deployment,
        // symmetrically with `lookupPublicKey`: a torn-down deployment must not
        // seed a routing row that can never be resolved. The lookup keys off
        // `address` rather than `deploymentId` because `address` is the field
        // the wire layer's ownership gate already authorized; the frame's
        // `deploymentId` is the workflow-run repo slug the supervisor derives
        // from the address (`deriveWorkflowRunRepoId`), not the deployment's raw
        // row id, so it is cross-checked against the slug re-derived from the
        // resolved row's address (which equals `agentAddress`) rather than
        // against `deployment.id`. A mismatch fails loud instead of silently
        // writing an inconsistent pair. The FK columns still take the raw row id
        // (`deployment.id`), which is what `signal_correlation.deployment_id` and
        // `approval.deployment_id` reference.
        //
        // The resolution takes a `FOR UPDATE` row lock and runs inside the
        // co-write transaction, so the "deployed" check and the inserts are
        // atomic against a concurrent deployment teardown. A teardown that flips
        // the row off "deployed" either commits after this lock is taken (its
        // flip cannot retroactively orphan a pair written under the lock) or
        // locks the row first, in which case this select waits, then re-checks
        // the committed row, finds it no longer deployed, and throws. The lock
        // order is workflow_deployment before signal_correlation and approval;
        // a teardown path must take the deployment lock before touching those
        // rows to keep the ordering acyclic.
        const deployment = await tx
          .select({
            id: workflowDeployment.id,
            tenantId: workflowDeployment.tenantId,
          })
          .from(workflowDeployment)
          .where(
            and(
              eq(workflowDeployment.address, agentAddress),
              eq(workflowDeployment.status, "deployed"),
            ),
          )
          .for("update")
          .limit(1)
          .then((rows) => rows[0]);
        if (deployment === undefined) {
          throw new Error(
            `No deployed workflow deployment for address "${agentAddress}"; cannot register signal correlation ${correlationId}`,
          );
        }
        const addressSlug = deriveWorkflowRunRepoId(agentAddress);
        if (addressSlug !== deploymentId) {
          throw new Error(
            `Deployment id mismatch registering signal correlation ${correlationId}: frame claims "${deploymentId}" but address "${agentAddress}" derives the workflow-run repo slug "${addressSlug}"`,
          );
        }
        const tenantId = deployment.tenantId;

        await signalCorrelationStore.registerIfAbsent(
          {
            correlationId,
            tenantId,
            deploymentId: deployment.id,
            agentAddress,
            runId,
            signalName: signalName(correlationId),
            kind,
          },
          tx,
        );
        await approvalStore.createIfAbsent(
          {
            id: generateId("approval"),
            tenantId,
            deploymentId: deployment.id,
            runId,
            agentAddress,
            correlationId,
            status: "pending",
            // The register frame guarantees the snapshot (the ask rail is its
            // only producer), so the approver-facing columns are always
            // populated -- never null on this path.
            toolDefinition: {
              name: approvalSnapshot.name,
              description: approvalSnapshot.description,
              inputSchema: approvalSnapshot.inputSchema,
            },
            toolArguments: approvalSnapshot.arguments,
            scope: null,
            timeoutAt: null,
          },
          tx,
        );
      });
    },

    async receiveAgentStatePack(repoId, pack, ref, commitSha) {
      if (repoId.kind !== "agent-state") {
        throw new Error(
          `hub-session lookups receiveAgentStatePack received unsupported repo kind ${JSON.stringify(repoId.kind)}`,
        );
      }
      const agentAddress = repoId.id;
      const agentId = parseAgentId(agentAddress);
      try {
        await agentRepoStore.receiveAgentStatePack(
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
        // Any other failure from the repo subsystem reaches the
        // WebSocket handler as an unhandled rejection unless we catch
        // it here. Transient failures during receivePack (the agent
        // directory being torn down concurrently with an in-flight
        // pack write, filesystem errors mid-rename, etc.) are
        // recoverable from the sender's perspective — the sender can
        // re-push. Surface every such failure as a structured pack
        // rejection (`corrupt` is the closest existing reason — from
        // the sender's perspective the pack failed to index) and log
        // the underlying error so the cause stays traceable on the
        // hub side.
        logger.error`State pack receive failed for ${agentAddress}: ${msg}`;
        return { accepted: false, reason: "corrupt" as const };
      }
      return { accepted: true };
    },

    async receiveWorkflowRunPack(repoId, pack, ref, commitSha) {
      if (repoId.kind !== "workflow-run") {
        throw new Error(
          `hub-session lookups receiveWorkflowRunPack received unsupported repo kind ${JSON.stringify(repoId.kind)}`,
        );
      }
      const deploymentId = repoId.id;
      try {
        await agentRepoStore.receiveWorkflowRunPack(
          { kind: "workflow-run", id: deploymentId },
          pack,
          ref,
          commitSha,
        );
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (msg.startsWith("path_violation")) {
          logger.warn`Workflow-run pack rejected for ${deploymentId}: ${msg}`;
          return { accepted: false, reason: "path_violation" as const };
        }
        // Mirror the agent-state branch's catch-all: any other failure
        // from the repo subsystem (filesystem races, kind-handler
        // diagnostics surfaced as Error messages, etc.) becomes a
        // structured `corrupt` rejection so the sender can re-push,
        // and the underlying error is logged so the cause stays
        // traceable on the hub side.
        logger.error`Workflow-run pack receive failed for ${deploymentId}: ${msg}`;
        return { accepted: false, reason: "corrupt" as const };
      }
      return { accepted: true };
    },
  };
}

/**
 * Extract the instance id from an `<instanceId>@<domain>` agent address.
 * Throws on any input the `@intx/types`-owned `parseAgentAddress`
 * rejects: missing or leading `@`, empty domain, or an instance id
 * without the canonical `ins_` prefix.
 */
export function parseAgentId(agentAddress: string): string {
  const parsed = parseAgentAddress(agentAddress);
  if (parsed === null) {
    throw new Error(`Invalid agent address: "${agentAddress}"`);
  }
  return parsed.instanceId;
}

export async function findInstance(
  db: DB["db"],
  agentAddress: string,
): Promise<Awaited<ReturnType<typeof db.query.agentInstance.findFirst>>> {
  return db.query.agentInstance.findFirst({
    where: and(
      eq(agentInstance.address, agentAddress),
      isNull(agentInstance.endedAt),
    ),
  });
}

export async function requireInstance(
  db: DB["db"],
  agentAddress: string,
): Promise<
  NonNullable<Awaited<ReturnType<typeof db.query.agentInstance.findFirst>>>
> {
  const row = await findInstance(db, agentAddress);
  if (!row) {
    throw new Error(`No active instance found for address "${agentAddress}"`);
  }
  return row;
}
