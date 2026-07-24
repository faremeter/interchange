// Lookups that the sidecar wire layer issues against host state. These
// are queries (one answer per question) rather than events (broadcast
// notifications), so they live separately from the event emitter.
//
// Each lookup is a stateless DB or repo call. They are gathered into a
// single struct that the hub app passes to `createSidecarRouter` as
// `lookups`.

import { eq, and, isNull } from "drizzle-orm";
import type { DB } from "@intx/db";
import {
  createApprovalStore,
  createSignalCorrelationStore,
  createWorkflowRunStore,
} from "@intx/db";
import {
  agentInstance,
  agentSession,
  principal,
  sessionMail,
  workflowDeployment,
  workflowRun,
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
): Required<Omit<SidecarLookups, "materializeMailTriggeredRunGrants">> {
  const { db, agentRepoStore } = deps;

  const signalCorrelationStore = createSignalCorrelationStore(db);
  const approvalStore = createApprovalStore(db);
  const workflowRunStore = createWorkflowRunStore(db);

  return {
    async lookupPublicKey(agentAddress) {
      // Route by address space. A workflow-derived address's key lives on
      // its workflow_deployment row; a plain `ins_<hex>` address is backed by
      // either a launched agent_instance or a folded workflow_run -- the two
      // share that address space under the fold -- and `resolveRoutableAddress`
      // resolves both together. A missing endpoint (or a null key) returns
      // null so the reconnect challenge fails closed and the address stays
      // unrouted rather than routing without ownership proof.
      if (isWorkflowDerivedAddress(agentAddress)) {
        // Filter to a live ("deployed") deployment so a torn-down
        // deployment's key can no longer satisfy a challenge. A null
        // publicKey (deployed but not yet acked, or pre-migration) or an
        // absent row returns null.
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
      const endpoint = await resolveRoutableAddress(db, agentAddress);
      return endpoint?.publicKey ?? null;
    },

    async lookupDeployRef(agentAddress) {
      const agentId = parseAgentId(agentAddress);
      return agentRepoStore.getDeployRef(agentId);
    },

    async persistMail({ senderAddress, recipients, raw }) {
      // The sender and recipients are plain addresses backed by either a
      // launched agent_instance or a folded workflow_run; resolve each through
      // the fold-aware resolver. A mail record's `instanceId` is the agent
      // instance's id, or null when the endpoint is a folded run (which is not
      // an instance) -- the record still anchors on its session either way.
      const sender = await resolveRoutableAddress(db, senderAddress);
      if (sender === undefined) {
        throw new Error(
          `No active endpoint found for sender address "${senderAddress}"`,
        );
      }
      if (sender.sessionId === null) {
        throw new Error(
          `Endpoint ${sender.id} has no session for address "${senderAddress}"`,
        );
      }
      const createdAt = new Date();
      const senderInstanceId = sender.kind === "instance" ? sender.id : null;

      // Outbound record on the sender's session.
      const outboundId = generateId("sessionMail");
      const outboundRecord = {
        id: outboundId,
        sessionId: sender.sessionId,
        instanceId: senderInstanceId,
        tenantId: sender.tenantId,
        direction: "outbound" as const,
        status: "delivered" as const,
        raw,
        createdAt,
      };

      // Inbound records for each recipient that is a live endpoint.
      // Recipients that are not (e.g. human user addresses) are skipped.
      const recipientResults = await Promise.all(
        recipients.map(async (addr) => {
          const endpoint = await resolveRoutableAddress(db, addr);
          if (endpoint === undefined) {
            return null;
          }
          if (endpoint.sessionId === null) {
            logger.warn`Active endpoint ${endpoint.id} for "${addr}" has no session; skipping inbound record`;
            return null;
          }
          return { addr, endpoint, sessionId: endpoint.sessionId };
        }),
      );
      const recipientEndpoints = recipientResults.filter(
        (r): r is NonNullable<typeof r> => r !== null,
      );

      const inboundEntries = recipientEndpoints.map(
        ({ addr, endpoint, sessionId }) => {
          const id = generateId("sessionMail");
          const instanceId = endpoint.kind === "instance" ? endpoint.id : null;
          return {
            record: {
              id,
              sessionId,
              instanceId,
              tenantId: endpoint.tenantId,
              direction: "inbound" as const,
              status: "delivered" as const,
              raw,
              createdAt,
            },
            result: {
              id,
              direction: "inbound" as const,
              instanceId,
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
          instanceId: senderInstanceId,
          address: sender.address,
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

        // Lazily anchor the run before its correlation and approval reference
        // it. A workflow-spawned internal run never crosses the external
        // trigger route that mints a run principal, so its run row would
        // otherwise not exist; ensure it here so the co-written rows have a
        // referent. The principal is null: an internal run inherits its
        // deployment's grants and has no principal of its own. The insert is
        // idempotent on the run id, so a redelivered register frame -- the same
        // redelivery the co-writes below tolerate -- is a no-op.
        await workflowRunStore.createIfAbsent(
          {
            id: runId,
            deploymentId: deployment.id,
            tenantId,
            principalId: null,
            status: "running",
          },
          tx,
        );

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
      let newlyTerminalRuns;
      try {
        newlyTerminalRuns = await agentRepoStore.receiveWorkflowRunPack(
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

      // The substrate has already durably advanced the git ref by the time it
      // returns, so the pack is accepted regardless of what happens below. The
      // per-run status flip and principal deactivation are a best-effort
      // downstream side effect of that durable advance, not part of accepting
      // the pack. A failure here leaves the run "running" in the DB with its
      // principal still active; there is no automatic re-fire, because a
      // redelivery of the same durable tip produces no newly-terminal signal
      // (the substrate's per-commit walk short-circuits on an already-present
      // tip). The failure is therefore logged at ERROR as the only record that
      // the row needs a manual flip, and the pack verdict stays accepted so the
      // sidecar is acked and does not wedge re-pushing a pack that already
      // landed.
      const now = new Date();
      for (const { runId, status } of newlyTerminalRuns) {
        try {
          await db.transaction(async (tx) => {
            const won = await workflowRunStore.markTerminal(
              runId,
              status,
              now,
              tx,
            );
            if (won === null) {
              // No running row matched. Either the run is already terminal (a
              // benign replay against an already-settled row) or no row exists
              // at all -- the run reached a terminal event before its anchor
              // committed, so its terminal state has nowhere to land. Only the
              // second case is a defect; distinguish them and log the missing
              // anchor loudly rather than silently treating both as done.
              const [existing] = await tx
                .select({ id: workflowRun.id })
                .from(workflowRun)
                .where(eq(workflowRun.id, runId));
              if (existing === undefined) {
                logger.error`Terminal event for run ${runId} (deployment ${deploymentId}, target status ${status}) has no workflow_run row; the run terminated before its anchor committed`;
              }
              return;
            }
            // Deactivate the run's own principal, if it has one. Externally-
            // triggered runs carry a principal; internal, workflow-spawned runs
            // have `principalId = null` and inherit the deployment's grants, so
            // there is nothing to deactivate. Deactivation is gated on winning
            // the flip -- the single claim point -- not on the principal's own
            // status.
            if (won.principalId !== null) {
              await tx
                .update(principal)
                .set({ status: "deactivated", updatedAt: now })
                // The `refId` clause is a defensive mirror of the per-instance
                // teardown in instances.ts: `won.principalId` is already this
                // run's own principal, and `principal.id` is the primary key,
                // so the `refId` match is belt-and-suspenders that the id we
                // won belongs to this run.
                .where(
                  and(
                    eq(principal.id, won.principalId),
                    eq(principal.refId, runId),
                  ),
                );
            }
          });
        } catch (err) {
          // Per-run isolation: a failed flip for one run must not abort the
          // rest of the batch, and must not throw out of this method -- a throw
          // would leave the sidecar with neither an ack nor a reject for a pack
          // the substrate already accepted. This ERROR is the only signal that
          // the run is stuck "running" in the DB with its principal active, so
          // it carries enough to find and flip the row by hand.
          const msg = err instanceof Error ? err.message : String(err);
          logger.error`Terminal DB flip failed for run ${runId} (deployment ${deploymentId}, target status ${status}); run left running in the DB: ${msg}`;
        }
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

/**
 * A live routing endpoint backing a plain `ins_<hex>` agent address, normalized
 * across the agent-instance -> workflow-run fold. Both a legacy launched
 * instance (`agent_instance`) and a folded run (`workflow_run`) share that
 * address space, so callers work against this shape rather than a specific
 * table.
 */
export interface RoutableEndpoint {
  readonly kind: "instance" | "run";
  readonly id: string;
  readonly tenantId: string;
  readonly address: string;
  readonly publicKey: string | null;
  /**
   * The live session backing this endpoint. An instance carries its own
   * `sessionId`; a folded run has no session column, so this is the run's
   * not-yet-ended `agent_session`, keyed by the run's principal. Transitional
   * -- it retires when mail record-keeping moves off `agent_session`.
   */
  readonly sessionId: string | null;
}

/**
 * Resolve a plain (non-workflow-derived) agent address to the routing endpoint
 * backing it. Under the fold a plain `ins_<hex>` address is prefix-
 * indistinguishable between a legacy `agent_instance` and a folded
 * `workflow_run`, so this queries BOTH tables. A given launch writes exactly
 * one, so at most one matches; a match in both is a corruption signal and
 * throws rather than silently picking a table (the two ids are drawn from the
 * same random space, so a real collision cannot happen by construction).
 * Callers must NOT reintroduce a blind single-table fallback.
 */
export async function resolveRoutableAddress(
  db: DB["db"],
  address: string,
): Promise<RoutableEndpoint | undefined> {
  const [instanceRows, runRows] = await Promise.all([
    db
      .select({
        id: agentInstance.id,
        tenantId: agentInstance.tenantId,
        publicKey: agentInstance.publicKey,
        sessionId: agentInstance.sessionId,
      })
      .from(agentInstance)
      .where(
        and(eq(agentInstance.address, address), isNull(agentInstance.endedAt)),
      )
      .limit(1),
    db
      .select({
        id: workflowRun.id,
        tenantId: workflowRun.tenantId,
        publicKey: workflowRun.publicKey,
        principalId: workflowRun.principalId,
      })
      .from(workflowRun)
      .where(and(eq(workflowRun.address, address), isNull(workflowRun.endedAt)))
      .limit(1),
  ]);

  const instanceRow = instanceRows[0];
  const runRow = runRows[0];

  if (instanceRow !== undefined && runRow !== undefined) {
    throw new Error(
      `address "${address}" resolves to both an agent instance (${instanceRow.id}) ` +
        `and a workflow run (${runRow.id}); refusing to route an ambiguous address`,
    );
  }

  if (instanceRow !== undefined) {
    return {
      kind: "instance",
      id: instanceRow.id,
      tenantId: instanceRow.tenantId,
      address,
      publicKey: instanceRow.publicKey,
      sessionId: instanceRow.sessionId,
    };
  }

  if (runRow !== undefined) {
    return {
      kind: "run",
      id: runRow.id,
      tenantId: runRow.tenantId,
      address,
      publicKey: runRow.publicKey,
      sessionId: await resolveRunSessionId(db, runRow.principalId),
    };
  }

  return undefined;
}

/**
 * A folded run has no session column; its live session is the not-yet-ended
 * `agent_session` keyed by the run's principal. Returns null when the run has no
 * principal or no live session. Transitional, alongside
 * `RoutableEndpoint.sessionId`.
 */
async function resolveRunSessionId(
  db: DB["db"],
  principalId: string | null,
): Promise<string | null> {
  if (principalId === null) {
    return null;
  }
  const row = await db
    .select({ id: agentSession.id })
    .from(agentSession)
    .where(
      and(
        eq(agentSession.principalId, principalId),
        isNull(agentSession.endedAt),
      ),
    )
    .limit(1)
    .then((rows) => rows[0]);
  return row?.id ?? null;
}
