// Lookups that the sidecar wire layer issues against host state. These
// are queries (one answer per question) rather than events (broadcast
// notifications), so they live separately from the event emitter.
//
// Each lookup is a stateless DB or repo call. They are gathered into a
// single struct that the hub app passes to `createSidecarRouter` as
// `lookups`.

import { eq, and, asc, inArray, isNull } from "drizzle-orm";
import type { DB } from "@intx/db";
import {
  createApprovalStore,
  createSignalCorrelationStore,
  createWorkflowRunDispatchStore,
  createWorkflowRunStore,
} from "@intx/db";
import {
  agentSession,
  liveWorkflowRunStatuses,
  principal,
  sessionMail,
  sidecarAllocation,
  workflowRun,
} from "@intx/db/schema";
import { getLogger } from "@intx/log";
import { parseRunAddress, signalName } from "@intx/types";
import { SignalDeliverFrame } from "@intx/types/sidecar";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import type { AgentRepoStore } from "./agent-repo";
import { generateId } from "@intx/hub-common";
import type { SidecarLookups } from "./ws/sidecar-events";
import {
  listAcceptedWorkflowDispatches,
  listConsumedWorkflowDispatches,
} from "./workflow-dispatch-settlement";
import { readCommittedWorkflowRunLifecycle } from "./workflow-run-kind";

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
  const workflowRunDispatchStore = createWorkflowRunDispatchStore(db);

  return {
    async lookupPublicKey(agentAddress) {
      // Every routable address names one workflow run, whose key lives on its
      // single self-anchored workflow_run row, keyed by address. Read the key
      // off that row, gated on a live run (born "deployed", "running" after its
      // first trigger) so a decommissioned deployment's key can no longer
      // satisfy a challenge. The "deployed" arm is load-bearing: the reconnect
      // ownership challenge fires in the deploy->first-trigger window, so a
      // "running"-only gate would fail every such challenge closed. A missing
      // row or a null publicKey (live but not yet acked) returns null so the
      // reconnect challenge fails closed and the address stays unrouted rather
      // than routing without ownership proof.
      const row = await db
        .select({ publicKey: workflowRun.publicKey })
        .from(workflowRun)
        .where(
          and(
            eq(workflowRun.address, agentAddress),
            inArray(workflowRun.status, [...liveWorkflowRunStatuses]),
          ),
        )
        .limit(1)
        .then((rows) => rows[0]);
      return row?.publicKey ?? null;
    },

    async lookupDeployRef() {
      // A workflow run is a supervised workflow-process pinned forever like a
      // native deployment: it keeps its deploy-time definition and never
      // reconciles, so no address enrolls in the reconnect deploy-ref catch-up.
      return null;
    },

    async persistMail({ senderAddress, recipients, raw }) {
      // The sender and recipients are run addresses, each backed by its
      // self-anchored workflow_run; resolve each through the resolver. A mail
      // record's `runId` is always null for a run -- it keys on the run's
      // session instead.
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
      const senderRunId = null;

      // Outbound record on the sender's session.
      const outboundId = generateId("sessionMail");
      const outboundRecord = {
        id: outboundId,
        sessionId: sender.sessionId,
        runId: senderRunId,
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
          // A folded run is not an instance, so its mail records no runId.
          const runId = null;
          return {
            record: {
              id,
              sessionId,
              runId,
              tenantId: endpoint.tenantId,
              direction: "inbound" as const,
              status: "delivered" as const,
              raw,
              createdAt,
            },
            result: {
              id,
              direction: "inbound" as const,
              runId,
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
          runId: senderRunId,
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
        // Resolve tenancy and the run's definition from the deployment's anchor
        // run -- the workflow_run whose id is the deployment id, which the
        // address names. The anchor is the tenancy origin every approval needs
        // (an approval has no agent_instance/agent/principal referent). The
        // lookup keys off `address` (the field the wire layer's ownership gate
        // authorized), not the frame's `deploymentId`: that is the workflow-run
        // repo slug the supervisor derives from the address
        // (`deriveWorkflowRunRepoId`), cross-checked below against the slug
        // re-derived from `agentAddress` rather than against the row id. A
        // mismatch fails loud instead of silently writing an inconsistent pair.
        // The FK columns take the anchor run's id (= the deployment id), which
        // is what `signal_correlation.deployment_id` and `approval.deployment_id`
        // reference.
        //
        // The resolution takes a `FOR UPDATE` row lock and runs inside the
        // co-write transaction, gated on a live anchor run ("deployed" or
        // "running"), so the liveness check and the inserts are atomic against a
        // concurrent teardown that flips the anchor run terminal. The lock order
        // is workflow_run before signal_correlation and approval; a teardown
        // path must take the anchor-run lock before touching those rows to keep
        // the ordering acyclic.
        const anchor = await tx
          .select({
            id: workflowRun.id,
            tenantId: workflowRun.tenantId,
            definitionId: workflowRun.definitionId,
          })
          .from(workflowRun)
          .where(
            and(
              eq(workflowRun.address, agentAddress),
              inArray(workflowRun.status, [...liveWorkflowRunStatuses]),
            ),
          )
          .for("update")
          .limit(1)
          .then((rows) => rows[0]);
        if (anchor === undefined) {
          throw new Error(
            `No live workflow run for address "${agentAddress}"; cannot register signal correlation ${correlationId}`,
          );
        }
        const addressSlug = deriveWorkflowRunRepoId(agentAddress);
        if (addressSlug !== deploymentId) {
          throw new Error(
            `Deployment id mismatch registering signal correlation ${correlationId}: frame claims "${deploymentId}" but address "${agentAddress}" derives the workflow-run repo slug "${addressSlug}"`,
          );
        }
        const tenantId = anchor.tenantId;
        const definitionId = anchor.definitionId;

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
            anchorRunId: anchor.id,
            definitionId,
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
            deploymentId: anchor.id,
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
            deploymentId: anchor.id,
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

    async receiveWorkflowRunPack(repoId, pack, ref, commitSha, source) {
      if (repoId.kind !== "workflow-run") {
        throw new Error(
          `hub-session lookups receiveWorkflowRunPack received unsupported repo kind ${JSON.stringify(repoId.kind)}`,
        );
      }
      const workflowRunRepoId = repoId.id;
      if (deriveWorkflowRunRepoId(source.agentAddress) !== workflowRunRepoId) {
        logger.warn`Workflow-run pack rejected for ${workflowRunRepoId}: source address does not own the repository`;
        return { accepted: false, reason: "path_violation" as const };
      }
      const [anchor] = await db
        .select({
          id: workflowRun.id,
          address: workflowRun.address,
          anchorRunId: workflowRun.anchorRunId,
        })
        .from(workflowRun)
        .where(
          and(
            eq(workflowRun.address, source.agentAddress),
            inArray(workflowRun.status, [...liveWorkflowRunStatuses]),
          ),
        )
        .limit(1);
      if (
        anchor === undefined ||
        anchor.anchorRunId !== anchor.id ||
        anchor.address === null
      ) {
        logger.warn`Workflow-run pack rejected for ${workflowRunRepoId}: source address has no live deployment anchor`;
        return { accepted: false, reason: "path_violation" as const };
      }
      const anchorAddress = anchor.address;
      let newlyTerminalRuns;
      try {
        if (source.kind === "allocated") {
          newlyTerminalRuns = await db.transaction(async (tx) => {
            const [allocation] = await tx
              .select()
              .from(sidecarAllocation)
              .where(eq(sidecarAllocation.anchorRunId, anchor.id))
              .limit(1)
              .for("update");
            if (
              allocation === undefined ||
              allocation.id !== source.allocationId ||
              allocation.anchorRunId !== source.anchorRunId ||
              source.anchorRunId !== anchor.id ||
              allocation.status !== "allocated" ||
              allocation.generation !== source.generation ||
              allocation.ensureAcceptedGeneration !== source.generation
            ) {
              return null;
            }

            // Replacement advances this same row. Keep its lock until the
            // repository ref has advanced so ownership cannot change after
            // validation but before the old worker's pack becomes
            // authoritative.
            return agentRepoStore.receiveWorkflowRunPack(
              { kind: "workflow-run", id: workflowRunRepoId },
              pack,
              ref,
              commitSha,
            );
          });
          if (newlyTerminalRuns === null) {
            logger.warn`Workflow-run pack rejected for ${workflowRunRepoId}: source connection does not own the deployment's current allocation`;
            return { accepted: false, reason: "path_violation" as const };
          }
        } else {
          const allocation = await db.query.sidecarAllocation.findFirst({
            where: eq(sidecarAllocation.anchorRunId, anchor.id),
          });
          if (allocation !== undefined) {
            logger.warn`Workflow-run pack rejected for ${workflowRunRepoId}: source connection does not own the deployment's current allocation`;
            return { accepted: false, reason: "path_violation" as const };
          }
          newlyTerminalRuns = await agentRepoStore.receiveWorkflowRunPack(
            { kind: "workflow-run", id: workflowRunRepoId },
            pack,
            ref,
            commitSha,
          );
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (msg.startsWith("path_violation")) {
          logger.warn`Workflow-run pack rejected for ${workflowRunRepoId}: ${msg}`;
          return { accepted: false, reason: "path_violation" as const };
        }
        // Mirror the agent-state branch's catch-all: any other failure from
        // the fenced receive (transaction failures, filesystem races,
        // kind-handler diagnostics surfaced as Error messages, etc.) becomes
        // a structured `corrupt` rejection so the sender can re-push, and the
        // underlying error is logged so the cause stays traceable on the hub
        // side.
        logger.error`Workflow-run pack receive failed for ${workflowRunRepoId}: ${msg}`;
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
            const [ownedRun] = await tx
              .select({ anchorRunId: workflowRun.anchorRunId })
              .from(workflowRun)
              .where(eq(workflowRun.id, runId))
              .limit(1);
            if (ownedRun?.anchorRunId !== anchor.id) {
              logger.error`Ignoring terminal event for run ${runId}: it does not belong to source deployment ${anchor.id}`;
              return;
            }
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
                logger.error`Terminal event for run ${runId} (deployment ${anchor.id}, target status ${status}) has no workflow_run row; the run terminated before its anchor committed`;
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
          logger.error`Terminal DB flip failed for run ${runId} (deployment ${anchor.id}, target status ${status}); run left running in the DB: ${msg}`;
        }
      }

      // A sidecar-local mail ack is only receipt. The raw trigger remains in
      // workflow_run_dispatch until the accepted Git tip proves either that
      // the run recorded it (RunStarted / SignalReceived), or that the
      // supervisor consumed it with an explicit rejection. Rescan the bounded
      // retained claim-check index after every accepted pack, and search the
      // stable run log newest-first only for currently-unsettled ids. Settlement
      // is idempotent, and a later pack naturally retries a transient database
      // failure here.
      let topLevelTerminalSettlementProjected = false;
      try {
        const reads = await agentRepoStore.repoStore.openCommittedReads(
          { kind: "hub" },
          repoId,
          ref,
        );
        if (reads !== null) {
          const unsettledDispatches =
            await workflowRunDispatchStore.listUnsettled(anchor.id);
          const unsettledByMessageId = new Map(
            unsettledDispatches.map((dispatch) => [
              dispatch.messageId,
              dispatch,
            ]),
          );
          for (const consumed of await listConsumedWorkflowDispatches(reads)) {
            if (consumed.address !== anchorAddress) continue;
            const persisted = unsettledByMessageId.get(consumed.messageId);
            if (persisted?.kind !== "mail") continue;
            if (consumed.rejection === undefined) {
              await workflowRunDispatchStore.settle(
                anchor.id,
                consumed.messageId,
                now,
              );
            } else {
              await workflowRunDispatchStore.fail({
                anchorRunId: anchor.id,
                messageId: consumed.messageId,
                code: consumed.rejection.code,
                message: consumed.rejection.message,
                now,
              });
            }
            unsettledByMessageId.delete(consumed.messageId);
          }

          // Mail is recorded on the stable deployment run, while a signal is
          // recorded on the exact run named by its durable delivery frame.
          // Group retained dispatches by that Git run before scanning so an
          // internal run's SignalReceived evidence settles its own dispatch.
          // The `runs/<runId>/` log keys on the run id (the address local
          // part), NOT the full address: post-collapse the top-level run's id
          // IS `anchor.id`, so mail keys on `anchor.id`; a signal keys on its
          // frame's own run id. (The `addresses/<address>/` consumed subtree
          // above keys on the full address -- a different subtree.)
          const messageIdsByRun = new Map<string, Set<string>>();
          for (const dispatch of unsettledByMessageId.values()) {
            const runId =
              dispatch.kind === "mail"
                ? anchor.id
                : SignalDeliverFrame.assert(
                    JSON.parse(new TextDecoder().decode(dispatch.rawMessage)),
                  ).runId;
            const messageIds = messageIdsByRun.get(runId) ?? new Set<string>();
            messageIds.add(dispatch.messageId);
            messageIdsByRun.set(runId, messageIds);
          }
          for (const [runId, messageIds] of messageIdsByRun) {
            for (const accepted of await listAcceptedWorkflowDispatches(
              reads,
              runId,
              messageIds,
            )) {
              const persisted = unsettledByMessageId.get(accepted.messageId);
              if (persisted?.kind !== accepted.kind) continue;
              await workflowRunDispatchStore.settle(
                anchor.id,
                accepted.messageId,
                now,
              );
              unsettledByMessageId.delete(accepted.messageId);
            }
          }
          topLevelTerminalSettlementProjected =
            (await readCommittedWorkflowRunLifecycle(reads, anchor.id)) ===
            "terminal";
        }
      } catch (error) {
        logger.error`Workflow dispatch settlement failed for ${anchor.id}; accepted Git state remains authoritative and the retained payload will be retried: ${error instanceof Error ? error.message : String(error)}`;
      }

      if (topLevelTerminalSettlementProjected) {
        try {
          await workflowRunDispatchStore.failUnsettled(
            anchor.id,
            "workflow_run_terminal",
            `Workflow run ${anchorAddress} is terminal and cannot accept this dispatch`,
            now,
          );
        } catch (error) {
          logger.error`Failed to close unsettled workflow dispatches for terminal run ${anchorAddress}: ${error instanceof Error ? error.message : String(error)}`;
        }
      }

      return { accepted: true };
    },
  };
}

/**
 * Extract the run id from an `<runId>@<domain>` agent address.
 * Throws on any input the `@intx/types`-owned `parseRunAddress`
 * rejects: missing or leading `@`, empty domain, or a run id
 * without the canonical `run_` prefix.
 */
export function parseAgentId(agentAddress: string): string {
  const parsed = parseRunAddress(agentAddress);
  if (parsed === null) {
    throw new Error(`Invalid agent address: "${agentAddress}"`);
  }
  return parsed.runId;
}

/**
 * A live routing endpoint backing a run address. Each address names one
 * self-anchored `workflow_run`, so the endpoint is always that run.
 */
export interface RoutableEndpoint {
  readonly id: string;
  readonly tenantId: string;
  readonly address: string;
  readonly publicKey: string | null;
  /**
   * The endpoint's raw run status. Resolution is `endedAt`-filtered, so a
   * resolved endpoint is not necessarily live: a leaked run is deliberately
   * kept routable (terminal status, null `endedAt`) to stay reachable, and the
   * reconnect reaction reads this to keep such an endpoint routable without
   * restoring a collector.
   */
  readonly status: string;
  /**
   * The live session backing this endpoint. A folded run has no session column,
   * so this is the run's not-yet-ended `agent_session`, keyed by the run's
   * principal. Transitional -- it retires when mail record-keeping moves off
   * `agent_session`.
   */
  readonly sessionId: string | null;
}

/**
 * Resolve a run address to the `workflow_run` endpoint backing it, keyed by
 * the run's `address`. Every routable address names one self-anchored run --
 * the deployment's anchor -- so this resolves the run's own address (the
 * source `persistMail` depends on to record a triggered deployment's mail).
 */
export async function resolveRoutableAddress(
  db: DB["db"],
  address: string,
): Promise<RoutableEndpoint | undefined> {
  const runRow = await db
    .select({
      id: workflowRun.id,
      tenantId: workflowRun.tenantId,
      publicKey: workflowRun.publicKey,
      status: workflowRun.status,
      principalId: workflowRun.principalId,
    })
    .from(workflowRun)
    .where(and(eq(workflowRun.address, address), isNull(workflowRun.endedAt)))
    .limit(1)
    .then((rows) => rows[0]);

  if (runRow === undefined) {
    return undefined;
  }

  return {
    id: runRow.id,
    tenantId: runRow.tenantId,
    address,
    publicKey: runRow.publicKey,
    status: runRow.status,
    sessionId: await resolveRunSessionId(db, runRow.principalId),
  };
}

/**
 * A folded run has no session column; its session is the `agent_session` keyed
 * by the run's principal. By default this is the live (not-yet-ended) session,
 * matching routing semantics; `includeEnded` also resolves a stopped run's
 * ended session, which mail history needs. Returns null when the run has no
 * principal or no matching session. Transitional, alongside
 * `RoutableEndpoint.sessionId`.
 */
export async function resolveRunSessionId(
  db: DB["db"],
  principalId: string | null,
  opts: { includeEnded?: boolean } = {},
): Promise<string | null> {
  if (principalId === null) {
    return null;
  }
  // One session per run principal (invariant), so limit(1) returns the whole
  // history; order deterministically so a hypothetical second row cannot make
  // the pick flap. If a run ever grows multiple sessions per principal this
  // becomes a union and limit(1) silently truncates.
  const conditions = [eq(agentSession.principalId, principalId)];
  if (opts.includeEnded !== true) {
    conditions.push(isNull(agentSession.endedAt));
  }
  const row = await db
    .select({ id: agentSession.id })
    .from(agentSession)
    .where(and(...conditions))
    .orderBy(asc(agentSession.createdAt))
    .limit(1)
    .then((rows) => rows[0]);
  return row?.id ?? null;
}

/**
 * The folded run that owns a session, or null when the session belongs to no
 * run. This is the inverse of `resolveRunSessionId`: a mail-read path holds a
 * `sessionMail.sessionId` and no address, so it recovers the owning run by
 * joining `workflow_run` to `agent_session` on their shared principal (a folded
 * run, its session, and its launch all key on the same `instancePrincipalId`).
 * Scoped to the tenant and routed through `workflow_run` so the returned id is
 * proven to name a real run of this tenant -- callers key an authorization
 * subject on it, so a session held by a non-run principal must fail closed to
 * null rather than resolve to a fabricated subject.
 */
export async function resolveRunIdForSession(
  db: DB["db"],
  sessionId: string,
  tenantId: string,
): Promise<string | null> {
  // No `endedAt` filter: a stopped run's mail must stay fetchable, so the
  // session resolves whether or not it has ended. The run principal is minted
  // per launch and shared 1:1 by the run and its session, so at most one row
  // matches; order deterministically anyway so a hypothetical second row cannot
  // make the pick flap, mirroring `resolveRunSessionId`.
  const row = await db
    .select({ id: workflowRun.id })
    .from(workflowRun)
    .innerJoin(
      agentSession,
      eq(agentSession.principalId, workflowRun.principalId),
    )
    .where(
      and(eq(agentSession.id, sessionId), eq(workflowRun.tenantId, tenantId)),
    )
    .orderBy(asc(workflowRun.createdAt))
    .limit(1)
    .then((rows) => rows[0]);
  return row?.id ?? null;
}

/**
 * A folded run resolved BY ID for the instance read/interact surface, shaped
 * into one instance-shaped record. Unlike `resolveRoutableAddress` (keyed by
 * address, live-only), this is keyed by the path id and does NOT filter
 * terminated rows -- a stopped run's detail, mail history, and turns are still
 * served. Keep the two separate: routing must never reach a dead endpoint,
 * while the read surface must still render one.
 */
export interface RoutableRecord {
  readonly id: string;
  readonly tenantId: string;
  /** The routing address. Non-null: a run resolves here only when it owns an
   * address. */
  readonly address: string;
  readonly publicKey: string | null;
  /** Raw run status. The wire mapping onto the instance status enum is a
   * hub-api concern, done by the response shaper. */
  readonly status: string;
  readonly createdAt: Date;
  /** A run has no `updatedAt` column, so it reports `endedAt ?? createdAt`. */
  readonly updatedAt: Date;
  readonly endedAt: Date | null;
  /** The folded definition this run belongs to (`workflow_definition.id`). */
  readonly definitionId: string;
  readonly principalId: string | null;
  readonly kernelId: string | null;
  readonly sidecarId: string | null;
}

/**
 * Shape a run row and its already-resolved routing address into the run
 * record. Callers decide whether the run resolves at all -- only a top-level
 * run (`isTopLevelRun`) does -- and pass the address they have narrowed; this
 * only maps the columns, including the run's `endedAt ?? createdAt` stand-in
 * for the absent `updatedAt`.
 */
export function runRowToRoutableRecord(
  run: {
    id: string;
    tenantId: string;
    publicKey: string | null;
    status: string;
    createdAt: Date;
    endedAt: Date | null;
    definitionId: string;
    principalId: string | null;
    kernelId: string | null;
    sidecarId: string | null;
  },
  address: string,
): RoutableRecord {
  return {
    id: run.id,
    tenantId: run.tenantId,
    address,
    publicKey: run.publicKey,
    status: run.status,
    createdAt: run.createdAt,
    updatedAt: run.endedAt ?? run.createdAt,
    endedAt: run.endedAt,
    definitionId: run.definitionId,
    principalId: run.principalId,
    kernelId: run.kernelId,
    sidecarId: run.sidecarId,
  };
}

/**
 * A run is a top-level run -- the addressable head of a deployment -- when it
 * owns a routing address AND self-anchors (`anchorRunId === id`). A lazy child
 * park row anchors on its parent (`anchorRunId !== id`) and carries no address;
 * either condition excludes it. This is the single predicate the run read
 * surface classifies on, so the resolver and the run list cannot drift.
 */
export function isTopLevelRun(row: {
  id: string;
  address: string | null;
  anchorRunId: string | null;
}): boolean {
  return row.address !== null && row.anchorRunId === row.id;
}

/**
 * Resolve a run id to its record. A run resolves only when it is a top-level
 * run (`isTopLevelRun`): it owns a routing address and self-anchors. A child
 * park row (address-null, anchored on its parent) is not served here.
 */
export async function findRoutableById(
  db: DB["db"],
  id: string,
  tenantId: string,
): Promise<RoutableRecord | undefined> {
  const runRow = await db
    .select({
      id: workflowRun.id,
      tenantId: workflowRun.tenantId,
      address: workflowRun.address,
      anchorRunId: workflowRun.anchorRunId,
      publicKey: workflowRun.publicKey,
      status: workflowRun.status,
      createdAt: workflowRun.createdAt,
      endedAt: workflowRun.endedAt,
      principalId: workflowRun.principalId,
      kernelId: workflowRun.kernelId,
      sidecarId: workflowRun.sidecarId,
      definitionId: workflowRun.definitionId,
    })
    .from(workflowRun)
    .where(and(eq(workflowRun.id, id), eq(workflowRun.tenantId, tenantId)))
    .limit(1)
    .then((rows) => rows[0]);

  // The `address === null` arm is redundant with `isTopLevelRun` (which already
  // requires a non-null address) but narrows `address` from `string | null` to
  // `string` for `runRowToRoutableRecord`, which `isTopLevelRun`'s boolean
  // return cannot do.
  if (
    runRow === undefined ||
    !isTopLevelRun(runRow) ||
    runRow.address === null
  ) {
    return undefined;
  }
  return runRowToRoutableRecord(runRow, runRow.address);
}
