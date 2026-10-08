// Lookups the sidecar wire layer issues against host state: one answer per
// question, gathered into the struct the hub app passes to
// `createSidecarRouter` as `lookups`.

import { eq, and, asc, inArray, isNotNull, isNull } from "drizzle-orm";
import type { DB } from "@intx/db";
import {
  createApprovalStore,
  createSignalCorrelationStore,
  createWorkflowPendingProjectionStore,
  createWorkflowRunStore,
} from "@intx/db";
import {
  agentSession,
  liveWorkflowRunStatuses,
  sessionMail,
  sidecarAllocation,
  workflowRun,
} from "@intx/db/schema";
import { getLogger } from "@intx/log";
import { parseRunAddress, signalName } from "@intx/types";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import type { AgentRepoStore } from "./agent-repo";
import { generateId } from "@intx/hub-common";
import type { SidecarLookups } from "./ws/sidecar-events";
import { createWorkflowDispatchProjection } from "./workflow-dispatch-projection";
import type { WorkflowHistoryReceiveTracker } from "./workflow-history-receives";
import { readWorkflowRunRefTips } from "./workflow-run-restore";
import { projectTerminalRun } from "./workflow-run-terminal-projection";

const logger = getLogger(["hub", "lookups"]);

export type HubSessionLookupsDeps = {
  db: DB["db"];
  agentRepoStore: AgentRepoStore;
  historyReceives: WorkflowHistoryReceiveTracker;
};

export function createHubSessionLookups(
  deps: HubSessionLookupsDeps,
): Required<
  Omit<
    SidecarLookups,
    | "materializeMailTriggeredRunGrants"
    | "resyncCredentials"
    | "resolveSenderKey"
    | "resolveSenderKeyStrict"
  >
> {
  const { db, agentRepoStore, historyReceives } = deps;

  const signalCorrelationStore = createSignalCorrelationStore(db);
  const approvalStore = createApprovalStore(db);
  const workflowRunStore = createWorkflowRunStore(db);
  const pendingProjections = createWorkflowPendingProjectionStore(db);
  const dispatchProjection = createWorkflowDispatchProjection({
    db,
    repoStore: agentRepoStore.repoStore,
  });

  return {
    async lookupDeployRef() {
      // A workflow run keeps its deploy-time definition and never reconciles,
      // so no address enrolls in the reconnect deploy-ref catch-up.
      return null;
    },

    readWorkflowRunRefTips(agentAddress) {
      return readWorkflowRunRefTips(agentRepoStore.repoStore, agentAddress);
    },

    async persistMail({ senderAddress, recipients, raw }) {
      // Sender and recipients are run addresses backed by self-anchored
      // workflow_runs. A mail record's `runId` is always null for a run -- it
      // keys on the run's session instead.
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
      anchorRunId,
      agentAddress,
      kind,
      approvalSnapshot,
    }) {
      // Co-write both rows in one transaction so a resolver never sees a
      // correlation without its approval. Both inserts are idempotent on their
      // dedup key, so a redelivered frame (reconnect, log replay, restart) is a
      // no-op. `timeoutAt` is null: an agent-step suspend holds indefinitely,
      // so no deadline reaches this co-write.
      await db.transaction(async (tx) => {
        // Resolve tenancy and the run's definition from the deployment's anchor
        // run (the workflow_run whose id is the deployment id). The lookup keys
        // off `address` (what the wire layer's ownership gate authorized), not
        // the frame's `anchorRunId`: that is the repo slug derived from the
        // address, cross-checked below via `deriveWorkflowRunRepoId` so a
        // mismatch fails loud instead of writing an inconsistent pair.
        //
        // Takes a `FOR UPDATE` row lock inside the co-write transaction, gated
        // on a live anchor run, so the liveness check and the inserts are atomic
        // against a concurrent teardown. Lock order is workflow_run before
        // signal_correlation and approval; teardown must keep that order acyclic.
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
        if (addressSlug !== anchorRunId) {
          throw new Error(
            `Anchor run id mismatch registering signal correlation ${correlationId}: frame claims "${anchorRunId}" but address "${agentAddress}" derives the workflow-run repo slug "${addressSlug}"`,
          );
        }
        const tenantId = anchor.tenantId;
        const definitionId = anchor.definitionId;

        // Ensure the run row exists before the co-written rows reference it: a
        // workflow-spawned internal run never crosses the trigger route that
        // mints a run principal. Principal is null (internal runs inherit their
        // deployment's grants); the insert is idempotent on the run id, so a
        // redelivered frame is a no-op.
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
            anchorRunId: anchor.id,
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
            anchorRunId: anchor.id,
            runId,
            agentAddress,
            correlationId,
            status: "pending",
            // The register frame (the ask rail's only producer) always carries
            // the snapshot, so these columns are never null on this path.
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
        // Catch-all: any failure from the repo subsystem would otherwise
        // surface as an unhandled rejection on the WebSocket handler.
        // Transient receive failures (concurrent teardown, filesystem errors)
        // are recoverable -- the sender can re-push -- so surface them as a
        // structured `corrupt` rejection and log the cause.
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
          tenantId: workflowRun.tenantId,
          definitionId: workflowRun.definitionId,
          createdAt: workflowRun.createdAt,
        })
        .from(workflowRun)
        .where(
          and(
            eq(workflowRun.address, source.agentAddress),
            inArray(workflowRun.status, [...liveWorkflowRunStatuses]),
            isNotNull(workflowRun.definitionId),
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
      // The repository must exist before the row does. Recovery reads a
      // missing repository as lost history and a ref-less one as a receive
      // that never advanced Git, so a crash between the two must leave the
      // latter.
      try {
        await agentRepoStore.repoStore.initRepo(repoId);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.error`Workflow-run pack receive failed for ${workflowRunRepoId}: cannot initialize repository: ${msg}`;
        return { accepted: false, reason: "corrupt" as const };
      }
      // Recorded before Git can advance: Git acceptance and the status
      // projection below are not atomic, and this row is the only durable trace
      // of a projection that fails after the ref moves. It is removed only when
      // the receive provably left Git unchanged or every run reached a final
      // decision; otherwise lifecycle recovery reconciles the deployment.
      const pendingId = generateId("workflowPendingProjection");
      historyReceives.begin(pendingId);
      let newlyTerminalRuns;
      let reachedGit = false;
      try {
        try {
          await pendingProjections.open(pendingId, anchor.id);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logger.error`Workflow-run pack receive failed for ${workflowRunRepoId}: cannot record pending projection: ${msg}`;
          return { accepted: false, reason: "corrupt" as const };
        }
        const received = await db.transaction(async (tx) => {
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
            await pendingProjections.close(pendingId, tx);
            return {
              rejected:
                "source connection does not own the deployment's current allocation",
            } as const;
          }
          // Recheck under the lock: the lifecycle service records a run's
          // outcome while holding this row, and history accepted after that
          // would contradict the recorded outcome.
          const [live] = await tx
            .select({ id: workflowRun.id })
            .from(workflowRun)
            .where(
              and(
                eq(workflowRun.id, anchor.id),
                inArray(workflowRun.status, [...liveWorkflowRunStatuses]),
              ),
            )
            .limit(1);
          if (live === undefined) {
            await pendingProjections.close(pendingId, tx);
            return {
              rejected: `workflow run ${anchor.id} is no longer live`,
            } as const;
          }

          // Replacement advances this same row. Keep its lock until the
          // repository ref has advanced so ownership cannot change after
          // validation but before the old worker's pack becomes authoritative.
          reachedGit = true;
          return {
            runs: await agentRepoStore.receiveWorkflowRunPack(
              { kind: "workflow-run", id: workflowRunRepoId },
              pack,
              ref,
              commitSha,
            ),
          } as const;
        });
        if ("rejected" in received) {
          logger.warn`Workflow-run pack rejected for ${workflowRunRepoId}: ${received.rejected}`;
          return { accepted: false, reason: "path_violation" as const };
        }
        newlyTerminalRuns = received.runs;
      } catch (err) {
        // A receive that reached Git keeps its pending row: the ref may have
        // advanced before the failure, so only a Git read can prove nothing was
        // lost. One that failed earlier provably left Git unchanged.
        if (!reachedGit)
          await pendingProjections.close(pendingId).catch((cause: unknown) => {
            logger.warn`Cannot clear pending projection for ${anchor.id}: ${cause instanceof Error ? cause.message : String(cause)}`;
          });
        const msg = err instanceof Error ? err.message : String(err);
        if (msg.startsWith("path_violation")) {
          logger.warn`Workflow-run pack rejected for ${workflowRunRepoId}: ${msg}`;
          return { accepted: false, reason: "path_violation" as const };
        }
        // Mirror the agent-state catch-all: any other failure from the fenced
        // receive becomes a structured `corrupt` rejection so the sender can
        // re-push, with the cause logged on the hub side.
        logger.error`Workflow-run pack receive failed for ${workflowRunRepoId}: ${msg}`;
        return { accepted: false, reason: "corrupt" as const };
      } finally {
        historyReceives.end(pendingId);
      }

      // The substrate already durably advanced the git ref, so the pack is
      // accepted regardless of what happens below; the status flip is a
      // downstream side effect, not part of acceptance, so the sidecar does not
      // wedge re-pushing a pack that already landed. A redelivered tip produces
      // no newly-terminal signal; a failed flip stays pending and lifecycle
      // recovery projects the run from Git.
      const now = new Date();
      let decided = true;
      for (const { runId, status, terminalEventJson } of newlyTerminalRuns) {
        try {
          const outcome = await db.transaction((tx) =>
            projectTerminalRun(tx, workflowRunStore, {
              anchor,
              runId,
              status,
              terminalEvent: JSON.parse(terminalEventJson) as unknown,
              now,
            }),
          );
          if (outcome === "foreign")
            logger.error`Ignoring terminal event for run ${runId}: it does not belong to source deployment ${anchor.id}`;
        } catch (err) {
          // Per-run isolation: a failed flip for one run must not abort the
          // rest of the batch, and must not throw out of this method -- a throw
          // would leave the sidecar with neither an ack nor a reject for a pack
          // the substrate already accepted.
          decided = false;
          const msg = err instanceof Error ? err.message : String(err);
          logger.error`Terminal DB flip failed for run ${runId} (deployment ${anchor.id}, target status ${status}); its projection stays pending: ${msg}`;
        }
      }
      if (decided) {
        try {
          await pendingProjections.close(pendingId);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logger.warn`Cannot clear pending projection for ${anchor.id}: ${msg}`;
        }
      }

      // Git acceptance survives projection failures. The terminal recovery pass
      // retries these outcomes even after the allocation has been released.
      try {
        await dispatchProjection.project(anchor.id);
      } catch (error) {
        logger.error`Workflow dispatch settlement failed for ${anchor.id}: ${error instanceof Error ? error.message : String(error)}`;
      }

      return { accepted: true };
    },
  };
}

/**
 * Extract the run id from an `<runId>@<domain>` run address. Throws when
 * `parseRunAddress` rejects the input.
 */
export function parseAgentId(agentAddress: string): string {
  const parsed = parseRunAddress(agentAddress);
  if (parsed === null) {
    throw new Error(`Invalid run address: "${agentAddress}"`);
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
   * kept routable (terminal status, null `endedAt`) to stay reachable.
   */
  readonly status: string;
  /**
   * The live session backing this endpoint: the run's not-yet-ended
   * `agent_session`, keyed by the run's principal (a folded run has no session
   * column).
   */
  readonly sessionId: string | null;
}

/**
 * Resolve a run address to the `workflow_run` endpoint backing it, keyed by
 * the run's `address`. Every routable address names one self-anchored run --
 * the deployment's anchor.
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
 * by the run's principal. Defaults to the live (not-yet-ended) session,
 * matching routing semantics; `includeEnded` also resolves a stopped run's
 * ended session, which mail history needs. Returns null when the run has no
 * principal or no matching session.
 */
export async function resolveRunSessionId(
  db: DB["db"],
  principalId: string | null,
  opts: { includeEnded?: boolean } = {},
): Promise<string | null> {
  if (principalId === null) {
    return null;
  }
  // One session per run principal (invariant); order deterministically so a
  // hypothetical second row cannot make the pick flap.
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
 * run. Inverse of `resolveRunSessionId`: a mail-read path holds only a
 * `sessionMail.sessionId`, so it recovers the owning run by joining
 * `workflow_run` to `agent_session` on their shared principal. Scoped to the
 * tenant and routed through `workflow_run` so the returned id names a real run
 * of this tenant -- callers key an authorization subject on it.
 */
export async function resolveRunIdForSession(
  db: DB["db"],
  sessionId: string,
  tenantId: string,
): Promise<string | null> {
  // No `endedAt` filter: a stopped run's mail must stay fetchable. The run
  // principal is minted per launch and shared 1:1 by run and session, so at
  // most one row matches; order deterministically anyway.
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
 * A folded run resolved BY ID for the instance read/interact surface. Unlike
 * `resolveRoutableAddress` (keyed by address, live-only), this is keyed by the
 * path id and does NOT filter terminated rows -- a stopped run's detail, mail
 * history, and turns are still served. Keep the two separate: routing must
 * never reach a dead endpoint, while the read surface must still render one.
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
  /** Runs have no `updatedAt` column; report `endedAt ?? createdAt`. */
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
 * record. Callers decide whether the run resolves at all and pass the address
 * they have narrowed; this only maps the columns.
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
 * park row anchors on its parent and carries no address; either condition
 * excludes it. This is the single predicate the run read surface classifies on.
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
 * run (`isTopLevelRun`); a child park row is not served here.
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

  // The `address === null` arm is redundant with `isTopLevelRun` but narrows
  // `address` to `string` for `runRowToRoutableRecord`, which the boolean
  // return cannot do.
  if (
    runRow === undefined ||
    !isTopLevelRun(runRow) ||
    runRow.address === null ||
    runRow.definitionId === null
  ) {
    return undefined;
  }
  return runRowToRoutableRecord(
    { ...runRow, definitionId: runRow.definitionId },
    runRow.address,
  );
}
