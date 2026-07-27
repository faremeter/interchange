// Lookups that the sidecar wire layer issues against host state. These
// are queries (one answer per question) rather than events (broadcast
// notifications), so they live separately from the event emitter.
//
// Each lookup is a stateless DB or repo call. They are gathered into a
// single struct that the hub app passes to `createSidecarRouter` as
// `lookups`.

import { eq, and, asc, isNull } from "drizzle-orm";
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
  workflowDefinition,
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
      // Route by address space. A workflow-derived address's key lives on the
      // deployment's anchor workflow_run row; a plain `ins_<hex>` address is
      // backed by either a launched agent_instance or a folded workflow_run --
      // the two share that address space under the fold -- and
      // `resolveRoutableAddress` resolves both together. A missing endpoint (or
      // a null key) returns null so the reconnect challenge fails closed and
      // the address stays unrouted rather than routing without ownership proof.
      if (isWorkflowDerivedAddress(agentAddress)) {
        // Read the key off the deployment's anchor run, gated on a live
        // ("running") run so a decommissioned deployment's key can no longer
        // satisfy a challenge. The anchor run is born running and no path flips
        // it terminal today: the gate is the read-side invariant a future
        // undeploy/teardown feature will satisfy -- mirroring the deployment's
        // dormant `status='error'` contract -- not dead code. A null publicKey
        // (running but not yet acked) or an absent row returns null.
        const row = await db
          .select({ publicKey: workflowRun.publicKey })
          .from(workflowRun)
          .where(
            and(
              eq(workflowRun.address, agentAddress),
              eq(workflowRun.status, "running"),
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
        // co-write transaction, gated on a live "running" anchor run, so the
        // liveness check and the inserts are atomic against a concurrent
        // teardown that flips the anchor run terminal. The lock order is
        // workflow_run before signal_correlation and approval; a teardown path
        // must take the anchor-run lock before touching those rows to keep the
        // ordering acyclic.
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
              eq(workflowRun.status, "running"),
            ),
          )
          .for("update")
          .limit(1)
          .then((rows) => rows[0]);
        if (anchor === undefined) {
          throw new Error(
            `No running workflow run for address "${agentAddress}"; cannot register signal correlation ${correlationId}`,
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
            deploymentId: anchor.id,
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
   * The endpoint's raw table status (the instance or run enum). Resolution is
   * `endedAt`-filtered, so a resolved endpoint is not necessarily live: a
   * leaked run/instance is deliberately kept routable (terminal status, null
   * `endedAt`) to stay reachable, and the reconnect reaction reads this to keep
   * such an endpoint routable without restoring a collector.
   */
  readonly status: string;
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
  // A workflow-derived address belongs to a deployment's anchor run, resolved
  // through the workflow-derived key path (`lookupPublicKey`/deploy-ack), not
  // here. `resolveRoutableAddress` owns plain `ins_<hex>` resolution; excluding
  // the derived family keeps the anchor run invisible to every plain-address
  // consumer (mail persistence, the reconnect reaction), preserving the
  // undefined result those paths saw before the anchor row existed.
  if (isWorkflowDerivedAddress(address)) {
    return undefined;
  }
  const [instanceRows, runRows] = await Promise.all([
    db
      .select({
        id: agentInstance.id,
        tenantId: agentInstance.tenantId,
        publicKey: agentInstance.publicKey,
        status: agentInstance.status,
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
        status: workflowRun.status,
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
      status: instanceRow.status,
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
      status: runRow.status,
      sessionId: await resolveRunSessionId(db, runRow.principalId),
    };
  }

  return undefined;
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
 * A routing endpoint resolved BY ID for the instance read/interact surface,
 * normalized across the agent-instance -> workflow-run fold into one
 * instance-shaped record. Unlike `resolveRoutableAddress` (keyed by address,
 * live-only), this is keyed by the path id and does NOT filter terminated rows
 * -- a stopped instance's detail, mail history, and turns are still served.
 * Keep the two separate: routing must never reach a dead endpoint, while the
 * read surface must still render one.
 */
export interface RoutableRecord {
  readonly kind: "instance" | "run";
  readonly id: string;
  readonly tenantId: string;
  /** The routing address. Non-null for both kinds: an instance's column is
   * not-null, and a run resolves here only when it owns an address. */
  readonly address: string;
  readonly publicKey: string | null;
  /** Raw table status (instance or run enum). The wire mapping onto the
   * instance status enum is a hub-api concern, done by the response shaper. */
  readonly status: string;
  readonly createdAt: Date;
  /** A run has no `updatedAt` column, so it reports `endedAt ?? createdAt`. */
  readonly updatedAt: Date;
  readonly endedAt: Date | null;
  /** The folded definition this instance/run belongs to (`workflow_definition.id`).
   * Non-null for both kinds. */
  readonly definitionId: string;
  readonly principalId: string | null;
  readonly sessionId: string | null;
  readonly kernelId: string | null;
  readonly sidecarId: string | null;
}

/**
 * Resolve a plain instance/run id to its instance-shaped record. Queries BOTH
 * tables (a launch writes exactly one, so at most one matches; a match in both
 * is corruption and throws, as in `resolveRoutableAddress`). A run resolves
 * only when it presents as an instance: it owns a routing address AND its
 * definition names an origin agent. A run with no address is deployment-
 * anchored, not an instance, and returns undefined (mirroring the stop route).
 */
export async function findRoutableById(
  db: DB["db"],
  id: string,
  tenantId: string,
): Promise<RoutableRecord | undefined> {
  const [instanceRows, runRows] = await Promise.all([
    db
      .select({
        id: agentInstance.id,
        tenantId: agentInstance.tenantId,
        address: agentInstance.address,
        publicKey: agentInstance.publicKey,
        status: agentInstance.status,
        createdAt: agentInstance.createdAt,
        updatedAt: agentInstance.updatedAt,
        endedAt: agentInstance.endedAt,
        definitionId: workflowDefinition.id,
        principalId: agentInstance.principalId,
        sessionId: agentInstance.sessionId,
        kernelId: agentInstance.kernelId,
        sidecarId: agentInstance.sidecarId,
      })
      .from(agentInstance)
      .innerJoin(
        workflowDefinition,
        eq(agentInstance.agentId, workflowDefinition.originAgentId),
      )
      .where(
        and(eq(agentInstance.id, id), eq(agentInstance.tenantId, tenantId)),
      )
      .limit(1),
    db
      .select({
        id: workflowRun.id,
        tenantId: workflowRun.tenantId,
        address: workflowRun.address,
        publicKey: workflowRun.publicKey,
        status: workflowRun.status,
        createdAt: workflowRun.createdAt,
        endedAt: workflowRun.endedAt,
        principalId: workflowRun.principalId,
        kernelId: workflowRun.kernelId,
        sidecarId: workflowRun.sidecarId,
        definitionId: workflowRun.definitionId,
        // The origin agent is not part of the record; it only gates whether a
        // run presents as an instance (a native run's definition has none).
        originAgentId: workflowDefinition.originAgentId,
      })
      .from(workflowRun)
      .leftJoin(
        workflowDefinition,
        eq(workflowRun.definitionId, workflowDefinition.id),
      )
      .where(and(eq(workflowRun.id, id), eq(workflowRun.tenantId, tenantId)))
      .limit(1),
  ]);

  const instanceRow = instanceRows[0];
  const runRow = runRows[0];

  if (instanceRow !== undefined && runRow !== undefined) {
    throw new Error(
      `id "${id}" resolves to both an agent instance and a workflow run; ` +
        `refusing to resolve an ambiguous instance record`,
    );
  }

  if (instanceRow !== undefined) {
    return {
      kind: "instance",
      id: instanceRow.id,
      tenantId: instanceRow.tenantId,
      address: instanceRow.address,
      publicKey: instanceRow.publicKey,
      status: instanceRow.status,
      createdAt: instanceRow.createdAt,
      updatedAt: instanceRow.updatedAt,
      endedAt: instanceRow.endedAt,
      definitionId: instanceRow.definitionId,
      principalId: instanceRow.principalId,
      sessionId: instanceRow.sessionId,
      kernelId: instanceRow.kernelId,
      sidecarId: instanceRow.sidecarId,
    };
  }

  if (runRow !== undefined) {
    if (runRow.address === null) {
      // Deployment-anchored native run, not a folded instance; not served here.
      return undefined;
    }
    if (isWorkflowDerivedAddress(runRow.address)) {
      // A deployment's anchor run owns a workflow-derived address. Like an
      // address-less deployment-anchored run it is not a folded instance and is
      // not served on the instance read surface -- and unlike a plain-address
      // run with no origin agent below, its missing origin agent is expected,
      // not corruption, so it must not warn.
      return undefined;
    }
    if (runRow.originAgentId === null) {
      // A folded run owns a plain address but its definition names no origin
      // agent: backfill corruption. Surface it rather than bury it as a silent
      // 404.
      logger.warn`Run ${runRow.id} owns a routing address but its definition has no origin agent; treating as not found`;
      return undefined;
    }
    return {
      kind: "run",
      id: runRow.id,
      tenantId: runRow.tenantId,
      address: runRow.address,
      publicKey: runRow.publicKey,
      status: runRow.status,
      createdAt: runRow.createdAt,
      updatedAt: runRow.endedAt ?? runRow.createdAt,
      endedAt: runRow.endedAt,
      definitionId: runRow.definitionId,
      principalId: runRow.principalId,
      sessionId: await resolveRunSessionId(db, runRow.principalId),
      kernelId: runRow.kernelId,
      sidecarId: runRow.sidecarId,
    };
  }

  return undefined;
}
