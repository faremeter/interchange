import { and, eq, inArray, isNotNull, isNull, or, type SQL } from "drizzle-orm";

import type { DB, DBExecutor } from "./client";
import { approval } from "./schema/approvals";
import { signalCorrelation } from "./schema/signal-correlations";
import { liveWorkflowRunStatuses, workflowRun } from "./schema/workflow-run";
import { parseApprovalRow } from "./parse-row";

type DBHandle = DB["db"];

type ApprovalRow = typeof approval.$inferSelect;
type ApprovalInsert = typeof approval.$inferInsert;
type ParsedApproval = ReturnType<typeof parseApprovalRow>;

// A resolution is any terminal status: the pending state is excluded because
// resolving is precisely the transition out of it.
type ApprovalResolution = Exclude<ApprovalRow["status"], "pending">;
type ApprovalScope = NonNullable<ApprovalRow["scope"]>;

export type ResolveApprovalArgs = {
  status: ApprovalResolution;
  scope?: ApprovalScope;
  resolvedAt: Date;
};

/**
 * Store for the `approval` table backing the approval round-trip. Each method
 * accepts an optional transaction handle so the resolver can flip the approval
 * and its signal correlation in a single `db.transaction`; when omitted the
 * method runs against the store's own connection.
 */
export function createApprovalStore(db: DBHandle) {
  return {
    async create(
      row: ApprovalInsert,
      tx?: DBExecutor,
    ): Promise<ParsedApproval> {
      const [inserted] = await (tx ?? db)
        .insert(approval)
        .values(row)
        .returning();
      if (inserted === undefined) {
        throw new Error(
          `approvalStore.create: insert returned no row for ${row.id}`,
        );
      }
      return parseApprovalRow(inserted);
    },

    /**
     * Idempotent variant of `create`. On a `correlationId` unique conflict the
     * insert is a no-op and this returns `null` rather than throwing, so a
     * redelivered register frame (sidecar reconnect, workflow-log replay,
     * supervisor restart re-emitting) does not fail the co-write. Conflicts
     * on `correlationId` -- not the `id` primary key -- because the register
     * co-write mints a fresh `id` per frame while the correlation is the stable
     * dedup key. Returns the parsed row only when this call performed the
     * insert.
     */
    async createIfAbsent(
      row: ApprovalInsert,
      tx?: DBExecutor,
    ): Promise<ParsedApproval | null> {
      const [inserted] = await (tx ?? db)
        .insert(approval)
        .values(row)
        .onConflictDoNothing({ target: approval.correlationId })
        .returning();
      return inserted === undefined ? null : parseApprovalRow(inserted);
    },

    async findByCorrelationId(
      correlationId: string,
      tx?: DBExecutor,
    ): Promise<ParsedApproval | null> {
      const row = await (tx ?? db).query.approval.findFirst({
        where: eq(approval.correlationId, correlationId),
      });
      return row === undefined ? null : parseApprovalRow(row);
    },

    /**
     * Look up an approval by its primary key. The resolve/reject routes key on
     * the `approvalId` path parameter, which is the row's `id`, so they need a
     * by-id read distinct from the by-correlation read the register co-write
     * uses. Returns null when no row carries the id.
     */
    async findById(
      id: string,
      tx?: DBExecutor,
    ): Promise<ParsedApproval | null> {
      const row = await (tx ?? db).query.approval.findFirst({
        where: eq(approval.id, id),
      });
      return row === undefined ? null : parseApprovalRow(row);
    },

    /**
     * List a run's approvals, newest first, scoped by `tenantId` so one
     * tenant's approvals never leak into another's view. Serves the run
     * approvals list route; callers apply whatever status/scope predicate they
     * need over the result.
     */
    async listByRunId(
      tenantId: string,
      runId: string,
      tx?: DBExecutor,
    ): Promise<ParsedApproval[]> {
      const rows = await (tx ?? db).query.approval.findMany({
        where: and(eq(approval.tenantId, tenantId), eq(approval.runId, runId)),
        orderBy: (a, { desc }) => desc(a.createdAt),
      });
      return rows.map(parseApprovalRow);
    },

    /**
     * Conditionally resolve a pending approval. The `WHERE status = 'pending'`
     * guard makes resolution terminal at the database: the first caller to
     * resolve a given correlation gets the updated row back; any later caller
     * (a duplicate delivery, a timeout racing an approval) matches no row and
     * receives null.
     */
    async resolve(
      correlationId: string,
      args: ResolveApprovalArgs,
      tx?: DBExecutor,
    ): Promise<ParsedApproval | null> {
      const [updated] = await (tx ?? db)
        .update(approval)
        .set({
          status: args.status,
          scope: args.scope ?? null,
          resolvedAt: args.resolvedAt,
          updatedAt: new Date(),
        })
        .where(
          and(
            eq(approval.correlationId, correlationId),
            eq(approval.status, "pending"),
          ),
        )
        .returning();
      return updated === undefined ? null : parseApprovalRow(updated);
    },
  };
}

export type ApprovalStore = ReturnType<typeof createApprovalStore>;

/**
 * Expire the pending approvals of the given runs of a deployment and close
 * their signal correlations, in the caller's transaction. A run that ended
 * can take no decision, so its approvals must stop being offered to an
 * approver.
 */
export async function expireRunApprovals(
  tx: DBExecutor,
  anchorRunId: string,
  runIds: readonly string[],
  now: Date | SQL,
): Promise<void> {
  if (runIds.length === 0) return;
  await expireOpenApprovals(
    tx,
    and(
      eq(signalCorrelation.anchorRunId, anchorRunId),
      inArray(signalCorrelation.runId, [...runIds]),
    ),
    now,
  );
}

/**
 * Expire every pending approval of a deployment and close their signal
 * correlations, in the caller's transaction. Once the deployment has ended, a
 * decision is refused for each of its runs: the ones still recorded as live,
 * and the ones that ended earlier and were registered after their end.
 */
export async function expireDeploymentApprovals(
  tx: DBExecutor,
  anchorRunId: string,
  now: Date | SQL,
): Promise<void> {
  await expireOpenApprovals(
    tx,
    eq(signalCorrelation.anchorRunId, anchorRunId),
    now,
  );
}

async function expireOpenApprovals(
  tx: DBExecutor,
  correlations: SQL | undefined,
  now: Date | SQL,
): Promise<void> {
  // The correlation is claimed before its approval, as the resolver does, so
  // the two take their row locks in one order.
  const claimed = await tx
    .update(signalCorrelation)
    .set({ resolvedAt: now })
    .where(and(correlations, isNull(signalCorrelation.resolvedAt)))
    .returning({ correlationId: signalCorrelation.correlationId });
  if (claimed.length === 0) return;
  await tx
    .update(approval)
    .set({ status: "expired", resolvedAt: now, updatedAt: now })
    .where(
      and(
        inArray(
          approval.correlationId,
          claimed.map((row) => row.correlationId),
        ),
        eq(approval.status, "pending"),
      ),
    );
}

/**
 * A correlation no decision has claimed: `open` while its approval is
 * pending, and closed once an expiry took it.
 */
export type UndecidedCorrelation = {
  correlationId: string;
  runId: string;
  open: boolean;
};

/**
 * The correlations of a deployment that a pack's wait changes can move: each
 * open one of `runIds`, and each of `correlationIds` that an expiry closed.
 * A decided correlation is never among them.
 */
export async function listUndecidedCorrelations(
  tx: DBExecutor,
  anchorRunId: string,
  runIds: readonly string[],
  correlationIds: readonly string[],
): Promise<UndecidedCorrelation[]> {
  const candidates = [
    ...(runIds.length > 0
      ? [
          and(
            isNull(signalCorrelation.resolvedAt),
            inArray(signalCorrelation.runId, [...runIds]),
          ),
        ]
      : []),
    ...(correlationIds.length > 0
      ? [inArray(signalCorrelation.correlationId, [...correlationIds])]
      : []),
  ];
  if (candidates.length === 0) return [];
  const rows = await tx
    .select({
      correlationId: signalCorrelation.correlationId,
      runId: signalCorrelation.runId,
      resolvedAt: signalCorrelation.resolvedAt,
    })
    .from(signalCorrelation)
    .where(
      and(
        eq(signalCorrelation.anchorRunId, anchorRunId),
        // Only a decision records a signal id.
        isNull(signalCorrelation.signalId),
        or(...candidates),
      ),
    );
  return rows.map(({ resolvedAt, ...row }) => ({
    ...row,
    open: resolvedAt === null,
  }));
}

/**
 * Expire the pending approvals of the given correlations and close those
 * correlations, in the caller's transaction. A run that stopped waiting on an
 * approval can take no decision on it. A decided approval is left as it was.
 */
export async function expireApprovals(
  tx: DBExecutor,
  correlationIds: readonly string[],
  now: Date,
): Promise<void> {
  if (correlationIds.length === 0) return;
  await expireOpenApprovals(
    tx,
    inArray(signalCorrelation.correlationId, [...correlationIds]),
    now,
  );
}

/**
 * Take the given approvals of a deployment that an expiry closed back to
 * pending and open their correlations again, in the caller's transaction. A
 * run that waits again on an approval it retired gets it back. A decided
 * approval is left as it was, and so is every approval of a deployment whose
 * anchor run has ended: it takes no decision.
 *
 * The caller holds the deployment's allocation row, as every path that ends
 * the anchor run does. Without it the anchor could end between the read
 * below and the write, and leave what this opened pending.
 */
export async function reopenApprovals(
  tx: DBExecutor,
  anchorRunId: string,
  correlationIds: readonly string[],
  now: Date,
): Promise<void> {
  if (correlationIds.length === 0) return;
  const [live] = await tx
    .select({ id: workflowRun.id })
    .from(workflowRun)
    .where(
      and(
        eq(workflowRun.id, anchorRunId),
        inArray(workflowRun.status, [...liveWorkflowRunStatuses]),
      ),
    );
  if (live === undefined) return;
  // Only a decision records a signal id, so a closed correlation without one
  // was closed by an expiry.
  const reopened = await tx
    .update(signalCorrelation)
    .set({ resolvedAt: null })
    .where(
      and(
        eq(signalCorrelation.anchorRunId, anchorRunId),
        inArray(signalCorrelation.correlationId, [...correlationIds]),
        isNotNull(signalCorrelation.resolvedAt),
        isNull(signalCorrelation.signalId),
      ),
    )
    .returning({ correlationId: signalCorrelation.correlationId });
  if (reopened.length === 0) return;
  await tx
    .update(approval)
    .set({ status: "pending", resolvedAt: null, updatedAt: now })
    .where(
      and(
        inArray(
          approval.correlationId,
          reopened.map((row) => row.correlationId),
        ),
        eq(approval.status, "expired"),
      ),
    );
}
