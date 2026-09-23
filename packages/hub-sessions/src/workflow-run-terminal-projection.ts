import { and, eq } from "drizzle-orm";

import type { DBExecutor, WorkflowRunStore } from "@intx/db";
import { principal, workflowRun } from "@intx/db/schema";

export type TerminalProjectionAnchor = {
  id: string;
  tenantId: string;
  definitionId: string;
};

/**
 * `projected` and `settled` both leave the row reflecting accepted history;
 * `foreign` means the run id belongs to another deployment and was ignored.
 * Every outcome is a final decision for the event.
 */
export type TerminalProjectionOutcome = "projected" | "settled" | "foreign";

/**
 * Project one accepted terminal event onto its `workflow_run` row: settle the
 * run and deactivate its principal, minting the row first when the event is
 * the first the Hub sees of the run.
 */
export async function projectTerminalRun(
  tx: DBExecutor,
  runs: WorkflowRunStore,
  args: {
    anchor: TerminalProjectionAnchor;
    runId: string;
    status: "completed" | "failed" | "cancelled";
    now: Date;
  },
): Promise<TerminalProjectionOutcome> {
  const { anchor, runId, status, now } = args;
  // Lazily anchor the run before settling it. An internal run that parks only
  // on a plain signal gate never reaches `registerSignalCorrelation`, the sole
  // other path that mints an internal run row, so its terminal event can be the
  // first the hub sees of the run. A never-minted row is ordinary bookkeeping,
  // not a deployment-boundary violation, so mint it here against this
  // deployment's anchor rather than letting the ownership guard below mistake
  // absence for foreignness. The insert no-ops when any row already exists,
  // which keeps that guard authoritative for a row that exists and anchors
  // elsewhere. The principal is null: an internal run inherits its
  // deployment's grants and has none of its own.
  //
  // The mint necessarily precedes the ownership guard, so an id the hub has
  // never seen is claimed under THIS anchor before anything establishes it
  // belongs here. That ordering is required -- the guard reads the row the mint
  // may have to create -- and it is bounded rather than unbounded: internal run
  // ids are supplied by the sidecar and accepted verbatim, so the value is
  // caller-influenced, but it is a different population from the anchor ids
  // the hub mints itself, and nothing resolves an internal id without also
  // constraining the anchor or the tenant. The insert cannot take a row away
  // from another deployment; the worst it does is create one for an id that
  // deployment would otherwise have created later.
  await runs.createIfAbsent(
    {
      id: runId,
      anchorRunId: anchor.id,
      definitionId: anchor.definitionId,
      tenantId: anchor.tenantId,
      principalId: null,
      status: "running",
    },
    tx,
  );
  const [ownedRun] = await tx
    .select({ anchorRunId: workflowRun.anchorRunId })
    .from(workflowRun)
    .where(eq(workflowRun.id, runId))
    .limit(1);
  if (ownedRun?.anchorRunId !== anchor.id) return "foreign";
  const won = await runs.markTerminal(runId, status, now, tx);
  // The row exists (the mint above guarantees it) and belongs to this
  // deployment (the guard above), so no live row matched only because the run
  // is already terminal. Leave its settled status and `endedAt` alone.
  if (won === null) return "settled";
  // Deactivate the run's own principal, if it has one. Externally-triggered
  // runs carry a principal; internal, workflow-spawned runs have
  // `principalId = null` and inherit the deployment's grants, so there is
  // nothing to deactivate. Deactivation is gated on winning the flip -- the
  // single claim point -- not on the principal's own status.
  if (won.principalId !== null) {
    await tx
      .update(principal)
      .set({ status: "deactivated", updatedAt: now })
      // `won.principalId` is already this run's own principal and
      // `principal.id` is the primary key, so the `refId` match only confirms
      // the id we won belongs to this run.
      .where(
        and(eq(principal.id, won.principalId), eq(principal.refId, runId)),
      );
  }
  return "projected";
}
