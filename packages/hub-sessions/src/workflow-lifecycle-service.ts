import { and, eq, inArray, isNotNull, or } from "drizzle-orm";

import {
  createSidecarAllocationStore,
  createWorkflowRunStore,
  parseWorkflowRunRow,
  type DB,
  type DBExecutor,
} from "@intx/db";
import {
  isLiveWorkflowRunStatus,
  principal,
  sidecarAllocation,
  workflowRun,
} from "@intx/db/schema";
import { getLogger } from "@intx/log";
import { lifecycleDeadline } from "@intx/types";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import type { WorkflowRunReader } from "./workflow-run-reader";
import type { AllocatedSidecarTarget } from "./ws/sidecar-handler";
import { classifyTerminalEvent } from "./workflow-run-kind";

const logger = getLogger(["hub", "workflow-lifecycle"]);

type Run = ReturnType<typeof parseWorkflowRunRow>;
type Allocation = typeof sidecarAllocation.$inferSelect;
type ReleaseResult =
  | "pending"
  | "released"
  | "live"
  | "not_found"
  | "cleanup_failed";

export type WorkflowLifecycleServiceDeps = {
  db: DB["db"];
  runReader: WorkflowRunReader;
  now?: () => Date;
};

export function createWorkflowLifecycleService({
  db,
  runReader,
  now = () => new Date(),
}: WorkflowLifecycleServiceDeps) {
  const allocations = createSidecarAllocationStore(db);
  const runs = createWorkflowRunStore(db);

  async function withRun<T>(
    tenantId: string,
    runId: string,
    action: (
      tx: DBExecutor,
      run: Run,
      allocation: Allocation | undefined,
    ) => Promise<T>,
  ) {
    return db.transaction(async (tx) => {
      // Match pack ingestion's lock order so terminal projection and release
      // cannot race an accepted write from the previous allocation generation.
      const [allocation] = await tx
        .select()
        .from(sidecarAllocation)
        .where(eq(sidecarAllocation.anchorRunId, runId))
        .for("update");
      const [row] = await tx
        .select()
        .from(workflowRun)
        .where(
          and(
            eq(workflowRun.id, runId),
            eq(workflowRun.tenantId, tenantId),
            eq(workflowRun.anchorRunId, runId),
          ),
        )
        .for("update");
      if (row === undefined) return null;
      return action(tx, parseWorkflowRunRow(row), allocation);
    });
  }

  async function projectTerminal(tx: DBExecutor, run: Run): Promise<Run> {
    if (!isLiveWorkflowRunStatus(run.status) || run.address === null)
      return run;
    const events = await runReader.readRunEvents(
      { kind: "workflow-run", id: deriveWorkflowRunRepoId(run.address) },
      "refs/heads/main",
      run.id,
    );
    const last = events.at(-1);
    if (last === undefined) return run;
    const terminal = classifyTerminalEvent(last.type);
    if (!terminal.terminal) return run;
    const rawAt = last.body["at"];
    const at = typeof rawAt === "string" ? new Date(rawAt).getTime() : NaN;
    if (!Number.isFinite(at))
      throw new Error(
        `Terminal workflow event for ${run.id} has an invalid timestamp`,
      );
    // Worker timestamps cannot extend retention beyond the first Hub
    // observation. Once projected, endedAt is never advanced on retries.
    const endedAt = new Date(
      Math.max(run.createdAt.getTime(), Math.min(now().getTime(), at)),
    );
    const updated = await runs.markTerminal(
      run.id,
      terminal.status,
      endedAt,
      tx,
    );
    if (updated === null) return run;
    if (updated.principalId !== null) {
      await tx
        .update(principal)
        .set({ status: "deactivated", updatedAt: now() })
        .where(
          and(
            eq(principal.id, updated.principalId),
            eq(principal.refId, run.id),
          ),
        );
    }
    return updated;
  }

  async function getStatus(tenantId: string, runId: string) {
    const row = await db.query.workflowRun.findFirst({
      where: and(
        eq(workflowRun.id, runId),
        eq(workflowRun.tenantId, tenantId),
        eq(workflowRun.anchorRunId, runId),
      ),
    });
    if (row === undefined) return null;
    const run = parseWorkflowRunRow(row);
    const allocation = await allocations.findByAnchorRunId(runId);
    return {
      runId,
      status: run.status,
      policy: run.lifecyclePolicy ?? {},
      expiresAt: run.expiresAt?.toISOString() ?? null,
      cancellationRequestedAt:
        run.cancellationRequestedAt?.toISOString() ?? null,
      capacityReleaseAt: run.capacityReleaseAt?.toISOString() ?? null,
      allocation:
        allocation === null
          ? null
          : {
              id: allocation.id,
              status: allocation.status,
              failureCode: allocation.failureCode ?? null,
              failureMessage: allocation.failureMessage ?? null,
            },
    };
  }

  async function releaseCapacity(
    tenantId: string,
    runId: string,
  ): Promise<ReleaseResult> {
    const result = await withRun(
      tenantId,
      runId,
      async (tx, original, allocation): Promise<ReleaseResult> => {
        const run = await projectTerminal(tx, original);
        if (isLiveWorkflowRunStatus(run.status)) return "live";
        if (
          allocation === undefined ||
          allocation.status === "released" ||
          allocation.status === "failed"
        )
          return "released";
        if (allocation.status === "destroy_failed") return "cleanup_failed";
        const requestedAt = now();
        if (
          run.capacityReleaseAt === null ||
          run.capacityReleaseAt > requestedAt
        ) {
          await tx
            .update(workflowRun)
            .set({ capacityReleaseAt: requestedAt })
            .where(eq(workflowRun.id, runId));
        }
        return "pending";
      },
    );
    return result ?? "not_found";
  }

  async function reconcileRun(tenantId: string, runId: string): Promise<void> {
    await withRun(
      tenantId,
      runId,
      async (
        tx,
        original,
        allocation,
      ): Promise<AllocatedSidecarTarget | null> => {
        if (
          allocation === undefined ||
          allocation.status === "released" ||
          allocation.status === "failed" ||
          allocation.status === "destroy_failed" ||
          allocation.status === "releasing"
        )
          return null;
        const run = await projectTerminal(tx, original);
        if (run.status === "deployed" || run.status === "running") return null;
        const retention = run.lifecyclePolicy?.capacityRetention?.[run.status];
        const releaseAt =
          run.capacityReleaseAt ??
          (retention === undefined || run.endedAt === null
            ? null
            : lifecycleDeadline(run.endedAt, retention));
        if (releaseAt === null) return null;
        if (run.capacityReleaseAt === null) {
          await tx
            .update(workflowRun)
            .set({ capacityReleaseAt: releaseAt })
            .where(eq(workflowRun.id, runId));
        }
        if (releaseAt > now()) return null;
        const releasing = await allocations.beginRelease(
          {
            allocationId: allocation.id,
            expectedStatus: allocation.status,
            expectedGeneration: allocation.generation,
            now: now(),
          },
          tx,
        );
        return releasing === null
          ? null
          : { allocationId: releasing.id, generation: releasing.generation };
      },
    );
  }

  async function reconcile(): Promise<void> {
    const candidates = await db
      .select({ id: workflowRun.id, tenantId: workflowRun.tenantId })
      .from(workflowRun)
      .innerJoin(
        sidecarAllocation,
        eq(sidecarAllocation.anchorRunId, workflowRun.id),
      )
      .where(
        and(
          eq(workflowRun.id, workflowRun.anchorRunId),
          or(
            isNotNull(workflowRun.lifecyclePolicy),
            isNotNull(workflowRun.capacityReleaseAt),
          ),
          inArray(sidecarAllocation.status, [
            "pending",
            "provisioning",
            "allocated",
            "replacing",
          ]),
        ),
      );
    for (const run of candidates) {
      try {
        await reconcileRun(run.tenantId, run.id);
      } catch (error) {
        logger.error`Lifecycle reconciliation failed for ${run.id}: ${error instanceof Error ? error.message : String(error)}`;
      }
    }
  }

  return { getStatus, releaseCapacity, reconcile };
}

export type WorkflowLifecycleService = ReturnType<
  typeof createWorkflowLifecycleService
>;
