import { and, eq, inArray, isNotNull, or } from "drizzle-orm";

import {
  createSidecarAllocationStore,
  createWorkflowRunStore,
  createWorkflowRunDispatchStore,
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
import type {
  AllocatedSidecarTarget,
  SidecarAllocationRouter,
} from "./ws/sidecar-handler";
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
  cancelGraceMs?: number;
  sendControl?: SidecarAllocationRouter["sendWorkflowControl"];
};

export function createWorkflowLifecycleService({
  db,
  runReader,
  now = () => new Date(),
  cancelGraceMs = 30_000,
  sendControl,
}: WorkflowLifecycleServiceDeps) {
  if (cancelGraceMs < 0 || !Number.isFinite(cancelGraceMs))
    throw new Error("Cancellation grace must be finite and non-negative");
  const allocations = createSidecarAllocationStore(db);
  const dispatches = createWorkflowRunDispatchStore(db);
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
        .where(
          and(
            eq(sidecarAllocation.anchorRunId, runId),
            eq(sidecarAllocation.tenantId, tenantId),
          ),
        )
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
      cancellationDeadline: run.cancellationDeadline?.toISOString() ?? null,
      cancellationReason: run.cancellationReason ?? null,
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

  async function projectTerminalForStop(
    tx: DBExecutor,
    run: Run,
  ): Promise<Run> {
    try {
      return await projectTerminal(tx, run);
    } catch (error) {
      logger.warn`Cannot read terminal state for ${run.id}; proceeding with requested cancellation: ${error instanceof Error ? error.message : String(error)}`;
      return run;
    }
  }

  async function beginCancellation(
    tx: DBExecutor,
    run: Run,
    reason: string,
  ): Promise<Run> {
    if (run.cancellationRequestedAt !== null) return run;
    const requestedAt = now();
    const deadline = new Date(requestedAt.getTime() + cancelGraceMs);
    await tx
      .update(workflowRun)
      .set({
        cancellationRequestedAt: requestedAt,
        cancellationDeadline: deadline,
        cancellationReason: reason,
      })
      .where(eq(workflowRun.id, run.id));
    return {
      ...run,
      cancellationRequestedAt: requestedAt,
      cancellationDeadline: deadline,
      cancellationReason: reason,
    };
  }

  async function requestCancellation(
    tenantId: string,
    runId: string,
    reason: string,
  ): Promise<"pending" | "terminal" | "not_found"> {
    const result = await withRun(tenantId, runId, async (tx, original) => {
      const run = await projectTerminalForStop(tx, original);
      if (!isLiveWorkflowRunStatus(run.status)) return "terminal" as const;
      await beginCancellation(tx, run, reason);
      return "pending" as const;
    });
    return result ?? "not_found";
  }

  async function markStopped(tx: DBExecutor, run: Run): Promise<void> {
    // Forced termination may leave only a partial event log. The Hub records
    // the outcome after the worker confirms its stop or the provisioner confirms
    // destruction; it never reports cancellation while an unfenced worker lives.
    const endedAt = now();
    const stopped = await tx
      .update(workflowRun)
      .set({ status: "cancelled", endedAt })
      .where(
        and(
          eq(workflowRun.anchorRunId, run.id),
          inArray(workflowRun.status, ["deployed", "running"]),
        ),
      )
      .returning({ principalId: workflowRun.principalId });
    const principalIds = stopped.flatMap((row) =>
      row.principalId === null ? [] : [row.principalId],
    );
    if (principalIds.length > 0)
      await tx
        .update(principal)
        .set({ status: "deactivated", updatedAt: endedAt })
        .where(inArray(principal.id, principalIds));
    await dispatches.failUnsettled(
      run.id,
      "workflow_cancelled",
      run.cancellationReason ?? "Workflow cancelled",
      endedAt,
      tx,
    );
  }

  type ControlRequest = {
    target: AllocatedSidecarTarget;
    tenantId: string;
    runId: string;
    agentAddress: string;
    action: "cancel" | "stop";
    reason: string;
    timeoutMs: number;
  };

  async function forceRelease(command: ControlRequest): Promise<void> {
    await withRun(
      command.tenantId,
      command.runId,
      async (tx, original, allocation) => {
        const run = await projectTerminalForStop(tx, original);
        if (
          !isLiveWorkflowRunStatus(run.status) ||
          allocation === undefined ||
          allocation.generation !== command.target.generation ||
          allocation.status === "releasing" ||
          allocation.status === "released" ||
          allocation.status === "failed" ||
          allocation.status === "destroy_failed"
        )
          return;
        await allocations.beginRelease(
          {
            allocationId: allocation.id,
            expectedGeneration: allocation.generation,
            expectedStatus: allocation.status,
            failureCode: "workflow_stop_failed",
            failureMessage:
              "Workflow stop could not be confirmed; reclaiming capacity",
            now: now(),
          },
          tx,
        );
      },
    );
  }

  async function reconcileRun(tenantId: string, runId: string): Promise<void> {
    const command = await withRun(
      tenantId,
      runId,
      async (tx, original, allocation): Promise<ControlRequest | null> => {
        const stopping =
          original.cancellationRequestedAt !== null ||
          (original.expiresAt !== null && original.expiresAt <= now());
        let run = stopping
          ? await projectTerminalForStop(tx, original)
          : await projectTerminal(tx, original);
        if (isLiveWorkflowRunStatus(run.status)) {
          if (run.expiresAt !== null && run.expiresAt <= now()) {
            run = await beginCancellation(
              tx,
              run,
              "Maximum deployment lifetime exceeded",
            );
          }
          if (run.cancellationRequestedAt === null) return null;
          if (
            allocation === undefined ||
            allocation.status === "released" ||
            allocation.status === "failed"
          ) {
            await markStopped(tx, run);
            return null;
          }
          if (
            allocation.status === "releasing" ||
            allocation.status === "destroy_failed"
          )
            return null;
          if (
            allocation.status !== "allocated" ||
            allocation.sidecarId === null ||
            run.address === null
          ) {
            await allocations.beginRelease(
              {
                allocationId: allocation.id,
                expectedStatus: allocation.status,
                expectedGeneration: allocation.generation,
                now: now(),
              },
              tx,
            );
            return null;
          }
          const remaining =
            (run.cancellationDeadline ?? now()).getTime() - now().getTime();
          return {
            target: {
              allocationId: allocation.id,
              generation: allocation.generation,
            },
            tenantId,
            runId,
            agentAddress: run.address,
            action: remaining > 0 ? "cancel" : "stop",
            reason: run.cancellationReason ?? "Workflow cancelled",
            timeoutMs:
              remaining > 0 ? Math.max(1, Math.min(5_000, remaining)) : 10_000,
          };
        }
        if (
          allocation === undefined ||
          allocation.status === "released" ||
          allocation.status === "failed" ||
          allocation.status === "destroy_failed" ||
          allocation.status === "releasing"
        )
          return null;
        if (run.status === "deployed" || run.status === "running") return null;
        const retention = run.lifecyclePolicy?.capacityRetention?.[run.status];
        const releaseAt =
          run.capacityReleaseAt ??
          (retention === undefined || run.endedAt === null
            ? null
            : lifecycleDeadline(run.endedAt, retention));
        if (releaseAt === null) return null;
        if (run.capacityReleaseAt === null)
          await tx
            .update(workflowRun)
            .set({ capacityReleaseAt: releaseAt })
            .where(eq(workflowRun.id, runId));
        if (releaseAt > now()) return null;
        await allocations.beginRelease(
          {
            allocationId: allocation.id,
            expectedStatus: allocation.status,
            expectedGeneration: allocation.generation,
            now: now(),
          },
          tx,
        );
        return null;
      },
    );
    if (command === null) return;
    try {
      if (sendControl === undefined)
        throw new Error("Workflow control is unavailable");
      await sendControl(
        command.target,
        {
          runId,
          agentAddress: command.agentAddress,
          action: command.action,
          reason: command.reason,
        },
        command.timeoutMs,
      );
      if (command.action === "stop") {
        await withRun(tenantId, runId, async (tx, original, allocation) => {
          const run = await projectTerminalForStop(tx, original);
          if (
            allocation?.generation === command.target.generation &&
            isLiveWorkflowRunStatus(run.status)
          )
            await markStopped(tx, run);
        });
      }
    } catch (error) {
      logger.warn`Workflow ${command.action} failed for ${runId}: ${error instanceof Error ? error.message : String(error)}`;
      if (command.action === "stop") await forceRelease(command);
    }
  }

  async function reconcile(): Promise<void> {
    const candidates = await db
      .select({ id: workflowRun.id, tenantId: workflowRun.tenantId })
      .from(workflowRun)
      .leftJoin(
        sidecarAllocation,
        eq(sidecarAllocation.anchorRunId, workflowRun.id),
      )
      .where(
        and(
          eq(workflowRun.id, workflowRun.anchorRunId),
          or(
            isNotNull(workflowRun.lifecyclePolicy),
            isNotNull(workflowRun.capacityReleaseAt),
            isNotNull(workflowRun.cancellationRequestedAt),
            isNotNull(workflowRun.expiresAt),
          ),
          or(
            inArray(sidecarAllocation.status, [
              "pending",
              "provisioning",
              "allocated",
              "replacing",
            ]),
            and(
              inArray(workflowRun.status, ["deployed", "running"]),
              or(
                isNotNull(workflowRun.cancellationRequestedAt),
                isNotNull(workflowRun.expiresAt),
              ),
            ),
          ),
        ),
      );
    await Promise.all(
      candidates.map(async (run) => {
        try {
          await reconcileRun(run.tenantId, run.id);
        } catch (error) {
          logger.error`Lifecycle reconciliation failed for ${run.id}: ${error instanceof Error ? error.message : String(error)}`;
        }
      }),
    );
  }

  return { getStatus, releaseCapacity, requestCancellation, reconcile };
}

export type WorkflowLifecycleService = ReturnType<
  typeof createWorkflowLifecycleService
>;
