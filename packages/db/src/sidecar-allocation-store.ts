import { type } from "arktype";
import {
  and,
  asc,
  eq,
  gt,
  inArray,
  isNull,
  lte,
  notInArray,
  or,
  sql,
} from "drizzle-orm";

import {
  sidecarAllocationStatuses,
  type SidecarAllocationStatus,
} from "@intx/types";

import type { DB, DBExecutor } from "./client";
import { createWorkflowRunDispatchStore } from "./workflow-run-dispatch-store";
import { canExecuteWorkflowRun } from "./workflow-lifecycle-policy";
import {
  isLiveWorkflowRunStatus,
  liveWorkflowRunStatuses,
  principal,
  sidecar,
  sidecarAllocation,
  workflowRun,
  workflowRunLaunchSpec,
  type WorkflowRunCredentialRefs,
} from "./schema";

type DBHandle = DB["db"];
type SidecarAllocationRow = typeof sidecarAllocation.$inferSelect;

const SidecarAllocationStatusValidator = type.enumerated(
  ...sidecarAllocationStatuses,
);
const SidecarProvisionerApiVersion = type("1");

const activeStatuses = [
  "pending",
  "provisioning",
  "allocated",
  "replacing",
  "releasing",
] as const;

export type SidecarAllocation = {
  readonly id: string;
  readonly anchorRunId: string;
  readonly tenantId: string;
  readonly provisionerId: string;
  readonly provisionerApiVersion: 1;
  readonly provisionerBindingFingerprint: string;
  readonly sidecarId?: string;
  readonly status: SidecarAllocationStatus;
  readonly generation: number;
  readonly ensureAcceptedGeneration?: number;
  readonly externalRef?: string;
  readonly nextAttemptAt?: Date;
  readonly reconciliationLeaseId?: string;
  readonly reconciliationLeaseExpiresAt?: Date;
  /** Outstanding deploy attempt, retained after its reconciliation lease ends. */
  readonly initializationLeaseId?: string;
  readonly ensureAttempts: number;
  readonly destroyAttempts: number;
  readonly connectDeadline?: Date;
  readonly failureCode?: string;
  readonly failureMessage?: string;
  readonly createdAt: Date;
  readonly updatedAt: Date;
};

export type CreatePendingSidecarAllocationArgs = {
  readonly id: string;
  readonly anchorRunId: string;
  readonly tenantId: string;
  readonly provisionerId: string;
  readonly provisionerApiVersion: 1;
  readonly provisionerBindingFingerprint: string;
  readonly now?: Date;
};

export type CreateAdoptedSidecarAllocationArgs =
  CreatePendingSidecarAllocationArgs & {
    readonly sidecarId: string;
    readonly generation: number;
    readonly externalRef?: string;
    readonly connectDeadline: Date;
  };

export type ClaimSidecarAllocationArgs = {
  readonly excludedAllocationIds?: readonly string[];
  readonly leaseId: string;
  readonly leaseDurationMs: number;
};

export type ParkSidecarReconciliationPolicy =
  | {
      readonly kind: "await-connection";
      readonly fallbackNextAttemptAt: Date;
    }
  | {
      readonly kind: "retry-after-error";
      readonly notBefore: Date;
    };

export type BindInitialSidecarArgs = {
  readonly allocationId: string;
  readonly expectedGeneration: number;
  readonly sidecarId: string;
  readonly tokenHashSha256: Uint8Array;
  readonly connectDeadline: Date;
  readonly expectedLeaseId?: string;
  readonly now?: Date;
};

export type BindReplacementSidecarArgs = {
  readonly allocationId: string;
  readonly generation: number;
  readonly sidecarId: string;
  readonly tokenHashSha256: Uint8Array;
  readonly connectDeadline: Date;
  readonly expectedLeaseId?: string;
  readonly now?: Date;
};

export type MarkSidecarAllocatedArgs = {
  readonly allocationId: string;
  readonly generation: number;
  readonly externalRef?: string;
  readonly expectedLeaseId?: string;
  readonly now?: Date;
};

export type ScheduleSidecarAllocationRetryArgs = {
  readonly allocationId: string;
  readonly expectedStatus: (typeof activeStatuses)[number];
  readonly expectedGeneration: number;
  readonly nextAttemptAt: Date;
  readonly expectedLeaseId?: string;
  readonly attempt?: "ensure" | "destroy";
  readonly failure?: {
    readonly code: string;
    readonly message: string;
  };
  readonly now?: Date;
};

export type BeginSidecarReplacementArgs = {
  readonly allocationId: string;
  readonly expectedStatus: "provisioning" | "allocated";
  readonly expectedGeneration: number;
  readonly expectedLeaseId: string;
  readonly onlyIfInitializationIncomplete?: boolean;
  readonly expectedInitializationLeaseId?: string;
  readonly nextAttemptAt: Date;
  readonly failureCode: string;
  readonly failureMessage: string;
  readonly now?: Date;
};

export type BeginSidecarReleaseArgs = {
  readonly allocationId: string;
  readonly expectedStatus: Exclude<
    (typeof activeStatuses)[number],
    "releasing"
  >;
  readonly expectedGeneration: number;
  readonly failureCode?: string;
  readonly failureMessage?: string;
  readonly expectedLeaseId?: string;
  readonly expectedInitializationLeaseId?: string;
  readonly now?: Date;
};

export type BeginUnrecoverableSidecarReleaseArgs = {
  readonly allocationId: string;
  readonly expectedGeneration: number;
  readonly expectedLeaseId: string;
  readonly onlyIfInitializationIncomplete?: boolean;
  readonly expectedInitializationLeaseId?: string;
  readonly failureCode: string;
  readonly failureMessage: string;
  readonly now?: Date;
};

export type MarkSidecarReleasedArgs = {
  readonly allocationId: string;
  readonly generation: number;
  readonly expectedLeaseId?: string;
  readonly now?: Date;
};

export type MarkSidecarConnectionReadyArgs = {
  readonly allocationId: string;
  readonly generation: number;
  readonly expectedLeaseId?: string;
  readonly now?: Date;
};

export type MarkSidecarConnectionLostArgs = {
  readonly allocationId: string;
  readonly generation: number;
  readonly connectDeadline: Date;
  readonly now?: Date;
};

export type ScheduleSidecarReconnectIfUnscheduledArgs = {
  readonly allocationId: string;
  readonly generation: number;
  readonly connectDeadline: Date;
  readonly now?: Date;
};

export type FailSidecarAllocationArgs = {
  readonly allocationId: string;
  readonly expectedStatus: "pending" | "provisioning";
  readonly expectedGeneration: number;
  readonly code: string;
  readonly message: string;
  readonly expectedLeaseId?: string;
  readonly now?: Date;
};

export type MarkSidecarDestroyFailedArgs = Omit<
  FailSidecarAllocationArgs,
  "expectedStatus"
>;

type InitializationArgs = {
  readonly allocationId: string;
  readonly generation: number;
  readonly anchorRunId: string;
  readonly tenantId: string;
  readonly leaseId: string;
  readonly signal: AbortSignal;
};

function parseSidecarAllocationRow(
  row: SidecarAllocationRow,
): SidecarAllocation {
  return {
    id: row.id,
    anchorRunId: row.anchorRunId,
    tenantId: row.tenantId,
    provisionerId: row.provisionerId,
    provisionerApiVersion: SidecarProvisionerApiVersion.assert(
      row.provisionerApiVersion,
    ),
    provisionerBindingFingerprint: row.provisionerBindingFingerprint,
    ...(row.sidecarId !== null ? { sidecarId: row.sidecarId } : {}),
    status: SidecarAllocationStatusValidator.assert(row.status),
    generation: row.generation,
    ...(row.ensureAcceptedGeneration !== null
      ? { ensureAcceptedGeneration: row.ensureAcceptedGeneration }
      : {}),
    ...(row.externalRef !== null ? { externalRef: row.externalRef } : {}),
    ...(row.nextAttemptAt !== null ? { nextAttemptAt: row.nextAttemptAt } : {}),
    ...(row.reconciliationLeaseId !== null
      ? { reconciliationLeaseId: row.reconciliationLeaseId }
      : {}),
    ...(row.reconciliationLeaseExpiresAt !== null
      ? { reconciliationLeaseExpiresAt: row.reconciliationLeaseExpiresAt }
      : {}),
    ...(row.initializationLeaseId !== null
      ? { initializationLeaseId: row.initializationLeaseId }
      : {}),
    ensureAttempts: row.ensureAttempts,
    destroyAttempts: row.destroyAttempts,
    ...(row.connectDeadline !== null
      ? { connectDeadline: row.connectDeadline }
      : {}),
    ...(row.failureCode !== null ? { failureCode: row.failureCode } : {}),
    ...(row.failureMessage !== null
      ? { failureMessage: row.failureMessage }
      : {}),
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

function databaseTimestamp(override?: Date) {
  return override ?? sql`now()`;
}

function leaseCondition(expectedLeaseId?: string) {
  return expectedLeaseId === undefined
    ? []
    : [
        eq(sidecarAllocation.reconciliationLeaseId, expectedLeaseId),
        gt(
          sidecarAllocation.reconciliationLeaseExpiresAt,
          sql`clock_timestamp()`,
        ),
      ];
}

export function createSidecarAllocationStore(db: DBHandle) {
  const workflowRunDispatchStore = createWorkflowRunDispatchStore(db);

  function initializationConditions(
    args: InitializationArgs,
    expectedMarker: string | null,
    { requireCurrentLease = true }: { requireCurrentLease?: boolean } = {},
  ) {
    return and(
      eq(sidecarAllocation.id, args.allocationId),
      eq(sidecarAllocation.anchorRunId, args.anchorRunId),
      eq(sidecarAllocation.tenantId, args.tenantId),
      eq(sidecarAllocation.status, "allocated"),
      eq(sidecarAllocation.generation, args.generation),
      eq(sidecarAllocation.ensureAcceptedGeneration, args.generation),
      ...(requireCurrentLease ? leaseCondition(args.leaseId) : []),
      expectedMarker === null
        ? isNull(sidecarAllocation.initializationLeaseId)
        : eq(sidecarAllocation.initializationLeaseId, expectedMarker),
    );
  }

  async function writeInitialization(
    args: InitializationArgs,
    completion?: {
      readonly publicKey: string;
      readonly credentialRefs?: WorkflowRunCredentialRefs;
    },
  ): Promise<{ previousPublicKey: string | null } | null> {
    args.signal.throwIfAborted();
    return db.transaction(async (tx) => {
      const condition = initializationConditions(
        args,
        completion === undefined ? null : args.leaseId,
      );
      const [allocation] = await tx
        .select({ id: sidecarAllocation.id })
        .from(sidecarAllocation)
        .where(condition)
        .for("update");
      args.signal.throwIfAborted();
      if (allocation === undefined) return null;
      let previousPublicKey: string | null = null;
      if (completion === undefined) {
        const [previous] = await tx
          .select({ publicKey: workflowRun.publicKey })
          .from(workflowRun)
          .where(
            and(
              eq(workflowRun.id, args.anchorRunId),
              eq(workflowRun.anchorRunId, args.anchorRunId),
              eq(workflowRun.tenantId, args.tenantId),
            ),
          )
          .for("update");
        if (previous === undefined)
          throw new Error("Initialization anchor is missing");
        previousPublicKey = previous.publicKey;
      }
      const [anchor] = await tx
        .update(workflowRun)
        .set({
          publicKey: completion?.publicKey ?? null,
          ...(completion?.credentialRefs !== undefined
            ? { credentialRefs: completion.credentialRefs }
            : {}),
        })
        .where(
          and(
            eq(workflowRun.id, args.anchorRunId),
            eq(workflowRun.anchorRunId, args.anchorRunId),
            eq(workflowRun.tenantId, args.tenantId),
          ),
        )
        .returning({ id: workflowRun.id });
      if (anchor === undefined)
        throw new Error("Initialization anchor is missing");
      // Check the lease again after waiting on the anchor row. Both writes roll
      // back if ownership expired while the transaction held the allocation lock.
      const [updated] = await tx
        .update(sidecarAllocation)
        .set({
          initializationLeaseId: completion === undefined ? args.leaseId : null,
        })
        .where(condition)
        .returning({ id: sidecarAllocation.id });
      args.signal.throwIfAborted();
      if (updated === undefined)
        throw new Error("Initialization lease expired");
      return { previousPublicKey };
    });
  }

  async function initializationCompleted(
    tx: DBExecutor,
    args: BeginSidecarReplacementArgs | BeginUnrecoverableSidecarReleaseArgs,
  ): Promise<boolean> {
    const [allocation] = await tx
      .select({
        anchorRunId: sidecarAllocation.anchorRunId,
        initializationLeaseId: sidecarAllocation.initializationLeaseId,
      })
      .from(sidecarAllocation)
      .where(
        and(
          eq(sidecarAllocation.id, args.allocationId),
          eq(sidecarAllocation.generation, args.expectedGeneration),
          ...leaseCondition(args.expectedLeaseId),
        ),
      )
      .for("update");
    if (allocation === undefined || allocation.initializationLeaseId !== null)
      return false;
    // Read the key after acquiring the allocation lock, so a completion that
    // committed while we waited is visible even if its response was lost.
    const [anchor] = await tx
      .select({ publicKey: workflowRun.publicKey })
      .from(workflowRun)
      .where(eq(workflowRun.id, allocation.anchorRunId));
    return anchor !== undefined && anchor.publicKey !== null;
  }

  async function insertSidecarIdentity(
    tx: DBExecutor,
    args: {
      sidecarId: string;
      tokenHashSha256: Uint8Array;
      now: Date | ReturnType<typeof sql>;
    },
  ): Promise<void> {
    await tx.insert(sidecar).values({
      id: args.sidecarId,
      url: null,
      tokenHashSha256: args.tokenHashSha256,
      status: "offline",
      createdAt: args.now,
      updatedAt: args.now,
    });
  }

  async function failRunningRuns(
    tx: DBExecutor,
    anchorRunId: string,
    now: Date | ReturnType<typeof sql>,
  ): Promise<void> {
    // Fail every live run anchored here -- both "running" runs and a "deployed"
    // anchor torn down before its first trigger -- so a release settles them.
    const failedRuns = await tx
      .update(workflowRun)
      .set({ status: "failed", endedAt: now })
      .where(
        and(
          eq(workflowRun.anchorRunId, anchorRunId),
          inArray(workflowRun.status, [...liveWorkflowRunStatuses]),
        ),
      )
      .returning({ principalId: workflowRun.principalId });
    const principalIds = failedRuns.flatMap(({ principalId }) =>
      principalId === null ? [] : [principalId],
    );
    if (principalIds.length > 0) {
      await tx
        .update(principal)
        .set({ status: "deactivated", updatedAt: now })
        .where(inArray(principal.id, principalIds));
    }
  }

  async function beginRelease(
    args: BeginSidecarReleaseArgs,
    tx?: DBExecutor,
  ): Promise<SidecarAllocation | null> {
    const now = databaseTimestamp(args.now);
    const [updated] = await (tx ?? db)
      .update(sidecarAllocation)
      .set({
        status: "releasing",
        generation: args.expectedGeneration + 1,
        initializationLeaseId: null,
        nextAttemptAt: now,
        reconciliationLeaseId: null,
        reconciliationLeaseExpiresAt: null,
        connectDeadline: null,
        ...(args.failureCode !== undefined
          ? { failureCode: args.failureCode }
          : {}),
        ...(args.failureMessage !== undefined
          ? { failureMessage: args.failureMessage }
          : {}),
        updatedAt: now,
      })
      .where(
        and(
          eq(sidecarAllocation.id, args.allocationId),
          eq(sidecarAllocation.status, args.expectedStatus),
          eq(sidecarAllocation.generation, args.expectedGeneration),
          ...leaseCondition(args.expectedLeaseId),
          // A caller without the lease cannot interrupt an active reconciler.
          ...(args.expectedLeaseId === undefined
            ? [
                or(
                  isNull(sidecarAllocation.reconciliationLeaseExpiresAt),
                  lte(
                    sidecarAllocation.reconciliationLeaseExpiresAt,
                    sql`clock_timestamp()`,
                  ),
                ),
              ]
            : []),
          ...(args.expectedInitializationLeaseId !== undefined
            ? [
                eq(
                  sidecarAllocation.initializationLeaseId,
                  args.expectedInitializationLeaseId,
                ),
              ]
            : []),
        ),
      )
      .returning();
    return updated === undefined ? null : parseSidecarAllocationRow(updated);
  }

  return {
    beginInitialization(args: InitializationArgs) {
      return writeInitialization(args);
    },

    async completeInitialization(
      args: InitializationArgs & {
        readonly publicKey: string;
        readonly credentialRefs?: WorkflowRunCredentialRefs;
      },
    ): Promise<boolean> {
      return (await writeInitialization(args, args)) !== null;
    },

    async clearUnsentInitialization(
      args: InitializationArgs & { readonly previousPublicKey: string | null },
    ): Promise<boolean> {
      // No signal gate: the sole caller invokes this only for a proven-unsent
      // frame, which is reachable precisely when the attempt was cancelled.
      // The WHERE clause is the guard. Marker equality proves no newer
      // attempt began, and generation/status equality proves the allocation
      // did not move on. The reconciliation lease is deliberately not
      // required: a disconnect nulls the lease while leaving this attempt's
      // marker behind, and only this attempt's own clear may remove it. Restore
      // its previous key in the same transaction so absence of the marker cannot
      // expose a live workflow as keyless or certify a different attempt.
      return db.transaction(async (tx) => {
        const condition = initializationConditions(args, args.leaseId, {
          requireCurrentLease: false,
        });
        const [allocation] = await tx
          .select({ id: sidecarAllocation.id })
          .from(sidecarAllocation)
          .where(condition)
          .for("update");
        if (allocation === undefined) return false;
        const [anchor] = await tx
          .update(workflowRun)
          .set({ publicKey: args.previousPublicKey })
          .where(
            and(
              eq(workflowRun.id, args.anchorRunId),
              eq(workflowRun.anchorRunId, args.anchorRunId),
              eq(workflowRun.tenantId, args.tenantId),
            ),
          )
          .returning({ id: workflowRun.id });
        if (anchor === undefined)
          throw new Error("Initialization anchor is missing");
        const [updated] = await tx
          .update(sidecarAllocation)
          .set({ initializationLeaseId: null })
          .where(condition)
          .returning({ id: sidecarAllocation.id });
        if (updated === undefined)
          throw new Error("Initialization attempt changed before rollback");
        return true;
      });
    },

    async createPending(
      args: CreatePendingSidecarAllocationArgs,
      tx?: DBExecutor,
    ): Promise<SidecarAllocation> {
      const executor = tx ?? db;
      const [anchor] = await executor
        .select({
          tenantId: workflowRun.tenantId,
          anchorRunId: workflowRun.anchorRunId,
          status: workflowRun.status,
        })
        .from(workflowRun)
        .where(eq(workflowRun.id, args.anchorRunId))
        .limit(1);
      if (anchor === undefined) {
        throw new Error(
          `sidecarAllocationStore.createPending: anchor run ${args.anchorRunId} does not exist`,
        );
      }
      if (
        anchor.tenantId !== args.tenantId ||
        anchor.anchorRunId !== args.anchorRunId
      ) {
        throw new Error(
          `sidecarAllocationStore.createPending: run ${args.anchorRunId} is not an anchor for tenant ${args.tenantId}`,
        );
      }
      if (!isLiveWorkflowRunStatus(anchor.status)) {
        throw new Error(
          `sidecarAllocationStore.createPending: anchor run ${args.anchorRunId} is ${anchor.status}, expected a live run`,
        );
      }
      const launchSpec = await executor.query.workflowRunLaunchSpec.findFirst({
        columns: { anchorRunId: true },
        where: eq(workflowRunLaunchSpec.anchorRunId, args.anchorRunId),
      });
      if (launchSpec === undefined) {
        throw new Error(
          `sidecarAllocationStore.createPending: anchor run ${args.anchorRunId} has no launch specification`,
        );
      }
      if (args.provisionerApiVersion !== 1) {
        throw new Error(
          "sidecarAllocationStore.createPending: unsupported API version",
        );
      }
      const now = databaseTimestamp(args.now);
      const [inserted] = await executor
        .insert(sidecarAllocation)
        .values({
          id: args.id,
          anchorRunId: args.anchorRunId,
          tenantId: args.tenantId,
          provisionerId: args.provisionerId,
          provisionerApiVersion: args.provisionerApiVersion,
          provisionerBindingFingerprint: args.provisionerBindingFingerprint,
          status: "pending",
          generation: 0,
          nextAttemptAt: now,
          createdAt: now,
          updatedAt: now,
        })
        .returning();
      if (inserted === undefined) {
        throw new Error(
          `sidecarAllocationStore.createPending: insert returned no row for ${args.id}`,
        );
      }
      return parseSidecarAllocationRow(inserted);
    },

    async createAdopted(
      args: CreateAdoptedSidecarAllocationArgs,
      tx?: DBExecutor,
    ): Promise<SidecarAllocation> {
      const createdAt = databaseTimestamp(args.now);
      const [inserted] = await (tx ?? db)
        .insert(sidecarAllocation)
        .values({
          id: args.id,
          anchorRunId: args.anchorRunId,
          tenantId: args.tenantId,
          provisionerId: args.provisionerId,
          provisionerApiVersion: args.provisionerApiVersion,
          provisionerBindingFingerprint: args.provisionerBindingFingerprint,
          sidecarId: args.sidecarId,
          status: "allocated",
          generation: args.generation,
          ensureAcceptedGeneration: args.generation,
          ...(args.externalRef !== undefined
            ? { externalRef: args.externalRef }
            : {}),
          connectDeadline: args.connectDeadline,
          nextAttemptAt: createdAt,
          createdAt,
          updatedAt: createdAt,
        })
        .returning();
      if (inserted === undefined) {
        throw new Error(
          `sidecarAllocationStore.createAdopted: insert returned no row for ${args.id}`,
        );
      }
      return parseSidecarAllocationRow(inserted);
    },

    async bindInitialSidecar(
      args: BindInitialSidecarArgs,
    ): Promise<SidecarAllocation | null> {
      return db.transaction(async (tx) => {
        const [allocation] = await tx
          .select()
          .from(sidecarAllocation)
          .where(
            and(
              eq(sidecarAllocation.id, args.allocationId),
              ...leaseCondition(args.expectedLeaseId),
            ),
          )
          .limit(1)
          .for("update");
        if (
          allocation === undefined ||
          allocation.status !== "pending" ||
          allocation.generation !== args.expectedGeneration ||
          (args.expectedLeaseId !== undefined &&
            allocation.reconciliationLeaseId !== args.expectedLeaseId)
        ) {
          return null;
        }
        const now = databaseTimestamp(args.now);
        await insertSidecarIdentity(tx, {
          sidecarId: args.sidecarId,
          tokenHashSha256: args.tokenHashSha256,
          now,
        });
        const [updated] = await tx
          .update(sidecarAllocation)
          .set({
            sidecarId: args.sidecarId,
            status: "provisioning",
            generation: args.expectedGeneration + 1,
            ensureAcceptedGeneration: null,
            externalRef: null,
            connectDeadline: args.connectDeadline,
            nextAttemptAt: now,
            failureCode: null,
            failureMessage: null,
            updatedAt: now,
          })
          .where(
            and(
              eq(sidecarAllocation.id, args.allocationId),
              eq(sidecarAllocation.status, "pending"),
              eq(sidecarAllocation.generation, args.expectedGeneration),
              ...leaseCondition(args.expectedLeaseId),
            ),
          )
          .returning();
        if (updated === undefined) {
          throw new Error(
            `sidecarAllocationStore.bindInitialSidecar: locked allocation ${args.allocationId} changed before update`,
          );
        }
        return parseSidecarAllocationRow(updated);
      });
    },

    async bindReplacementSidecar(
      args: BindReplacementSidecarArgs,
    ): Promise<SidecarAllocation | null> {
      return db.transaction(async (tx) => {
        const [allocation] = await tx
          .select()
          .from(sidecarAllocation)
          .where(
            and(
              eq(sidecarAllocation.id, args.allocationId),
              ...leaseCondition(args.expectedLeaseId),
            ),
          )
          .limit(1)
          .for("update");
        if (
          allocation === undefined ||
          allocation.status !== "replacing" ||
          allocation.generation !== args.generation ||
          (args.expectedLeaseId !== undefined &&
            allocation.reconciliationLeaseId !== args.expectedLeaseId)
        ) {
          return null;
        }
        const now = databaseTimestamp(args.now);
        await insertSidecarIdentity(tx, {
          sidecarId: args.sidecarId,
          tokenHashSha256: args.tokenHashSha256,
          now,
        });
        const [updated] = await tx
          .update(sidecarAllocation)
          .set({
            sidecarId: args.sidecarId,
            status: "provisioning",
            ensureAcceptedGeneration: null,
            externalRef: null,
            connectDeadline: args.connectDeadline,
            nextAttemptAt: now,
            destroyAttempts: sql`${sidecarAllocation.destroyAttempts} + 1`,
            failureCode: null,
            failureMessage: null,
            updatedAt: now,
          })
          .where(
            and(
              eq(sidecarAllocation.id, args.allocationId),
              eq(sidecarAllocation.status, "replacing"),
              eq(sidecarAllocation.generation, args.generation),
              ...leaseCondition(args.expectedLeaseId),
            ),
          )
          .returning();
        if (updated === undefined) {
          throw new Error(
            `sidecarAllocationStore.bindReplacementSidecar: locked allocation ${args.allocationId} changed before update`,
          );
        }
        return parseSidecarAllocationRow(updated);
      });
    },

    async markAllocated(
      args: MarkSidecarAllocatedArgs,
    ): Promise<SidecarAllocation | null> {
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          status: "allocated",
          ensureAcceptedGeneration: args.generation,
          externalRef: args.externalRef ?? null,
          nextAttemptAt: sidecarAllocation.connectDeadline,
          ensureAttempts: sql`${sidecarAllocation.ensureAttempts} + 1`,
          failureCode: null,
          failureMessage: null,
          updatedAt: databaseTimestamp(args.now),
        })
        .where(
          and(
            eq(sidecarAllocation.id, args.allocationId),
            eq(sidecarAllocation.status, "provisioning"),
            eq(sidecarAllocation.generation, args.generation),
            ...leaseCondition(args.expectedLeaseId),
          ),
        )
        .returning();
      return updated === undefined ? null : parseSidecarAllocationRow(updated);
    },

    async scheduleRetry(
      args: ScheduleSidecarAllocationRetryArgs,
    ): Promise<SidecarAllocation | null> {
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          nextAttemptAt: args.nextAttemptAt,
          reconciliationLeaseId: null,
          reconciliationLeaseExpiresAt: null,
          ...(args.attempt === "ensure"
            ? { ensureAttempts: sql`${sidecarAllocation.ensureAttempts} + 1` }
            : {}),
          ...(args.attempt === "destroy"
            ? { destroyAttempts: sql`${sidecarAllocation.destroyAttempts} + 1` }
            : {}),
          ...(args.failure !== undefined
            ? {
                failureCode: args.failure.code,
                failureMessage: args.failure.message,
              }
            : {}),
          updatedAt: databaseTimestamp(args.now),
        })
        .where(
          and(
            eq(sidecarAllocation.id, args.allocationId),
            eq(sidecarAllocation.status, args.expectedStatus),
            eq(sidecarAllocation.generation, args.expectedGeneration),
            ...leaseCondition(args.expectedLeaseId),
          ),
        )
        .returning();
      return updated === undefined ? null : parseSidecarAllocationRow(updated);
    },

    async beginReplacement(
      args: BeginSidecarReplacementArgs,
    ): Promise<SidecarAllocation | null> {
      const now = databaseTimestamp(args.now);
      return db.transaction(async (tx) => {
        if (
          args.onlyIfInitializationIncomplete &&
          (await initializationCompleted(tx, args))
        )
          return null;
        const [updated] = await tx
          .update(sidecarAllocation)
          .set({
            status: "replacing",
            generation: args.expectedGeneration + 1,
            initializationLeaseId: null,
            ensureAcceptedGeneration: null,
            nextAttemptAt: args.nextAttemptAt,
            reconciliationLeaseId: null,
            reconciliationLeaseExpiresAt: null,
            connectDeadline: null,
            failureCode: args.failureCode,
            failureMessage: args.failureMessage,
            updatedAt: now,
          })
          .where(
            and(
              eq(sidecarAllocation.id, args.allocationId),
              eq(sidecarAllocation.status, args.expectedStatus),
              eq(sidecarAllocation.generation, args.expectedGeneration),
              ...leaseCondition(args.expectedLeaseId),
              ...(args.expectedInitializationLeaseId !== undefined
                ? [
                    eq(
                      sidecarAllocation.initializationLeaseId,
                      args.expectedInitializationLeaseId,
                    ),
                  ]
                : []),
            ),
          )
          .returning();
        if (updated === undefined) return null;

        // The anchor public key is durable proof that this allocation
        // generation completed restore and deployment. Clear it atomically
        // with the generation advance so a replacement cannot mistake stale
        // local sidecar state for a successfully restored workflow.
        await tx
          .update(workflowRun)
          .set({ publicKey: null })
          .where(eq(workflowRun.id, updated.anchorRunId));

        return parseSidecarAllocationRow(updated);
      });
    },

    beginRelease,

    async beginUnrecoverableRelease(
      args: BeginUnrecoverableSidecarReleaseArgs,
    ): Promise<SidecarAllocation | null> {
      const now = databaseTimestamp(args.now);
      return db.transaction(async (tx) => {
        if (
          args.onlyIfInitializationIncomplete &&
          (await initializationCompleted(tx, args))
        )
          return null;
        const releasing = await beginRelease(
          {
            ...args,
            expectedStatus: "allocated",
          },
          tx,
        );
        if (releasing === null) return null;

        await failRunningRuns(tx, releasing.anchorRunId, now);
        await workflowRunDispatchStore.failUnsettled(
          releasing.anchorRunId,
          args.failureCode,
          args.failureMessage,
          now,
          tx,
        );
        return releasing;
      });
    },

    async markReleased(
      args: MarkSidecarReleasedArgs,
    ): Promise<SidecarAllocation | null> {
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          status: "released",
          nextAttemptAt: null,
          reconciliationLeaseId: null,
          reconciliationLeaseExpiresAt: null,
          connectDeadline: null,
          destroyAttempts: sql`${sidecarAllocation.destroyAttempts} + 1`,
          updatedAt: databaseTimestamp(args.now),
        })
        .where(
          and(
            eq(sidecarAllocation.id, args.allocationId),
            eq(sidecarAllocation.status, "releasing"),
            eq(sidecarAllocation.generation, args.generation),
            ...leaseCondition(args.expectedLeaseId),
          ),
        )
        .returning();
      return updated === undefined ? null : parseSidecarAllocationRow(updated);
    },

    async markDestroyFailed(
      args: MarkSidecarDestroyFailedArgs,
    ): Promise<SidecarAllocation | null> {
      const now = databaseTimestamp(args.now);
      return db.transaction(async (tx) => {
        const [updated] = await tx
          .update(sidecarAllocation)
          .set({
            status: "destroy_failed",
            failureCode: args.code,
            failureMessage: args.message,
            nextAttemptAt: null,
            reconciliationLeaseId: null,
            reconciliationLeaseExpiresAt: null,
            connectDeadline: null,
            destroyAttempts: sql`${sidecarAllocation.destroyAttempts} + 1`,
            updatedAt: now,
          })
          .where(
            and(
              eq(sidecarAllocation.id, args.allocationId),
              inArray(sidecarAllocation.status, ["replacing", "releasing"]),
              eq(sidecarAllocation.generation, args.expectedGeneration),
              ...leaseCondition(args.expectedLeaseId),
            ),
          )
          .returning();
        if (updated === undefined) return null;

        await failRunningRuns(tx, updated.anchorRunId, now);
        await workflowRunDispatchStore.failUnsettled(
          updated.anchorRunId,
          args.code,
          args.message,
          now,
          tx,
        );
        return parseSidecarAllocationRow(updated);
      });
    },

    async failWithoutInfrastructure(
      args: FailSidecarAllocationArgs,
      tx?: DBExecutor,
    ): Promise<SidecarAllocation | null> {
      const now = databaseTimestamp(args.now);
      const fail = async (
        executor: DBExecutor,
      ): Promise<SidecarAllocation | null> => {
        const [updated] = await executor
          .update(sidecarAllocation)
          .set({
            status: "failed",
            failureCode: args.code,
            failureMessage: args.message,
            nextAttemptAt: null,
            reconciliationLeaseId: null,
            reconciliationLeaseExpiresAt: null,
            connectDeadline: null,
            updatedAt: now,
          })
          .where(
            and(
              eq(sidecarAllocation.id, args.allocationId),
              eq(sidecarAllocation.status, args.expectedStatus),
              eq(sidecarAllocation.generation, args.expectedGeneration),
              ...leaseCondition(args.expectedLeaseId),
            ),
          )
          .returning();
        if (updated === undefined) return null;

        await failRunningRuns(executor, updated.anchorRunId, now);
        await workflowRunDispatchStore.failUnsettled(
          updated.anchorRunId,
          args.code,
          args.message,
          now,
          executor,
        );
        return parseSidecarAllocationRow(updated);
      };
      return tx === undefined ? db.transaction(fail) : fail(tx);
    },

    async hasRunnableAnchor(
      anchorRunId: string,
      now = new Date(),
    ): Promise<boolean> {
      const run = await db.query.workflowRun.findFirst({
        where: and(
          eq(workflowRun.id, anchorRunId),
          eq(workflowRun.anchorRunId, anchorRunId),
        ),
        columns: {
          status: true,
          expiresAt: true,
          cancellationRequestedAt: true,
        },
      });
      return run !== undefined && canExecuteWorkflowRun(run, now);
    },

    async findById(id: string): Promise<SidecarAllocation | null> {
      const row = await db.query.sidecarAllocation.findFirst({
        where: eq(sidecarAllocation.id, id),
      });
      return row === undefined ? null : parseSidecarAllocationRow(row);
    },

    async findByAnchorRunId(
      anchorRunId: string,
    ): Promise<SidecarAllocation | null> {
      const row = await db.query.sidecarAllocation.findFirst({
        where: eq(sidecarAllocation.anchorRunId, anchorRunId),
      });
      return row === undefined ? null : parseSidecarAllocationRow(row);
    },

    async claimNextReconcilable(
      args: ClaimSidecarAllocationArgs,
    ): Promise<SidecarAllocation | null> {
      return db.transaction(async (tx) => {
        const [candidate] = await tx
          .select({ id: sidecarAllocation.id })
          .from(sidecarAllocation)
          .where(
            and(
              inArray(sidecarAllocation.status, activeStatuses),
              lte(sidecarAllocation.nextAttemptAt, sql`now()`),
              ...(args.excludedAllocationIds !== undefined &&
              args.excludedAllocationIds.length > 0
                ? [
                    notInArray(sidecarAllocation.id, [
                      ...args.excludedAllocationIds,
                    ]),
                  ]
                : []),
              or(
                isNull(sidecarAllocation.reconciliationLeaseExpiresAt),
                lte(sidecarAllocation.reconciliationLeaseExpiresAt, sql`now()`),
              ),
            ),
          )
          .orderBy(
            asc(sidecarAllocation.nextAttemptAt),
            asc(sidecarAllocation.createdAt),
          )
          .limit(1)
          .for("update", { skipLocked: true });
        if (candidate === undefined) return null;
        const [claimed] = await tx
          .update(sidecarAllocation)
          .set({
            reconciliationLeaseId: args.leaseId,
            reconciliationLeaseExpiresAt: sql`now() + (${args.leaseDurationMs} * interval '1 millisecond')`,
          })
          .where(eq(sidecarAllocation.id, candidate.id))
          .returning();
        return claimed === undefined
          ? null
          : parseSidecarAllocationRow(claimed);
      });
    },

    async extendReconciliationLease(
      allocationId: string,
      leaseId: string,
      leaseDurationMs: number,
    ): Promise<boolean> {
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          reconciliationLeaseExpiresAt: sql`clock_timestamp() + (${leaseDurationMs} * interval '1 millisecond')`,
        })
        .where(
          and(
            eq(sidecarAllocation.id, allocationId),
            ...leaseCondition(leaseId),
          ),
        )
        .returning({ id: sidecarAllocation.id });
      return updated !== undefined;
    },

    async isReconciliationLeaseCurrent(
      allocationId: string,
      generation: number,
      leaseId: string,
    ): Promise<boolean> {
      const [allocation] = await db
        .select({ id: sidecarAllocation.id })
        .from(sidecarAllocation)
        .where(
          and(
            eq(sidecarAllocation.id, allocationId),
            eq(sidecarAllocation.generation, generation),
            ...leaseCondition(leaseId),
          ),
        )
        .limit(1);
      return allocation !== undefined;
    },

    async markConnectionReady(
      args: MarkSidecarConnectionReadyArgs,
    ): Promise<SidecarAllocation | null> {
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          connectDeadline: null,
          nextAttemptAt: null,
          reconciliationLeaseId: null,
          reconciliationLeaseExpiresAt: null,
          failureCode: null,
          failureMessage: null,
          updatedAt: databaseTimestamp(args.now),
        })
        .where(
          and(
            eq(sidecarAllocation.id, args.allocationId),
            eq(sidecarAllocation.status, "allocated"),
            eq(sidecarAllocation.generation, args.generation),
            eq(sidecarAllocation.ensureAcceptedGeneration, args.generation),
            isNull(sidecarAllocation.initializationLeaseId),
            ...leaseCondition(args.expectedLeaseId),
          ),
        )
        .returning();
      return updated === undefined ? null : parseSidecarAllocationRow(updated);
    },

    async markConnectionLost(
      args: MarkSidecarConnectionLostArgs,
    ): Promise<SidecarAllocation | null> {
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          connectDeadline: args.connectDeadline,
          nextAttemptAt: args.connectDeadline,
          reconciliationLeaseId: null,
          reconciliationLeaseExpiresAt: null,
          updatedAt: databaseTimestamp(args.now),
        })
        .where(
          and(
            eq(sidecarAllocation.id, args.allocationId),
            eq(sidecarAllocation.status, "allocated"),
            eq(sidecarAllocation.generation, args.generation),
            eq(sidecarAllocation.ensureAcceptedGeneration, args.generation),
          ),
        )
        .returning();
      return updated === undefined ? null : parseSidecarAllocationRow(updated);
    },

    async scheduleReconnectIfUnscheduled(
      args: ScheduleSidecarReconnectIfUnscheduledArgs,
    ): Promise<SidecarAllocation | null> {
      const connectDeadline = sql.param(
        args.connectDeadline,
        sidecarAllocation.connectDeadline,
      );
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          connectDeadline: sql`coalesce(${sidecarAllocation.connectDeadline}, ${connectDeadline})`,
          nextAttemptAt: sql`coalesce(${sidecarAllocation.connectDeadline}, ${connectDeadline})`,
          updatedAt: databaseTimestamp(args.now),
        })
        .where(
          and(
            eq(sidecarAllocation.id, args.allocationId),
            eq(sidecarAllocation.status, "allocated"),
            eq(sidecarAllocation.generation, args.generation),
            eq(sidecarAllocation.ensureAcceptedGeneration, args.generation),
            isNull(sidecarAllocation.nextAttemptAt),
            isNull(sidecarAllocation.reconciliationLeaseId),
            isNull(sidecarAllocation.reconciliationLeaseExpiresAt),
          ),
        )
        .returning();
      return updated === undefined ? null : parseSidecarAllocationRow(updated);
    },

    async parkReconciliation(
      allocationId: string,
      leaseId: string,
      policy: ParkSidecarReconciliationPolicy,
    ): Promise<boolean> {
      const fallbackNextAttemptAt =
        policy.kind === "await-connection"
          ? policy.fallbackNextAttemptAt
          : policy.notBefore;
      const fallback = sql.param(
        fallbackNextAttemptAt,
        sidecarAllocation.nextAttemptAt,
      );
      const nextAttemptAt =
        policy.kind === "await-connection"
          ? sql`case when ${sidecarAllocation.connectDeadline} is null then ${fallback} else coalesce(${sidecarAllocation.nextAttemptAt}, ${sidecarAllocation.connectDeadline}) end`
          : sql`greatest(coalesce(${sidecarAllocation.nextAttemptAt}, ${sidecarAllocation.connectDeadline}, ${fallback}), ${fallback})`;
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          nextAttemptAt,
          reconciliationLeaseId: null,
          reconciliationLeaseExpiresAt: null,
        })
        .where(
          and(
            eq(sidecarAllocation.id, allocationId),
            ...leaseCondition(leaseId),
          ),
        )
        .returning({ id: sidecarAllocation.id });
      return updated !== undefined;
    },

    async wakeReconciliation(
      allocationId: string,
      generation: number,
    ): Promise<boolean> {
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          nextAttemptAt: sql`now()`,
        })
        .where(
          and(
            eq(sidecarAllocation.id, allocationId),
            eq(sidecarAllocation.generation, generation),
            inArray(sidecarAllocation.status, activeStatuses),
          ),
        )
        .returning({ id: sidecarAllocation.id });
      return updated !== undefined;
    },

    async listActive(): Promise<SidecarAllocation[]> {
      const rows = await db
        .select()
        .from(sidecarAllocation)
        .where(inArray(sidecarAllocation.status, activeStatuses));
      return rows.map(parseSidecarAllocationRow);
    },
  };
}

export type SidecarAllocationStore = ReturnType<
  typeof createSidecarAllocationStore
>;
