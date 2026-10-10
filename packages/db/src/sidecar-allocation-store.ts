import { type } from "arktype";
import {
  and,
  asc,
  eq,
  gt,
  inArray,
  isNotNull,
  isNull,
  lte,
  notInArray,
  or,
  sql,
  type SQL,
} from "drizzle-orm";

import {
  sidecarAllocationStatuses,
  type SidecarAllocationStatus,
} from "@intx/types";

import type { DB, DBExecutor } from "./client";
import { createWorkflowPendingProjectionStore } from "./workflow-pending-projection-store";
import {
  assertSidecarHasRoom,
  assertSidecarReusable,
  lockSidecars,
  type SidecarAllocationStoreOptions,
} from "./sidecar-reuse";
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

// Why the Hub fails a deployment whose sidecar reported it stopped.
export const SIDECAR_DEPLOYMENT_STOPPED_FAILURE_CODE =
  "sidecar_deployment_stopped";
export const SIDECAR_CLEANUP_UNCONFIRMED_FAILURE_CODE =
  "sidecar_cleanup_unconfirmed";
export const SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE =
  "sidecar_cleanup_retry_exhausted";
export const SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE =
  "sidecar_cleanup_disconnect_timeout";

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
  /** Confirmed removal for this cleanup generation, retained across retries. */
  readonly deploymentCleanupConfirmed: boolean;
  /** Also bounds waiting for a cleanup connection while releasing. */
  readonly connectDeadline?: Date;
  /** How long the sidecar may stay disconnected before the Hub fails it. */
  readonly maxDisconnectedMs: number;
  /** When the first deploy first failed before its deploy frame was sent. */
  readonly firstDeployFailedAt?: Date;
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
  readonly maxDisconnectedMs: number;
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
  /**
   * An existing sidecar the provisioner placed this generation on instead of
   * starting the one bound to it. The unused bound identity is deleted.
   */
  readonly sidecarId?: string;
  readonly externalRef?: string;
  readonly expectedLeaseId?: string;
  readonly now?: Date;
};

export type ScheduleSidecarAllocationRetryArgs = {
  readonly allocationId: string;
  readonly expectedStatus: (typeof activeStatuses)[number];
  readonly expectedGeneration: number;
  readonly nextAttemptAt: Date;
  /** Minimum wait measured on the database clock, despite Hub clock skew. */
  readonly minimumDelayMs?: number;
  readonly expectedLeaseId?: string;
  readonly firstDeployFailedAt?: Date;
  readonly now?: Date;
} & (
  | {
      readonly attempt: "destroy";
      readonly failure?: never;
    }
  | {
      readonly attempt?: "ensure";
      readonly failure?: {
        readonly code: string;
        readonly message: string;
      };
    }
);

export type BeginSidecarReplacementArgs = {
  readonly allocationId: string;
  readonly expectedGeneration: number;
  readonly expectedLeaseId: string;
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

export type FailStoppedSidecarDeploymentArgs = {
  readonly allocationId: string;
  readonly expectedGeneration: number;
  readonly expectedLeaseId: string;
  readonly failureCode: string;
  readonly failureMessage: string;
  readonly now?: Date;
};

export type BeginUnrecoverableSidecarReleaseArgs = {
  readonly allocationId: string;
  readonly expectedStatus: "allocated" | "provisioning";
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
  readonly now?: Date;
  /** `now` plus the window a sidecar gets to connect the first time. */
  readonly firstConnectDeadline: Date;
};

export type ScheduleSidecarReconnectAfterHubStartArgs =
  MarkSidecarConnectionLostArgs;

export type ScheduleSidecarReconnectIfUnscheduledArgs =
  MarkSidecarConnectionLostArgs;

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
> & {
  /** A connection-wait deadline is not a failed cleanup attempt. */
  readonly countAttempt?: boolean;
};

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
    deploymentCleanupConfirmed: row.deploymentCleanupConfirmed,
    ...(row.connectDeadline !== null
      ? { connectDeadline: row.connectDeadline }
      : {}),
    maxDisconnectedMs: row.maxDisconnectedMs,
    ...(row.firstDeployFailedAt !== null
      ? { firstDeployFailedAt: row.firstDeployFailedAt }
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

// When a sidecar that lost its connection at `now` has stayed away for as
// long as its allocation allows.
function disconnectDeadline(now?: Date) {
  const start =
    now === undefined
      ? sql`now()`
      : sql`${sql.param(now, sidecarAllocation.connectDeadline)}::timestamp`;
  return sql`${start} + ${sidecarAllocation.maxDisconnectedMs} * interval '1 millisecond'`;
}

// The disconnect limit covers a deployment once its first deploy completed;
// until then a lost sidecar gets only the first-connect window.
function reconnectDeadline(
  args: MarkSidecarConnectionLostArgs,
  beforeFirstDeploy: SQL,
) {
  return sql`case when exists (select 1 from ${workflowRun} where ${workflowRun.id} = ${sidecarAllocation.anchorRunId} and ${workflowRun.publicKey} is not null) then ${disconnectDeadline(args.now)} else ${beforeFirstDeploy} end`;
}

function firstConnectDeadline(args: MarkSidecarConnectionLostArgs) {
  return sql`${sql.param(args.firstConnectDeadline, sidecarAllocation.connectDeadline)}::timestamp`;
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

// Stores used only for lifecycle updates cannot accidentally place work without
// the connection's current retained-inventory proof.
export function createSidecarAllocationStore(
  db: DBHandle,
): Omit<SidecarAllocationStore, "markAllocated" | "createAdopted">;
export function createSidecarAllocationStore(
  db: DBHandle,
  options: Required<SidecarAllocationStoreOptions>,
): SidecarAllocationStore;
export function createSidecarAllocationStore(
  db: DBHandle,
  options?: Required<SidecarAllocationStoreOptions>,
) {
  return buildSidecarAllocationStore(db, options);
}

function buildSidecarAllocationStore(
  db: DBHandle,
  options: SidecarAllocationStoreOptions = {},
) {
  const workflowRunDispatchStore = createWorkflowRunDispatchStore(db);
  const pendingProjections = createWorkflowPendingProjectionStore(db);

  async function lockAllocationWithSidecars(
    tx: DBExecutor,
    condition: SQL | undefined,
    destinationSidecarId?: string,
  ) {
    const [candidate] = await tx
      .select({ sidecarId: sidecarAllocation.sidecarId })
      .from(sidecarAllocation)
      .where(condition);
    if (candidate === undefined) return undefined;
    await lockSidecars(
      tx,
      [candidate.sidecarId, destinationSidecarId].filter((id) => id != null),
    );
    // A move changes status or generation, so rechecking the condition rejects
    // work whose binding moved while the sidecar lock was awaited.
    const [allocation] = await tx
      .select()
      .from(sidecarAllocation)
      .where(condition)
      .for("update");
    return allocation;
  }

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
  ): Promise<boolean> {
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
      if (allocation === undefined) return false;
      if (completion !== undefined) {
        const [anchor] = await tx
          .update(workflowRun)
          .set({
            publicKey: completion.publicKey,
            ...(completion.credentialRefs !== undefined
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
      }
      // Check the lease again: ownership can expire while the transaction holds
      // the allocation lock, such as while a completion waits on the anchor
      // row, and the completion's anchor write then rolls back with this one.
      const [updated] = await tx
        .update(sidecarAllocation)
        .set({
          initializationLeaseId: completion === undefined ? args.leaseId : null,
          // Completion proves the first connection succeeded. A later disconnect
          // acquires this same row lock and starts its own deadline.
          ...(completion === undefined ? {} : { connectDeadline: null }),
        })
        .where(condition)
        .returning({ id: sidecarAllocation.id });
      args.signal.throwIfAborted();
      if (updated === undefined)
        throw new Error("Initialization lease expired");
      return true;
    });
  }

  async function initializationCompleted(
    tx: DBExecutor,
    args: BeginUnrecoverableSidecarReleaseArgs,
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
    failure: { readonly code: string; readonly message: string },
  ): Promise<void> {
    // The first failure of a live deployment is the one its runs failed for.
    await tx
      .update(workflowRun)
      .set({ failureCode: failure.code, failureMessage: failure.message })
      .where(
        and(
          eq(workflowRun.id, anchorRunId),
          isNull(workflowRun.failureCode),
          inArray(workflowRun.status, [...liveWorkflowRunStatuses]),
        ),
      );
    // A pending projection means accepted history may already hold some of
    // these runs' outcomes. Record when the deployment was lost and fail the
    // runs once that history is reconciled.
    if (await pendingProjections.hasAny(anchorRunId, tx)) {
      await tx
        .update(workflowRun)
        .set({ infrastructureFailedAt: now })
        .where(
          and(
            eq(workflowRun.id, anchorRunId),
            isNull(workflowRun.infrastructureFailedAt),
          ),
        );
      return;
    }
    const failed = await failLiveRuns(tx, anchorRunId, now, failure);
    // A failure deferred earlier has nothing left to fail, and applying it
    // later would drop the reason these runs failed for.
    await settleDeferredFailure(tx, anchorRunId, failed);
  }

  // History reconciled meanwhile may have settled the anchor, and then the
  // deferred failure is not why it ended.
  async function settleDeferredFailure(
    tx: DBExecutor,
    anchorRunId: string,
    failed: readonly string[],
  ): Promise<void> {
    await tx
      .update(workflowRun)
      .set({
        infrastructureFailedAt: null,
        ...(failed.includes(anchorRunId)
          ? {}
          : { failureCode: null, failureMessage: null }),
      })
      .where(
        and(
          eq(workflowRun.id, anchorRunId),
          isNotNull(workflowRun.infrastructureFailedAt),
        ),
      );
  }

  async function failLiveRuns(
    tx: DBExecutor,
    anchorRunId: string,
    now: Date | ReturnType<typeof sql>,
    failure?: { readonly code: string; readonly message: string },
  ): Promise<string[]> {
    // Every run failed here carries the deployment's reason: the one its anchor
    // holds, which a deferred failure kept there, or else the one given.
    const [anchor] = await tx
      .select({
        code: workflowRun.failureCode,
        message: workflowRun.failureMessage,
      })
      .from(workflowRun)
      .where(eq(workflowRun.id, anchorRunId));
    const reason =
      anchor?.code != null
        ? { code: anchor.code, message: anchor.message }
        : failure;
    // Fail every live run anchored here -- both "running" runs and a "deployed"
    // anchor torn down before its first trigger -- so a release settles them.
    const failedRuns = await tx
      .update(workflowRun)
      .set({
        status: "failed",
        endedAt: now,
        ...(reason !== undefined
          ? {
              failureCode: sql`coalesce(${workflowRun.failureCode}, ${reason.code})`,
              failureMessage: sql`coalesce(${workflowRun.failureMessage}, ${reason.message})`,
            }
          : {}),
      })
      .where(
        and(
          eq(workflowRun.anchorRunId, anchorRunId),
          inArray(workflowRun.status, [...liveWorkflowRunStatuses]),
        ),
      )
      .returning({ id: workflowRun.id, principalId: workflowRun.principalId });
    const principalIds = failedRuns.flatMap(({ principalId }) =>
      principalId === null ? [] : [principalId],
    );
    if (principalIds.length > 0) {
      await tx
        .update(principal)
        .set({ status: "deactivated", updatedAt: now })
        .where(inArray(principal.id, principalIds));
    }
    return failedRuns.map(({ id }) => id);
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
        destroyAttempts: 0,
        deploymentCleanupConfirmed: false,
        initializationLeaseId: null,
        nextAttemptAt: now,
        reconciliationLeaseId: null,
        reconciliationLeaseExpiresAt: null,
        // Preserve an initialized deployment's existing disconnect window.
        // A first-connect deadline does not bound subsequent cleanup waiting.
        connectDeadline: sql`case when exists (select 1 from ${workflowRun} where ${workflowRun.id} = ${sidecarAllocation.anchorRunId} and ${workflowRun.publicKey} is not null) then ${sidecarAllocation.connectDeadline} else null end`,
        // Placement and initialization retry diagnostics are not release causes.
        // Preserve a recorded workflow failure, or the reason supplied by release.
        failureCode:
          args.failureCode ??
          sql`(select ${workflowRun.failureCode} from ${workflowRun} where ${workflowRun.id} = ${sidecarAllocation.anchorRunId})`,
        failureMessage:
          args.failureMessage ??
          (args.failureCode === undefined
            ? sql`(select ${workflowRun.failureMessage} from ${workflowRun} where ${workflowRun.id} = ${sidecarAllocation.anchorRunId})`
            : null),
        updatedAt: now,
      })
      .where(
        and(
          eq(sidecarAllocation.id, args.allocationId),
          eq(sidecarAllocation.status, args.expectedStatus),
          eq(sidecarAllocation.generation, args.expectedGeneration),
          ...leaseCondition(args.expectedLeaseId),
          // An ordinary caller cannot interrupt an active reconciler. A
          // definitive initialization refusal instead owns the exact attempt
          // marker, which outlives a disconnect's reconciliation lease.
          ...(args.expectedLeaseId === undefined &&
          args.expectedInitializationLeaseId === undefined
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

  async function releaseFailedAllocation(
    tx: DBExecutor,
    args: BeginSidecarReleaseArgs & {
      readonly failureCode: string;
      readonly failureMessage: string;
    },
  ): Promise<SidecarAllocation | null> {
    const releasing = await beginRelease(args, tx);
    if (releasing === null) return null;
    const now = databaseTimestamp(args.now);
    await failRunningRuns(tx, releasing.anchorRunId, now, {
      code: args.failureCode,
      message: args.failureMessage,
    });
    await workflowRunDispatchStore.abandonUnsettled(
      releasing.anchorRunId,
      args.failureCode,
      args.failureMessage,
      now,
      tx,
    );
    return releasing;
  }

  async function insertAdopted(
    tx: DBExecutor,
    args: CreateAdoptedSidecarAllocationArgs,
  ): Promise<SidecarAllocation> {
    // Adopting a probe places a deployment on its sidecar, so it is held to
    // the same room check as a placement through `markAllocated`.
    await assertSidecarHasRoom(tx, {
      ...options,
      sidecarId: args.sidecarId,
      placing: { allocationId: args.id },
    });
    const createdAt = databaseTimestamp(args.now);
    const [inserted] = await tx
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
        maxDisconnectedMs: args.maxDisconnectedMs,
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
      return writeInitialization(args, args);
    },

    async clearUnsentInitialization(
      args: InitializationArgs,
    ): Promise<boolean> {
      // No signal gate: the sole caller invokes this only for a proven-unsent
      // frame, which is reachable precisely when the attempt was cancelled.
      // The WHERE clause is the guard. Marker equality proves no newer
      // attempt began, and generation/status equality proves the allocation
      // did not move on. The reconciliation lease is deliberately not
      // required: a disconnect nulls the lease while leaving this attempt's
      // marker behind, and only this attempt's own clear may remove it.
      const [updated] = await db
        .update(sidecarAllocation)
        .set({ initializationLeaseId: null })
        .where(
          initializationConditions(args, args.leaseId, {
            requireCurrentLease: false,
          }),
        )
        .returning({ id: sidecarAllocation.id });
      return updated !== undefined;
    },

    async rejectInitialization(
      args: InitializationArgs & { readonly message: string },
    ): Promise<SidecarAllocation | null> {
      // Like an unsent rollback, a proven refusal remains valid after lease
      // cancellation. Lock the attempt marker before changing either the
      // allocation or its runs; a completion or newer generation wins otherwise.
      return db.transaction(async (tx) => {
        const [allocation] = await tx
          .select({ id: sidecarAllocation.id })
          .from(sidecarAllocation)
          .where(
            initializationConditions(args, args.leaseId, {
              requireCurrentLease: false,
            }),
          )
          .for("update");
        if (allocation === undefined) return null;
        return releaseFailedAllocation(tx, {
          allocationId: args.allocationId,
          expectedStatus: "allocated",
          expectedGeneration: args.generation,
          expectedInitializationLeaseId: args.leaseId,
          failureCode: "sidecar_deployment_rejected",
          failureMessage: args.message,
        });
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
          maxDisconnectedMs: args.maxDisconnectedMs,
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
      return tx === undefined
        ? db.transaction((inner) => insertAdopted(inner, args))
        : insertAdopted(tx, args);
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
            deploymentCleanupConfirmed: false,
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
        const allocation = await lockAllocationWithSidecars(
          tx,
          and(
            eq(sidecarAllocation.id, args.allocationId),
            eq(sidecarAllocation.status, "replacing"),
            eq(sidecarAllocation.generation, args.generation),
            ...leaseCondition(args.expectedLeaseId),
          ),
        );
        if (allocation === undefined) return null;
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
            deploymentCleanupConfirmed: false,
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

    /**
     * Records the provisioner's acceptance of a generation. Throws
     * `SidecarReuseRejectedError` when it placed the generation on a sidecar
     * it cannot reuse, or `SidecarInventoryUnavailableError` while inventory is
     * unknown. Either failure leaves the allocation unchanged.
     */
    async markAllocated(
      args: MarkSidecarAllocatedArgs,
    ): Promise<SidecarAllocation | null> {
      return db.transaction(async (tx) => {
        const condition = and(
          eq(sidecarAllocation.id, args.allocationId),
          eq(sidecarAllocation.status, "provisioning"),
          eq(sidecarAllocation.generation, args.generation),
          ...leaseCondition(args.expectedLeaseId),
        );
        const allocation = await lockAllocationWithSidecars(
          tx,
          condition,
          args.sidecarId,
        );
        if (allocation === undefined) return null;
        const boundSidecarId = allocation.sidecarId;
        const reusedSidecarId =
          args.sidecarId !== undefined && args.sidecarId !== boundSidecarId
            ? args.sidecarId
            : undefined;
        if (reusedSidecarId !== undefined) {
          await assertSidecarReusable(tx, {
            sidecarId: reusedSidecarId,
            binding: {
              provisionerId: allocation.provisionerId,
              provisionerApiVersion: SidecarProvisionerApiVersion.assert(
                allocation.provisionerApiVersion,
              ),
              provisionerBindingFingerprint:
                allocation.provisionerBindingFingerprint,
            },
            placing: { allocationId: allocation.id },
          });
          await assertSidecarHasRoom(tx, {
            ...options,
            sidecarId: reusedSidecarId,
            placing: { allocationId: allocation.id },
          });
        }
        const [updated] = await tx
          .update(sidecarAllocation)
          .set({
            status: "allocated",
            ...(reusedSidecarId !== undefined
              ? { sidecarId: reusedSidecarId }
              : {}),
            ensureAcceptedGeneration: args.generation,
            externalRef: args.externalRef ?? null,
            nextAttemptAt: sidecarAllocation.connectDeadline,
            ensureAttempts: sql`${sidecarAllocation.ensureAttempts} + 1`,
            failureCode: null,
            failureMessage: null,
            updatedAt: databaseTimestamp(args.now),
          })
          .where(condition)
          .returning();
        if (updated === undefined) {
          throw new Error(
            `sidecarAllocationStore.markAllocated: locked allocation ${args.allocationId} changed before update`,
          );
        }
        if (reusedSidecarId !== undefined && boundSidecarId !== null) {
          await tx.delete(sidecar).where(eq(sidecar.id, boundSidecarId));
        }
        return parseSidecarAllocationRow(updated);
      });
    },

    async scheduleRetry(
      args: ScheduleSidecarAllocationRetryArgs,
    ): Promise<SidecarAllocation | null> {
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          nextAttemptAt:
            args.minimumDelayMs === undefined
              ? args.nextAttemptAt
              : sql`greatest(${sql.param(args.nextAttemptAt, sidecarAllocation.nextAttemptAt)}, clock_timestamp() + (${args.minimumDelayMs} * interval '1 millisecond'))`,
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
          ...(args.firstDeployFailedAt !== undefined
            ? { firstDeployFailedAt: args.firstDeployFailedAt }
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

    async confirmDeploymentCleanup(args: {
      readonly allocationId: string;
      readonly generation: number;
      readonly expectedLeaseId: string;
      readonly now?: Date;
    }): Promise<boolean> {
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          deploymentCleanupConfirmed: true,
          connectDeadline: null,
          updatedAt: databaseTimestamp(args.now),
        })
        .where(
          and(
            eq(sidecarAllocation.id, args.allocationId),
            eq(sidecarAllocation.generation, args.generation),
            inArray(sidecarAllocation.status, ["releasing", "replacing"]),
            ...leaseCondition(args.expectedLeaseId),
          ),
        )
        .returning({ id: sidecarAllocation.id });
      return updated !== undefined;
    },

    /** Record observed cleanup connectivity without releasing the reconciliation lease. */
    async recordCleanupConnection(args: {
      readonly allocationId: string;
      readonly generation: number;
      readonly expectedLeaseId: string;
      readonly connected: boolean;
      readonly now?: Date;
    }): Promise<SidecarAllocation | null> {
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          connectDeadline: args.connected
            ? null
            : sql`coalesce(${sidecarAllocation.connectDeadline}, ${disconnectDeadline(args.now)})`,
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

    // Only provisioning is replaced: nothing has run on it, so a new
    // generation starts the deployment from scratch.
    async beginReplacement(
      args: BeginSidecarReplacementArgs,
    ): Promise<SidecarAllocation | null> {
      const now = databaseTimestamp(args.now);
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          status: "replacing",
          generation: args.expectedGeneration + 1,
          deploymentCleanupConfirmed: false,
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
            eq(sidecarAllocation.status, "provisioning"),
            eq(sidecarAllocation.generation, args.expectedGeneration),
            ...leaseCondition(args.expectedLeaseId),
          ),
        )
        .returning();
      return updated === undefined ? null : parseSidecarAllocationRow(updated);
    },

    beginRelease,

    /**
     * Fail the live runs of a deployment its sidecar reported stopped, and
     * keep the capacity: the stopped copy's files stay for inspection until
     * the retention for failed runs releases them. The allocation then rests
     * on its connection as a ready one does, keeping the reason it failed.
     */
    async failStoppedDeployment(
      args: FailStoppedSidecarDeploymentArgs,
    ): Promise<boolean> {
      const now = databaseTimestamp(args.now);
      return db.transaction(async (tx) => {
        const [updated] = await tx
          .update(sidecarAllocation)
          .set({
            failureCode: args.failureCode,
            failureMessage: args.failureMessage,
            updatedAt: now,
          })
          .where(
            and(
              eq(sidecarAllocation.id, args.allocationId),
              eq(sidecarAllocation.status, "allocated"),
              eq(sidecarAllocation.generation, args.expectedGeneration),
              ...leaseCondition(args.expectedLeaseId),
            ),
          )
          .returning({ anchorRunId: sidecarAllocation.anchorRunId });
        if (updated === undefined) return false;
        await failRunningRuns(tx, updated.anchorRunId, now, {
          code: args.failureCode,
          message: args.failureMessage,
        });
        await workflowRunDispatchStore.abandonUnsettled(
          updated.anchorRunId,
          args.failureCode,
          args.failureMessage,
          now,
          tx,
        );
        // Settles it as `markConnectionReady` does, under the same conditions.
        // An allocation whose ensure is not accepted or whose initialization is
        // still in flight keeps its schedule and lease, so the work that owns
        // it settles it.
        await tx
          .update(sidecarAllocation)
          .set({
            connectDeadline: null,
            nextAttemptAt: null,
            reconciliationLeaseId: null,
            reconciliationLeaseExpiresAt: null,
            updatedAt: now,
          })
          .where(
            and(
              eq(sidecarAllocation.id, args.allocationId),
              eq(sidecarAllocation.status, "allocated"),
              eq(sidecarAllocation.generation, args.expectedGeneration),
              eq(
                sidecarAllocation.ensureAcceptedGeneration,
                args.expectedGeneration,
              ),
              isNull(sidecarAllocation.initializationLeaseId),
              ...leaseCondition(args.expectedLeaseId),
            ),
          );
        return true;
      });
    },

    async beginUnrecoverableRelease(
      args: BeginUnrecoverableSidecarReleaseArgs,
    ): Promise<SidecarAllocation | null> {
      return db.transaction(async (tx) => {
        if (
          args.onlyIfInitializationIncomplete &&
          (await initializationCompleted(tx, args))
        )
          return null;
        return releaseFailedAllocation(tx, args);
      });
    },

    async markReleased(
      args: MarkSidecarReleasedArgs,
    ): Promise<SidecarAllocation | null> {
      const now = databaseTimestamp(args.now);
      const cleanupFailure = inArray(sidecarAllocation.failureCode, [
        SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE,
        SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE,
      ]);
      return db.transaction(async (tx) => {
        const condition = and(
          eq(sidecarAllocation.id, args.allocationId),
          eq(sidecarAllocation.status, "releasing"),
          eq(sidecarAllocation.generation, args.generation),
          ...leaseCondition(args.expectedLeaseId),
        );
        if ((await lockAllocationWithSidecars(tx, condition)) === undefined)
          return null;
        const [updated] = await tx
          .update(sidecarAllocation)
          .set({
            status: "released",
            deploymentCleanupConfirmed: true,
            failureCode: sql`case when ${cleanupFailure} then null else ${sidecarAllocation.failureCode} end`,
            failureMessage: sql`case when ${cleanupFailure} then null else ${sidecarAllocation.failureMessage} end`,
            nextAttemptAt: null,
            reconciliationLeaseId: null,
            reconciliationLeaseExpiresAt: null,
            connectDeadline: null,
            destroyAttempts: sql`${sidecarAllocation.destroyAttempts} + 1`,
            updatedAt: now,
          })
          .where(condition)
          .returning();
        if (updated === undefined) return null;

        await workflowRunDispatchStore.abandonUnsettled(
          updated.anchorRunId,
          "workflow_capacity_released",
          "Workflow capacity was released before delivery could be confirmed",
          now,
          tx,
        );
        return parseSidecarAllocationRow(updated);
      });
    },

    async markDestroyFailed(
      args: MarkSidecarDestroyFailedArgs,
    ): Promise<SidecarAllocation | null> {
      const now = databaseTimestamp(args.now);
      return db.transaction(async (tx) => {
        const condition = and(
          eq(sidecarAllocation.id, args.allocationId),
          inArray(sidecarAllocation.status, ["replacing", "releasing"]),
          eq(sidecarAllocation.generation, args.expectedGeneration),
          ...leaseCondition(args.expectedLeaseId),
        );
        const allocation = await lockAllocationWithSidecars(tx, condition);
        if (allocation === undefined) return null;
        // Preserve the workflow's cause before the allocation code becomes
        // the cleanup outcome recorded on the allocation.
        const workflowFailure =
          allocation.status === "releasing" &&
          allocation.failureCode !== null &&
          allocation.failureCode !==
            SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE &&
          allocation.failureCode !==
            SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE
            ? {
                code: allocation.failureCode,
                message: allocation.failureMessage ?? allocation.failureCode,
              }
            : { code: args.code, message: args.message };
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
            ...(args.countAttempt === false
              ? {}
              : {
                  destroyAttempts: sql`${sidecarAllocation.destroyAttempts} + 1`,
                }),
            updatedAt: now,
          })
          .where(condition)
          .returning();
        if (updated === undefined) return null;

        await failRunningRuns(tx, updated.anchorRunId, now, workflowFailure);
        await workflowRunDispatchStore.abandonUnsettled(
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
        const condition = and(
          eq(sidecarAllocation.id, args.allocationId),
          eq(sidecarAllocation.status, args.expectedStatus),
          eq(sidecarAllocation.generation, args.expectedGeneration),
          ...leaseCondition(args.expectedLeaseId),
        );
        if (
          (await lockAllocationWithSidecars(executor, condition)) === undefined
        )
          return null;
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
          .where(condition)
          .returning();
        if (updated === undefined) return null;

        await failRunningRuns(executor, updated.anchorRunId, now, {
          code: args.code,
          message: args.message,
        });
        await workflowRunDispatchStore.abandonUnsettled(
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

    /**
     * Apply an infrastructure failure deferred while accepted history was
     * unreconciled: fail the runs still live at the recorded time. Returns
     * false while a pending projection remains.
     */
    async applyDeferredInfrastructureFailure(
      anchorRunId: string,
      tx: DBExecutor,
    ): Promise<boolean> {
      const [anchor] = await tx
        .select({ failedAt: workflowRun.infrastructureFailedAt })
        .from(workflowRun)
        .where(eq(workflowRun.id, anchorRunId));
      if (anchor?.failedAt == null) return true;
      if (await pendingProjections.hasAny(anchorRunId, tx)) return false;
      const failed = await failLiveRuns(tx, anchorRunId, anchor.failedAt);
      await settleDeferredFailure(tx, anchorRunId, failed);
      return true;
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
      // A stopped deployment the Hub failed stays connected until retention
      // releases it, and a later ready pass must not erase why it failed.
      const keepsReason = sql`${sidecarAllocation.failureCode} = ${SIDECAR_DEPLOYMENT_STOPPED_FAILURE_CODE}`;
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          connectDeadline: null,
          nextAttemptAt: null,
          reconciliationLeaseId: null,
          reconciliationLeaseExpiresAt: null,
          failureCode: sql`case when ${keepsReason} then ${sidecarAllocation.failureCode} end`,
          failureMessage: sql`case when ${keepsReason} then ${sidecarAllocation.failureMessage} end`,
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
      const connectDeadline = reconnectDeadline(
        args,
        firstConnectDeadline(args),
      );
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          connectDeadline,
          nextAttemptAt: connectDeadline,
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

    // No sidecar could connect while the Hub was down, so every generation gets
    // a fresh window from Hub start, unless the deadline it already has before
    // its first deploy completed is later.
    async scheduleReconnectAfterHubStart(
      args: ScheduleSidecarReconnectAfterHubStartArgs,
    ): Promise<SidecarAllocation | null> {
      const connectDeadline = reconnectDeadline(
        args,
        sql`greatest(${sidecarAllocation.connectDeadline}, ${firstConnectDeadline(args)})`,
      );
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          connectDeadline,
          nextAttemptAt: connectDeadline,
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
      const connectDeadline = reconnectDeadline(
        args,
        firstConnectDeadline(args),
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

    /** Resume a timeout or an unchanged parked wait, without reviving its workflow. */
    async resumeCleanupAfterDisconnect(
      allocationId: string,
      generation: number,
      parked?: { readonly deadline: Date; readonly destroyAttempts: number },
    ): Promise<boolean> {
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          status: "releasing",
          connectDeadline: null,
          nextAttemptAt: sql`now()`,
          updatedAt: sql`now()`,
        })
        .where(
          and(
            eq(sidecarAllocation.id, allocationId),
            eq(sidecarAllocation.generation, generation),
            or(
              and(
                eq(sidecarAllocation.status, "destroy_failed"),
                eq(
                  sidecarAllocation.failureCode,
                  SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE,
                ),
              ),
              parked === undefined
                ? undefined
                : and(
                    eq(sidecarAllocation.status, "releasing"),
                    eq(sidecarAllocation.connectDeadline, parked.deadline),
                    eq(sidecarAllocation.nextAttemptAt, parked.deadline),
                    eq(
                      sidecarAllocation.destroyAttempts,
                      parked.destroyAttempts,
                    ),
                    or(
                      isNull(sidecarAllocation.reconciliationLeaseExpiresAt),
                      lte(
                        sidecarAllocation.reconciliationLeaseExpiresAt,
                        sql`clock_timestamp()`,
                      ),
                    ),
                  ),
            ),
          ),
        )
        .returning({ id: sidecarAllocation.id });
      return updated !== undefined;
    },

    async wakeReconciliation(
      allocationId: string,
      generation: number,
      { connected = false }: { readonly connected?: boolean } = {},
    ): Promise<boolean> {
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          nextAttemptAt: sql`now()`,
          // A reconnect ends an initialized copy's disconnect window before
          // the readiness pass. Preserve the first-connect limit until deploy
          // completes; repeated hellos must not extend initialization forever.
          ...(connected
            ? {
                connectDeadline: sql`case when ${sidecarAllocation.status} = 'releasing' or exists (select 1 from ${workflowRun} where ${workflowRun.id} = ${sidecarAllocation.anchorRunId} and ${workflowRun.publicKey} is not null) then null else ${sidecarAllocation.connectDeadline} end`,
              }
            : {}),
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

    /** Includes failed cleanup obligations whose fences must survive a restart. */
    async listActive(): Promise<SidecarAllocation[]> {
      const rows = await db
        .select()
        .from(sidecarAllocation)
        .where(
          or(
            inArray(sidecarAllocation.status, activeStatuses),
            eq(sidecarAllocation.status, "destroy_failed"),
          ),
        );
      return rows.map(parseSidecarAllocationRow);
    },
  };
}

export type SidecarAllocationStore = ReturnType<
  typeof buildSidecarAllocationStore
>;
