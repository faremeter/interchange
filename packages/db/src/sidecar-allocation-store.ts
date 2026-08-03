import { type } from "arktype";
import { and, asc, eq, inArray, isNull, lte, or, sql } from "drizzle-orm";

import {
  SidecarPlacementRequirement,
  sidecarAllocationStatuses,
  type SidecarAllocationStatus,
} from "@intx/types";

import type { DB, DBExecutor } from "./client";
import {
  sidecar,
  sidecarAllocation,
  workflowRun,
  workflowRunLaunchSpec,
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
  readonly placement: SidecarPlacementRequirement;
  readonly status: SidecarAllocationStatus;
  readonly generation: number;
  readonly ensureAcceptedGeneration?: number;
  readonly externalRef?: string;
  readonly nextAttemptAt?: Date;
  readonly reconciliationLeaseId?: string;
  readonly reconciliationLeaseExpiresAt?: Date;
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
  readonly placement?: SidecarPlacementRequirement;
  readonly now?: Date;
};

export type ClaimSidecarAllocationArgs = {
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

function parseSidecarAllocationRow(
  row: SidecarAllocationRow,
): SidecarAllocation {
  const placement = SidecarPlacementRequirement.assert({
    sharing: row.placementSharing,
    reuse: row.sidecarReuse,
  });
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
    placement,
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
    : [eq(sidecarAllocation.reconciliationLeaseId, expectedLeaseId)];
}

export function createSidecarAllocationStore(db: DBHandle) {
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
      credentialScope: "allocated",
      status: "offline",
      createdAt: args.now,
      updatedAt: args.now,
    });
  }

  return {
    async createPending(
      args: CreatePendingSidecarAllocationArgs,
      tx?: DBExecutor,
    ): Promise<SidecarAllocation> {
      const executor = tx ?? db;
      const [anchor] = await executor
        .select({
          tenantId: workflowRun.tenantId,
          deploymentId: workflowRun.deploymentId,
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
        anchor.deploymentId !== args.anchorRunId
      ) {
        throw new Error(
          `sidecarAllocationStore.createPending: run ${args.anchorRunId} is not an anchor for tenant ${args.tenantId}`,
        );
      }
      if (anchor.status !== "running") {
        throw new Error(
          `sidecarAllocationStore.createPending: anchor run ${args.anchorRunId} is ${anchor.status}, expected running`,
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
      const placement = SidecarPlacementRequirement.assert(
        args.placement ?? { sharing: "exclusive", reuse: "never" },
      );
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
          placementSharing: placement.sharing,
          sidecarReuse: placement.reuse ?? "never",
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

    async bindInitialSidecar(
      args: BindInitialSidecarArgs,
    ): Promise<SidecarAllocation | null> {
      return db.transaction(async (tx) => {
        const [allocation] = await tx
          .select()
          .from(sidecarAllocation)
          .where(eq(sidecarAllocation.id, args.allocationId))
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
          .where(eq(sidecarAllocation.id, args.allocationId))
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
      const [updated] = await db
        .update(sidecarAllocation)
        .set({
          status: "replacing",
          generation: args.expectedGeneration + 1,
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
          ),
        )
        .returning();
      return updated === undefined ? null : parseSidecarAllocationRow(updated);
    },

    async beginRelease(
      args: BeginSidecarReleaseArgs,
      tx?: DBExecutor,
    ): Promise<SidecarAllocation | null> {
      const now = databaseTimestamp(args.now);
      const [updated] = await (tx ?? db)
        .update(sidecarAllocation)
        .set({
          status: "releasing",
          generation: args.expectedGeneration + 1,
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
          ),
        )
        .returning();
      return updated === undefined ? null : parseSidecarAllocationRow(updated);
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

    async failWithoutInfrastructure(
      args: FailSidecarAllocationArgs,
      tx?: DBExecutor,
    ): Promise<SidecarAllocation | null> {
      const [updated] = await (tx ?? db)
        .update(sidecarAllocation)
        .set({
          status: "failed",
          failureCode: args.code,
          failureMessage: args.message,
          nextAttemptAt: null,
          reconciliationLeaseId: null,
          reconciliationLeaseExpiresAt: null,
          connectDeadline: null,
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
          reconciliationLeaseExpiresAt: sql`now() + (${leaseDurationMs} * interval '1 millisecond')`,
        })
        .where(
          and(
            eq(sidecarAllocation.id, allocationId),
            eq(sidecarAllocation.reconciliationLeaseId, leaseId),
          ),
        )
        .returning({ id: sidecarAllocation.id });
      return updated !== undefined;
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
            eq(sidecarAllocation.reconciliationLeaseId, leaseId),
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
        .set({ nextAttemptAt: sql`now()` })
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
