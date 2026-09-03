import { and, eq, gt, inArray, isNull } from "drizzle-orm";

import { SidecarCapabilityDeclaration } from "@intx/types";

import type { DB, DBExecutor } from "./client";
import {
  executionHost,
  executionHostAssignment,
  executionHostSession,
  principal,
  sidecarAllocation,
  sidecarOperation,
  workflowProbe,
} from "./schema";

export type ExecutionHostAssignmentStatus =
  | "claiming"
  | "assigned"
  | "releasing"
  | "destroyed";

export type ExecutionHostAssignment = {
  readonly operationId: string;
  readonly generation: number;
  readonly sidecarId: string;
  readonly hostId: string;
  readonly hostSessionId: string;
  readonly hostSessionGeneration: number;
  readonly capabilities: readonly SidecarCapabilityDeclaration[];
  readonly status: ExecutionHostAssignmentStatus;
  readonly destroyedGeneration?: number;
};

export type ExecutionHostClaimCandidate = {
  readonly hostId: string;
  readonly principalId: string;
  readonly ownerPrincipalId: string;
  readonly tenantId: string;
  readonly sessionId: string;
  readonly sessionGeneration: number;
  readonly hubInstanceId: string;
  readonly capabilities: readonly SidecarCapabilityDeclaration[];
};

export type ClaimExecutionHostArgs = {
  readonly operationId: string;
  readonly generation: number;
  readonly sidecarId: string;
  readonly tenantId: string;
  readonly placementPrincipalId: string;
  readonly targetHostPrincipalId?: string;
  readonly candidate: ExecutionHostClaimCandidate;
  readonly now?: Date;
};

export type SettleExecutionHostAssignmentArgs = {
  readonly operationId: string;
  readonly generation: number;
  readonly sidecarId: string;
  readonly hostId: string;
  readonly hostSessionId: string;
  readonly hostSessionGeneration: number;
  readonly now?: Date;
};

export type DestroyExecutionHostAssignmentArgs = {
  readonly operationId: string;
  readonly generation: number;
  readonly sidecarId: string;
  readonly candidate: ExecutionHostClaimCandidate;
  readonly now?: Date;
};

export type CompleteExecutionHostReleaseArgs =
  SettleExecutionHostAssignmentArgs & {
    readonly destroyedGeneration: number;
  };

function parseAssignment(
  row: typeof executionHostAssignment.$inferSelect,
): ExecutionHostAssignment {
  return {
    operationId: row.operationId,
    generation: row.generation,
    sidecarId: row.sidecarId,
    hostId: row.hostId,
    hostSessionId: row.hostSessionId,
    hostSessionGeneration: row.hostSessionGeneration,
    capabilities: SidecarCapabilityDeclaration.array().assert(row.capabilities),
    status: row.status,
    ...(row.destroyedGeneration !== null
      ? { destroyedGeneration: row.destroyedGeneration }
      : {}),
  };
}

export function createExecutionHostAssignmentStore(db: DB["db"]) {
  async function listAvailableCandidates(
    candidates: readonly ExecutionHostClaimCandidate[],
    now = new Date(),
  ): Promise<ExecutionHostClaimCandidate[]> {
    if (candidates.length === 0) return [];
    const offered = new Map(candidates.map((host) => [host.hostId, host]));
    const rows = await db
      .select({
        hostId: executionHostSession.hostId,
        principalId: executionHost.principalId,
        ownerPrincipalId: executionHost.ownerPrincipalId,
        tenantId: executionHost.tenantId,
        sessionId: executionHostSession.sessionId,
        sessionGeneration: executionHostSession.generation,
        hubInstanceId: executionHostSession.hubInstanceId,
        capabilities: executionHostSession.capabilities,
      })
      .from(executionHostSession)
      .innerJoin(
        executionHost,
        eq(executionHost.id, executionHostSession.hostId),
      )
      .innerJoin(principal, eq(principal.id, executionHost.principalId))
      .leftJoin(
        executionHostAssignment,
        and(
          eq(executionHostAssignment.hostId, executionHostSession.hostId),
          inArray(executionHostAssignment.status, [
            "claiming",
            "assigned",
            "releasing",
          ]),
        ),
      )
      .where(
        and(
          inArray(executionHostSession.hostId, [...offered.keys()]),
          eq(principal.status, "active"),
          gt(executionHostSession.leaseExpiresAt, now),
          isNull(executionHostAssignment.sidecarId),
        ),
      );

    return rows.flatMap((row) => {
      const host = offered.get(row.hostId);
      if (
        host?.sessionId !== row.sessionId ||
        host.sessionGeneration !== row.sessionGeneration ||
        host.hubInstanceId !== row.hubInstanceId
      ) {
        return [];
      }
      return [
        {
          ...row,
          capabilities: SidecarCapabilityDeclaration.array().assert(
            row.capabilities,
          ),
        },
      ];
    });
  }

  async function matchesTargetHost(args: {
    operationId: string;
    generation: number;
    sidecarId: string;
    hostPrincipalId: string;
  }): Promise<boolean> {
    const [assignment] = await db
      .select({ sidecarId: executionHostAssignment.sidecarId })
      .from(executionHostAssignment)
      .innerJoin(
        executionHost,
        eq(executionHost.id, executionHostAssignment.hostId),
      )
      .where(
        and(
          eq(executionHostAssignment.operationId, args.operationId),
          eq(executionHostAssignment.generation, args.generation),
          eq(executionHostAssignment.sidecarId, args.sidecarId),
          eq(executionHostAssignment.status, "assigned"),
          eq(executionHost.principalId, args.hostPrincipalId),
        ),
      )
      .limit(1);
    return assignment !== undefined;
  }

  async function hasCurrentOwner(
    tx: DBExecutor,
    args: ClaimExecutionHostArgs,
  ): Promise<boolean> {
    const [operation] = await tx
      .select({ id: sidecarOperation.id })
      .from(sidecarOperation)
      .where(eq(sidecarOperation.id, args.operationId))
      .limit(1)
      .for("update");
    if (operation === undefined) return false;

    const [allocation] = await tx
      .select({
        status: sidecarAllocation.status,
        generation: sidecarAllocation.generation,
        sidecarId: sidecarAllocation.sidecarId,
        tenantId: sidecarAllocation.tenantId,
        placementPrincipalId: sidecarAllocation.placementPrincipalId,
        targetHostPrincipalId: sidecarAllocation.targetHostPrincipalId,
      })
      .from(sidecarAllocation)
      .where(eq(sidecarAllocation.id, args.operationId))
      .limit(1)
      .for("update");
    if (allocation !== undefined) {
      return (
        (allocation.status === "provisioning" ||
          allocation.status === "allocated") &&
        allocation.generation === args.generation &&
        allocation.sidecarId === args.sidecarId &&
        allocation.tenantId === args.tenantId &&
        allocation.placementPrincipalId === args.placementPrincipalId &&
        allocation.targetHostPrincipalId ===
          (args.targetHostPrincipalId ?? null)
      );
    }

    const [probe] = await tx
      .select({
        generation: workflowProbe.generation,
        sidecarId: workflowProbe.sidecarId,
        tenantId: workflowProbe.tenantId,
        placementPrincipalId: workflowProbe.placementPrincipalId,
        status: workflowProbe.status,
      })
      .from(workflowProbe)
      .where(eq(workflowProbe.id, args.operationId))
      .limit(1)
      .for("update");
    return (
      probe !== undefined &&
      (probe.status === "provisioning" || probe.status === "probing") &&
      probe.generation === args.generation &&
      probe.sidecarId === args.sidecarId &&
      probe.tenantId === args.tenantId &&
      probe.placementPrincipalId === args.placementPrincipalId &&
      args.targetHostPrincipalId === undefined
    );
  }

  async function claim(
    args: ClaimExecutionHostArgs,
  ): Promise<ExecutionHostAssignment | null> {
    return db.transaction(async (tx) => {
      if (!(await hasCurrentOwner(tx, args))) return null;

      const exact = await tx.query.executionHostAssignment.findFirst({
        where: eq(executionHostAssignment.sidecarId, args.sidecarId),
      });
      if (exact !== undefined) {
        if (
          exact.operationId !== args.operationId ||
          exact.generation !== args.generation ||
          exact.status === "destroyed"
        ) {
          return null;
        }
        return parseAssignment(exact);
      }

      const [session] = await tx
        .select({
          hostId: executionHostSession.hostId,
          sessionId: executionHostSession.sessionId,
          generation: executionHostSession.generation,
          hubInstanceId: executionHostSession.hubInstanceId,
          leaseExpiresAt: executionHostSession.leaseExpiresAt,
          capabilities: executionHostSession.capabilities,
          principalId: executionHost.principalId,
          ownerPrincipalId: executionHost.ownerPrincipalId,
          tenantId: executionHost.tenantId,
          principalStatus: principal.status,
        })
        .from(executionHostSession)
        .innerJoin(
          executionHost,
          eq(executionHost.id, executionHostSession.hostId),
        )
        .innerJoin(principal, eq(principal.id, executionHost.principalId))
        .where(
          and(
            eq(executionHostSession.hostId, args.candidate.hostId),
            eq(executionHostSession.sessionId, args.candidate.sessionId),
            eq(
              executionHostSession.generation,
              args.candidate.sessionGeneration,
            ),
            eq(
              executionHostSession.hubInstanceId,
              args.candidate.hubInstanceId,
            ),
            gt(executionHostSession.leaseExpiresAt, args.now ?? new Date()),
          ),
        )
        .limit(1)
        .for("update");
      if (
        session === undefined ||
        session.principalStatus !== "active" ||
        session.tenantId !== args.tenantId ||
        session.principalId !== args.candidate.principalId ||
        (args.targetHostPrincipalId !== undefined &&
          session.principalId !== args.targetHostPrincipalId)
      ) {
        return null;
      }

      const active = await tx.query.executionHostAssignment.findFirst({
        where: and(
          inArray(executionHostAssignment.status, [
            "claiming",
            "assigned",
            "releasing",
          ]),
          eq(executionHostAssignment.operationId, args.operationId),
        ),
      });
      if (active !== undefined) return null;
      const hostActive = await tx.query.executionHostAssignment.findFirst({
        where: and(
          inArray(executionHostAssignment.status, [
            "claiming",
            "assigned",
            "releasing",
          ]),
          eq(executionHostAssignment.hostId, session.hostId),
        ),
      });
      if (hostActive !== undefined) return null;

      const now = args.now ?? new Date();
      const [created] = await tx
        .insert(executionHostAssignment)
        .values({
          operationId: args.operationId,
          generation: args.generation,
          sidecarId: args.sidecarId,
          hostId: session.hostId,
          hostSessionId: session.sessionId,
          hostSessionGeneration: session.generation,
          capabilities: SidecarCapabilityDeclaration.array().assert(
            session.capabilities,
          ),
          status: "claiming",
          createdAt: now,
          updatedAt: now,
        })
        .returning();
      return created === undefined ? null : parseAssignment(created);
    });
  }

  async function markAssigned(
    args: SettleExecutionHostAssignmentArgs,
  ): Promise<ExecutionHostAssignment | null> {
    return db.transaction(async (tx) => {
      const now = args.now ?? new Date();
      const [current] = await tx
        .select({ hostId: executionHostSession.hostId })
        .from(executionHostSession)
        .where(
          and(
            eq(executionHostSession.hostId, args.hostId),
            eq(executionHostSession.sessionId, args.hostSessionId),
            eq(executionHostSession.generation, args.hostSessionGeneration),
            gt(executionHostSession.leaseExpiresAt, now),
          ),
        )
        .limit(1)
        .for("update");
      if (current === undefined) return null;
      const [updated] = await tx
        .update(executionHostAssignment)
        .set({ status: "assigned", updatedAt: now })
        .where(
          and(
            eq(executionHostAssignment.operationId, args.operationId),
            eq(executionHostAssignment.generation, args.generation),
            eq(executionHostAssignment.sidecarId, args.sidecarId),
            eq(executionHostAssignment.hostId, args.hostId),
            eq(executionHostAssignment.hostSessionId, args.hostSessionId),
            eq(
              executionHostAssignment.hostSessionGeneration,
              args.hostSessionGeneration,
            ),
            inArray(executionHostAssignment.status, ["claiming", "assigned"]),
          ),
        )
        .returning();
      return updated === undefined ? null : parseAssignment(updated);
    });
  }

  async function beginRelease(
    args: DestroyExecutionHostAssignmentArgs,
  ): Promise<ExecutionHostAssignment | null> {
    return db.transaction(async (tx) => {
      const [assignment] = await tx
        .select()
        .from(executionHostAssignment)
        .where(
          and(
            eq(executionHostAssignment.operationId, args.operationId),
            eq(executionHostAssignment.sidecarId, args.sidecarId),
          ),
        )
        .limit(1)
        .for("update");
      if (assignment === undefined) return null;
      if (args.generation < assignment.generation) return null;
      if (
        assignment.destroyedGeneration !== null &&
        args.generation < assignment.destroyedGeneration
      ) {
        return null;
      }
      if (assignment.status === "destroyed") {
        return parseAssignment(assignment);
      }
      const session = await tx.query.executionHostSession.findFirst({
        where: and(
          eq(executionHostSession.hostId, assignment.hostId),
          eq(executionHostSession.sessionId, args.candidate.sessionId),
          eq(executionHostSession.generation, args.candidate.sessionGeneration),
          eq(executionHostSession.hubInstanceId, args.candidate.hubInstanceId),
          gt(executionHostSession.leaseExpiresAt, args.now ?? new Date()),
        ),
      });
      if (
        session === undefined ||
        assignment.hostId !== args.candidate.hostId
      ) {
        return null;
      }
      const [updated] = await tx
        .update(executionHostAssignment)
        .set({
          status: "releasing",
          destroyedGeneration: args.generation,
          hostSessionId: session.sessionId,
          hostSessionGeneration: session.generation,
          updatedAt: args.now ?? new Date(),
        })
        .where(eq(executionHostAssignment.sidecarId, args.sidecarId))
        .returning();
      return updated === undefined ? null : parseAssignment(updated);
    });
  }

  async function markDestroyed(
    args: CompleteExecutionHostReleaseArgs,
  ): Promise<ExecutionHostAssignment | null> {
    return db.transaction(async (tx) => {
      const now = args.now ?? new Date();
      const [current] = await tx
        .select({ hostId: executionHostSession.hostId })
        .from(executionHostSession)
        .where(
          and(
            eq(executionHostSession.hostId, args.hostId),
            eq(executionHostSession.sessionId, args.hostSessionId),
            eq(executionHostSession.generation, args.hostSessionGeneration),
            gt(executionHostSession.leaseExpiresAt, now),
          ),
        )
        .limit(1)
        .for("update");
      if (current === undefined) return null;
      const [updated] = await tx
        .update(executionHostAssignment)
        .set({ status: "destroyed", updatedAt: now })
        .where(
          and(
            eq(executionHostAssignment.operationId, args.operationId),
            eq(executionHostAssignment.generation, args.generation),
            eq(executionHostAssignment.sidecarId, args.sidecarId),
            eq(executionHostAssignment.hostId, args.hostId),
            eq(executionHostAssignment.hostSessionId, args.hostSessionId),
            eq(
              executionHostAssignment.hostSessionGeneration,
              args.hostSessionGeneration,
            ),
            eq(executionHostAssignment.status, "releasing"),
            eq(
              executionHostAssignment.destroyedGeneration,
              args.destroyedGeneration,
            ),
          ),
        )
        .returning();
      return updated === undefined ? null : parseAssignment(updated);
    });
  }

  async function findBySidecarId(
    sidecarId: string,
    executor: DBExecutor = db,
  ): Promise<ExecutionHostAssignment | null> {
    const row = await executor.query.executionHostAssignment.findFirst({
      where: eq(executionHostAssignment.sidecarId, sidecarId),
    });
    return row === undefined ? null : parseAssignment(row);
  }

  return {
    beginRelease,
    claim,
    findBySidecarId,
    listAvailableCandidates,
    markAssigned,
    markDestroyed,
    matchesTargetHost,
  };
}

export type ExecutionHostAssignmentStore = ReturnType<
  typeof createExecutionHostAssignmentStore
>;
