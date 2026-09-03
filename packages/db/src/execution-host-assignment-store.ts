import { and, eq, gt, inArray } from "drizzle-orm";

import type { SidecarCapabilityDeclaration } from "@intx/types";

import type { DB } from "./client";
import {
  executionHost,
  executionHostAssignment,
  executionHostSession,
  principal,
  sidecarAllocation,
} from "./schema";

export type ExecutionHostAssignmentStatus =
  | "claiming"
  | "assigned"
  | "releasing"
  | "destroyed";

export type ExecutionHostAssignment = {
  readonly allocationId: string;
  readonly generation: number;
  readonly sidecarId: string;
  readonly hostId: string;
  readonly hostSessionId: string;
  readonly hostSessionGeneration: number;
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
  readonly allocationId: string;
  readonly generation: number;
  readonly sidecarId: string;
  readonly tenantId: string;
  readonly placementPrincipalId: string;
  readonly targetHostPrincipalId?: string;
  readonly candidate: ExecutionHostClaimCandidate;
  readonly now?: Date;
};

export type SettleExecutionHostAssignmentArgs = {
  readonly allocationId: string;
  readonly generation: number;
  readonly sidecarId: string;
  readonly hostId: string;
  readonly hostSessionId: string;
  readonly hostSessionGeneration: number;
  readonly now?: Date;
};

export type DestroyExecutionHostAssignmentArgs = {
  readonly allocationId: string;
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
    allocationId: row.allocationId,
    generation: row.generation,
    sidecarId: row.sidecarId,
    hostId: row.hostId,
    hostSessionId: row.hostSessionId,
    hostSessionGeneration: row.hostSessionGeneration,
    status: row.status,
    ...(row.destroyedGeneration !== null
      ? { destroyedGeneration: row.destroyedGeneration }
      : {}),
  };
}

export function createExecutionHostAssignmentStore(db: DB["db"]) {
  async function claim(
    args: ClaimExecutionHostArgs,
  ): Promise<ExecutionHostAssignment | null> {
    return db.transaction(async (tx) => {
      const [allocation] = await tx
        .select({
          id: sidecarAllocation.id,
          generation: sidecarAllocation.generation,
          sidecarId: sidecarAllocation.sidecarId,
        })
        .from(sidecarAllocation)
        .where(eq(sidecarAllocation.id, args.allocationId))
        .limit(1)
        .for("update");
      if (
        allocation === undefined ||
        allocation.generation !== args.generation ||
        allocation.sidecarId !== args.sidecarId
      ) {
        return null;
      }

      const exact = await tx.query.executionHostAssignment.findFirst({
        where: eq(executionHostAssignment.sidecarId, args.sidecarId),
      });
      if (exact !== undefined) {
        if (
          exact.allocationId !== args.allocationId ||
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
        session.ownerPrincipalId !== args.placementPrincipalId ||
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
          eq(executionHostAssignment.allocationId, args.allocationId),
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
          allocationId: args.allocationId,
          generation: args.generation,
          sidecarId: args.sidecarId,
          hostId: session.hostId,
          hostSessionId: session.sessionId,
          hostSessionGeneration: session.generation,
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
            eq(executionHostAssignment.allocationId, args.allocationId),
            eq(executionHostAssignment.generation, args.generation),
            eq(executionHostAssignment.sidecarId, args.sidecarId),
            eq(executionHostAssignment.hostId, args.hostId),
            eq(executionHostAssignment.hostSessionId, args.hostSessionId),
            eq(
              executionHostAssignment.hostSessionGeneration,
              args.hostSessionGeneration,
            ),
            eq(executionHostAssignment.status, "claiming"),
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
            eq(executionHostAssignment.allocationId, args.allocationId),
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
            eq(executionHostAssignment.allocationId, args.allocationId),
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
  ): Promise<ExecutionHostAssignment | null> {
    const row = await db.query.executionHostAssignment.findFirst({
      where: eq(executionHostAssignment.sidecarId, sidecarId),
    });
    return row === undefined ? null : parseAssignment(row);
  }

  return {
    beginRelease,
    claim,
    findBySidecarId,
    markAssigned,
    markDestroyed,
  };
}

export type ExecutionHostAssignmentStore = ReturnType<
  typeof createExecutionHostAssignmentStore
>;
