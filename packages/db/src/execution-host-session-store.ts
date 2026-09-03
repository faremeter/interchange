import { and, eq, gt } from "drizzle-orm";

import { SidecarCapabilityDeclaration } from "@intx/types";

import type { DB } from "./client";
import { executionHost, executionHostSession, principal } from "./schema";

export type ExecutionHostSession = {
  readonly hostId: string;
  readonly principalId: string;
  readonly ownerPrincipalId: string;
  readonly tenantId: string;
  readonly sessionId: string;
  readonly generation: number;
  readonly hubInstanceId: string;
  readonly capabilities: readonly SidecarCapabilityDeclaration[];
  readonly leaseExpiresAt: Date;
};

export type BeginExecutionHostSessionArgs = {
  readonly tokenHashSha256: Uint8Array;
  readonly sessionId: string;
  readonly hubInstanceId: string;
  readonly capabilities: readonly SidecarCapabilityDeclaration[];
  readonly leaseExpiresAt: Date;
  readonly now?: Date;
};

export type RefreshExecutionHostSessionArgs = {
  readonly hostId: string;
  readonly sessionId: string;
  readonly generation: number;
  readonly hubInstanceId: string;
  readonly leaseExpiresAt: Date;
  readonly now?: Date;
};

export type ExpireExecutionHostSessionArgs = {
  readonly hostId: string;
  readonly sessionId: string;
  readonly generation: number;
  readonly hubInstanceId: string;
  readonly now?: Date;
};

export function createExecutionHostSessionStore(db: DB["db"]) {
  async function begin(
    args: BeginExecutionHostSessionArgs,
  ): Promise<ExecutionHostSession | null> {
    return db.transaction(async (tx) => {
      const [identity] = await tx
        .select({
          hostId: executionHost.id,
          principalId: executionHost.principalId,
          ownerPrincipalId: executionHost.ownerPrincipalId,
          tenantId: executionHost.tenantId,
        })
        .from(executionHost)
        .innerJoin(principal, eq(principal.id, executionHost.principalId))
        .where(
          and(
            eq(executionHost.tokenHashSha256, args.tokenHashSha256),
            eq(principal.kind, "host"),
            eq(principal.status, "active"),
          ),
        )
        .limit(1)
        .for("update");
      if (identity === undefined) return null;

      const current = await tx.query.executionHostSession.findFirst({
        where: eq(executionHostSession.hostId, identity.hostId),
      });
      const generation = (current?.generation ?? 0) + 1;
      const now = args.now ?? new Date();
      const capabilities = args.capabilities.map((capability) => ({
        ...capability,
      }));
      await tx
        .insert(executionHostSession)
        .values({
          hostId: identity.hostId,
          sessionId: args.sessionId,
          generation,
          hubInstanceId: args.hubInstanceId,
          capabilities,
          leaseExpiresAt: args.leaseExpiresAt,
          updatedAt: now,
        })
        .onConflictDoUpdate({
          target: executionHostSession.hostId,
          set: {
            sessionId: args.sessionId,
            generation,
            hubInstanceId: args.hubInstanceId,
            capabilities,
            leaseExpiresAt: args.leaseExpiresAt,
            updatedAt: now,
          },
        });
      return {
        ...identity,
        sessionId: args.sessionId,
        generation,
        hubInstanceId: args.hubInstanceId,
        capabilities,
        leaseExpiresAt: args.leaseExpiresAt,
      };
    });
  }

  async function refresh(
    args: RefreshExecutionHostSessionArgs,
  ): Promise<boolean> {
    const now = args.now ?? new Date();
    const [updated] = await db
      .update(executionHostSession)
      .set({ leaseExpiresAt: args.leaseExpiresAt, updatedAt: now })
      .where(
        and(
          eq(executionHostSession.hostId, args.hostId),
          eq(executionHostSession.sessionId, args.sessionId),
          eq(executionHostSession.generation, args.generation),
          eq(executionHostSession.hubInstanceId, args.hubInstanceId),
          gt(executionHostSession.leaseExpiresAt, now),
        ),
      )
      .returning({ hostId: executionHostSession.hostId });
    return updated !== undefined;
  }

  async function expire(
    args: ExpireExecutionHostSessionArgs,
  ): Promise<boolean> {
    const now = args.now ?? new Date();
    const [updated] = await db
      .update(executionHostSession)
      .set({ leaseExpiresAt: now, updatedAt: now })
      .where(
        and(
          eq(executionHostSession.hostId, args.hostId),
          eq(executionHostSession.sessionId, args.sessionId),
          eq(executionHostSession.generation, args.generation),
          eq(executionHostSession.hubInstanceId, args.hubInstanceId),
        ),
      )
      .returning({ hostId: executionHostSession.hostId });
    return updated !== undefined;
  }

  async function findCurrent(
    hostId: string,
  ): Promise<ExecutionHostSession | null> {
    const row = await db.query.executionHostSession.findFirst({
      where: eq(executionHostSession.hostId, hostId),
    });
    if (row === undefined) return null;
    const host = await db.query.executionHost.findFirst({
      where: eq(executionHost.id, hostId),
    });
    if (host === undefined) return null;
    return {
      hostId,
      principalId: host.principalId,
      ownerPrincipalId: host.ownerPrincipalId,
      tenantId: host.tenantId,
      sessionId: row.sessionId,
      generation: row.generation,
      hubInstanceId: row.hubInstanceId,
      capabilities: SidecarCapabilityDeclaration.array().assert(
        row.capabilities,
      ),
      leaseExpiresAt: row.leaseExpiresAt,
    };
  }

  return { begin, expire, findCurrent, refresh };
}

export type ExecutionHostSessionStore = ReturnType<
  typeof createExecutionHostSessionStore
>;
