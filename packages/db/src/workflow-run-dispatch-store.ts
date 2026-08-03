import { isDeepStrictEqual } from "node:util";

import {
  and,
  asc,
  eq,
  inArray,
  isNull,
  lte,
  or,
  sql,
  type SQL,
} from "drizzle-orm";

import {
  RunGrantsFrame,
  SignalDeliverFrame,
  type RunGrantsFrame as RunGrantsFrameType,
  type SignalDeliverFrame as SignalDeliverFrameType,
} from "@intx/types/sidecar";

import type { DB, DBExecutor } from "./client";
import { parseWorkflowRunDispatchRow } from "./parse-row";
import { sidecarAllocation, workflowRun, workflowRunDispatch } from "./schema";

type DBHandle = DB["db"];
type ParsedDispatch = ReturnType<typeof parseWorkflowRunDispatchRow>;

export type EnqueueWorkflowRunDispatchArgs = {
  readonly id: string;
  readonly anchorRunId: string;
  readonly messageId: string;
  readonly rawMessage: Uint8Array;
  readonly stepGrants: RunGrantsFrameType["stepGrants"];
  readonly now?: Date;
};

export type EnqueueWorkflowSignalDispatchArgs = {
  readonly id: string;
  readonly anchorRunId: string;
  readonly signal: Omit<SignalDeliverFrameType, "type">;
  readonly now?: Date;
};

export type EnqueueWorkflowRunDispatchResult = {
  readonly dispatch: ParsedDispatch;
  readonly created: boolean;
};

type InsertOrReconcileDispatchArgs = {
  readonly operation: "enqueue" | "enqueueSignal";
  readonly id: string;
  readonly anchorRunId: string;
  readonly messageId: string;
  readonly kind: ParsedDispatch["kind"];
  readonly rawMessage: Uint8Array;
  readonly stepGrants: RunGrantsFrameType["stepGrants"];
  readonly now?: Date;
};

export class WorkflowRunDispatchPayloadConflictError extends Error {
  constructor(
    readonly messageId: string,
    readonly payloadKind: "message" | "signal",
    operation: InsertOrReconcileDispatchArgs["operation"],
  ) {
    super(
      `workflowRunDispatchStore.${operation}: ${payloadKind} ${messageId} conflicts with its durable payload`,
    );
    this.name = "WorkflowRunDispatchPayloadConflictError";
  }
}

export type ClaimWorkflowRunDispatchArgs = {
  readonly leaseId: string;
  readonly leaseDurationMs: number;
};

export type AcknowledgeWorkflowRunDispatchArgs = {
  readonly allocationId: string;
  readonly anchorRunId: string;
  readonly messageId: string;
  readonly generation: number;
  readonly now?: Date;
};

export type RetryWorkflowRunDispatchArgs = {
  readonly dispatchId: string;
  readonly nextAttemptAt: Date;
  readonly code: string;
  readonly message: string;
  readonly expectedLeaseId?: string;
  readonly now?: Date;
};

export type FailWorkflowRunDispatchArgs = {
  readonly anchorRunId: string;
  readonly messageId: string;
  readonly code: string;
  readonly message: string;
  readonly now?: Date;
};

function databaseTimestamp(override?: Date) {
  return override ?? sql`now()`;
}

function leaseCondition(expectedLeaseId?: string) {
  return expectedLeaseId === undefined
    ? []
    : [eq(workflowRunDispatch.deliveryLeaseId, expectedLeaseId)];
}

function byteArraysEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  return left.every((value, index) => value === right[index]);
}

function grantsEqual(
  left: RunGrantsFrameType["stepGrants"],
  right: RunGrantsFrameType["stepGrants"],
): boolean {
  return isDeepStrictEqual(left, right);
}

function validateStepGrants(
  anchorRunId: string,
  stepGrants: RunGrantsFrameType["stepGrants"],
) {
  return RunGrantsFrame.assert({
    type: "run.grants",
    agentAddress: "persisted@validation.invalid",
    runId: anchorRunId,
    stepGrants,
  }).stepGrants;
}

async function insertOrReconcileDispatch(
  executor: DBExecutor,
  args: InsertOrReconcileDispatchArgs,
): Promise<EnqueueWorkflowRunDispatchResult> {
  const [anchor] = await executor
    .select({ deploymentId: workflowRun.deploymentId })
    .from(workflowRun)
    .where(eq(workflowRun.id, args.anchorRunId))
    .limit(1);
  if (anchor?.deploymentId !== args.anchorRunId) {
    throw new Error(
      `workflowRunDispatchStore.${args.operation}: run ${args.anchorRunId} is not a deployment anchor`,
    );
  }

  const now = databaseTimestamp(args.now);
  const [inserted] = await executor
    .insert(workflowRunDispatch)
    .values({
      id: args.id,
      anchorRunId: args.anchorRunId,
      messageId: args.messageId,
      kind: args.kind,
      rawMessage: args.rawMessage,
      stepGrants: args.stepGrants,
      status: "pending",
      nextAttemptAt: now,
      createdAt: now,
      updatedAt: now,
    })
    .onConflictDoNothing({
      target: [workflowRunDispatch.anchorRunId, workflowRunDispatch.messageId],
    })
    .returning();
  if (inserted !== undefined) {
    return {
      dispatch: parseWorkflowRunDispatchRow(inserted),
      created: true,
    };
  }

  const existing = await executor.query.workflowRunDispatch.findFirst({
    where: and(
      eq(workflowRunDispatch.anchorRunId, args.anchorRunId),
      eq(workflowRunDispatch.messageId, args.messageId),
    ),
  });
  if (existing === undefined) {
    throw new Error(
      `workflowRunDispatchStore.${args.operation}: conflicting row disappeared for ${args.anchorRunId}/${args.messageId}`,
    );
  }
  const parsed = parseWorkflowRunDispatchRow(existing);
  if (
    parsed.kind !== args.kind ||
    !byteArraysEqual(parsed.rawMessage, args.rawMessage) ||
    !grantsEqual(parsed.stepGrants, args.stepGrants)
  ) {
    const payloadKind = args.kind === "mail" ? "message" : "signal";
    throw new WorkflowRunDispatchPayloadConflictError(
      args.messageId,
      payloadKind,
      args.operation,
    );
  }
  return { dispatch: parsed, created: false };
}

async function acknowledgeMatchingDispatch(
  executor: DBExecutor,
  generation: number,
  conditions: SQL[],
  nowOverride?: Date,
): Promise<ParsedDispatch | null> {
  const now = databaseTimestamp(nowOverride);
  const [updated] = await executor
    .update(workflowRunDispatch)
    .set({
      status: "acknowledged",
      acknowledgedGeneration: generation,
      acknowledgedAt: now,
      attemptCount: sql`${workflowRunDispatch.attemptCount} + 1`,
      nextAttemptAt: null,
      deliveryLeaseId: null,
      deliveryLeaseExpiresAt: null,
      failureCode: null,
      failureMessage: null,
      updatedAt: now,
    })
    .where(and(...conditions))
    .returning();
  return updated === undefined ? null : parseWorkflowRunDispatchRow(updated);
}

export function createWorkflowRunDispatchStore(db: DBHandle) {
  return {
    async enqueue(
      args: EnqueueWorkflowRunDispatchArgs,
      tx?: DBExecutor,
    ): Promise<EnqueueWorkflowRunDispatchResult> {
      const stepGrants = validateStepGrants(args.anchorRunId, args.stepGrants);
      return insertOrReconcileDispatch(tx ?? db, {
        operation: "enqueue",
        id: args.id,
        anchorRunId: args.anchorRunId,
        messageId: args.messageId,
        kind: "mail",
        rawMessage: args.rawMessage,
        stepGrants,
        ...(args.now !== undefined ? { now: args.now } : {}),
      });
    },

    async enqueueSignal(
      args: EnqueueWorkflowSignalDispatchArgs,
      tx?: DBExecutor,
    ): Promise<EnqueueWorkflowRunDispatchResult> {
      const signal = SignalDeliverFrame.assert({
        type: "signal.deliver",
        ...args.signal,
      });
      const encoded = new TextEncoder().encode(JSON.stringify(signal));
      return insertOrReconcileDispatch(tx ?? db, {
        operation: "enqueueSignal",
        id: args.id,
        anchorRunId: args.anchorRunId,
        messageId: signal.signalId,
        kind: "signal",
        rawMessage: encoded,
        stepGrants: [],
        ...(args.now !== undefined ? { now: args.now } : {}),
      });
    },

    async claimNextPending(
      args: ClaimWorkflowRunDispatchArgs,
    ): Promise<ParsedDispatch | null> {
      return db.transaction(async (tx) => {
        const [candidate] = await tx
          .select({ id: workflowRunDispatch.id })
          .from(workflowRunDispatch)
          .where(
            and(
              eq(workflowRunDispatch.status, "pending"),
              lte(workflowRunDispatch.nextAttemptAt, sql`now()`),
              or(
                isNull(workflowRunDispatch.deliveryLeaseExpiresAt),
                lte(workflowRunDispatch.deliveryLeaseExpiresAt, sql`now()`),
              ),
            ),
          )
          .orderBy(
            asc(workflowRunDispatch.nextAttemptAt),
            asc(workflowRunDispatch.createdAt),
          )
          .limit(1)
          .for("update", { skipLocked: true });
        if (candidate === undefined) return null;
        const [claimed] = await tx
          .update(workflowRunDispatch)
          .set({
            deliveryLeaseId: args.leaseId,
            deliveryLeaseExpiresAt: sql`now() + (${args.leaseDurationMs} * interval '1 millisecond')`,
          })
          .where(eq(workflowRunDispatch.id, candidate.id))
          .returning();
        return claimed === undefined
          ? null
          : parseWorkflowRunDispatchRow(claimed);
      });
    },

    async acknowledge(
      args: AcknowledgeWorkflowRunDispatchArgs,
    ): Promise<ParsedDispatch | null> {
      return db.transaction(async (tx) => {
        const [allocation] = await tx
          .select({
            anchorRunId: sidecarAllocation.anchorRunId,
            status: sidecarAllocation.status,
            generation: sidecarAllocation.generation,
            ensureAcceptedGeneration:
              sidecarAllocation.ensureAcceptedGeneration,
          })
          .from(sidecarAllocation)
          .where(eq(sidecarAllocation.id, args.allocationId))
          .limit(1)
          .for("update");
        if (
          allocation === undefined ||
          allocation.anchorRunId !== args.anchorRunId ||
          allocation.status !== "allocated" ||
          allocation.generation !== args.generation ||
          allocation.ensureAcceptedGeneration !== args.generation
        ) {
          return null;
        }
        return acknowledgeMatchingDispatch(
          tx,
          args.generation,
          [
            eq(workflowRunDispatch.anchorRunId, args.anchorRunId),
            eq(workflowRunDispatch.messageId, args.messageId),
            eq(workflowRunDispatch.kind, "mail"),
            eq(workflowRunDispatch.status, "pending"),
          ],
          args.now,
        );
      });
    },

    async scheduleRetry(
      args: RetryWorkflowRunDispatchArgs,
    ): Promise<ParsedDispatch | null> {
      const [updated] = await db
        .update(workflowRunDispatch)
        .set({
          nextAttemptAt: args.nextAttemptAt,
          attemptCount: sql`${workflowRunDispatch.attemptCount} + 1`,
          deliveryLeaseId: null,
          deliveryLeaseExpiresAt: null,
          failureCode: args.code,
          failureMessage: args.message,
          updatedAt: databaseTimestamp(args.now),
        })
        .where(
          and(
            eq(workflowRunDispatch.id, args.dispatchId),
            eq(workflowRunDispatch.status, "pending"),
            ...leaseCondition(args.expectedLeaseId),
          ),
        )
        .returning();
      return updated === undefined
        ? null
        : parseWorkflowRunDispatchRow(updated);
    },

    async settle(
      anchorRunId: string,
      messageId: string,
      now = new Date(),
    ): Promise<ParsedDispatch | null> {
      const [updated] = await db
        .update(workflowRunDispatch)
        .set({
          status: "settled",
          nextAttemptAt: null,
          deliveryLeaseId: null,
          deliveryLeaseExpiresAt: null,
          failureCode: null,
          failureMessage: null,
          settledAt: now,
          updatedAt: now,
        })
        .where(
          and(
            eq(workflowRunDispatch.anchorRunId, anchorRunId),
            eq(workflowRunDispatch.messageId, messageId),
            inArray(workflowRunDispatch.status, ["pending", "acknowledged"]),
          ),
        )
        .returning();
      return updated === undefined
        ? null
        : parseWorkflowRunDispatchRow(updated);
    },

    async fail(
      args: FailWorkflowRunDispatchArgs,
    ): Promise<ParsedDispatch | null> {
      const now = args.now ?? new Date();
      const [updated] = await db
        .update(workflowRunDispatch)
        .set({
          status: "failed",
          nextAttemptAt: null,
          deliveryLeaseId: null,
          deliveryLeaseExpiresAt: null,
          failureCode: args.code,
          failureMessage: args.message,
          updatedAt: now,
        })
        .where(
          and(
            eq(workflowRunDispatch.anchorRunId, args.anchorRunId),
            eq(workflowRunDispatch.messageId, args.messageId),
            inArray(workflowRunDispatch.status, ["pending", "acknowledged"]),
          ),
        )
        .returning();
      return updated === undefined
        ? null
        : parseWorkflowRunDispatchRow(updated);
    },

    async failUnsettled(
      anchorRunId: string,
      code: string,
      message: string,
      now: Date | SQL = new Date(),
      tx?: DBExecutor,
    ): Promise<number> {
      const rows = await (tx ?? db)
        .update(workflowRunDispatch)
        .set({
          status: "failed",
          nextAttemptAt: null,
          deliveryLeaseId: null,
          deliveryLeaseExpiresAt: null,
          failureCode: code,
          failureMessage: message,
          updatedAt: now,
        })
        .where(
          and(
            eq(workflowRunDispatch.anchorRunId, anchorRunId),
            inArray(workflowRunDispatch.status, ["pending", "acknowledged"]),
          ),
        )
        .returning({ id: workflowRunDispatch.id });
      return rows.length;
    },

    async requeueUnsettled(anchorRunId: string): Promise<number> {
      const rows = await db
        .update(workflowRunDispatch)
        .set({
          status: "pending",
          acknowledgedGeneration: null,
          acknowledgedAt: null,
          nextAttemptAt: sql`now()`,
          deliveryLeaseId: null,
          deliveryLeaseExpiresAt: null,
          failureCode: null,
          failureMessage: null,
          updatedAt: sql`now()`,
        })
        .where(
          and(
            eq(workflowRunDispatch.anchorRunId, anchorRunId),
            inArray(workflowRunDispatch.status, ["pending", "acknowledged"]),
          ),
        )
        .returning({ id: workflowRunDispatch.id });
      return rows.length;
    },

    async findById(id: string): Promise<ParsedDispatch | null> {
      const row = await db.query.workflowRunDispatch.findFirst({
        where: eq(workflowRunDispatch.id, id),
      });
      return row === undefined ? null : parseWorkflowRunDispatchRow(row);
    },

    async listUnsettled(anchorRunId: string): Promise<ParsedDispatch[]> {
      const rows = await db
        .select()
        .from(workflowRunDispatch)
        .where(
          and(
            eq(workflowRunDispatch.anchorRunId, anchorRunId),
            inArray(workflowRunDispatch.status, ["pending", "acknowledged"]),
          ),
        )
        .orderBy(asc(workflowRunDispatch.createdAt));
      return rows.map(parseWorkflowRunDispatchRow);
    },
  };
}

export type WorkflowRunDispatchStore = ReturnType<
  typeof createWorkflowRunDispatchStore
>;
