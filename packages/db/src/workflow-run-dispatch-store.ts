import { isDeepStrictEqual } from "node:util";

import {
  and,
  asc,
  eq,
  exists,
  gt,
  inArray,
  isNull,
  lte,
  notInArray,
  or,
  sql,
  type SQL,
} from "drizzle-orm";

import { deriveDeliveryKey } from "@intx/types/message-id";
import {
  RunGrantsFrame,
  SignalDeliverFrame,
  type RunGrantsFrame as RunGrantsFrameType,
  type SignalDeliverFrame as SignalDeliverFrameType,
} from "@intx/types/sidecar";

import type { DB, DBExecutor } from "./client";
import {
  parseWorkflowRunDispatchKind,
  parseWorkflowRunDispatchRow,
} from "./parse-row";
import {
  liveWorkflowRunStatuses,
  sidecarAllocation,
  workflowRun,
  workflowRunDispatch,
  unresolvedWorkflowRunDispatchStatuses,
} from "./schema";

type DBHandle = DB["db"];
type ParsedDispatch = ReturnType<typeof parseWorkflowRunDispatchRow>;

export type EnqueueWorkflowRunDispatchArgs = {
  readonly anchorRunId: string;
  readonly messageId: string;
  readonly senderAddress: string;
  readonly rawMessage: Uint8Array;
  readonly stepGrants: RunGrantsFrameType["stepGrants"];
  readonly now?: Date;
};

export type EnqueueWorkflowSignalDispatchArgs = {
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
  readonly anchorRunId: string;
  readonly messageId: string;
  readonly deliveryKey: string;
  readonly kind: ParsedDispatch["kind"];
  readonly senderAddress: string | null;
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
  readonly excludedDispatchIds?: readonly string[];
  readonly leaseId: string;
  readonly leaseDurationMs: number;
};

export type WorkflowRunDispatchAcknowledgement = {
  readonly allocationId: string;
  readonly anchorRunId: string;
  readonly deliveryKey: string;
  readonly generation: number;
};

export type AcknowledgeWorkflowRunDispatchArgs =
  WorkflowRunDispatchAcknowledgement & { readonly now?: Date };

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
  readonly deliveryKey: string;
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
  // The anchor row lock, held to the end of the enclosing transaction,
  // serializes the cross-kind check below with any other enqueue to this
  // anchor.
  const [anchor] = await executor
    .select({ anchorRunId: workflowRun.anchorRunId })
    .from(workflowRun)
    .where(eq(workflowRun.id, args.anchorRunId))
    .limit(1)
    .for("update");
  if (anchor?.anchorRunId !== args.anchorRunId) {
    throw new Error(
      `workflowRunDispatchStore.${args.operation}: run ${args.anchorRunId} is not a deployment anchor`,
    );
  }
  // A signal id may not equal the Message-ID of a mail row in the anchor, nor
  // the reverse. Mail from a legacy claim-check entry resumes an input gate
  // with its bare Message-ID as the signal id, and a workflow-run repo written
  // before the delivery key holds such SignalReceived records. The run records
  // one SignalReceived per id, so the two would share one record and one of
  // them would never be delivered. A mail row written before the key also
  // keeps `dispatch:<anchor>:<Message-ID>` as its row id, which a signal with
  // that id would collide with.
  const [crossKind] = await executor
    .select({ id: workflowRunDispatch.id })
    .from(workflowRunDispatch)
    .where(
      and(
        eq(workflowRunDispatch.anchorRunId, args.anchorRunId),
        args.kind === "mail"
          ? and(
              eq(workflowRunDispatch.kind, "signal"),
              eq(workflowRunDispatch.deliveryKey, args.messageId),
            )
          : and(
              eq(workflowRunDispatch.kind, "mail"),
              eq(workflowRunDispatch.messageId, args.messageId),
            ),
      ),
    )
    .limit(1);
  if (crossKind !== undefined) {
    throw new WorkflowRunDispatchPayloadConflictError(
      args.messageId,
      args.kind === "mail" ? "message" : "signal",
      args.operation,
    );
  }

  const now = databaseTimestamp(args.now);
  const [inserted] = await executor
    .insert(workflowRunDispatch)
    .values({
      id: `dispatch:${args.anchorRunId}:${args.deliveryKey}`,
      anchorRunId: args.anchorRunId,
      messageId: args.messageId,
      deliveryKey: args.deliveryKey,
      kind: args.kind,
      senderAddress: args.senderAddress,
      rawMessage: args.rawMessage,
      stepGrants: args.stepGrants,
      status: "pending",
      nextAttemptAt: now,
      createdAt: now,
      updatedAt: now,
    })
    .onConflictDoNothing({
      target: [
        workflowRunDispatch.anchorRunId,
        workflowRunDispatch.deliveryKey,
      ],
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
      eq(workflowRunDispatch.deliveryKey, args.deliveryKey),
    ),
  });
  if (existing === undefined) {
    throw new Error(
      `workflowRunDispatchStore.${args.operation}: conflicting row disappeared for ${args.anchorRunId}/${args.deliveryKey}`,
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
  // An enqueue's anchor lock must hold until its insert, so an enqueue with
  // no caller transaction runs in its own.
  function withTransaction<T>(
    tx: DBExecutor | undefined,
    run: (executor: DBExecutor) => Promise<T>,
  ): Promise<T> {
    return tx === undefined ? db.transaction(run) : run(tx);
  }

  return {
    async enqueue(
      args: EnqueueWorkflowRunDispatchArgs,
      tx?: DBExecutor,
    ): Promise<EnqueueWorkflowRunDispatchResult> {
      const stepGrants = validateStepGrants(args.anchorRunId, args.stepGrants);
      const deliveryKey = await deriveDeliveryKey(
        args.senderAddress,
        args.messageId,
      );
      return withTransaction(tx, (executor) =>
        insertOrReconcileDispatch(executor, {
          operation: "enqueue",
          anchorRunId: args.anchorRunId,
          messageId: args.messageId,
          deliveryKey,
          kind: "mail",
          senderAddress: args.senderAddress,
          rawMessage: args.rawMessage,
          stepGrants,
          ...(args.now !== undefined ? { now: args.now } : {}),
        }),
      );
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
      return withTransaction(tx, (executor) =>
        insertOrReconcileDispatch(executor, {
          operation: "enqueueSignal",
          anchorRunId: args.anchorRunId,
          messageId: signal.signalId,
          deliveryKey: signal.signalId,
          kind: "signal",
          senderAddress: null,
          rawMessage: encoded,
          stepGrants: [],
          ...(args.now !== undefined ? { now: args.now } : {}),
        }),
      );
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
              exists(
                tx
                  .select({ id: workflowRun.id })
                  .from(workflowRun)
                  .where(
                    and(
                      eq(workflowRun.id, workflowRunDispatch.anchorRunId),
                      inArray(workflowRun.status, [...liveWorkflowRunStatuses]),
                      isNull(workflowRun.cancellationRequestedAt),
                      or(
                        isNull(workflowRun.expiresAt),
                        gt(workflowRun.expiresAt, sql`now()`),
                      ),
                    ),
                  ),
              ),
              lte(workflowRunDispatch.nextAttemptAt, sql`now()`),
              ...(args.excludedDispatchIds !== undefined &&
              args.excludedDispatchIds.length > 0
                ? [
                    notInArray(workflowRunDispatch.id, [
                      ...args.excludedDispatchIds,
                    ]),
                  ]
                : []),
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
            eq(workflowRunDispatch.deliveryKey, args.deliveryKey),
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
      deliveryKey: string,
      now = new Date(),
      tx?: DBExecutor,
    ): Promise<ParsedDispatch | null> {
      const [updated] = await (tx ?? db)
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
            eq(workflowRunDispatch.deliveryKey, deliveryKey),
            inArray(workflowRunDispatch.status, [
              ...unresolvedWorkflowRunDispatchStatuses,
            ]),
          ),
        )
        .returning();
      return updated === undefined
        ? null
        : parseWorkflowRunDispatchRow(updated);
    },

    async fail(
      args: FailWorkflowRunDispatchArgs,
      tx?: DBExecutor,
    ): Promise<ParsedDispatch | null> {
      const now = args.now ?? new Date();
      const [updated] = await (tx ?? db)
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
            eq(workflowRunDispatch.deliveryKey, args.deliveryKey),
            inArray(workflowRunDispatch.status, [
              ...unresolvedWorkflowRunDispatchStatuses,
            ]),
          ),
        )
        .returning();
      return updated === undefined
        ? null
        : parseWorkflowRunDispatchRow(updated);
    },

    /**
     * Settle or fail many of a deployment's unresolved dispatches, without
     * reading them back: a row another writer resolved first stays as it is.
     */
    async resolveMany(
      anchorRunId: string,
      outcomes: readonly {
        readonly deliveryKey: string;
        readonly failure?: { readonly code: string; readonly message: string };
      }[],
      now: Date,
      tx?: DBExecutor,
    ): Promise<void> {
      const executor = tx ?? db;
      const unresolvedNamed = (keys: string[]) =>
        and(
          eq(workflowRunDispatch.anchorRunId, anchorRunId),
          inArray(workflowRunDispatch.deliveryKey, keys),
          inArray(workflowRunDispatch.status, [
            ...unresolvedWorkflowRunDispatchStatuses,
          ]),
        );
      const settled: string[] = [];
      const failed = new Map<
        string,
        { code: string; message: string; keys: string[] }
      >();
      for (const { deliveryKey, failure } of outcomes) {
        if (failure === undefined) {
          settled.push(deliveryKey);
          continue;
        }
        const group = JSON.stringify([failure.code, failure.message]);
        const keys = failed.get(group)?.keys;
        if (keys !== undefined) keys.push(deliveryKey);
        else failed.set(group, { ...failure, keys: [deliveryKey] });
      }
      for (const keys of chunked(settled)) {
        await executor
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
          .where(unresolvedNamed(keys));
      }
      for (const { code, message, keys: groupKeys } of failed.values()) {
        for (const keys of chunked(groupKeys)) {
          await executor
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
            .where(unresolvedNamed(keys));
        }
      }
    },

    /**
     * End attempts alongside the run/allocation transition that fences socket
     * delivery. An abandoned row's `nextAttemptAt` marks it as owed an evidence
     * scan; delivery never claims it, since claims require `pending`.
     */
    async abandonUnsettled(
      anchorRunId: string,
      code: string,
      message: string,
      now: Date | SQL = new Date(),
      tx?: DBExecutor,
    ): Promise<number> {
      const rows = await (tx ?? db)
        .update(workflowRunDispatch)
        .set({
          status: "abandoned",
          nextAttemptAt: now,
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
      // Abandonment ends delivery attempts even if this allocation reconnects.
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

    /** Stop scanning abandoned rows once their deployment's history is final. */
    async concludeAbandonedScans(
      anchorRunId: string,
      deliveryKeys: readonly string[],
      now = new Date(),
    ): Promise<number> {
      if (deliveryKeys.length === 0) return 0;
      const rows = await db
        .update(workflowRunDispatch)
        .set({ nextAttemptAt: null, updatedAt: now })
        .where(
          and(
            eq(workflowRunDispatch.anchorRunId, anchorRunId),
            eq(workflowRunDispatch.status, "abandoned"),
            inArray(workflowRunDispatch.deliveryKey, [...deliveryKeys]),
          ),
        )
        .returning({ id: workflowRunDispatch.id });
      return rows.length;
    },

    async listUnsettled(anchorRunId: string): Promise<ParsedDispatch[]> {
      const rows = await db
        .select()
        .from(workflowRunDispatch)
        .where(
          and(
            eq(workflowRunDispatch.anchorRunId, anchorRunId),
            inArray(workflowRunDispatch.status, [
              ...unresolvedWorkflowRunDispatchStatuses,
            ]),
          ),
        )
        .orderBy(asc(workflowRunDispatch.createdAt));
      return rows.map(parseWorkflowRunDispatchRow);
    },

    /**
     * The identities of a deployment's unsettled dispatches, in the order
     * `listUnsettled` gives.
     */
    async listUnsettledKeys(
      anchorRunId: string,
      tx?: DBExecutor,
    ): Promise<UnsettledDispatchKey[]> {
      return selectUnsettledKeys(tx ?? db, anchorRunId, undefined);
    },

    /**
     * The unsettled dispatches of a deployment that evidence names, as
     * `listUnsettledKeys` gives them: the rows keyed by one of `keys`, and
     * the mail rows whose Message-ID is one of `messageIds`. Reads only
     * those rows, so a receive does not pay for the deployment's backlog.
     */
    async listUnsettledKeysNamed(
      anchorRunId: string,
      named: {
        readonly keys: readonly string[];
        readonly messageIds: readonly string[];
      },
      tx?: DBExecutor,
    ): Promise<UnsettledDispatchKey[]> {
      const keyChunks = chunked(named.keys);
      const messageIdChunks = chunked(named.messageIds);
      const found = new Map<string, UnsettledDispatchKey>();
      for (
        let i = 0;
        i < Math.max(keyChunks.length, messageIdChunks.length);
        i += 1
      ) {
        const keys = keyChunks[i];
        const messageIds = messageIdChunks[i];
        const match = or(
          keys === undefined
            ? undefined
            : inArray(workflowRunDispatch.deliveryKey, keys),
          messageIds === undefined
            ? undefined
            : and(
                eq(workflowRunDispatch.kind, "mail"),
                inArray(workflowRunDispatch.messageId, messageIds),
              ),
        );
        for (const row of await selectUnsettledKeys(
          tx ?? db,
          anchorRunId,
          match,
        )) {
          found.set(row.deliveryKey, row);
        }
      }
      return [...found.values()].sort(
        (a, b) => a.createdAt.getTime() - b.createdAt.getTime(),
      );
    },
  };
}

// Bounds the parameters of one statement.
const KEY_BATCH = 1000;

function chunked(keys: readonly string[]): string[][] {
  const chunks: string[][] = [];
  for (let start = 0; start < keys.length; start += KEY_BATCH) {
    chunks.push(keys.slice(start, start + KEY_BATCH));
  }
  return chunks;
}

type UnsettledDispatchKey = {
  deliveryKey: string;
  messageId: string;
  kind: ParsedDispatch["kind"];
  rawMessage: Uint8Array | null;
  createdAt: Date;
};

// A signal carries its frame, which names the run it is for; a mail's
// bytes are not read.
async function selectUnsettledKeys(
  executor: DBExecutor,
  anchorRunId: string,
  match: SQL | undefined,
): Promise<UnsettledDispatchKey[]> {
  const rows = await executor
    .select({
      deliveryKey: workflowRunDispatch.deliveryKey,
      messageId: workflowRunDispatch.messageId,
      kind: workflowRunDispatch.kind,
      createdAt: workflowRunDispatch.createdAt,
      rawMessage:
        sql<Uint8Array | null>`case when ${workflowRunDispatch.kind} = 'signal' then ${workflowRunDispatch.rawMessage} end`.mapWith(
          workflowRunDispatch.rawMessage,
        ),
    })
    .from(workflowRunDispatch)
    .where(
      and(
        eq(workflowRunDispatch.anchorRunId, anchorRunId),
        inArray(workflowRunDispatch.status, [
          ...unresolvedWorkflowRunDispatchStatuses,
        ]),
        match,
      ),
    )
    .orderBy(asc(workflowRunDispatch.createdAt));
  return rows.map((row) => ({
    ...row,
    kind: parseWorkflowRunDispatchKind(row.kind),
  }));
}

export type WorkflowRunDispatchStore = ReturnType<
  typeof createWorkflowRunDispatchStore
>;
