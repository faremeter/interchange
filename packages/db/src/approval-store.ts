import { and, eq } from "drizzle-orm";

import type { DB, DBExecutor } from "./client";
import { approval } from "./schema/approvals";
import { parseApprovalRow } from "./parse-row";

type DBHandle = DB["db"];

type ApprovalRow = typeof approval.$inferSelect;
type ApprovalInsert = typeof approval.$inferInsert;
type ParsedApproval = ReturnType<typeof parseApprovalRow>;

// A resolution is any terminal status: the pending state is excluded because
// resolving is precisely the transition out of it.
type ApprovalResolution = Exclude<ApprovalRow["status"], "pending">;
type ApprovalScope = NonNullable<ApprovalRow["scope"]>;

export type ResolveApprovalArgs = {
  status: ApprovalResolution;
  scope?: ApprovalScope;
  resolvedAt: Date;
};

/**
 * Store for the `approval` table backing the approval round-trip. Each method
 * accepts an optional transaction handle so the resolver can flip the approval
 * and its signal correlation in a single `db.transaction`; when omitted the
 * method runs against the store's own connection.
 */
export function createApprovalStore(db: DBHandle) {
  return {
    async create(
      row: ApprovalInsert,
      tx?: DBExecutor,
    ): Promise<ParsedApproval> {
      const [inserted] = await (tx ?? db)
        .insert(approval)
        .values(row)
        .returning();
      if (inserted === undefined) {
        throw new Error(
          `approvalStore.create: insert returned no row for ${row.id}`,
        );
      }
      return parseApprovalRow(inserted);
    },

    /**
     * Idempotent variant of `create`. On a `correlationId` unique conflict the
     * insert is a no-op and this returns `null` rather than throwing, so a
     * redelivered register frame (sidecar reconnect, workflow-log replay,
     * supervisor restart re-emitting) does not fail the co-write. Conflicts
     * on `correlationId` -- not the `id` primary key -- because the register
     * co-write mints a fresh `id` per frame while the correlation is the stable
     * dedup key. Returns the parsed row only when this call performed the
     * insert.
     */
    async createIfAbsent(
      row: ApprovalInsert,
      tx?: DBExecutor,
    ): Promise<ParsedApproval | null> {
      const [inserted] = await (tx ?? db)
        .insert(approval)
        .values(row)
        .onConflictDoNothing({ target: approval.correlationId })
        .returning();
      return inserted === undefined ? null : parseApprovalRow(inserted);
    },

    async findByCorrelationId(
      correlationId: string,
      tx?: DBExecutor,
    ): Promise<ParsedApproval | null> {
      const row = await (tx ?? db).query.approval.findFirst({
        where: eq(approval.correlationId, correlationId),
      });
      return row === undefined ? null : parseApprovalRow(row);
    },

    /**
     * Look up an approval by its primary key. The resolve/reject routes key on
     * the `approvalId` path parameter, which is the row's `id`, so they need a
     * by-id read distinct from the by-correlation read the register co-write
     * uses. Returns null when no row carries the id.
     */
    async findById(
      id: string,
      tx?: DBExecutor,
    ): Promise<ParsedApproval | null> {
      const row = await (tx ?? db).query.approval.findFirst({
        where: eq(approval.id, id),
      });
      return row === undefined ? null : parseApprovalRow(row);
    },

    /**
     * Conditionally resolve a pending approval. The `WHERE status = 'pending'`
     * guard makes resolution terminal at the database: the first caller to
     * resolve a given correlation gets the updated row back; any later caller
     * (a duplicate delivery, a timeout racing an approval) matches no row and
     * receives null.
     */
    async resolve(
      correlationId: string,
      args: ResolveApprovalArgs,
      tx?: DBExecutor,
    ): Promise<ParsedApproval | null> {
      const [updated] = await (tx ?? db)
        .update(approval)
        .set({
          status: args.status,
          scope: args.scope ?? null,
          resolvedAt: args.resolvedAt,
          updatedAt: new Date(),
        })
        .where(
          and(
            eq(approval.correlationId, correlationId),
            eq(approval.status, "pending"),
          ),
        )
        .returning();
      return updated === undefined ? null : parseApprovalRow(updated);
    },
  };
}

export type ApprovalStore = ReturnType<typeof createApprovalStore>;
