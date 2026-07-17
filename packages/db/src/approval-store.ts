import { and, eq } from "drizzle-orm";

import type { DB } from "./client";
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
    async create(row: ApprovalInsert, tx?: DBHandle): Promise<ApprovalRow> {
      const [inserted] = await (tx ?? db)
        .insert(approval)
        .values(row)
        .returning();
      if (inserted === undefined) {
        throw new Error(
          `approvalStore.create: insert returned no row for ${row.id}`,
        );
      }
      return inserted;
    },

    async findByCorrelationId(
      correlationId: string,
      tx?: DBHandle,
    ): Promise<ParsedApproval | null> {
      const row = await (tx ?? db).query.approval.findFirst({
        where: eq(approval.correlationId, correlationId),
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
      tx?: DBHandle,
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
