import { and, eq, isNull, sql } from "drizzle-orm";

import { WorkflowProbeResultFrame } from "@intx/types/sidecar";
import { ToolPackageManifest } from "@intx/types/tool-packages";
import { WorkflowDefinitionSource } from "@intx/types/workflow-sources";

import type { DB } from "./client";
import {
  isReferencedRowViolation,
  pgErrorCode,
  PG_FOREIGN_KEY_VIOLATION,
} from "./pg-error";
import {
  workflowDefinitionArtifact,
  workflowDefinitionRevision,
} from "./schema/workflow-definition-artifacts";

const ProbeResult = WorkflowProbeResultFrame.omit("type", "requestId");
type ProbeResult = typeof ProbeResult.infer;
type RevisionRow = typeof workflowDefinitionRevision.$inferSelect;

/** `revision_missing`: a failed probe deleted the revision before this claim. */
type WorkflowProbeClaim = "claimed" | "held" | "revision_missing";

function parseRevision(row: RevisionRow) {
  return {
    ...row,
    source: WorkflowDefinitionSource.assert(row.source),
    closure: ToolPackageManifest.assert(row.closure),
  };
}

export function createWorkflowDefinitionArtifactStore(db: DB["db"]) {
  async function getRevision(id: string) {
    const row = await db.query.workflowDefinitionRevision.findFirst({
      where: eq(workflowDefinitionRevision.id, id),
    });
    return row === undefined ? null : parseRevision(row);
  }

  return {
    getRevision,

    async hasRevision(id: string): Promise<boolean> {
      const row = await db.query.workflowDefinitionRevision.findFirst({
        columns: { id: true },
        where: eq(workflowDefinitionRevision.id, id),
      });
      return row !== undefined;
    },

    // Concurrent resolutions converge on the first stored closure, so callers
    // read the revision back instead of trusting the closure they resolved.
    async createRevision(row: typeof workflowDefinitionRevision.$inferInsert) {
      WorkflowDefinitionSource.assert(row.source);
      ToolPackageManifest.assert(row.closure);
      await db
        .insert(workflowDefinitionRevision)
        .values(row)
        .onConflictDoNothing();
    },

    // Any artifact row, pending or published, keeps its revision.
    async deleteRevision(id: string): Promise<boolean> {
      try {
        const rows = await db
          .delete(workflowDefinitionRevision)
          .where(eq(workflowDefinitionRevision.id, id))
          .returning({ id: workflowDefinitionRevision.id });
        return rows.length === 1;
      } catch (error) {
        if (isReferencedRowViolation(error)) return false;
        throw error;
      }
    },

    async get(id: string): Promise<{
      result: ProbeResult | null;
      leaseOwner: string | null;
    } | null> {
      const row = await db.query.workflowDefinitionArtifact.findFirst({
        where: eq(workflowDefinitionArtifact.id, id),
      });
      if (row === undefined) return null;
      return {
        result: row.result === null ? null : ProbeResult.assert(row.result),
        leaseOwner: row.leaseOwner,
      };
    },

    // Database time owns lease expiry across Hub processes. A late producer
    // cannot publish after another process has taken over its claim.
    async claim(args: {
      id: string;
      revisionId: string;
      owner: string;
      leaseMs: number;
    }): Promise<WorkflowProbeClaim> {
      const leaseExpiresAt = sql`clock_timestamp() + ${args.leaseMs} * interval '1 millisecond'`;
      try {
        const rows = await db
          .insert(workflowDefinitionArtifact)
          .values({
            id: args.id,
            revisionId: args.revisionId,
            leaseOwner: args.owner,
            leaseExpiresAt,
          })
          .onConflictDoUpdate({
            target: workflowDefinitionArtifact.id,
            set: { leaseOwner: args.owner, leaseExpiresAt },
            setWhere: sql`${workflowDefinitionArtifact.result} is null and ${workflowDefinitionArtifact.leaseExpiresAt} <= clock_timestamp()`,
          })
          .returning({ id: workflowDefinitionArtifact.id });
        return rows.length === 1 ? "claimed" : "held";
      } catch (error) {
        if (pgErrorCode(error) === PG_FOREIGN_KEY_VIOLATION) {
          return "revision_missing";
        }
        throw error;
      }
    },

    async renew(id: string, owner: string, leaseMs: number): Promise<boolean> {
      const rows = await db
        .update(workflowDefinitionArtifact)
        .set({
          leaseExpiresAt: sql`clock_timestamp() + ${leaseMs} * interval '1 millisecond'`,
        })
        .where(
          and(
            eq(workflowDefinitionArtifact.id, id),
            eq(workflowDefinitionArtifact.leaseOwner, owner),
            isNull(workflowDefinitionArtifact.result),
            sql`${workflowDefinitionArtifact.leaseExpiresAt} > clock_timestamp()`,
          ),
        )
        .returning({ id: workflowDefinitionArtifact.id });
      return rows.length === 1;
    },

    async complete(
      id: string,
      owner: string,
      result: ProbeResult,
    ): Promise<boolean> {
      ProbeResult.assert(result);
      const rows = await db
        .update(workflowDefinitionArtifact)
        .set({ result, leaseOwner: null, leaseExpiresAt: null })
        .where(
          and(
            eq(workflowDefinitionArtifact.id, id),
            eq(workflowDefinitionArtifact.leaseOwner, owner),
            isNull(workflowDefinitionArtifact.result),
            sql`${workflowDefinitionArtifact.leaseExpiresAt} > clock_timestamp()`,
          ),
        )
        .returning({ id: workflowDefinitionArtifact.id });
      return rows.length === 1;
    },

    async abandon(id: string, owner: string): Promise<void> {
      await db
        .delete(workflowDefinitionArtifact)
        .where(
          and(
            eq(workflowDefinitionArtifact.id, id),
            eq(workflowDefinitionArtifact.leaseOwner, owner),
            isNull(workflowDefinitionArtifact.result),
          ),
        );
    },
  };
}

export type WorkflowDefinitionArtifactStore = ReturnType<
  typeof createWorkflowDefinitionArtifactStore
>;
