import { eq } from "drizzle-orm";

import type { DB, DBExecutor } from "./client";
import { parseWorkflowRunLaunchSpecRow } from "./parse-row";
import { workflowRunLaunchSpec } from "./schema/workflow-run-launch-spec";

type DBHandle = DB["db"];
type LaunchSpecInsert = typeof workflowRunLaunchSpec.$inferInsert;
type ParsedLaunchSpec = ReturnType<typeof parseWorkflowRunLaunchSpecRow>;

/** Store for immutable workflow recovery inputs keyed by anchor run. */
export function createWorkflowRunLaunchSpecStore(db: DBHandle) {
  return {
    async create(
      row: LaunchSpecInsert,
      tx?: DBExecutor,
    ): Promise<ParsedLaunchSpec> {
      parseWorkflowRunLaunchSpecRow({
        ...row,
        schemaVersion: row.schemaVersion ?? 1,
        toolPackagePins: row.toolPackagePins ?? null,
        createdAt: row.createdAt ?? new Date(),
      });
      const [inserted] = await (tx ?? db)
        .insert(workflowRunLaunchSpec)
        .values(row)
        .returning();
      if (inserted === undefined) {
        throw new Error(
          `workflowRunLaunchSpecStore.create: insert returned no row for ${row.anchorRunId}`,
        );
      }
      return parseWorkflowRunLaunchSpecRow(inserted);
    },

    async get(
      anchorRunId: string,
      tx?: DBExecutor,
    ): Promise<ParsedLaunchSpec | null> {
      const row = await (tx ?? db).query.workflowRunLaunchSpec.findFirst({
        where: eq(workflowRunLaunchSpec.anchorRunId, anchorRunId),
      });
      return row === undefined ? null : parseWorkflowRunLaunchSpecRow(row);
    },
  };
}

export type WorkflowRunLaunchSpecStore = ReturnType<
  typeof createWorkflowRunLaunchSpecStore
>;
