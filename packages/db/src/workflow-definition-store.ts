import { and, eq } from "drizzle-orm";

import type { DB } from "./client";
import {
  workflowDefinition,
  workflowDefinitionVersion,
} from "./schema/workflow-definitions";
import { parseWorkflowDefinitionRow } from "./parse-row";

type DBHandle = DB["db"];
type ParsedWorkflowDefinition = ReturnType<typeof parseWorkflowDefinitionRow>;

/**
 * The definition that a workflow asset belongs to, or null when the asset was
 * never folded into a definition. This is the single expression of the
 * deployment -> definition mapping (a deployment names its asset via
 * `definitionAssetId`); the run backfill and the native-run insert sites both
 * resolve through it. Null on a miss is deliberate, not an error: a deployment
 * whose asset the run-once fold never covered has no definition yet, and its
 * runs anchor on `deploymentId` until that gap closes.
 */
export async function resolveDefinitionIdForAsset(
  db: DBHandle,
  assetId: string,
): Promise<string | null> {
  const row = await db
    .select({ id: workflowDefinition.id })
    .from(workflowDefinition)
    .where(eq(workflowDefinition.assetId, assetId))
    .limit(1)
    .then((rows) => rows[0]);
  return row?.id ?? null;
}

export type WorkflowDefinitionRollbackResult =
  | { ok: true; definition: ParsedWorkflowDefinition }
  | { ok: false; reason: "definition_not_found" | "version_not_found" };

/**
 * Store for the first-class `workflow_definition` and its version history.
 */
export function createWorkflowDefinitionStore(db: DBHandle) {
  return {
    /**
     * Roll a definition back to a prior version: deactivate the current
     * version, activate the target, and repoint `currentVersion` in one
     * transaction, so a reader never sees two active versions or a
     * `currentVersion` that names an inactive row. Returns a discriminated
     * result rather than throwing for the not-found cases the route reports as
     * 404 / 400.
     */
    async rollback(
      tenantId: string,
      definitionId: string,
      targetVersion: string,
    ): Promise<WorkflowDefinitionRollbackResult> {
      return db.transaction(async (tx) => {
        const existing = await tx.query.workflowDefinition.findFirst({
          where: and(
            eq(workflowDefinition.id, definitionId),
            eq(workflowDefinition.tenantId, tenantId),
          ),
        });
        if (existing === undefined) {
          return { ok: false, reason: "definition_not_found" };
        }

        const target = await tx.query.workflowDefinitionVersion.findFirst({
          where: and(
            eq(workflowDefinitionVersion.definitionId, definitionId),
            eq(workflowDefinitionVersion.version, targetVersion),
          ),
        });
        if (target === undefined) {
          return { ok: false, reason: "version_not_found" };
        }

        await tx
          .update(workflowDefinitionVersion)
          .set({ status: "inactive" })
          .where(
            and(
              eq(workflowDefinitionVersion.definitionId, definitionId),
              eq(workflowDefinitionVersion.version, existing.currentVersion),
            ),
          );
        await tx
          .update(workflowDefinitionVersion)
          .set({ status: "active" })
          .where(
            and(
              eq(workflowDefinitionVersion.definitionId, definitionId),
              eq(workflowDefinitionVersion.version, targetVersion),
            ),
          );

        const [updated] = await tx
          .update(workflowDefinition)
          .set({ currentVersion: targetVersion, updatedAt: new Date() })
          .where(eq(workflowDefinition.id, definitionId))
          .returning();
        if (updated === undefined) {
          throw new Error(
            `workflowDefinitionStore.rollback: update returned no row for ${definitionId}`,
          );
        }
        return { ok: true, definition: parseWorkflowDefinitionRow(updated) };
      });
    },
  };
}
