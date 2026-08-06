import { eq } from "drizzle-orm";

import {
  resolveDefinitionIdForAsset,
  type DBExecutor,
  type WorkflowDefinitionSelector,
} from "@intx/db";
import {
  asset,
  workflowDefinition,
  workflowDefinitionVersion,
} from "@intx/db/schema";
import { generateId } from "@intx/hub-common";

/**
 * The first-class `workflow_definition` a selector names, created if absent. A
 * native workflow carries its body in the asset it points at, so this only
 * projects a definition row -- plus its version "1" -- over that asset, keyed
 * by the selector's wire hash, so a run can anchor on a first-class definition.
 * A single asset backs many definitions distinguished by their wire hash, so
 * distinct hashes over one asset yield distinct definitions.
 * `currentVersion`/`status` are left to the table defaults; the definition
 * carries no `modelRequirements` manifest, so it deploys as a workflow rather
 * than launching as an instance.
 *
 * Idempotent and concurrency-safe: the definition insert conflicts on the
 * unique `(assetId, wireHash)` and the version on `(definitionId, version)`, so
 * two concurrent deploys of the same selector still yield exactly one
 * definition and one version. The returned id is read back after the conflict
 * -- the winner's id, not a locally-minted one -- so a losing insert anchors
 * its version on the real row. `created` distinguishes a fresh projection from
 * a pre-existing one for callers that report fold counts.
 */
export async function ensureWorkflowDefinitionForAsset(
  db: DBExecutor,
  selector: WorkflowDefinitionSelector,
): Promise<{ definitionId: string; created: boolean }> {
  const { assetId, wireHash } = selector;
  const assetRow = await db
    .select({
      tenantId: asset.tenantId,
      creatorPrincipalId: asset.creatorPrincipalId,
      name: asset.name,
      displayName: asset.displayName,
    })
    .from(asset)
    .where(eq(asset.id, assetId))
    .limit(1)
    .then((rows) => rows[0]);
  if (assetRow === undefined) {
    throw new Error(
      `ensureWorkflowDefinitionForAsset: no asset found for id "${assetId}"`,
    );
  }

  const inserted = await db
    .insert(workflowDefinition)
    .values({
      id: generateId("workflowDefinition"),
      tenantId: assetRow.tenantId,
      creatorPrincipalId: assetRow.creatorPrincipalId,
      assetId,
      wireHash,
      name: assetRow.name,
      description: assetRow.displayName,
    })
    .onConflictDoNothing({
      target: [workflowDefinition.assetId, workflowDefinition.wireHash],
    })
    .returning({ id: workflowDefinition.id });

  const created = inserted.length > 0;
  const insertedId = inserted[0]?.id;
  const definitionId =
    insertedId ?? (await resolveDefinitionIdForAsset(db, selector));
  if (definitionId === null || definitionId === undefined) {
    throw new Error(
      `ensureWorkflowDefinitionForAsset: definition for asset "${assetId}" (wire hash "${wireHash}") missing after insert`,
    );
  }

  await db
    .insert(workflowDefinitionVersion)
    .values({
      id: generateId("workflowDefinitionVersion"),
      definitionId,
      version: "1",
    })
    .onConflictDoNothing({
      target: [
        workflowDefinitionVersion.definitionId,
        workflowDefinitionVersion.version,
      ],
    });

  return { definitionId, created };
}
