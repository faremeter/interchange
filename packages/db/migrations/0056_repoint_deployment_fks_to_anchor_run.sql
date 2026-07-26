-- Re-point the three deployment_id foreign keys from the workflow_deployment
-- projection onto the anchor run -- the workflow_run whose id equals the
-- deployment id -- leaving the projection unreferenced. Every deployment_id
-- value already equals an anchor run's id (migration 0055 backfilled one per
-- deployment), so each re-pointed constraint validates on add; a deployment
-- that somehow lacks its anchor run makes the ADD CONSTRAINT abort, a free
-- integrity check.
--
-- First, fill definition_id on the runs the projection still anchors, so every
-- run carries its definition -- the run anchors on the definition, not the
-- deployment. A pre-existing non-anchor run -- a folded or child run created
-- before this branch added the column -- has a null definition_id that only the
-- deployment -> asset -> definition join can resolve; migration 0055 fills the
-- anchor runs it inserts but never touches these existing rows, and
-- workflow_deployment is the only place their mapping survives. asset_id is
-- unique per definition, so the join matches at most one definition per run.
UPDATE "workflow_run" r
SET definition_id = wdef.id
FROM "workflow_deployment" wd
JOIN "workflow_definition" wdef ON wdef.asset_id = wd.definition_asset_id
WHERE r.deployment_id = wd.id
  AND r.definition_id IS NULL;
--> statement-breakpoint
ALTER TABLE "approval" DROP CONSTRAINT "approval_deployment_id_workflow_deployment_id_fk";
--> statement-breakpoint
ALTER TABLE "signal_correlation" DROP CONSTRAINT "signal_correlation_deployment_id_workflow_deployment_id_fk";
--> statement-breakpoint
ALTER TABLE "workflow_run" DROP CONSTRAINT "workflow_run_deployment_id_workflow_deployment_id_fk";
--> statement-breakpoint
ALTER TABLE "approval" ADD CONSTRAINT "approval_deployment_id_workflow_run_id_fk" FOREIGN KEY ("deployment_id") REFERENCES "public"."workflow_run"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "signal_correlation" ADD CONSTRAINT "signal_correlation_deployment_id_workflow_run_id_fk" FOREIGN KEY ("deployment_id") REFERENCES "public"."workflow_run"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD CONSTRAINT "workflow_run_deployment_id_workflow_run_id_fk" FOREIGN KEY ("deployment_id") REFERENCES "public"."workflow_run"("id") ON DELETE cascade ON UPDATE no action;
