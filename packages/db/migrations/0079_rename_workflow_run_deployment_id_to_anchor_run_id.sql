ALTER TABLE "workflow_run" RENAME COLUMN "deployment_id" TO "anchor_run_id";--> statement-breakpoint
ALTER TABLE "workflow_run" DROP CONSTRAINT "workflow_run_deployment_id_workflow_run_id_fk";
--> statement-breakpoint
ALTER TABLE "workflow_run" ADD CONSTRAINT "workflow_run_anchor_run_id_workflow_run_id_fk" FOREIGN KEY ("anchor_run_id") REFERENCES "public"."workflow_run"("id") ON DELETE cascade ON UPDATE no action;