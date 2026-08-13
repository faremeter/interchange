ALTER TABLE "signal_correlation" RENAME COLUMN "deployment_id" TO "anchor_run_id";--> statement-breakpoint
ALTER TABLE "approval" RENAME COLUMN "deployment_id" TO "anchor_run_id";--> statement-breakpoint
ALTER TABLE "approval" DROP CONSTRAINT "approval_deployment_id_workflow_run_id_fk";
--> statement-breakpoint
ALTER TABLE "signal_correlation" DROP CONSTRAINT "signal_correlation_deployment_id_workflow_run_id_fk";
--> statement-breakpoint
ALTER INDEX "approval_deployment_idx" RENAME TO "approval_anchor_run_idx";--> statement-breakpoint
ALTER TABLE "approval" ADD CONSTRAINT "approval_anchor_run_id_workflow_run_id_fk" FOREIGN KEY ("anchor_run_id") REFERENCES "public"."workflow_run"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "signal_correlation" ADD CONSTRAINT "signal_correlation_anchor_run_id_workflow_run_id_fk" FOREIGN KEY ("anchor_run_id") REFERENCES "public"."workflow_run"("id") ON DELETE cascade ON UPDATE no action;