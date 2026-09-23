ALTER TABLE "workflow_run" ADD COLUMN "infrastructure_failed_at" timestamp;--> statement-breakpoint
CREATE INDEX "workflow_run_infrastructure_failed_idx" ON "workflow_run" USING btree ("id") WHERE "workflow_run"."infrastructure_failed_at" is not null;--> statement-breakpoint
-- The lifecycle sweep relies on the planner choosing its small partial
-- indexes. The new column has no statistics until the next analyze, and
-- without them the planner may walk every deployment instead.
ANALYZE "workflow_run";--> statement-breakpoint
ANALYZE "sidecar_allocation";
