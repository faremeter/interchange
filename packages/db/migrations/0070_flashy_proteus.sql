CREATE TABLE "workflow_run_execution" (
	"id" text PRIMARY KEY NOT NULL,
	"run_id" text NOT NULL,
	"message_id" text NOT NULL,
	"status" text DEFAULT 'running' NOT NULL,
	"started_at" timestamp DEFAULT now() NOT NULL,
	"ended_at" timestamp,
	"failure_reason" text
);
--> statement-breakpoint
ALTER TABLE "workflow_run_execution" ADD CONSTRAINT "workflow_run_execution_run_id_workflow_run_id_fk" FOREIGN KEY ("run_id") REFERENCES "public"."workflow_run"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "workflow_run_execution_run_id_id_idx" ON "workflow_run_execution" USING btree ("run_id","id");--> statement-breakpoint
CREATE INDEX "workflow_run_execution_status_idx" ON "workflow_run_execution" USING btree ("status");