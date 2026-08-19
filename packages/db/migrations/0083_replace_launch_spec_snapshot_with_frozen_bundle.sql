ALTER TABLE "workflow_run_launch_spec" ADD COLUMN "frozen_approval_bundle" jsonb NOT NULL;--> statement-breakpoint
ALTER TABLE "workflow_run_launch_spec" DROP COLUMN "definition_snapshot";--> statement-breakpoint
ALTER TABLE "workflow_run_launch_spec" DROP COLUMN "definition_hash";