ALTER TABLE "workflow_definition" ADD COLUMN "lifecycle_policy" jsonb;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "lifecycle_policy" jsonb;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "expires_at" timestamp;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "cancellation_requested_at" timestamp;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "cancellation_deadline" timestamp;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "capacity_release_at" timestamp;