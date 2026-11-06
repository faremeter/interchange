ALTER TABLE "workflow_run" ADD COLUMN "standby_state" text;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "standby_state_at" timestamp;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "standby_wake_at" timestamp;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "standby_reason" text;