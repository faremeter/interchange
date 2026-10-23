ALTER TABLE "workflow_run" ADD COLUMN "park_state" text;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "park_state_at" timestamp;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "parked_wake_at" timestamp;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "park_reason" text;