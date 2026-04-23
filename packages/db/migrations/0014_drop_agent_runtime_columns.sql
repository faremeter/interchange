-- Normalize any stale runtime statuses before dropping the columns
-- that tracked them. The agent table now only uses deployed/stopped.
UPDATE "agent" SET "status" = 'deployed' WHERE "status" IN ('running', 'updating', 'error');
--> statement-breakpoint
ALTER TABLE "agent" DROP CONSTRAINT IF EXISTS "agent_sidecar_id_sidecar_id_fk";
--> statement-breakpoint
ALTER TABLE "agent" DROP CONSTRAINT IF EXISTS "agent_sidecar_id_fkey";
--> statement-breakpoint
ALTER TABLE "agent" DROP COLUMN "sidecar_id";--> statement-breakpoint
ALTER TABLE "agent" DROP COLUMN "public_key";--> statement-breakpoint
ALTER TABLE "agent" DROP COLUMN "kernel_id";--> statement-breakpoint
ALTER TABLE "agent" DROP COLUMN "session_id";
