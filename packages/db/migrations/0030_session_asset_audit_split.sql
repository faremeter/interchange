ALTER TABLE "session_asset" DROP CONSTRAINT "session_asset_instance_id_agent_asset_id_pk";--> statement-breakpoint
ALTER TABLE "session_asset" ALTER COLUMN "agent_asset_id" DROP NOT NULL;--> statement-breakpoint
ALTER TABLE "session_asset" ADD CONSTRAINT "session_asset_instance_id_mount_path_pk" PRIMARY KEY("instance_id","mount_path");--> statement-breakpoint
ALTER TABLE "session_asset" ADD COLUMN "source" text NOT NULL DEFAULT 'direct';--> statement-breakpoint
ALTER TABLE "session_asset" ALTER COLUMN "source" DROP DEFAULT;
