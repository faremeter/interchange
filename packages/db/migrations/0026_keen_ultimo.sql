CREATE TABLE "session_asset" (
	"instance_id" text NOT NULL,
	"agent_asset_id" text NOT NULL,
	"mount_path" text NOT NULL,
	"asset_pack_sha" text NOT NULL,
	"source_commit_sha" text NOT NULL,
	"materialized_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "session_asset_instance_id_agent_asset_id_pk" PRIMARY KEY("instance_id","agent_asset_id")
);
--> statement-breakpoint
ALTER TABLE "session_asset" ADD CONSTRAINT "session_asset_instance_id_agent_instance_id_fk" FOREIGN KEY ("instance_id") REFERENCES "public"."agent_instance"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "session_asset" ADD CONSTRAINT "session_asset_agent_asset_id_agent_asset_id_fk" FOREIGN KEY ("agent_asset_id") REFERENCES "public"."agent_asset"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "session_asset_pack_sha_idx" ON "session_asset" USING btree ("asset_pack_sha");