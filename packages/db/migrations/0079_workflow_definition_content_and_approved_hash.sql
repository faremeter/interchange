DROP INDEX "workflow_definition_asset_idx";--> statement-breakpoint
ALTER TABLE "workflow_definition" ADD COLUMN "wire_hash" text;--> statement-breakpoint
ALTER TABLE "workflow_definition_version" ADD COLUMN "approved_wire_hash" text;--> statement-breakpoint
CREATE UNIQUE INDEX "workflow_definition_asset_wire_hash_idx" ON "workflow_definition" USING btree ("asset_id","wire_hash");