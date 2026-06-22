CREATE TABLE "workflow_deployment" (
	"id" text PRIMARY KEY NOT NULL,
	"tenant_id" text NOT NULL,
	"definition_asset_id" text NOT NULL,
	"status" text DEFAULT 'deployed' NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "workflow_deployment" ADD CONSTRAINT "workflow_deployment_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "workflow_deployment" ADD CONSTRAINT "workflow_deployment_definition_asset_id_asset_id_fk" FOREIGN KEY ("definition_asset_id") REFERENCES "public"."asset"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "workflow_deployment_tenant_idx" ON "workflow_deployment" USING btree ("tenant_id","created_at");