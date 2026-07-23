CREATE TABLE "workflow_definition" (
	"id" text PRIMARY KEY NOT NULL,
	"tenant_id" text NOT NULL,
	"creator_principal_id" text,
	"asset_id" text,
	"name" text NOT NULL,
	"description" text,
	"grant_requirements" jsonb,
	"current_version" text DEFAULT '1' NOT NULL,
	"status" text DEFAULT 'deployed' NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "workflow_definition_version" (
	"id" text PRIMARY KEY NOT NULL,
	"definition_id" text NOT NULL,
	"version" text NOT NULL,
	"status" text DEFAULT 'active' NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "workflow_definition_version_definition_version" UNIQUE("definition_id","version")
);
--> statement-breakpoint
ALTER TABLE "workflow_definition" ADD CONSTRAINT "workflow_definition_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "workflow_definition" ADD CONSTRAINT "workflow_definition_creator_principal_id_principal_id_fk" FOREIGN KEY ("creator_principal_id") REFERENCES "public"."principal"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "workflow_definition" ADD CONSTRAINT "workflow_definition_asset_id_asset_id_fk" FOREIGN KEY ("asset_id") REFERENCES "public"."asset"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "workflow_definition_version" ADD CONSTRAINT "workflow_definition_version_definition_id_workflow_definition_id_fk" FOREIGN KEY ("definition_id") REFERENCES "public"."workflow_definition"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "workflow_definition_tenant_idx" ON "workflow_definition" USING btree ("tenant_id","created_at");--> statement-breakpoint
CREATE UNIQUE INDEX "workflow_definition_asset_idx" ON "workflow_definition" USING btree ("asset_id");--> statement-breakpoint
CREATE INDEX "workflow_definition_version_definition_idx" ON "workflow_definition_version" USING btree ("definition_id");