CREATE TABLE "workflow_run_launch_spec" (
	"anchor_run_id" text PRIMARY KEY NOT NULL,
	"schema_version" integer DEFAULT 1 NOT NULL,
	"session_id" text NOT NULL,
	"deployment_domain" text NOT NULL,
	"source_authority_principal_id" text NOT NULL,
	"definition_snapshot" jsonb NOT NULL,
	"definition_hash" text NOT NULL,
	"source_offering_ids" jsonb NOT NULL,
	"default_source_offering_id" text NOT NULL,
	"deploy_content" jsonb NOT NULL,
	"tool_package_pins" jsonb,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "workflow_run_launch_spec" ADD CONSTRAINT "workflow_run_launch_spec_anchor_run_id_workflow_run_id_fk" FOREIGN KEY ("anchor_run_id") REFERENCES "public"."workflow_run"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "workflow_run_launch_spec" ADD CONSTRAINT "workflow_run_launch_spec_source_authority_principal_id_principal_id_fk" FOREIGN KEY ("source_authority_principal_id") REFERENCES "public"."principal"("id") ON DELETE restrict ON UPDATE no action;