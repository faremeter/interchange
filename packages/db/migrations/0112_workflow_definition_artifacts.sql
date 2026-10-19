CREATE TABLE "workflow_definition_artifact" (
	"id" text PRIMARY KEY NOT NULL,
	"revision_id" text NOT NULL,
	"result" jsonb,
	"lease_owner" text,
	"lease_expires_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "workflow_definition_artifact_state_check" CHECK (("workflow_definition_artifact"."result" is null and "workflow_definition_artifact"."lease_owner" is not null and "workflow_definition_artifact"."lease_expires_at" is not null) or ("workflow_definition_artifact"."result" is not null and "workflow_definition_artifact"."lease_owner" is null and "workflow_definition_artifact"."lease_expires_at" is null))
);
--> statement-breakpoint
CREATE TABLE "workflow_definition_revision" (
	"id" text PRIMARY KEY NOT NULL,
	"tenant_id" text NOT NULL,
	"asset_id" text NOT NULL,
	"asset_commit_sha" text NOT NULL,
	"source" jsonb NOT NULL,
	"entry" text NOT NULL,
	"closure" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "workflow_run_launch_spec" ADD COLUMN "definition_artifact_id" text;--> statement-breakpoint
ALTER TABLE "workflow_definition_artifact" ADD CONSTRAINT "workflow_definition_artifact_revision_id_workflow_definition_revision_id_fk" FOREIGN KEY ("revision_id") REFERENCES "public"."workflow_definition_revision"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "workflow_definition_revision" ADD CONSTRAINT "workflow_definition_revision_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "workflow_definition_revision" ADD CONSTRAINT "workflow_definition_revision_asset_id_asset_id_fk" FOREIGN KEY ("asset_id") REFERENCES "public"."asset"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "workflow_definition_artifact_revision_idx" ON "workflow_definition_artifact" USING btree ("revision_id");--> statement-breakpoint
ALTER TABLE "workflow_run_launch_spec" ADD CONSTRAINT "workflow_run_launch_spec_definition_artifact_id_workflow_definition_artifact_id_fk" FOREIGN KEY ("definition_artifact_id") REFERENCES "public"."workflow_definition_artifact"("id") ON DELETE restrict ON UPDATE no action;