CREATE TABLE "workflow_probe" (
	"id" text PRIMARY KEY NOT NULL,
	"tenant_id" text NOT NULL,
	"definition_asset_id" text NOT NULL,
	"source" jsonb NOT NULL,
	"entry" text NOT NULL,
	"pin" text,
	"status" text DEFAULT 'pending' NOT NULL,
	"provisioner_id" text NOT NULL,
	"provisioner_api_version" integer NOT NULL,
	"provisioner_binding_fingerprint" text NOT NULL,
	"sidecar_id" text,
	"generation" integer DEFAULT 0 NOT NULL,
	"external_ref" text,
	"result" jsonb,
	"failure_code" text,
	"failure_message" text,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "workflow_probe_status_check" CHECK ("workflow_probe"."status" in ('pending', 'provisioning', 'probing', 'releasing', 'succeeded', 'failed')),
	CONSTRAINT "workflow_probe_succeeded_result_check" CHECK ("workflow_probe"."status" <> 'succeeded' or "workflow_probe"."result" is not null),
	CONSTRAINT "workflow_probe_generation_check" CHECK ("workflow_probe"."generation" >= 0)
);
--> statement-breakpoint
DELETE FROM "sidecar" WHERE "credential_scope" = 'shared';--> statement-breakpoint
ALTER TABLE "sidecar" DROP CONSTRAINT "sidecar_credential_scope_check";--> statement-breakpoint
ALTER TABLE "workflow_probe" ADD CONSTRAINT "workflow_probe_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "workflow_probe" ADD CONSTRAINT "workflow_probe_definition_asset_id_asset_id_fk" FOREIGN KEY ("definition_asset_id") REFERENCES "public"."asset"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "workflow_probe" ADD CONSTRAINT "workflow_probe_sidecar_id_sidecar_id_fk" FOREIGN KEY ("sidecar_id") REFERENCES "public"."sidecar"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "workflow_probe_active_idx" ON "workflow_probe" USING btree ("created_at") WHERE "workflow_probe"."status" in ('pending', 'provisioning', 'probing', 'releasing');--> statement-breakpoint
CREATE INDEX "workflow_probe_sidecar_idx" ON "workflow_probe" USING btree ("sidecar_id");--> statement-breakpoint
ALTER TABLE "sidecar" DROP COLUMN "credential_scope";
