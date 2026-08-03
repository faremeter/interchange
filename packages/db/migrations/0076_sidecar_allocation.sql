CREATE TABLE "sidecar_allocation" (
	"id" text PRIMARY KEY NOT NULL,
	"anchor_run_id" text NOT NULL,
	"tenant_id" text NOT NULL,
	"provisioner_id" text NOT NULL,
	"provisioner_api_version" integer NOT NULL,
	"provisioner_binding_fingerprint" text NOT NULL,
	"sidecar_id" text,
	"placement_sharing" text NOT NULL,
	"sidecar_reuse" text DEFAULT 'never' NOT NULL,
	"status" text DEFAULT 'pending' NOT NULL,
	"generation" integer DEFAULT 0 NOT NULL,
	"ensure_accepted_generation" integer,
	"external_ref" text,
	"next_attempt_at" timestamp,
	"reconciliation_lease_id" text,
	"reconciliation_lease_expires_at" timestamp,
	"ensure_attempts" integer DEFAULT 0 NOT NULL,
	"destroy_attempts" integer DEFAULT 0 NOT NULL,
	"connect_deadline" timestamp,
	"failure_code" text,
	"failure_message" text,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "sidecar_allocation_status_check" CHECK ("sidecar_allocation"."status" in ('pending', 'provisioning', 'allocated', 'replacing', 'releasing', 'released', 'failed')),
	CONSTRAINT "sidecar_allocation_placement_check" CHECK ("sidecar_allocation"."placement_sharing" = 'exclusive'),
	CONSTRAINT "sidecar_allocation_generation_check" CHECK ("sidecar_allocation"."generation" >= 0),
	CONSTRAINT "sidecar_allocation_accepted_generation_check" CHECK ("sidecar_allocation"."ensure_accepted_generation" is null or "sidecar_allocation"."ensure_accepted_generation" <= "sidecar_allocation"."generation")
);
--> statement-breakpoint
ALTER TABLE "sidecar" ALTER COLUMN "url" DROP NOT NULL;--> statement-breakpoint
ALTER TABLE "sidecar" ADD COLUMN "credential_scope" text DEFAULT 'shared' NOT NULL;--> statement-breakpoint
ALTER TABLE "sidecar_allocation" ADD CONSTRAINT "sidecar_allocation_anchor_run_id_workflow_run_id_fk" FOREIGN KEY ("anchor_run_id") REFERENCES "public"."workflow_run"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "sidecar_allocation" ADD CONSTRAINT "sidecar_allocation_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "sidecar_allocation" ADD CONSTRAINT "sidecar_allocation_sidecar_id_sidecar_id_fk" FOREIGN KEY ("sidecar_id") REFERENCES "public"."sidecar"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "sidecar_allocation_anchor_run_idx" ON "sidecar_allocation" USING btree ("anchor_run_id");--> statement-breakpoint
CREATE UNIQUE INDEX "sidecar_allocation_active_sidecar_idx" ON "sidecar_allocation" USING btree ("sidecar_id") WHERE "sidecar_allocation"."status" in ('provisioning', 'allocated', 'replacing', 'releasing');--> statement-breakpoint
CREATE INDEX "sidecar_allocation_sidecar_idx" ON "sidecar_allocation" USING btree ("sidecar_id");--> statement-breakpoint
CREATE INDEX "sidecar_allocation_reconciliation_idx" ON "sidecar_allocation" USING btree ("next_attempt_at","created_at") WHERE "sidecar_allocation"."status" in ('pending', 'provisioning', 'allocated', 'replacing', 'releasing') and "sidecar_allocation"."next_attempt_at" is not null;--> statement-breakpoint
ALTER TABLE "sidecar" ADD CONSTRAINT "sidecar_credential_scope_check" CHECK ("sidecar"."credential_scope" in ('shared', 'allocated'));