CREATE TABLE "workflow_run_dispatch" (
	"id" text PRIMARY KEY NOT NULL,
	"anchor_run_id" text NOT NULL,
	"message_id" text NOT NULL,
	"raw_message" "bytea" NOT NULL,
	"step_grants" jsonb NOT NULL,
	"status" text DEFAULT 'pending' NOT NULL,
	"acknowledged_generation" integer,
	"attempt_count" integer DEFAULT 0 NOT NULL,
	"next_attempt_at" timestamp DEFAULT now(),
	"delivery_lease_id" text,
	"delivery_lease_expires_at" timestamp,
	"failure_code" text,
	"failure_message" text,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	"acknowledged_at" timestamp,
	"settled_at" timestamp,
	CONSTRAINT "workflow_run_dispatch_status_check" CHECK ("workflow_run_dispatch"."status" in ('pending', 'acknowledged', 'settled', 'failed')),
	CONSTRAINT "workflow_run_dispatch_attempt_count_check" CHECK ("workflow_run_dispatch"."attempt_count" >= 0),
	CONSTRAINT "workflow_run_dispatch_acknowledged_generation_check" CHECK ("workflow_run_dispatch"."acknowledged_generation" is null or "workflow_run_dispatch"."acknowledged_generation" >= 0),
	CONSTRAINT "workflow_run_dispatch_acknowledged_state_check" CHECK ("workflow_run_dispatch"."status" <> 'acknowledged' or "workflow_run_dispatch"."acknowledged_generation" is not null),
	CONSTRAINT "workflow_run_dispatch_pending_schedule_check" CHECK ("workflow_run_dispatch"."status" <> 'pending' or "workflow_run_dispatch"."next_attempt_at" is not null)
);
--> statement-breakpoint
ALTER TABLE "workflow_run_dispatch" ADD CONSTRAINT "workflow_run_dispatch_anchor_run_id_workflow_run_id_fk" FOREIGN KEY ("anchor_run_id") REFERENCES "public"."workflow_run"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "workflow_run_dispatch_anchor_message_idx" ON "workflow_run_dispatch" USING btree ("anchor_run_id","message_id");--> statement-breakpoint
CREATE INDEX "workflow_run_dispatch_delivery_idx" ON "workflow_run_dispatch" USING btree ("next_attempt_at","created_at") WHERE "workflow_run_dispatch"."status" = 'pending';--> statement-breakpoint
CREATE INDEX "workflow_run_dispatch_anchor_status_idx" ON "workflow_run_dispatch" USING btree ("anchor_run_id","status");