CREATE TABLE "approval" (
	"id" text PRIMARY KEY NOT NULL,
	"tenant_id" text NOT NULL,
	"instance_id" text NOT NULL,
	"agent_id" text NOT NULL,
	"origin_principal_id" text NOT NULL,
	"correlation_id" text NOT NULL,
	"tool_definition" jsonb NOT NULL,
	"tool_arguments" jsonb NOT NULL,
	"scope" text,
	"status" text DEFAULT 'pending' NOT NULL,
	"origin_kind" text NOT NULL,
	"timeout_at" timestamp NOT NULL,
	"resolved_at" timestamp,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "approval_correlation_id_unique" UNIQUE("correlation_id")
);
--> statement-breakpoint
CREATE TABLE "signal_correlation" (
	"correlation_id" text PRIMARY KEY NOT NULL,
	"tenant_id" text NOT NULL,
	"deployment_id" text NOT NULL,
	"agent_address" text NOT NULL,
	"run_id" text NOT NULL,
	"signal_name" text NOT NULL,
	"kind" text NOT NULL,
	"signal_id" text,
	"resolved_at" timestamp,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "approval" ADD CONSTRAINT "approval_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "approval" ADD CONSTRAINT "approval_instance_id_agent_instance_id_fk" FOREIGN KEY ("instance_id") REFERENCES "public"."agent_instance"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "approval" ADD CONSTRAINT "approval_agent_id_agent_id_fk" FOREIGN KEY ("agent_id") REFERENCES "public"."agent"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "approval" ADD CONSTRAINT "approval_origin_principal_id_principal_id_fk" FOREIGN KEY ("origin_principal_id") REFERENCES "public"."principal"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "signal_correlation" ADD CONSTRAINT "signal_correlation_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE cascade ON UPDATE no action;