CREATE TABLE "workflow_run" (
	"id" text PRIMARY KEY NOT NULL,
	"deployment_id" text NOT NULL,
	"tenant_id" text NOT NULL,
	"principal_id" text,
	"status" text DEFAULT 'running' NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"ended_at" timestamp
);
--> statement-breakpoint
ALTER TABLE "workflow_run" ADD CONSTRAINT "workflow_run_deployment_id_workflow_deployment_id_fk" FOREIGN KEY ("deployment_id") REFERENCES "public"."workflow_deployment"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD CONSTRAINT "workflow_run_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD CONSTRAINT "workflow_run_principal_id_principal_id_fk" FOREIGN KEY ("principal_id") REFERENCES "public"."principal"("id") ON DELETE set null ON UPDATE no action;