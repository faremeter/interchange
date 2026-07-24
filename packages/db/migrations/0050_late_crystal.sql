ALTER TABLE "workflow_run" ALTER COLUMN "deployment_id" DROP NOT NULL;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "definition_id" text;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "address" text;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "public_key" text;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "sidecar_id" text;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "kernel_id" text;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD COLUMN "model_preferences" jsonb;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD CONSTRAINT "workflow_run_definition_id_workflow_definition_id_fk" FOREIGN KEY ("definition_id") REFERENCES "public"."workflow_definition"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD CONSTRAINT "workflow_run_sidecar_id_sidecar_id_fk" FOREIGN KEY ("sidecar_id") REFERENCES "public"."sidecar"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "workflow_run_definition_idx" ON "workflow_run" USING btree ("definition_id");