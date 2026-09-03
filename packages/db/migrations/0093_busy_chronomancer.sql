ALTER TABLE "sidecar_allocation" ADD COLUMN "placement_principal_id" text;--> statement-breakpoint
UPDATE "sidecar_allocation"
SET "placement_principal_id" = "workflow_run_launch_spec"."source_authority_principal_id"
FROM "workflow_run_launch_spec"
WHERE "workflow_run_launch_spec"."anchor_run_id" = "sidecar_allocation"."anchor_run_id";--> statement-breakpoint
ALTER TABLE "sidecar_allocation" ALTER COLUMN "placement_principal_id" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "sidecar_allocation" ADD CONSTRAINT "sidecar_allocation_placement_principal_id_principal_id_fk" FOREIGN KEY ("placement_principal_id") REFERENCES "public"."principal"("id") ON DELETE restrict ON UPDATE no action;
