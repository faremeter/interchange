ALTER TABLE "sidecar_allocation" ADD COLUMN "target_host_principal_id" text;--> statement-breakpoint
ALTER TABLE "sidecar_allocation" ADD COLUMN "placement_policy" jsonb;--> statement-breakpoint
UPDATE "sidecar_allocation"
SET "placement_policy" = jsonb_build_object(
	'tenantPolicies', '[]'::jsonb,
	'workflowRules', COALESCE(
		"workflow_run_launch_spec"."frozen_approval_bundle" #> '{projection,sidecarPlacement,capabilities}',
		'[]'::jsonb
	)
)
FROM "workflow_run_launch_spec"
WHERE "workflow_run_launch_spec"."anchor_run_id" = "sidecar_allocation"."anchor_run_id";--> statement-breakpoint
ALTER TABLE "sidecar_allocation" ALTER COLUMN "placement_policy" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "sidecar_allocation" ADD CONSTRAINT "sidecar_allocation_target_host_principal_id_principal_id_fk" FOREIGN KEY ("target_host_principal_id") REFERENCES "public"."principal"("id") ON DELETE restrict ON UPDATE no action;
