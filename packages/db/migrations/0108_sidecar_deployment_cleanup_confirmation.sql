ALTER TABLE "sidecar_allocation" ADD COLUMN "deployment_cleanup_confirmed" boolean DEFAULT false NOT NULL;
--> statement-breakpoint
-- Provisioner API version 2 reports whether destroy confirmed cleanup. The Hub
-- registers only version 2 provisioners, so rows bound under version 1 move to
-- the same binding's version 2 contract.
UPDATE "sidecar_allocation" SET "provisioner_api_version" = 2 WHERE "provisioner_api_version" = 1;
--> statement-breakpoint
UPDATE "workflow_probe" SET "provisioner_api_version" = 2 WHERE "provisioner_api_version" = 1;
