ALTER TABLE "sidecar_allocation" ADD COLUMN "adapter_manifest_digest" text NOT NULL DEFAULT 'unrecorded';--> statement-breakpoint
ALTER TABLE "sidecar_allocation" ALTER COLUMN "adapter_manifest_digest" DROP DEFAULT;--> statement-breakpoint
ALTER TABLE "workflow_probe" ADD COLUMN "adapter_manifest_digest" text NOT NULL DEFAULT 'unrecorded';--> statement-breakpoint
ALTER TABLE "workflow_probe" ALTER COLUMN "adapter_manifest_digest" DROP DEFAULT;
