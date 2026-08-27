ALTER TABLE "sidecar_allocation" DROP CONSTRAINT "sidecar_allocation_placement_check";--> statement-breakpoint
ALTER TABLE "sidecar_allocation" DROP COLUMN "placement_sharing";--> statement-breakpoint
ALTER TABLE "sidecar_allocation" DROP COLUMN "sidecar_reuse";