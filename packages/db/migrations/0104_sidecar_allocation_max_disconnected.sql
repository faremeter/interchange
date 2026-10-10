ALTER TABLE "sidecar_allocation" ADD COLUMN "max_disconnected_ms" bigint;--> statement-breakpoint
-- Allocations created before deployments had a disconnect limit take the
-- platform default of 15 minutes. On a fresh database this affects zero rows.
UPDATE "sidecar_allocation" SET "max_disconnected_ms" = 900000;--> statement-breakpoint
ALTER TABLE "sidecar_allocation" ALTER COLUMN "max_disconnected_ms" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "sidecar_allocation" ADD CONSTRAINT "sidecar_allocation_max_disconnected_check" CHECK ("sidecar_allocation"."max_disconnected_ms" > 0);
