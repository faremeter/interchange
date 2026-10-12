ALTER TABLE "sidecar" ALTER COLUMN "last_heartbeat" SET DATA TYPE timestamp with time zone;--> statement-breakpoint
ALTER TABLE "sidecar" ALTER COLUMN "created_at" SET DATA TYPE timestamp with time zone;--> statement-breakpoint
ALTER TABLE "sidecar" ALTER COLUMN "created_at" SET DEFAULT now();--> statement-breakpoint
ALTER TABLE "sidecar" ALTER COLUMN "updated_at" SET DATA TYPE timestamp with time zone;--> statement-breakpoint
ALTER TABLE "sidecar" ALTER COLUMN "updated_at" SET DEFAULT now();