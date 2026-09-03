-- Discard alpha probe state instead of backfilling placement identity.
DELETE FROM "workflow_probe";--> statement-breakpoint
CREATE TABLE "sidecar_operation" (
	"id" text PRIMARY KEY NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
INSERT INTO "sidecar_operation" ("id", "created_at")
SELECT "id", "created_at" FROM "sidecar_allocation";--> statement-breakpoint
ALTER TABLE "execution_host_assignment" RENAME COLUMN "allocation_id" TO "operation_id";--> statement-breakpoint
ALTER TABLE "execution_host_assignment" DROP CONSTRAINT "execution_host_assignment_allocation_id_sidecar_allocation_id_fk";
--> statement-breakpoint
DROP INDEX "execution_host_assignment_active_allocation_idx";--> statement-breakpoint
DROP INDEX "execution_host_assignment_allocation_idx";--> statement-breakpoint
ALTER TABLE "workflow_probe" ADD COLUMN "placement_principal_id" text NOT NULL;--> statement-breakpoint
ALTER TABLE "workflow_probe" ADD COLUMN "placement_policy" jsonb NOT NULL;--> statement-breakpoint
ALTER TABLE "execution_host_assignment" ADD COLUMN "capabilities" jsonb;--> statement-breakpoint
UPDATE "execution_host_assignment"
SET "capabilities" = COALESCE("execution_host_session"."capabilities", '[]'::jsonb)
FROM "execution_host_session"
WHERE "execution_host_session"."host_id" = "execution_host_assignment"."host_id";--> statement-breakpoint
UPDATE "execution_host_assignment"
SET "capabilities" = '[]'::jsonb
WHERE "capabilities" IS NULL;--> statement-breakpoint
ALTER TABLE "execution_host_assignment" ALTER COLUMN "capabilities" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "sidecar_allocation" ADD CONSTRAINT "sidecar_allocation_id_sidecar_operation_id_fk" FOREIGN KEY ("id") REFERENCES "public"."sidecar_operation"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "workflow_probe" ADD CONSTRAINT "workflow_probe_id_sidecar_operation_id_fk" FOREIGN KEY ("id") REFERENCES "public"."sidecar_operation"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "workflow_probe" ADD CONSTRAINT "workflow_probe_placement_principal_id_principal_id_fk" FOREIGN KEY ("placement_principal_id") REFERENCES "public"."principal"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "execution_host_assignment" ADD CONSTRAINT "execution_host_assignment_operation_id_sidecar_operation_id_fk" FOREIGN KEY ("operation_id") REFERENCES "public"."sidecar_operation"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "execution_host_assignment_active_operation_idx" ON "execution_host_assignment" USING btree ("operation_id") WHERE "execution_host_assignment"."status" in ('claiming', 'assigned', 'releasing');--> statement-breakpoint
CREATE INDEX "execution_host_assignment_operation_idx" ON "execution_host_assignment" USING btree ("operation_id");
