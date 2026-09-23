CREATE TABLE "workflow_pending_projection" (
	"id" text PRIMARY KEY NOT NULL,
	"anchor_run_id" text NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"history_required" boolean DEFAULT true NOT NULL
);
--> statement-breakpoint
ALTER TABLE "workflow_pending_projection" ADD CONSTRAINT "workflow_pending_projection_anchor_run_id_workflow_run_id_fk" FOREIGN KEY ("anchor_run_id") REFERENCES "public"."workflow_run"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "workflow_pending_projection_anchor_idx" ON "workflow_pending_projection" USING btree ("anchor_run_id","created_at");--> statement-breakpoint
-- Receives before this table existed left no record of a failed projection, so
-- every deployment that still has a live run is recorded as possibly owing one.
-- Such a deployment may never have pushed history, so its row does not require
-- the deployment's Git ref to exist.
INSERT INTO "workflow_pending_projection" ("id", "anchor_run_id", "history_required")
SELECT 'wpp_' || replace(gen_random_uuid()::text, '-', ''), "anchor"."id", false
FROM "workflow_run" AS "anchor"
WHERE "anchor"."id" = "anchor"."anchor_run_id"
  AND "anchor"."address" IS NOT NULL
  AND EXISTS (
    SELECT 1 FROM "workflow_run" AS "run"
    WHERE "run"."anchor_run_id" = "anchor"."id"
      AND "run"."status" IN ('deployed', 'running')
  );
