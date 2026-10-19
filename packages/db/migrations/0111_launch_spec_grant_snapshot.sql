ALTER TABLE "workflow_run_launch_spec" ADD COLUMN "grant_snapshot" jsonb;--> statement-breakpoint
UPDATE "workflow_run_launch_spec" AS spec
SET "grant_snapshot" = version."grant_snapshot"
FROM "workflow_run" AS run
JOIN "workflow_definition_version" AS version
  ON version."definition_id" = run."definition_id" AND version."version" = '1'
WHERE spec."anchor_run_id" = run."id";
