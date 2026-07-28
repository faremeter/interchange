-- Classify each definition as a single-instance launch target (`instance`) or a
-- multi-step workflow deploy target (`workflow`), so the launch route can gate
-- on `kind` instead of `origin_agent_id` -- letting that column be dropped
-- later. Every folded (agent-origin) definition is an instance; a native
-- workflow-origin definition is a workflow. ADD COLUMN with a `workflow`
-- default sets every existing row to `workflow`; the backfill then flips the
-- folded rows, identified by a non-null origin_agent_id, to `instance`.
ALTER TABLE "workflow_definition" ADD COLUMN "kind" text DEFAULT 'workflow' NOT NULL;
--> statement-breakpoint
UPDATE "workflow_definition" SET "kind" = 'instance' WHERE "origin_agent_id" IS NOT NULL;
