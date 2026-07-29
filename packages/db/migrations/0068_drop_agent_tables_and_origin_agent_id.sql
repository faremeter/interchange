-- The fold onto workflow_definition/workflow_run is complete: nothing reads
-- the legacy agent tables or workflow_definition.origin_agent_id anymore. Drop
-- them. Child-first order (no CASCADE) so a bare DROP fails loudly if an
-- unmodelled dependent survives; IF EXISTS on every statement makes the file
-- idempotent, since the runner is non-transactional and re-runs from the top
-- after a partial failure.
DROP TABLE IF EXISTS "agent_instance";--> statement-breakpoint
DROP TABLE IF EXISTS "agent_version";--> statement-breakpoint
DROP TABLE IF EXISTS "agent";--> statement-breakpoint
DROP INDEX IF EXISTS "workflow_definition_origin_agent_idx";--> statement-breakpoint
ALTER TABLE "workflow_definition" DROP COLUMN IF EXISTS "origin_agent_id";
