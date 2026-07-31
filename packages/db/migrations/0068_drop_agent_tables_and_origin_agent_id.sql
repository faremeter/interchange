-- The fold onto workflow_definition/workflow_run is complete: nothing reads
-- the legacy agent tables or workflow_definition.origin_agent_id anymore. Drop
-- them. Child-first order (no CASCADE) so a bare DROP fails loudly if an
-- unmodelled dependent survives; IF EXISTS on every statement makes the file
-- idempotent, since the runner is non-transactional and re-runs from the top
-- after a partial failure.
--
-- Guard first: agent_instance holds legacy routing, mail, and turn state that
-- the workflow-asset fold never converted into workflow_run (the fold projected
-- definitions from assets; it never migrated instances), and the fold tooling
-- has since been retired, so a bare DROP would destroy that state silently.
-- Abort while the table still holds rows so the loss is loud, not silent. The
-- to_regclass check keeps the file idempotent: on a re-run after the drop the
-- table is gone and the guard is skipped rather than erroring on a missing
-- relation.
DO $$
BEGIN
  IF to_regclass('agent_instance') IS NOT NULL THEN
    IF EXISTS (SELECT 1 FROM "agent_instance") THEN
      RAISE EXCEPTION 'agent_instance still holds rows: dropping the agent tables would destroy legacy agent routing, mail, and turn state that was never folded into workflow_run and has no automated conversion (the fold tooling has been retired). Retire the remaining agent instances by hand before applying this migration (see the agent-fold migration note in DEV.md).';
    END IF;
  END IF;
END $$;
--> statement-breakpoint
DROP TABLE IF EXISTS "agent_instance";--> statement-breakpoint
DROP TABLE IF EXISTS "agent_version";--> statement-breakpoint
DROP TABLE IF EXISTS "agent";--> statement-breakpoint
DROP INDEX IF EXISTS "workflow_definition_origin_agent_idx";--> statement-breakpoint
ALTER TABLE "workflow_definition" DROP COLUMN IF EXISTS "origin_agent_id";
