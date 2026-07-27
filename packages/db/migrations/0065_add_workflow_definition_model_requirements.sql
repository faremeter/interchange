-- Mirror each folded agent's model requirements onto its definition, so a
-- folded launch resolves its inference sources from the definition rather than
-- the agent row. The column is folded-only: null for a workflow-origin
-- definition, which carries no requirements manifest. A null/empty manifest is
-- legitimate -- it resolves to an unlaunchable empty source chain, exactly as
-- the agent field did -- so the copy preserves null verbatim and the column
-- takes no NOT NULL constraint.
--
-- Guard first (the migration runner is not transactional): the backfill joins
-- each folded definition to its agent over origin_agent_id, a plain text column
-- rather than an FK. If any folded definition names an agent that does not
-- exist, the join silently skips it and its column stays null --
-- indistinguishable from a legitimately-null manifest. Fail loud before writing
-- so a broken back-reference surfaces here, not as a mysteriously unlaunchable
-- agent later.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM "workflow_definition" wd
    WHERE wd.origin_agent_id IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM "agent" a WHERE a.id = wd.origin_agent_id
      )
  ) THEN
    RAISE EXCEPTION 'folded definition(s) reference a missing agent; cannot backfill workflow_definition.model_requirements';
  END IF;
END $$;
--> statement-breakpoint
ALTER TABLE "workflow_definition" ADD COLUMN "model_requirements" jsonb;
--> statement-breakpoint
UPDATE "workflow_definition" wd
SET model_requirements = a.model_requirements
FROM "agent" a
WHERE wd.origin_agent_id = a.id;
