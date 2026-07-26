-- Re-point the offering foreign key off the agent onto the folded workflow
-- definition. The column keeps its agent_id name -- the offering API exposes
-- agentId -- but its values become workflow_definition ids: every agent was
-- folded to exactly one definition, linked by workflow_definition.origin_agent_id
-- (a partial unique index, so the join matches at most one definition).
--
-- The guard runs before any write -- the migration runner is not transactional,
-- so a half-applied rewrite must be caught up front. It aborts loudly if any
-- offering's agent has no folded definition, meaning the agent fold has not
-- run; the total fold makes that impossible in a folded database, so the guard
-- is a fail-loud assertion.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM "offering" o
    WHERE NOT EXISTS (
      SELECT 1 FROM "workflow_definition" wd
      WHERE wd.origin_agent_id = o.agent_id
    )
  ) THEN
    RAISE EXCEPTION 'offering(s) reference an agent with no folded definition; run the agent fold (bin/db-backfill) before applying this migration';
  END IF;
END $$;
--> statement-breakpoint
-- Drop the old agent foreign key BEFORE rewriting the values: the rewrite sets
-- agent_id to a workflow_definition id, which is not an agent id, so it would
-- violate the still-active agent FK. The offering table was renamed from
-- `capability` (migration 0004); Postgres does not rename constraints on a
-- table rename, so its agent FK still carries the original `capability_`-
-- prefixed name, not the `offering_`-prefixed name drizzle's snapshot infers.
ALTER TABLE "offering" DROP CONSTRAINT "capability_agent_id_agent_id_fk";
--> statement-breakpoint
UPDATE "offering" o
SET agent_id = wd.id
FROM "workflow_definition" wd
WHERE wd.origin_agent_id = o.agent_id;
--> statement-breakpoint
ALTER TABLE "offering" ADD CONSTRAINT "offering_agent_id_workflow_definition_id_fk" FOREIGN KEY ("agent_id") REFERENCES "public"."workflow_definition"("id") ON DELETE restrict ON UPDATE no action;
