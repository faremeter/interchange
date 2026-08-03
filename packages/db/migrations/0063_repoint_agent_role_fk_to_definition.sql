-- Re-point the agent_role foreign key off the agent onto the folded workflow
-- definition. The column keeps its agent_id name, but its values become
-- workflow_definition ids: every agent was folded to exactly one definition,
-- linked by workflow_definition.origin_agent_id (a partial unique index, so
-- the join matches at most one definition). Role assignments follow the
-- definition so they survive the agent table's retirement.
--
-- The guard runs before any write. The production path applies migrations under
-- `drizzle-kit migrate`, which wraps the pending set in a transaction, and
-- PostgreSQL DDL is transactional, so an aborting guard rolls the whole set
-- back -- there is no partial commit to defend against. It aborts loudly if any
-- agent_role row references an agent with no folded definition, meaning the
-- agent fold has not run; the total fold makes that impossible in a folded
-- database, so the guard is a fail-loud assertion.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM "agent_role" ar
    WHERE NOT EXISTS (
      SELECT 1 FROM "workflow_definition" wd
      WHERE wd.origin_agent_id = ar.agent_id
    )
  ) THEN
    RAISE EXCEPTION 'agent_role row(s) reference an agent with no folded definition; run the agent fold (bin/db-backfill) before applying this migration';
  END IF;
END $$;
--> statement-breakpoint
-- Drop the old agent foreign key BEFORE rewriting the values: the rewrite sets
-- agent_id to a workflow_definition id, which is not an agent id, so it would
-- violate the still-active agent FK.
ALTER TABLE "agent_role" DROP CONSTRAINT "agent_role_agent_id_agent_id_fk";
--> statement-breakpoint
UPDATE "agent_role" ar
SET agent_id = wd.id
FROM "workflow_definition" wd
WHERE wd.origin_agent_id = ar.agent_id;
--> statement-breakpoint
ALTER TABLE "agent_role" ADD CONSTRAINT "agent_role_agent_id_workflow_definition_id_fk" FOREIGN KEY ("agent_id") REFERENCES "public"."workflow_definition"("id") ON DELETE cascade ON UPDATE no action;
