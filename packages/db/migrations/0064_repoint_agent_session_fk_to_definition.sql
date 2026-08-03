-- Re-point the agent_session foreign key off the agent onto the folded
-- workflow definition. The column keeps its agent_id name, but its values
-- become workflow_definition ids: a launched session is keyed to the folded
-- definition it runs, so it survives the agent table's retirement. The delete
-- rule also changes -- cascade (on agent, from 0008) becomes restrict: session
-- history is an audit record, so a definition with live sessions must not be
-- droppable out from under them.
--
-- The guard runs before any write. The production path applies migrations under
-- `drizzle-kit migrate`, which wraps the pending set in a transaction, and
-- PostgreSQL DDL is transactional, so an aborting guard rolls the whole set
-- back -- there is no partial commit to defend against.
-- Unlike the offering (0058) and agent_role (0063) re-points, it guards the
-- AGENT table, not agent_session: the launch handler writes a fresh
-- agent_session row at runtime for an agent that may have no existing sessions,
-- so a child-table guard would pass vacuously for a never-launched unfolded
-- agent and the writer would then explode on its undefined definition id.
-- Every agent folded implies every session's agent folded, covering both this
-- historical rewrite and the runtime writer.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM "agent" a
    WHERE NOT EXISTS (
      SELECT 1 FROM "workflow_definition" wd
      WHERE wd.origin_agent_id = a.id
    )
  ) THEN
    RAISE EXCEPTION 'agent(s) have no folded definition; run the agent fold (bin/db-backfill) before applying this migration';
  END IF;
END $$;
--> statement-breakpoint
-- Drop the old agent foreign key BEFORE rewriting the values: the rewrite sets
-- agent_id to a workflow_definition id, which is not an agent id, so it would
-- violate the still-active agent FK. agent_session's agent FK was written as a
-- raw inline REFERENCES in 0008, so Postgres auto-named it
-- agent_session_agent_id_fkey -- not the drizzle-style
-- agent_session_agent_id_agent_id_fk the snapshot infers.
ALTER TABLE "agent_session" DROP CONSTRAINT "agent_session_agent_id_fkey";
--> statement-breakpoint
UPDATE "agent_session" s
SET agent_id = wd.id
FROM "workflow_definition" wd
WHERE wd.origin_agent_id = s.agent_id;
--> statement-breakpoint
ALTER TABLE "agent_session" ADD CONSTRAINT "agent_session_agent_id_workflow_definition_id_fk" FOREIGN KEY ("agent_id") REFERENCES "public"."workflow_definition"("id") ON DELETE restrict ON UPDATE no action;
