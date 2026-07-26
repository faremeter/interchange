-- Drop the workflow_deployment projection. Its routing identity, reconnect key,
-- and status now live on the anchor run, its foreign keys were re-pointed onto
-- the anchor run, and nothing reads or writes it. This is irreversible.
--
-- Promote workflow_run.definition_id to NOT NULL: every run now carries a
-- definition (set at birth for new runs, filled by 0056 for the rows the
-- projection anchored). The guard below aborts before writing anything if any
-- run is still null -- that means the workflow-asset fold has not run, and
-- dropping the projection would strand those rows with no way to resolve their
-- definition. A blunt IS NULL check catches every class, including a run with a
-- null deployment_id the deployment join could never reach. Abort loudly so the
-- operator runs the fold and retries.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM "workflow_run" WHERE definition_id IS NULL) THEN
    RAISE EXCEPTION 'workflow_run rows without a definition remain; run the workflow-asset fold (bin/db-backfill) before applying this migration';
  END IF;
END $$;
--> statement-breakpoint
ALTER TABLE "workflow_run" ALTER COLUMN "definition_id" SET NOT NULL;
--> statement-breakpoint
DROP TABLE "workflow_deployment";
