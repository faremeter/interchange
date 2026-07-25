-- Reconstruct an anchor workflow_run for every deployment that predates the
-- runtime anchor insert. A deployment's routing address and reconnect key now
-- live on its anchor run (the workflow_run whose id equals the deployment id),
-- which new deploys create at runtime -- but deployments created earlier have a
-- workflow_deployment row and no anchor run, so the reconnect key lookup finds
-- no run for their address and their reconnect challenge fails closed. This
-- copies the deployment's identity onto a fresh anchor run so those deployments
-- reconnect exactly as ones deployed after the runtime anchor insert do.
--
-- A runtime-created anchor run also carries its definition -- deploy resolves
-- and writes definition_id -- so a reconstructed one must too; an anchor run
-- without a definition is only half the deployment's first-class stand-in. A
-- definition_id resolves from the deployment's asset, which the workflow-asset
-- fold populates. The guard below aborts the migration before it writes
-- anything if any deployment still needing an anchor run has no folded
-- definition, so the backfill reconstructs every anchor run completely or
-- writes nothing and tells the operator to run the fold first. The migration
-- runner is not transactional, so the guard runs first rather than inserting
-- and rolling back.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM "workflow_deployment" wd
    WHERE NOT EXISTS (
        SELECT 1 FROM "workflow_run" r WHERE r.id = wd.id
      )
      AND NOT EXISTS (
        SELECT 1 FROM "workflow_definition" wdef
        WHERE wdef.asset_id = wd.definition_asset_id
      )
  ) THEN
    RAISE EXCEPTION 'workflow-asset fold has not run: a deployment lacks a folded definition and its anchor run would be definition-less; run bin/db-backfill before applying this migration';
  END IF;
END $$;
--> statement-breakpoint
-- public_key is copied from the deployment (its old deploy-ack key) so the
-- reconnect challenge resolves the same key it did before; created_at is copied
-- so listing order is stable; status is the anchor's live value; definition_id
-- resolves through the folded definition the guard above proved present. The
-- remaining columns match a runtime anchor insert: no principal, no runtime
-- bindings, not ended. NOT EXISTS on the id keeps it idempotent and skips
-- deployments that already have a runtime-created anchor run.
INSERT INTO "workflow_run" (
  id,
  tenant_id,
  deployment_id,
  definition_id,
  address,
  public_key,
  status,
  created_at
)
SELECT
  wd.id,
  wd.tenant_id,
  wd.id,
  wdef.id,
  wd.address,
  wd.public_key,
  'running',
  wd.created_at
FROM "workflow_deployment" wd
LEFT JOIN "workflow_definition" wdef ON wdef.asset_id = wd.definition_asset_id
WHERE NOT EXISTS (
  SELECT 1 FROM "workflow_run" r WHERE r.id = wd.id
);
