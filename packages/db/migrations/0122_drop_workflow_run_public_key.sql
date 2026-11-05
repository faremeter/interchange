-- Drop the deployment anchor's copy of its sidecar key. The run principal's
-- active key is the deployment's key, and the hub writes the two together, so
-- they agree. Refuse to drop a column that disagrees with its run principal
-- rather than lose the key it holds: a key the run principal does not hold
-- as its active key, or no key where the run principal holds an active one.
-- The check and the drop run in one statement under the lock the drop needs
-- on the run table. A writer of a run's key locks the deployment's anchor row
-- before it writes the key, so this lock also keeps another writer from
-- changing a key between the check and the drop. A hub transaction can still
-- deadlock with the migration's locks, here or in an earlier migration that
-- runs in the same transaction; the migration then rolls back whole and can
-- run again.
DO $$
BEGIN
  LOCK TABLE "workflow_run" IN ACCESS EXCLUSIVE MODE;
  IF EXISTS (
    SELECT 1 FROM "workflow_run" r
    WHERE r."public_key" IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM "principal" p
      JOIN "principal_key" k ON k."principal_id" = p."id" AND k."status" = 'active'
      WHERE p."tenant_id" = r."tenant_id" AND p."kind" = 'workflow' AND p."ref_id" = r."id"
        AND k."public_key" = r."public_key"
    )
  ) THEN
    RAISE EXCEPTION 'a deployment anchor holds a key its run principal does not hold as active';
  END IF;
  IF EXISTS (
    SELECT 1 FROM "workflow_run" r
    JOIN "principal" p
      ON p."tenant_id" = r."tenant_id" AND p."kind" = 'workflow' AND p."ref_id" = r."id"
    JOIN "principal_key" k ON k."principal_id" = p."id" AND k."status" = 'active'
    WHERE r."anchor_run_id" = r."id" AND r."public_key" IS NULL
  ) THEN
    RAISE EXCEPTION 'a deployment anchor without a key has a run principal with an active key';
  END IF;
  ALTER TABLE "workflow_run" DROP COLUMN "public_key";
END $$;
