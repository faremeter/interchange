-- Remove `credential:{id}` grant rows that reference a credential that no longer
-- exists. Before credential DELETE cleaned up its per-credential grants, deleting
-- a personal credential left its `credential:{id}` grant behind, and migration
-- 0037 backfilled the same shape for pre-existing personal credentials, so a
-- credential hard-deleted before that cleanup shipped left an orphaned grant.
--
-- The coarse wildcard `credential:*` role resource is excluded explicitly: no
-- credential has the id `*`, so the NOT EXISTS guard alone would wrongly match
-- it. There is no tenant scope: credential ids are globally unique, so an
-- orphaned grant is one whose id matches no credential in any tenant. This
-- removes every action on an orphaned resource, matching the credential DELETE
-- handler.
--
-- Idempotent: after this runs no orphaned row remains, so a re-run is a no-op.
DELETE FROM "grant"
WHERE "resource" LIKE 'credential:%'
  AND "resource" <> 'credential:*'
  AND NOT EXISTS (
    SELECT 1
    FROM "credential" c
    WHERE c."id" = substr("grant"."resource", length('credential:') + 1)
  );
