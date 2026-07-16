-- Backfill `credential:{id}` / `use` grants for the owner of every existing
-- personal credential (rows where `principal_id` is set) that does not already
-- have one. This keeps existing personal-credential owners working once
-- launch enforcement fails closed. Organizational credentials
-- (`principal_id` IS NULL) are intentionally excluded: they remain gated
-- behind tenant-owner role inheritance and explicit administrative grants.
--
-- Idempotent: the `WHERE NOT EXISTS` guard skips any credential whose owner
-- already holds a matching grant, so re-application inserts nothing and does
-- not fail. Grant ids reproduce the app convention (`grt_` prefix followed by
-- 32 hex characters) via `gen_random_uuid()`.
INSERT INTO "grant" (
  "id",
  "tenant_id",
  "principal_id",
  "resource",
  "action",
  "effect",
  "origin",
  "expires_at"
)
SELECT
  'grt_' || replace(gen_random_uuid()::text, '-', ''),
  c."tenant_id",
  c."principal_id",
  'credential:' || c."id",
  'use',
  'allow',
  'creator',
  NULL
FROM "credential" c
WHERE c."principal_id" IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM "grant" g
    WHERE g."principal_id" = c."principal_id"
      AND g."resource" = 'credential:' || c."id"
      AND g."action" = 'use'
  );
