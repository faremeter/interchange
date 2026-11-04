-- Give every existing deployment its run principal. A deployment's run
-- principal is created at deploy now, and the hub expects one for each
-- deployment anchor. A deployment that predates that has one only when its
-- first trigger created it.
--
-- The principal is the `workflow` principal whose `ref_id` is the anchor run
-- id. Its id is the one `deriveRunPrincipalId` gives: `prn_` and the lowercase
-- hex of the first 16 bytes of SHA-256 over the UTF-8 tenant id, a zero byte,
-- and the UTF-8 run id. A trigger that fires the deployment later derives the
-- same id and links this principal.
--
-- It gets no key: a run principal holds no hub key. Its `created_at` is the
-- anchor's, since a run principal is created at deploy. The invoker-grant
-- time limit counts from it, so a deployment older than that limit that has
-- not fired gets no invoker `allow` rows at its first trigger, as a new
-- deployment would. A deployment that is no longer live gets a deactivated
-- principal, updated when the anchor ended, as a stop deactivates one. An
-- anchor that already has its principal keeps it unchanged.
--
-- A hub that predates run principals at deploy reads an existing run
-- principal as a committed first trigger, so it would start such a run with
-- no grants. Stop such hubs before this migration runs.
INSERT INTO "principal" ("id", "tenant_id", "kind", "ref_id", "status", "created_at", "updated_at")
SELECT
  'prn_' || encode(
    substring(
      sha256(convert_to(r."tenant_id", 'UTF8') || '\x00'::bytea || convert_to(r."id", 'UTF8'))
      from 1 for 16
    ),
    'hex'
  ),
  r."tenant_id",
  'workflow',
  r."id",
  CASE WHEN r."status" IN ('deployed', 'running') THEN 'active' ELSE 'deactivated' END,
  r."created_at",
  CASE WHEN r."status" IN ('deployed', 'running') THEN r."created_at" ELSE coalesce(r."ended_at", r."created_at") END
FROM "workflow_run" r
WHERE r."anchor_run_id" = r."id"
ON CONFLICT ("tenant_id", "kind", "ref_id") DO NOTHING;
