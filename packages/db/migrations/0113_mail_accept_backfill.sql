-- Keep the deployments and runs that exist before the inbound mail sender
-- gate reachable. The gate refuses hub-relayed mail to a run unless a
-- `mail.accept` rule admits the sender, and state that predates the gate has
-- no such rules, so without this backfill every existing deployment and run
-- stops receiving mail.
--
-- A deployment is classified by the grant snapshot in its launch spec, which
-- is the snapshot the hub reads at runtime. A run is classified by the launch
-- spec of its deployment. The shared definition version row is not read: a
-- later approval can rewrite it, so it does not describe a deployment.
--
-- A mail-trigger deployment has a `mail.address:` grant on a step of its
-- snapshot. A sending deployment has no mail trigger but grants
-- `tool:mail_send` on a step, so its runs send mail and wait for replies. A
-- mail-tool deployment grants any of the mail tools (`tool:mail_send`,
-- `tool:mail_reply`, `tool:mail_wait`, `tool:mail_read`, `tool:mail_search`,
-- `tool:mail_flag`, `tool:mail_expunge`), so its runs can read mail that
-- reaches their address. A deployment with no mail trigger and no mail tool
-- reads no mail before the gate either, so it and its runs stay unchanged.
--
--   1. Each live deployment anchor gets a `legacy_mail_accept` rule. A
--      mail-trigger deployment gets `invoker-correspondent`: the first mail
--      to a deployment that has not fired is admitted, its sender becomes the
--      invoker, and a run gets a correspondent grant for each party it mails
--      from now on. A deployment that already fired keeps its run, so the
--      invoker half admits no new sender there. A sending deployment gets
--      `correspondent`: the reply to each party its runs mail from now on is
--      admitted.
--   2. Each live run (status `deployed` or `running`) of a mail-trigger or
--      mail-tool deployment that has a committed run principal gets one
--      `mail.accept:tenant:<run tenant>` allow row. The invoker of an existing
--      run and the parties it mailed have no durable record, so the run's own
--      tenant is the closest rule that the data can express. Mail from a
--      sender in another tenant is refused.
--      A mail-tool deployment with no mail trigger and no `tool:mail_send`
--      gets no anchor rule, so a run it starts from now on is the same as a
--      run of a new deployment.
--
-- The rule is on the deployment's anchor run, not in its launch-spec
-- snapshot. The snapshot is the grant shape the operator approved for the
-- deployment, and this migration does not change approved content. A
-- deployment made after this migration has no legacy rule.
--
-- The row matches the shape of the launch-time accept rows: action `accept`,
-- effect `allow`, origin `creator`, no conditions and no expiry.
--
-- The data statements are idempotent: an anchor that already holds a rule is
-- left as it is, and the `WHERE NOT EXISTS` guard skips a run principal that
-- already holds the row. Grant ids follow the app convention (`grt_` and 32
-- hex characters).
ALTER TABLE "workflow_run" ADD COLUMN "legacy_mail_accept" text;--> statement-breakpoint
ALTER TABLE "workflow_run" ADD CONSTRAINT "workflow_run_legacy_mail_accept_check" CHECK ("workflow_run"."legacy_mail_accept" is null or "workflow_run"."legacy_mail_accept" in ('invoker-correspondent', 'correspondent'));--> statement-breakpoint
UPDATE "workflow_run" r
SET "legacy_mail_accept" = 'invoker-correspondent'
WHERE r."id" = r."anchor_run_id"
  AND r."status" IN ('deployed', 'running')
  AND r."legacy_mail_accept" IS NULL
  AND EXISTS (
    SELECT 1
    FROM "workflow_run_launch_spec" spec,
      jsonb_array_elements(spec."grant_snapshot" -> 'perStep') AS s(step),
      jsonb_array_elements_text(s.step -> 'grants') AS g(value)
    WHERE spec."anchor_run_id" = r."id"
      AND spec."grant_snapshot" IS NOT NULL
      AND g.value LIKE 'mail.address:%'
  );
--> statement-breakpoint
UPDATE "workflow_run" r
SET "legacy_mail_accept" = 'correspondent'
WHERE r."id" = r."anchor_run_id"
  AND r."status" IN ('deployed', 'running')
  AND r."legacy_mail_accept" IS NULL
  AND EXISTS (
    SELECT 1
    FROM "workflow_run_launch_spec" spec,
      jsonb_array_elements(spec."grant_snapshot" -> 'perStep') AS s(step),
      jsonb_array_elements_text(s.step -> 'grants') AS g(value)
    WHERE spec."anchor_run_id" = r."id"
      AND spec."grant_snapshot" IS NOT NULL
      AND g.value = 'tool:mail_send'
  );
--> statement-breakpoint
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
  r."tenant_id",
  p."id",
  'mail.accept:tenant:' || r."tenant_id",
  'accept',
  'allow',
  'creator',
  NULL
FROM "workflow_run" r
JOIN "principal" p
  ON p."tenant_id" = r."tenant_id"
  AND p."kind" = 'workflow'
  AND p."ref_id" = r."id"
WHERE r."status" IN ('deployed', 'running')
  AND EXISTS (
    SELECT 1
    FROM "workflow_run_launch_spec" spec,
      jsonb_array_elements(spec."grant_snapshot" -> 'perStep') AS s(step),
      jsonb_array_elements_text(s.step -> 'grants') AS g(value)
    WHERE spec."anchor_run_id" = r."anchor_run_id"
      AND spec."grant_snapshot" IS NOT NULL
      AND (
        g.value LIKE 'mail.address:%'
        OR g.value IN (
          'tool:mail_send', 'tool:mail_reply', 'tool:mail_wait', 'tool:mail_read',
          'tool:mail_search', 'tool:mail_flag', 'tool:mail_expunge'
        )
      )
  )
  AND NOT EXISTS (
    SELECT 1
    FROM "grant" existing
    WHERE existing."principal_id" = p."id"
      AND existing."resource" = 'mail.accept:tenant:' || r."tenant_id"
      AND existing."action" = 'accept'
  );
