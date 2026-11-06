-- Keep the mail that existing deployments and users can send. The send gate
-- refuses a send unless a `mail.send` grant of the sender allows it, and state
-- that predates the gate has no such grants.
--
--   1. Each deployment anchor whose first trigger linked its run principal
--      gets one `mail.send:*:*` allow row, the row the first trigger stages
--      for a new deployment: action `send`, origin `creator`, no conditions
--      and no expiry. An anchor of any status gets it, and so does a run
--      principal that is not active: the first trigger does not run again
--      for a linked anchor, and the send check does not read the sender's
--      status, so mail that an ended run sent is decided by this row. An
--      anchor that has not fired gets the row at its first trigger.
--      An operator row on `mail.send:*:*` with conditions or an expiry does
--      not count as that row.
--   2. Each holder of a live `allow` grant whose action can match `manage`
--      and whose resource can match a `workflow-run:` resource gets a
--      `mail.send:tenant:<grant tenant>` allow row. Such a holder can send
--      mail through the REST route to a run in that tenant today. Each new
--      row keeps the holder, origin, conditions and expiry of its source
--      rows; source rows that agree on all of these give one row. An action
--      can match when it is `manage`, or when it holds a `*` and matches
--      `manage` as a pattern. A resource can match when the text before
--      its first `*` starts with `workflow-run:`, or when it holds a `*` and
--      `workflow-run:` starts with that text (the rule the hub uses for
--      `credential:` grants). A `deny` or `ask` row is skipped, because the
--      route still checks `manage` and that check enforces it. A run
--      principal is skipped: statement 1 gives it its send grant, and a
--      delegated row on it would never lapse. A row whose conditions name a
--      key other than `time_window` is skipped too: the send check cannot
--      evaluate such a key and would fail on the copy. Such a key is `tool`,
--      which holds only for a run token's consumer, so a user's
--      `tool`-conditioned `manage` grant does not pass the route today.
--
-- Apply this migration with the old hub stopped, then start the new hub. An
-- anchor that the old hub links after this runs, or a tenant that it creates,
-- gets no send rows, and the new hub refuses its sends. Running this again
-- later is not a remedy: it would restore a row that an operator removed.
--
-- The data statements are idempotent: a `WHERE NOT EXISTS` guard skips a
-- holder that already holds the row it would get. Grant ids follow the app
-- convention (`grt_` and 32 hex characters).
INSERT INTO "grant" (
  "id", "tenant_id", "principal_id", "resource", "action", "effect",
  "origin", "expires_at"
)
SELECT
  'grt_' || replace(gen_random_uuid()::text, '-', ''),
  r."tenant_id",
  r."principal_id",
  'mail.send:*:*',
  'send',
  'allow',
  'creator',
  NULL
FROM "workflow_run" r
JOIN "principal" p
  ON p."id" = r."principal_id" AND p."tenant_id" = r."tenant_id"
WHERE r."id" = r."anchor_run_id"
  AND NOT EXISTS (
    SELECT 1 FROM "grant" existing
    WHERE existing."principal_id" = r."principal_id"
      AND existing."resource" = 'mail.send:*:*'
      AND existing."action" = 'send'
      AND existing."effect" = 'allow'
      AND (existing."conditions" IS NULL OR existing."conditions" = '{}'::jsonb)
      AND existing."expires_at" IS NULL
  );
--> statement-breakpoint
INSERT INTO "grant" (
  "id", "tenant_id", "principal_id", "role_id", "resource", "action",
  "effect", "conditions", "origin", "expires_at"
)
SELECT
  'grt_' || replace(gen_random_uuid()::text, '-', ''),
  source."tenant_id",
  source."principal_id",
  source."role_id",
  'mail.send:tenant:' || source."tenant_id",
  'send',
  'allow',
  source."conditions",
  source."origin",
  source."expires_at"
FROM (
  SELECT DISTINCT
    g."tenant_id", g."principal_id", g."role_id", g."conditions",
    g."origin", g."expires_at"
  FROM "grant" g
  LEFT JOIN "principal" holder ON holder."id" = g."principal_id"
  WHERE g."effect" = 'allow'
    AND (
      g."action" = 'manage'
      OR (
        strpos(g."action", '*') > 0
        AND 'manage' LIKE replace(
          replace(replace(replace(g."action", '\', '\\'), '%', '\%'), '_', '\_'),
          '*', '%'
        ) ESCAPE '\'
      )
    )
    AND (g."expires_at" IS NULL OR g."expires_at" > now())
    -- A CASE fixes the order: `-` raises on a jsonb scalar, so a row whose
    -- conditions are not an object must not reach it. A JSON null holds no
    -- condition; any other non-object shape is skipped.
    AND CASE
      WHEN g."conditions" IS NULL OR jsonb_typeof(g."conditions") = 'null'
        THEN true
      WHEN jsonb_typeof(g."conditions") = 'object'
        THEN g."conditions" - 'time_window' = '{}'::jsonb
      ELSE false
    END
    AND (g."role_id" IS NOT NULL OR holder."kind" <> 'workflow')
    AND (
      starts_with(split_part(g."resource", '*', 1), 'workflow-run:')
      OR (
        strpos(g."resource", '*') > 0
        AND starts_with('workflow-run:', split_part(g."resource", '*', 1))
      )
    )
) source
WHERE NOT EXISTS (
  SELECT 1 FROM "grant" existing
  WHERE existing."tenant_id" = source."tenant_id"
    AND existing."principal_id" IS NOT DISTINCT FROM source."principal_id"
    AND existing."role_id" IS NOT DISTINCT FROM source."role_id"
    AND existing."resource" = 'mail.send:tenant:' || source."tenant_id"
    AND existing."action" = 'send'
    AND existing."effect" = 'allow'
    AND existing."origin" = source."origin"
    AND existing."conditions" IS NOT DISTINCT FROM source."conditions"
    AND existing."expires_at" IS NOT DISTINCT FROM source."expires_at"
);
