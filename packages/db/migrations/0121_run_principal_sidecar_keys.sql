-- Put each deployment's sidecar key on its run principal. The sidecar running
-- a deployment mints the deployment's key and holds the private half; the hub
-- records the public half on the deployment anchor, and from now on also as
-- the active key of the anchor's run principal, with no private key.
--
-- A run principal that fired before run principals were created at deploy got
-- a hub-minted key at its first trigger. Nothing signs with that key, and it
-- would block recording the sidecar key, so it is deleted. A definition-level
-- workflow principal, whose `ref_id` names a definition rather than a run,
-- keeps its key.
--
-- The copy refuses data it cannot copy faithfully rather than skipping it:
-- a key on a run row that is not a deployment anchor, a key that is not 32
-- bytes of lowercase hex, two anchors with one key, a key on an anchor with no
-- run principal, or a key another principal already holds. A terminal
-- anchor's key is copied too, so its mail still verifies.
--
-- The checks, the copy and the final check run in one statement under a lock
-- on the tables they read, so another writer cannot record, clear or mint
-- a key between them.
DO $$
BEGIN
  LOCK TABLE "workflow_run", "principal", "principal_key" IN SHARE ROW EXCLUSIVE MODE;
  IF EXISTS (
    SELECT 1 FROM "workflow_run"
    WHERE "public_key" IS NOT NULL AND "anchor_run_id" IS DISTINCT FROM "id"
  ) THEN
    RAISE EXCEPTION 'a workflow_run that is not a deployment anchor holds a public key';
  END IF;
  IF EXISTS (
    SELECT 1 FROM "workflow_run"
    WHERE "public_key" IS NOT NULL AND "public_key" !~ '^[0-9a-f]{64}$'
  ) THEN
    RAISE EXCEPTION 'a workflow_run public key is not 32 bytes of lowercase hex';
  END IF;
  IF EXISTS (
    SELECT "public_key" FROM "workflow_run"
    WHERE "public_key" IS NOT NULL
    GROUP BY "public_key" HAVING count(*) > 1
  ) THEN
    RAISE EXCEPTION 'two workflow_run rows hold the same public key';
  END IF;
  IF EXISTS (
    SELECT 1 FROM "workflow_run" r
    WHERE r."public_key" IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM "principal" p
      WHERE p."tenant_id" = r."tenant_id" AND p."kind" = 'workflow' AND p."ref_id" = r."id"
    )
  ) THEN
    RAISE EXCEPTION 'a deployment anchor with a public key has no run principal';
  END IF;
  IF EXISTS (
    SELECT 1 FROM "workflow_run" r
    JOIN "principal_key" k ON k."public_key" = r."public_key"
    JOIN "principal" p ON p."id" = k."principal_id"
    WHERE NOT (p."tenant_id" = r."tenant_id" AND p."kind" = 'workflow' AND p."ref_id" = r."id")
  ) THEN
    RAISE EXCEPTION 'a deployment public key is already held by another principal';
  END IF;

  DELETE FROM "principal_key" k
  USING "principal" p, "workflow_run" r
  WHERE k."principal_id" = p."id"
    AND p."kind" = 'workflow'
    AND p."tenant_id" = r."tenant_id"
    AND p."ref_id" = r."id"
    AND k."private_key" IS NOT NULL;

  INSERT INTO "principal_key" ("id", "principal_id", "public_key", "private_key", "status", "created_at", "updated_at")
  SELECT
    'pky_' || replace(gen_random_uuid()::text, '-', ''),
    p."id",
    r."public_key",
    NULL,
    'active',
    now(),
    now()
  FROM "workflow_run" r
  JOIN "principal" p
    ON p."tenant_id" = r."tenant_id" AND p."kind" = 'workflow' AND p."ref_id" = r."id"
  WHERE r."public_key" IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM "principal_key" k
      WHERE k."principal_id" = p."id" AND k."status" = 'active'
    );

  IF EXISTS (
    SELECT 1 FROM "workflow_run" r
    JOIN "principal" p
      ON p."tenant_id" = r."tenant_id" AND p."kind" = 'workflow' AND p."ref_id" = r."id"
    WHERE r."public_key" IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM "principal_key" k
      WHERE k."principal_id" = p."id" AND k."status" = 'active' AND k."public_key" = r."public_key"
    )
  ) THEN
    RAISE EXCEPTION 'a deployment public key was not copied to its run principal';
  END IF;
END $$;
