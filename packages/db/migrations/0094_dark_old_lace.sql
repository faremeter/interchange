-- Constrain the tenant slug to a single DNS label. The create route derives
-- tenant.domain from the lowercased slug, and that domain is both the sender
-- stamp on outbound mail and the recipient run address, so a slug that is not a
-- legal label gives that route a corrupt admission policy key and routing key.
-- The derivation is the route's own convention, not a relation between the
-- columns: this constrains what the route derives a domain from, and leaves
-- tenant.domain itself under no grammar constraint.
--
-- Guard first. A database holding a non-conforming slug fails this migration
-- either way, because the constraint below cannot be validated against such a
-- row -- but the bare constraint violation names no row, so the operator cannot
-- tell which tenant broke the deploy. The guard changes only the quality of
-- that failure. The production path applies migrations under `drizzle-kit
-- migrate`, which wraps the pending set in a transaction, and PostgreSQL DDL is
-- transactional, so the abort rolls the whole set back.
--
-- The guard locks the table before it counts, and its predicate is the
-- constraint predicate below under NOT and nothing else. Predicate equality
-- alone does not stop a row from clearing the guard and then failing the
-- constraint -- which would leave the operator with the bare violation after
-- being told the table was clean. The guard's SELECT and the constraint's
-- validation scan are separate statements, so under READ COMMITTED each takes
-- its own snapshot, and a row committed between the two is invisible to the
-- first and visible to the second. Whether ACCESS EXCLUSIVE closes that window
-- is the caller's to decide, not this file's, because the lock lasts only as
-- long as the transaction holding it. Under a caller that wraps the pending set
-- in one transaction -- `drizzle-kit migrate`, the production path -- the lock
-- is held across both statements, so a writer either commits before it is
-- granted, and is therefore counted, or waits until the migration ends. Under a
-- caller that executes each statement in autocommit, the lock is released when
-- this DO block commits and the ALTER TABLE re-acquires it, so the window
-- between them stays open and the guard narrows it rather than closing it. The
-- ALTER TABLE below takes that same mode, so the lock is acquired earlier rather
-- than newly introduced, and the added hold is the guard's own scan of a table
-- holding one row per tenant. Taking it up front also avoids upgrading ACCESS
-- SHARE to ACCESS EXCLUSIVE mid-transaction, which deadlocks against a
-- conflicting waiter that queues between the two.
--
-- The column is NOT NULL, so the predicate is never null; were it ever null the
-- two would still agree, because a check admits a row whose predicate is null
-- and a WHERE does not select one.
--
-- The list is capped. Nothing bounds how many rows a column that accepted any
-- string can have accumulated, and an unbounded message risks a log pipeline
-- truncating away the instructions, which are the part the operator needs, so
-- the instructions come first and the sample last. The total states whether the
-- sample is the whole set, and ordering by slug makes the sample stable, so
-- fixing the listed rows and re-running walks the set rather than resampling it.
--
-- Each value is rendered as a JSON string rather than a SQL literal. The column
-- accepted any string, so a slug can hold a newline or a tab, and a SQL literal
-- reproduces those verbatim: the message would break across lines, which a log
-- pipeline reads as separate entries and may keep only the first of. JSON
-- escapes every control character, so the message stays one line and an
-- otherwise invisible slug is still legible. The operator keys the fix on the
-- tenant id rather than on the slug, so nothing here needs to be a SQL literal.
DO $$
DECLARE
  offending_total bigint;
  offending_sample text;
BEGIN
  LOCK TABLE "tenant" IN ACCESS EXCLUSIVE MODE;

  SELECT
    count(*),
    string_agg(
      to_json(offending.slug)::text || ' (tenant ' || to_json(offending.id)::text || ')',
      ', ' ORDER BY offending.slug
    ) FILTER (WHERE offending.seq <= 25)
  INTO offending_total, offending_sample
  FROM (
    SELECT
      "tenant"."id" AS id,
      "tenant"."slug" AS slug,
      row_number() OVER (ORDER BY "tenant"."slug") AS seq
    FROM "tenant"
    WHERE NOT ("tenant"."slug" ~ '^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$' and length("tenant"."slug") <= 63)
  ) AS offending;

  IF offending_total > 0 THEN
    RAISE EXCEPTION '% tenant row(s) hold a slug that is not a single DNS label, so tenant_slug_dns_label_check cannot be added. Rename each slug to a single label -- a letter or a digit first and last, letters, digits and hyphens between, at most 63 characters, per RFC 1035 section 2.3.1 as relaxed by RFC 1123 section 2.1 -- then re-run the migration. Renaming a slug does not move the mail identity of a tenant: that is tenant.domain, a separate column this migration neither reads nor changes, and it is what carries the sender stamp on outbound mail and the recipient run address. The create route derives the domain of a new tenant as the lowercased slug followed by .localhost, so read each row before touching its domain: a domain still derived that way from the old slug wants moving too, and moving one that was not breaks whatever addresses it. Offending rows, lowest slug first, at most 25 shown -- fix these and re-run to see the next: %', offending_total, offending_sample;
  END IF;
END $$;
--> statement-breakpoint
ALTER TABLE "tenant" ADD CONSTRAINT "tenant_slug_dns_label_check" CHECK ("tenant"."slug" ~ '^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$' and length("tenant"."slug") <= 63);
