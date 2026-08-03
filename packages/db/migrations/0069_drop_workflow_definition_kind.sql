-- The instance/workflow `kind` discriminator is redundant: a run already
-- classifies structurally (a plain routing address with a null deployment id is
-- an instance-shaped run; a deployment-anchored run carries its deployment id),
-- and the interactive-launch path gates on the presence of a model_requirements
-- manifest instead of kind. Drop the column. The production path applies
-- migrations under `drizzle-kit migrate`, which wraps the pending set in a
-- transaction, and PostgreSQL DDL is transactional, so an aborting migration
-- rolls back with no partial-commit re-run to defend against; the IF EXISTS
-- keeps the drop tolerant of an already-absent column regardless.
ALTER TABLE "workflow_definition" DROP COLUMN IF EXISTS "kind";