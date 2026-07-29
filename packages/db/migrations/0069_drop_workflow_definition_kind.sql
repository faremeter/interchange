-- The instance/workflow `kind` discriminator is redundant: a run already
-- classifies structurally (a plain routing address with a null deployment id is
-- an instance-shaped run; a deployment-anchored run carries its deployment id),
-- and the interactive-launch path gates on the presence of a model_requirements
-- manifest instead of kind. Drop the column. IF EXISTS keeps the file
-- idempotent under the non-transactional runner's re-run-from-top on partial
-- failure.
ALTER TABLE "workflow_definition" DROP COLUMN IF EXISTS "kind";