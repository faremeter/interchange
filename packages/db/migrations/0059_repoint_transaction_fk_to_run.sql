-- Re-point the transaction ledger off the agent onto the run that incurs a
-- charge. The table is unwritten (no producer), so there is no data to migrate
-- and no contract to preserve: rename agent_id to run_id and reference
-- workflow_run, keeping the nullable set-null delete so ledger history survives
-- the deletion of the run it references.
ALTER TABLE "transaction" RENAME COLUMN "agent_id" TO "run_id";
--> statement-breakpoint
ALTER TABLE "transaction" DROP CONSTRAINT "transaction_agent_id_agent_id_fk";
--> statement-breakpoint
ALTER TABLE "transaction" ADD CONSTRAINT "transaction_run_id_workflow_run_id_fk" FOREIGN KEY ("run_id") REFERENCES "public"."workflow_run"("id") ON DELETE set null ON UPDATE no action;
