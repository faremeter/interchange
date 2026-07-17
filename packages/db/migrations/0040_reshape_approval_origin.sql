-- Reshape `approval` so its origin is the workflow deployment it came from.
-- The dropped instance/agent/origin-principal FKs had no valid referent: a
-- workflow deployment has no agent_instance/agent row and its run principal is
-- a substrate principal, not a `principal`-table row. The three added columns
-- are NOT NULL with no default and no backfill; like the repo's other such
-- adds (e.g. 0014, `workflow_deployment.address`), this relies on the table
-- being empty when the migration runs. The table is unreleased -- the approval
-- routes are 501 stubs -- so no populated row predates these columns.
ALTER TABLE "approval" DROP CONSTRAINT "approval_instance_id_agent_instance_id_fk";
--> statement-breakpoint
ALTER TABLE "approval" DROP CONSTRAINT "approval_agent_id_agent_id_fk";
--> statement-breakpoint
ALTER TABLE "approval" DROP CONSTRAINT "approval_origin_principal_id_principal_id_fk";
--> statement-breakpoint
ALTER TABLE "approval" ADD COLUMN "deployment_id" text NOT NULL;--> statement-breakpoint
ALTER TABLE "approval" ADD COLUMN "run_id" text NOT NULL;--> statement-breakpoint
ALTER TABLE "approval" ADD COLUMN "agent_address" text NOT NULL;--> statement-breakpoint
ALTER TABLE "approval" ADD CONSTRAINT "approval_deployment_id_workflow_deployment_id_fk" FOREIGN KEY ("deployment_id") REFERENCES "public"."workflow_deployment"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "approval_tenant_status_idx" ON "approval" USING btree ("tenant_id","status","created_at");--> statement-breakpoint
CREATE INDEX "approval_deployment_idx" ON "approval" USING btree ("deployment_id");--> statement-breakpoint
ALTER TABLE "approval" DROP COLUMN "instance_id";--> statement-breakpoint
ALTER TABLE "approval" DROP COLUMN "agent_id";--> statement-breakpoint
ALTER TABLE "approval" DROP COLUMN "origin_principal_id";--> statement-breakpoint
ALTER TABLE "approval" DROP COLUMN "origin_kind";