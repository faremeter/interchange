ALTER TABLE "workflow_run_dispatch" ADD COLUMN "sender_address" text;--> statement-breakpoint
-- Fail in-flight mail dispatches that predate the authenticated-sender
-- requirement. They carry no persisted hub-verified sender, and neither
-- fabricating one nor re-reading the signed From is permitted, so they are
-- failed in place rather than delivered unauthenticated. Only unsettled
-- (pending/acknowledged) mail rows are ever re-dispatched and reconstruct a
-- mail.inbound frame; terminal (settled/failed) mail rows never do, so they
-- keep their NULL sender under the status-scoped check below. On a fresh
-- database this affects zero rows.
UPDATE "workflow_run_dispatch"
SET "status" = 'failed',
	"failure_code" = 'sender_address_migration',
	"failure_message" = 'in-flight mail dispatch predates the authenticated-sender requirement and carries no hub-verified sender',
	"next_attempt_at" = NULL,
	"delivery_lease_id" = NULL,
	"delivery_lease_expires_at" = NULL,
	"updated_at" = now()
WHERE "kind" = 'mail'
	AND "status" IN ('pending', 'acknowledged')
	AND "sender_address" IS NULL;--> statement-breakpoint
ALTER TABLE "workflow_run_dispatch" ADD CONSTRAINT "workflow_run_dispatch_mail_sender_check" CHECK ("workflow_run_dispatch"."kind" <> 'mail' or "workflow_run_dispatch"."status" in ('settled', 'failed') or "workflow_run_dispatch"."sender_address" is not null);
