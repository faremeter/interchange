DROP INDEX "workflow_run_dispatch_anchor_message_idx";--> statement-breakpoint
ALTER TABLE "workflow_run_dispatch" ADD COLUMN "delivery_key" text;--> statement-breakpoint
-- A mail row's key is `deriveDeliveryKey(sender_address, message_id)`:
-- 'mail-' and the hex sha256 of the UTF-8 sender, a 0x00 byte and the UTF-8
-- Message-ID. A text column cannot hold U+0000, so the separator is
-- unambiguous. A signal row's key is its signal id. A mail row with no sender
-- is one 0091 failed in place; it keeps its bare Message-ID as its key, so a
-- later message with that Message-ID from a real sender is a new row and is
-- delivered.
UPDATE "workflow_run_dispatch"
SET "delivery_key" = CASE
	WHEN "kind" = 'mail' AND "sender_address" IS NOT NULL THEN
		'mail-' || encode(
			sha256(
				convert_to("sender_address", 'UTF8')
				|| decode('00', 'hex')
				|| convert_to("message_id", 'UTF8')
			),
			'hex'
		)
	ELSE "message_id"
END;--> statement-breakpoint
ALTER TABLE "workflow_run_dispatch" ALTER COLUMN "delivery_key" SET NOT NULL;--> statement-breakpoint
CREATE UNIQUE INDEX "workflow_run_dispatch_anchor_delivery_key_idx" ON "workflow_run_dispatch" USING btree ("anchor_run_id","delivery_key");
