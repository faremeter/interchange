ALTER TABLE "workflow_deployment" ADD COLUMN "address" text NOT NULL;--> statement-breakpoint
ALTER TABLE "workflow_deployment" ADD COLUMN "public_key" text;--> statement-breakpoint
CREATE UNIQUE INDEX "workflow_deployment_address_idx" ON "workflow_deployment" USING btree ("address");