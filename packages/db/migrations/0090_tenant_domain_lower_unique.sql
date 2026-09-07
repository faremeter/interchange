ALTER TABLE "tenant" DROP CONSTRAINT "tenant_domain_unique";--> statement-breakpoint
CREATE UNIQUE INDEX "tenant_domain_lower_idx" ON "tenant" USING btree (lower("domain"));