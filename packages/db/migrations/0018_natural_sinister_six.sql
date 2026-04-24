UPDATE "agent" SET "creator_principal_id" = "principal_id" WHERE "creator_principal_id" IS NULL AND "principal_id" IS NOT NULL;
--> statement-breakpoint
ALTER TABLE "agent" DROP CONSTRAINT "agent_principal_id_principal_id_fk";
--> statement-breakpoint
ALTER TABLE "agent" DROP COLUMN "principal_id";
