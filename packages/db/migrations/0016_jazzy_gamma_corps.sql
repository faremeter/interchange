ALTER TABLE "agent" ADD COLUMN "creator_principal_id" text;--> statement-breakpoint
ALTER TABLE "agent" ADD COLUMN "grant_requirements" jsonb;--> statement-breakpoint
ALTER TABLE "agent" ADD CONSTRAINT "agent_creator_principal_id_principal_id_fk" FOREIGN KEY ("creator_principal_id") REFERENCES "public"."principal"("id") ON DELETE no action ON UPDATE no action;
