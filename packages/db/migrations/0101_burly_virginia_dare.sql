ALTER TABLE "credential" DROP CONSTRAINT "credential_principal_id_principal_id_fk";
--> statement-breakpoint
ALTER TABLE "credential" ADD CONSTRAINT "credential_principal_id_principal_id_fk" FOREIGN KEY ("principal_id") REFERENCES "public"."principal"("id") ON DELETE restrict ON UPDATE no action;