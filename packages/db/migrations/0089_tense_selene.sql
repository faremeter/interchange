CREATE TABLE "principal_key" (
	"id" text PRIMARY KEY NOT NULL,
	"principal_id" text NOT NULL,
	"public_key" text NOT NULL,
	"private_key" text NOT NULL,
	"status" text NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "principal_key_public_key_unique" UNIQUE("public_key")
);
--> statement-breakpoint
ALTER TABLE "principal_key" ADD CONSTRAINT "principal_key_principal_id_principal_id_fk" FOREIGN KEY ("principal_id") REFERENCES "public"."principal"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "principal_key_one_active" ON "principal_key" USING btree ("principal_id") WHERE "principal_key"."status" = 'active';