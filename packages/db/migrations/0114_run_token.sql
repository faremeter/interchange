CREATE TABLE "run_token" (
	"id" text PRIMARY KEY NOT NULL,
	"principal_id" text NOT NULL,
	"consumer" text NOT NULL,
	"token_hash_sha256" "bytea" NOT NULL,
	"sealed_token" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "run_token_token_hash_sha256_unique" UNIQUE("token_hash_sha256")
);
--> statement-breakpoint
ALTER TABLE "run_token" ADD CONSTRAINT "run_token_principal_id_principal_id_fk" FOREIGN KEY ("principal_id") REFERENCES "public"."principal"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "run_token_principal_consumer_idx" ON "run_token" USING btree ("principal_id","consumer");