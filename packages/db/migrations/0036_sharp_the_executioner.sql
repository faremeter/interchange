ALTER TABLE "sidecar" ADD COLUMN "token_hash_sha256" "bytea" NOT NULL;--> statement-breakpoint
ALTER TABLE "sidecar" ADD CONSTRAINT "sidecar_token_hash_sha256_unique" UNIQUE("token_hash_sha256");