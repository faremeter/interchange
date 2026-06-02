CREATE TABLE "git_token" (
	"id" text PRIMARY KEY NOT NULL,
	"tenant_id" text,
	"user_id" text NOT NULL,
	"principal_id" text,
	"name" text NOT NULL,
	"kind" text NOT NULL,
	"token_hash_sha256" "bytea" NOT NULL,
	"resource" text NOT NULL,
	"ref_pattern" text NOT NULL,
	"actions" text[] NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"revoked_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "git_token_token_hash_sha256_unique" UNIQUE("token_hash_sha256")
);
--> statement-breakpoint
ALTER TABLE "git_token" ADD CONSTRAINT "git_token_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "git_token" ADD CONSTRAINT "git_token_user_id_user_id_fk" FOREIGN KEY ("user_id") REFERENCES "public"."user"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "git_token" ADD CONSTRAINT "git_token_principal_id_principal_id_fk" FOREIGN KEY ("principal_id") REFERENCES "public"."principal"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "git_token_user_id_name_active_idx" ON "git_token" USING btree ("user_id","name") WHERE "git_token"."revoked_at" is null;