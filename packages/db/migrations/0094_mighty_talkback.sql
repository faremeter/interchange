CREATE TABLE "execution_host" (
	"id" text PRIMARY KEY NOT NULL,
	"tenant_id" text NOT NULL,
	"principal_id" text NOT NULL,
	"owner_principal_id" text NOT NULL,
	"display_name" text NOT NULL,
	"token_hash_sha256" "bytea" NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "execution_host_token_hash_sha256_unique" UNIQUE("token_hash_sha256"),
	CONSTRAINT "execution_host_principal_id_unique" UNIQUE("principal_id")
);
--> statement-breakpoint
ALTER TABLE "execution_host" ADD CONSTRAINT "execution_host_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "execution_host" ADD CONSTRAINT "execution_host_principal_id_principal_id_fk" FOREIGN KEY ("principal_id") REFERENCES "public"."principal"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "execution_host" ADD CONSTRAINT "execution_host_owner_principal_id_principal_id_fk" FOREIGN KEY ("owner_principal_id") REFERENCES "public"."principal"("id") ON DELETE restrict ON UPDATE no action;
