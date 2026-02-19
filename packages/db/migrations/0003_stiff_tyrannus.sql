CREATE TABLE "provider" (
	"id" text PRIMARY KEY NOT NULL,
	"tenant_id" text NOT NULL,
	"name" text NOT NULL,
	"plugin" text NOT NULL,
	"authorization_url" text,
	"token_url" text,
	"user_info_url" text,
	"scopes" text[],
	"metadata" jsonb,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "provider_tenant_name" UNIQUE("tenant_id","name")
);
--> statement-breakpoint
CREATE TABLE "oauth_client" (
	"id" text PRIMARY KEY NOT NULL,
	"tenant_id" text NOT NULL,
	"provider_id" text NOT NULL,
	"name" text NOT NULL,
	"client_id" text NOT NULL,
	"client_secret" text NOT NULL,
	"redirect_uris" text[],
	"default_scopes" text[],
	"metadata" jsonb,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "oauth_client_tenant_provider" UNIQUE("tenant_id","provider_id")
);
--> statement-breakpoint
ALTER TABLE "credential" ADD COLUMN "principal_id" text;--> statement-breakpoint
ALTER TABLE "credential" ADD COLUMN "provider_id" text NOT NULL;--> statement-breakpoint
ALTER TABLE "credential" ADD COLUMN "oauth_client_id" text;--> statement-breakpoint
ALTER TABLE "credential" ADD COLUMN "refresh_secret" text;--> statement-breakpoint
ALTER TABLE "credential" ADD COLUMN "scopes" text[];--> statement-breakpoint
ALTER TABLE "credential" ADD COLUMN "expires_at" timestamp;--> statement-breakpoint
ALTER TABLE "credential" ADD COLUMN "status" text DEFAULT 'active' NOT NULL;--> statement-breakpoint
ALTER TABLE "provider" ADD CONSTRAINT "provider_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "oauth_client" ADD CONSTRAINT "oauth_client_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "oauth_client" ADD CONSTRAINT "oauth_client_provider_id_provider_id_fk" FOREIGN KEY ("provider_id") REFERENCES "public"."provider"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "credential" ADD CONSTRAINT "credential_principal_id_principal_id_fk" FOREIGN KEY ("principal_id") REFERENCES "public"."principal"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "credential" ADD CONSTRAINT "credential_provider_id_provider_id_fk" FOREIGN KEY ("provider_id") REFERENCES "public"."provider"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "credential" ADD CONSTRAINT "credential_oauth_client_id_oauth_client_id_fk" FOREIGN KEY ("oauth_client_id") REFERENCES "public"."oauth_client"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "credential" ADD CONSTRAINT "credential_tenant_name" UNIQUE("tenant_id","name");