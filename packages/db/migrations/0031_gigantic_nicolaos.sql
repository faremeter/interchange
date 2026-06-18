CREATE TABLE "model" (
	"id" text PRIMARY KEY NOT NULL,
	"tenant_id" text NOT NULL,
	"canonical_name" text NOT NULL,
	"display_name" text,
	"description" text,
	"disabled" boolean DEFAULT false NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "model_tenant_canonical_name" UNIQUE("tenant_id","canonical_name")
);
--> statement-breakpoint
CREATE TABLE "model_offering" (
	"id" text PRIMARY KEY NOT NULL,
	"tenant_id" text NOT NULL,
	"model_id" text NOT NULL,
	"provider_id" text NOT NULL,
	"priority" integer NOT NULL,
	"deployment_tags" text[] DEFAULT '{}' NOT NULL,
	"capabilities" text[] DEFAULT '{}' NOT NULL,
	"disabled" boolean DEFAULT false NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "model_offering_tenant_model_provider" UNIQUE("tenant_id","model_id","provider_id")
);
--> statement-breakpoint
CREATE TABLE "model_pricing" (
	"id" text PRIMARY KEY NOT NULL,
	"tenant_id" text NOT NULL,
	"offering_id" text NOT NULL,
	"currency" text NOT NULL,
	"input_token_price" text,
	"output_token_price" text,
	"cache_read_token_price" text,
	"cache_write_token_price" text,
	"thinking_token_price" text,
	"per_request_fee" text,
	"per_image_fee" text,
	"per_audio_fee" text,
	"effective_from" timestamp with time zone NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "model_pricing_offering_currency_effective_from" UNIQUE("offering_id","currency","effective_from")
);
--> statement-breakpoint
CREATE TABLE "model_provider" (
	"id" text PRIMARY KEY NOT NULL,
	"tenant_id" text NOT NULL,
	"name" text NOT NULL,
	"plugin" text NOT NULL,
	"base_url" text NOT NULL,
	"credential_id" text,
	"wallet_id" text,
	"disabled" boolean DEFAULT false NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "model_provider_tenant_name" UNIQUE("tenant_id","name"),
	CONSTRAINT "model_provider_auth_xor" CHECK (("model_provider"."credential_id" is not null) <> ("model_provider"."wallet_id" is not null))
);
--> statement-breakpoint
ALTER TABLE "model" ADD CONSTRAINT "model_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "model_offering" ADD CONSTRAINT "model_offering_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "model_offering" ADD CONSTRAINT "model_offering_model_id_model_id_fk" FOREIGN KEY ("model_id") REFERENCES "public"."model"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "model_offering" ADD CONSTRAINT "model_offering_provider_id_model_provider_id_fk" FOREIGN KEY ("provider_id") REFERENCES "public"."model_provider"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "model_pricing" ADD CONSTRAINT "model_pricing_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "model_pricing" ADD CONSTRAINT "model_pricing_offering_id_model_offering_id_fk" FOREIGN KEY ("offering_id") REFERENCES "public"."model_offering"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "model_provider" ADD CONSTRAINT "model_provider_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "model_provider" ADD CONSTRAINT "model_provider_credential_id_credential_id_fk" FOREIGN KEY ("credential_id") REFERENCES "public"."credential"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "model_provider" ADD CONSTRAINT "model_provider_wallet_id_wallet_id_fk" FOREIGN KEY ("wallet_id") REFERENCES "public"."wallet"("id") ON DELETE restrict ON UPDATE no action;