CREATE TABLE "asset" (
	"id" text PRIMARY KEY NOT NULL,
	"tenant_id" text NOT NULL,
	"kind" text NOT NULL,
	"name" text NOT NULL,
	"display_name" text,
	"creator_principal_id" text,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "asset_tenant_kind_name" UNIQUE("tenant_id","kind","name")
);
--> statement-breakpoint
CREATE TABLE "agent_asset" (
	"id" text PRIMARY KEY NOT NULL,
	"agent_id" text NOT NULL,
	"asset_id" text NOT NULL,
	"ref" text NOT NULL,
	"access_mode" text DEFAULT 'read-only' NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "agent_asset_agent_asset" UNIQUE("agent_id","asset_id")
);
--> statement-breakpoint
ALTER TABLE "asset" ADD CONSTRAINT "asset_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "asset" ADD CONSTRAINT "asset_creator_principal_id_principal_id_fk" FOREIGN KEY ("creator_principal_id") REFERENCES "public"."principal"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_asset" ADD CONSTRAINT "agent_asset_agent_id_agent_id_fk" FOREIGN KEY ("agent_id") REFERENCES "public"."agent"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_asset" ADD CONSTRAINT "agent_asset_asset_id_asset_id_fk" FOREIGN KEY ("asset_id") REFERENCES "public"."asset"("id") ON DELETE cascade ON UPDATE no action;