CREATE TABLE "agent_role" (
	"agent_id" text NOT NULL,
	"role_id" text NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "agent_role_agent_id_role_id_pk" PRIMARY KEY("agent_id","role_id")
);
--> statement-breakpoint
ALTER TABLE "agent_role" ADD CONSTRAINT "agent_role_agent_id_agent_id_fk" FOREIGN KEY ("agent_id") REFERENCES "public"."agent"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_role" ADD CONSTRAINT "agent_role_role_id_role_id_fk" FOREIGN KEY ("role_id") REFERENCES "public"."role"("id") ON DELETE cascade ON UPDATE no action;
