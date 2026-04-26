CREATE TABLE "session_mail" (
	"id" text PRIMARY KEY NOT NULL,
	"session_id" text NOT NULL,
	"instance_id" text,
	"tenant_id" text NOT NULL,
	"direction" text NOT NULL,
	"status" text DEFAULT 'pending' NOT NULL,
	"raw" "bytea" NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "session_mail" ADD CONSTRAINT "session_mail_session_id_agent_session_id_fk" FOREIGN KEY ("session_id") REFERENCES "public"."agent_session"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "session_mail" ADD CONSTRAINT "session_mail_instance_id_agent_instance_id_fk" FOREIGN KEY ("instance_id") REFERENCES "public"."agent_instance"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "session_mail" ADD CONSTRAINT "session_mail_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "session_mail_instance_id_created_at_idx" ON "session_mail" USING btree ("instance_id","created_at");