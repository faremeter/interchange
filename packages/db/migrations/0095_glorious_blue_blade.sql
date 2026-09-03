CREATE TABLE "execution_host_session" (
	"host_id" text PRIMARY KEY NOT NULL,
	"session_id" text NOT NULL,
	"generation" integer NOT NULL,
	"hub_instance_id" text NOT NULL,
	"capabilities" jsonb NOT NULL,
	"lease_expires_at" timestamp NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "execution_host_session_session_id_unique" UNIQUE("session_id"),
	CONSTRAINT "execution_host_session_generation_check" CHECK ("execution_host_session"."generation" >= 1)
);
--> statement-breakpoint
ALTER TABLE "execution_host_session" ADD CONSTRAINT "execution_host_session_host_id_execution_host_id_fk" FOREIGN KEY ("host_id") REFERENCES "public"."execution_host"("id") ON DELETE cascade ON UPDATE no action;
