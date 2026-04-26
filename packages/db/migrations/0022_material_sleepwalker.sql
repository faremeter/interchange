CREATE TABLE "inference_turn" (
	"id" text PRIMARY KEY NOT NULL,
	"session_id" text NOT NULL,
	"instance_id" text NOT NULL,
	"tenant_id" text NOT NULL,
	"model" text NOT NULL,
	"status" text DEFAULT 'running' NOT NULL,
	"started_at" timestamp NOT NULL,
	"ended_at" timestamp
);
--> statement-breakpoint
CREATE TABLE "turn_part" (
	"id" text PRIMARY KEY NOT NULL,
	"turn_id" text NOT NULL,
	"session_id" text NOT NULL,
	"type" text NOT NULL,
	"content" text,
	"metadata" jsonb,
	"ordinal" integer NOT NULL
);
--> statement-breakpoint
ALTER TABLE "inference_turn" ADD CONSTRAINT "inference_turn_session_id_agent_session_id_fk" FOREIGN KEY ("session_id") REFERENCES "public"."agent_session"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "inference_turn" ADD CONSTRAINT "inference_turn_instance_id_agent_instance_id_fk" FOREIGN KEY ("instance_id") REFERENCES "public"."agent_instance"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "inference_turn" ADD CONSTRAINT "inference_turn_tenant_id_tenant_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenant"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "turn_part" ADD CONSTRAINT "turn_part_turn_id_inference_turn_id_fk" FOREIGN KEY ("turn_id") REFERENCES "public"."inference_turn"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "turn_part" ADD CONSTRAINT "turn_part_session_id_agent_session_id_fk" FOREIGN KEY ("session_id") REFERENCES "public"."agent_session"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "inference_turn_instance_id_started_at_idx" ON "inference_turn" USING btree ("instance_id","started_at");