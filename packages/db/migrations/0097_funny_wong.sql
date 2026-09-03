CREATE TABLE "execution_host_assignment" (
	"sidecar_id" text PRIMARY KEY NOT NULL,
	"allocation_id" text NOT NULL,
	"generation" integer NOT NULL,
	"host_id" text NOT NULL,
	"host_session_id" text NOT NULL,
	"host_session_generation" integer NOT NULL,
	"status" text NOT NULL,
	"destroyed_generation" integer,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "execution_host_assignment_generation_check" CHECK ("execution_host_assignment"."generation" >= 0),
	CONSTRAINT "execution_host_assignment_session_generation_check" CHECK ("execution_host_assignment"."host_session_generation" >= 1),
	CONSTRAINT "execution_host_assignment_destroyed_check" CHECK (("execution_host_assignment"."status" = 'destroyed') = ("execution_host_assignment"."destroyed_generation" is not null))
);
--> statement-breakpoint
ALTER TABLE "execution_host_assignment" ADD CONSTRAINT "execution_host_assignment_sidecar_id_sidecar_id_fk" FOREIGN KEY ("sidecar_id") REFERENCES "public"."sidecar"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "execution_host_assignment" ADD CONSTRAINT "execution_host_assignment_allocation_id_sidecar_allocation_id_fk" FOREIGN KEY ("allocation_id") REFERENCES "public"."sidecar_allocation"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "execution_host_assignment" ADD CONSTRAINT "execution_host_assignment_host_id_execution_host_id_fk" FOREIGN KEY ("host_id") REFERENCES "public"."execution_host"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "execution_host_assignment_active_host_idx" ON "execution_host_assignment" USING btree ("host_id") WHERE "execution_host_assignment"."status" in ('claiming', 'assigned');--> statement-breakpoint
CREATE UNIQUE INDEX "execution_host_assignment_active_allocation_idx" ON "execution_host_assignment" USING btree ("allocation_id") WHERE "execution_host_assignment"."status" in ('claiming', 'assigned');--> statement-breakpoint
CREATE INDEX "execution_host_assignment_allocation_idx" ON "execution_host_assignment" USING btree ("allocation_id");
