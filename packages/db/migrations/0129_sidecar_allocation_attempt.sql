CREATE TABLE "sidecar_allocation_attempt" (
	"allocation_id" text NOT NULL,
	"generation" integer NOT NULL,
	"provisioner_id" text NOT NULL,
	"code" text NOT NULL,
	"message" text NOT NULL,
	"retryable" boolean NOT NULL,
	"cleanup_required" boolean NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "sidecar_allocation_attempt_allocation_id_generation_pk" PRIMARY KEY("allocation_id","generation")
);
--> statement-breakpoint
ALTER TABLE "sidecar_allocation_attempt" ADD CONSTRAINT "sidecar_allocation_attempt_allocation_id_sidecar_allocation_id_fk" FOREIGN KEY ("allocation_id") REFERENCES "public"."sidecar_allocation"("id") ON DELETE cascade ON UPDATE no action;