ALTER TABLE "sidecar" ADD COLUMN "provisioner_id" text;--> statement-breakpoint
ALTER TABLE "sidecar" ADD COLUMN "provisioner_api_version" integer;--> statement-breakpoint
ALTER TABLE "sidecar" ADD COLUMN "provisioner_binding_fingerprint" text;--> statement-breakpoint
ALTER TABLE "sidecar" ADD COLUMN "adapter_manifest_digest" text;--> statement-breakpoint
ALTER TABLE "sidecar" ADD COLUMN "retired_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "sidecar" ADD COLUMN "reconnect_window_ended_at" timestamp with time zone;--> statement-breakpoint
UPDATE "sidecar" SET
	"provisioner_id" = "placed"."provisioner_id",
	"provisioner_api_version" = "placed"."provisioner_api_version",
	"provisioner_binding_fingerprint" = "placed"."provisioner_binding_fingerprint",
	"adapter_manifest_digest" = "placed"."adapter_manifest_digest"
FROM (
	SELECT DISTINCT ON ("sidecar_id")
		"sidecar_id", "provisioner_id", "provisioner_api_version",
		"provisioner_binding_fingerprint", "adapter_manifest_digest"
	FROM (
		SELECT "sidecar_id", "provisioner_id", "provisioner_api_version",
			"provisioner_binding_fingerprint", "adapter_manifest_digest", "created_at"
		FROM "sidecar_allocation" WHERE "sidecar_id" IS NOT NULL
		UNION ALL
		SELECT "sidecar_id", "provisioner_id", "provisioner_api_version",
			"provisioner_binding_fingerprint", "adapter_manifest_digest", "created_at"
		FROM "workflow_probe" WHERE "sidecar_id" IS NOT NULL
	) AS "work"
	ORDER BY "sidecar_id", "created_at"
) AS "placed"
WHERE "sidecar"."id" = "placed"."sidecar_id";--> statement-breakpoint
DELETE FROM "sidecar" WHERE "provisioner_id" IS NULL;--> statement-breakpoint
UPDATE "sidecar" SET "retired_at" = now()
WHERE NOT EXISTS (
	SELECT 1 FROM "sidecar_allocation"
	WHERE "sidecar_allocation"."sidecar_id" = "sidecar"."id"
		AND "sidecar_allocation"."status" IN ('provisioning', 'allocated', 'replacing', 'releasing', 'destroy_failed')
) AND NOT EXISTS (
	SELECT 1 FROM "workflow_probe"
	WHERE "workflow_probe"."sidecar_id" = "sidecar"."id"
		AND "workflow_probe"."status" IN ('provisioning', 'probing', 'releasing')
);--> statement-breakpoint
ALTER TABLE "sidecar" ALTER COLUMN "provisioner_id" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "sidecar" ALTER COLUMN "provisioner_api_version" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "sidecar" ALTER COLUMN "provisioner_binding_fingerprint" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "sidecar" ALTER COLUMN "adapter_manifest_digest" SET NOT NULL;
