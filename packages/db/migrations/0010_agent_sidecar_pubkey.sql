ALTER TABLE "agent" ADD COLUMN "sidecar_id" text REFERENCES "sidecar"("id") ON DELETE SET NULL;
ALTER TABLE "agent" ADD COLUMN "public_key" text;
