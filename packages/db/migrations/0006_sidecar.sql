CREATE TABLE IF NOT EXISTS "sidecar" (
  "id" text PRIMARY KEY NOT NULL,
  "url" text NOT NULL,
  "status" text DEFAULT 'online' NOT NULL,
  "last_heartbeat" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
