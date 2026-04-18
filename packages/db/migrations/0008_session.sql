CREATE TABLE IF NOT EXISTS "agent_session" (
  "id" text PRIMARY KEY NOT NULL,
  "tenant_id" text NOT NULL REFERENCES "tenant"("id") ON DELETE CASCADE,
  "agent_id" text NOT NULL REFERENCES "agent"("id") ON DELETE CASCADE,
  "principal_id" text NOT NULL REFERENCES "principal"("id"),
  "status" text DEFAULT 'active' NOT NULL,
  "created_at" timestamp DEFAULT now() NOT NULL,
  "updated_at" timestamp DEFAULT now() NOT NULL,
  "ended_at" timestamp
);
