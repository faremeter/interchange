CREATE TABLE IF NOT EXISTS "session_message" (
  "id" text PRIMARY KEY NOT NULL,
  "session_id" text NOT NULL REFERENCES "agent_session"("id") ON DELETE CASCADE,
  "tenant_id" text NOT NULL REFERENCES "tenant"("id") ON DELETE CASCADE,
  "role" text NOT NULL,
  "status" text DEFAULT 'pending' NOT NULL,
  "created_at" timestamp DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS "message_part" (
  "id" text PRIMARY KEY NOT NULL,
  "message_id" text NOT NULL REFERENCES "session_message"("id") ON DELETE CASCADE,
  "session_id" text NOT NULL REFERENCES "agent_session"("id") ON DELETE CASCADE,
  "type" text NOT NULL,
  "content" text,
  "metadata" jsonb,
  "ordinal" integer NOT NULL
);
