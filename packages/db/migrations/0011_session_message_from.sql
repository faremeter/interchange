ALTER TABLE "session_message" ADD COLUMN "from" text NOT NULL DEFAULT 'unknown';
ALTER TABLE "session_message" ALTER COLUMN "from" DROP DEFAULT;
