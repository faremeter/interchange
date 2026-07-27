-- Drop the agent_asset table. Manual attach-to-an-agent was never wired,
-- so the table has no writer and is empty, and the launch-time direct
-- attachment path that read it is gone. session_asset's agent_asset_id
-- foreign key and its source column only distinguished direct attachments
-- from resolver-derived materializations; with the direct path gone every
-- row is resolver-derived, so both are dead. Drop the two columns first --
-- dropping agent_asset_id takes its foreign key with it -- so the table
-- has no remaining dependents when it goes (no CASCADE needed).
ALTER TABLE "session_asset" DROP COLUMN "agent_asset_id";--> statement-breakpoint
ALTER TABLE "session_asset" DROP COLUMN "source";--> statement-breakpoint
DROP TABLE "agent_asset";
