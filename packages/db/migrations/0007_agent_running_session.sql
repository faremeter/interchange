ALTER TYPE agent_status ADD VALUE IF NOT EXISTS 'running';

ALTER TABLE agent ADD COLUMN session_id text;