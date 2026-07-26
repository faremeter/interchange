-- Drop session_mail's foreign key to agent_instance. instance_id becomes a
-- bare polymorphic column -- a legacy agent_instance id, or null for a folded
-- run -- mirroring inference_turn.instance_id (migration 0052). A folded run's
-- id comes from a shared id space that agent_instance does not contain, so no
-- single-table foreign key spans both.
ALTER TABLE "session_mail" DROP CONSTRAINT "session_mail_instance_id_agent_instance_id_fk";
