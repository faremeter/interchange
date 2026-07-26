-- Drop session_asset's foreign key to agent_instance. instance_id becomes a
-- bare polymorphic column -- a legacy agent_instance id or a folded run id --
-- mirroring inference_turn.instance_id (migration 0052). A folded launch
-- already writes its workflow_run id here, which the agent_instance FK rejected,
-- so this also unblocks attaching assets to a folded run. instance_id stays NOT
-- NULL and part of the (instance_id, mount_path) primary key.
ALTER TABLE "session_asset" DROP CONSTRAINT "session_asset_instance_id_agent_instance_id_fk";
