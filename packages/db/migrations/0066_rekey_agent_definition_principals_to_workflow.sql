-- Re-key each folded agent's actor identity onto the workflow model. A
-- definition-level agent principal's ref_id is the legacy agent id; the fold
-- moved that agent onto a workflow_definition, so its principal moves too --
-- kind agent -> workflow, ref_id agent.id -> definition.id. Afterwards the
-- folded agent's stable identity is a workflow-kind principal keyed by the
-- definition, and any grants it owns ride along under the new key untouched.
--
-- The mapping is 1:1 per tenant, which is what keeps the rewrite from
-- colliding on principal's UNIQUE(tenant_id, kind, ref_id): the partial-unique
-- index on workflow_definition.origin_agent_id admits at most one definition
-- per agent id, so no two agent principals collapse onto the same workflow key.
-- Existing workflow principals key on a run id, never a definition id, so the
-- re-keyed rows cannot collide with them either. The tenant predicate holds the
-- rewrite inside a single tenant even though agent ids are globally unique.
--
-- Only the definition-level class re-keys. An instance-level agent principal's
-- ref_id is an agent_instance id, which matches no origin_agent_id (disjoint id
-- spaces), so the join leaves it kind agent -- it retires later with the
-- agent_instance table it depends on. Idempotent: kind = 'agent' guards the
-- rewrite, and once it runs no definition principal remains agent-kind, so a
-- re-run is a no-op.
UPDATE "principal" p
SET kind = 'workflow', ref_id = wd.id
FROM "workflow_definition" wd
WHERE p.kind = 'agent'
  AND wd.origin_agent_id = p.ref_id
  AND wd.tenant_id = p.tenant_id;
