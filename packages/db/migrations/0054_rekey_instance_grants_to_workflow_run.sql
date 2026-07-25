-- Re-key run-scoped grants onto the folded workflow-run resource. A folded run
-- shares the instance id space, so the rewrite is prefix-identity: the id after
-- the colon is unchanged, only the type name moves instance: -> workflow-run:
-- (carrying instance:* -> workflow-run:* too). The match is colon-delimited so
-- it never touches the unrelated agent-state: grants; there is no instance-state
-- sibling. Idempotent: after this runs no row matches instance:%, so a re-run is
-- a no-op.
UPDATE "grant"
SET resource = 'workflow-run:' || substr(resource, length('instance:') + 1)
WHERE resource LIKE 'instance:%';
