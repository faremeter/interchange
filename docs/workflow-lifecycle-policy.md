# Workflow lifetime and capacity retention

Draft proposal.

We should support releasing capacity as soon as a workflow finishes, keeping a
failed environment around for inspection, and stopping deployments that have
lived too long. Tenant and installed-workflow policy should decide when those
things happen. These settings belong to the deployment configuration, outside
the workflow's executable definition.

The proposed policy shape, shown as TypeScript:

```typescript
type LifecycleDuration = `${number}${"s" | "m" | "h" | "d"}`;

type WorkflowLifecyclePolicy = {
  maxLifetime?: LifecycleDuration;
  capacityRetention?: {
    completed?: LifecycleDuration;
    failed?: LifecycleDuration;
    cancelled?: LifecycleDuration;
  };
};
```

Both tenants and installed workflows would accept a `lifecycle` field with this
shape. Validate durations at the API boundary as non-negative whole numbers
with a unit, at most `36500d` (the same limit applies in seconds, minutes, or
hours); `maxLifetime` must be greater than zero. This fixed range leaves room
for adding deployment and terminal timestamps. Omit a field to schedule no action
when it is absent throughout the hierarchy.

A tenant could set:

```json
{
  "lifecycle": {
    "maxLifetime": "24h",
    "capacityRetention": {
      "completed": "0s",
      "failed": "1h",
      "cancelled": "0s"
    }
  }
}
```

An installed workflow could shorten those limits:

```json
{
  "lifecycle": {
    "maxLifetime": "2h",
    "capacityRetention": {
      "failed": "15m"
    }
  }
}
```

Omitted fields inherit through the tenant hierarchy. Tenant values are defaults
and ceilings: descendants and installed workflows can shorten them; values above
an ancestor's limit are rejected when set. Tightening an ancestor never blocks
deployment: a descendant or installed-workflow value it now exceeds is capped at
the inherited limit when a deployment is created. Inherited limits are visible
below the tenant that sets them: a deployment's saved policy reflects them, and
an edit that exceeds one is rejected. A field absent throughout the
hierarchy schedules no automatic action. The example durations are not platform
defaults.

`maxLifetime` measures wall-clock time from creation of the deployment's anchor
run. Provisioning and waiting count; restarts, replacement workers, and any
future hibernation do not reset the clock. This also covers deployments that
never receive their first trigger. If still live at expiry, the Hub stops the
deployment as `cancelled`, using a bounded cancellation grace period followed by
enforced termination if needed.

`capacityRetention` starts when the top-level run becomes terminal. Here, failure
retains the environment for 15 minutes; success and cancellation request
immediate release. A child run finishing does not release the deployment's
allocation. Retained Hub history has a separate lifetime.

The Hub should persist deadlines and reconcile due actions from durable state.
An accepted terminal event must remain discoverable for cleanup after a restart
or a failed status projection. Cancellation and release must tolerate retries,
and a release is complete only when the provisioner confirms it. A failed
cleanup remains visible and must not make the capacity available for reuse.

An explicit release request would use the same path:

```http
POST /api/tenants/:tenantId/workflows/runs/:runId/capacity/release
```

The caller needs `manage` on `workflow-run:<runId>`, matching the existing stop
route, and the run must belong to the tenant in the path. Deployment creation's
`workflow:*/create` grant alone does not authorize release.

This proposed endpoint accepts a terminal top-level run: `202` for pending
release, `204` if already released, and `409` for a live run. Release status is
observable separately from the run's terminal status.

Releasing the allocation gives up the deployment's claim on capacity. The
provisioner decides whether to destroy the backing resources or prepare them for
reuse. This fits the work on provisioning pre-existing capacity: another
deployment could claim it when the provisioner binding, capabilities, and
placement requirements match. The previous assignment, credentials, and local
state must be retired or reset before it becomes available. Capacity retained
for inspecting a failed run is still reserved to that run.

The provisioner owns how long unused backing capacity stays available before
being destroyed. The Hub's allocation reconciler performs cleanup by calling
`SidecarProvisioner.destroy()` on the plugin bound to the allocation.
These duration rules do not need a separate
workflow-aware reaping plugin. Hibernating and resuming a live run remains
separate work, especially when it depends on files held only on the sidecar.
