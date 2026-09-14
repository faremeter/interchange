# Workflow lifetime and capacity retention

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
with a unit; `maxLifetime` must be greater than zero.

A tenant config can contain:

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

Tenant policy is set through `PATCH /api/tenants/:tenantId` as `config.lifecycle`.
Installed-workflow overrides are replaced through
`PATCH /api/tenants/:tenantId/workflows/definitions/:definitionId/lifecycle`
with a `{ "lifecycle": ... }` body. Both require management permission.

The effective policy and deadlines are saved when the deployment is created.
Later policy edits apply only to new deployments.

Omitted fields inherit through the tenant hierarchy. Tenant values are defaults
and ceilings: descendants and installed workflows can shorten them; values above
an ancestor's limit are rejected. A field absent throughout the hierarchy
schedules no automatic action. The example durations are not platform defaults.

`maxLifetime` measures wall-clock time from creation of the deployment's anchor
run. Provisioning and waiting count; restarts, replacement workers, and any
future hibernation do not reset the clock. This also covers deployments that
never receive their first trigger. If still live at expiry, the Hub stops the
deployment as `cancelled`, allowing a 30-second cancellation grace period before stopping the process.
If the worker cannot confirm its stop, the Hub releases its allocation and waits
for confirmed destruction before recording cancellation. Enforcement resumes
after a Hub outage; the deadline does not promise an exact destruction time.

`capacityRetention` starts when the top-level run becomes terminal. Here, failure
retains the environment for 15 minutes; success and cancellation request
immediate release. A child run finishing does not release the deployment's
allocation. Top-level scratch survives termination for inspection until
allocation cleanup. Retained Hub history has a separate lifetime.

The Hub persists deadlines and reconciles due actions independently of provisioner
calls. Expired or cancelling runs cannot start, restore, or accept new work.
An accepted terminal event must remain discoverable for cleanup after a restart
or a failed status projection. Cancellation and release must tolerate retries,
and a release is complete only when the provisioner confirms it. Forced termination can leave a partial event log; the Hub records the confirmed
outcome in the run row without fabricating workflow events. A failed
cleanup remains visible and must not make the capacity available for reuse.

An explicit release request would use the same path:

```http
POST /api/tenants/:tenantId/workflows/runs/:runId/capacity/release
```

The caller needs `manage` on `workflow-run:<runId>`, matching the existing stop
route, and the run must belong to the tenant in the path. Deployment creation's
`workflow:*/create` grant alone does not authorize release.

This endpoint accepts a terminal top-level run: `202` for pending
release, `204` if already released, and `409` for a live run. Release status is
available at `GET /api/tenants/:tenantId/workflows/runs/:runId/lifecycle`,
alongside the saved policy, deadlines, and cleanup errors. Permanent cleanup
failure returns `409` on a new release request and requires operator intervention.
`DELETE /api/tenants/:tenantId/workflows/runs/:runId` requests cancellation of a
live run and returns `202`; its cancellation retention policy then applies.

Releasing the allocation gives up the deployment's claim on capacity. The
provisioner decides whether to destroy the backing resources or prepare them for
reuse. This fits the work on provisioning pre-existing capacity: another
deployment could claim it when the provisioner binding, capabilities, and
placement requirements match. The previous assignment, credentials, and local
state must be retired or reset before it becomes available. Capacity retained
for inspecting a failed run is still reserved to that run. Retention is a cleanup
deadline, not a minimum preservation guarantee: manual release or infrastructure
failure can end it earlier.

The provisioner owns how long unused backing capacity stays available before
being destroyed. The Hub's allocation reconciler performs cleanup by calling
`SidecarProvisioner.destroy()` on the plugin bound to the allocation.
These duration rules do not need a separate
workflow-aware reaping plugin. Hibernating and resuming a live run remains
separate work, especially when it depends on files held only on the sidecar.
