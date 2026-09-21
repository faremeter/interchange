# Workflow lifetime and capacity retention

A lifecycle policy releases capacity when a workflow finishes, keeps a failed
environment around for inspection, and stops deployments that have lived too
long. Tenant and installed-workflow policy decide when those things happen.
These settings belong to the deployment configuration, outside the workflow's
executable definition.

The policy shape, shown as TypeScript:

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

Both tenants and installed workflows accept a `lifecycle` field with this
shape. The API validates durations as non-negative whole numbers with a unit,
at most `36500d` (the same limit applies in seconds, minutes, or hours);
`maxLifetime` must be greater than zero. This fixed range leaves headroom when
a duration is added to a deployment or terminal timestamp. Omit a field to
inherit it, or to take the platform default when it is absent throughout the
hierarchy.

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

An installed workflow can shorten those limits:

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
That request merges `config` by top-level key, so it keeps other keys such as
`sidecarPlacement`; a `null` value removes a key. A stored key that fails
validation, such as a `lifecycle` key saved before lifecycle policies existed,
must be replaced or removed. Until then, reading the tenant, and deploying or
editing a lifecycle policy anywhere in its subtree, fail with
`409 invalid_tenant_config`, and updates that leave the key in place are
rejected. Errors involving inherited config omit its values from the response;
the server logs retain the validation details for administrators.
Installed-workflow overrides are replaced through
`PATCH /api/tenants/:tenantId/workflows/definitions/:definitionId/lifecycle`
with a `{ "lifecycle": ... }` body. Both require management permission. Each
revision of a workflow asset is a separate definition; an override covers every
revision of the definition's asset, including revisions deployed later, so it
requires management permission on every existing revision.

The effective policy and deadlines are saved when the deployment is created.
Later policy edits apply only to new deployments.

Omitted fields inherit through the tenant hierarchy. Tenant values are defaults
and ceilings: descendants and installed workflows can shorten them; values above
an ancestor's limit are rejected when set. Tightening an ancestor never blocks
deployment: a descendant or installed-workflow value it now exceeds is capped at
the inherited limit when a deployment is created. Inherited limits are visible
below the tenant that sets them: a deployment's saved policy reflects them, and
an edit that exceeds one is rejected. A field absent throughout the
hierarchy takes the platform default. The Hub reads each default from an
optional environment variable at startup and refuses to start if one is not a
valid duration or the maximum lifetime is zero:

| Field                         | Environment variable                   | Default |
| ----------------------------- | -------------------------------------- | ------- |
| `maxLifetime`                 | `WORKFLOW_DEFAULT_MAX_LIFETIME`        | `7d`    |
| `capacityRetention.completed` | `WORKFLOW_DEFAULT_RETENTION_COMPLETED` | `30m`   |
| `capacityRetention.failed`    | `WORKFLOW_DEFAULT_RETENTION_FAILED`    | `24h`   |
| `capacityRetention.cancelled` | `WORKFLOW_DEFAULT_RETENTION_CANCELLED` | `1h`    |

The platform default is not a ceiling: a tenant or installed workflow may set a
longer duration. To effectively disable an action, set its field to `36500d`.

`maxLifetime` measures wall-clock time from creation of the deployment's anchor
run. Provisioning and waiting count; restarts, replacement workers, and any
future hibernation do not reset the clock. This also covers deployments that
never receive their first trigger. If still live at expiry, the Hub stops the
deployment as `cancelled`, allowing a 30-second cancellation grace period before stopping the process.
Forced stop preempts a pending cooperative cancellation. The sidecar acknowledges
stop only after the child exits and its restart record is removed; inspection
state remains until allocation cleanup.
Cancellation also removes the restart record before acknowledgement when no
supervisor remains, preventing a later sidecar restart from reviving the run.
If the worker cannot confirm its stop, the Hub releases its allocation and waits
for confirmed destruction before recording cancellation. Enforcement resumes
after a Hub outage; the deadline does not promise an exact destruction time.

`capacityRetention` starts when the top-level run becomes terminal. Here, failure
retains the environment for 15 minutes; success and cancellation request
immediate release. A child run finishing does not release the deployment's
allocation. Top-level scratch survives termination for inspection until
allocation cleanup. Retained Hub history has a separate lifetime.

The Hub uses the terminal event's timestamp, bounded by run creation and Hub
observation time. Missing or invalid timestamps use observation time. Once saved,
the run's end time is not changed by retries.

The Hub persists deadlines and reconciles due actions independently of provisioner
calls. Expired or cancelling runs cannot start, restore, or accept new work.
An accepted terminal event must remain discoverable for cleanup after a restart
or a failed status projection. Cancellation and release must tolerate retries,
and a release is complete only when the provisioner confirms it. Forced termination can leave a partial event log; the Hub records the confirmed
outcome in the run row without fabricating workflow events. A failed
cleanup remains visible and must not make the capacity available for reuse.

Before a workflow-run pack can advance Git, the Hub records a pending projection
for its deployment, and removes it once every run in the pack has its status
recorded or the receive provably left Git unchanged. Any pending projection left
behind, for a live or finished deployment, is reconciled from Git after a
30-second grace: the Hub records the accepted terminal outcome of every run,
including child runs it never recorded, and then clears it. A receive still in
progress is never reconciled, because it may advance Git after the read. A
forced stop records `cancelled` only while no pending projection remains; until
then the worker is still stopped but the outcome waits. A missing repository
under a receive's pending projection is treated as unreadable history, never as
empty history; a repository whose ref was never written provably accepted
nothing. Reconciliation that cannot read Git backs off and retries; an explicit
release retries the read and answers 503 while accepted history remains
unreconciled.

Hub delivery checks lifecycle state under the anchor run's row lock, which
cancellation and retirement of a runnable deployment's allocation also take,
immediately before queuing restore, deployment, mail, or signal frames on the
socket. Pack ingestion holds only the allocation row, so a long receive does not
delay delivery. For deployment sends, initialization reservations finish before admission;
worker acknowledgements are awaited after the lock is released. Retries and reconnect
replay use that same check; stop and cancellation commands remain deliverable.
This orders new sends against cancellation but cannot recall frames already
queued before cancellation or expiry; those frames may arrive later.

An explicit release request uses the same path:

```http
POST /api/tenants/:tenantId/workflows/runs/:runId/capacity/release
```

The caller needs `manage` on `workflow-run:<runId>`, matching the existing stop
route, and the run must belong to the tenant in the path. Deployment creation's
`workflow:*/create` grant alone does not authorize release.

This endpoint accepts a terminal top-level run: `202` for pending
release, `204` if already released, `409` for a live run, and `503` while the
run's accepted history is not reconciled yet. Release status is
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
