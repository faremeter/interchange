# Workflow lifetime and capacity retention

A lifecycle policy releases capacity when a workflow finishes, keeps a failed
environment around for inspection, stops deployments that have lived too long,
and fails deployments whose sidecar has been out of contact too long. Tenant
and installed-workflow policy decide when those things happen. These settings
belong to the deployment configuration, outside the workflow's executable
definition.

The policy shape, shown as TypeScript:

```typescript
type LifecycleDuration = `${number}${"s" | "m" | "h" | "d"}`;

type WorkflowLifecyclePolicy = {
  maxLifetime?: LifecycleDuration;
  maxDisconnected?: LifecycleDuration;
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
`maxLifetime` and `maxDisconnected` must be greater than zero. This fixed range leaves headroom when
a duration is added to a deployment or terminal timestamp. Omit a field to
inherit it, or to take the platform default when it is absent throughout the
hierarchy.

A tenant config can contain:

```json
{
  "lifecycle": {
    "maxLifetime": "24h",
    "maxDisconnected": "30m",
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
valid duration or the maximum lifetime or disconnect limit is zero:

| Field                         | Environment variable                   | Default |
| ----------------------------- | -------------------------------------- | ------- |
| `maxLifetime`                 | `WORKFLOW_DEFAULT_MAX_LIFETIME`        | `7d`    |
| `maxDisconnected`             | `WORKFLOW_DEFAULT_MAX_DISCONNECTED`    | `15m`   |
| `capacityRetention.completed` | `WORKFLOW_DEFAULT_RETENTION_COMPLETED` | `30m`   |
| `capacityRetention.failed`    | `WORKFLOW_DEFAULT_RETENTION_FAILED`    | `24h`   |
| `capacityRetention.cancelled` | `WORKFLOW_DEFAULT_RETENTION_CANCELLED` | `1h`    |

The platform default is not a ceiling: a tenant or installed workflow may set a
longer duration. To effectively disable an action, set its field to `36500d`.

`maxLifetime` measures wall-clock time from creation of the deployment's anchor
run. Provisioning and waiting count; restarts and any future hibernation do
not reset the clock. This also covers deployments that
never receive their first trigger. If still live at expiry, the Hub stops the
deployment as `cancelled`, allowing a 30-second cancellation grace period before stopping the process.
Forced stop preempts a pending cooperative cancellation. The sidecar acknowledges
stop only after the child exits and its restart record is marked stopped, so a
sidecar restart reports the deployment instead of reviving it; the record and
inspection state remain until allocation cleanup undeploys it. The acknowledgement reports the tip of
each workflow-history ref, and the sidecar pushes any commit the Hub has not
acknowledged. The Hub confirms the stop only once it holds those tips; until
then the stop is retried until 60 seconds past the cancellation deadline, or
past Hub startup if that is later.
Cancellation also marks the restart record stopped before acknowledgement when
no supervisor remains, preventing a later sidecar restart from reviving the run.
If the worker cannot confirm its stop, the Hub releases its allocation and waits
for confirmed destruction before recording cancellation. History the Hub never
received is destroyed with the worker, so the recorded outcome then reflects only
accepted history, even if the worker had committed a different one. Enforcement resumes
after a Hub outage; the deadline does not promise an exact destruction time.

`maxDisconnected` bounds how long a deployment's sidecar may be out of contact
with the Hub. The clock starts when its connection drops and starts again when
the Hub starts, since no sidecar can reconnect while the Hub is down; at a Hub
start, a deployment whose first deploy has not completed gets at least the
Hub's first-connect window, the 2 minutes a newly placed sidecar has to
connect. A sidecar that reconnects in time keeps its
deployment. Past the limit the Hub fails the deployment's live runs as an
infrastructure loss and releases its capacity at once. A sidecar that comes
back after that has its copy undeployed, and the history it pushes is refused
because its allocation was released, so whatever it did while away is
discarded. A restarting sidecar connects only after it has
restored its deployments, 8 at a time; when every workflow child takes its
whole 30-second ready timeout, a full one spends about 8 minutes on those
timeouts alone, so a shorter limit can fail the deployments of a sidecar that
is only restarting.

On a sidecar that also hosts other work, release removes only this
deployment's hold on the sidecar. A connected sidecar is told to undeploy the
deployment, which stops its child as part of teardown. An unreachable copy can
keep running past the Hub's cleanup disconnect deadline: a recorded failure
cannot stop a disconnected process. Expiry records `destroy_failed` with
`sidecar_cleanup_disconnect_timeout`; that failure resumes cleanup on
reconnect. Exhausted retries and permanent provider rejection instead require
operator recovery and preserve cleanup-only bindings for inventory without
restoring workflow routes. Removal is confirmed by an undeploy acknowledgement
or by provider destruction that guarantees the worker and its restorable state
cannot return.

`capacityRetention` starts when the top-level run becomes terminal. Here, failure
retains the environment for 15 minutes; success and cancellation request
immediate release. A deployment its sidecar reports stopped on its own, because
its workflow child ended itself or could not be restored, fails and is retained
as a failure, so its stopped copy can be inspected. A child run finishing does not release the deployment's
allocation. Top-level scratch survives termination for inspection until
allocation cleanup. Retained Hub history has a separate lifetime.

The Hub uses the terminal event's timestamp, bounded by run creation and Hub
observation time. Missing or invalid timestamps use observation time. Once saved,
the run's end time is not changed by retries.

The Hub persists deadlines and reconciles due actions independently of provisioner
calls. Expired or cancelling runs cannot start, restore, or accept new work.
An accepted terminal event must remain discoverable for cleanup after a restart
or a failed status projection. Cancellation and release must tolerate retries.
Release completes only when the provider's hold is released and deployment
cleanup is confirmed. Forced termination can leave a partial event log; the Hub
records the confirmed outcome in the run row without fabricating workflow events.
A failed cleanup remains visible. It reserves an active slot until removal
has been confirmed. The separate
provider obligation can remain failed after the copy has freed its active slot.
See [Release cleanup](SIDECAR_PLACEMENT.md#release-cleanup).

Before a workflow-run pack can advance Git, the Hub records a pending projection
for its deployment, and removes it once every run in the pack has its status
recorded or the receive provably left Git unchanged. Any pending projection left
behind, for a live or finished deployment, is reconciled from Git after a
30-second grace: the Hub records the accepted terminal outcome of every run,
including child runs it never recorded, and then clears it. A receive still in
progress is never reconciled, because it may advance Git after the read. A
forced stop records `cancelled` only while no pending projection remains; until
then the worker is still stopped but the outcome waits. Unrecoverable capacity
loss waits the same way: the Hub records when capacity was lost, and after
reconciliation fails only the runs still live, ending them at that time. A
missing repository
under a receive's pending projection is treated as unreadable history, never as
empty history; a repository whose ref was never written provably accepted
nothing. Reconciliation that cannot read Git backs off and retries; an explicit
release retries the read and answers 503 while accepted history remains
unreconciled.

Lifecycle reconciliation uses the shared reconciliation scheduler with eight
independent slots per Hub. Each slot selects one run at a time, and active runs
are excluded from further selection until their work finishes. Selection
considers only deployments with work due: a live run that is cancelling or past
its expiry, retained capacity whose release time has passed or is not yet
recorded, a capacity loss waiting on reconciliation, and accepted history left
unprojected past its grace. A deployment that ended without a saved policy is
released only on request. Selection walks run IDs up to a captured upper bound,
then waits one second before another scan. Work that becomes due during a scan
can wait for the next one, and work that stays due, such as a cancellation
waiting on its worker, does not cause continuous retries. A
stalled run occupies one slot while the other slots continue processing
candidates. A live run with no cancellation, expiry, or capacity loss to act on
is not locked, so a pack receive in progress does not hold a slot.

Dispatch outcomes are projected from Hub-owned Git after pack acceptance and
retried independently for unresolved deliveries, even when the database has not
yet recorded the run's terminal status or the allocation has been released.
Each pass pins workflow history on `refs/heads/main` before reading consumed-mail
outcomes on `refs/heads/events`. Proven deliveries are settled, recorded rejections
retain their reason, and remaining examined deliveries are failed only after both
histories are read and the pinned workflow history proves the run is terminal.
History reads do not hold the database locks needed for cleanup.

Forced stop, confirmed capacity release, and unrecoverable allocation failure
mark outstanding delivery attempts `abandoned`: retries have stopped, but
acceptance is unconfirmed. A confirmed release records abandonment even while
accepted history is awaiting projection, without changing the run's outcome.
Later Git evidence can resolve an abandoned delivery as settled or failed. If forced
termination permanently lost that evidence, the delivery remains abandoned.
Recorded rejections and settled deliveries are never overwritten by abandonment.
Settled and failed records remain final; recovery scans pending, acknowledged,
and abandoned records. An abandoned record stays in recovery until one scan
reads its deployment's final history; if that scan finds no evidence, it
remains abandoned and is not scanned again.
Recovery scans use bounded independent slots; a stalled read occupies its own
slot and is not duplicated by later polls.

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

This endpoint accepts a terminal top-level run: `202` for pending release,
`204` if already released, `409` for a live run or any `destroy_failed`
allocation, and `503` while the run's accepted history is not reconciled yet.
Release status is available at
`GET /api/tenants/:tenantId/workflows/runs/:runId/lifecycle`, alongside the saved
policy, deadlines, and cleanup errors. A cleanup disconnect timeout resumes on
reconnect; exhausted retries and permanent provider rejection require operator
intervention. `DELETE /api/tenants/:tenantId/workflows/runs/:runId` requests
cancellation of a live run and returns `202`; its cancellation retention
policy then applies.

Beginning release removes workflow routes. The allocation reaches `released`
only after the provider's hold is released and deployment cleanup is confirmed.
Confirmed removal frees the active sidecar slot even if the provider's hold
remains pending or failed. The provisioner decides whether to destroy the backing
resources or prepare them for reuse. This fits the work on provisioning pre-existing capacity: another
deployment could claim it when the provisioner binding, capabilities, and
placement requirements match. The previous assignment, credentials, and local
state must be retired or reset before dedicated capacity becomes available
again; a shared sidecar drops a released deployment when the Hub undeploys it.
Capacity retained for inspecting a failed run is still reserved to that run.
Retention is a cleanup deadline, not a minimum preservation guarantee: manual
release or infrastructure failure can end it earlier.

The provisioner owns how long unused backing capacity stays available before
being destroyed. The Hub's allocation reconciler requests acknowledged undeploy
from connected sidecars independently of provider availability and releases the
provider hold through `SidecarProvisioner.destroy()`. Either an undeploy
acknowledgement or confirmed provider destruction proves removal.
These duration rules do not need a separate
workflow-aware reaping plugin. Hibernating and resuming a live run remains
separate work, especially when it depends on files held only on the sidecar.
