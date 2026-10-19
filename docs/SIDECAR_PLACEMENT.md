# Capability-Based Sidecar Placement

Workflows describe the runtime they need without naming a provisioner.
Provisioners advertise runtime guarantees, and tenant policy adds constraints
that workflows cannot weaken.

Capability selectors use grant-style specificity over namespaced identifiers.
A selector is exact (`runtime:browser`), a trailing namespace wildcard
(`runtime:*`), or the global wildcard (`*`). Other wildcard placements are
rejected.

## Workflow requirements

```ts
const workflow = defineWorkflow({
  id: "ios-app-agent",
  agent: iosAppAgent,
  sidecarPlacement: {
    capabilities: [
      { capability: "platform:ios", effect: "require" },
      { capability: "device:simulator", effect: "require" },
    ],
  },
});
```

Rules may require a capability or require that it is blocked:

```ts
type SidecarCapabilityRule = {
  capability: string;
  effect: "require" | "block";
};
```

Grant-style specificity supports broad rules with narrower exceptions:

```ts
const browserOnly = [
  { capability: "runtime:*", effect: "block" },
  { capability: "runtime:browser", effect: "require" },
] satisfies readonly SidecarCapabilityRule[];
```

The namespace rule is itself a guarantee that must be declared. For example,
`browserOnly` matches a provisioner that declares both `runtime:*` as blocked
and the narrower `runtime:browser` exception as available; enumerating exact
blocked runtimes does not establish the namespace-wide guarantee.

Requirements from inline loops, trigger bodies, and child workflows are folded
into the deployment because they execute in the same sidecar. Exact duplicate
`(capability, effect)` rules collapse to their first occurrence; opposing
effects remain distinct so the normal block-wins tie-break still applies.

## Provisioner declarations

```ts
const iosSimulator: SidecarProvisioner = {
  id: "ios-simulator",
  apiVersion: 2,
  bindingFingerprint: "ios-simulator:v1",
  capabilities: [
    { capability: "platform:ios", state: "available" },
    { capability: "device:simulator", state: "available" },
    { capability: "runtime:posix", state: "blocked" },
  ],
  async ensure(_request) {
    return { kind: "accepted" };
  },
  async destroy(_request) {
    return { kind: "destroyed", cleanup: "confirmed" };
  },
};
```

```ts
type SidecarCapabilityDeclaration = {
  capability: string;
  state: "available" | "blocked";
};
```

Asset-sourced workflow deployments automatically reuse validated probe results
for the same pinned revision under the current `WORKFLOW_PROBE_CACHE_VERSION`.
This internal constant in `workflow-probe-cache.ts` must be bumped when probe
behavior or the stored result shape changes. It invalidates probe results while
preserving the revision's frozen dependency resolution.

Probe workers sharing the Hub's cache must use compatible evaluator code and
environments; deployment configuration owns that requirement. Provisioner ids,
binding fingerprints and adapter manifests do not partition the probe cache.
The Hub still checks current placement policy for every deployment.

An omitted capability is unknown. It does not mean blocked. Provisioners decide
internally whether to create, isolate, share, or reuse their backing capacity.
To place a probe or deployment on a sidecar it already runs, a provisioner
answers `ensure` with that sidecar's id instead of starting capacity for the
identity the request carries. The Hub accepts that sidecar only when it was
minted for the same provisioner binding, whether or not it still hosts other
work, and has not been retired; otherwise it rejects the placement with
`sidecar_reuse_rejected`. The provisioner counts its holds on a sidecar, one
per allocation id it answered with that sidecar, and releases one per
`destroy`. When none remain, it decides whether the sidecar keeps running idle
for later work or stops. The Hub keeps an idle sidecar connected and routes
nothing to it. A provisioner that keeps it running answers `cleanup: required`,
and the Hub removes the deployment over that connection. Stopping a sidecar in
answer to a `destroy` must remove its restorable deployment state and report
`cleanup: confirmed`; a provider returning `cleanup: required` must keep a
sidecar cleanup path available. The provisioner keeps its holds durably, since
the Hub never re-announces them after a restart.

The Hub retires a sidecar identity once it learns that no sidecar runs with
it: its `ensure` was rejected, a `destroy` confirmed cleanup, or its
provisioner reported it stopped. A retired identity takes no new work, but
work still on it keeps reconnecting, so work on a sidecar a provisioner wrongly
reported gone is not cut off. Once neither current work nor a cleanup
obligation is left on it, the Hub refuses its next handshake: it asks the
sidecar to undeploy everything it reports and closes the socket. Only new
handshakes are refused; a connection already open stays open, idle, until it
closes.

A provisioner reports a stopped sidecar through the optional `attach` hook:
the Hub calls `attach` once at start with a host whose `sidecarStopped`
retires a sidecar minted for that provisioner's current binding and answers
`not_owned`, which is final, for any other. A provisioner reports a sidecar it
stops later, after answering its last `destroy` with `cleanup: required`, and
a reused sidecar it stopped whose placement the Hub never recorded, since the
`destroy` named the identity the Hub offered. A thrown `sidecarStopped` is
transient, so the provisioner keeps the report durably, like its holds, and
retries it, also after a Hub restart. Without a report, such a sidecar keeps a
valid token, which opens only an idle connection that gets work only if a
provisioner places work on that sidecar again. The same holds for a sidecar
minted under a binding the provisioner no longer has: it cannot report it, but
reuse already refuses it because the binding differs.

A report leaves work still on the sidecar alone by default: disconnected work
keeps its reconnect window, since the provisioner may bring the sidecar back
elsewhere. A provisioner that knows the work will not come back on that
sidecar passes `endReconnectWindow`. The Hub records that on the sidecar and
ends the reconnect window of its disconnected work at once, handing it to the
usual connection loss path, which today replaces a parked run and fails an
active one. Work that disconnects later, and work a Hub restart would
otherwise give a fresh window, gets no window either. Work still connected to
the sidecar contradicts the report; the Hub logs it and leaves that work
running until it disconnects. `endReconnectWindow` covers allocations only,
since a probe needs no help: one still waiting for its sidecar fails when its
first-connect window ends, and one already running fails as soon as its
connection drops. The Hub attaches provisioners only after its startup has
scheduled every allocation's reconnect, so a report retried at startup sees
work connected before the restart as disconnected.

A provisioner's declared guarantees, such as `isolation:workload`, must still
hold for work that shares a sidecar. The stock sidecar does not isolate the
work it hosts from each other or from
itself: every workflow child runs as the sidecar's OS user over its data
directory, so work that shares one can read each other's keys and run state,
and any child can read the sidecar's own environment, its Hub token and
credential encryption key included. A provisioner that declares
`isolation:workload` does not share a sidecar.

### Active and retained capacity

A sidecar defaults to 128 active deployments and 256 retained records. Operators
set `SIDECAR_MAX_ACTIVE_DEPLOYMENTS` (positive integer) and
`SIDECAR_MAX_RETAINED_DEPLOYMENTS` (non-negative integer) at sidecar startup;
zero refuses retention and triggers cleanup as described below. The combined
limit cannot exceed 2,048 records. Startup fails if stored inventory
exceeds either configured limit, without deleting records to make it fit.
Starting, running, stopped-but-unconfirmed, and cleanup copies
whose removal is unconfirmed keep their reservation. Confirmed removal frees the
active slot even when releasing the provider's hold is still pending or failed.
The complete `hello` advertises both limits and reports both pools; `welcome`
routes the active copies. The Hub rejects a `hello` that omits its capacity.
The complete `hello` must fit the 64 MiB WebSocket frame-size limit as well as
the record ceiling. Hello errors use the same 4,096-character limit as individual
stop reports.
The Hub reads capacity and retained-copy proofs together from the current
connection and uses the advertised active limit for placement and probe adoption.
Restart resumes active records without stopped or teardown marks and leaves
retained records unspawned.

A full sidecar rejects a new deployment before accepting or starting it. Its
`agent.deploy.error` carries `error: { code: "capacity_full", message }`, which the Hub records
as `sidecar_deployment_rejected` and releases through ordinary cleanup. An error
with code `deployment_failed` or a missing reply still leaves initialization uncertain.
Refusal still requires cleanup of history the Hub may have restored before sending
the deploy frame; it does not confirm removal.

For a terminal run, the Hub requests `workflow.control` with `action: retain`.
A reconnect reporting a terminal copy live triggers this request immediately
after welcome; the lifecycle sweep shares an in-flight request and retries failures.
The sidecar stops execution and durably records `retention: kept` before
acknowledging the freed active slot. Reconnect replays that decision in hello.
The lifecycle sweep requests retention in the background, with at most eight
requests in flight per sidecar and one per allocation. Copies already awaiting
a request or waiting for room on their sidecar are excluded from later retention
scans. A slow sidecar does not consume another sidecar's request slots. Expiry,
accepted-history recovery, and confirmed retention refusals remain eligible.
Waiting for a retention acknowledgement does not hold a lifecycle worker; a
later sweep acts on a refusal.
The Hub matches confirmation to the allocation's accepted generation and counts
unconfirmed copies conservatively. Admission and probe adoption share the same
sidecar lock, so simultaneous placements cannot reserve the same last slot.
Provisioner holds continue to own both active and retained copies until release.

New placements and probe adoption are blocked while the sidecar's inventory is
unknown, including during reconnect. They wait for a complete hello outside any
database transaction, then repeat the locked capacity check. The connection
deadline is shared across retries; probe adoption expiry follows probe cleanup.
Existing workflows and cleanup continue during the wait.
Reused placements release their reconciliation slot while waiting and resume
the accepted provisioner result on the same Hub without repeating `ensure`.
Inventory must arrive within the original connection deadline. While that Hub
still holds the accepted result, expiry fails the deployment and releases its
provider hold instead of starting another placement. A timely hello can still be
used by a delayed reconciliation claim. A Hub restart loses that in-memory result
and uses interrupted-provisioning recovery, which may replace the
placement.

When the kept pool is full, the sidecar durably records `retention: refused`
for the newly stopped copy and logs an error. Existing retained copies stay.
The Hub finishes recovering accepted history, records
`sidecar_retention_limit_exceeded` on the allocation, and releases the copy
without changing its workflow outcome. A cleanup failure can replace the
allocation reason, logging the prior reason before doing so. Successful cleanup
after a disconnect timeout clears that timeout; the prior allocation reason
remains in the logs. History still only on that sidecar can
be lost with the copy, as under ordinary release. The sidecar's refusal survives
retry and restart even if a kept slot becomes available later.

An uncertain marker write keeps its reservations; failed deletion keeps the
copy counted until removal is durable. Unconfirmed retention writes do not free
active capacity. Recovery of history already accepted by the Hub is retried
under the release policy; retention does not import later sidecar commits.

Retain confirms the durable copy after the run is terminal. The Hub's
terminal-history cutoff still applies: it waits for earlier receives to settle,
then treats the history already accepted as final. Later sidecar commits remain
local and are not imported into an ended run. Stopping a terminal copy that
reconnects after retention expires uses the same cutoff. A live workflow stop
still waits for the worker's reported history before recording the terminal
outcome.

Kept copies with confirmed finality are skipped until release or accepted-history
recovery is due. Failed retain requests back off from five seconds to five
minutes, without extending the release deadline. Disconnected copies wait for
reconnect; their release and disconnect policies still apply.
Deployment-record durability requires the [sidecar storage guarantees](../apps/sidecar/README.md).

A sidecar reads its adapter manifest only when it starts. A provisioner that
answers `ensure` with a sidecar it already runs must return one that booted
with the `adapterManifest` the request carries. This rule also applies when the
provisioner retries an `ensure` for the same generation. The Hub records a
digest of the manifest with each probe and allocation generation and with each
sidecar identity it mints. It accepts a reused sidecar only when the sidecar's
digest matches the request's. Thus the Hub refuses reuse across a manifest
change. A deployment that adopts its probe's sidecar goes through the same
check, which it passes unless the sidecar was retired, because the same call
created that probe under the same manifest. A sidecar
recorded before the Hub kept these digests never matches. A manifest change applies to new placements only: work that
runs stays on its sidecar.

Work placed on a sidecar a provisioner already runs gets the same 2 minutes to
connect as a new sidecar, even while that sidecar is restarting: after that a
probe fails, and a deployment is failed and has to be started again, which
costs little since it has not run yet.

### Provisioning retries

Provisioning and parked restoration allow at most 10 attempts for retryable
failures. Once a first placement is accepted, missing the connection deadline
fails it with `sidecar_connect_failed` instead of starting another attempt.
Binding an identity spends an attempt before `ensure`, even if interrupted.
Cleanup and Hub restarts preserve the count. Only replacing a parked worker
that completed initialization resets the count and backoff; replacements that
never connect keep spending the same budget.

Retryable provisioning failures back off for 1, 2, 4, 8, 16, then 30 seconds,
independently of cleanup attempts. Old-sidecar reconnects preserve this delay.
Exhaustion fails the workflow with `sidecar_provisioning_retry_exhausted` unless
cancellation is pending and no deployment remains. Outstanding capacity is released
through the cleanup policy below. The failure includes the last cause
and is recorded before replacement cleanup. That cleanup continues under the
same generation, preserving its confirmations, attempt count and disconnect
deadline. No eleventh attempt is started.

### Release and replacement cleanup

Release requires both the provisioner's hold to be released and deployment
cleanup to be confirmed. A successful `destroy` must report `cleanup: confirmed`
when the worker and local state cannot return, or `cleanup: required` when the
sidecar must acknowledge removal. Repeated calls must preserve that answer
until removal is confirmed. A never-accepted ensure needs no sidecar
acknowledgement because it could not have received a deployment frame.

A provider must not permanently stop the only cleanup endpoint, preserve its
restorable deployment state, and return `cleanup: required`. A stopped process
alone does not prove that its deployment state cannot return. Similarly, an
allocation missing from an in-memory map after a provider restart is unknown,
unless the provider has independent proof that its worker and state are gone.

The reconciler owns acknowledged undeploys and requests connected-sidecar
cleanup independently of provider availability. Provider destruction can still
confirm removal when sidecar cleanup fails. Fencing preserves cleanup access
without workflow routes, and a lost acknowledgement can be retried after the
record is gone.

The allocation's `deployment_cleanup_confirmed` flag records removal for the
current cleanup generation. The Hub persists it after an undeploy acknowledgement
or confirmed provider destruction, under the current generation and lease. Later
attempts, including after a Hub restart, skip sidecar cleanup once this proof is
recorded; the provider's hold must still be released. Starting a new release or
replacement, or binding replacement capacity, resets the flag, except that
releasing a replacement preserves its existing copy-removal confirmation. A
replacement may still need to remove the old copy of a parked deployment even
though its ensure acceptance has already been cleared. Removal
defaults to unconfirmed; a failed write never counts as confirmation, while a
lost commit response can be recovered by reading the row on the next attempt.

Binding replacement capacity advances the generation again after cleanup, so a
delayed hello carrying the old cleanup binding cannot displace the replacement.

The independent `provider_hold_released` flag records any successful `destroy`
result under the current generation and lease, even when cleanup is still required.
It survives retries and Hub restarts and resets with a new release, replacement,
or capacity binding; releasing a replacement preserves it. Once recorded, provider
failures cannot spend the cleanup budget or prevent release after the sidecar
confirms removal. While removal is unconfirmed, the Hub can still ask the provider
for destruction confirmation. A release falls back to sidecar cleanup or the
disconnect wait if its provider is unavailable. Replacement also needs a provider
for its next placement and follows the provider-availability grace below.

Release and replacement use the same cleanup limits. A new release or replacement
starts a fresh attempt budget; retries, reconnects and giving up an exhausted
replacement preserve the remaining budget.
When the provider has released its hold but cleanup still needs a disconnected
sidecar, the allocation stays `releasing` or `replacing` without spending cleanup attempts.
It waits until reconnect or a durable deadline based on `maxDisconnected`.
An initialized deployment's disconnect deadline survives release;
otherwise the first observed cleanup wait starts the window, including cleanup
of a parked deployment's old copy during replacement. Repeated checks and
Hub restarts do not extend it. Releasing during replacement preserves the same
cleanup deadline. A current reconnect clears the disconnect deadline with its
reconciliation wake, so a later disconnection gets a new
window even if the readiness or cleanup pass has not run yet. The initial
deployment's first-connect deadline stays until initialization completes.

Expiry records `sidecar_cleanup_disconnect_timeout`, logs an error, and keeps
the reservation and cleanup binding. If replacement cleanup times out or exhausts
its retry budget, the still-live workflow fails and no replacement is provisioned.
A reconnect before failure preserves `replacing` and allows replacement after
confirmed cleanup. A returned cleanup connection moves only
this failure back to `releasing`, preserving its attempt count. The workflow's
terminal outcome stays unchanged and it regains no routes; the reconciler owns
the acknowledged undeploy.
The repair sweep retries a missed reconnect write for a parked release or replacement
or a disconnect-timeout failure while the cleanup connection is present. A parked
cleanup wait is woken only while its deadline schedule and attempt count are unchanged
and no active reconciliation lease owns it. Repeated notifications cannot reset the attempt budget
or reopen exhaustion or permanent provider rejection. Failed provider calls while
the hold remains unreleased and attempted undeploys spend the retry budget,
including a request whose connection disappears before its acknowledgement. Provider confirmation
of removal completes cleanup even while the sidecar is disconnected.
Binding synchronization and identity lookup failures retry without spending
cleanup attempts, with a five-minute grace per Hub process. Sending the request
or confirming removal clears this grace; disconnects and reconnects do not.
Expiry records `sidecar_cleanup_preparation_timeout`
and requires operator recovery. Hub restart resets the grace. Failed provider
calls for an unreleased hold still count if sidecar preparation also fails.

If a provisioner needed for provisioning, replacement, or releasing an outstanding
hold is missing from the Hub's plugin registry, or its version or binding differs
from the recorded provider, the Hub gives the configuration five minutes to recover.
Provider availability is checked only for provider operations. Readiness and
initialization of an allocated worker do not require a provisioner, so a missing
provider cannot fail an otherwise healthy deployment after Hub restart.
The Hub keeps this deadline in memory per allocation and checks every 30 seconds.
Repeated checks and reconnects do not extend it or spend provisioning or cleanup
attempts. Finding the matching provider clears the deadline and resumes work.
A Hub restart starts a fresh five-minute window when it next observes the missing
provider; this grace period has no durable database state.
It is a best-effort limit per Hub process, not a cluster-wide deadline. If multiple
Hubs claim the same allocation, each observes its own grace window.

If the provider is still unavailable at expiry, the Hub records
`provisioner_unavailable` and fails the live workflow unless cancellation is pending
and no deployment remains. Unfinished cleanup becomes `destroy_failed`, preserving
its cleanup binding and removal proof; when no infrastructure obligation remains,
the allocation becomes `failed`.
Restoring the provider does not automatically reopen either failure.
Connected-sidecar cleanup still runs before checking the provider. A release whose
hold was already released does not need the provider and can finish sidecar cleanup
under the attempt and disconnect limits. Actual failed undeploys still
spend cleanup attempts during the provider grace period and may exhaust that
budget before the configuration deadline.

Provider and sidecar failures share `maxCleanupAttempts` (10 by default).
Exhaustion records `destroy_failed` with `sidecar_cleanup_retry_exhausted` and
stops polling. Exhaustion and permanent provider rejections both keep a cleanup
binding and generation fence for inventory reconciliation, including retained-copy
proofs. Neither reconnect nor Hub restart reopens them, and their reported copies
are not removed through the hello orphan-cleanup path. A copy whose removal or
retained storage is confirmed does not occupy an active slot; the failed provider
obligation remains recorded independently. Unconfirmed copies keep their active
reservation. Both failures require operator recovery; there is no in-product
action to retry cleanup. After confirming outside the Hub that the copy and its
provider hold are gone, a principal with the run's `recover` action releases the
reservation through `POST /api/tenants/:tenantId/workflows/runs/:runId/capacity/recover`.
The Hub does not verify that claim and records who made it. Reconnects resume
disconnected waits immediately but preserve backoff after counted failures.
If neither undeploy nor provider destruction stopped the copy, it may remain
running without Hub routes until cleanup resumes after a disconnect timeout
or an operator stops it after another cleanup failure.

These bounds intentionally give up automatic convergence with later infrastructure
changes. If A times out before B releases the last hold on their shared sidecar,
B can confirm destruction while A remains failed: the Hub no longer polls A's
provider, and the destroyed sidecar cannot reconnect. External removal after a
timeout has the same limitation. A successful removal whose final acknowledgement
is lost can also exhaust retries and leave a failed record after the copy is gone.
Such records require operator recovery. A disconnect deadline limits Hub waiting;
it neither proves removal nor stops an unreachable worker.

Cleanup retries log warnings without replacing the allocation's release reason.
Exhaustion and permanent rejection log errors and record the cleanup outcome on
the allocation. When release cleanup fails a live deployment, its original workflow or
allocation reason takes precedence for the workflow failure. Replacement cleanup
failure records the cleanup reason on the still-live workflow. Dispatch abandonment
caused by that failure records the cleanup error separately.
Successful release after a reconnect clears the disconnect-timeout marker.
Detailed cleanup history is kept in logs.

## Tenant policy

Tenant configuration uses the same rules:

```ts
const config: TenantConfig = {
  sidecarPlacement: {
    capabilities: [
      { capability: "network:outbound", effect: "block" },
      { capability: "runtime:posix", effect: "block" },
    ],
  },
};
```

Every policy in the tenant ancestry is enforced independently. Workflows and
child tenants may add constraints but cannot override an ancestor's policy.

## Probe policy

A Hub composition may add provider-neutral requirements for the temporary
capacity that evaluates workflow source code:

```ts
await createHubServer({
  sidecarProvisioners,
  probeSidecarProvisioners,
  probeSidecarCapabilityRules: [
    { capability: "isolation:workload", effect: "require" },
    { capability: "network:outbound", effect: "block" },
  ],
});
```

Probe rules are enforced independently from tenant policy and apply only to
probe provisioner selection. They do not become workflow requirements. If the
selected probe provisioner does not satisfy the final workflow requirements,
the Hub destroys the probe capacity and creates the deployment through the
final selected provisioner.

Probe and deployment provisioners are configured as separate lists. The same
provisioner may appear in both lists, which permits the Hub to adopt matching
probe capacity for the deployment.

Capabilities describe guarantees, not vendors. A sandbox-backed provisioner
can declare `isolation:workload` and a more specific mechanism such as
`isolation:microvm`; policies should not name the provisioner implementation.

## Selection

The Hub resolves inherited tenant policy before selecting probe capacity. After
probing and freezing the workflow, it:

1. Reads the folded workflow requirements.
2. Combines those requirements with the already-resolved tenant policy.
3. Evaluates every configured deployment provisioner against its declared guarantees.
4. Fails when no provisioner matches.
5. Passes the non-empty matching set to the configured chooser.
6. Stores the selected provisioner binding for reconciliation and cleanup.

The default chooser selects the first match in registration order. A Hub
composition may provide an asynchronous chooser for probe capacity, deployment
capacity, or both to implement another policy such as round-robin selection.
