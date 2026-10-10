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
  apiVersion: 1,
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

An omitted capability is unknown. It does not mean blocked. Provisioners decide
internally whether to create, isolate, share, or reuse their backing capacity.
To place a probe or deployment on a sidecar it already runs, a provisioner
answers `ensure` with that sidecar's id instead of starting capacity for the
identity the request carries. The Hub accepts that sidecar only while it still
hosts another probe or allocation of the same provisioner binding, counting
work being released or replaced, and otherwise rejects the placement with
`sidecar_reuse_rejected`. It counts its holds on a sidecar, one per
allocation id it answered with that sidecar, releases one per `destroy`, and
stops the sidecar once none remain. That final shutdown must remove its restorable
deployment state and report `cleanup: confirmed`; a provider returning
`cleanup: required` must keep a sidecar cleanup path available. It keeps its holds
durably, since the Hub never re-announces them after a restart.

A provisioner's declared guarantees, such as `isolation:workload`, must still
hold for work that shares a sidecar. The stock sidecar does not isolate the
work it hosts from each other or from
itself: every workflow child runs as the sidecar's OS user over its data
directory, so work that shares one can read each other's keys and run state,
and any child can read the sidecar's own environment, its Hub token and
credential encryption key included. A provisioner that declares
`isolation:workload` does not share a sidecar.

### Active and retained capacity

A sidecar reserves at most 128 active deployments and keeps at most 256
retained records. Starting, running, stopped-but-unconfirmed, and cleanup copies
whose removal is unconfirmed keep their reservation. Confirmed removal frees the
active slot even when releasing the provider's hold is still pending or failed.
The complete `hello` reports both pools,
up to 384 records; `welcome` routes at most 128. Restart resumes active records without stopped or teardown marks and leaves
retained records unspawned.

A full sidecar rejects a new deployment before accepting or starting it. Its
`agent.deploy.error` carries `error: { code: "capacity_full", message }`, which the Hub records
as `sidecar_deployment_rejected` and releases through ordinary cleanup. An error
with code `deployment_failed` or a missing reply still leaves initialization uncertain. A refused
request does not imply that the allocation's previously staged state is absent.

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
and uses the existing interrupted-provisioning recovery, which may replace the
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
under the existing release policy; retention does not import later sidecar commits.

Retain confirms the durable copy after the run is terminal. The Hub's existing
terminal-history cutoff still applies: it waits for earlier receives to settle,
then treats the history already accepted as final. Later sidecar commits remain
local and are not imported into an ended run. A live workflow stop still waits
for the worker's reported history before recording the terminal outcome.

Kept copies with confirmed finality are skipped until release or accepted-history
recovery is due. Failed retain requests back off from five seconds to five
minutes, without extending the release deadline. Disconnected copies wait for
reconnect; their existing release and disconnect policies still apply.
Deployment-record durability requires the [sidecar storage guarantees](../apps/sidecar/README.md).

Work placed on a sidecar a provisioner already runs gets the same 2 minutes to
connect as a new sidecar, even while that sidecar is restarting: after that a
probe fails, and a deployment is failed and has to be started again, which
costs little since it has not run yet.

### Release cleanup

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
replacement, or binding replacement capacity, resets the flag. Existing rows
default to unconfirmed; a failed write never counts as confirmation, while a
lost commit response can be recovered by reading the row on the next attempt.

When the provider has released its hold but cleanup still needs a disconnected
sidecar, the allocation stays `releasing` without spending cleanup attempts.
It waits until reconnect or a durable deadline based on `maxDisconnected`.
An existing disconnect deadline for an initialized deployment survives release;
otherwise the first observed cleanup wait starts the window. Repeated checks and
Hub restarts do not extend it. A current reconnect clears the disconnect
deadline with its reconciliation wake, so a later disconnection gets a new
window even if the readiness or cleanup pass has not run yet. The initial
deployment's first-connect deadline stays until initialization completes.

Expiry records `sidecar_cleanup_disconnect_timeout`, logs an error, and keeps
the reservation and cleanup binding. A returned cleanup connection moves only
this failure back to `releasing`, preserving its attempt count. The workflow's
terminal outcome stays unchanged and it regains no routes; the reconciler owns
the acknowledged undeploy.
The existing repair sweep retries a missed reconnect write for a parked release
or a disconnect-timeout failure while the cleanup connection is present. A parked
release is woken only while its deadline schedule and attempt count are unchanged
and no active reconciliation lease owns it. Repeated notifications cannot reset the attempt budget
or reopen exhaustion or permanent provider rejection. Failed provider
calls and attempted undeploys still spend the retry budget, including a request
whose connection disappears before its acknowledgement. Provider confirmation
of removal completes cleanup even while the sidecar is disconnected.

If the provisioner is missing from the Hub's plugin registry, or its version or
binding differs from the allocation's recorded provider, the Hub logs that exact
configuration problem. A cleanup pass with no sidecar failure waits 30 seconds
without spending an attempt. Release remains pending until the original
matching provisioner returns and releases its hold. Connected-sidecar cleanup
still runs independently, and its confirmation survives this wait; an actual
sidecar cleanup failure still spends an attempt while the provider is missing.

Provider and sidecar failures share `maxCleanupAttempts` (10 by default).
Exhaustion records `destroy_failed` with `sidecar_cleanup_retry_exhausted` and
stops polling. Exhaustion and permanent provider rejections both keep a cleanup
binding and generation fence for inventory reconciliation, including retained-copy
proofs. Neither reconnect nor Hub restart reopens them, and their reported copies
are not removed through the hello orphan-cleanup path. A copy whose removal or
retained storage is confirmed does not occupy an active slot; the failed provider
obligation remains recorded independently. Unconfirmed copies keep their active
reservation. Both failures require operator recovery; there is no in-product
action to retry cleanup or clear an unconfirmed reservation. Reconnects can
accelerate pending attempts only while the retry budget remains.
If neither undeploy nor provider destruction stopped the copy, it may remain
running without Hub routes until cleanup resumes after a disconnect timeout
or an operator stops it after another cleanup failure.
Cleanup retries log warnings without replacing the allocation's release reason.
Exhaustion and permanent rejection log errors and record the cleanup outcome on
the allocation. When cleanup fails a live deployment, its original workflow or
allocation reason takes precedence for the workflow failure. Dispatch abandonment
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
