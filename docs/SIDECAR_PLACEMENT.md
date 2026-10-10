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
`isolation:workload` does not share a sidecar. A sidecar hosts at most
128 deployments, the most its `hello` can report: the Hub treats placing
another on a full sidecar like any placement it cannot accept, and the sidecar
refuses a deploy past it. A sidecar that restarts with more run records than
that, such as those of self-terminated deployments the Hub has since failed,
restores only up to the limit and keeps the rest unspawned. It goes through
the records in a fixed order, so a boot over the same records leaves out the
same ones. It reports each of them stopped after every `welcome`, outside the
`hello`: the Hub fails a current deployment left out and undeploys a stale
one, which deletes its run record.

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

A permanent provider rejection records `destroy_failed`, stops reconciliation,
and keeps a cleanup-only binding for inventory and operator recovery. Reconnect
does not issue an untracked undeploy for such a copy. The active reservation
remains only while removal is unconfirmed.

If the provisioner is missing from the Hub's plugin registry, or its version or
binding differs from the allocation's recorded provider, the Hub logs that exact
configuration problem. A cleanup pass with no sidecar failure waits 30 seconds
without spending an attempt. Release remains pending until the original
matching provisioner returns and releases its hold. Connected-sidecar cleanup
still runs independently, and its confirmation survives this wait; an actual
sidecar cleanup failure still spends an attempt while the provider is missing.

Cleanup retries log warnings without replacing the allocation's release reason.

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
