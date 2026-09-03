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
    return { kind: "destroyed" };
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
probe capacity for the deployment. Claimed host capacity is adopted only when
that exact host's recorded capabilities also satisfy the final workflow policy.

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

The chooser also receives the tenant, the authenticated principal whose request
owns placement, and the effective capability policy used to filter candidates.
It does not receive user records, grants, credentials, or a snapshot of live
sidecars.

The same placement principal is persisted on the selected allocation and passed
to every `ensure()` call, including replacement generations after Hub restart.
A deployment may also target an exact active host principal owned by that
placement principal or shared with it through a `host:{id}` / `use` grant. The
target is optional. The request's `targetHostPrincipalId` is the host's
principal id (`prn_...`). The grant resource uses the host record's `id`
(`hst_...`): `host:hst_...` with action `use`.

The Hub snapshots the effective tenant and workflow capability policy onto the
allocation. Every replacement receives that deploy-time policy rather than
re-reading mutable tenant configuration, so a retry cannot silently change the
guarantees under which the provisioner binding was selected.

Every provisioner receives the same `existingSidecars` service with each
`ensure()` and `destroy()` call. A provisioner may atomically claim matching
capacity accessible to the placement principal and supply its own host
selection policy. Untargeted requests may fall back to creating new capacity:

```ts
async ensure(request, { existingSidecars }) {
  const existing = await existingSidecars.claim(request, {
    chooseHost(candidates) {
      const index = Math.floor(Math.random() * candidates.length);
      return candidates[index]?.hostId ?? null;
    },
  });
  return existing ?? createNewCapacity(request);
}

async destroy(request, { existingSidecars }) {
  const released = await existingSidecars.release(request);
  return released ?? destroyCreatedCapacity(request);
}
```

The Hub still fixes the provisioner binding before `ensure()`. Within `claim()`,
the required `chooseHost` callback receives only eligible, unreserved hosts,
represented by `hostId`, `hostPrincipalId`, and `capabilities`. The provisioner
returns an offered host id or `null` to decline existing capacity. There is no
Hub-defined host ordering. The callback may be asynchronous; grants,
credentials, and control-session details remain inside the Hub.

Candidate availability is advisory. The Hub rechecks access, capabilities,
and the current session after selection, then attempts an atomic reservation.
If the host became unavailable, it calls the chooser with the remaining
eligible candidates. Each host is attempted at most once per claim call.
When an untargeted request exhausts its candidates or the chooser declines,
`claim()` returns `null`. An existing assignment is resumed without invoking
the chooser again, and uncertain assignment acknowledgements throw into
recovery instead of selecting another host.

An exact `targetHostPrincipalId` restricts the offered candidates to that host.
If it cannot be claimed, `claim()` returns a retryable rejection rather than
`null`, preventing the fallback in the example above. Before accepting a
targeted deployment's provisioning result, the Hub verifies an acknowledged
assignment for the exact operation, generation, sidecar identity, and host
principal. A provisioner that accepts other capacity is sent through the
uncertain-provisioning cleanup path.

The request's `allocationId` is an opaque `sidecar_operation` identity. The Hub
resolves whether that operation currently belongs to a probe or deployment;
provisioners do not receive or infer that distinction. When claimed probe
capacity is eligible for adoption, the same operation identity continues into
the deployment. The Hub rechecks the claimed host's recorded capabilities
against the final workflow policy before adopting it.
