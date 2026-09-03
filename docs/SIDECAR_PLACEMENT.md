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

The chooser also receives the tenant, the authenticated principal whose request
owns placement, and the effective capability policy used to filter candidates.
It does not receive user records, grants, credentials, or a snapshot of live
sidecars. This lets a provisioner consult its own principal-scoped capacity
registry without making volatile provider state part of the Hub contract.

The same placement principal is persisted on the selected allocation and passed
to every `ensure()` call, including replacement generations after Hub restart.
A deployment may also target an exact active host principal owned by that
placement principal. The target is optional; without one, a host-backed
provisioner may choose any compatible host in the owner's pool.

The Hub snapshots the effective tenant and workflow capability policy onto the
allocation. Every replacement receives that deploy-time policy rather than
re-reading mutable tenant configuration, so a retry cannot silently change the
guarantees under which the provisioner binding was selected.

A provisioner backed by pre-existing capacity owns the registration and
capability declarations for that capacity and must claim one matching slot
atomically. The Hub still fixes the provisioner binding before `ensure()`;
availability checks performed by a chooser are advisory, and a rejected
`ensure()` follows the normal retry or terminal-failure lifecycle rather than
selecting another provisioner.

Hub compositions can register host-backed provisioners without exposing host
connections to the plugin:

```ts
await createHubServer({
  hostCapacityProvisioners: [
    {
      id: "ios-host",
      bindingFingerprint: "ios-host:v1",
      capabilities: [{ capability: "runtime:ios-jsc-v1", state: "available" }],
    },
  ],
});
```

Each configured provisioner advertises its operator-defined outer guarantees.
Its private broker then checks the selected live host's current declarations
against the persisted placement policy before claiming it.
