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

Capabilities describe guarantees, not vendors. A sandbox-backed provisioner
can declare `isolation:workload` and a more specific mechanism such as
`isolation:microvm`; policies should not name the provisioner implementation.

## Selection

After probing and freezing the workflow, the Hub:

1. Reads the folded workflow requirements.
2. Resolves inherited tenant policy.
3. Filters provisioners by their declared guarantees.
4. Uses the configured default when it matches, or the only remaining match.
5. Fails when no provisioner matches or selection is ambiguous.
6. Stores the selected provisioner binding for reconciliation and cleanup.

Plugin registration order is never a tiebreaker.
