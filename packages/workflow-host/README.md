# @intx/workflow-host

Production-host implementations for the abstract
`WorkflowRuntimeEnv` from `@intx/workflow`.

The host is the part of the workflow runtime that knows how to
talk to real infrastructure: a `workflow-run` repo, the
substrate's per-repo lock, an inference harness, child-workflow
spawning. It is intentionally host-agnostic at the import layer —
nothing in the package name or public API presumes a specific
deployment context (sidecar, CLI, integration test harness, an
out-of-process supervisor variant). Each is expected to instantiate
the same package with its own dependency-injected bindings rather
than fork another app.

The package is organized along the abstract pieces it implements:

- `adapters/` — concrete implementations of the four
  `WorkflowRuntimeEnv` adapter slots. `repo-store.ts` wraps the
  workflow-run substrate (with `seq_conflict` translated to a
  single-writer-invariant programming error). `blob-substrate.ts`
  spills above 1 MiB. `step-invoker.ts` constructs an in-process
  agent per step. `spawn-child.ts` resolves a `definitionRef` and
  delegates the spawn to a runtime-supplied callback.
- `seams/` — the substrate-shaped seams. `scheduler.ts` runs both a
  startup recovery walk and a live `subscribeKind` loop so a
  `TimerSet` committed by an active workflow process fires without
  waiting for a process restart. `signal-channel.ts` funnels live
  `SignalReceived` commits into the matching awaiter, with resume
  rehydration consulting `unconsumedSignals` so a signal that
  arrived while offline replays before live subscription begins.
- `ipc/` — control and event channel implementations the
  supervisor wraps. Threat model lives at the top of
  `ipc/index.ts`; the supervisor uses these primitives directly.
- `supervisor/` — the per-deployment supervisor. See the next
  section.
- `child/`, `bin/workflow-child` — the workflow-process child
  entry function and its package-owned binary. The binary path is
  the one the supervisor's `subprocessSpawner` invokes. The child
  entry function lands alongside this binary in a separate commit;
  the supervisor here spawns the binary and does not depend on
  the function's body.

Public surface (the package barrel re-exports these):

- `createWorkflowRunRepoStore` — the production `RepoStore` adapter.
- `createWorkflowRunBlobSubstrate` — the production `BlobSubstrate`
  adapter with 1 MiB inline-vs-blob spill threshold.
- `createWorkflowStepInvoker` — the production `StepInvoker` adapter.
- `createWorkflowSpawnChild` — the production `SpawnChildWorkflow`
  adapter.
- `createWorkflowSupervisor` — the per-deployment supervisor
  factory. See "Supervisor" below for the bindings shape.
- `assembleCredentialsSnapshot` — per-step grant assembly used by
  the supervisor at spawn time and on every `grants-updated` push.
- `commitCancelRequested` — low-level Q3 `CancelRequested` commit
  primitive the supervisor invokes for every cancellation origin.

Plus the relevant options and callback types (`StepEnvBase`,
`RunChildWorkflow`, `ChildTerminalStatus`,
`WorkflowSupervisorBindings`, `SubprocessSpawner`,
`MailBusBindings`, `PrincipalSigner`, `SignedPayload`, etc.).

## Supervisor

`createWorkflowSupervisor(bindings)` returns a per-deployment
supervisor that owns one workflow-process child for the lifetime of
the deployment. The supervisor is host-agnostic library code; the
host supplies its own concrete bindings.

### `WorkflowSupervisorBindings`

The constructor argument shape:

- `repoStore` — substrate-shaped `RepoStore` handle. The supervisor
  reads grants and commits events through this one handle. Per-
  principal write-sites pass the principal kind explicitly; there
  is no per-principal `RepoStore` view.
- `signAsPrincipal: (kind, payload) => SignedPayload` — host-owned
  per-principal signing callback. The supervisor never holds the
  principal's private key; it asks the host to mint a signature
  under the named principal's identity. Today the only kind the
  supervisor signs as is `"supervisor"`, used for every
  CancelRequested origin in the Q3 map except `hub-admin`.
- `mailBus` — minimal `MailBusBindings` shape: `registerAddress`,
  `unregisterAddress`, `subscribeMailForAddress`. The supervisor
  registers the deployment's address at spawn time, subscribes to
  inbound mail, and unregisters on teardown.
- `subprocessSpawner` — invoked once per spawn to launch the
  package-owned `bin/workflow-child` script. Production wires it
  against `Bun.spawn`; tests inject a deterministic mock.
- `binaryPath` — absolute path to the package-owned binary the
  spawner invokes (resolved by the host via
  `require.resolve` / `import.meta.resolve` against the workflow-
  host package).
- `substrateEnv`, `workflowRunRepoId`, `workflowRunRef`,
  `deploymentId`, `deploymentMailAddress`, `readPrincipal`,
  `deriveStepAddress`, `deriveStepRepoId?`, `ipcKeyPairFactory?` —
  per-deployment configuration the supervisor needs in its closure
  state.
- `trivialLaunch: (bindings) => Promise<void>` — host-injected
  callback the supervisor invokes on the trivial branch of
  `deploy(frame)`. See "Deploy routing" below for the trivial-
  branch invariants the callback runs under.

### Deploy routing

`deploy(frame)` is the single ingress for inbound `agent.deploy`
frames. The supervisor decides between two branches and the host
does not re-decide:

- **Trivial branch (1-step workflows).** The supervisor calls
  `bindings.trivialLaunch(frame)` directly. No IPC channel opens.
  No workflow-process child spawns. No mail-bus subscription
  registers. No workflow-run event is emitted; `signAsPrincipal`
  is not invoked. `getCredentialsSnapshot()` continues to return
  `null`. The trivial branch is a true passthrough -- the host
  callback owns the entire deploy, and the supervisor's other
  bindings stay inert.
- **Multi-step branch (`steps.length >= 2`).** The supervisor
  routes through the same lifecycle `spawn(opts)` runs: per-step
  `agent-state` repo provisioning, key minting, child spawn via
  `subprocessSpawner`, mail-bus registration, IPC handshake, and
  `credentialsSnapshot` assembly. The multi-step branch's body
  lands as `agent.deploy` frames are extended to carry a
  `WorkflowDefinition`; the routing seam exists today.

The `agent.deploy` wire frame currently carries only a
`HarnessConfig` (no workflow definition), so every frame today is
trivial. The supervisor codifies the seam now so the frame-format
extension lands as a pure data-shape change.

The sidecar's production wiring lives at
`apps/sidecar/src/workflow-host-wiring.ts`:
`createSidecarDeployRouter` constructs a fresh per-deployment
supervisor on every inbound frame whose `trivialLaunch` closes
over `SessionManager.provisionAgent` plus the hub-pairing-key
recording the legacy handler performed inline. The bytes flowing
through the deploy-flow integration test path stay bit-identical
to the pre-supervisor surface.

### Lifecycle

`spawn(opts)` performs the IPC handshake (mint channelId, mint HMAC
key, mint IPC Ed25519 keypair, build spawn-time env, invoke the
spawner, wait for the child's `ready` frame), assembles the
`credentialsSnapshot` from each step's `agent-state` repo,
registers the deployment's mail address, and begins forwarding
inbound mail as `trigger.fire` control frames. The IPC private key
never leaves the supervisor's closure — only the public key (as
`HOST_PUBKEY`) ships in spawn-time env.

`requestCancel(opts)` signs and commits a `CancelRequested` event
through `signAsPrincipal("supervisor", ...)` for every origin in
the Q3 map. The `self`-origin case carries the workflow-process's
stated reason; the supervisor wraps it into the same supervisor-
signed shape as the operator and drain origins.

`shutdown()` unregisters the mail address, kills the child, and
disposes subscriptions.

`drain` and `recycle` are stubs in this commit; the full
implementations land with the drain controller and recycle paths
respectively.

### Host wiring

A host that wants to instantiate a supervisor constructs the
bindings against its own infrastructure. The reference
implementation for the in-tree sidecar lives at
`apps/sidecar/src/workflow-host-wiring.ts` and is intentionally
thin — anything that would benefit a future alternative-sidecar
implementation belongs inside this package, not in the wiring.
