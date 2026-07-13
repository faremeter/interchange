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
- `child/` — the workflow-process child entry function
  (`runWorkflowChild`) plus the process-boundary helper
  (`runWorkflowChildFromProcessEnv`) hosts use to compose their own
  thin binary. The package no longer ships a `bin` entry; each host
  owns the binary that wires its substrate factory into the
  process-shaped boundary. See "Child Entry" and "Hosting the
  workflow-process child" below.

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
  host-owned `bin/workflow-child` script. Production wires it
  against `Bun.spawn`; tests inject a deterministic mock.
- `binaryPath` — absolute path to the host-owned binary the
  spawner invokes (resolved by the host via
  `require.resolve` / `import.meta.resolve` against the host's own
  package, `@intx/<host>`).
- `substrateEnv`, `workflowRunRepoId`, `workflowRunRef`,
  `deploymentId`, `deploymentMailAddress`, `readPrincipal`,
  `deriveStepAddress`, `deriveStepRepoId?`, `ipcKeyPairFactory?` —
  per-deployment configuration the supervisor needs in its closure
  state.

### Deploy routing

The sidecar's deploy router is the single ingress for inbound
`agent.deploy` frames; its production wiring lives at
`apps/sidecar/src/workflow-host-wiring.ts` in
`createSidecarDeployRouter`. Every deploy stages through the
workflow-run substrate, and the router decides between two frame
shapes:

- **Provision-step frame (`provisionStep: true`).** The router
  primes the frame's per-step `agent-state` repo and records the
  hub key, without constructing a supervisor or spawning a child.
  The follow-up full-closure deploy pack then applies into the
  primed repo and verifies against the recorded key.
- **Workflow frame (carries a `WorkflowDefinition`).** The router
  constructs a fresh per-deployment supervisor and drives its
  `spawn(opts)` lifecycle: per-step `agent-state` repo
  provisioning, key minting, child spawn via `subprocessSpawner`,
  mail-bus registration, IPC handshake, and `credentialsSnapshot`
  assembly.

A frame carrying neither shape is rejected -- there is no
in-process deploy path.

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

## Child Entry

`runWorkflowChild` is the workflow-process child's runtime body.
Each host ships a thin entry script that wires its substrate
factory into the process boundary via
`runWorkflowChildFromProcessEnv`; tests call `runWorkflowChild`
directly with mock streams and an in-memory substrate.

### Signature

```ts
runWorkflowChild(opts: {
  env: SpawnTimeEnv;             // parsed via parseSpawnTimeEnv
  controlReader: NdjsonReader;   // supervisor -> child
  controlWriter: NdjsonWriter;   // child -> supervisor
  eventWriter: FrameWriter;      // child -> supervisor (InferenceEvents)
  bindings: RunWorkflowChildBindings;
}): Promise<RunWorkflowChildResult>
```

Every I/O stream and every substrate handle is injected. Nothing
inside the function reads `process.env` or reaches into a singleton
— the binary owns the only crossing of that boundary. Injected I/O
is the testability contract: an integration test instantiates the
child against in-memory NDJSON streams, a memory-backed substrate,
and a stub `StepInvoker`/`SpawnChildWorkflow` without ever
forking a process.

### Lifecycle

1. Open the control channel and event channel via the IPC primitives.
2. Construct a `WorkflowRuntimeEnv` from the production adapters
   (`createWorkflowRunRepoStore`, `createWorkflowRunBlobSubstrate`)
   and substrate-shaped seams (`createWorkflowHostSignalChannel`,
   plus the host-process scheduler singleton carried on
   `bindings.scheduler`).
3. Self-discover in-flight runs by enumerating `runs/<runId>/` and
   resuming any whose log lacks a terminal event.
4. Emit `ready` on the control channel.
5. Loop on `trigger.fire`, `grants-updated`, `drain`, `recycle`,
   `shutdown`, and `signal.deliver` frames.

### Authorize Closure

The child's `WorkflowAuthorizeFn` closure is backed by a
`CredentialsSnapshotRef`. A `grants-updated` control frame swaps
the snapshot in place, so subsequent steps see fresh grants
without reconstructing the env. The closure looks up the
originating step's grants by `stepId` and delegates to a
host-supplied `GrantEvaluator`.

### Placeholders

`DrainController` is a no-op placeholder in this commit; the real
controller lands separately. `recycle` is a no-op pending the
recycle path.

## Hosting the workflow-process child

`@intx/workflow-host` ships the runtime body
(`runWorkflowChild`) and the process-boundary helper
(`runWorkflowChildFromProcessEnv`) as a library. The package
itself does NOT ship a `bin` entry; each host owns the binary the
supervisor's `subprocessSpawner` invokes.

The contract is intentionally narrow:

1. **The host owns the binary.** It is typically five lines: a
   `bun` shebang, an `import` of
   `runWorkflowChildFromProcessEnv`, an `import` of the host's
   substrate factory, and an `await` of the helper. The supervisor's
   `binaryPath` binding is resolved statically by the host's wiring
   module (e.g. `import.meta.resolve("@intx/<host>/bin/workflow-child")`)
   so the path is fixed at wiring-module load time, not via runtime
   env.
2. **The host owns the substrate factory.** A `SubstrateFactory`
   is a callback that receives the typed `SubstrateFactoryEnv`
   struct -- the parsed `SpawnTimeEnv` (IPC trust anchors +
   deployment ids) plus a narrowed `substrateConfig` record carrying
   only the keys the host listed in
   `RunWorkflowChildFromProcessEnvOpts.substrateConfigKeys`. The
   factory returns `RunWorkflowChildBindings`: substrate `RepoStore`,
   principal, per-deployment repo ids, scheduler, step invoker, child
   spawner, grant evaluator. The factory consumes the typed struct,
   never `NodeJS.ProcessEnv` directly.
3. **The helper fails loudly.** A missing or malformed spawn-time
   env throws via `parseSpawnTimeEnv`; a substrate-config key the
   host listed but the supervisor did not populate throws before the
   factory runs; factory rejection and runtime-body rejection
   propagate unchanged. The helper does not catch or coerce; the
   host's binary decides the exit semantics (the convention is
   `process.exit(1)` with a stderr message on rejection).

Example host binary (`apps/<host>/bin/workflow-child`):

```ts
#!/usr/bin/env bun
import { runWorkflowChildFromProcessEnv } from "@intx/workflow-host";
import { createSubstrate } from "../src/workflow-substrate-factory";

await runWorkflowChildFromProcessEnv(createSubstrate, {
  substrateConfigKeys: ["SIDECAR_DATA_DIR" /* ... */],
}).catch((cause) => {
  process.stderr.write(
    `workflow-child: ${cause instanceof Error ? cause.message : String(cause)}\n`,
  );
  process.exit(1);
});
```

The reference in-tree implementation lives in `apps/sidecar`. An
alternative-sidecar implementer follows the same pattern: write a
substrate factory against its own infrastructure, ship a ~5-line
entry script, and resolve the `binaryPath` binding to that script
in its supervisor-wiring module.

### Scheduler adapter

The host-singleton `SchedulerHandle` returned by
`createWorkflowHostScheduler` does not match the runtime's
`Scheduler.scheduleIn` shape directly. `adaptHostScheduler(handle)`
returns the runtime-shaped `Scheduler` the substrate factory hands
to `RunWorkflowChildBindings.scheduler`. The adapter is a thin
wrap: the host scheduler's live `TimerSet` ingest already queues
the timer at commit time; `scheduleIn` returns a dispose that
forwards to `handle.cancelQueued(runId, timerId)` so a runtime
body that settles on a sibling event before the deadline cancels
the queued entry cleanly.
