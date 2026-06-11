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
- `ipc/`, `supervisor/`, `child/`, `bin/` — forthcoming. These pieces
  ferry control commands into the workflow runtime process, ferry
  inference events back out, and own the per-deployment lifecycle.
  When they land, the supervisor will accept dependency-injected
  bindings (mail-bus bindings, signing-key callback, RepoStore,
  subprocess spawner) so any host wires it the same way.

Public surface (the package barrel re-exports these):

- `createWorkflowRunRepoStore` — the production `RepoStore` adapter.
- `createWorkflowRunBlobSubstrate` — the production `BlobSubstrate`
  adapter with 1 MiB inline-vs-blob spill threshold.
- `createWorkflowStepInvoker` — the production `StepInvoker` adapter.
- `createWorkflowSpawnChild` — the production `SpawnChildWorkflow`
  adapter.

Plus the relevant options and callback types (`StepEnvBase`,
`RunChildWorkflow`, `ChildTerminalStatus`, etc.).
