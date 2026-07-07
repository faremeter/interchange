# Unified Execution Host Design

> **STATUS — implemented (the unified single-agent path is built end to end).**
>
> This document is the design of record. The unified single-agent path it
> describes is built: the sidecar spawns a child that runs a real agent for
> a workflow step with materialized tools, threads agent events up, keeps
> the agent warm across messages, and persists its conversation durably
> across a child respawn. The per-message latency hardening the design
> implies was measured and landed, save one residual substrate
> optimization tracked as a separate follow-up. The recursive per-rung
> sandbox model in the later sections remains forward-looking.
>
> References are by file path and symbol name, deliberately without line
> numbers, so the document outlives the code's line numbering.

## 1. Problem and goals

### Current state: two execution runtimes

The sidecar today runs agents through **two separate execution runtimes** that
do not share an implementation:

1. **The in-process harness runtime.** `provisionAgent` /
   `createSessionManager` in `packages/hub-agent/src/session-manager.ts`, paired
   with the concrete `HarnessBuilder` in `apps/sidecar/src/default-harness.ts`.
   This builds a long-lived, mail-driven agent: a connector reactor
   (`createHarness` in `packages/harness/src/harness.ts`) that owns its own
   `MessageTransport` subscription, holds multi-turn conversation state in
   memory, drains an INBOX continuously, and routes `connector.reply` events
   back to the transport. **This is the only thing in the system that runs a
   real agent today** — real tool composition, real inference, real multi-turn
   conversation.

2. **The spawned workflow-process child.** The supervisor
   (`packages/workflow-host/src/supervisor/supervisor.ts`) spawns a child
   (`packages/workflow-host/src/child/run-child.ts`) per deployment. The child
   provides durable, resumable, audited multi-step orchestration: an inbox
   claim-check substrate, a FIFO dispatch loop, per-message `runtimeRun`
   invocations, `discoverInFlightRuns` resume, signal/drain/recycle machinery,
   and a single-writer workflow-run repo. **But its step-invoker is a stub.**
   `apps/sidecar/src/workflow-substrate-factory.ts` wires `baseInvokeStep` /
   `childInvokeStep` to return `{ output: { reply: req.agent.id, turn: null } }`
   — no inference, no tools, the event firehose dropped. The orchestration
   machinery is real and proven; the work inside each step is placeholder.

The net effect: the durable runtime cannot run real agents, and the
real-agent runtime is not durable. INTR-209's multi-step workflows deploy,
dispatch, signal, and resume correctly but every step returns placeholder
output.

### Why unify

Maintaining two runtimes means two tool-composition paths, two storage
models, two inference wirings, two lifecycles, and a permanent fork between
"durable but fake" and "real but ephemeral." Every capability — a new tool
surface, an inference-source rotation, an audit field — has to be built twice
or it diverges. The durable execution model (inbox claim-check, resume,
audited run log) is exactly what a long-lived agent should have, and it
already exists; it is simply not connected to real agent execution.

An earlier review concluded this could not be unified because the harness is
a long-lived mail reactor and the step-invoker is a per-step
instantiate-send-teardown batch model
(`packages/workflow-host/src/adapters/step-invoker.ts`). That conclusion was
wrong. Both shapes are **incidental, not fundamental**: the child can host a
long-lived agent (no code boundary forbids `createHarness` running in the
child — it needs a `MessageTransport` and a `ContextStore`, both suppliable in
the child), and one child can host multiple runs (the dispatch loop and
`discoverInFlightRuns` already drive many concurrent runs; the
single-deployment binding is host wiring, not a substrate law). The blocker
was an architectural decision nobody had made — who owns the mailbox — not a
capability ceiling.

### What "done" means

- **One extensible, recursive execution host.** The workflow-process child is
  the single place real agents run, hosting both a long-lived single agent and a
  multi-step workflow, multiplexed. The host is **recursive** — every child is
  itself a spawner that can nest sub-children to arbitrary depth (the existing
  `childWorkflow` / `spawnChild` recursion) — and each spawn rung is an
  **independently-pluggable sandbox boundary** that runs in-process by default
  and isolates only when a node's declared isolation requires it. In INTR-209's
  base build the recursion runs in-process (today's behavior); the hardened
  per-rung boundaries are a deferred follow-on (Phase 6).
- **Real agent execution everywhere.** The step-invoker stub is gone; steps
  run real `createAgent` instances with real tools and real inference. The
  same composition serves the single-agent and multi-step cases.
- **The in-process runtime is retired.** `provisionAgent`,
  `createSessionManager`, and `default-harness.ts`'s transport/reactor
  ownership are deleted, not duplicated. Their tool/inference/reactor
  _composition_ moves into the child and is reused.
- **Identity is preserved.** A launched agent keeps its legacy
  `ins_<hex>@<domain>` address and its `agent_instance` row; the deploy-ack
  listener, mail routing, grants, and reconnect continue to find it.

## 2. Target architecture

### Component picture

The execution host is **recursive**: the sidecar spawns a workflow child; that
child is itself a spawner and can spawn its own sub-children (grandchildren,
...) to arbitrary depth. The recursion **already exists** — the `childWorkflow`
primitive (`packages/workflow/src/definition/primitives.ts`), the runtime's
`spawnChild` / `SpawnChildWorkflow` seam, and `createSidecarRunChild`
(`apps/sidecar/src/workflow-substrate-factory.ts`), which is self-referential
and documented as "designed for arbitrary depth." **Today that recursion runs
in-process**: a `childWorkflow` executes within the parent child's process,
reusing the parent's substrate and the same `childRunId`-scoped subtree. The
addition in this design is that **each spawn rung is an independently-pluggable
sandbox boundary** (§3d-bis): a rung runs in-process by default and spawns a
sandboxed sub-child _only when a node's declared isolation requires a stricter
boundary than the current rung_ (§3f).

```
                              HUB
   deploy / mail / signal / drain          agent.event / mail.persisted
        frames  |   ^  pack push                |  ^  timeline (up all rungs)
                v   |                            v  |
 ┌───────────────────────────────────────────────────────────┐
 │                         SIDECAR                             │
 │  hub-link ── DeployRouter (workflow-host-wiring.ts)        │
 │                 │  keys top-level child per isolation-domain│
 │                 v                                          │
 │        ┌──────────────────────────────────────────┐       │
 │        │  SUPERVISOR (per isolation-domain)        │       │
 │        │  - owns mail ingestion (top-level only)   │       │
 │        │  - inbox claim-check (durable) + dispatch │       │
 │        │  - credentials snapshot push / recycle    │       │
 │        │  - SINGLE WRITER of workflow-run repo(s)  │       │
 │        └───────────────┬──────────────────────────┘       │
 │   control IPC (NDJSON, │ event channel (fd3, HMAC)         │
 │   Ed25519) + substrate │ ^ InferenceEvents                 │
 │   write proxy          v │                                 │
 │   ┌──────────────────────────────────────────────────┐    │
 │   │  CHILD rung 0 (workflow-process)                  │    │
 │   │  - runtimeRun per trigger.fired                   │    │
 │   │  - REAL step-invoker: createAgent + tools +       │    │
 │   │      inference + event firehose                   │    │
 │   │  - tool materialization (in-process)              │    │
 │   │  - warm-agent cache (long-lived single)           │    │
 │   │  - proxy RepoStore (writes -> supervisor)         │    │
 │   │  - IS A SPAWNER: spawnChild / createSidecarRunChild│   │
 │   │                                                   │    │
 │   │   childWorkflow / per-step / map-branch:          │    │
 │   │   declared isolation? ── no ──> run IN-PROCESS    │    │
 │   │                       └─ yes ─> spawn sub-child    │    │
 │   │                                 via SandboxBoundary│    │
 │   │      ┌────────────────────────────────────────┐   │    │
 │   │      │ SUB-CHILD rung 1 (own boundary:        │   │    │
 │   │      │   in-process / namespace / container)  │   │    │
 │   │      │ - same runtimeRun + step-invoker       │   │    │
 │   │      │ - IS ALSO A SPAWNER (rung 2, ...)      │   │    │
 │   │      │ - writes proxy to the SINGLE WRITER    │   │    │
 │   │      │ - events thread UP to the hub timeline │   │    │
 │   │      └────────────────────────────────────────┘   │    │
 │   └──────────────────────────────────────────────────┘    │
 │                 shared on-disk substrate (data dir)       │
 └───────────────────────────────────────────────────────────┘
```

The picture is the same component at every rung: a `runtimeRun` host that is
itself a spawner. Mail ingestion lives **only at rung 0's supervisor**
(sub-children are driven by `spawnChild`, not inbound mail — see the
mailbox-ownership confirmation at the end of §3). The workflow-run repo's
single writer is the rung-0 supervisor; every rung's writes proxy to it (§3d-bis
addresses how that proxy works across a hardened boundary). The event firehose
threads up through every rung to the hub timeline.

### How each workload flows through it

**A single launched agent** (`ins_<hex>@<domain>`):

1. Hub `POST instance` -> `instances.ts` mints the legacy address + row, ships
   grants on disk (the 8a grants-bridge, see §3g).
2. The deploy carries a one-step workflow whose single step's agent is the
   launched agent. The DeployRouter routes it to the supervisor for the
   address's isolation-domain.
3. The supervisor registers the legacy address on the mail bus, assembles the
   step's credentials snapshot from the on-disk grants, spawns (or reuses) the
   child, and pushes `grants-updated`.
4. Inbound mail lands at the supervisor, enters the inbox claim-check, and the
   dispatch loop emits `trigger.fired`. The child opens a `runtimeRun` for the
   one-step workflow; the **real step-invoker** instantiates the agent with
   tools + inference, delivers the message as the step input, and runs a real
   turn. The agent instance is **kept warm** across triggers (§3b).
5. Inference events flow up the event channel -> supervisor -> hub timeline.
   Conversation state is committed to the workflow-run substrate (§3a, §3c),
   so kill+respawn resumes via `discoverInFlightRuns`.

**A multi-step workflow** (`ins_dep_<...>`): identical machinery. The
supervisor owns mail at the deployment address; the dispatch loop drives
`runtimeRun` over the multi-step definition; each step uses the same real
step-invoker; per-step addresses/grants come from the credentials snapshot.
The only difference from the single-agent case is the number of steps and the
address shape — both already handled by the existing derivation
(`deriveStepAddress` / `deriveStepRepoId` in
`packages/workflow-host/src/supervisor/credentials.ts`, which already
documents the single-step collapse to the deployment's own address).

**A workflow with a `childWorkflow` / `map` fan-out / per-step isolated node**:
the rung-0 child's `runtimeRun` reaches the node and consults its declared
isolation (§3f). If the node declares no stricter boundary, `spawnChild`
resolves the child definition and runs it **in-process** under the same child
(today's behavior, `createSidecarRunChild` recursing on itself). If the node
declares a stricter granularity or sandbox boundary, the same `spawnChild` path
launches a **sandboxed sub-child** through the `SandboxBoundary` seam (§3d-bis);
that sub-child is itself a `runtimeRun` host with the same real step-invoker,
proxies its writes to the rung-0 single writer, and threads its events up to the
hub timeline. The sub-child can recurse again. Depth is bounded only by the
definitions' nesting and any operator ceiling (§6).

The workloads converge on one recursive host: one step-invoker, one durability
model, one spawner shape at every rung. The single agent becomes "a long-lived,
warm-kept, one-step workflow"; a `childWorkflow` becomes "a nested rung that
runs in-process unless its isolation says otherwise."

## 3. Load-bearing design decisions

### 3a. Mailbox ownership (the hard one)

**The conflict.** Two layers both want to own "drain the mailbox and hold
conversation state":

- The **supervisor** owns durable mail ingestion: `mailBus.registerAddress` +
  `subscribeMailForAddress` -> inbox claim-check substrate -> FIFO dispatch
  loop -> `trigger.fired` (one run per message), in
  `packages/workflow-host/src/supervisor/supervisor.ts`.
- The **harness** owns its own ingestion: it subscribes a `MessageTransport`
  directly, runs a connector reactor, drains INBOX continuously, and holds
  multi-turn connector state in memory (`packages/harness/src/harness.ts`,
  `MailEnv.transport`).

These are incompatible if both are live. **Decision: the supervisor is the
sole mail owner. The harness's transport-subscription + INBOX-ownership half
is dropped. The agent receives synthesized step inputs, never its own
transport subscription.**

**Chosen model — supervisor owns mail; agent runs as a warm step:**

- Mail for _every_ address (single-agent and multi-step) lands at the
  supervisor and enters the durable inbox claim-check. The supervisor's
  dispatch loop is the only thing that pulls a message and turns it into a
  `trigger.fired`.
- For a single launched agent, each `trigger.fired` delivers the inbound
  message as the step input to the warm agent instance via the step path
  (`agent.send`-shaped delivery inside the real step-invoker), not via the
  agent's own transport.
- **Multi-turn conversation state** is no longer the harness's in-memory
  connector-router snapshot. It moves to the workflow-run substrate: the
  conversation/connector state for the agent is committed under the
  workflow-run repo (the same substrate the run log uses), so it is durable
  and resumable. The connector router's `snapshot()` /`restore()` surface
  (`packages/harness/src/harness.ts`,
  `createWrappedStorageOverrides`) is reused, but its persistence sink changes
  from the agent's in-memory storage to the workflow-run substrate write path.
- **Connector/reply semantics map onto the step path** as follows. Today the
  harness routes an agent's outbound `connector.reply` back through its
  transport. Under the unified host, the agent's reply becomes the step
  output; the supervisor (the mail owner) is responsible for any outbound mail
  send. So: agent produces reply -> step output -> supervisor sends outbound
  via the mail bus. The agent never calls `transport.send` itself; the mail
  tools the agent has are backed by a transport surface whose send routes
  through the supervisor's outbound path, preserving signing and audit.

**What this requires concretely:**

- A `MessageTransport`-shaped surface for the agent whose **inbound** side is a
  no-op (the supervisor delivers inputs via the step path) and whose
  **outbound** side (`send`, `append`) routes through the supervisor's mail bus
  so replies are signed and audited exactly as today. The harness's
  `MailToolWrapper` (`packages/harness/src/harness.ts`) already takes a
  `MessageTransport`; we supply this supervisor-backed transport.
- A connector-state persistence binding that writes to the workflow-run
  substrate instead of the agent's local isogit store.

**Rejected alternatives:**

- _Harness owns mail, supervisor is bypassed for single agents_ (the
  "harness-beside-runtime" path). This keeps the harness's transport
  subscription live and runs it next to the workflow runtime in the child.
  Rejected: it gives process-level unification but **no durability dividend** —
  the single agent keeps the current non-durable in-memory-reactor model, so
  you pay the IPC/supervisor overhead and get nothing back. It also leaves two
  mail-ingestion paths alive in one process (the supervisor's inbox for
  multi-step, the harness's transport for single), which is exactly the fork
  we are trying to delete.
- _Both own mail, coordinate via a lock._ Rejected outright: two owners of
  conversation state is the bug. There is exactly one mailbox owner.

This is the **load-bearing decision of the whole design.** It is the thing
that is genuinely hard, and it must be settled before any build. See §6 for
its failure modes and the open question about reply latency.

### 3b. Warm-agent lifecycle

Today the step-invoker is instantiate-send-teardown:
`packages/workflow-host/src/adapters/step-invoker.ts` builds the env, calls
`agentFactory(req.agent, env)`, does one `agent.send`, and `agent.close()`s in
a `finally`. That is correct for a multi-step workflow where each step is a
fresh agent invocation. It is wrong for a long-lived single agent, where
re-materializing tools (and re-spawning LSP) per inbound message is absurd and
loses conversation continuity.

**Decision: the step-invoker gains a warm-keep mode for long-lived single-step
workflows.** Where it lives, when it tears down, how recycle/drain interact:

- **Where it lives.** A per-address warm-agent cache inside the child,
  keyed by the agent's identity (the single step's stepId/address). The cache
  holds the constructed `Agent` (tools loaded, plugins live, LSP subprocess
  attached) across `trigger.fired`s. The cache lives in the child's address
  space, owned by the run-loop, not in the supervisor.
- **When it's built.** Lazily on the first `trigger.fired` for the address
  (or eagerly at spawn for a hot path; lazy is the default to avoid paying
  tool materialization before any mail arrives).
- **When it's torn down.** On `agent.undeploy` (deployment teardown), on child
  `shutdown`, and on recycle. A long-lived agent is _not_ torn down between
  messages — that is the entire point of warm-keep.
- **Recycle interaction.** The supervisor's recycle policy
  (`packages/workflow-host/src/supervisor/recycle.ts`) kills and respawns the
  child on drain/SIGTERM/SIGKILL/respawn. On respawn the warm-agent cache is
  empty; the agent is rebuilt lazily on the next trigger, and conversation
  state is restored from the substrate (§3c). Recycle therefore transparently
  re-warms.
- **Drain interaction.** Drain (`packages/workflow-host/src/drain-controller.ts`,
  `supervisor/drain-timeout.ts`) flips the drain signal; in-flight step work
  observes it at the runtime's observation points. A warm agent mid-turn on a
  cancel-mode drain aborts the turn (the step-invoker already wires
  `signal.aborted` -> `agent.close()`); wait-mode lets the turn finish. After
  drain the agent is closed as part of teardown.

**Multi-step steps stay instantiate-send-teardown.** The warm-keep mode is
gated on "single-step workflow whose step is a long-lived agent," not applied
to every step. A 5-step workflow still builds and tears down each step's agent
per invocation; warm-keep there would hold five agents (and five LSP
subprocesses) warm for no benefit.

### 3c. Durability model for the single agent

A long-lived agent today (in-process) has no durability: kill the sidecar and
the connector reactor's in-memory state is gone; reconnect re-provisions from
the persisted config but conversation continuity is whatever the agent's
isogit store last committed. Under the unified host the single agent gains the
multi-step durability model:

- **What a "run" means for a long-lived agent.** Each inbound message is one
  `runtimeRun` of the one-step workflow — one `RunStarted` ->
  `StepStarted` -> `StepCompleted` -> `RunCompleted` bracket, committed to the
  workflow-run repo under `runs/<runId>/...`. This is exactly the bracket the
  current `driveTrivialRunChain` projector hand-rolls
  (`apps/sidecar/src/workflow-host-wiring.ts`), except now the runtime emits it
  natively because a real `runtimeRun` is driving the step. That projector is
  deleted (§4).
- **Conversation state across runs.** The agent's connector/conversation state
  is committed to the workflow-run substrate at run boundaries (and at
  connector-state-change points, mirroring the harness's existing
  `onConnectorStateChanged` hook). A new run reads the prior conversation
  state from the substrate before delivering the next message, so multi-turn
  continuity survives across runs and across child respawns.
- **Kill+respawn mid-conversation.** If the child dies mid-turn, the in-flight
  run's log lacks a terminal event; `discoverInFlightRuns`
  (`packages/workflow-host/src/child/run-child.ts`) finds it on respawn and
  resumes via `runtimeRun(..., { resumeFromEvents })`. The warm-agent cache is
  rebuilt lazily; the resumed run replays from its event log. The agent's
  conversation state is restored from the substrate. This is strictly more
  durable than the in-process runtime, which had no per-message run log at all.

**Open question (flagged):** the exact granularity of conversation-state
commits — per run, per turn, or per connector-state-change — trades durability
against substrate write volume. The harness already has the change-point hook;
the question is whether committing on every change is acceptable write load on
the single-writer workflow-run ref. See §6.

### 3d. Tool materialization and execution in the child (the other hard one)

Today tools are composed only in the sidecar harness builder
(`apps/sidecar/src/default-harness.ts`): `materializeToolPackages` reads the
deploy tree, runs the tool-package loader (`@intx/tool-packaging`:
`createToolLoader`, `applyAtomic`, `createTarballCache`), composes the plugin
chain (posix reads `env.plugins`; the LSP plugin factory **spawns a
subprocess**), and hands the resulting `toolFactories` to `defineAgent`. The
child does none of this — `workflow.json`'s `steps[].agent.toolFactories` are
stripped to bare `{ id, requires }` metadata on serialization
(`packages/workflow/src/definition/workflow.ts`, `projectAgent`).

**The tool-execution locus — stated explicitly, because it determines the
isolation model.** The child _is_ the sidecar binary. `apps/sidecar/bin/workflow-child`
imports `createSubstrate` and `SIDECAR_SUBSTRATE_CONFIG_KEYS` from
`apps/sidecar/src/workflow-substrate-factory` and runs
`runWorkflowChildFromProcessEnv(createSubstrate, ...)`. There is **no
cross-process injection of tool factories** — there could not be, because tool
factories are closures and closures do not cross a process boundary. When the
unified host lands, tool materialization and tool _execution_ both happen
**inside the child process**: the loader runs there, the LSP subprocess is a
child _of the child_, and a posix/shell tool's filesystem writes land in the
child's filesystem view. The child is therefore the natural isolation boundary
for everything an agent's tools can do — which is exactly why the isolation
unit and the sandbox boundary (§3f and Decision 1 below) are properties of the
_child_.

**Decision: materialize and run tools in the child, rooted per-step.**

- **What moves from `default-harness.ts`:** `materializeToolPackages` and its
  supporting machinery — the deploy-tree reader (`readDeployTree` from
  `@intx/hub-agent`), the loader construction (`createToolLoader`,
  `createTarballCache`, `applyAtomic`), the plugin-chain instantiation
  (including LSP subprocess lifecycle and posix `env.plugins` threading), and
  the disposer capture. This becomes child-side code the real step-invoker
  calls when building a step's agent. "Moves into the child" means "moves into
  the sidecar code that the `bin/workflow-child` binary already runs," not a
  new process hop.
- **What's reused unchanged:** the `@intx/tool-packaging` package itself, the
  `@intx/agent` `defineAgent` / `createAgent` surface, the
  `@intx/storage-isogit` stores, the inference runtime. None of these are
  re-implemented; their call sites move from the in-process harness builder
  into the substrate factory the child binary runs.
- **Rooting.** The tarball cache and tool instance dir root per agent/step
  under the workflow-run repo's working tree (the child has `getRepoDir`). For
  a single long-lived agent there is one materialization, reused across
  triggers (warm-keep, §3b). For multi-step, materialization is per step
  invocation as today.

**Layering — why `@intx/workflow-host` stays portable (the clarification
strengthens the call).** `@intx/workflow-host` is deliberately host-agnostic:
it takes substrate, spawner, mail bus, and step-invoker as injected bindings,
and the step-invoker accepts `buildEnv` + `agentFactory`
(`packages/workflow-host/src/adapters/step-invoker.ts`,
`WorkflowStepInvokerOpts`). The host-specific tool runtime —
`@intx/tool-packaging`, the LSP plugin, posix — lives entirely in
`apps/sidecar` (the substrate factory and the code reachable from
`bin/workflow-child`). Because the child _is_ the sidecar binary, the sidecar's
tool runtime is present in the child's address space without workflow-host ever
depending on it. The portable package never gains a dependency on the tool
runtime; an alternative host that ships a different `bin/workflow-child` with a
different tool runtime reuses all of workflow-host unchanged. The locus
correction (tools run _in_ the child, which is the sidecar binary) is precisely
what makes this clean: one process, one tool runtime, and a portable
orchestration package that knows nothing about it.

This is the second genuinely hard part: the LSP-subprocess-inside-the-child
lifecycle is the riskiest sub-item, and it is what the sandbox boundary
(Decision 1) must contain. See §6.

### 3d-bis. Pluggable sandbox boundary (Decision 1)

The child is the isolation unit (§3d). The _mechanism_ that draws that
boundary must be **pluggable**, not pinned to host subprocesses, because the
unit may need to run as a plain process today and a hardened container or
namespace tomorrow without re-architecting the supervisor.

**The seam — where it plugs in.** The supervisor already takes its child-launch
strategy as two injected bindings, in `apps/sidecar/src/workflow-host-wiring.ts`:

- `binaryPath` — the executable the child runs (defaults to
  `SIDECAR_WORKFLOW_CHILD_BINARY`, the resolved `bin/workflow-child`).
- `subprocessSpawner: SubprocessSpawner` — the function that launches the child
  and returns a `SubprocessHandle` (`pid`, `controlWriter`, `controlReader`,
  `eventReader`, `kill`, `exited`). The default is `defaultSubprocessSpawner`,
  which calls `Bun.spawn([binaryPath], { stdio: [...], env })`. The deploy
  router threads `multistepSubprocessSpawner` / `multistepBinaryPath` through
  `createSidecarDeployRouter` to the per-deployment supervisor.

This seam **is** the sandbox-boundary plug point. A sandbox implementation is a
`SubprocessSpawner` (plus, where needed, a `binaryPath` that points at a
launcher) that stands the child up inside the chosen boundary and returns a
`SubprocessHandle` whose four IPC surfaces (control reader/writer, event
reader, kill/exited) behave identically to the plain case. The supervisor and
child are already written against `SubprocessHandle`/`SubprocessSpawner` and do
not know how the process was launched — so a sandbox is a spawner swap, not a
supervisor change.

**Abstraction — the `SandboxBoundary` strategy.** Introduce a named strategy
the sidecar selects per child:

- `host-subprocess` (default, dev) — today's `defaultSubprocessSpawner`
  (`Bun.spawn`). No isolation beyond the OS process.
- `os-namespace` (later) — launches the child under `unshare` + `chroot` /
  `pivot_root` with an `overlayfs` rootfs, mapped uid/gid, restricted mounts.
  Provided as a `SubprocessSpawner` whose `binaryPath` is a namespace launcher
  that execs `bin/workflow-child` inside the new namespaces.
- `oci-container` (later) — launches the child as a container
  (runc/containerd/Docker) from an image, with the IPC channels (stdio control,
  fd3 event channel) bridged across the container boundary. Provided as a
  `SubprocessSpawner` that creates/starts the container and adapts the
  container's stdio + a passed fd into a `SubprocessHandle`.

Each implementation must satisfy the `SubprocessHandle` contract exactly: the
control channel (NDJSON, Ed25519-signed) and the event channel (fd3, HMAC) must
survive the boundary, and `kill`/`exited` must map onto the boundary's
lifecycle (container stop, namespace process teardown). The fd3 event-channel
inheritance is the sharpest constraint for a container boundary — see THE
CONTRACT below and §6.

**THE CONTRACT — the tool runtime must live inside the boundary.** This is new
design surface §3d/§3f did not previously cover, and it is the load-bearing
constraint of Decision 1. Because tools materialize and execute _in the child_
(§3d), whatever boundary the child runs inside must contain the child's
**entire tool runtime**:

- The tool-package tarballs the agent pins — or network egress sufficient to
  fetch them from the configured registries at materialization time.
- The tarball cache (`createTarballCache`) and the tool instance dir, on a
  filesystem the child can write.
- The LSP server binaries (and any other external tool binaries) the plugin
  chain spawns, present and executable in the boundary's filesystem/namespace.

Provisioning implications, stated concretely:

- A **containerized** child needs an **image (or mounted volume)** carrying the
  tool runtime: the Bun runtime + `bin/workflow-child`, the LSP server
  binaries, and either the pinned tarballs baked in or registry network egress
  from the container. The substrate config keys
  (`SIDECAR_SUBSTRATE_CONFIG_KEYS`) and the IPC fds must reach the container.
- A **namespaced** child needs the same artifacts present in its **rootfs**
  (via the overlay's lower layers or bind mounts): Bun, `bin/workflow-child`,
  LSP binaries, tarball cache directory, registry egress if tarballs are not
  pre-staged.
- The **plain subprocess** default inherits the sidecar host's filesystem, so
  the tool runtime is already present — which is why it is the default and why
  it imposes no provisioning work.

A sandbox implementation that draws the boundary but leaves the tool runtime
outside it produces a child that deploys but whose tools fail at materialization
(missing tarballs) or invocation (missing LSP binary). The contract is: **draw
the boundary around the tool runtime, not just around the process.**

**Sequencing — abstraction now, mechanism later.** The `SandboxBoundary`
abstraction and THE CONTRACT are designed and the seam is named **now** (they
shape the §3f isolation domain and the child-keying). The concrete
`os-namespace` / `oci-container` implementations are a **later phase**; the
default stays `host-subprocess` (`defaultSubprocessSpawner`) until then. The
unified-host build does **not** block on container/namespace infrastructure —
it ships on the plain-subprocess boundary, with the abstraction in place so the
hardened boundary is a spawner implementation added later, not a re-design.
This is called out so review does not read Decision 1 as a dependency on
container infra for INTR-209.

#### The boundary is per-rung (recursive)

The `SandboxBoundary` applies at **every spawn rung**, not just sidecar->child.
The child's own `spawnChild` path is the same seam one level down:

- Today, `createSidecarRunChild` (`apps/sidecar/src/workflow-substrate-factory.ts`)
  builds the child's `spawnChild` slot via `createWorkflowSpawnChild`
  (`packages/workflow-host/src/adapters/spawn-child.ts`), and that adapter
  resolves the child `definitionRef` and delegates to a `runChild` callback that
  runs the sub-workflow **in-process** — reusing the parent's `deps.substrate`
  and the parent-allocated `childRunId`. The recursion is self-referential and
  arbitrary-depth by construction.
- The refinement: `createSidecarRunChild`'s `runChild` gains the **same
  `SubprocessSpawner` / `SandboxBoundary` seam** the rung-0 supervisor has. When
  a node's declared isolation (§3f) calls for a stricter boundary than the
  current rung, `runChild` does not run the sub-workflow in-process; it **spawns
  a sandboxed sub-child** (its own supervisor + child) via the selected
  `SubprocessSpawner`, exactly as rung 0 spawns rung 0's child. When the node
  declares nothing stricter, `runChild` stays on the in-process path — the cheap
  default is preserved bit-for-bit.

So "spawner" is a capability every rung has; the only question at each rung is
whether the next node runs in-process (default) or in a fresh boundary
(isolation-driven).

#### Single-writer discipline across rungs

The workflow-run repo has exactly one writer: the **rung-0 supervisor**. This
must hold no matter how deep the nesting:

- **In-process recursion (today, the default):** every rung shares the rung-0
  child's proxy `RepoStore` (the proxy whose `writeTreePreservingPrefix`
  forwards over the control IPC to the rung-0 supervisor). A grandchild's writes
  land under `runs/<grandchildRunId>/...` of the _same_ rung-0 workflow-run repo,
  sibling to its ancestors' subtrees (sub-namespacing in
  `apps/sidecar/src/workflow-substrate-factory.ts`). There is one writer and one
  repo; depth only adds `runId` subtrees. **This already works.**
- **Sandboxed sub-child (a hardened rung):** a sub-child in its own
  container/namespace can no longer share the parent's in-memory proxy object.
  It needs its own substrate-write channel back to the **rung-0 single writer**.
  Two routings are possible, and the choice is load-bearing:
  - **Chain routing:** sub-child proxies writes to its immediate parent rung,
    which forwards up to rung 0. Simple to wire (each rung already has an
    upstream control channel) but introduces the **trust-inversion sharp edge**
    (§6): a _more_-isolated grandchild routes its writes through a _less_-trusted
    parent, which can observe or tamper with them. Unacceptable when the reason
    for the stricter boundary was to distrust the parent.
  - **Direct-to-writer routing (recommended):** a sandboxed sub-child gets a
    substrate-write channel **straight to the rung-0 supervisor** (the single
    writer), bypassing intermediate rungs. The supervisor remains the sole
    writer and the sole authority that validates the sub-child's principal
    (`WorkflowRunWorkflowProcessPrincipal` scoped to the deployment). This
    preserves single-writer and avoids the trust inversion, at the cost of the
    supervisor maintaining a write channel per sandboxed rung rather than only
    to rung 0.

  **Decision: direct-to-writer for sandboxed rungs; shared-proxy for in-process
  rungs.** In-process rungs share the parent proxy because they are already
  inside the parent's trust boundary (same process); a sandboxed rung gets a
  direct channel to the single writer because its whole point is to _not_ trust
  the intermediate. This is flagged as an open question in §6 because it makes
  the supervisor's write-channel fan-out a function of sandboxed-rung count.

#### IPC and event threading across rungs

- **Control IPC (NDJSON + Ed25519) and event channel (fd3 + HMAC)** are
  per-rung: each spawn rung establishes the same `SubprocessHandle` surface with
  its immediate parent. A hardened-boundary spawner must make these channels
  survive the boundary (the fd3-across-a-container constraint, §6).
- **Event firehose up multiple rungs:** an `InferenceEvent` emitted in a
  grandchild rides that rung's event channel up to its parent rung, which
  forwards it up to rung 0, which the DeployRouter publishes via
  `publishWorkflowInferenceEvent` to the hub timeline. For **in-process** rungs
  this is a function call up the stack (negligible). For **sandboxed** rungs each
  hop is an IPC forward; deep nesting adds per-hop latency to the firehose (§6).
  Unlike substrate writes, event forwarding _can_ safely chain through parents
  (events are observational, not authoritative), so events chain up while
  sandboxed writes go direct — an asymmetry worth stating explicitly.

### 3e. Event threading

Today the real step-invoker drops events: the substrate factory's `invokeStep`
wrapper takes `(req, onEvent)` and `void onEvent`s it
(`apps/sidecar/src/workflow-substrate-factory.ts`), with the comment "the
event funnel inside the adapter lands when the harness's emit hook is wired."
The transport for events already exists end-to-end:

- Child emits via `createEventChannelSender` (HMAC over fd3) in
  `packages/workflow-host/src/child/run-child.ts`. The event channel is
  **newline-delimited**: fd3 is a byte-stream pipe, so the kernel may
  coalesce successive frames into one read or split one frame across reads.
  The sender terminates every frame with `\n` and the receiver
  (`receiveEventChannel`) buffers raw bytes and splits on that terminator,
  mirroring the control channel's NDJSON discipline. (The one-write-equals-
  one-frame assumption the channel originally carried held only because the
  channel was empty until events actually flowed; threading real events
  surfaced the framing requirement.)
- Supervisor reads the event channel and the DeployRouter forwards via
  `publishWorkflowInferenceEvent(frame.agentAddress, event)` in
  `apps/sidecar/src/workflow-host-wiring.ts`.
- The hub fans the event to per-agent listeners -> the timeline.

**Decision: thread the agent's `onEvent` through the step-invoker to the event
channel.** Concretely:

- `createWorkflowStepInvoker` (`packages/workflow-host/src/adapters/step-invoker.ts`)
  gains an `onEvent` parameter (or the agent env carries an emit sink). Inside
  `invokeStep`, before `agent.send`, subscribe the agent's event stream
  (`agent.stream()` / the reactor's event surface) and forward each
  `InferenceEvent` to `onEvent`.
- The substrate factory stops voiding `onEvent` and connects it to the child's
  event sender.

This is a **signature change** on the step-invoker adapter (it currently has no
`onEvent` on the agent-build path), plus a call-site connection — bounded, but
it touches the adapter's public surface, so it is a deliberate API change, not
a one-line edit.

### 3f. Multiplexing and workflow-declared isolation granularity (Decision 2)

Today one child hosts one deployment: `activeSupervisors` keys one supervisor
(thus one child) per deployment address in
`apps/sidecar/src/workflow-host-wiring.ts`, and the substrate factory pins one
`WORKFLOW_RUN_REPO_ID` at spawn
(`apps/sidecar/src/workflow-substrate-factory.ts`). Neither is a substrate law:

- One child **already** hosts many concurrent runs (the dispatch loop and
  `discoverInFlightRuns` drive N runs; sub-namespacing is `runs/<runId>/...`).
- The scheduler is already list-shaped: `listActiveDeployments: () =>
[workflowRunRepoId]` in the substrate factory.
- The single-writer discipline serializes at the **supervisor**, so more runs
  in one child do not race — the supervisor is the serialization point.

**Decision: generalize from one-repo-per-child to a set, and make isolation
granularity a property the workflow declares — not merely an operator default.**
The DeployRouter keys children by an **isolation domain** that is _computed from
the workflow's declared isolation_; the substrate factory and scheduler accept a
set of workflow-run repos per child.

#### The `isolation` declaration — workflow-level default plus per-node override

Isolation is declarable at **two levels**: the workflow level (the top-level
default that drives the rung-0 child's domain key) and the **per-node** level
(a specific `step` / `childWorkflow` / `map` node may declare a _stricter_
boundary than its enclosing workflow, which is what triggers a sandboxed
sub-child for that node). Both share one shape:

```ts
type IsolationGranularity = "per-tenant" | "per-agent";

interface IsolationSpec {
  /** Child-process sharing granularity. */
  granularity: IsolationGranularity;
  /**
   * Sandbox boundary (§3d-bis / Decision 1) the node's child runs inside.
   * Advisory at authoring time; the sidecar's SandboxBoundary selection and
   * operator policy resolve the effective mechanism.
   */
  sandbox?: "host-subprocess" | "os-namespace" | "oci-container";
}

// Workflow-level default.
interface WorkflowDefinition {
  // ...existing fields...
  isolation?: IsolationSpec; // default { granularity: "per-tenant" }
}

// Per-node override on the primitives that can spawn a rung.
// (packages/workflow/src/definition/primitives.ts)
interface StepPrimitive {
  /* ...existing... */ isolation?: IsolationSpec;
}
interface ChildWorkflowPrimitive {
  /* ...existing... */ isolation?: IsolationSpec;
}
interface MapPrimitive {
  /* ...existing... */ isolation?: IsolationSpec;
}
// And on the matching constructor opts: StepOpts, ChildWorkflowOpts, MapOpts.
```

- **Where per-node isolation attaches.** On the three primitives that are spawn
  triggers (§ "Spawn triggers" below): `StepPrimitive` (per-step isolation),
  `ChildWorkflowPrimitive` (the nested-workflow rung), and `MapPrimitive` (the
  fan-out branch — declared on the `map` node, applied to each fan-out instance
  of its inner `step`). Added to the matching `StepOpts` / `ChildWorkflowOpts` /
  `MapOpts` constructors in `packages/workflow/src/definition/primitives.ts`.
  Primitives that cannot spawn a rung (`gate`, `sleep`, `awaitSignal`,
  `escalation`) do not carry the field.
- **Defaulting — inherit, in-process unless stricter.** Absent per-node
  `isolation` means **inherit the enclosing rung's boundary and run in-process**
  (the cheap default). A node only causes a sandboxed sub-child when it declares
  a granularity/sandbox **stricter** than the rung it would run in. "Stricter"
  is the same ordering used for the operator floor: on granularity
  `per-tenant < per-agent`, and on sandbox `host-subprocess < os-namespace <
oci-container`. A per-node declaration equal to or weaker than the current rung
  is a no-op (it still runs in-process) — a node cannot _relax_ isolation below
  its enclosing boundary (you cannot escape a container by declaring
  `host-subprocess` on a child node; the floor is monotonic going down a rung).
- **Workflow-level defaulting.** Unchanged from Decision 2: `defineWorkflow`
  normalizes an absent workflow-level `isolation` to
  `{ granularity: "per-tenant" }`. The single-agent launch path
  (`wrapHarnessAsSingleStepWorkflow`) sets the workflow-level field from the
  launch request / agent policy.
- **Validation.** `defineWorkflow`'s `normalize` validates every `isolation`
  occurrence — workflow-level and per-node — against the allowed
  `granularity` / `sandbox` sets, throwing a structured error on an unknown
  value, and rejects a per-node declaration that is structurally malformed. The
  monotonic-floor check (a per-node value cannot weaken the enclosing boundary)
  is enforced at deploy/spawn resolution, not authoring time, because the
  enclosing rung's _effective_ boundary depends on operator policy not visible
  to `defineWorkflow`.
- **Hashing.** Every `isolation` occurrence — workflow-level and per-node —
  flows into `hashDefinition` via `projectForHash`
  (`packages/workflow/src/definition/workflow.ts`). Per-node isolation is part
  of the content-addressed definition: two definitions differing only in a
  node's declared isolation are different definitions, which is correct — the
  isolation a node runs under is deploy-affecting and the `RunStarted`
  `definitionHash` should reflect it. `projectForHash`'s `projectPrimitive` /
  `projectAgent` already project per-primitive fields; `isolation` joins them.
  (Definition-surface/versioning consideration; see §6.)

#### Spawn triggers (where a new boundary CAN be drawn)

A new sandbox boundary (a sandboxed sub-child) can be drawn only at a node that
spawns a rung, and only when that node's effective isolation is stricter than
its enclosing rung:

- **`childWorkflow`** — the nested-workflow rung (the existing recursion point).
- **per-step isolation** — a single `step` whose agent does
  untrusted/high-value work (shell/file/LSP) runs in its own boundary.
- **`map` fan-out** — each fan-out instance of the inner step runs in its own
  boundary when the `map` node declares isolation; this is the highest-fanout
  spawn case and the one most able to multiply process count (§6).

Every trigger is governed by "only when isolation requires it" — no stricter
declaration than the current rung means the node runs **in-process**, even for
`childWorkflow` and `map`, preserving the cheap path.

#### How `isolation` flows to child-keying

`WorkflowDefinition.isolation` -> the deploy frame's workflow projection -> the
DeployRouter's isolation-domain computation -> the supervisor/child the deploy
binds to:

- **Domain-key derivation** (in the DeployRouter,
  `apps/sidecar/src/workflow-host-wiring.ts`, replacing the per-deployment-address
  `activeSupervisors` key):
  - `granularity: "per-tenant"` => domain key = the tenant id. All per-tenant
    workflows for one tenant resolve to the **same** key and therefore the same
    child.
  - `granularity: "per-agent"` => domain key = the agent/deployment identity
    (the deployment id / legacy agent slug). Each per-agent workflow resolves to
    a **unique** key and gets its **own** child.
  - The `sandbox` hint, when present, participates in the key (a `per-tenant`
    workflow requesting `oci-container` cannot share a `host-subprocess` child
    of the same tenant — different boundary => different child), subject to
    operator policy resolving the effective boundary first.
- **DeployRouter select/reuse.** On `agent.deploy`, the router computes the
  domain key, then **looks up an existing child** for that key in the
  (generalized) `activeSupervisors` map:
  - Hit => register this deployment's workflow-run repo with the existing
    supervisor (add to its repo set / `listActiveDeployments`), push the
    deployment's credentials snapshot, and route its mail address to that child.
    No new process.
  - Miss => spawn a new child via the selected `SubprocessSpawner` (Decision 1),
    seed it with this deployment's workflow-run repo, and record it under the
    domain key.
  - `agent.undeploy` removes the deployment's repo from the child's set; the
    child is torn down only when its set empties.

So two per-tenant workflows for one tenant **share** a child (amortized cost); a
per-agent workflow gets its **own** child (max isolation). The author declares
intent; the router computes the domain and shares or isolates accordingly.

#### Child-keying in the recursive case

The DeployRouter's `activeSupervisors` map keys only **top-level (rung-0)**
children — the ones a `agent.deploy` frame binds to. A **per-node** isolated
sub-child is _not_ a sibling under the sidecar's `activeSupervisors`; it is a
nested rung **under a running child**, managed by that child's own sub-child
registry:

- **Top-level (rung 0):** keyed by the workflow-level isolation domain in the
  sidecar's `activeSupervisors` (`apps/sidecar/src/workflow-host-wiring.ts`), as
  above. This is the only map the DeployRouter touches.
- **Nested (rung ≥ 1):** when a node's effective isolation is stricter than its
  rung, the rung's `createSidecarRunChild` `runChild` (the spawner one level
  down) spawns the sub-child and records it in a **per-child sub-child
  registry** — the recursive analogue of `activeSupervisors`, owned by the child
  process, keyed by the node id + the node's run instance (for `map`, one entry
  per fan-out instance). Teardown is symmetric: the sub-child is torn down when
  its node's run reaches a terminal phase, and the registry is drained when the
  child shuts down (which the rung-0 supervisor's recycle/drain already
  cascades).
- **Sharing within a rung:** in-process nodes do not get a registry entry (they
  run inside the rung's own process). Sandboxed sub-children at the same rung
  _could_ share a sub-child if they declare the same domain+boundary, but the
  default is per-node-instance (a per-step or per-map-branch isolation is by
  nature an isolation request, so sharing is the exception, not the rule).

The relationship in one line: **the sidecar's `activeSupervisors` owns the
forest's roots; each child owns its own subtree.** No global registry tracks
arbitrary-depth rungs — each rung tracks only its direct children, matching the
recursion's shape.

#### The tunable, with cost/benefit

| Granularity           | Who chooses                                  | Process count | Blast radius        | Cost                                                             |
| --------------------- | -------------------------------------------- | ------------- | ------------------- | ---------------------------------------------------------------- |
| Per-agent (own child) | Workflow declares `granularity: "per-agent"` | High (≈ N)    | One agent           | Max isolation; full per-agent process/fd/LSP/cache cost          |
| Per-tenant (default)  | Default / `granularity: "per-tenant"`        | Medium        | One tenant's agents | Amortized tool cache + LSP per tenant; tenant-level fault domain |
| Per-everything        | Operator-only (collapse the domain key)      | 1             | Everything          | Cheapest; one crash kills all agents — dev/small only            |

**Default: per-tenant.** It amortizes the expensive shared resources (tarball
cache, LSP subprocesses, the child process itself) across a tenant's agents
while keeping the fault domain to a tenant — tenants are already the system's
natural trust/isolation boundary.

**Security framing.** `per-agent` is the choice for **untrusted or high-value
agents doing shell/file/LSP work**: because tools materialize and execute in the
child (§3d), a per-agent child means a compromised or runaway agent's tool
activity is confined to a process serving only that agent, and (with a hardened
sandbox boundary, Decision 1) confined to that boundary. The **definition author
declares it** when they know the agent is sensitive; **operator policy** can
raise the floor (below).

**Per-everything is operator-only.** No workflow can declare `per-everything`
(it would let one author collapse other tenants' isolation). It exists solely as
an operator-side collapse of the domain key for dev/small single-tenant
deployments.

#### Operator policy ceiling (open question)

A workflow _declares_ its isolation, but the operator should be able to enforce
a **minimum** regardless of declaration — e.g. "this tenant's agents are always
at least per-agent," overriding a workflow that declared `per-tenant`. The clean
model: the effective granularity = `max(declared, operator-policy-floor)` on an
ordering `per-tenant < per-agent`, resolved in the DeployRouter before the
domain key is computed. **Open question:** where the operator policy floor lives
(tenant config? a sidecar policy file? a hub-side deploy-time check?) and
whether the _sandbox boundary_ is similarly floor-able (operator forces
`oci-container` minimum for a tenant). Flagged in §6; the field shape above does
not block on resolving it — the floor is an additional input to the same domain
computation.

#### What enforces per-run isolation within a shared child

Sub-namespacing: every run's substrate access is keyed `runs/<runId>/...`
(`apps/sidecar/src/workflow-substrate-factory.ts` documents this). Each run's
agent gets its own workdir/storage root; the per-step env builder roots storage
per step/run. Two runs in one child cannot see each other's state because every
substrate adapter routes through `runId`. The remaining shared surfaces inside a
multiplexed child are the process itself (a crash/OOM is shared — the
blast-radius cost above), the scheduler singleton (shared, already designed to
list multiple deployments), and the LSP subprocesses / tarball cache (shared; a
wedged LSP is a domain-level fault — tenant-level under the default). A
`per-agent` declaration removes this sharing entirely by giving the agent its
own child.

**Sequencing.** The `isolation` field, its defaulting/validation/hashing, and
the domain-key derivation are designed now. Start the build at **per-deployment
(today's behavior)**, which is the degenerate `per-agent` case, and widen to the
declared `per-tenant` sharing once warm-agent durability is proven (§5), so
multiplexing is the _last_ capability added, not entangled with the harder
mailbox/durability work.

### 3g. Identity preservation

A launched agent keeps its legacy `ins_<hex>@<domain>` address and its
`agent_instance` row. This coexists with the child/workflow-run model exactly
as the multi-step path already does:

- **Address shape.** `instances.ts` continues to mint `ins_<hex>@<domain>` via
  `formatAgentAddress(generateId("instance"), domain)`. The address does not
  become `ins_dep_<...>`. The deploy-ack listener's discriminator
  (`isWorkflowDerivedAddress` in `packages/workflow-deploy/src/orchestrator.ts`)
  keys on the literal `ins_dep_` segment, so it correctly returns `false` for a
  launched agent — the key persists, reconnect works, nothing in the listener
  changes (`packages/hub-sessions/src/hub-session-orchestrator.ts`).
- **Workflow-run repo id.** The child's workflow-run repo for a launched agent
  is keyed by `deriveWorkflowRunRepoId(legacyAddress)`
  (`packages/workflow-deploy/src/orchestrator.ts`), which sanitizes the legacy
  address into a substrate-safe slug. This is already how the trivial branch
  keys its workflow-run repo today
  (`apps/sidecar/src/workflow-host-wiring.ts`). The read/write repo-id
  invariant (hub reconstructs the same slug for reads) is preserved.
- **Grants placement.** The supervisor's credentials assembly reads each step's
  grants from `state/grants.json` in the step's `agent-state` repo
  (`packages/workflow-host/src/supervisor/credentials.ts`, `STEP_GRANTS_PATH`).
  The launched agent's grants must land there. **This is exactly the
  grants-on-disk bridge already built and proven** during the Commit-1
  exploration, saved as
  `dispatch/workflow-launch-and-converge/8a-route_single_step_via_child/8a-groundwork.patch`.
  It is reusable here verbatim: the single-step deploy writes `state/grants.json`
  from `config.grants` before spawn, and the per-step `deriveStepAddress` /
  `deriveStepRepoId` return the legacy address and the legacy agent-state repo
  id. Reuse this patch as the identity/grants foundation.

## 4. What gets retired vs reused

### Retired (deleted, not duplicated)

- `packages/hub-agent/src/session-manager.ts`: `provisionAgent`,
  `createSessionManager`, `startSession`/`destroySession`/`abortSession` as the
  in-process execution surface, `restoreSessions`, `persistHubPublicKey`,
  `onAgentEvent` fan-out, the mail-commit queue. The `SessionManager` type
  collapses to whatever (if anything) the boot edge still needs; the
  in-process-runtime surface is gone.
- `apps/sidecar/src/default-harness.ts`: the **transport/reactor ownership** —
  the `createHarness` call that owns transport subscription, the connector
  reactor, INBOX draining, `MailEnv.transport` wiring. The `HarnessBuilder`
  seam (`packages/hub-agent/src/harness-builder.ts`) is retired as a standalone
  concept; its composition logic moves (see Reused).
- `apps/sidecar/src/workflow-host-wiring.ts`: the in-process trivial deploy
  branch (`frame.workflow === undefined`) and the hand-rolled run-event
  projector — `driveTrivialRunChain`, `TrivialRunCell`, `TRIVIAL_STEP_ID`,
  `TRIVIAL_DEFINITION_HASH`. The native `runtimeRun` bracket replaces the
  projector.
- The `TrivialLaunch` / `TrivialLaunchBindings` / `trivialLaunch` seam in
  `packages/workflow-host/src/supervisor/types.ts` and
  `supervisor/supervisor.ts`: with every deploy going through `spawn()` + real
  step execution, the in-process-launch callback is dead.
- The substrate factory's stub invokers `baseInvokeStep` / `childInvokeStep`
  and the throwing-Proxy `StepEnvBase` slots in
  `apps/sidecar/src/workflow-substrate-factory.ts`.

### Reused (one implementation, in the child = the sidecar binary)

- Tool composition: `materializeToolPackages` logic from `default-harness.ts`
  plus `@intx/tool-packaging` (loader, tarball cache, `applyAtomic`) and the
  plugin/LSP chain. Its call sites move into the substrate factory the child
  binary runs (§3d); the portable `@intx/workflow-host` does not gain the
  dependency. Connected via the step-invoker's `buildEnv`/`agentFactory`.
- Inference source resolution: the per-step source table the substrate factory
  already parses (`createSidecarStepBuildEnv`, `STEP_INFERENCE_SOURCES`).
- The agent runtime: `@intx/agent` `defineAgent` / `createAgent`, the reactor,
  the connector router and its `snapshot()`/`restore()` surface from
  `packages/harness/src/harness.ts` (reused for conversation state, repointed
  to the substrate per §3a).
- Storage/audit: `@intx/storage-isogit` stores, rooted per step/run.
- The entire supervisor/child orchestration, IPC, substrate-write bridge,
  scheduler, recycle/drain/terminal machinery — already real, unchanged in
  shape.
- **The child-launch seam** (`SubprocessSpawner` / `binaryPath` /
  `SubprocessHandle` in `apps/sidecar/src/workflow-host-wiring.ts`,
  `defaultSubprocessSpawner`): reused as-is and **generalized into the
  `SandboxBoundary` plug point** (§3d-bis). No change to the supervisor/child
  IPC contract; sandbox implementations are new `SubprocessSpawner`s added later.

### New design surface (did not exist before this revision)

- `WorkflowDefinition.isolation` (workflow-level) **and per-node `isolation`** on
  `StepPrimitive` / `ChildWorkflowPrimitive` / `MapPrimitive` and their
  `StepOpts` / `ChildWorkflowOpts` / `MapOpts` constructors
  (`packages/workflow/src/definition/primitives.ts`), plus `defineWorkflow`
  defaulting/validation and `projectForHash` inclusion for every occurrence
  (`packages/workflow/src/definition/workflow.ts`), per Decision 2 + the
  recursive refinement.
- The `SandboxBoundary` strategy abstraction + THE CONTRACT (tool runtime lives
  inside the boundary), **applied per-rung** (§3d-bis): the rung-0 supervisor's
  `SubprocessSpawner` seam _and_ the child's `createSidecarRunChild` /
  `createWorkflowSpawnChild` `runChild` path gain the same boundary seam.
  Concrete `os-namespace`/`oci-container` spawners are deferred (later phase, §5).
- The DeployRouter isolation-domain key derivation
  (`apps/sidecar/src/workflow-host-wiring.ts`, generalizing `activeSupervisors`
  from per-deployment-address to per-isolation-domain) **plus the per-child
  sub-child registry** (the recursive analogue, owned by each child), per
  Decision 2 + the recursive refinement.
- The recursive substrate-write routing decision (in-process rungs share the
  parent proxy; sandboxed rungs get a direct-to-rung-0-writer channel; §3d-bis).

### Trivial-named symbols' fate

Every surviving "trivial"-named symbol is renamed to reflect that the
single-step identity path is the canonical single-agent deploy, not a "trivial"
special case — or deleted where the in-process branch it served is gone:

- `wrapHarnessAsSingleStepWorkflow`
  (`packages/workflow-deploy/src/orchestrator.ts`) kept its name; still used to
  build the one-step definition.
- `buildTrivialApprovalSet` -> `buildSingleStepApprovalSet`
  (`packages/hub-sessions/src/session-service.ts`).
- `isTrivialDeploy` / `trivialBindings` / the trivial orchestrator branch:
  **deleted** if single-step routes through the multi-step branch (likely);
  otherwise renamed `isSingleStepDeploy`. Confirm during build.
- `deriveDeploymentId` (`apps/sidecar/src/workflow-host-wiring.ts`): kept; still
  called at the workflow-host wiring sites.
- `TrivialLaunch` / `trivialLaunch` / `trivialClaimedSlugSucceeded`: deleted
  with the in-process branch.

## 5. Phased build path

The whole thing ships together (INTR-209 held until done), but the build is
phased so each phase is independently verifiable and the risky work is
front-loaded. Verification prefers a **spawned-child integration test** driving
a real `agent.send` via the **inference test-provider seam** (`BaseEnv.deps`
from `@intx/inference-testing`, since real inference in CI is impractical and
non-deterministic).

### Phase 1 — Child runs a real agent for one step

- **Changes.** `apps/sidecar/src/workflow-substrate-factory.ts`: replace the
  throwing-Proxy `StepEnvBase` slots in `createSidecarStepBuildEnv` with real
  per-step storage/workdir/audit/directors; replace `baseInvokeStep`'s
  placeholder with a real `createWorkflowStepInvoker({ workflowAuthorize,
buildEnv, agentFactory: createAgent })`. Tools empty for now.
- **Verification.** Spawned-child integration test: a one-step workflow whose
  agent uses the test inference provider returns a real `{ reply }` from
  `agent.send`, not `req.agent.id`. **This is the first proof the child runs a
  real agent.**
- **Riskiest sub-item.** Rooting per-step isogit storage/workdir correctly
  under the workflow-run repo without colliding with the run log.

### Phase 2 — Tool materialization in the child (RISKIEST PHASE)

- **Changes.** Move `materializeToolPackages` logic + `@intx/tool-packaging`
  loader + plugin/LSP chain into the sidecar's child-side wiring, injected via
  the step-invoker's `buildEnv`/`agentFactory`. Root tarball cache + tool
  instance dir per step/agent.
- **Verification.** Spawned-child integration test: an agent with a posix tool
  invokes it; the result lands in `{ turn }`. A second test with the LSP plugin
  confirms the subprocess spawns and tears down with the agent.
- **Riskiest sub-item.** **The LSP-subprocess-inside-the-child lifecycle.** If
  this proves fraught, it is the signal to revisit isolation granularity (a
  wedged LSP in a shared child is a tenant-level fault) or to reconsider scope.

### Phase 3 — Event threading

- **Changes.** Add `onEvent` to `createWorkflowStepInvoker`
  (`packages/workflow-host/src/adapters/step-invoker.ts`); subscribe the
  agent's event stream inside `invokeStep`; stop voiding `onEvent` in the
  substrate factory.
- **Verification.** The Phase-1/2 integration test asserts `inference.start` /
  turn events arrive at the supervisor's `publishWorkflowInferenceEvent` sink
  and reach the hub timeline.

### Phase 4 — Warm-agent lifecycle + single-agent durability (mailbox decision)

- **Changes.** Warm-agent cache in the child (§3b); supervisor-as-sole-mail-owner
  with synthesized step-input delivery (§3a); conversation state committed to
  the workflow-run substrate (§3c); supervisor-backed outbound transport for
  replies. Apply the 8a grants-bridge patch for identity/grants (§3g).
- **Verification.** (1) Two sequential messages to one address reuse one warm
  agent and preserve conversation state. (2) Multi-turn conversation over the
  inbox claim-check produces a durable, resumable run log. (3) **Kill + respawn
  the child mid-conversation; `discoverInFlightRuns` resumes and conversation
  state is restored.** This is the milestone that proves the dividend (durable
  single agent) — gate it hardest.
- **Riskiest sub-item.** The mailbox-ownership reconciliation (§3a) and reply
  latency through the inbox/dispatch path (§6).

### Phase 4b — Isolation declaration (workflow + per-node) + sandbox-boundary abstraction (pure additions)

These pieces are designed now and land as **pure additions** before the
multiplex/recursive phases consume them. They do not change runtime behavior on
their own (every default resolves to today's per-deployment / in-process /
`host-subprocess` behavior), so they are low-risk and can land any time after
the field is needed.

- **Changes.** (1) Add `WorkflowDefinition.isolation` (workflow-level) **and
  per-node `isolation`** on `StepPrimitive` / `ChildWorkflowPrimitive` /
  `MapPrimitive` + their constructors
  (`packages/workflow/src/definition/primitives.ts`), with `defineWorkflow`
  defaulting/validation and `projectForHash` inclusion for every occurrence
  (`packages/workflow/src/definition/workflow.ts`). (2) Introduce the
  `SandboxBoundary` strategy + THE CONTRACT around the existing
  `SubprocessSpawner`/`binaryPath` seam (`apps/sidecar/src/workflow-host-wiring.ts`),
  with only the `host-subprocess` implementation (the existing
  `defaultSubprocessSpawner`) wired. No `os-namespace`/`oci-container` yet.
- **Verification.** (1) `defineWorkflow` defaults absent workflow-level
  `isolation` to `{ granularity: "per-tenant" }` and absent per-node isolation to
  inherit-in-process; rejects an unknown `granularity`/`sandbox` at any
  occurrence; every occurrence round-trips through `hashDefinition` (definitions
  differing only in a node's isolation hash differently). (2) The
  `SandboxBoundary` selection returns the `host-subprocess` spawner for the
  default; build unchanged from Phase 3.
- **Riskiest sub-item.** Hashing inclusion (workflow-level _and_ per-node) is a
  definition-surface/versioning change (§6) — confirm no existing fixture
  hard-codes a definition hash the new fields would shift.

### Phase 4c — In-process recursive honoring (already-exists path, wire the decision)

The in-process recursion already exists (`createSidecarRunChild` self-referential,
`createWorkflowSpawnChild`, arbitrary depth). This phase makes it **consult the
declared isolation** and stay in-process when nothing stricter is declared — the
base-build behavior of the recursive model, with **no sandboxed sub-child yet**.

- **Changes.** Thread each node's effective isolation into the rung's
  `runChild` decision (`apps/sidecar/src/workflow-substrate-factory.ts`): no
  stricter declaration => in-process (today's path, unchanged); stricter
  declaration => a structured "sandboxed sub-child required but the hardened
  boundary is not yet implemented" error (loud, not silent). Add the per-child
  sub-child registry scaffolding (§3f) even though only the in-process branch is
  taken.
- **Verification.** A `childWorkflow` / `map` with no per-node isolation runs
  in-process and its events/writes land under `runs/<childRunId>/...` of the
  rung-0 repo (existing behavior, now under the isolation-aware code path). A
  node declaring a stricter boundary surfaces the structured "not yet
  implemented" error rather than silently running in-process (which would be an
  isolation-policy violation).
- **Riskiest sub-item.** Ensuring "stricter declared but unimplemented" fails
  loud — a silent downgrade to in-process is a security bug (an
  isolation-requiring node running without isolation).

### Phase 5 — Workflow-declared multiplex + retire the in-process runtime

- **Changes.** Generalize the substrate factory + supervisor registry from one
  workflow-run repo to a set; replace the per-deployment-address
  `activeSupervisors` key with the **isolation-domain key computed from the
  workflow-level isolation** (per-tenant => key by tenant, per-agent => key by
  deployment; `sandbox` hint participates; operator floor applied). Wire child
  select/reuse in the DeployRouter. Delete `provisionAgent` /
  `createSessionManager` / `default-harness.ts` transport-reactor ownership / the
  trivial branch + projector / `TrivialLaunch`. Rename surviving trivial-named
  symbols (§4).
- **Verification.** (1) Two per-tenant workflows for one tenant share a child;
  a per-agent workflow gets its own — assert the `activeSupervisors`/domain-key
  mapping. (2) One child hosts two deployments' runs without cross-contamination
  (sub-namespacing holds). (3) Full INTR-209 fixture re-run on the unified host
  (§7). (4) The retired symbols have no remaining callers (build proves it).

### Phase 6 (deferred, separate initiative) — Hardened per-rung sandbox boundaries

Routing `spawnChild` through an actual hardened boundary — launching a sandboxed
sub-child into a namespace/container — lands here, **after** the unified host
ships. This is the `os-namespace` / `oci-container` `SubprocessSpawner`
implementations (rung-0 and recursive), THE CONTRACT's image/rootfs provisioning,
the fd3-across-a-boundary IPC bridging, and the direct-to-rung-0-writer substrate
channel for sandboxed rungs (§3d-bis). It is explicitly **out of INTR-209's
scope** (see §6) — the unified host ships on the in-process / `host-subprocess`
default, and the hardened boundary is added without re-architecting because the
seam and the contract are already in place.

**Milestone where steps first do real work:** end of **Phase 2** — workflow
steps run real agents with real tools, making INTR-209 genuinely functional
(no more placeholder output) even though shipping waits for Phase 5. The
recursive in-process honoring (Phase 4c) and multiplex (Phase 5) complete the
INTR-209 end state; the hardened boundaries (Phase 6) are a follow-on.

## 6. Risks, open questions, alternatives considered

### The two hard parts and their failure modes

1. **Mailbox ownership (§3a).** Failure mode: reply latency. Routing every
   inbound message through the durable inbox claim-check + FIFO dispatch adds
   substrate-write latency on the request path that the in-process harness
   (direct transport subscription) does not pay. For a chatty interactive
   agent this could be perceptible. _Open question:_ is the per-message inbox
   round-trip acceptable for interactive single agents, or does the single
   agent need a fast-path that still commits durably but does not block the
   reply on the full claim-check? This must be measured early (Phase 4).
   Second failure mode: conversation-state commit volume on the single-writer
   workflow-run ref (§3c open question) — committing connector state on every
   change could overwhelm the single writer.

2. **Tool materialization in the child (§3d).** Failure mode: the
   LSP-subprocess-inside-the-child lifecycle. The child already manages one
   subprocess relationship (it _is_ a subprocess of the supervisor); nesting
   LSP subprocesses under it, per agent, with correct teardown on
   recycle/drain/crash, is the most operationally fiddly part. Failure here
   amplifies the §3f isolation cost (a wedged LSP in a shared child). _Open
   question:_ does LSP need its own supervision/restart inside the child, or is
   "die with the agent" sufficient?

### Decision 1 / Decision 2 risks and open questions

3. **Sandbox boundary + tool provisioning (Decision 1, §3d-bis).** The
   abstraction is low-risk (a spawner swap behind an existing seam) and ships on
   the plain-subprocess default, so it does **not** gate INTR-209. The _concrete_
   hardened boundaries carry a real, separately-scoped provisioning burden: a
   container/namespace child needs the **entire tool runtime inside the
   boundary** (Bun + `bin/workflow-child` + LSP binaries + tarball cache +
   registry egress or pre-staged tarballs). Flagged scope item: building the
   `oci-container` boundary is image-pipeline + IPC-bridging work
   (the fd3 event channel crossing a container boundary is the sharpest part)
   that should be its own initiative, not folded into the unified-host build.
   _Open question:_ whether the fd3 HMAC event channel can cross a container
   boundary cleanly, or whether the event channel needs a transport that
   survives containerization (a socket rather than an inherited fd).

4. **`isolation` field as a definition surface (Decision 2, §3f).** Including
   `isolation` in `hashDefinition`/`projectForHash` means it is part of the
   content-addressed identity — correct (isolation is deploy-affecting), but it
   is a **versioning consideration**: adding the field shifts the hash of every
   definition that adopts it, and any fixture or stored artifact that hard-codes
   a definition hash must be regenerated. Phase 4b verification checks this.
   _Open question:_ whether `isolation` should be excluded from the hash and
   carried as deploy metadata instead, if it turns out that two deployments
   differing only in isolation should be considered the "same" workflow for
   audit/dedup purposes. The default position is "include it"; revisit if dedup
   semantics argue otherwise.

### Recursive-host considerations and sharp edges (flagged, not papered over)

5. **Per-step / per-node differing isolation — RESOLVED (was a non-goal).** The
   earlier revision flagged "a workflow whose steps need different isolation" as
   a conscious non-goal. The recursive model **resolves it**: a step,
   `childWorkflow`, or `map` branch declares its own `isolation` (§3f), and a
   stricter declaration spawns a **nested sandboxed sub-child** for just that
   node while the rest of the workflow stays in-process. The earlier worry — "a
   single `runtimeRun` would span two boundaries" — does not arise, because the
   isolated node runs as its **own** `runtimeRun` rung (the existing
   `spawnChild` recursion), not as a foreign-boundary step inside the parent's
   run. The split-into-separate-workflow workaround is no longer required; the
   author declares per-node isolation inline. This is the main thing the
   recursive refinement buys.

6. **Trust inversion: a stricter grandchild whose writes proxy through a
   less-trusted parent.** This is the sharpest edge the recursive model
   introduces, and §3d-bis's substrate-write routing decision exists to defuse
   it. If a sandboxed sub-child (isolated _because_ its work is untrusted, or
   because the parent is untrusted) routed its substrate writes **through** its
   parent rung, a less-trusted intermediate would sit on the write path of a
   more-isolated descendant — able to observe or tamper. The design's answer:
   **sandboxed rungs write direct-to-rung-0-supervisor** (the single writer),
   bypassing intermediates; only **in-process** rungs share the parent proxy
   (and an in-process rung is already inside the parent's trust boundary, so
   there is no inversion). _Open question / residual risk:_ the direct-to-writer
   channel means the rung-0 supervisor must authenticate and authorize each
   sandboxed rung's principal independently, and maintain a write channel per
   sandboxed rung. Whether the supervisor's principal model
   (`WorkflowRunWorkflowProcessPrincipal` scoped to the deployment) is granular
   enough to distinguish rungs — or whether a sandboxed rung needs its own
   principal so a compromised rung cannot forge a sibling's writes — is an open
   question that must be settled before the hardened-boundary phase (Phase 6).

7. **Process/resource count under deep nesting and `map` fan-out.** With
   per-node isolation, a deeply nested or high-fanout workflow can spawn **many**
   sandboxed children: a `map` over N items with the inner step declared
   isolated spawns up to N sandboxed sub-children, each with its own
   process/fd/LSP/tarball-cache cost (THE CONTRACT, §3d-bis). The in-process
   default bounds this (no isolation declared => one process), but a workflow
   that declares isolation on a wide `map` is the worst case. _Open question:_
   the design does **not** yet specify a **depth ceiling** or a **fanout
   concurrency cap** on sandboxed sub-children. Both are probably needed before
   Phase 6 (a runaway `map` of isolated steps could exhaust host processes/fds);
   the cap likely belongs as an operator policy alongside the isolation floor
   (§3f). Flagged as an open question, not designed here.

8. **Credentials snapshot / grants flow to a sandboxed sub-child.** The rung-0
   supervisor assembles the credentials snapshot and pushes it over the control
   IPC (`assembleCredentialsSnapshot`, `grants-updated`). For an **in-process**
   rung the snapshot is shared from the parent (same process). For a **sandboxed**
   rung, the sub-child needs its own `grants-updated` push over its own control
   channel — and the grants it receives must be the node's grants, not the
   parent's (a stricter-isolation node may intentionally run with a narrower
   grant set). _Open question:_ whether per-node grants are assembled by rung 0
   (consistent single authority, more channels) or delegated to the parent rung
   (fewer channels, but reintroduces the trust inversion for grants). The
   direct-to-writer decision (sharp edge 6) argues for rung-0 assembling per-node
   grants too. Settle with the principal-model question before Phase 6.

9. **Event-firehose latency up multiple sandboxed rungs.** An `InferenceEvent`
   from a grandchild chains up through each rung to the hub timeline (§3d-bis).
   For in-process rungs this is a function call (negligible). For **sandboxed**
   rungs each hop is an IPC forward, so a deeply-nested isolated workflow adds
   per-hop latency to the firehose. Acceptable for audit/observability (events
   are not on the request path), but flagged so deep nesting's observability lag
   is a known property, not a surprise.

### Isolation lifecycle sharp edges

10. **A shared child cannot safely _downgrade_ isolation in place.** Once a
    per-tenant child is hosting N agents, a new deploy that requires _stricter_
    isolation (per-agent, or a stricter sandbox boundary) must get its **own**
    child — it cannot join the shared one. That is handled correctly by the
    domain-key derivation (a stricter declaration produces a different key, so it
    misses the shared child and spawns its own). The sharp edge is the reverse:
    a deploy that _relaxes_ isolation (per-agent -> per-tenant) for an agent that
    already has a dedicated child does not retroactively fold it into the shared
    child — it keeps its own until torn down and re-deployed. This is acceptable
    (relaxation is not a safety concern and converges on next deploy) but is
    called out so "isolation changes apply on next deploy, not live" is an
    explicit contract. _Open question:_ whether a live isolation change should
    force a recycle to converge immediately, or whether next-deploy convergence
    is sufficient (the design assumes the latter).

### Alternatives rejected

- **Harness-beside-runtime (path 1).** Run the existing harness, with its own
  transport subscription, next to the workflow runtime in the child. Rejected:
  process unification without the durability dividend; leaves two
  mail-ingestion paths alive in one process. (§3a.)
- **In-process-everything.** Keep the in-process harness as the one model and
  have the workflow runtime drive steps in-process in the sidecar, never
  spawning a child for single agents. Rejected as the _end state_ because it
  gives up the durable, resumable, audited execution that the child model
  provides and that multi-step workflows require — but **retained as the
  fallback** if Phase 4 shows the mailbox round-trip latency is unacceptable
  for interactive agents. If that happens, the honest answer may be a hybrid:
  multi-step on the child, single interactive agents in-process. This is the
  most likely thing that makes the project bigger than scoped.
- **Always-spawn-naive.** Route the single agent through the existing
  instantiate-send-teardown step-invoker with no warm-keep. Rejected:
  re-materializes tools and re-spawns LSP per message; loses conversation
  continuity. (§3b.)

### What could make this bigger than scoped

- The mailbox round-trip latency forcing a single-agent fast-path or a
  hybrid (the in-process fallback above). **This is the biggest scope risk.**
- LSP lifecycle in the child proving to need real supervision.
- Conversation-state durability granularity forcing a substrate-write-volume
  redesign on the single-writer ref.
- The hardened sandbox boundaries (`os-namespace` / `oci-container`) and the
  recursive direct-to-writer / per-rung-IPC work — a deliberately **deferred,
  separate initiative** (Phase 6; the abstraction + contract + in-process
  recursive honoring ship now on the plain-subprocess default). If a security
  requirement forces a hardened boundary into INTR-209's scope, the
  image/IPC-bridging work plus the trust-inversion / principal-model / depth-cap
  open questions (sharp edges 6–8) are substantial. Keep Phase 6 out of the
  unified-host build unless explicitly required.

### What I am least sure about

Two things, both empirical and both deferred past the INTR-209 base build:

- **The mailbox-ownership latency tradeoff (§3a).** Whether an interactive
  single agent can tolerate the durable inbox/dispatch round-trip on every
  message is an empirical question I cannot answer from the code. **Phase 4 must
  measure this before Phase 5 commits to deleting the in-process runtime.** If
  unacceptable, the in-process-everything fallback (hybrid) becomes the honest
  end state, narrowing the goal from "one host for everything" to "one host for
  durable/multi-step work; in-process for interactive single agents."
- **The recursive trust/principal model (sharp edges 6 + 8).** The
  direct-to-writer routing for sandboxed rungs is the right _shape_, but whether
  the supervisor's principal model can distinguish and authorize per-rung writes
  and per-rung grants — so a compromised sandboxed rung cannot forge a sibling's
  writes or read a sibling's grants — is unsettled. This only bites at Phase 6
  (hardened boundaries); the in-process base build does not hit it (in-process
  rungs are inside the parent's trust boundary by construction). But it must be
  designed before any sandboxed rung ships, and I am not yet sure the existing
  `WorkflowRunWorkflowProcessPrincipal` (scoped to the deployment, not the rung)
  is granular enough.

### Sanity-check: do the recursive refinement and the earlier decisions still hold?

The operator asked whether the recursion breaks the two load-bearing earlier
decisions. It does not — confirmed against the code:

- **Mailbox ownership (§3a) holds.** The recursion is **below the mail layer**.
  Only the **rung-0** supervisor owns mail (`mailBus.registerAddress` /
  `subscribeMailForAddress`, the inbox claim-check, the dispatch loop). A
  sub-child at any rung is driven by **`spawnChild`** (the runtime's
  `SpawnChildWorkflow` seam invoked when `runtimeRun` reaches a `childWorkflow` /
  `map` node), **not** by inbound mail — `createSidecarRunChild`'s `runChild`
  takes a materialized `input` and a parent-allocated `childRunId`, never a mail
  subscription. So there is still exactly one mailbox owner regardless of nesting
  depth; the single-owner invariant is untouched.
- **Warm-agent lifecycle (§3b) holds, with a clarification.** Warm-keep applies
  to the **rung-0 long-lived single agent** (the one driven by repeated
  `trigger.fired` from mail). Per-node sub-children are **per-invocation**: a
  `childWorkflow` / `map` node spawns its sub-child when `runtimeRun` reaches the
  node and tears it down when that node's run reaches a terminal phase
  (`createSidecarRunChild`'s `runChild` already `stop()`s the per-run signal
  channel in a `finally`). They are not warm-kept across invocations, which is
  correct — a fan-out branch or a one-shot child workflow has no long-lived mail
  loop to warm-keep for. So warm-keep (long-lived, rung 0) and per-invocation
  sub-children (rung ≥ 1) coexist without conflict.

No earlier decision is broken by the recursion. The recursion sits strictly
below mail and strictly below the rung-0 warm agent; it reuses the existing
`spawnChild` seam and only adds the per-rung pluggable boundary on top.

## 7. Verification and acceptance

The unified host is proven by four classes of check:

1. **Existing INTR-209 fixture, re-run on the unified host.** The green
   `tests/workflow-deploy/multistep-signal.test.ts` fixture (and the sidecar
   `workflow-host-wiring*.test.ts` suite) must pass unchanged against the
   unified host — multi-step deploy/dispatch/signal/drain/resume must continue
   to work, now with **real** step output instead of placeholder. A passing
   fixture with real step execution is the multi-step acceptance gate.

2. **New warm-agent durability integration tests** (spawned child, test
   inference provider):
   - Two sequential messages reuse one warm agent; conversation state
     preserved.
   - Multi-turn conversation produces a durable, resumable workflow-run log.
   - Kill + respawn mid-conversation: `discoverInFlightRuns` resumes and
     conversation state is restored from the substrate.
   - Identity: the agent's `agent_instance` row stays at `ins_<hex>`;
     `isWorkflowDerivedAddress` returns `false`; the deploy-ack listener
     persists the public key; grants resolve from `state/grants.json` and an
     authorize call that depends on a granted resource succeeds while an
     ungranted one fails closed.

3. **Multiplex + workflow-declared isolation integration tests.**
   - One child hosts two deployments' runs concurrently; assert no
     cross-contamination of run logs, conversation state, or grants
     (sub-namespacing holds), and that one run's failure does not corrupt the
     other.
   - Two `per-tenant` workflows for one tenant resolve to the **same** child
     (shared domain key); a `per-agent` workflow gets its **own** child —
     assert the domain-key -> child mapping.
   - `defineWorkflow` unit tests: absent workflow-level `isolation` defaults to
     per-tenant; absent per-node isolation inherits in-process; unknown
     `granularity`/`sandbox` at any occurrence is rejected; every occurrence
     round-trips through `hashDefinition`.
   - `SandboxBoundary` selection returns the `host-subprocess` spawner for the
     default (the hardened boundaries are out of scope for INTR-209 and are not
     gated here).

4. **Recursive in-process honoring integration tests** (the base-build recursive
   behavior; hardened boundaries deferred to Phase 6):
   - A `childWorkflow` / `map` node with **no** stricter isolation runs
     in-process; its events and writes land under `runs/<childRunId>/...` of the
     rung-0 workflow-run repo (sub-namespacing across depth holds), and the
     single writer remains the rung-0 supervisor.
   - A node declaring a **stricter** boundary (with hardened boundaries
     unimplemented) surfaces the structured "sandboxed sub-child required but not
     yet implemented" error — **not** a silent in-process downgrade (an
     isolation-policy violation must fail loud, per Phase 4c).
   - The per-child sub-child registry tears a sub-child down when its node's run
     terminates; rung-0 recycle/drain cascades to nested rungs.

Acceptance for go-live: all four classes green, the in-process runtime symbols
have no remaining callers (build-enforced), and the Phase-4 latency measurement
is within an agreed interactive budget (or the hybrid fallback is explicitly
adopted with the operator's sign-off). The hardened per-rung boundaries (Phase 6)
are explicitly **not** a go-live gate for INTR-209.

## Appendix: stable reference index

- In-process runtime: `packages/hub-agent/src/session-manager.ts`
  (`provisionAgent`, `createSessionManager`, `startSession`, `restoreSessions`);
  `packages/hub-agent/src/harness-builder.ts` (`HarnessBuilder`,
  `BuildHarnessArgs`, `HarnessBundle`).
- Concrete harness builder: `apps/sidecar/src/default-harness.ts`
  (`materializeToolPackages`, `build`).
- Harness runtime: `packages/harness/src/harness.ts` (`createHarness`,
  `Harness`, `MailEnv`, `MailToolWrapper`, `createWrappedStorageOverrides`,
  connector router).
- Agent runtime: `packages/agent/src/env.ts` (`BaseEnv`);
  `@intx/agent` (`defineAgent`, `createAgent`).
- Step-invoker: `packages/workflow-host/src/adapters/step-invoker.ts`
  (`createWorkflowStepInvoker`, `WorkflowStepInvokerOpts`, `invokeStep`).
- Supervisor: `packages/workflow-host/src/supervisor/supervisor.ts`
  (mail ingestion, dispatch loop, `spawn`); `supervisor/credentials.ts`
  (`assembleCredentialsSnapshot`, `deriveStepAddress`, `deriveStepRepoId`,
  `STEP_GRANTS_PATH`, `defaultStepRepoId`); `supervisor/types.ts`
  (`TrivialLaunch`, `SupervisorDeployFrame`); `supervisor/recycle.ts`;
  `drain-controller.ts`.
- Child: `packages/workflow-host/src/child/run-child.ts`
  (`runWorkflowChild`, `loadWorkflowDefinition`, `discoverInFlightRuns`,
  event sender, `trigger.fired` handling).
- Mail bus: `packages/workflow-host/src/mail-bus/hub-transport-adapter.ts`
  (`routeInbound`, `subscribeMailForAddress`).
- Substrate factory: `apps/sidecar/src/workflow-substrate-factory.ts`
  (`createSidecarSubstrateFactory`, `createSidecarStepBuildEnv`,
  `baseInvokeStep`, `childInvokeStep`, `SIDECAR_SUBSTRATE_CONFIG_KEYS`,
  `STEP_INFERENCE_SOURCES`, `listActiveDeployments`, sub-namespacing).
- Child binary (the child _is_ the sidecar binary): `apps/sidecar/bin/workflow-child`
  (imports `createSubstrate` + `SIDECAR_SUBSTRATE_CONFIG_KEYS`, runs
  `runWorkflowChildFromProcessEnv`).
- Deploy router / wiring: `apps/sidecar/src/workflow-host-wiring.ts`
  (`createSidecarDeployRouter`, `deployMultiStep`, `activeSupervisors`,
  `deriveDeploymentId`, `driveTrivialRunChain`, `TrivialRunCell`,
  `publishWorkflowInferenceEvent`).
- Child-launch / sandbox-boundary seam (Decision 1):
  `apps/sidecar/src/workflow-host-wiring.ts` (`SubprocessSpawner`,
  `SubprocessHandle`, `defaultSubprocessSpawner`, `binaryPath`,
  `SIDECAR_WORKFLOW_CHILD_BINARY`; `multistepSubprocessSpawner` /
  `multistepBinaryPath` threaded through `createSidecarDeployRouter`).
- Workflow-definition isolation surface (Decision 2):
  `packages/workflow/src/definition/workflow.ts` (`WorkflowDefinition`,
  `WorkflowConfig`, `SingularWorkflowConfig`, `defineWorkflow`, `normalize`,
  `hashDefinition`, `projectForHash`, `projectPrimitive`, `projectAgent` —
  workflow-level `isolation` added here).
- Per-node isolation surface (recursive refinement):
  `packages/workflow/src/definition/primitives.ts` (`PrimitiveBase`,
  `StepPrimitive`/`StepOpts`, `ChildWorkflowPrimitive`/`ChildWorkflowOpts`,
  `MapPrimitive`/`MapOpts` — per-node `isolation` added to the spawn-trigger
  primitives and their constructors).
- Recursive spawn / per-rung host (already exists; the recursion point):
  `apps/sidecar/src/workflow-substrate-factory.ts` (`createSidecarRunChild`
  self-referential `runChild`, sub-namespace `runs/<runId>/...`, proxy
  `RepoStore`); `packages/workflow-host/src/adapters/spawn-child.ts`
  (`createWorkflowSpawnChild`, `SpawnChildWorkflow`, `RunChildWorkflow`,
  `WorkflowSpawnChildOpts`); child-process principal
  `WorkflowRunWorkflowProcessPrincipal`.
- Identity: `packages/workflow-deploy/src/orchestrator.ts`
  (`deriveWorkflowRunRepoId`, `isWorkflowDerivedAddress`,
  `wrapHarnessAsSingleStepWorkflow`); `packages/types/src/agent-address.ts`
  (`formatAgentAddress`, `parseAgentAddress`);
  `packages/hub-sessions/src/hub-session-orchestrator.ts` (deploy-ack listener);
  `packages/hub-sessions/src/hub-session-lookups.ts` (`findInstance`,
  `lookupPublicKey`).
- Grants bridge (reusable):
  `dispatch/workflow-launch-and-converge/8a-route_single_step_via_child/8a-groundwork.patch`.
- Transport interface: `packages/types/src/runtime.ts` (`MessageTransport`).
- Tool packaging: `@intx/tool-packaging` (`createToolLoader`, `applyAtomic`,
  `createTarballCache`).
- INTR-209 fixture: `tests/workflow-deploy/multistep-signal.test.ts`.
