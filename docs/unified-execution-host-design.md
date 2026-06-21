# Unified Execution Host Design

> **DESIGN PROPOSAL — not yet implemented.**
>
> This document describes a target architecture and a build path for it. No
> part of it has been built. It is the artifact the operator reviews for a
> go/no-go decision on INTR-209 and to guide the build once approved.
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

- **One extensible execution host.** The workflow-process child is the single
  place real agents run, hosting both a long-lived single agent and a
  multi-step workflow, multiplexed.
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

```
                              HUB
   deploy / mail / signal / drain          agent.event / mail.persisted
        frames  |   ^  pack push                |  ^  timeline
                v   |                            v  |
 ┌───────────────────────────────────────────────────────────┐
 │                         SIDECAR                             │
 │                                                            │
 │  hub-link ── DeployRouter (workflow-host-wiring.ts)        │
 │                 │  registers per isolation-domain          │
 │                 v                                          │
 │        ┌──────────────────────────────────────────┐       │
 │        │  SUPERVISOR (per isolation-domain)        │       │
 │        │  - owns mail ingestion for every address  │       │
 │        │  - inbox claim-check substrate (durable)  │       │
 │        │  - FIFO dispatch loop -> trigger.fired     │       │
 │        │  - credentials snapshot push              │       │
 │        │  - recycle / drain / terminal broadcast   │       │
 │        │  - SINGLE WRITER of workflow-run repo(s)  │       │
 │        └───────────────┬──────────────────────────┘       │
 │            control IPC │ event channel (fd3, HMAC)         │
 │           (NDJSON,     │ ^ InferenceEvents                 │
 │            Ed25519)    v │                                 │
 │        ┌──────────────────────────────────────────┐       │
 │        │  CHILD (workflow-process)                 │       │
 │        │  - runtimeRun per trigger.fired           │       │
 │        │  - REAL step-invoker: createAgent +       │       │
 │        │      tools + inference + event firehose   │       │
 │        │  - tool materialization (moved in)        │       │
 │        │  - warm-agent cache (long-lived single)   │       │
 │        │  - proxy RepoStore (writes -> supervisor) │       │
 │        │  - discoverInFlightRuns resume            │       │
 │        │  - hosts N runs / M deployments (mux)     │       │
 │        └──────────────────────────────────────────┘       │
 │                 shared on-disk substrate (data dir)       │
 └───────────────────────────────────────────────────────────┘
```

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

The two workloads converge on one host, one step-invoker, one durability
model. The single agent becomes "a long-lived, warm-kept, one-step workflow."

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

### 3e. Event threading

Today the real step-invoker drops events: the substrate factory's `invokeStep`
wrapper takes `(req, onEvent)` and `void onEvent`s it
(`apps/sidecar/src/workflow-substrate-factory.ts`), with the comment "the
event funnel inside the adapter lands when the harness's emit hook is wired."
The transport for events already exists end-to-end:

- Child emits via `createEventChannelSender` (HMAC over fd3) in
  `packages/workflow-host/src/child/run-child.ts`.
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

#### The `isolation` field on `WorkflowDefinition`

Add a first-class, optional `isolation` declaration to `WorkflowDefinition`
(`packages/workflow/src/definition/workflow.ts`):

```ts
type IsolationGranularity = "per-tenant" | "per-agent";

interface WorkflowIsolation {
  /** Child-process sharing granularity. Default "per-tenant". */
  granularity: IsolationGranularity;
  /**
   * Optional hint for the sandbox boundary (§3d-bis / Decision 1) this
   * workflow's child should run inside. Advisory: the sidecar's
   * SandboxBoundary selection and operator policy resolve the actual
   * mechanism. Absent => the host default ("host-subprocess" until the
   * hardened boundaries land).
   */
  sandbox?: "host-subprocess" | "os-namespace" | "oci-container";
}

interface WorkflowDefinition {
  // ...existing fields...
  isolation?: WorkflowIsolation;
}
```

- **Shape and defaulting.** `isolation` is optional on the authoring config
  (`WorkflowConfig` / `SingularWorkflowConfig`). `defineWorkflow` normalizes an
  absent value to the default `{ granularity: "per-tenant" }`, so the
  normalized `WorkflowDefinition` always carries a concrete isolation (no
  read-site defaulting downstream — the boundary resolves it once, per the
  defaults-at-the-edge rule). The single-agent launch path
  (`wrapHarnessAsSingleStepAgent` building the one-step definition) sets the
  field from the launch request / agent policy; absent => per-tenant.
- **Validation.** `defineWorkflow`'s `normalize` validates `granularity`
  against the allowed set and `sandbox` (when present) against the known
  boundary names, throwing a structured error on an unknown value — the same
  loud-failure posture the rest of `normalize` uses.
- **Hashing.** `isolation` flows into `hashDefinition` via `projectForHash`
  (`packages/workflow/src/definition/workflow.ts`). It is part of the
  content-addressed definition: two deployments that differ only in declared
  isolation are different definitions, which is correct — isolation is a
  deploy-affecting property, and the `RunStarted` `definitionHash` should
  reflect it. (This is a definition-surface/versioning consideration; see §6.)

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

- `WorkflowDefinition.isolation` field + `defineWorkflow` defaulting/validation
  - `projectForHash` inclusion (`packages/workflow/src/definition/workflow.ts`),
    per Decision 2.
- The `SandboxBoundary` strategy abstraction + THE CONTRACT (tool runtime lives
  inside the boundary), per Decision 1. Concrete `os-namespace`/`oci-container`
  spawners are deferred (later phase, §5).
- The DeployRouter isolation-domain key derivation
  (`apps/sidecar/src/workflow-host-wiring.ts`, generalizing `activeSupervisors`
  from per-deployment-address to per-isolation-domain), per Decision 2.

### Trivial-named symbols' fate

Every surviving "trivial"-named symbol is renamed to reflect that the
single-step identity path is the canonical single-agent deploy, not a "trivial"
special case — or deleted where the in-process branch it served is gone:

- `wrapHarnessAsTrivialAgent` -> `wrapHarnessAsSingleStepAgent`
  (`packages/workflow-deploy/src/orchestrator.ts`), still used to build the
  one-step definition.
- `buildTrivialApprovalSet` -> `buildSingleStepApprovalSet`
  (`packages/hub-sessions/src/session-service.ts`).
- `isTrivialDeploy` / `trivialBindings` / the trivial orchestrator branch:
  **deleted** if single-step routes through the multi-step branch (likely);
  otherwise renamed `isSingleStepDeploy`. Confirm during build.
- `deriveTrivialDeploymentId` (`apps/sidecar/src/workflow-host-wiring.ts`):
  deleted; call `deriveWorkflowRunRepoId` directly at the call sites (it is a
  thin delegator).
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

### Phase 4b — Isolation field + sandbox-boundary abstraction (pure additions)

These two pieces are designed now and land as **pure additions** before the
multiplex phase consumes them. They do not change runtime behavior on their own
(the default resolves to today's per-deployment / `host-subprocess` behavior),
so they are low-risk and can land any time after the field is needed by the
DeployRouter generalization.

- **Changes.** (1) Add `WorkflowDefinition.isolation` with `defineWorkflow`
  defaulting/validation and `projectForHash` inclusion
  (`packages/workflow/src/definition/workflow.ts`), Decision 2. (2) Introduce
  the `SandboxBoundary` strategy + THE CONTRACT around the existing
  `SubprocessSpawner`/`binaryPath` seam (`apps/sidecar/src/workflow-host-wiring.ts`),
  Decision 1, with only the `host-subprocess` implementation (the existing
  `defaultSubprocessSpawner`) wired. No `os-namespace`/`oci-container` yet.
- **Verification.** (1) `defineWorkflow` defaults absent `isolation` to
  `{ granularity: "per-tenant" }`; rejects an unknown `granularity`/`sandbox`;
  the value round-trips through `hashDefinition` (two definitions differing only
  in `isolation` hash differently). (2) The `SandboxBoundary` selection returns
  the `host-subprocess` spawner for the default and the build is unchanged from
  Phase 3 behavior.
- **Riskiest sub-item.** Hashing inclusion is a definition-surface/versioning
  change (§6) — confirm no existing fixture hard-codes a definition hash that
  the new field would shift.

### Phase 5 — Workflow-declared multiplex + retire the in-process runtime

- **Changes.** Generalize the substrate factory + supervisor registry from one
  workflow-run repo to a set; replace the per-deployment-address
  `activeSupervisors` key with the **isolation-domain key computed from
  `WorkflowDefinition.isolation`** (Decision 2: per-tenant => key by tenant,
  per-agent => key by deployment; `sandbox` hint participates; operator floor
  applied). Wire child select/reuse in the DeployRouter. Delete
  `provisionAgent` / `createSessionManager` / `default-harness.ts`
  transport-reactor ownership / the trivial branch + projector / `TrivialLaunch`.
  Rename surviving trivial-named symbols (§4).
- **Verification.** (1) Two per-tenant workflows for one tenant share a child;
  a per-agent workflow gets its own — assert the `activeSupervisors`/domain-key
  mapping. (2) One child hosts two deployments' runs without cross-contamination
  (sub-namespacing holds). (3) Full INTR-209 fixture re-run on the unified host
  (§7). (4) The retired symbols have no remaining callers (build proves it).

**Milestone where steps first do real work:** end of **Phase 2** — workflow
steps run real agents with real tools, making INTR-209 genuinely functional
(no more placeholder output) even though shipping waits for Phase 5.

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

### Sharp edges in per-workflow isolation (flagged, not papered over)

5. **A multi-step workflow whose steps need _different_ isolation.** The
   `isolation` field as designed is **per-workflow**, not per-step. A workflow
   with one untrusted shell step and several trusted steps cannot today say
   "isolate only the shell step." The whole workflow's child takes the strictest
   declared granularity, or the author splits the untrusted work into a separate
   `per-agent` workflow. _Open question:_ whether per-step isolation is needed.
   The design deliberately does **not** add per-step isolation now — it is a
   substantial complication (a single `runtimeRun` would span two boundaries,
   breaking the "one child per run" model), and the split-into-separate-workflow
   workaround covers the motivating case. Flagged so the limitation is a
   conscious choice, not an oversight.

6. **A shared child cannot safely _downgrade_ isolation in place.** Once a
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
- The hardened sandbox boundaries (`os-namespace` / `oci-container`) — these are
  a deliberately **deferred, separate initiative** (the abstraction + contract
  ship now on the plain-subprocess default), but if a security requirement
  forces a hardened boundary into INTR-209's scope, the image/IPC-bridging work
  (item 3 above) is substantial. Keep it out of the unified-host build unless
  explicitly required.

### What I am least sure about

The mailbox-ownership latency tradeoff (§3a). The architecture is clean and
the durability dividend is real, but whether an interactive single agent can
tolerate the durable inbox/dispatch round-trip on every message is an
empirical question I cannot answer from the code. **Phase 4 must measure this
before Phase 5 commits to deleting the in-process runtime.** If the latency is
unacceptable, the in-process-everything fallback (hybrid) becomes the honest
end state, and the design goal narrows from "one host for everything" to "one
host for durable/multi-step work; in-process for interactive single agents."

## 7. Verification and acceptance

The unified host is proven by three classes of check:

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
   - `defineWorkflow` unit tests: absent `isolation` defaults to per-tenant;
     unknown `granularity`/`sandbox` is rejected; the field round-trips through
     `hashDefinition`.
   - `SandboxBoundary` selection returns the `host-subprocess` spawner for the
     default (the hardened boundaries are out of scope for INTR-209 and are not
     gated here).

Acceptance for go-live: all three classes green, the in-process runtime
symbols have no remaining callers (build-enforced), and the Phase-4 latency
measurement is within an agreed interactive budget (or the hybrid fallback is
explicitly adopted with the operator's sign-off).

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
  `deriveTrivialDeploymentId`, `driveTrivialRunChain`, `TrivialRunCell`,
  `publishWorkflowInferenceEvent`).
- Child-launch / sandbox-boundary seam (Decision 1):
  `apps/sidecar/src/workflow-host-wiring.ts` (`SubprocessSpawner`,
  `SubprocessHandle`, `defaultSubprocessSpawner`, `binaryPath`,
  `SIDECAR_WORKFLOW_CHILD_BINARY`; `multistepSubprocessSpawner` /
  `multistepBinaryPath` threaded through `createSidecarDeployRouter`).
- Workflow-definition isolation surface (Decision 2):
  `packages/workflow/src/definition/workflow.ts` (`WorkflowDefinition`,
  `WorkflowConfig`, `SingularWorkflowConfig`, `defineWorkflow`, `normalize`,
  `hashDefinition`, `projectForHash` — the `isolation` field is added here).
- Identity: `packages/workflow-deploy/src/orchestrator.ts`
  (`deriveWorkflowRunRepoId`, `isWorkflowDerivedAddress`,
  `wrapHarnessAsTrivialAgent`); `packages/types/src/agent-address.ts`
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
