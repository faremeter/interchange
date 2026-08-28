# Interchange

[![CI](https://github.com/faremeter/interchange/actions/workflows/ci.yml/badge.svg)](https://github.com/faremeter/interchange/actions/workflows/ci.yml)
[![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)](#project-status)
[![Version](https://img.shields.io/github/v/tag/faremeter/interchange?sort=semver&label=version&color=blue)](https://github.com/faremeter/interchange/tags)
[![License: LGPL-2.1](https://img.shields.io/badge/license-LGPL--2.1-green.svg)](./LICENSE)
[![Runtimes: Bun | Node | Deno](https://img.shields.io/badge/runtimes-Bun%20%7C%20Node%20%7C%20Deno-black.svg)](#runtime-support)

**Run AI agents as first-class principals — with their own identity,
permissions, and credentials — designed to run anywhere from a
long-lived server to a Cloudflare Worker.**

_An agentic operating system._

Most agent code today is a script in a loop calling a model API. That
works until you need to run someone _else's_ agent on their behalf:
now you need identity, scoped permissions, credential management, an
audit trail, and a runtime that survives a crash. Interchange is that
layer.

The same authorization engine that gates an agent's API calls also
gates which **tools** it is allowed to invoke — so "what this agent
is allowed to do" is one enforced policy, not something bolted on
after.

What you build on top is up to you:

- **A coding assistant** that reads, writes, and reasons about a real
  repository
- **A mail-driven workflow agent** that acts on its own inbox
- **An autonomous trader**
- **A research harness** of agents that talk to each other

> [!WARNING]
> **Interchange is alpha.** The architecture is settled and
> the stack runs real workloads, but public APIs and wire formats can
> still change between releases. Pin your versions, and see
> [Project status](#project-status) for what's solid and what's still
> moving.

## Contents

- [Why Interchange](#why-interchange)
- [Runtime support](#runtime-support)
- [Quickstart](#quickstart)
- [How it works](#how-it-works)
- [Project status](#project-status)
- [Documentation](#documentation)
- [Development](#development)
- [Reference](#reference)
- [License](#license)

## Why Interchange

An agent runtime is easy to prototype and hard to make _safe to run on
someone's behalf_ — across many machines, surviving crashes, and
answerable for what it did. Interchange is the layer that makes an
agent a managed principal instead of a loose script:

- **One authorization model for users and agents.** Principals —
  people _and_ agents — live under a single grant engine
  ([`@intx/authz`](./packages/authz)). The same policy decides what an
  agent's API calls may touch and which tools it may invoke. The agent
  never sees the policy: a blocked tool call returns a generic refusal,
  not the rule that stopped it.
- **Delegated authority, setuid-style.** An agent's capabilities are
  resolved at launch from two delegated sources — the definition's
  **creator** and the **invoker** — within the boundary of the
  **tenant's** policy. An agent can perform privileged work on behalf
  of a user who couldn't do it directly (query a production database,
  trigger a deploy) without ever handing that user the keys. Trust is
  explicit, auditable, and revocable.
- **Verifiable identity, managed credentials.** Each agent gets its own
  Ed25519 key pair at launch and signs its git commits and its messages
  with it — `git verify-commit` works out of the box. Credentials are
  resolved and pushed to where the agent runs rather than baked into
  it, and the agent itself never touches key material.
- **Git-backed state you can audit, resume, and rewind.** An agent's
  context is a real git repository, auto-committed every cycle. Crash
  and re-run: it picks up where it left off. Branch it to explore an
  alternative, or roll `HEAD` back to open the agent at an earlier
  state. The audit log is not a separate artifact — it is the commit
  history.
- **Distributed by design.** A control-plane **hub** orchestrates;
  portable **harnesses** run the agents. The same core ships once and
  runs as a local or server process today, with container and VM hosts
  in flight and Cloudflare Workers, edge, and the browser following as
  the cross-runtime work lands. The hub brokers session channels, so
  harnesses need no public address and work behind NAT, and it tracks
  the health of every running agent.
- **Durable by default.** Conversations persist to a git-backed
  mailbox and agent state is committed locally. When a harness
  restarts, it proves ownership of each agent by signing a challenge
  with the agent's key, resumes from persisted state, and flushes any
  messages that queued while it was gone. Nothing is lost to a restart.
- **Model-agnostic inference with failover.** A definition declares the
  models it needs; the hub resolves an ordered provider list at launch.
  Inference fails over per call across providers _and_ protocols
  (Anthropic ↔ OpenAI-compatible ↔ Gemini), and the provider list is
  hot-swappable on a running agent without interrupting in-flight
  calls.
- **Multi-tenant isolation.** Agents, data, credentials, and message
  buses are tenant-scoped and isolated by default, and tenants nest
  into policy-inheriting hierarchies. Cross-tenant _federation_ —
  discovery and invocation across trust boundaries — is designed and
  partly scaffolded, but not yet wired end-to-end (see
  [Project status](#project-status)).

Where it can, Interchange builds on established formats rather than
inventing its own — OpenAPI for the hub's HTTP surface, SSH-format
signatures that `git verify-commit` understands, and MIME for messages.
Broader protocol integrations — MCP and A2A offering discovery, JMAP
messaging, DID identity — are on the [roadmap](#project-status), not in
the tree yet.

## Runtime support

The portable agent-runtime core targets every JavaScript runtime. Full
cross-runtime coverage is verified on each release; Bun additionally
gates every commit through CI.

| Runtime                | Status         | Verified                    |
| ---------------------- | -------------- | --------------------------- |
| **Bun** 1.2+           | ✅ Supported   | Every commit (CI) + release |
| **Node.js**            | ✅ Supported   | Every release               |
| **Deno**               | ✅ Supported   | Every release               |
| **Cloudflare Workers** | 🚧 In progress | —                           |
| **Browser**            | 🚧 In progress | —                           |

**Legend.** ✅ Supported — exercised by the test suite and confirmed
green on that runtime. Bun is the primary development runtime and runs
on every commit through [CI](./.github/workflows/ci.yml); Node.js and
Deno are checked on every release. 🚧 In progress — the portable core
is built for these targets (no Node built-ins, no filesystem
assumptions), and the remaining work is tracked under
[Project status](#project-status).

Bun 1.2+ is the supported runtime for local development of the full
stack; see [`DEV.md`](./DEV.md).

## Quickstart

### The smallest possible agent

Define an agent, build its environment, send a prompt, close. The
full file is
[`examples/agent-quickstart`](./examples/agent-quickstart/README.md);
this is its shape:

```ts
import {
  createAgent,
  createDefaultDirectorRegistry,
  defineAgent,
} from "@intx/agent";
import { noopAuditStore, permissiveAuthorize } from "@intx/agent/testing";
import { createIsogitStore } from "@intx/storage-isogit/node";

const def = defineAgent({
  id: "quickstart",
  systemPrompt: "You are a helpful assistant. Keep replies concise.",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "claude-sonnet-5" }],
  },
});

const agent = await createAgent(def, {
  source, // resolved inference source: provider, model, and API key
  storage: await createIsogitStore(contextDir),
  workdir: contextDir,
  audit: noopAuditStore(),
  authorize: permissiveAuthorize(),
  directors: createDefaultDirectorRegistry(),
});

const { reply } = await agent.send("Name three planets.");
console.log(reply);
await agent.close();
```

`agent.send()` and `agent.close()` are the only two methods you have
to know. The `contextDir` is a real git repository — re-run against
it and the conversation picks up where it left off. More runnable
consumers live in [`examples/`](./examples/README.md).

### Run the full stack

From a fresh clone, point git at the repo hooks, install workspace
dependencies, build, then reset the database and start the hub,
sidecar, and admin UI with seed data:

```bash
git config core.hooksPath .githooks   # required by the environment check
bun install                           # workspace deps and @intx/* symlinks
make                                   # lint, build, admin-ui bundle, test
bin/db-reset && bin/dev --seed         # reset the db, then start the stack
```

Entry points are in [`apps/`](./apps). Requires
[Bun](https://bun.sh/) 1.2+, [git](https://git-scm.com/) 2.34+, and
PostgreSQL 15+. See [`DEV.md`](./DEV.md) for everything else —
environment files, role setup, default ports, seed credentials,
partial-stack variants, and reset recipes.

## How it works

Two halves, sharing one set of type definitions in
[`@intx/types`](./packages/types):

```
  ┌─────────────────────┐                    ┌────────────────────────────────┐
  │         Hub         │  ◀── events ──▶    │            Sidecar             │
  │    control plane    │                    │    harness + agent runtime     │
  │                     │                    │                                │
  │  tenants            │                    │  storage · crypto · transport  │
  │  principals         │                    │  tools · inference             │
  │  capability grants  │                    │  (swappable per environment)   │
  │  credentials        │                    │                                │
  └─────────────────────┘                    └────────────────────────────────┘
   one authorization model                   portable core ships once, runs
   for users and agents                       server → Worker → browser → …
```

- **The hub** — a multi-tenant control plane. Manages tenants,
  principals (users _and_ agents under one authorization model),
  capability grants, credentials, and agent lifecycle. Agents under
  hub management run in sidecars that drive a harness on the hub's
  behalf.
- **The agent runtime** — [`@intx/agent`](./packages/agent) and the
  family of packages around it. A portable core that ships once and
  runs anywhere, swapping implementations of storage, cryptography,
  message transport, tools, and inference providers to match the
  environment.

For the full package map and dependency rules, see
[`LAYOUT.md`](./LAYOUT.md).

## Project status

Interchange is **alpha** — usable, running real workloads,
with the architecture settled and public APIs still liable to change
before 1.0. This section is the honest state of things, and the
repository is the source of truth. The design docs in
[`docs/`](./docs) describe the full intended system, some of which is
still ahead of the code; **Planned** below marks what is designed but
not yet in the tree.

### Shipped

- [x] **Multi-tenant hub** — tenants, principals (users and agents
      under one authorization model), capability grants, and
      credential management
- [x] **Authorization engine** ([`@intx/authz`](./packages/authz)) —
      one policy gates both API calls and tool invocations
- [x] **Agent runtime** ([`@intx/agent`](./packages/agent)) —
      construct, send, stream events, close; git-backed context with
      resume, rewind, and audit-log-as-git-history
- [x] **Workflow execution model** — every agent runs as a supervised
      workflow-process child (the in-process runtime has been retired)
- [x] **Sidecar orchestration** — hub-managed agents with reconnect
      and state restoration on redeploy
- [x] **Inference** — Anthropic and OpenAI-compatible adapters plus
      Google Gemini, streaming, compaction, the director system, and
      per-call provider failover with live source hot-swap, backed by a
      provider wire-capture rig with deterministic replay tests
- [x] **Admin UI** — React 19 + TanStack Router tenant-management
      console
- [x] **Web Crypto migration** — the portability foundation that
      unblocks non-Node runtimes
- [x] **Continuous integration** — `make all` gates `main`; Bun runs
      per commit, Node and Deno per release
- [x] **A dozen runnable examples** ([`examples/`](./examples/README.md))

### In progress

- [ ] **Full cross-runtime portability** — making the storage layer
      runtime-agnostic and removing the remaining Node-only byte
      utilities, en route to browser and Cloudflare Worker targets
- [ ] **Container and VM deployment** — packaging the sidecar host to
      run in containers and VMs; in flight, not yet merged
- [ ] **Documentation accuracy pass** — reconciling the public docs
      with the current runtime

### Next

- [ ] **Node and Deno in the CI matrix** — promote them from
      release-checked to per-commit
- [ ] **Browser and Cloudflare Worker** — prove the constrained-runtime
      deployment path end-to-end

### Planned (not yet in the tree)

Described in the [design docs](./docs), scaffolded in places, but not
something you can run today:

- [ ] **Cross-tenant federation** — discovery and invocation across
      trust boundaries. The trust-relationship schema and its CRUD API
      exist; the cross-boundary discovery and invocation path does not.
- [ ] **Remote tools** — invoking offerings exposed by other agents or
      services over the network. Today the harness runs local tool
      packages only ([`tools-posix`](./packages/tools-posix),
      [`tools-lsp`](./packages/tools-lsp),
      [`tools-mail`](./packages/tools-mail)).
- [ ] **Host-failure agent migration** — rescheduling agents off an
      unhealthy harness onto a healthy one. Today there are per-instance
      health checks but no migration.
- [ ] **Networked mail transport** — a real SMTP/IMAP (and JMAP for
      clients) transport on the wire. Today messaging runs on an
      in-memory transport over a git-backed MIME mailbox.
- [ ] **MCP and A2A offering discovery** — today discovery is OpenAPI
      plus a database-backed offering catalog.
- [ ] **More harness environments** — mobile and embedded hosts, beyond
      the local/server process host that runs today.

## Documentation

| You want to…                                          | Go to                                                                        |
| ----------------------------------------------------- | ---------------------------------------------------------------------------- |
| Use the agent runtime in your own program             | [`examples/`](./examples/README.md)                                          |
| Understand how the packages fit together              | [`LAYOUT.md`](./LAYOUT.md)                                                   |
| Run the full stack (hub + sidecar + admin UI) locally | [`DEV.md`](./DEV.md)                                                         |
| Read the system design (target architecture)          | [`docs/`](./docs) — [`ARCHITECTURE.md`](./docs/ARCHITECTURE.md), and friends |
| Browse the HTTP API                                   | [`docs/API.md`](./docs/API.md)                                               |

## Development

Build, lint, test, format, and docs all go through the `Makefile` at
the repo root, which verifies the environment via
[`bin/check-env`](./bin/check-env) before running each command:

| Target                | Description                                  |
| --------------------- | -------------------------------------------- |
| `make all`            | lint + build + admin-ui bundle + test        |
| `make build`          | type check (`tsc -b --noEmit --force`)       |
| `make build-admin-ui` | bundle the admin UI (`vite build`)           |
| `make lint`           | prettier + eslint + API docs freshness       |
| `make format`         | auto-format                                  |
| `make test`           | run tests                                    |
| `make docs`           | regenerate [`docs/API.md`](./docs/API.md)    |
| `make clean`          | remove `tsbuildinfo`, `dist/`, and env stamp |

Run `make all` before declaring a change correct; individual package
builds do not guarantee the full project graph compiles.

## Reference

### HTTP API

The hub exposes a REST API at `http://localhost:3000`. The OpenAPI
spec is at `GET /openapi.json`; the human-readable reference is
generated into [`docs/API.md`](./docs/API.md) by
[`bin/gen-api-docs.ts`](./bin/gen-api-docs.ts) from ArkType
introspection over the type definitions in
[`@intx/types`](./packages/types).

### Inference discovery

A wire-capture rig records real upstream responses from Gemini
and the OpenCode Zen relay's five models across text, multimodal,
function calling, and reasoning capabilities; the captured bytes
back deterministic tests in
[`@intx/inference-testing`](./packages/inference-testing). See
[`@intx/inference-discovery`](./packages/inference-discovery) for
the runtime and CLI, and
[`packages/inference-discovery-openai/docs/discovery.md`](./packages/inference-discovery-openai/docs/discovery.md)
for the OpenCode Zen observed-vs-documented narrative.

## License

[LGPL-2.1-only](./LICENSE).
