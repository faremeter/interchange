# Interchange

**Run AI agents as first-class principals — with their own identity,
permissions, and credentials — anywhere from a long-lived server to a
Cloudflare Worker.**

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

## The smallest possible agent

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
import { createIsogitStore } from "@intx/storage-isogit";

const def = defineAgent({
  id: "quickstart",
  systemPrompt: "You are a helpful assistant. Keep replies concise.",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "claude-sonnet-4-6" }],
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
it and the conversation picks up where it left off.

## How it works

Two halves, sharing one set of type definitions in
[`@intx/types`](./packages/types):

```
  ┌─────────────────────┐                    ┌────────────────────────────────┐
  │         Hub         │  ◀── events ──▶    │            Sidecar             │
  │    control plane    │                    │    harness + agent runtime     │
  │                     │                    │                                │
  │  tenants            │                    │  storage · crypto · transport  │
  │  principals         │                    │  tools · payments              │
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
  message transport, tools, and payments to match the environment.

## Start here

| You want to…                                          | Go to                                                                        |
| ----------------------------------------------------- | ---------------------------------------------------------------------------- |
| Use the agent runtime in your own program             | [`examples/`](./examples/README.md)                                          |
| Understand how the packages fit together              | [`LAYOUT.md`](./LAYOUT.md)                                                   |
| Run the full stack (hub + sidecar + admin UI) locally | [`DEV.md`](./DEV.md)                                                         |
| Write code in this repository                         | [`CONVENTIONS.md`](./CONVENTIONS.md), [`AGENTS.md`](./AGENTS.md)             |
| Read the system design                                | [`docs/`](./docs) — [`ARCHITECTURE.md`](./docs/ARCHITECTURE.md), and friends |

## The impatient path

Install workspace dependencies, then run the full stack with seed
data:

```bash
bun install
bin/db-reset && bun bin/dev.ts --seed
```

Starts the hub, sidecar, and admin UI — entry points in
[`apps/`](./apps).

Requires [Bun](https://bun.sh/) 1.2+ and PostgreSQL 15+. See
[`DEV.md`](./DEV.md) for everything else — environment files, role
setup, default ports, seed credentials, partial-stack variants,
reset recipes.

## Build verbs

Build, lint, test, and format go through the `Makefile` at the repo
root, which verifies the environment via
[`bin/check-env`](./bin/check-env) before running each command
directly.

| Target        | Description                                  |
| ------------- | -------------------------------------------- |
| `make all`    | lint + build + test (full verification)      |
| `make build`  | type check (`tsc -b --noEmit`)               |
| `make lint`   | prettier + eslint + API docs freshness       |
| `make format` | auto-format                                  |
| `make test`   | run tests                                    |
| `make docs`   | regenerate [`docs/API.md`](./docs/API.md)    |
| `make clean`  | remove `tsbuildinfo`, `dist/`, and env stamp |

Run `make all` before declaring a change correct; individual
package builds do not guarantee the full project graph compiles.

## HTTP API

The hub exposes a REST API at `http://localhost:3000`. The OpenAPI
spec is at `GET /openapi.json`; the human-readable reference is
generated into [`docs/API.md`](./docs/API.md) by
[`bin/gen-api-docs.ts`](./bin/gen-api-docs.ts) from ArkType
introspection over the type definitions in
[`@intx/types`](./packages/types).

## Inference discovery

A wire-capture rig records real upstream responses from Gemini
and the OpenCode Zen relay's five models across text, multimodal,
function calling, and reasoning capabilities; the captured bytes
back deterministic tests in
[`@intx/inference-testing`](./packages/inference-testing). See
[`@intx/inference-discovery`](./packages/inference-discovery) for
the runtime and CLI, and
[`docs/OPENCODE_DISCOVERY.md`](./docs/OPENCODE_DISCOVERY.md) for
the OpenCode Zen observed-vs-documented narrative.
