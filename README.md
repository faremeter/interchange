# Interchange

An agentic operating system.

The hub — a multi-tenant control plane — manages tenants,
principals (users and agents under one authorization model),
capability grants, credentials, and agent lifecycle. Agents under
hub management run in sidecars that drive a harness on the hub's
behalf. The agent runtime — [`@intx/agent`](./packages/agent)
and the family of packages around it — runs anywhere from a
long-lived server to a Cloudflare Worker, swapping implementations
of storage, cryptography, message transport, tools, and payments to
match the environment. Both ends share the type definitions in
[`@intx/types`](./packages/types).

What you build on top is up to you. A coding assistant. A
mail-driven workflow agent. An autonomous trader. A research
harness.

## Start here

| You want to…                                    | Go to                                                                        |
| ----------------------------------------------- | ---------------------------------------------------------------------------- |
| Use the agent runtime in your own program       | [`examples/`](./examples/README.md)                                          |
| Understand how the packages fit together        | [`LAYOUT.md`](./LAYOUT.md)                                                   |
| Run the full stack (hub + sidecar + UI) locally | [`DEV.md`](./DEV.md)                                                         |
| Write code in this repository                   | [`CONVENTIONS.md`](./CONVENTIONS.md), [`AGENTS.md`](./AGENTS.md)             |
| Read the system design                          | [`docs/`](./docs) — [`ARCHITECTURE.md`](./docs/ARCHITECTURE.md), and friends |

## The impatient path

Run the full stack with seed data:

```bash
bin/db-reset && bun bin/dev.ts --seed
```

Starts the hub, sidecar, and UI — entry points in
[`apps/`](./apps).

Requires [Bun](https://bun.sh/) 1.2+ and PostgreSQL 15+. See
[`DEV.md`](./DEV.md) for everything else — environment files, role
setup, default ports, seed credentials, partial-stack variants,
reset recipes.

## Build verbs

Build, lint, test, and format go through the `Makefile` at the repo
root, which verifies the environment via
[`bin/check-env`](./bin/check-env) before delegating to the
underlying `bun run` scripts.

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
