# Development

## Prerequisites

- [Bun](https://bun.sh/) runtime
- PostgreSQL (local or remote)
- Git hooks configured: `git config core.hooksPath .githooks`

## Environment Setup

Copy each example env file and fill in values:

```
cp .env.example .env
cp .env.hub.example .env.hub
cp .env.migrate.example .env.migrate
cp .env.sidecar.example .env.sidecar   # optional, dev defaults are provided
```

`.env` contains shared settings (database host/port/name, demo runner config). `.env.hub` adds hub-specific secrets (database credentials, auth). `.env.migrate` adds migration database credentials. `.env.sidecar` overrides sidecar defaults (hub URL, sidecar ID, data directory).

## Running the Stack

The dev orchestrator starts everything in the correct order with colored log output:

```
bun bin/dev.ts
```

This runs: database migration, hub server (with `--watch` for auto-reload), sidecar, and UI dev server. Press Ctrl+C for graceful shutdown of all services.

Options:

| Flag           | Effect                                   |
| -------------- | ---------------------------------------- |
| `--seed`       | Seed the database after the hub is ready |
| `--no-ui`      | Skip the UI dev server                   |
| `--no-sidecar` | Skip the sidecar                         |

Default ports: hub on 3000, UI on 5173. The sidecar connects to the hub via websocket at `ws://localhost:3000/api/sidecars/ws`.

## Build Pipeline

```
bun run check    # TypeScript type checking (tsc -b --noEmit)
bun run lint     # Prettier + ESLint
bun run format   # Prettier auto-fix
bun run test     # All tests
```

The pre-commit hook runs `bun run lint` against staged files.

## Bin Scripts

All scripts live in `bin/`. Shell scripts that start with `#!/usr/bin/env opsh` use the bundled `opsh` shell framework (`bin/opsh`).

| Script                | Usage                                            | Description                                                              |
| --------------------- | ------------------------------------------------ | ------------------------------------------------------------------------ |
| `bin/dev.ts`          | `bun bin/dev.ts [flags]`                         | Dev orchestrator (see above)                                             |
| `bin/hub`             | `bin/hub`                                        | Run the hub server standalone (loads `.env` and `.env.hub`)              |
| `bin/db-migrate`      | `bin/db-migrate`                                 | Generate and apply database migrations (loads `.env` and `.env.migrate`) |
| `bin/seed.ts`         | `bun bin/seed.ts`                                | Seed the database via the hub API (requires running hub, uses `HUB_URL`) |
| `bin/add-package`     | `bin/add-package <name>`                         | Scaffold a new `@interchange/<name>` package                             |
| `bin/check-env`       | `bin/check-env`                                  | Verify git hooks are configured                                          |
| `bin/audit`           | `bin/audit --dir <path> --session <id> [--json]` | Inspect an agent's tool authorization audit trail                        |
| `bin/gen-api-docs.ts` | `bun bin/gen-api-docs.ts`                        | Generate API documentation from route schemas                            |
| `bin/posix-demo`      | `bin/posix-demo`                                 | Run the POSIX agent demo (uses `.env` for provider config)               |
| `bin/ring-demo`       | `bin/ring-demo`                                  | Run the ring agent demo (uses `.env` for provider config)                |

## Seed Data

`bin/seed.ts` creates the full dev dataset: users, tenants, agents, roles, grants, credentials, and offerings. Read the script for user accounts, passwords, and tenant memberships.

## Database

Migrations live in `packages/db`. The `bin/db-migrate` script runs `drizzle-kit generate` then `drizzle-kit migrate`. The dev orchestrator (`bin/dev.ts`) runs `drizzle-kit migrate` directly on startup, skipping the generate step.

Two database users are expected: one for the hub (read/write, configured in `.env.hub`) and one for migrations (DDL privileges, configured in `.env.migrate`).

## Project Structure

```
apps/
  hub/          Hub server (Hono, websocket, API routes)
  sidecar/      Sidecar process (agent lifecycle, websocket client)
  ui/           Web UI (Vite + React)
packages/
  db/           Drizzle ORM schema and migrations
  types/        Shared TypeScript types (arktype validators)
  message-memory/  In-memory IMAP-like message transport
  storage-isogit/  Git-backed agent state (isomorphic-git)
  inference/    LLM inference reactor
  harness/      Agent harness (tools, transport, audit)
  hub/          Hub library (sidecar router, event collection)
  ...
bin/            Development and operational scripts
.githooks/      Git hooks (pre-commit lint, commit-msg format)
```
