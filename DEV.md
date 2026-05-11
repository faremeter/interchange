# Development

## Prerequisites

- [Bun](https://bun.sh/) (1.2+)
- [opsh](https://github.com/alexanderguy/opsh) (v0.7+) -- for the shell scripts in `bin/`
- PostgreSQL (15+)
- Git hooks configured: `git config core.hooksPath .githooks`

## Quick Start

If env files are already configured (see Environment Setup below):

```bash
bin/db-reset && bun bin/dev.ts --seed
```

This drops and recreates the database, runs migrations, grants permissions, starts all services, and seeds test data. After startup, the hub is at `http://localhost:3000` and the UI is at `http://localhost:5173`.

Seed accounts (all use password `password123`):

| User          | Email             | Role                                |
| ------------- | ----------------- | ----------------------------------- |
| Alice Admin   | alice@example.com | Owner of Acme Corp and Widget Labs  |
| Bob Builder   | bob@example.com   | Member of Acme Corp and Widget Labs |
| Carol Creator | carol@example.com | Admin of Widget Labs                |

## Environment Setup

Copy each example env file and fill in values:

```bash
cp .env.example .env
cp .env.hub.example .env.hub
cp .env.migrate.example .env.migrate
cp .env.sidecar.example .env.sidecar   # optional, dev defaults are provided
```

The example files contain working dev defaults for most values. The only value you must generate is `BETTER_AUTH_SECRET` in `.env.hub` (any 32+ byte hex string works, e.g. `openssl rand -hex 32`).

| File           | Contains                                                                        |
| -------------- | ------------------------------------------------------------------------------- |
| `.env`         | Shared settings: database host/port/name, demo runner config                    |
| `.env.hub`     | Hub secrets: database credentials, auth secret, OAuth (optional)                |
| `.env.migrate` | Migration database credentials (DDL user)                                       |
| `.env.sidecar` | Sidecar overrides: hub URL, sidecar ID, data directory (optional, has defaults) |

### Database Users

The system uses two PostgreSQL users:

- **Migration user** (configured in `.env.migrate`): Owns DDL privileges, creates and owns all tables. Used by `drizzle-kit migrate`.
- **Hub user** (configured in `.env.hub`): Read/write application user. Needs explicit grants after migrations because it does not own the tables.

Both users must exist in PostgreSQL before running the stack. `bin/db-reset` handles all the grant choreography automatically, but the users themselves must be created once:

```bash
psql -d postgres -c "CREATE USER \"interchange-migrate\" WITH PASSWORD 'migrate-dev-password';"
psql -d postgres -c "CREATE USER \"interchange-hub\" WITH PASSWORD 'hub-dev-password';"
```

If you get "role already exists" errors, the users are already set up and you can proceed.

## Running the Stack

The dev orchestrator starts everything in the correct order with colored log output:

```bash
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

## Database

Migrations live in `packages/db`. The `bin/db-migrate` script runs `drizzle-kit generate` then `drizzle-kit migrate`. The dev orchestrator (`bin/dev.ts`) runs `drizzle-kit migrate` directly on startup, skipping the generate step.

### Full Reset

`bin/db-reset` performs a complete database teardown and rebuild as the local superuser:

1. Drop the database
2. Create a fresh database
3. Grant database and schema access to both app users
4. Run all migrations (as the migration user)
5. Grant table and sequence access to the hub user

This is the correct way to get a clean database. Do not attempt the steps manually.

### Applying Migrations Only

If the database already exists and you just need to apply new migrations:

```bash
bin/db-migrate
```

## Build Pipeline

```bash
bun run check    # TypeScript type checking (tsc -b --noEmit)
bun run lint     # Prettier + ESLint + API docs freshness
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
| `bin/db-reset`        | `bin/db-reset`                                   | Drop, recreate, migrate, and grant permissions (full DB reset)           |
| `bin/seed.ts`         | `bun bin/seed.ts`                                | Seed the database via the hub API (requires running hub, uses `HUB_URL`) |
| `bin/add-package`     | `bin/add-package <name>`                         | Scaffold a new `@interchange/<name>` package                             |
| `bin/check-env`       | `bin/check-env`                                  | Verify git hooks are configured                                          |
| `bin/audit`           | `bin/audit --dir <path> --session <id> [--json]` | Inspect an agent's tool authorization audit trail                        |
| `bin/gen-api-docs.ts` | `bun bin/gen-api-docs.ts`                        | Generate API documentation from route schemas                            |
| `bin/posix-demo`      | `bin/posix-demo`                                 | Run the POSIX agent demo (uses `.env` for provider config)               |
| `bin/ring-demo`       | `bin/ring-demo`                                  | Run the ring agent demo (uses `.env` for provider config)                |

## Seed Data

`bin/seed.ts` creates the full dev dataset: users, tenants, agents, roles, grants, credentials, and offerings. It requires a running hub (the dev orchestrator handles this when `--seed` is passed). See the Quick Start section for seed account credentials.

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
