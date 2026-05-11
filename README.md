# Interchange

Multi-tenant control plane for agent orchestration. Manages tenants, principals (users and agents under a unified authorization model), roles, capability grants, and agent lifecycle.

## Prerequisites

- [Bun](https://bun.sh/) (1.2+)
- [opsh](https://github.com/alexanderguy/opsh) (v0.7+) -- for the shell scripts in `bin/`
- PostgreSQL (15+)

## Setup

### 1. Install dependencies

```
bun install
```

### 2. Configure git hooks

```
git config core.hooksPath .githooks
```

Verify with `bin/check-env`.

### 3. Create the database

Connect as a Postgres superuser and create the database:

```sql
CREATE DATABASE interchange;
```

Then run the init script to create service roles and set up permissions:

```
psql -d interchange -f db/init.sql
```

This creates two roles:

- **interchange-migrate** -- owns the schema and runs migrations (DDL)
- **interchange-hub** -- application role with DML access to all tables

Default privileges are configured so that any table created by `interchange-migrate` is automatically accessible to `interchange-hub`.

### 4. Environment files

Four env files are needed, all gitignored. Copy from the examples:

```
cp .env.example .env
cp .env.hub.example .env.hub
cp .env.migrate.example .env.migrate
cp .env.sidecar.example .env.sidecar
```

**.env** -- shared database connection:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=interchange
```

**.env.hub** -- hub server config:

```
DB_USER=interchange-hub
DB_PASSWORD=hub-dev-password

BETTER_AUTH_SECRET=<random 64-char hex string>
BETTER_AUTH_BASE_URL=http://localhost:3000

HUB_DATA_DIR=./tmp/hub-data

GOOGLE_CLIENT_ID=
GOOGLE_CLIENT_SECRET=
```

Generate `BETTER_AUTH_SECRET` with `openssl rand -hex 32`.

**.env.migrate** -- migration credentials:

```
DB_USER=interchange-migrate
DB_PASSWORD=migrate-dev-password
```

**.env.sidecar** -- sidecar config (optional, dev defaults are provided):

```
HUB_WS_URL=ws://localhost:3000/api/sidecars/ws
SIDECAR_ID=dev-sidecar-1
SIDECAR_TOKEN=dev-token
SIDECAR_DATA_DIR=./tmp/sidecar-data
```

### 5. Run migrations

```
bin/db-migrate
```

This runs a type check, generates any new Drizzle migration files, and applies them to the database as `interchange-migrate`.

## Running

### Full stack (recommended)

```
bun bin/dev.ts --seed
```

Starts hub, sidecar, and UI with file watching, and seeds test data. To skip seeding:

```
bun bin/dev.ts
```

To start without the UI:

```
bun bin/dev.ts --no-ui
```

- Hub: `http://localhost:3000`
- UI: `http://localhost:5173`
- Seed login: `alice@example.com` / `password123`

### Hub only

```
bin/hub
```

Starts the Hono API server on `http://localhost:3000` with file watching. Verify with:

```
curl http://localhost:3000/status
```

### UI only

In a second terminal (with the hub already running):

```
cd apps/ui
bunx vite
```

Opens on `http://localhost:5173`. The Vite dev server proxies `/api` requests to the hub on port 3000.

### Seed data

With the hub running, seed the database with test data:

```
bun bin/seed.ts
```

Creates 3 users (alice/bob/carol, all with password `password123`), 2 tenants, 3 agents, roles, grants, credentials, wallets, offerings, and a federation trust. The seed is idempotent -- it signs in existing users and skips resources that already exist (409).

### Full database reset

To start completely fresh:

```
bin/db-reset && bun bin/dev.ts --seed
```

This drops and recreates the database, runs migrations, grants permissions, and starts all services with seed data.

## Project structure

```
apps/
  hub/              Entry point -- starts the Hono server, wires DB + auth
  sidecar/          Agent sidecar process -- WebSocket client, key store, tool dispatch
  ui/               React SPA -- Vite + TanStack Router/Query + Tailwind

packages/
  authz/            Authorization engine -- pattern matching, specificity, evaluation
  crypto-node/      Cryptographic primitives -- signing, verification, PGP, SSH signatures
  db/               Drizzle schema, migrations, connection pooling
  harness/          Agent harness -- deploy tree, plugin system, tool orchestration
  hub/              Hono app factory, route handlers, middleware
  inference/        LLM inference -- provider adapters, streaming, audit collection
  log/              LogTape wrapper (setup, getLogger, Hono middleware)
  message-memory/   JMAP-style message memory -- mailbox, search, send, threads
  mime/             MIME message construction and PGP signing
  pack-transport/   Git pack protocol -- chunked transport for pack send/receive
  storage-isogit/   Git object storage -- isogit-backed mail store, history, pack ops
  tools-posix/      POSIX tool implementations -- file read/write, shell execution
  types/            Shared type definitions (ArkType validators, runtime types, wire formats)

bin/
  hub               Start the hub server (sources .env + .env.hub)
  db-migrate        Generate and apply Drizzle migrations
  db-reset          Drop and recreate the database, run migrations, grant permissions
  dev.ts            Start the full stack (hub + sidecar + UI) with file watching
  seed.ts           Seed test data via the HTTP API
  gen-api-docs.ts   Generate docs/API.md from OpenAPI spec + ArkType introspection
  add-package       Scaffold a new workspace package
  check-env         Verify git hooks configuration

db/
  init.sql          Bootstrap database roles and permissions

docs/
  API.md            Generated API reference (gitignored from prettier)
  ARCHITECTURE.md   System architecture overview
  AUTH.md           Authorization model design
  CREDENTIALS.md    Credential management
  HARNESS_DESIGN.md Agent harness design
  IMPLEMENTATION.md Implementation notes
  INFERENCE.md      Inference subsystem design
  MESSAGE.md        Messaging system design
  PRODUCT.md        Product requirements
  ROUTES.md         API route conventions
```

## Scripts

These are run from the repo root via `bun run`:

| Script   | Command                             |
| -------- | ----------------------------------- |
| `check`  | `bun run check` -- type check       |
| `lint`   | `bun run lint` -- prettier + eslint |
| `format` | `bun run format` -- auto-format     |
| `test`   | `bun test` -- run tests             |

## API

The hub exposes a REST API at `http://localhost:3000`. All tenant-scoped routes require authentication and authorization grants. The OpenAPI spec is available at `GET /openapi.json`.

Full reference: `docs/API.md` (generated by `bun bin/gen-api-docs.ts`).
