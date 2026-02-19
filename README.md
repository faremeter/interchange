# Interchange

Multi-tenant control plane for agent orchestration. Manages tenants, principals (users and agents under a unified authorization model), roles, capability grants, and agent lifecycle.

## Prerequisites

- [Bun](https://bun.sh/) (1.2+)
- [opsh](https://github.com/anomalyco/opsh) (v0.7+) -- for the shell scripts in `bin/`
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

Three env files are needed, all gitignored. Copy from the examples:

```
cp .env.example .env
cp .env.hub.example .env.hub
cp .env.migrate.example .env.migrate
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
BETTER_AUTH_URL=http://localhost:3000

GOOGLE_CLIENT_ID=
GOOGLE_CLIENT_SECRET=
```

Generate `BETTER_AUTH_SECRET` with `openssl rand -hex 32`.

**.env.migrate** -- migration credentials:

```
DB_USER=interchange-migrate
DB_PASSWORD=migrate-dev-password
```

### 5. Run migrations

```
bin/db-migrate
```

This runs a type check, generates any new Drizzle migration files, and applies them to the database as `interchange-migrate`.

## Running

### Hub (API server)

```
bin/hub
```

Starts the Hono API server on `http://localhost:3000` with file watching. Verify with:

```
curl http://localhost:3000/status
```

### UI (development)

In a second terminal:

```
cd apps/ui
bunx vite
```

Opens on `http://localhost:5173`. The Vite dev server proxies `/api` requests to the hub on port 3000.

### Seed data

With the hub running, seed the database with test data:

```
bin/seed.ts
```

Creates 3 users (alice/bob/carol, all with password `password123`), 2 tenants, 3 agents, roles, grants, credentials, wallets, capabilities, and a federation trust. The seed is idempotent -- it signs in existing users and skips resources that already exist (409).

To start completely fresh, drop and recreate the schema, then re-run migrations and seed:

```
psql -d interchange -c 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;'
psql -d interchange -f db/init.sql
bin/db-migrate
bin/seed.ts
```

## Project structure

```
apps/
  hub/          Entry point -- starts the Hono server, wires DB + auth
  ui/           React SPA -- Vite + TanStack Router/Query + Tailwind

packages/
  authz/        Authorization engine -- pattern matching, specificity, evaluation
  db/           Drizzle schema, migrations, connection pooling
  hub/          Hono app factory, route handlers, middleware
  log/          LogTape wrapper (setup, getLogger, Hono middleware)
  types/        Shared ArkType type definitions for API request/response

bin/
  hub           Start the hub server (sources .env + .env.hub)
  db-migrate    Generate and apply Drizzle migrations
  seed.ts       Seed test data via the HTTP API
  gen-api-docs.ts  Generate docs/API.md from OpenAPI spec + ArkType introspection
  add-package   Scaffold a new workspace package
  check-env     Verify git hooks configuration

db/
  init.sql      Bootstrap database roles and permissions

docs/
  AUTH.md       Authorization model design
  ROUTES.md     API route conventions
  API.md        Generated API reference (gitignored from prettier)
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

The hub exposes a REST API at `http://localhost:3000`. All tenant-scoped routes require authentication and authorization grants. Key routes:

- `POST /api/auth/sign-up/email` -- create account
- `POST /api/auth/sign-in/email` -- sign in (cookie-based sessions)
- `GET /api/me` -- current user profile
- `GET /api/me/principals` -- user's memberships across tenants
- `POST /api/tenants` -- create a tenant (bootstraps owner/admin/member roles with system grants)
- `GET /api/tenants/:tenantId` -- tenant detail
- `GET /api/tenants/:tenantId/principals` -- list members and agents
- `GET /api/tenants/:tenantId/roles` -- list roles
- `GET /api/tenants/:tenantId/grants` -- list capability grants
- `POST /api/tenants/:tenantId/agents` -- create an agent
- `GET /api/tenants/:tenantId/credentials` -- list credentials
- `GET /api/tenants/:tenantId/wallets` -- list wallets
- `GET /api/tenants/:tenantId/capabilities` -- list capabilities
- `GET /openapi.json` -- full OpenAPI spec

Full reference: run `bin/gen-api-docs.ts` to generate `docs/API.md`.
