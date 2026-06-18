# AGENTS.md

Instructions for AI agents working in this repository.

## Session Initialization

At the start of every session, before doing any other work:

1. Read `CONVENTIONS.md` and follow all conventions defined there
2. Scan `skills/` for local skill directories. Read the YAML frontmatter of each `SKILL.md` to learn what skills are available. Do not read the full skill body unless the skill is being invoked.

Do not proceed with any user requests until these steps are complete.

## Development Environment

Read `DEV.md` for the full development guide. The essentials:

### Running the Stack

```bash
bun install
bin/db-reset && bun bin/dev.ts --seed
```

`bun install` materializes the workspace symlinks under `node_modules/@intx/`;
without it `make build` fails with `TS2307: Cannot find module '@intx/...'`.
The remaining command gets a clean, running system with seed data. It drops and recreates the database, runs migrations, grants permissions, starts all services (hub, sidecar, admin UI), and seeds test data.

- Hub: `http://localhost:3000`
- Admin UI: `http://localhost:5173`
- Seed login: `alice@example.com` / `password123`

### Common Operations

| Task                          | Command                        |
| ----------------------------- | ------------------------------ |
| Start stack (no seed)         | `bun bin/dev.ts`               |
| Start stack with seed         | `bun bin/dev.ts --seed`        |
| Start stack without admin UI  | `bun bin/dev.ts --no-admin-ui` |
| Full database reset           | `bin/db-reset`                 |
| Full reset (DB + agent state) | `bin/db-reset --clean`         |
| Apply migrations only         | `bin/db-migrate`               |
| Seed (requires running hub)   | `bun bin/seed.ts`              |
| Full build verification       | `make all`                     |
| Type check only               | `make build`                   |
| Lint only                     | `make lint`                    |
| Run tests only                | `make test`                    |
| Auto-format                   | `make format`                  |
| Regenerate API docs           | `make docs`                    |

Use the `make` targets above for build, lint, test, format, and docs.
Do not invoke the underlying `bun run` scripts directly -- the Makefile
also runs `bin/check-env` to verify the environment before each build.

`bin/db-reset` only resets postgres. `--clean` additionally wipes
`HUB_DATA_DIR` and `SIDECAR_DATA_DIR` so the sidecar does not try to
reconnect stale agent instances against a fresh database. Use it after
any reset where the sidecar disk state no longer matches the DB.

### Database

The system uses two PostgreSQL users: a migration user (DDL, owns tables) and a hub user (read/write app user). `bin/db-reset` handles all permission grants automatically. Never run the grant steps manually; use the script.

If you need to reset the database while the stack is running, stop the stack first (Ctrl+C) or you will get "active connections" errors.

## Build Requirements

You must run the full build pipeline before declaring any task complete:

```bash
make all
```

This runs lint, type check (`tsc -b`), and tests in order, after verifying
the environment via `bin/check-env`. Do not substitute `bun run check`,
`bun run lint`, or `bun run test` for `make all`.

- `make build` validates the entire TypeScript project graph via `tsc -b`
- Individual package builds do not guarantee the full tree will build
- Type exports and imports may not be available until the full tree is built
- Tests may fail if dependent packages are not rebuilt

If the build fails, report the failure and identify the cause. If the failure is pre-existing and unrelated to your changes, say so explicitly and let the user decide how to proceed. Never silently skip a failing step or substitute a partial build.

## Code Reuse and Refactoring

Do not reimplement functionality that already exists in the codebase. Before writing new code:

1. Search for existing implementations that could serve the same purpose
2. If similar functionality exists, prefer refactoring it to meet the new requirements
3. Look for unexported functions in other packages that could be promoted to a shared location

When you detect that a refactor might be necessary, prompt the user with specific options and allow them to provide their own answer if none fit.

## Configuration

Do not modify configuration files (e.g. eslint, prettier, tsconfig) unless explicitly asked. Focus on writing working software, not changing the conventions that are being used.

## Personality

Do not use emojis in code or documentation. Act professionally.
