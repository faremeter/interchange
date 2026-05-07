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
bin/db-reset && bun bin/dev.ts --seed
```

This is the single command to get a clean, running system with seed data. It drops and recreates the database, runs migrations, grants permissions, starts all services (hub, sidecar, UI), and seeds test data.

- Hub: `http://localhost:3000`
- UI: `http://localhost:5173`
- Seed login: `alice@example.com` / `password123`

### Common Operations

| Task                        | Command                                         |
| --------------------------- | ----------------------------------------------- |
| Start stack (no seed)       | `bun bin/dev.ts`                                |
| Start stack with seed       | `bun bin/dev.ts --seed`                         |
| Start stack without UI      | `bun bin/dev.ts --no-ui`                        |
| Full database reset         | `bin/db-reset`                                  |
| Apply migrations only       | `bin/db-migrate`                                |
| Seed (requires running hub) | `bun bin/seed.ts`                               |
| Full build verification     | `bun run check && bun run lint && bun run test` |

### Database

The system uses two PostgreSQL users: a migration user (DDL, owns tables) and a hub user (read/write app user). `bin/db-reset` handles all permission grants automatically. Never run the grant steps manually; use the script.

If you need to reset the database while the stack is running, stop the stack first (Ctrl+C) or you will get "active connections" errors.

## Build Requirements

You must run the full build pipeline before declaring any task complete:

```bash
bun run check && bun run lint && bun run test
```

- `bun run check` validates the entire TypeScript project graph via `tsc -b`
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
