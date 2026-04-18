# AGENTS.md

Instructions for AI agents working in this repository.

## Session Initialization

At the start of every session, before doing any other work:

1. Read `CONVENTIONS.md` and follow all conventions defined there

Do not proceed with any user requests until this step is complete.

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
