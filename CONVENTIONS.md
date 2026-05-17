# Interchange Code Conventions

This document describes the coding conventions, patterns, and best practices used in the Interchange codebase. Follow these guidelines when contributing to ensure consistency across the project.

## Table of Contents

- [Monorepo Structure](#monorepo-structure)
- [Build and Development Commands](#build-and-development-commands)
- [Quick Reference](#quick-reference)
- [TypeScript Configuration](#typescript-configuration)
- [Code Formatting](#code-formatting)
- [Naming Conventions](#naming-conventions)
- [Type System Patterns](#type-system-patterns)
- [Import/Export Patterns](#importexport-patterns)
- [Error Handling](#error-handling)
- [Async Patterns](#async-patterns)
- [Module Organization](#module-organization)
- [Testing](#testing)
- [Logging](#logging)
- [Documentation](#documentation)
- [ESLint Rules](#eslint-rules)
- [Git Workflow](#git-workflow)

---

## Monorepo Structure

This is a bun monorepo. When developing new TypeScript code:

- **Applications** go in `apps/`
- **Shared libraries** go in `packages/`

Do not create standalone TypeScript files in the repository root.

---

## Build and Development Commands

Use the `Makefile` at the repo root for build, lint, test, format, and
docs. The Makefile verifies the environment via `bin/check-env` before
each build and then delegates to the underlying `bun run` scripts.

```bash
# Full build verification
make all

# Individual targets
make build     # tsc -b --noEmit (validates entire TypeScript project graph)
make lint      # Prettier + ESLint + API docs freshness
make format    # Auto-format with Prettier
make test      # Run bun tests
make docs      # Regenerate API documentation
```

`make build` uses TypeScript project references (`tsc -b`). It validates the entire monorepo dependency graph, not just a single package. Changes to shared packages (types, authz, log) may break downstream consumers that `tsc -b` will catch.

---

## Quick Reference

### Do

- Run the full build pipeline before considering changes correct
- Use `arktype` for runtime validation
- Use `import type` for type-only imports
- Create factory functions with `create*` prefix
- Return `null` from handlers when request doesn't match
- Use `{ cause }` when re-throwing errors
- Use the package logger, never `console`
- Co-locate tests with source files
- Run `make format` before committing
- Run `make docs` after changing routes or type descriptions
- Let TypeScript infer types when obvious

### Don't

- Mix refactors/whitespace changes with functional changes
- Use `console.log` (use logger)
- Use default exports
- Create classes unless necessary (prefer factory functions)
- Ignore validation errors (always check with `instanceof type.errors`)
- Use `any` type (use `unknown` and narrow)
- Use type assertions (`as Type`) - they indicate interface problems
- Skip runtime validation in favor of type assertions (use arktype)
- Commit without running `make lint`
- Over-type code with explicit annotations the compiler can infer

---

## TypeScript Configuration

The project uses strict TypeScript settings defined in `tsconfig.base.json`. Key implications:

- **Strict mode enabled**: All strict type-checking options are active
- **`noUncheckedIndexedAccess`**: Array/object index access may return `undefined`. Always check before using.
- **`exactOptionalPropertyTypes`**: Optional properties cannot be explicitly set to `undefined`.
- **`verbatimModuleSyntax`**: Use `import type` for type-only imports.
- **ESNext target**: Modern JavaScript features are available; no need for polyfills.

---

## Code Formatting

Formatting is enforced via Prettier. See `.prettierrc.json` for the configuration.

Key formatting rules:

- **Indentation**: 2 spaces (no tabs)
- **Quotes**: Double quotes `"` for strings
- **Semicolons**: Required
- **Trailing commas**: Always (including function parameters)

Run `make format` to auto-format all files.

---

## Naming Conventions

### Files

| Type                | Convention                        | Example                          |
| ------------------- | --------------------------------- | -------------------------------- |
| Regular modules     | Lowercase, hyphens for multi-word | `sidecar-handler.ts`, `grant.ts` |
| Single-word modules | Lowercase                         | `schema.ts`, `common.ts`         |
| Test files          | `{name}.test.ts`                  | `sidecar-handler.test.ts`        |

### Functions

| Pattern     | Use Case                       | Example                                           |
| ----------- | ------------------------------ | ------------------------------------------------- |
| `camelCase` | All functions                  | `handleMessage`, `formatSession`                  |
| `create*`   | Factory functions              | `createSidecarRouter`, `createInMemoryGrantStore` |
| `is*`       | Boolean predicates             | `isLogLevel`, `isLazy`                            |
| `get*`      | Retrieval without side effects | `getConnectedSidecars`, `getRoutableAddresses`    |
| `handle*`   | Event/request handlers         | `handleOpen`, `handleRegister`                    |

### Variables

| Pattern                | Use Case                    | Example                      |
| ---------------------- | --------------------------- | ---------------------------- |
| `camelCase`            | Regular variables           | `agentAddress`, `requestId`  |
| `SCREAMING_SNAKE_CASE` | Constants, environment vars | `DEFAULT_REQUEST_TIMEOUT_MS` |
| `_` prefix             | Unused parameters           | `_ws`, `_ctx`                |

### Acronyms in Names

Preserve acronym capitalization based on position:

```typescript
// Acronyms stay capitalized when starting uppercase
getURLFromRequest;
requestURL;
parseHTTPHeaders;

// Lowercase-starting acronyms stay lowercase
url;
json;
```

Common acronyms: URL, HTTP, HTTPS, JSON, API, RPC, HTML, XML

"ID" is an abbreviation, so use standard camelCase: `userId`, `requestId`, `getId()`.

### Types and Interfaces

| Pattern           | Use Case                 | Example                          |
| ----------------- | ------------------------ | -------------------------------- |
| `PascalCase`      | Interfaces, type aliases | `SidecarRouter`, `HarnessConfig` |
| `*Args` / `*Opts` | Function arguments       | `CreateHandlerOpts`              |
| `*Response`       | API responses            | `SessionResponse`                |
| `*Info`           | Data structures          | `AgentInfo`, `TokenInfo`         |
| `*Handler`        | Handler interfaces       | `SidecarHandler`                 |

---

## Type System Patterns

### Runtime Validation with arktype

Use `arktype` for runtime type validation. Define the validator and TypeScript type together:

```typescript
import { type } from "arktype";

// Define runtime validator
export const CreateSession = type({
  agentId: "string",
  "invokerCapabilities?": type({
    resource: "string",
    action: "string",
    "conditions?": "Record<string, unknown> | null",
  }).array(),
});

// Derive TypeScript type from validator
export type CreateSession = typeof CreateSession.infer;
```

### Type Guards

Create type guards using arktype validation:

```typescript
import { type } from "arktype";

const Address = type("string");

export function isAddress(maybe: unknown): maybe is string {
  return !(Address(maybe) instanceof type.errors);
}
```

### Interfaces vs Types

- **`type`**: Use for data structures, unions, and arktype-derived types
- **`interface`**: Use for behavioral contracts (objects with methods)

```typescript
// Type for data structure
type SessionRow = typeof agentSession.$inferSelect;

// Interface for behavioral contract
interface SidecarRouter {
  handleOpen(ws: WsHandle): void;
  handleMessage(ws: WsHandle, data: string): void;
  routeMail(agentAddress: string, rawMessage: string): boolean;
}
```

### Const Assertions for Exhaustive Types

Use `as const` for exhaustive literal types:

```typescript
const FrameType = {
  Register: "register",
  MailOutbound: "mail.outbound",
  AgentEvent: "agent.event",
} as const;

type FrameType = (typeof FrameType)[keyof typeof FrameType];
```

### Type-Only Imports

Use `import type` for type-only imports (required by `verbatimModuleSyntax`):

```typescript
import type { SidecarFrame, HubFrame } from "@interchange/types/sidecar";
import type { AbortReason, HarnessConfig } from "@interchange/types/runtime";

// Mixed imports
import {
  type AuthzResult,
  authorize, // value import
} from "@interchange/authz";
```

### Avoid Over-Typing

Let TypeScript infer types when they are obvious:

```typescript
// Good - return type is obvious
const createHandler = async (network: string) => {
  const config = { network, enabled: true };
  return {
    getConfig: () => config,
    isEnabled: () => config.enabled,
  };
};

// Unnecessary - the return type is obvious
const createHandler = async (
  network: string,
): Promise<{
  getConfig: () => { network: string; enabled: boolean };
  isEnabled: () => boolean;
}> => { ... };
```

**When to add explicit types:**

- Public API boundaries where the type serves as documentation
- When the inferred type would be too wide
- When TypeScript cannot infer the type correctly
- Complex return types that benefit from explicit documentation

**When NOT to add explicit types:**

- Variable assignments with obvious literal values
- Return types that match a simple expression
- Loop variables and intermediate calculations
- Arrow function parameters in callbacks where context provides types

### Avoiding `any` and Type Assertions

Use `unknown` instead of `any` when the type is truly unknown, then narrow with validation:

```typescript
// Bad
function processData(data: any) {
  return data.value;
}

// Good
function processData(data: unknown) {
  const validated = MyDataType(data);
  if (validated instanceof type.errors) {
    throw new Error(`Invalid data: ${validated.summary}`);
  }
  return validated.value;
}
```

Type assertions (`as Type`) bypass type checking and often indicate interface problems. The ESLint rule `@typescript-eslint/no-unsafe-type-assertion` is enabled at error level to enforce this. Prefer runtime validation:

```typescript
// Bad
const data = (await response.json()) as UserData;

// Good
const raw = await response.json();
const data = UserData(raw);
if (data instanceof type.errors) {
  throw new Error(`Invalid response: ${data.summary}`);
}
```

#### Database query results

Drizzle types `jsonb()` as `unknown` and `text({ enum })` as `string`, which tempts cast-at-callsite. Validate once in the DB layer using `parseRow` functions in `packages/db/src/parse-row.ts`:

```typescript
// Bad
const skills = row.skills as string[];

// Good — parseRow validates at the boundary
const agent = parseAgentRow(row); // skills is already validated
```

#### Parsed JSON

Validate external JSON with arktype before using it:

```typescript
// Bad
const body = (await req.json()) as DeployManifest;

// Good
const raw: unknown = await req.json();
const body = DeployManifest.assert(raw);
```

#### Test mocks

Write factory functions that satisfy the full interface. For typed empty arrays, annotate the type explicitly:

```typescript
// Bad
const mock = {} as unknown as DB;

// Good
function createMockDB(opts: MockDBOpts) {
  return { query: { ... } } as unknown as Parameters<typeof createApp>[0]["db"];
}

// Bad
const items = [] as SomeType[];

// Good
const items: SomeType[] = [];
```

When a library type cannot be structurally satisfied in tests (e.g., betterAuth's `Auth`, Drizzle's `PgDatabase`), the `as unknown as LibraryType` double-cast is acceptable with an `eslint-disable-next-line` and a justification comment.

#### Third-party callbacks

Use type predicate guards instead of casting callback arguments:

```typescript
// Bad
onChange={(value) => setValue(value as CredentialType)}

// Good
function isCredentialType(v: string): v is CredentialType {
  return credentialTypes.includes(v as CredentialType);
}
onChange={(value) => {
  if (isCredentialType(value)) setValue(value);
}}
```

#### Router params

Use TanStack Router's `from` parameter instead of `strict: false` with a cast. An ESLint `no-restricted-syntax` rule enforces this:

```typescript
// Bad
const { tenantId } = useParams({ strict: false }) as { tenantId: string };

// Good
const { tenantId } = useParams({ from: "/authed/tenants/$tenantId" });
```

#### `as const`

`as const` is always allowed. It narrows literal types and serves a different purpose than type assertions. The lint rule does not flag it.

#### When `as` is acceptable

Only for binary/byte-level parsing where bounds are mathematically guaranteed, or when a library's type definitions cannot express the actual type. Each use requires `eslint-disable-next-line` with a justification:

```typescript
// eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- byte offset is bounds-checked above
const view = new DataView(buffer.slice(offset, offset + 4) as ArrayBuffer);
```

### Generic Constraints vs Index Signatures

Prefer generic type parameters with constraints over index signatures:

```typescript
// Bad - index signature (too permissive)
export interface Store {
  get(key: { id: string; [key: string]: unknown }): Promise<unknown>;
}

// Good - generic with constraint (type-safe)
export type BaseKey = { id: string };

export interface Store<TKey extends BaseKey = BaseKey> {
  get(key: TKey): Promise<unknown>;
}
```

---

## Import/Export Patterns

### Barrel Exports

Use `index.ts` files to re-export from modules:

```typescript
// packages/db/src/index.ts
export { createDB, type DB } from "./client";
export { createGrantStore } from "./grant-store";
export * as schema from "./schema";
```

Prefer flat re-exports (`export *`) for types packages. Use namespaced re-exports (`export * as`) when grouping a subdirectory under a single name. Use dedicated `package.json` entry points for sub-modules that consumers import directly (e.g., `@interchange/types/sidecar`).

### Named Exports (Preferred)

```typescript
// Good
export function createSidecarRouter(config: SidecarRouterConfig) { ... }
export const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

// Avoid
export default function createSidecarRouter(config: SidecarRouterConfig) { ... }
```

### Import Ordering

Order imports by category:

1. External library imports
2. Internal package imports (`@interchange/*`)
3. Relative imports

```typescript
// External libraries
import { type } from "arktype";
import { Hono } from "hono";

// Internal packages
import { getLogger } from "@interchange/log";
import type { SidecarFrame } from "@interchange/types/sidecar";

// Relative imports
import { formatSession } from "./helpers";
import { grantMiddleware } from "../middleware/grant";
```

---

## Error Handling

### Validation Errors

Check arktype validation errors before proceeding:

```typescript
const body = CreateSession(raw);

if (body instanceof type.errors) {
  return c.json({ error: body.summary }, 400);
}

// body is now typed correctly
```

### Error Chaining

Use `{ cause }` when re-throwing errors to preserve the error chain:

```typescript
try {
  await sendSessionCreate(agentAddress, config);
} catch (cause) {
  throw new Error("Failed to create agent session", { cause });
}
```

### Return `null` for "Not My Responsibility"

Handlers should return `null` when a request doesn't match their criteria:

```typescript
const handleVerify = async (requirements, payment) => {
  if (!isMatchingRequirement(requirements)) {
    return null; // Let another handler try
  }
  // Handle the request...
};
```

---

## Async Patterns

### Factory Functions

Use factory functions that return objects with methods:

```typescript
export function createSidecarRouter(
  config: SidecarRouterConfig = {},
): SidecarRouter {
  // Internal state
  const connections = new Map<WsHandle, SidecarConnection>();

  // Return object with methods
  return {
    handleOpen,
    handleMessage,
    handleClose,
    routeMail,
    sendSessionCreate,
  };
}
```

### Parallel Execution

Use `Promise.all` for independent parallel operations:

```typescript
const [agent, session] = await Promise.all([
  db.query.agent.findFirst({ where: eq(agent.id, agentId) }),
  db.query.agentSession.findFirst({ where: eq(agentSession.id, sessionId) }),
]);
```

### Timeouts

Use `setTimeout` with stored callbacks to implement request timeouts:

```typescript
return new Promise<void>((resolve, reject) => {
  const timer = setTimeout(() => {
    pending.delete(requestId);
    reject(new Error(`Request ${requestId} timed out after ${timeoutMs}ms`));
  }, timeoutMs);

  pending.set(requestId, { resolve, reject, timer });
  conn.send(frame);
});
```

---

## Module Organization

### Package Structure

Each package follows this structure:

```
packages/<name>/
├── package.json         # Package metadata and exports
├── tsconfig.json        # Extends tsconfig.base.json
└── src/
    ├── index.ts         # Public exports (barrel file)
    ├── *.test.ts        # Tests co-located with source
    └── <feature>/       # Feature-specific subdirectories
```

### Multiple Entry Points

Use `exports` in `package.json` for multiple entry points:

```json
{
  "exports": {
    ".": {
      "types": "./src/index.ts",
      "default": "./src/index.ts"
    },
    "./schema": {
      "types": "./src/schema/index.ts",
      "default": "./src/schema/index.ts"
    }
  }
}
```

---

## Testing

### Framework: bun:test

Tests use bun's built-in test runner.

### Test File Structure

```typescript
import { describe, test, expect, beforeEach } from "bun:test";
import { createSidecarRouter, type WsHandle } from "./sidecar-handler";

function createMockWs(): WsHandle & { sent: string[]; closed: boolean } {
  return {
    sent: [],
    closed: false,
    send(data: string) {
      this.sent.push(data);
    },
    close() {
      this.closed = true;
    },
  };
}

describe("SidecarRouter", () => {
  let router: SidecarRouter;

  beforeEach(() => {
    router = createSidecarRouter();
  });

  test("registers a sidecar and routes mail", () => {
    const ws = createMockWs();
    router.handleOpen(ws);
    // ...
    expect(ws.sent).toHaveLength(1);
  });
});
```

### Key Patterns

- No shebang line needed
- Use `describe`/`test`/`expect` from `bun:test`
- Co-locate tests with source files (`*.test.ts`)
- Use `beforeEach` for shared setup

### Common Assertions

```typescript
// Equality
expect(actual).toBe(expected); // Strict equality
expect(actual).toEqual(expected); // Deep equality

// Boolean
expect(condition).toBeTruthy();
expect(condition).toBeFalsy();

// Collections
expect(array).toHaveLength(3);
expect(array).toContain(item);

// Errors
expect(() => fn()).toThrow(/pattern/);
await expect(asyncFn()).rejects.toThrow();
```

### Test Coverage Philosophy

Focus test coverage on logic specific to this codebase:

- Business logic and domain-specific validation
- Integration points between components
- Error handling paths and edge cases
- Custom algorithms and data transformations

Do not write tests that merely verify functionality provided by external libraries. Trust well-maintained libraries to do their job.

### Test File Locations

Two locations are used for tests:

- **Co-located unit tests**: `packages/<name>/src/*.test.ts`. These tests cover a single package's internals using mocks and synthetic inputs. The default for all per-module tests.

- **Integration-shaped tests**: `tests/<package-name>/`. Tests that target a package's behavior but need the `@interchange/inference-testing` harness, or that span multiple packages, live here. Co-locating harness-driven tests in `packages/<name>/src/` would force the package to depend on `@interchange/inference-testing`, creating a workspace dependency cycle (because the harness depends on the package). The `tests/` tree breaks that cycle. Tests spanning multiple packages live under `tests/<primary-target-package>/`; the "primary target" is whatever package's behavior the test is asserting, with the other packages as setup dependencies.

The legacy `test/integration/` directory still hosts `deploy-flow.test.ts`, which spawns real servers and is out of scope for current cleanup. New tests should not go there.

Tests that are not parallel-safe (e.g. spawn servers, perform real `isomorphic-git` operations against `os.tmpdir()`) are excluded per-file via `bunfig.toml` and run as a second `bun test` invocation after the main suite. The exclude pattern is a per-file marker, not a directory convention. The `test` script in the root `package.json` runs the excluded files in a second pass.

---

## Logging

The project uses `@interchange/log`, which re-exports LogTape and provides a `setup` helper for application-level configuration.

### Application Configuration

Configure logging once at application startup:

```typescript
import { setup } from "@interchange/log";

// Development: pretty ANSI output, debug level
// Production: JSON Lines output, info level
await setup();

// Or with custom configuration
await setup({
  dev: true,
  levels: { "hub.requests": "debug" },
});
```

### Package Logger Setup

Each package creates loggers using `getLogger` with hierarchical category names:

```typescript
import { getLogger } from "@interchange/log";

const logger = getLogger(["hub", "ws", "sidecar"]);
```

### Logger Naming Convention

Use hierarchical logger names starting with the app or package name:

- `["hub"]`
- `["hub", "ws", "sidecar"]`
- `["hub", "middleware", "grant"]`
- `["sidecar", "runtime"]`

### Usage

LogTape loggers support tagged template literals for structured messages:

```typescript
logger.info`Sidecar ${sidecarId} registered with ${String(count)} agents`;
logger.warn`No mail outbound handler; dropping mail for ${recipients.join(", ")}`;
logger.error`Failed to connect: ${err instanceof Error ? err.message : String(err)}`;
```

### No Console

ESLint enforces `no-console: error`. Always use the package logger instead of `console.log`.

---

## Documentation

### TSDoc Comments

Document public APIs with TSDoc:

```typescript
/**
 * Creates a hub-side websocket router for sidecar connections.
 *
 * @param config - Router configuration options
 * @returns A SidecarRouter instance
 */
export function createSidecarRouter(
  config: SidecarRouterConfig = {},
): SidecarRouter { ... }
```

### Inline Comments

Use sparingly, prefer self-documenting code. When needed:

```typescript
// XXX - Temporary workaround until upstream fix
// TODO - Refactor when we add support for X
```

### Avoiding Redundant Comments

Code should be self-documenting. Do not add comments that describe what the code obviously does. Decorative comment blocks (ASCII art dividers, section headers) add visual noise without providing meaningful information.

Do not reference external tracking artifacts in code comments. Comments like `// Issue 1: ...` are meaningless to future readers who lack the context. An exception is URLs that point at long-lived resources (RFCs, specs, upstream bug reports).

**When comments ARE useful:**

- Complex algorithms that aren't immediately obvious
- Non-obvious workarounds or edge cases
- TODO/FIXME/XXX markers for future work
- Business logic that requires explanation

---

## ESLint Rules

ESLint is configured in `eslint.config.ts` using TypeScript-ESLint's strict and stylistic rules.

Key rules and their implications:

- **No console**: `console.log` and similar are errors. Use the package logger instead.
- **Unused variables**: Must be prefixed with `_` (e.g., `_ctx`, `_unused`). This applies to function parameters, caught errors, and destructured values.
- **Type definitions**: Both `type` and `interface` are allowed (`consistent-type-definitions` is disabled). Choose based on the guidelines in [Interfaces vs Types](#interfaces-vs-types).
- **Tagged templates**: `no-unused-expressions` allows tagged template literals (used by the logger).

### Unused Variables

Prefix unused variables with `_`:

```typescript
// Good
const handleRequest = async (_ctx, requirements) => { ... };

// Bad - will error
const handleRequest = async (ctx, requirements) => { ... };  // ctx unused
```

---

## Git Workflow

### Setup

Configure git hooks before making commits:

```bash
git config core.hooksPath .githooks
```

The pre-commit hook checks out the full staged tree into a temporary directory and runs `make lint` against it, ensuring only committed content is validated. The commit-msg hook enforces line length limits.

### Lockfile Artifacts

When a commit modifies any `package.json` file, the corresponding `bun.lock` must be included in the same commit. Never leave lockfile updates in a separate follow-up commit.

### Commit Messages

- **Summary line**: Max 72 characters, non-empty
- **Blank line**: Required between summary and body (if body exists)
- **Body lines**: Max 72 characters each

Summary lines must be English sentences with no abbreviations, no markup (e.g. feat, chore), and not end with any punctuation. Do not include filenames in commit messages. Do not use bullet points or feature lists in the commit body; the code already shows this.

**Format:**

- Write concise messages (1-2 sentences) that explain why, not what
- Focus on the purpose and context of the change

**Good examples:**

```
Add retry logic for failed network requests

Fix race condition in session destroy cleanup

Document API response format
```

**Bad examples:**

```
feat: add retry logic
Update code (too vague)
Fix bug in server.ts (includes filename)
```

The commit message body (if present) can provide additional context, but avoid turning it into a feature checklist. Describe the motivation and high-level approach instead.
