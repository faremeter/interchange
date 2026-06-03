# @intx/log

Thin wrapper around LogTape that the rest of the monorepo uses for
structured logging. Re-exports a narrow slice of the LogTape API —
only the symbols other `@intx/*` packages actually use today — and
adds a `setup()` helper that installs the project's preferred
development and production sinks.

Every package gets its logger through `getLogger`; applications
call `setup()` once at startup to choose the output format and
override per-category log levels.

Consumers that need a piece of LogTape this package does not
re-export should import it from `@logtape/logtape` directly rather
than widen this surface speculatively. Widen it here only when at
least one consumer needs the symbol.

```ts
import { getLogger, setup } from "@intx/log";

await setup({ dev: true, levels: { "hub.requests": "debug" } });

const logger = getLogger(["hub", "ws", "sidecar"]);
logger.info`Sidecar ${sidecarId} registered`;
```

## Surface

- `@intx/log` — `getLogger` for hierarchical loggers, `setup` and
  `SetupOptions` for application configuration, plus
  `configureSync`, `getConfig`, and `resetSync` for tests that need
  to drive LogTape configuration directly. Importing the package
  also installs the default console sink as a side effect so early
  diagnostics work before `setup()` is called.
- `@intx/log/hono` — Hono middleware re-exported from
  `@logtape/hono` for HTTP request logging. Hono is an optional
  peer dependency; only import this entry point from packages that
  already pull Hono in.
