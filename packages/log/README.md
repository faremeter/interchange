# @intx/log

Thin wrapper around LogTape that the rest of the monorepo uses for
structured logging. Re-exports the core LogTape API as-is so most
callers only need this package, and adds a `setup()` helper that
installs the project's preferred development and production sinks.

Every package gets its logger through `getLogger`; applications
call `setup()` once at startup to choose the output format and
override per-category log levels.

```ts
import { getLogger, setup } from "@intx/log";

await setup({ dev: true, levels: { "hub.requests": "debug" } });

const logger = getLogger(["hub", "ws", "sidecar"]);
logger.info`Sidecar ${sidecarId} registered`;
```

## Surface

- `@intx/log` — the LogTape re-export plus `setup()`,
  `getLogger`, and the sink installation that runs at import time
  so early diagnostics work before `setup()` is called.
- `@intx/log/hono` — Hono middleware re-exported from
  `@logtape/hono` for HTTP request logging. Hono is an optional
  peer dependency; only import this entry point from packages that
  already pull Hono in.
