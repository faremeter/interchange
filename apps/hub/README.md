# @intx/hub-app

The control-plane server process. Wires the control-plane packages
into a single Hono application and exports the Bun server that
operators run.

`src/index.ts` opens the Postgres connection (`@intx/db`),
constructs the better-auth wrapper and grant store, generates the
hub's deploy signing key, and builds the agent-repo, asset, and
session services from `@intx/hub-sessions`. It then assembles the
Hono app with `createApp` from `@intx/hub-api`, mounts the sidecar
WebSocket upgrade handler, and exports a Bun server object
(`fetch`, `websocket`, `port`) as the module default.

Run it under Bun. Startup requires `HUB_DATA_DIR` and reads
Postgres connection settings from `DB_HOST`, `DB_PORT`, `DB_USER`,
`DB_PASSWORD`, and `DB_NAME`. Optional `PG_SCHEMA` pins a dedicated
schema (used by the integration-test harness), `HUB_MAX_TARBALL_BYTES`
overrides the 10 MiB tool-package upload cap, and `PORT` sets the
listen port (default 3000).
