# @intx/db

Drizzle ORM client, schema, migrations, and row parsers for the
hub's PostgreSQL database. Owns the database-backed `GrantStore`,
the tenant hierarchy walk, the credential resolution layer, and
the `parseRow` family that validates `jsonb` columns at the
database boundary so internal code never re-checks them.

Consumed by `@intx/hub-api` (HTTP request handlers and grant
middleware) and `@intx/hub-sessions` (session orchestration and
agent repo metadata).

## Surface

- `@intx/db` — the client (`createDB`), config validator, migration
  runner, grant store, credential resolution, tenant hierarchy
  helpers, and the `parseRow` functions.
- `@intx/db/schema` — the Drizzle table exports, used by callers
  that compose queries against the schema directly.

```ts
import { createDB } from "@intx/db";
import { agent } from "@intx/db/schema";
import { eq } from "drizzle-orm";

const { db } = createDB(process.env);

const row = await db.query.agent.findFirst({
  where: eq(agent.id, agentId),
});
```

The `parseRow` functions (`parseAgentRow`, `parseGrantRow`, and so
on) validate each table's `jsonb` columns once and return fully
typed values; downstream code uses those values without
re-casting. See `CONVENTIONS.md` for the project-wide rule against
cast-at-callsite in DB consumers.
