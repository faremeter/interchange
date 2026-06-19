# @intx/db

Drizzle ORM client, schema, migrations, and row parsers for the
hub's PostgreSQL database. Owns the database-backed `GrantStore`,
the tenant hierarchy walk, the credential resolution layer, the
model catalog and its resolution into ordered inference sources,
and the `parseRow` family that validates `jsonb` columns at the
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

## Model catalog

The `model`, `model_provider`, `model_offering`, and `model_pricing`
tables hold a tenant-scoped catalog of models, the providers that
serve them, their pairings (offerings), and append-only multi-currency
pricing. `catalog-resolution` lists the models, providers, and
offerings visible to a tenant after walking the ancestor chain
leaf-to-root: the nearest tenant to define an entry wins (shadowing),
and a disabled entry suppresses it for that tenant and its
descendants. `model-source-resolution` turns an agent's model
requirements plus a launch's invoker preferences into an ordered
`InferenceSource[]` — the head is the active source, the tail is the
failover chain — dereferencing each offering's provider credential to
the secret. `pricing.resolveActivePrice` selects, per currency, the
row in effect at a given time from the append-only history, and
`getDescendantTenants` walks the subtree so a catalog edit can be
pushed to every affected running instance.
