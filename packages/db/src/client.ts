import { type } from "arktype";
import type { ExtractTablesWithRelations } from "drizzle-orm";
import type { PgDatabase, PgQueryResultHKT } from "drizzle-orm/pg-core";
import { drizzle } from "drizzle-orm/postgres-js";

import { DBConfig } from "./config";
import { createConnection } from "./connection";
import * as schema from "./schema";

/**
 * A drizzle database over the hub schema, typed against the driver-agnostic
 * `PgDatabase` base rather than a single driver. `createDB` builds one over
 * postgres-js; a caller can equally build one over pglite -- `drizzle(handle,
 * { schema })` from `drizzle-orm/pglite`, with the schema re-exported from
 * `@intx/db/schema` -- and pass it to the store factories. The required
 * `$client` keeps a bare `PgTransaction` unassignable (a transaction carries no
 * `$client`), so a parameter typed against this still rejects a tx where only a
 * top-level database belongs.
 */
export type AnyPgDatabase = PgDatabase<
  PgQueryResultHKT,
  typeof schema,
  ExtractTablesWithRelations<typeof schema>
> & { $client: unknown };

/**
 * The hub database handle: the database itself, plus `transaction` and
 * `close`. `createDB` returns the postgres-js instantiation, but the handle is
 * typed against `AnyPgDatabase` so the stores accept any pg driver -- a
 * caller-built pglite database included.
 */
export interface DB {
  db: AnyPgDatabase;
  transaction: AnyPgDatabase["transaction"];
  close: () => Promise<void>;
}

export function createDB(raw: unknown) {
  const config = DBConfig(raw);
  if (config instanceof type.errors) {
    throw new Error(`Invalid database config: ${config.summary}`);
  }

  const sql = createConnection(config);
  const db = drizzle(sql, { schema });

  return {
    db,
    transaction: db.transaction.bind(db),
    close: () => sql.end(),
  };
}

// `createDB` returns the concrete postgres-js handle so its own callers keep
// the driver's precise result types (e.g. a typed `db.execute`). This assures
// at the definition site that the concrete handle still satisfies `DB`, the
// driver-agnostic contract the stores consume.
const _createDBReturnsDB: (raw: unknown) => DB = createDB;
void _createDBReturnsDB;

/**
 * A handle that can execute queries: either the top-level `db` or a
 * transaction handle passed into a `db.transaction` callback. Store methods
 * that accept an optional `tx` type it against this so a caller can hand in
 * the transaction object and have the write join the surrounding transaction.
 * `DB["db"]` alone rejects a `PgTransaction` (it lacks the `$client` field the
 * top-level database carries), so a bare `DB["db"]` parameter cannot accept a
 * tx.
 */
export type DBExecutor =
  | DB["db"]
  | Parameters<Parameters<DB["db"]["transaction"]>[0]>[0];
