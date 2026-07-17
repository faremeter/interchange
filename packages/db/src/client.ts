import { type } from "arktype";
import { drizzle } from "drizzle-orm/postgres-js";

import { DBConfig } from "./config";
import { createConnection } from "./connection";
import * as schema from "./schema";

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

export type DB = ReturnType<typeof createDB>;

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
