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
