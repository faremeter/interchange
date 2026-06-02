import postgres from "postgres";

import type { DBConfig } from "./config";

function quoteIdentifier(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

export function createConnection(config: DBConfig) {
  return postgres({
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
    max: config.max ?? 10,
    ...(config.ssl !== undefined && { ssl: config.ssl }),
    ...(config.schema !== undefined && {
      // Pin the connection's search_path so unqualified table
      // references resolve to the caller's schema. The migration
      // runner emits SQL with the schema baked into FK references,
      // but ORM-issued queries bind table names without a schema
      // qualifier and rely on this setting.
      connection: { search_path: quoteIdentifier(config.schema) },
    }),
  });
}
