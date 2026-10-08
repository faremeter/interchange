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
    connection: {
      TimeZone: "UTC",
      statement_timeout: config.statementTimeoutMs ?? 60_000,
      ...(config.schema !== undefined && {
        // Pin search_path so ORM-issued queries (which bind table names
        // without a schema qualifier) resolve to the caller's schema; the
        // migration runner bakes the schema into its own SQL.
        search_path: quoteIdentifier(config.schema),
      }),
    },
  });
}
