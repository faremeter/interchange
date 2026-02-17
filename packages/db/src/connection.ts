import postgres from "postgres";

import type { DBConfig } from "./config";

export function createConnection(config: DBConfig) {
  return postgres({
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
    max: config.max ?? 10,
    ...(config.ssl !== undefined && { ssl: config.ssl }),
  });
}
