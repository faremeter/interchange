// Validated database connection inputs for the bin scripts that open a direct
// `db` handle (bin/seed's workflow-definition seeding).
//
// The `DB_*` parse lives in `bin/lib` so it can be unit-tested without
// importing an entry point.

import { requireEnvVar, requireIntVar } from "./env";

export type DbConfig = {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  schema?: string;
};

/**
 * Validate and resolve the database connection inputs from an environment
 * map. Throws with a diagnostic naming the offending variable when a required
 * value is missing or `DB_PORT` is not a positive integer, so a misconfigured
 * environment fails at the boundary rather than surfacing as an opaque
 * database-config error. `PG_SCHEMA` is threaded through only when set,
 * matching how the hub pins its connection schema.
 */
export function resolveDbConfig(
  env: Record<string, string | undefined>,
): DbConfig {
  const port = requireIntVar(env, "DB_PORT");
  const schema = env["PG_SCHEMA"];

  return {
    host: requireEnvVar(env, "DB_HOST"),
    port,
    user: requireEnvVar(env, "DB_USER"),
    password: requireEnvVar(env, "DB_PASSWORD"),
    database: requireEnvVar(env, "DB_NAME"),
    ...(schema !== undefined && schema !== "" && { schema }),
  };
}
