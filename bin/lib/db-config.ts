// Validated database connection inputs for the bin scripts that open a direct
// `db` handle (bin/seed's workflow-definition seeding). The `DB_*` parse lives
// in `bin/lib` so it can be unit-tested without importing an entry point.

import { requireEnvVar, requireIntVar } from "./env";

export type DbConfig = {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  schema?: string;
  statementTimeoutMs?: number;
};

/**
 * Validate and resolve the database connection inputs from an environment
 * map, throwing with a diagnostic naming the offending variable on a missing
 * required value or non-positive `DB_PORT`. `PG_SCHEMA` and
 * `DB_STATEMENT_TIMEOUT_MS` are optional, threaded through only when set.
 */
export function resolveDbConfig(
  env: Record<string, string | undefined>,
): DbConfig {
  const port = requireIntVar(env, "DB_PORT");
  const schema = env["PG_SCHEMA"];
  const statementTimeout = env["DB_STATEMENT_TIMEOUT_MS"];

  return {
    host: requireEnvVar(env, "DB_HOST"),
    port,
    user: requireEnvVar(env, "DB_USER"),
    password: requireEnvVar(env, "DB_PASSWORD"),
    database: requireEnvVar(env, "DB_NAME"),
    ...(schema !== undefined && schema !== "" && { schema }),
    ...(statementTimeout !== undefined &&
      statementTimeout !== "" && {
        statementTimeoutMs: requireIntVar(env, "DB_STATEMENT_TIMEOUT_MS"),
      }),
  };
}
