// Validated database connection inputs for `bin/db-backfill`.
//
// The `DB_*` parse lives here, apart from `bin/db-backfill.ts`, so it can be
// unit-tested without importing the entry point: the entry shares a basename
// with the `bin/db-backfill` launcher wrapper, and a test importing
// `./db-backfill` would resolve to the extensionless bash wrapper rather than
// the `.ts`. A `bin/lib` helper has no such twin.

export type BackfillDbConfig = {
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
export function resolveBackfillDbConfig(
  env: Record<string, string | undefined>,
): BackfillDbConfig {
  const requireVar = (name: string): string => {
    const value = env[name];
    if (value === undefined || value === "") {
      throw new Error(`${name} is required`);
    }
    return value;
  };

  const portRaw = requireVar("DB_PORT");
  const port = Number(portRaw);
  if (!Number.isInteger(port) || port <= 0) {
    throw new Error(
      `DB_PORT must be a positive integer; got ${JSON.stringify(portRaw)}`,
    );
  }

  const schema = env["PG_SCHEMA"];

  return {
    host: requireVar("DB_HOST"),
    port,
    user: requireVar("DB_USER"),
    password: requireVar("DB_PASSWORD"),
    database: requireVar("DB_NAME"),
    ...(schema !== undefined && schema !== "" && { schema }),
  };
}
