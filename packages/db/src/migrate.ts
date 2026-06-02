import { readdir, readFile } from "node:fs/promises";
import path from "node:path";

import { type } from "arktype";
import postgres from "postgres";

import { DBConfig } from "./config";

// Resolution of the migrations directory: the package layout pins
// the drizzle-generated SQL at `<pkgRoot>/migrations`. This file
// lives at `<pkgRoot>/src/migrate.ts`, so the directory is one level
// up from `__dirname`. Resolving via `import.meta` keeps the runtime
// honest about where it is reading SQL from instead of relying on a
// cwd-relative path that breaks under callers that change `cwd`.
const MIGRATIONS_DIR = path.resolve(
  path.dirname(new URL(import.meta.url).pathname),
  "..",
  "migrations",
);

function quoteIdentifier(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Apply the drizzle-generated migration SQL into the given postgres
 * schema. The schema is created if absent; nothing else is dropped.
 *
 * The migration source files reference the destination schema
 * literally (e.g. `"public"."user"` in FK constraints). To make the
 * same source apply cleanly into an arbitrary schema, we substitute
 * the literal token `"public"` with the quoted target schema before
 * executing each file. This is a textual substitution rather than a
 * postgres-side `search_path` trick because the FK references are
 * fully qualified; `search_path` would not redirect them.
 *
 * The substitution is bounded: the migration source is
 * machine-generated and never contains the string `"public"` other
 * than in schema-qualified identifiers, so there is no ambiguity to
 * worry about. The substitution is a no-op when the target schema is
 * literally `public`, which is the common case.
 */
export async function runMigrations(
  configRaw: unknown,
  options: { schema: string },
): Promise<void> {
  const config = DBConfig(configRaw);
  if (config instanceof type.errors) {
    throw new Error(`Invalid database config: ${config.summary}`);
  }

  const { schema } = options;
  if (schema.length === 0) {
    throw new Error("runMigrations: schema name must not be empty");
  }

  const schemaIdent = quoteIdentifier(schema);

  // Pin search_path on the migration connection so unqualified
  // `CREATE TABLE "name"` statements land in the target schema.
  // The FK references in the source SQL are already schema-qualified
  // (`"public"."user"`); we rewrite those to the target schema
  // below. Together these two mechanisms route every object the
  // migration touches into the caller's schema.
  const sql = postgres({
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
    max: 1,
    // Suppress NOTICE-level diagnostics (cascade reports, schema
    // already exists). They are informational and the harness's
    // teardown path is fine; postgres.js logs them by default.
    onnotice: () => undefined,
    ...(config.ssl !== undefined && { ssl: config.ssl }),
    connection: { search_path: schemaIdent },
  });

  try {
    // The CREATE SCHEMA must run on a connection that does not
    // require the schema to already exist for search_path to be
    // applied. postgres.js sets the GUC after connection-open, so
    // CREATE SCHEMA IF NOT EXISTS here is the first statement and
    // creates the schema before search_path matters.
    await sql.unsafe(`CREATE SCHEMA IF NOT EXISTS ${schemaIdent}`);

    const files = (await readdir(MIGRATIONS_DIR))
      .filter((f) => f.endsWith(".sql"))
      .sort();

    if (files.length === 0) {
      throw new Error(
        `runMigrations: no .sql files found in ${MIGRATIONS_DIR}`,
      );
    }

    for (const file of files) {
      const raw = await readFile(path.join(MIGRATIONS_DIR, file), "utf-8");
      // Substitute the literal `"public"` schema reference with the
      // target schema. The drizzle-generated SQL only uses `"public"`
      // as a quoted schema identifier in FK references; no string
      // literals contain it.
      const rendered = raw.replace(/"public"/g, schemaIdent);
      // drizzle emits multi-statement files separated by its own
      // statement-breakpoint marker. Split on it and execute each
      // statement individually so a syntax error in one statement
      // surfaces with the right context.
      const statements = rendered
        .split("--> statement-breakpoint")
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
      for (const stmt of statements) {
        await sql.unsafe(stmt);
      }
    }
  } finally {
    await sql.end();
  }
}

/**
 * Drop the given schema along with everything in it. Used by the
 * integration-test harness to tear down per-test schemas.
 */
export async function dropSchema(
  configRaw: unknown,
  options: { schema: string },
): Promise<void> {
  const config = DBConfig(configRaw);
  if (config instanceof type.errors) {
    throw new Error(`Invalid database config: ${config.summary}`);
  }

  const sql = postgres({
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
    max: 1,
    // Suppress NOTICE-level diagnostics (cascade reports, schema
    // already exists). They are informational and the harness's
    // teardown path is fine; postgres.js logs them by default.
    onnotice: () => undefined,
    ...(config.ssl !== undefined && { ssl: config.ssl }),
  });

  try {
    const schemaIdent = quoteIdentifier(options.schema);
    await sql.unsafe(`DROP SCHEMA IF EXISTS ${schemaIdent} CASCADE`);
  } finally {
    await sql.end();
  }
}
