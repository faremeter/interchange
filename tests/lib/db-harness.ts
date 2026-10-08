// DB-side test harness primitives shared across the `tests/` tree:
// resolves the postgres connection from `.env` + `.env.migrate` (the
// migration role, which owns DDL) and generates per-test schema names.

import { existsSync } from "node:fs";
import path from "node:path";

import { createDB, dropSchema, runMigrations, type DBConfig } from "@intx/db";
import { sql } from "drizzle-orm";

import { REPO_ROOT, optionalKey, parseEnvFileSync, requireKey } from "./env";
import { quoteIdent } from "./grants";

/**
 * True when the repo has `.env` and `.env.migrate`, so DB-dependent
 * suites can skip on a fresh checkout instead of failing `make all`.
 * A file that exists but lacks a required key still errors loudly in
 * `loadHarnessDbConfig`; this gate is only for the "no env at all" case.
 *
 * A skip must cover every DB-touching hook: wrap the suite in
 * `describe.skipIf(!harnessDbEnvAvailable())(...)` and keep the hooks
 * inside the `describe`, or open a file-scope hook with
 * `if (!harnessDbEnvAvailable()) return;` (file-scope hooks run even
 * when `skipIf` skips the bodies).
 */
export function harnessDbEnvAvailable(): boolean {
  return (
    existsSync(path.join(REPO_ROOT, ".env")) &&
    existsSync(path.join(REPO_ROOT, ".env.migrate"))
  );
}

/**
 * True when the repo also has `.env.hub` (the hub role). Callers that
 * read that role gate on this, so a fresh worktree skips instead of
 * failing on a missing key mid-suite.
 */
export function harnessHubEnvAvailable(): boolean {
  return (
    harnessDbEnvAvailable() && existsSync(path.join(REPO_ROOT, ".env.hub"))
  );
}

/**
 * Migration-role credentials from `.env` + `.env.migrate`, used to
 * create schemas and apply DDL; a spawned hub still runs as the hub
 * user (loaded separately).
 */
export function loadHarnessDbConfig(): DBConfig {
  // Synchronous reader for the test bootstrap; the synchronous I/O is
  // tolerable because this runs once per test file.
  const shared = parseEnvFileSync(path.join(REPO_ROOT, ".env"));
  const migrate = parseEnvFileSync(path.join(REPO_ROOT, ".env.migrate"));
  const merged = { ...shared, ...migrate };
  return {
    host: requireKey(merged, "DB_HOST", ".env"),
    port: Number(requireKey(merged, "DB_PORT", ".env")),
    user: requireKey(merged, "DB_USER", ".env.migrate"),
    password: optionalKey(merged, "DB_PASSWORD"),
    database: requireKey(merged, "DB_NAME", ".env"),
  };
}

export function randomSchemaName(): string {
  // Postgres schema names allowed by our identifier quoter are
  // permissive, but we keep this conservative for diagnostics.
  const rand = Math.random().toString(36).slice(2, 10);
  return `t_${Date.now().toString(36)}_${rand}`;
}

/**
 * A migrated, isolated postgres schema with a drizzle client bound to
 * it, for tests that exercise real query behaviour. `reset` truncates
 * every table between cases; `close` drops the schema. The client
 * connects as the migration role, which owns the schema.
 */
export type TestDb = {
  // Pin `db` to the concrete postgres-js type `createDB` returns so raw
  // `db.execute` results stay typed for tests that read `pg_*` rows.
  db: ReturnType<typeof createDB>["db"];
  schema: string;
  reset: () => Promise<void>;
  close: () => Promise<void>;
};

export async function createTestDb(): Promise<TestDb> {
  const config = loadHarnessDbConfig();
  const schema = randomSchemaName();
  await runMigrations(config, { schema });
  const handle = createDB({ ...config, schema });
  const db = handle.db;

  const reset = async (): Promise<void> => {
    const rows = await db.execute(
      sql`SELECT tablename FROM pg_tables WHERE schemaname = ${schema}`,
    );
    const targets: string[] = [];
    for (const row of rows) {
      const name = row["tablename"];
      if (typeof name !== "string") {
        throw new Error(
          `createTestDb.reset: unexpected pg_tables row: ${JSON.stringify(row)}`,
        );
      }
      targets.push(`${quoteIdent(schema)}.${quoteIdent(name)}`);
    }
    if (targets.length > 0) {
      await db.execute(
        sql.raw(`TRUNCATE ${targets.join(", ")} RESTART IDENTITY CASCADE`),
      );
    }
  };

  const close = async (): Promise<void> => {
    await handle.close();
    await dropSchema(config, { schema });
  };

  return { db, schema, reset, close };
}
