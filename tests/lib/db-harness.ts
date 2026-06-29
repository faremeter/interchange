// DB-side test harness primitives shared across the `tests/` tree.
//
// This module resolves the postgres connection a test should use by
// reading the repo's `.env` + `.env.migrate` directly (the migration
// role, which owns DDL and can create/drop schemas), and provides the
// per-test schema-name generator. Higher-level harness lifecycle
// helpers build on these.

import { existsSync } from "node:fs";
import path from "node:path";

import {
  createDB,
  dropSchema,
  runMigrations,
  type DB,
  type DBConfig,
} from "@intx/db";
import { sql } from "drizzle-orm";

import { REPO_ROOT, optionalKey, parseEnvFileSync, requireKey } from "./env";

/**
 * Returns true when the repo has the `.env` files this harness reads:
 * `.env` (connection host/port/database) and `.env.migrate` (the
 * migration role). Test suites use this with `describe.skipIf(...)` so a
 * fresh checkout without a configured database can still run `make all`.
 *
 * Absence-only: a file that exists but lacks a required key still
 * surfaces a loud error from `loadHarnessDbConfig`. The gate is
 * specifically for the "no database env at all" case.
 */
export function harnessDbEnvAvailable(): boolean {
  return (
    existsSync(path.join(REPO_ROOT, ".env")) &&
    existsSync(path.join(REPO_ROOT, ".env.migrate"))
  );
}

/**
 * Read the repo's `.env` + `.env.migrate` and surface the migration
 * user's credentials. The migration user is what the harnesses use to
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

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/**
 * A migrated, isolated postgres schema with a drizzle client bound to
 * it, for tests that exercise real query behaviour instead of mocking
 * the drizzle client.
 *
 * Lifecycle: `createTestDb` migrates a fresh schema once; `reset`
 * truncates every table between cases so each test starts empty;
 * `close` drops the schema and ends the connection. The client
 * connects as the migration role (which owns the schema), so no
 * grants are needed and no hub subprocess is involved.
 */
export type TestDb = {
  db: DB["db"];
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
