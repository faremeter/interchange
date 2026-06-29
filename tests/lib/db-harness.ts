// DB-side test harness primitives shared across the `tests/` tree.
//
// This module resolves the postgres connection a test should use by
// reading the repo's `.env` + `.env.migrate` directly (the migration
// role, which owns DDL and can create/drop schemas), and provides the
// per-test schema-name generator. Higher-level harness lifecycle
// helpers build on these.

import { existsSync } from "node:fs";
import path from "node:path";

import type { DBConfig } from "@intx/db";

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
