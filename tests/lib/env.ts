// Shared `.env`-reading primitives for the test harnesses under
// `tests/`.
//
// Both the hub-subprocess harness (`tests/hub-api/lib/git-harness.ts`)
// and the DB-only resolution harness (`tests/lib/db-harness.ts`) read
// the repo's `.env*` files to discover database credentials. Keeping
// the repo-root resolution and the parse/require helpers here gives a
// single source of truth: the harnesses never fall back to invented
// values deep in the call graph, and a missing required var raises
// loudly at the point of use.

import { existsSync, readFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
// tests/lib/env.ts → repo root is two up.
export const REPO_ROOT = path.resolve(HERE, "..", "..");

export function parseEnvFile(content: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const line of content.split("\n")) {
    const trimmed = line.trim();
    if (trimmed === "" || trimmed.startsWith("#")) continue;
    const stripped = trimmed.startsWith("export ") ? trimmed.slice(7) : trimmed;
    const eq = stripped.indexOf("=");
    if (eq < 0) continue;
    const k = stripped.slice(0, eq).trim();
    let v = stripped.slice(eq + 1).trim();
    if (
      (v.startsWith('"') && v.endsWith('"')) ||
      (v.startsWith("'") && v.endsWith("'"))
    ) {
      v = v.slice(1, -1);
    }
    out[k] = v;
  }
  return out;
}

export function parseEnvFileSync(p: string): Record<string, string> {
  if (!existsSync(p)) return {};
  return parseEnvFile(readFileSync(p, "utf-8"));
}

export async function loadEnvFile(p: string): Promise<Record<string, string>> {
  if (!existsSync(p)) return {};
  return parseEnvFile(await readFile(p, "utf-8"));
}

export function requireKey(
  source: Record<string, string>,
  key: string,
  origin: string,
): string {
  const v = source[key];
  if (v === undefined || v === "") {
    throw new Error(
      `test harness: required env var ${key} is missing from ${origin}; ` +
        `populate ${origin} (see .env*.example) before running integration tests`,
    );
  }
  return v;
}

// DB_PASSWORD is auth material whose required-ness depends on the
// server's pg_hba.conf. Under `trust` or `peer` the server never asks
// for a password and an empty value is correct; under `md5`/`scram`
// the empty value will surface as a real libpq authentication error
// at connect time, which is more informative than a synthetic env-var
// check here.
export function optionalKey(
  source: Record<string, string>,
  key: string,
): string {
  return source[key] ?? "";
}
