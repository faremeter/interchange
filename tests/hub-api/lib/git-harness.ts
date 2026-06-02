// Integration-test harness for hub-API tests that need a real hub
// subprocess plus per-test postgres-schema isolation.
//
// Purpose
// -------
// Hub-API integration tests exercise the running hub end-to-end
// against the real database, the real route layer, and (for git
// tests) the real system git binary. This module provides every
// piece of scaffolding those tests share, so individual test files
// stay focused on the behaviour they are asserting on rather than
// on subprocess management, port allocation, env hygiene, and
// schema teardown.
//
// What this module provides
// -------------------------
// - `startHub`: spawn the real hub server bound to a random port,
//   pointed at a freshly migrated, dedicated postgres schema, and
//   return a stop handle that drops the schema after the process
//   exits.
// - `discoverGitBinary`: locate `git` on PATH, parse its version,
//   and enforce the `>= 2.34` floor. The version parser tolerates
//   the vendor suffix `(Apple Git-NNN)` that ships with macOS git.
//   `bin/check-env` enforces the same floor at repo bootstrap and
//   reimplements the parsing in bash; both layers stay in sync by
//   convention rather than by import.
// - `runGit`: invoke the system git with every config-related env
//   var redirected away from the developer's home so a user-local
//   `.gitconfig` cannot leak into a test run.
// - `tokenAskpassEnv`: produce a `GIT_ASKPASS` shim env block that
//   echoes a bearer token regardless of which prompt git asks for.
// - `installSshAllowedSigner`: write a per-repo allowed-signers
//   file and configure the repo to verify SSH signatures against
//   it. Required for `git log --show-signature` to render a
//   meaningful verdict on hub-signed commits in downstream tests.
// - `loadHarnessDbConfig`: resolve the postgres connection a test
//   should use, by reading the repo's `.env` and `.env.migrate`
//   directly. The harness never falls back to invented values
//   deep in the call graph; if a required var is missing, the
//   loader raises.
//
// Lifecycle contract
// ------------------
// Tests follow the same shape:
//
//   1. `const hub = await startHub();` (or `startHubTracked()` from
//      the test-level fixture wrapper, which auto-stops on teardown)
//   2. Issue HTTP requests against `hub.url`, mint tokens, run git
//      against the smart-HTTP routes, etc.
//   3. `await hub.stop();` — stops the spawned hub, drops the
//      per-test schema, and removes the hub-data tempdir.
//
// Tests MUST call `stop` on every `HubHandle` they obtain. The
// schema row, the postgres connections, the hub-data tempdir, and
// the spawned bun process are all owned by the handle; leaking any
// of them leaks resources between test files.
//
// Per-schema isolation model
// --------------------------
// Each call to `startHub` provisions its own postgres schema named
// `t_<timestamp>_<random>` (or a caller-supplied name) and runs
// migrations against it under the migration role. The spawned hub
// connects as the hub-app role with `PG_SCHEMA` set so its DB
// client routes every query into that schema. Schemas are dropped
// in `stop` and are not reused. Two concurrent tests cannot
// collide on table state because they live in different schemas;
// the underlying database is shared, but the schema namespace
// keeps the rows separate.
//
// Env-redirection guarantees
// --------------------------
// `runGit` builds an env block that redirects every config-related
// variable git consults — `HOME`, `XDG_CONFIG_HOME`,
// `GIT_CONFIG_GLOBAL`, `GIT_CONFIG_SYSTEM`, `GIT_CONFIG_NOSYSTEM`,
// `GIT_TERMINAL_PROMPT` — to a discardable tempdir or a guard
// sentinel. A developer's `~/.gitconfig` cannot leak into a test
// invocation; nor can git prompt for credentials on a TTY. The
// tempdir is removed after the invocation returns.
//
// Constraint discipline
// ---------------------
// `startHub` is the only edge in this module that knows what an
// absent `dbSchemaName` means: a freshly generated `t_<random>`
// name. Inner helpers never invent values. If `PG_SCHEMA` reaches
// the spawned hub, it was deliberately set here.

import { spawn, spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { mkdtemp, readFile, rm, writeFile, chmod } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import postgres from "postgres";

import { runMigrations, dropSchema, type DBConfig } from "@intx/db";

// ---------------------------------------------------------------------------
// Repo-root resolution
// ---------------------------------------------------------------------------

const HERE = path.dirname(fileURLToPath(import.meta.url));
// tests/hub-api/lib/git-harness.ts → repo root is three up.
const REPO_ROOT = path.resolve(HERE, "..", "..", "..");

// ---------------------------------------------------------------------------
// .env loading
// ---------------------------------------------------------------------------

function parseEnvFile(content: string): Record<string, string> {
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

async function loadEnvFile(p: string): Promise<Record<string, string>> {
  if (!existsSync(p)) return {};
  return parseEnvFile(await readFile(p, "utf-8"));
}

/**
 * Required hub-side env. The harness writes these explicitly into
 * the spawned process; any missing var fails loudly here rather
 * than deep inside the hub bootstrap.
 */
type HarnessEnv = {
  db: DBConfig;
  betterAuthSecret: string;
};

function requireKey(
  source: Record<string, string>,
  key: string,
  origin: string,
): string {
  const v = source[key];
  if (v === undefined || v === "") {
    throw new Error(
      `git-harness: required env var ${key} is missing from ${origin}; ` +
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
function optionalKey(source: Record<string, string>, key: string): string {
  return source[key] ?? "";
}

/**
 * Read the repo's `.env` + `.env.migrate` and surface the migration
 * user's credentials. The migration user is what the harness uses
 * to create schemas and apply DDL; the spawned hub still runs as
 * the hub user (loaded from `.env.hub`).
 */
export function loadHarnessDbConfig(): DBConfig {
  // Synchronous reader for the test bootstrap; we tolerate the
  // synchronous I/O here because this runs once per test file.
  const sharedPath = path.join(REPO_ROOT, ".env");
  const migratePath = path.join(REPO_ROOT, ".env.migrate");
  const shared = existsSync(sharedPath)
    ? parseEnvFile(readFileSync(sharedPath, "utf-8"))
    : {};
  const migrate = existsSync(migratePath)
    ? parseEnvFile(readFileSync(migratePath, "utf-8"))
    : {};
  const merged = { ...shared, ...migrate };
  return {
    host: requireKey(merged, "DB_HOST", ".env"),
    port: Number(requireKey(merged, "DB_PORT", ".env")),
    user: requireKey(merged, "DB_USER", ".env.migrate"),
    password: optionalKey(merged, "DB_PASSWORD"),
    database: requireKey(merged, "DB_NAME", ".env"),
  };
}

async function loadHubEnv(): Promise<HarnessEnv> {
  const shared = await loadEnvFile(path.join(REPO_ROOT, ".env"));
  const hub = await loadEnvFile(path.join(REPO_ROOT, ".env.hub"));

  const sharedAndHub = { ...shared, ...hub };
  return {
    db: {
      host: requireKey(sharedAndHub, "DB_HOST", ".env"),
      port: Number(requireKey(sharedAndHub, "DB_PORT", ".env")),
      // The hub itself uses the hub-app role at runtime; migration
      // creds are loaded separately by loadHarnessDbConfig.
      user: requireKey(sharedAndHub, "DB_USER", ".env.hub"),
      password: optionalKey(sharedAndHub, "DB_PASSWORD"),
      database: requireKey(sharedAndHub, "DB_NAME", ".env"),
    },
    betterAuthSecret: requireKey(
      sharedAndHub,
      "BETTER_AUTH_SECRET",
      ".env.hub",
    ),
  };
}

// ---------------------------------------------------------------------------
// Git binary discovery
// ---------------------------------------------------------------------------

export type GitVersion = {
  major: number;
  minor: number;
  patch: number;
};

export type GitBinaryInfo = {
  path: string;
  version: GitVersion;
  raw: string;
};

const GIT_MIN_MAJOR = 2;
const GIT_MIN_MINOR = 34;

function parseGitVersion(raw: string): GitVersion {
  // `git version 2.50.1 (Apple Git-155)` or `git version 2.34.1`.
  const m = raw.match(/^git version (\d+)\.(\d+)\.(\d+)/);
  if (!m) {
    throw new Error(`Cannot parse git version from: ${raw}`);
  }
  return {
    major: Number(m[1]),
    minor: Number(m[2]),
    patch: Number(m[3]),
  };
}

let cachedBinary: GitBinaryInfo | null = null;

export function discoverGitBinary(): GitBinaryInfo {
  if (cachedBinary !== null) return cachedBinary;

  const whichResult = spawnSync("git", ["--version"], { encoding: "utf-8" });
  if (whichResult.status !== 0) {
    throw new Error(
      `git --version exited with ${whichResult.status ?? "null"}: ` +
        (whichResult.stderr || whichResult.error?.message || ""),
    );
  }
  const raw = whichResult.stdout.trim();
  const version = parseGitVersion(raw);
  const acceptable =
    version.major > GIT_MIN_MAJOR ||
    (version.major === GIT_MIN_MAJOR && version.minor >= GIT_MIN_MINOR);
  if (!acceptable) {
    throw new Error(
      `git ${GIT_MIN_MAJOR}.${GIT_MIN_MINOR}+ required; found ${raw}`,
    );
  }

  // Resolve the absolute path so callers see a stable handle.
  const whichPath = spawnSync(
    process.platform === "win32" ? "where" : "which",
    ["git"],
    { encoding: "utf-8" },
  );
  if (whichPath.status !== 0) {
    throw new Error(
      `which git exited with ${whichPath.status ?? "null"}: ${whichPath.stderr || ""}`,
    );
  }
  const firstLine = whichPath.stdout.split("\n")[0];
  if (firstLine === undefined) {
    throw new Error("which git produced empty stdout");
  }
  const resolved = firstLine.trim();

  cachedBinary = { path: resolved, version, raw };
  return cachedBinary;
}

// ---------------------------------------------------------------------------
// runGit: invoke git with a fully redirected config environment
// ---------------------------------------------------------------------------

export type RunGitOptions = {
  cwd: string;
  env?: Record<string, string>;
};

export type RunGitResult = {
  stdout: string;
  stderr: string;
  status: number;
};

/**
 * Build the env block git should run under. Every variable that
 * could pull in a user-local configuration is either redirected at
 * a discardable directory or set to a guard sentinel.
 */
async function buildIsolatedGitEnv(
  extra: Record<string, string> | undefined,
): Promise<{ env: Record<string, string>; cleanupDir: string }> {
  const tempHome = await mkdtemp(path.join(os.tmpdir(), "harness-githome-"));
  const env: Record<string, string> = {
    ...(extra ?? {}),
    HOME: tempHome,
    XDG_CONFIG_HOME: tempHome,
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: "/dev/null",
    GIT_CONFIG_SYSTEM: "/dev/null",
    GIT_TERMINAL_PROMPT: "0",
  };
  // PATH must reach `git` even though we use an absolute binary
  // path; some subcommands (e.g. credential helpers, hooks) shell
  // out and lookup their own dependencies on PATH.
  if (process.env["PATH"] !== undefined) {
    env["PATH"] = process.env["PATH"];
  }
  return { env, cleanupDir: tempHome };
}

export async function runGit(
  args: string[],
  options: RunGitOptions,
): Promise<RunGitResult> {
  const binary = discoverGitBinary();
  const { env, cleanupDir } = await buildIsolatedGitEnv(options.env);
  try {
    return await new Promise<RunGitResult>((resolve, reject) => {
      const child = spawn(binary.path, args, {
        cwd: options.cwd,
        env,
        stdio: ["ignore", "pipe", "pipe"],
      });
      let stdout = "";
      let stderr = "";
      child.stdout.on("data", (c: Buffer) => {
        stdout += c.toString("utf-8");
      });
      child.stderr.on("data", (c: Buffer) => {
        stderr += c.toString("utf-8");
      });
      child.on("error", (e: Error) => {
        reject(e);
      });
      child.on("close", (code) => {
        resolve({ stdout, stderr, status: code ?? -1 });
      });
    });
  } finally {
    await rm(cleanupDir, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------
// tokenAskpassEnv: GIT_ASKPASS shim that echoes a bearer token
// ---------------------------------------------------------------------------

/**
 * Materialize an executable GIT_ASKPASS shim that prints the given
 * token to stdout for any prompt git issues. Returns an env block
 * that points GIT_ASKPASS at the shim and disables terminal
 * prompts. The shim's tempdir is leaked deliberately: it must
 * outlive the test, since git invocations can read from it
 * asynchronously after the test scope returns. Tests that need
 * teardown should track the returned `cleanupDir` via the optional
 * second-return form.
 */
export async function tokenAskpassEnv(
  token: string,
): Promise<Record<string, string>> {
  const dir = await mkdtemp(path.join(os.tmpdir(), "harness-askpass-"));
  const shim = path.join(dir, "askpass.sh");
  // Print the token unconditionally. The `$@` argument is ignored;
  // we echo regardless of whether git asks for "Username" or
  // "Password". Bearer-token usage requires "Username" to be any
  // value and "Password" to be the token, so a constant echo works
  // when the harness pairs this with a URL whose userinfo
  // already contains a non-empty username (e.g. `x-access-token`).
  // For the lone-username case, downstream tests provide a
  // dedicated username via the URL and this shim returns the token
  // for the password prompt.
  const body = `#!/bin/sh\nprintf '%s\\n' '${token.replace(/'/g, "'\\''")}'\n`;
  await writeFile(shim, body, { encoding: "utf-8" });
  await chmod(shim, 0o755);
  return {
    GIT_ASKPASS: shim,
    GIT_TERMINAL_PROMPT: "0",
  };
}

// ---------------------------------------------------------------------------
// installSshAllowedSigner: trust an SSH signing key for a repo
// ---------------------------------------------------------------------------

/**
 * Configure the given repo to verify SSH-signed commits against
 * the provided allowed-signer identity. Required when downstream
 * tests run `git log --show-signature` to inspect the verdict on
 * hub-signed commits.
 *
 * `pubKey` is the raw `ssh-ed25519 AAAA...` line as emitted by
 * `ssh-keygen -y`; we accept it as-is and only prepend the signer
 * identity column expected by OpenSSH's allowed-signers format.
 */
export async function installSshAllowedSigner(
  repoDir: string,
  pubKey: string,
  signerEmail: string,
): Promise<void> {
  const allowedSignersPath = path.join(repoDir, ".harness-allowed-signers");
  const line = `${signerEmail} ${pubKey.trim()}\n`;
  await writeFile(allowedSignersPath, line, { encoding: "utf-8" });
  const set = async (key: string, value: string) => {
    const r = await runGit(["config", key, value], { cwd: repoDir });
    if (r.status !== 0) {
      throw new Error(
        `git config ${key} failed: ${r.stderr.trim() || r.stdout.trim()}`,
      );
    }
  };
  await set("gpg.format", "ssh");
  await set("gpg.ssh.allowedSignersFile", allowedSignersPath);
}

// ---------------------------------------------------------------------------
// Random port and schema name allocation
// ---------------------------------------------------------------------------

async function allocateRandomPort(): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (typeof addr === "object" && addr !== null && "port" in addr) {
        const port = addr.port;
        server.close(() => {
          resolve(port);
        });
      } else {
        server.close();
        reject(new Error("listener did not yield a port"));
      }
    });
  });
}

function randomSchemaName(): string {
  // Postgres schema names allowed by our identifier quoter are
  // permissive, but we keep this conservative for diagnostics.
  const rand = Math.random().toString(36).slice(2, 10);
  return `t_${Date.now().toString(36)}_${rand}`;
}

// ---------------------------------------------------------------------------
// startHub: spawn the hub against a fresh schema, return a stop handle
// ---------------------------------------------------------------------------

export type StartHubOptions = {
  /**
   * Specific schema name to use. When omitted, the harness
   * allocates `t_<timestamp>_<random>`. The schema is created if
   * absent and dropped on stop.
   */
  dbSchemaName?: string;
};

export type HubHandle = {
  url: string;
  schema: string;
  /**
   * On-disk root the hub was spawned with. Tests that need to
   * pre-stage repo content under `<dataDir>/<directoryPrefix>/<id>`
   * (e.g. seeding agent-state deploy artifacts ahead of a clone test)
   * read this path. The directory is cleaned up by `stop`; tests must
   * not delete it themselves.
   */
  dataDir: string;
  stop: () => Promise<void>;
};

async function waitForHub(url: string, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastErr: Error | null = null;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url);
      if (res.status < 500) return;
      lastErr = new Error(`hub returned status ${res.status}`);
    } catch (e) {
      lastErr = e instanceof Error ? e : new Error(String(e));
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(
    `hub did not become ready within ${timeoutMs}ms (last: ${lastErr?.message ?? "no probe"})`,
  );
}

export async function startHub(
  options: StartHubOptions = {},
): Promise<HubHandle> {
  const migrateConfig = loadHarnessDbConfig();
  const hubEnv = await loadHubEnv();

  const schema = options.dbSchemaName ?? randomSchemaName();

  // 1. Provision the schema and migrate it under the migration
  // role. The hub-app role then sees the tables via its own
  // connection.
  await runMigrations(migrateConfig, { schema });

  // The hub-app role needs explicit grants on this schema, since
  // the default-privileges grants apply only to the public schema.
  // Grant the same DML + USAGE the dev `bin/db-reset` grants.
  {
    const sql = postgres({
      host: migrateConfig.host,
      port: migrateConfig.port,
      user: migrateConfig.user,
      password: migrateConfig.password,
      database: migrateConfig.database,
      max: 1,
    });
    try {
      const quote = (n: string) => `"${n.replace(/"/g, '""')}"`;
      const schemaIdent = quote(schema);
      const hubRole = quote(hubEnv.db.user);
      await sql.unsafe(`GRANT USAGE ON SCHEMA ${schemaIdent} TO ${hubRole}`);
      await sql.unsafe(
        `GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ${schemaIdent} TO ${hubRole}`,
      );
      await sql.unsafe(
        `GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ${schemaIdent} TO ${hubRole}`,
      );
    } finally {
      await sql.end();
    }
  }

  // 2. Allocate a port and a hub-data dir.
  const port = await allocateRandomPort();
  const hubDataDir = await mkdtemp(path.join(os.tmpdir(), "harness-hubdata-"));

  // 3. Spawn the hub. The hub's bootstrap reads PG_SCHEMA and
  // threads it into the runtime DB client.
  const hubSrc = path.join(REPO_ROOT, "apps", "hub", "src", "index.ts");
  const childEnv: NodeJS.ProcessEnv = {
    ...process.env,
    DB_HOST: hubEnv.db.host,
    DB_PORT: String(hubEnv.db.port),
    DB_USER: hubEnv.db.user,
    DB_PASSWORD: hubEnv.db.password,
    DB_NAME: hubEnv.db.database,
    PG_SCHEMA: schema,
    PORT: String(port),
    HUB_DATA_DIR: hubDataDir,
    BETTER_AUTH_SECRET: hubEnv.betterAuthSecret,
    BETTER_AUTH_BASE_URL: `http://127.0.0.1:${port}`,
  };
  const child = spawn("bun", ["run", hubSrc], {
    cwd: REPO_ROOT,
    env: childEnv,
    stdio: ["ignore", "pipe", "pipe"],
  });

  // Bucket stdout/stderr for diagnostics on failure.
  const logs: string[] = [];
  child.stdout.on("data", (c: Buffer) => {
    logs.push(c.toString("utf-8"));
  });
  child.stderr.on("data", (c: Buffer) => {
    logs.push(c.toString("utf-8"));
  });

  let exited = false;
  let exitCode: number | null = null;
  const exitPromise = new Promise<void>((resolve) => {
    child.on("close", (code) => {
      exited = true;
      exitCode = code;
      resolve();
    });
  });

  const url = `http://127.0.0.1:${port}`;

  try {
    await waitForHub(url, 30_000);
  } catch (e) {
    // Surface what the hub printed so the test failure is
    // actionable.
    if (!exited) {
      child.kill("SIGTERM");
      await new Promise((r) => setTimeout(r, 500));
      if (!exited) child.kill("SIGKILL");
    }
    await dropSchema(migrateConfig, { schema });
    await rm(hubDataDir, { recursive: true, force: true });
    const message = e instanceof Error ? e.message : String(e);
    throw new Error(`${message}\n--- hub output ---\n${logs.join("")}`);
  }

  const stop = async (): Promise<void> => {
    if (!exited) {
      child.kill("SIGTERM");
      const killTimer = setTimeout(() => {
        if (!exited) child.kill("SIGKILL");
      }, 2000);
      await exitPromise;
      clearTimeout(killTimer);
    }
    void exitCode;
    await dropSchema(migrateConfig, { schema });
    await rm(hubDataDir, { recursive: true, force: true });
  };

  return { url, schema, dataDir: hubDataDir, stop };
}
