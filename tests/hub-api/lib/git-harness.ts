// Integration-test harness for hub-API tests: spawns a real hub
// subprocess against a freshly migrated, per-test postgres schema and
// returns a stop handle that drops the schema and removes the
// hub-data tempdir. Node-bound: it spawns child processes via
// `node:child_process`.
//
// Tests MUST call `stop` on every `HubHandle` they obtain; the
// schema, connections, tempdir, and spawned process are all owned by
// the handle. Each call to `startHub` provisions its own schema, so
// concurrent tests cannot collide on table state.

import { spawn, spawnSync } from "node:child_process";
import { mkdtemp, rm, writeFile, chmod } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";

import postgres from "postgres";

import { runMigrations, dropSchema, type DBConfig } from "@intx/db";
import {
  REPO_ROOT,
  loadEnvFile,
  optionalKey,
  requireKey,
} from "@intx/test-harness/env";
import {
  loadHarnessDbConfig,
  randomSchemaName,
} from "@intx/test-harness/db-harness";
import { grantHubSchemaAccess } from "@intx/test-harness/grants";
import { waitForHTTP } from "@intx/test-harness/http";

/**
 * Hub-side env the harness writes into the spawned process; a missing
 * var fails loudly here rather than deep inside the hub bootstrap.
 */
type HarnessEnv = {
  db: DBConfig;
  betterAuthSecret: string;
  credentialEncryptionKey: string;
  principalKeyEncryptionKey: string;
};

/**
 * True when the repo has every `.env` file a spawned hub needs
 * (`.env`, `.env.migrate`, `.env.hub`); suites gate with
 * `describe.skipIf(...)` so a checkout without hub env still runs
 * `make all`. A file that exists but lacks a key still errors loudly
 * in `loadHarnessDbConfig`/`loadHubEnv`.
 */
export { harnessHubEnvAvailable } from "@intx/test-harness/db-harness";

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
    credentialEncryptionKey: requireKey(
      sharedAndHub,
      "CREDENTIAL_ENCRYPTION_KEY",
      ".env.hub",
    ),
    principalKeyEncryptionKey: requireKey(
      sharedAndHub,
      "PRINCIPAL_KEY_ENCRYPTION_KEY",
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
 * Env block for git runs: every config-related variable is redirected
 * at a discardable tempdir or set to a guard sentinel, so a
 * developer's config cannot leak into a test.
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
  // PATH must reach git even though we spawn the absolute binary path;
  // subcommands (credential helpers, hooks) shell out for their own deps.
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
      child.stdout.on("data", (c: Uint8Array) => {
        stdout += new TextDecoder().decode(c);
      });
      child.stderr.on("data", (c: Uint8Array) => {
        stderr += new TextDecoder().decode(c);
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
 * Materialize an executable GIT_ASKPASS shim that echoes `token` for
 * any prompt git issues, and return an env block pointing GIT_ASKPASS
 * at it with terminal prompts disabled. The shim's tempdir is leaked
 * deliberately: git can read from it asynchronously after the test
 * scope returns.
 */
export async function tokenAskpassEnv(
  token: string,
): Promise<Record<string, string>> {
  const dir = await mkdtemp(path.join(os.tmpdir(), "harness-askpass-"));
  const shim = path.join(dir, "askpass.sh");
  // Echo the token for any prompt ("Username" or "Password"): bearer
  // auth requires any username and the token as the password, and the
  // harness pairs this with a URL whose userinfo carries the username.
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
 * Configure a repo to verify SSH-signed commits against an allowed-
 * signer entry built from `pubKey` (the raw `ssh-ed25519 AAAA...`
 * line) plus the signer identity. Required for `git log
 * --show-signature`.
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
// Random port allocation
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

  // The hub-app role needs explicit grants on this schema; the
  // default-privileges grants apply only to the public schema.
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
      await grantHubSchemaAccess(sql, schema, hubEnv.db.user);
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
    CREDENTIAL_ENCRYPTION_KEY: hubEnv.credentialEncryptionKey,
    PRINCIPAL_KEY_ENCRYPTION_KEY: hubEnv.principalKeyEncryptionKey,
    BETTER_AUTH_BASE_URL: `http://127.0.0.1:${port}`,
  };
  // --conditions=intx-src resolves @intx/* to source; the spawned hub
  // runs from the workspace, where the dev loop builds no dist.
  const child = spawn("bun", ["run", "--conditions=intx-src", hubSrc], {
    cwd: REPO_ROOT,
    env: childEnv,
    stdio: ["ignore", "pipe", "pipe"],
  });

  // Bucket stdout/stderr for diagnostics on failure.
  const logs: string[] = [];
  child.stdout.on("data", (c: Uint8Array) => {
    logs.push(new TextDecoder().decode(c));
  });
  child.stderr.on("data", (c: Uint8Array) => {
    logs.push(new TextDecoder().decode(c));
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
    await waitForHTTP(url, 30_000, 100);
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
