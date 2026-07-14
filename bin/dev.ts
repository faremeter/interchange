#!/usr/bin/env bun
/* eslint-disable no-console */

// Local development orchestrator.
//
// Starts the database migration, hub server, sidecar, and admin UI dev
// server in the correct order, with colored log prefixes and graceful
// shutdown.
//
// This module is Node-bound: it spawns child processes via zx (which
// wraps `node:child_process`), so it cannot run under a non-Node
// runtime regardless of whether it references Buffer.
//
// Usage:
//   bun bin/dev.ts                       # start hub + sidecar + admin-ui
//   bun bin/dev.ts --seed                # also seed the database after hub is ready
//   bun bin/dev.ts --no-admin-ui         # skip the admin UI dev server
//   bun bin/dev.ts --no-sidecar          # skip the sidecar
//   bun bin/dev.ts --no-publish-builtins # skip the built-in tool-package publish

import { $, type ProcessPromise, type ProcessOutput } from "zx";
import { readFileSync, existsSync } from "node:fs";
import { resolve } from "node:path";

import { publishToolPackages } from "./publish-tool-packages";
import { WORKSPACE_BUILTINS_REGISTRY } from "@intx/hub-sessions";

$.verbose = false;

const ROOT = resolve(import.meta.dirname, "..");

function hasStderr(err: unknown): err is { stderr: string } {
  return (
    typeof err === "object" &&
    err !== null &&
    "stderr" in err &&
    typeof (err as { stderr: unknown }).stderr === "string"
  );
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

const args = new Set(process.argv.slice(2));
const wantSeed = args.delete("--seed");
const skipAdminUI = args.delete("--no-admin-ui");
const skipSidecar = args.delete("--no-sidecar");
const skipPublishBuiltins = args.delete("--no-publish-builtins");

if (args.size > 0) {
  console.error(`Unknown flags: ${[...args].join(", ")}`);
  process.exit(1);
}

// The seed pins three built-in tool packages (`@intx/tools-*`); their
// tarballs live in the workspace-builtins package-registry asset and
// are published by the build/publish step above. Seeding without
// publishing produces agent definitions whose launch immediately
// fails with `tarball.missing` because the registry asset is empty.
// Fail loudly here rather than after a successful seed run.
if (skipPublishBuiltins && wantSeed) {
  console.error(
    "--no-publish-builtins is incompatible with --seed: the seed creates agent definitions that pin the workspace built-ins; without a publish the launch would fail with tarball.missing. Either drop --no-publish-builtins or drop --seed.",
  );
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Env file loading
// ---------------------------------------------------------------------------

function loadEnvFile(path: string): Record<string, string> {
  if (!existsSync(path)) return {};
  const vars: Record<string, string> = {};
  for (const line of readFileSync(path, "utf-8").split("\n")) {
    const trimmed = line.trim();
    if (trimmed === "" || trimmed.startsWith("#")) continue;
    const stripped = trimmed.startsWith("export ") ? trimmed.slice(7) : trimmed;
    const eqIdx = stripped.indexOf("=");
    if (eqIdx < 0) continue;
    const key = stripped.slice(0, eqIdx).trim();
    let value = stripped.slice(eqIdx + 1).trim();
    // Strip surrounding quotes
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    vars[key] = value;
  }
  return vars;
}

function requireEnvFiles(...names: string[]): Record<string, string> {
  const merged: Record<string, string> = {};
  for (const name of names) {
    const full = resolve(ROOT, name);
    if (!existsSync(full)) {
      console.error(
        `Missing ${name} — copy ${name}.example to ${name} and fill in values`,
      );
      process.exit(1);
    }
    Object.assign(merged, loadEnvFile(full));
  }
  return merged;
}

// ---------------------------------------------------------------------------
// Process management
// ---------------------------------------------------------------------------

const children: ProcessPromise[] = [];

function spawnLabeled(
  label: string,
  color: string,
  cmd: string[],
  env: Record<string, string>,
  cwd: string = ROOT,
): ProcessPromise {
  const proc = $({
    cwd,
    env: { ...process.env, ...env },
    // Prevent zx from throwing on non-zero exit during teardown.
    nothrow: true,
  })`${cmd}`;

  const prefix = `${color}[${label}]\x1b[0m`;

  proc.stdout.on("data", (chunk: Uint8Array) => {
    for (const line of new TextDecoder().decode(chunk).split("\n")) {
      if (line) process.stdout.write(`${prefix} ${line}\n`);
    }
  });

  proc.stderr.on("data", (chunk: Uint8Array) => {
    for (const line of new TextDecoder().decode(chunk).split("\n")) {
      if (line) process.stderr.write(`${prefix} ${line}\n`);
    }
  });

  children.push(proc);
  return proc;
}

let shuttingDown = false;
// The highest-severity exit code requested so far. Repeated shutdown
// calls (SIGINT during teardown, child-process failure landing while
// we are already winding down) monotonically escalate this value
// rather than overwriting it — a child-process failure that arrived
// after a Ctrl-C must not be silently downgraded to exit-0.
let pendingExitCode = 0;

async function shutdown(code: number): Promise<never> {
  if (code > pendingExitCode) pendingExitCode = code;
  if (shuttingDown) process.exit(pendingExitCode);
  shuttingDown = true;
  console.log("\nShutting down...");

  for (const child of children) {
    try {
      void child.kill("SIGTERM");
    } catch {
      // Already exited.
    }
  }

  // Give processes a moment to clean up, then force kill.
  await new Promise((r) => setTimeout(r, 2000));

  for (const child of children) {
    try {
      void child.kill("SIGKILL");
    } catch {
      // Already exited.
    }
  }

  process.exit(pendingExitCode);
}

process.on("SIGINT", () => void shutdown(0));
process.on("SIGTERM", () => void shutdown(0));

// Tear down everything if any long-running process exits unexpectedly.
function watchProcess(label: string, proc: ProcessPromise): void {
  proc
    .then((result: ProcessOutput) => {
      if (!shuttingDown && result.exitCode !== 0 && result.exitCode !== null) {
        console.error(
          `\x1b[31m${label} exited with code ${result.exitCode}\x1b[0m`,
        );
        void shutdown(1);
      }
    })
    .catch((err: unknown) => {
      if (!shuttingDown) {
        console.error(
          `\x1b[31m${label} failed: ${err instanceof Error ? err.message : String(err)}\x1b[0m`,
        );
        void shutdown(1);
      }
    });
}

// ---------------------------------------------------------------------------
// Health check
// ---------------------------------------------------------------------------

async function waitForHub(url: string, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url);
      if (res.status < 500) return;
    } catch {
      // Not up yet.
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  throw new Error(`Hub did not become ready within ${timeoutMs / 1000}s`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

console.log("Loading environment...");

const sharedEnv = loadEnvFile(resolve(ROOT, ".env"));
const hubEnv = requireEnvFiles(".env", ".env.hub");
const migrateEnv = requireEnvFiles(".env", ".env.migrate");

const hubPort = hubEnv["PORT"] ?? "3000";
const hubURL = `http://localhost:${hubPort}`;
const hubWsURL = `ws://localhost:${hubPort}/api/sidecars/ws`;

// Sidecar env — use .env.sidecar if present, otherwise provide dev defaults.
// The resolved SIDECAR_ID/SIDECAR_TOKEN below are what the sidecar presents
// on its handshake; the provisioning step hashes this same SIDECAR_TOKEN into
// the sidecar row so the hub's token-authenticated handshake accepts it.
const sidecarFileEnv = loadEnvFile(resolve(ROOT, ".env.sidecar"));
const sidecarEnv: Record<string, string> = {
  ...sharedEnv,
  HUB_WS_URL: hubWsURL,
  SIDECAR_ID: "dev-sidecar-1",
  SIDECAR_TOKEN: "dev-token",
  SIDECAR_DATA_DIR: resolve(ROOT, "tmp/sidecar-data"),
  ...sidecarFileEnv,
};

// -- Step 1: Migrate --

console.log("Running database migrations...");

try {
  // Run drizzle-kit migrate directly — skip the generate step in
  // bin/db-migrate which conflicts with hand-written migrations.
  const migrate = await $({
    cwd: resolve(ROOT, "packages/db"),
    env: { ...process.env, ...migrateEnv },
  })`bunx drizzle-kit migrate`;

  for (const line of migrate.stdout.split("\n")) {
    if (line) console.log(`\x1b[90m[migrate]\x1b[0m ${line}`);
  }
  for (const line of migrate.stderr.split("\n")) {
    if (line) console.error(`\x1b[90m[migrate]\x1b[0m ${line}`);
  }
} catch (err) {
  console.error("Database migration failed:");
  if (hasStderr(err)) {
    console.error(err.stderr);
  }
  process.exit(1);
}

console.log("Migrations complete.");

// -- Step 1b: Provision the sidecar identity --
//
// The hub authenticates the sidecar handshake against a per-sidecar token
// hash on the sidecar table, so the sidecar cannot connect until its row
// exists. Provision it before the sidecar spawns, hashing the same
// SIDECAR_TOKEN the sidecar will present. Skipped when no sidecar runs.

if (!skipSidecar) {
  console.log("Provisioning sidecar identity...");

  try {
    const provision = await $({
      env: {
        ...process.env,
        ...migrateEnv,
        SIDECAR_ID: sidecarEnv["SIDECAR_ID"],
        SIDECAR_TOKEN: sidecarEnv["SIDECAR_TOKEN"],
      },
    })`bun run ${resolve(ROOT, "bin/provision-sidecar.ts")}`;

    for (const line of provision.stdout.split("\n")) {
      if (line) console.log(`\x1b[90m[provision]\x1b[0m ${line}`);
    }
    for (const line of provision.stderr.split("\n")) {
      if (line) console.error(`\x1b[90m[provision]\x1b[0m ${line}`);
    }
  } catch (err) {
    console.error("Sidecar provisioning failed:");
    if (hasStderr(err)) {
      console.error(err.stderr);
    }
    process.exit(1);
  }

  console.log("Sidecar provisioning complete.");
}

// -- Step 2: Start hub --

console.log(`Starting hub on port ${hubPort}...`);

const hubProc = spawnLabeled(
  "hub",
  "\x1b[32m", // green
  ["bun", "run", "--watch", "apps/hub/src/index.ts"],
  hubEnv,
);
watchProcess("hub", hubProc);

try {
  await waitForHub(`${hubURL}/api/auth/get-session`, 30_000);
} catch (err) {
  console.error(err instanceof Error ? err.message : String(err));
  await shutdown(1);
}

console.log("Hub is ready.");

// -- Step 3a: Publish built-in tool packages --
//
// Built-ins ride the same asset-substrate path as any operator-published
// package-registry asset. Build the tarballs deterministically, then
// publish them into the workspace-builtins registry on the dev tenant.
// The publish step authenticates as the seeded admin and is idempotent:
// it sign-ups-or-signs-in, ensures the tenant exists, ensures the asset
// row exists, and PUTs every tarball under `dist/builtins/`.
//
// Runs before `--seed` so that when the seed wires up agent pins those
// pins reference tarballs that are already in the registry. The seed
// step itself remains the canonical place for agent/role/credential
// fixtures.

if (!skipPublishBuiltins) {
  console.log("Building built-in tool-package tarballs...");
  try {
    const build = await $({
      cwd: ROOT,
      env: { ...process.env, ...sharedEnv },
    })`bun run bin/build-builtins.ts`;
    for (const line of build.stdout.split("\n")) {
      if (line) console.log(`\x1b[90m[builtins]\x1b[0m ${line}`);
    }
    for (const line of build.stderr.split("\n")) {
      if (line) console.error(`\x1b[90m[builtins]\x1b[0m ${line}`);
    }
  } catch (err) {
    console.error("Built-in tarball build failed:");
    if (hasStderr(err)) {
      console.error(err.stderr);
    } else {
      console.error(err instanceof Error ? err.message : String(err));
    }
    await shutdown(1);
  }

  console.log("Publishing built-in tool packages to the hub...");
  // Dev orchestration owns the publish parameters: it knows the seed's
  // admin identity and tenant slug, and the publish entry point itself
  // is a boundary that takes a fully-populated PublishOptions. Defaults
  // are resolved here, at the edge, rather than inside the publish
  // function. Env overrides let an operator point dev.ts at a non-seed
  // hub without editing the script.
  const adminEmailEnv = process.env["HUB_ADMIN_EMAIL"];
  const adminPasswordEnv = process.env["HUB_ADMIN_PASSWORD"];
  const tenantSlugEnv = process.env["HUB_TENANT_SLUG"];
  const tenantNameEnv = process.env["HUB_TENANT_NAME"];
  try {
    await publishToolPackages({
      hubURL,
      adminEmail:
        adminEmailEnv === undefined || adminEmailEnv === ""
          ? "alice@example.com"
          : adminEmailEnv,
      adminPassword:
        adminPasswordEnv === undefined || adminPasswordEnv === ""
          ? "password123"
          : adminPasswordEnv,
      tenantSlug:
        tenantSlugEnv === undefined || tenantSlugEnv === ""
          ? "acme"
          : tenantSlugEnv,
      tenantName:
        tenantNameEnv === undefined || tenantNameEnv === ""
          ? "Acme Corp"
          : tenantNameEnv,
      registryName: WORKSPACE_BUILTINS_REGISTRY,
      fromDir: resolve(ROOT, "dist/builtins"),
    });
  } catch (err) {
    console.error("Publishing built-in tool packages failed:");
    console.error(err instanceof Error ? err.message : String(err));
    await shutdown(1);
  }
  console.log("Built-in tool packages published.");
}

// -- Step 3: Seed (optional) --

if (wantSeed) {
  console.log("Seeding database...");
  try {
    const seed = await $({
      cwd: ROOT,
      env: { ...process.env, ...hubEnv, HUB_URL: hubURL },
    })`bun bin/seed.ts`;

    for (const line of seed.stdout.split("\n")) {
      if (line) console.log(`\x1b[90m[seed]\x1b[0m ${line}`);
    }
    for (const line of seed.stderr.split("\n")) {
      if (line) console.error(`\x1b[90m[seed]\x1b[0m ${line}`);
    }
  } catch (err) {
    console.error("Seeding failed:");
    if (hasStderr(err)) {
      console.error(err.stderr);
    }
    await shutdown(1);
  }
  console.log("Seeding complete.");
}

// -- Step 4: Start sidecar --

if (!skipSidecar) {
  console.log("Starting sidecar...");

  const sidecarProc = spawnLabeled(
    "sidecar",
    "\x1b[33m", // yellow
    ["bun", "run", "apps/sidecar/src/index.ts"],
    sidecarEnv,
  );
  watchProcess("sidecar", sidecarProc);
}

// -- Step 5: Start Admin UI --

if (!skipAdminUI) {
  console.log("Starting admin UI dev server on port 5173...");

  const adminUIProc = spawnLabeled(
    "admin-ui",
    "\x1b[36m", // cyan
    ["bunx", "vite", "--port", "5173"],
    sharedEnv,
    resolve(ROOT, "apps/admin-ui"),
  );
  watchProcess("admin-ui", adminUIProc);
}

console.log("\n\x1b[1mDev environment is running.\x1b[0m");
console.log(`  Hub:      ${hubURL}`);
if (!skipSidecar) console.log(`  Sidecar:  connecting to ${hubWsURL}`);
if (!skipAdminUI) console.log(`  Admin UI: http://localhost:5173`);
console.log("\nPress Ctrl+C to stop all services.\n");

// Keep the process alive until a signal arrives.
// eslint-disable-next-line @typescript-eslint/no-empty-function
await new Promise((_resolve) => {});
