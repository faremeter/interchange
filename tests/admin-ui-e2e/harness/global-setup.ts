// Hermetic stack bring-up for the admin-UI browser end-to-end suite.
//
// Mirrors the zx orchestration model of `bin/dev.ts` — labeled
// background processes, fetch-poll health checks, SIGTERM-then-SIGKILL
// teardown — but scoped to a single Playwright run. It provisions a
// fresh database (via `bin/e2e-provision.ts`, the sole `@intx/*`
// boundary), spawns the hub against it on a per-run port, serves the
// pre-built admin UI through `vite preview` behind the same-origin
// `/api` proxy, and publishes the preview URL to the spec through
// `E2E_BASE_URL`. The returned closure tears the whole stack down and
// drops the database, leaving nothing orphaned.

import net from "node:net";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { type } from "arktype";
import { $, type ProcessPromise } from "zx";

$.verbose = false;

const HERE = path.dirname(fileURLToPath(import.meta.url));
// harness/ -> admin-ui-e2e/ -> tests/ -> repo root.
const REPO_ROOT = path.resolve(HERE, "..", "..", "..");
const E2E_PROVISION = path.join(REPO_ROOT, "bin", "e2e-provision.ts");
const ADMIN_UI_DIR = path.join(REPO_ROOT, "apps", "admin-ui");
const ADMIN_UI_DIST_INDEX = path.join(ADMIN_UI_DIR, "dist", "index.html");

const HOST = "127.0.0.1";
const HUB_READY_TIMEOUT_MS = 30_000;
const PREVIEW_READY_TIMEOUT_MS = 30_000;

/** Allocate a free TCP port by binding `:0` on the loopback interface and
 *  reading the assigned port before releasing it. There is an unavoidable
 *  race between release and the child process claiming the port; the
 *  suite runs a single worker, and `vite preview --strictPort` fails
 *  loudly rather than silently drifting to another port if it is lost. */
function allocatePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, HOST, () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        server.close();
        reject(new Error("failed to allocate a port: unexpected address"));
        return;
      }
      const { port } = address;
      server.close(() => resolve(port));
    });
  });
}

/** Forward a child stream to `write` one complete line at a time. A
 *  single stateful decoder (with `stream: true`) keeps multibyte
 *  characters split across chunk boundaries intact, and the buffer
 *  holds a trailing partial line until its newline arrives; whatever
 *  remains when the stream ends is flushed as a final line. */
function forwardLines(
  stream: NodeJS.ReadableStream,
  write: (line: string) => void,
): void {
  const decoder = new TextDecoder();
  let buffer = "";

  const emitCompleteLines = () => {
    const lastNewline = buffer.lastIndexOf("\n");
    if (lastNewline < 0) return;
    for (const line of buffer.slice(0, lastNewline).split("\n")) {
      if (line) write(line);
    }
    buffer = buffer.slice(lastNewline + 1);
  };

  stream.on("data", (chunk: Uint8Array) => {
    buffer += decoder.decode(chunk, { stream: true });
    emitCompleteLines();
  });
  stream.on("end", () => {
    buffer += decoder.decode();
    emitCompleteLines();
    if (buffer) write(buffer);
  });
}

function spawnLabeled(
  label: string,
  cmd: string[],
  env: NodeJS.ProcessEnv,
  cwd: string,
): ProcessPromise {
  const proc = $({ cwd, env, nothrow: true })`${cmd}`;
  const prefix = `[${label}]`;

  forwardLines(proc.stdout, (line) =>
    process.stdout.write(`${prefix} ${line}\n`),
  );
  forwardLines(proc.stderr, (line) =>
    process.stderr.write(`${prefix} ${line}\n`),
  );

  return proc;
}

async function waitForHTTP(url: string, timeoutMs: number): Promise<void> {
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
  throw new Error(`${url} did not become ready within ${timeoutMs / 1000}s`);
}

// The provision CLI output is external data (subprocess stdout), so
// validate its shape at this boundary rather than trusting it.
// DB_PASSWORD is plain `string` (not non-empty): under trust or peer
// authentication an empty password is legitimate.
const ProvisionResult = type({
  database: "string",
  hubEnv: {
    DB_HOST: "string",
    DB_PORT: "string",
    DB_USER: "string",
    DB_PASSWORD: "string",
    BETTER_AUTH_SECRET: "string",
    CREDENTIAL_ENCRYPTION_KEY: "string",
  },
});

function parseProvisionResult(stdout: string) {
  const raw: unknown = JSON.parse(stdout.trim());
  const result = ProvisionResult(raw);
  if (result instanceof type.errors) {
    throw new Error(
      `e2e-provision up returned invalid JSON: ${result.summary}`,
    );
  }
  return result;
}

// The resources the setup acquires in order. Each is recorded on this
// record the moment it exists, so a partial-failure cleanup can tear
// down exactly what was acquired and nothing more.
type AcquiredStack = {
  database?: string;
  hubDataDir?: string;
  hubProc?: ProcessPromise;
  previewProc?: ProcessPromise;
};

/** Wait for a spawned process to exit, but no longer than `ms`. The
 *  processes are spawned `nothrow`, so their ProcessPromise settles
 *  (never rejects) when the child exits; racing it against a timer
 *  bounds the wait without penalizing a child that exits quickly. */
async function exitedWithin(
  proc: ProcessPromise,
  ms: number,
): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const expired = new Promise<boolean>((resolve) => {
    timer = setTimeout(() => resolve(false), ms);
  });
  try {
    return await Promise.race([
      proc.then(
        () => true,
        () => true,
      ),
      expired,
    ]);
  } finally {
    clearTimeout(timer);
  }
}

const SIGTERM_GRACE_MS = 2000;

/**
 * Tear down whatever of the stack was acquired: SIGTERM then SIGKILL any
 * spawned process still running after the grace period, drop the
 * provisioned database, and remove the temp HUB_DATA_DIR. Process kills
 * are individually swallowed because killing an already-exited process
 * is expected and benign. The database drop and directory removal are
 * each attempted independently — one failing never skips the other —
 * and any failure among them is aggregated into a thrown error so the
 * success-path caller surfaces it. The partial-failure caller wraps
 * this whole call to swallow that error and preserve the original.
 */
async function teardownStack(acquired: AcquiredStack): Promise<void> {
  const { previewProc, hubProc, database, hubDataDir } = acquired;

  const procs: ProcessPromise[] = [];
  for (const proc of [previewProc, hubProc]) {
    if (proc !== undefined) procs.push(proc);
  }

  for (const proc of procs) {
    try {
      await proc.kill("SIGTERM");
    } catch {
      // Already exited.
    }
  }
  const surviving: ProcessPromise[] = [];
  await Promise.all(
    procs.map(async (proc) => {
      if (!(await exitedWithin(proc, SIGTERM_GRACE_MS))) surviving.push(proc);
    }),
  );
  for (const proc of surviving) {
    try {
      await proc.kill("SIGKILL");
    } catch {
      // Already exited.
    }
  }

  const failures: unknown[] = [];
  if (database !== undefined) {
    try {
      await $({
        cwd: REPO_ROOT,
      })`bun --conditions=intx-src ${E2E_PROVISION} down ${database}`;
    } catch (err) {
      failures.push(err);
    }
  }
  if (hubDataDir !== undefined) {
    try {
      fs.rmSync(hubDataDir, { recursive: true, force: true });
    } catch (err) {
      failures.push(err);
    }
  }
  if (failures.length > 0) {
    throw new Error(`stack teardown failed with ${failures.length} error(s)`, {
      cause: failures[0],
    });
  }
}

async function globalSetup(): Promise<() => Promise<void>> {
  if (!fs.existsSync(ADMIN_UI_DIST_INDEX)) {
    throw new Error(
      `admin UI bundle is missing at ${ADMIN_UI_DIST_INDEX}; run \`make build-admin-ui\` first`,
    );
  }

  // Playwright only runs the teardown this function RETURNS, and that
  // return does not happen until the whole stack is up. If any step
  // below throws (most likely a readiness poll timing out) the already-
  // acquired resources would leak, so track them as they come up and
  // tear them down in the catch before rethrowing the original error.
  const acquired: AcquiredStack = {};
  try {
    const [hubPort, previewPort] = await Promise.all([
      allocatePort(),
      allocatePort(),
    ]);

    const provisionOutput = await $({
      cwd: REPO_ROOT,
    })`bun --conditions=intx-src ${E2E_PROVISION} up`;
    const { database, hubEnv } = parseProvisionResult(provisionOutput.stdout);
    acquired.database = database;

    const hubDataDir = fs.mkdtempSync(path.join(os.tmpdir(), "intx-e2e-hub-"));
    acquired.hubDataDir = hubDataDir;
    const betterAuthBaseURL = `http://${HOST}:${hubPort}`;

    const hubSpawnEnv: NodeJS.ProcessEnv = {
      ...process.env,
      ...hubEnv,
      DB_NAME: database,
      PORT: String(hubPort),
      HUB_DATA_DIR: hubDataDir,
      BETTER_AUTH_BASE_URL: betterAuthBaseURL,
    };
    // The hub honors PG_SCHEMA, but the provisioner migrates only the
    // `public` schema of the fresh database; an ambient PG_SCHEMA (the
    // hub-subprocess harness sets one) would pin the hub to a schema
    // that does not exist here.
    delete hubSpawnEnv["PG_SCHEMA"];

    const hubProc = spawnLabeled(
      "hub",
      ["bun", "run", "--conditions=intx-src", "apps/hub/src/index.ts"],
      hubSpawnEnv,
      REPO_ROOT,
    );
    acquired.hubProc = hubProc;

    // Poll the hub directly, not through the preview proxy: the proxy is
    // not up yet, and a direct probe isolates a hub-start failure from a
    // proxy misconfiguration.
    await waitForHTTP(
      `http://${HOST}:${hubPort}/api/auth/get-session`,
      HUB_READY_TIMEOUT_MS,
    );

    // ADMIN_UI_HUB_ORIGIN is what the preview `/api` proxy rewrites the
    // Origin to; it must byte-match BETTER_AUTH_BASE_URL or better-auth
    // rejects the request as a cross-origin POST (403).
    const previewProc = spawnLabeled(
      "preview",
      [
        "bunx",
        "vite",
        "preview",
        "--host",
        HOST,
        "--port",
        String(previewPort),
        "--strictPort",
      ],
      { ...process.env, ADMIN_UI_HUB_ORIGIN: betterAuthBaseURL },
      ADMIN_UI_DIR,
    );
    acquired.previewProc = previewProc;

    await waitForHTTP(
      `http://${HOST}:${previewPort}/`,
      PREVIEW_READY_TIMEOUT_MS,
    );

    process.env["E2E_BASE_URL"] = `http://${HOST}:${previewPort}`;

    return () => teardownStack(acquired);
  } catch (err) {
    try {
      await teardownStack(acquired);
    } catch {
      // A secondary cleanup failure must not mask the original error.
    }
    throw err;
  }
}

export default globalSetup;
