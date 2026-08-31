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

// The seed pushes this workflow-source asset and authenticates as this user;
// the live deploy spec logs in as the same user and deploys the same asset.
const WORKFLOW_ASSET_NAME = "approval-flow";
const WORKFLOW_ENTRY = "./workflow.mjs";
// The seeded, tenant-owned Acme credential the live deploy references. A
// source-ref deploy resolves the source credential by id, so it must name a
// real credential rather than an inline key.
const SEED_CREDENTIAL_NAME = "Anthropic API Key";
const SEED_LOGIN_EMAIL = "alice@example.com";
const SEED_LOGIN_PASSWORD = "password123";
const SEED_TENANT_SLUG = "acme";

const SEED = path.join(REPO_ROOT, "bin", "seed.ts");

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
  onLine?: (line: string) => void,
): ProcessPromise {
  const proc = $({ cwd, env, nothrow: true })`${cmd}`;
  const prefix = `[${label}]`;

  const forward =
    (write: (chunk: string) => void) =>
    (line: string): void => {
      write(`${prefix} ${line}\n`);
      onLine?.(line);
    };
  forwardLines(
    proc.stdout,
    forward((chunk) => process.stdout.write(chunk)),
  );
  forwardLines(
    proc.stderr,
    forward((chunk) => process.stderr.write(chunk)),
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
  // The migration role's connection to the provisioned database. The seed
  // writes through this table-owning identity; unlike the hub role it always
  // carries a concrete password.
  dbEnv: {
    DB_HOST: "string",
    DB_PORT: "string",
    DB_USER: "string",
    DB_PASSWORD: "string",
    DB_NAME: "string",
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

// The hub HTTP responses consulted below are external data to this process,
// so validate their shape at this boundary. Each validator is intentionally
// narrow: it names only the fields the discovery reads, and arktype ignores
// the rest of the payload.
const PrincipalsResponse = type({
  data: type({ tenantId: "string", tenantSlug: "string" }).array(),
});
const AssetsResponse = type({ id: "string", name: "string" }).array();
const GitTokenResponse = type({ id: "string", secret: "string" });
const CredentialsResponse = type({
  data: type({ id: "string", name: "string" }).array(),
});

type CookieJar = string[];

/**
 * A minimal cookie-jar `fetch` for the discovery below. It mirrors the seed's
 * own `api` helper: it carries `Set-Cookie` values forward across calls so a
 * session established by sign-in authenticates the later reads.
 */
async function hubFetch(
  hubURL: string,
  method: string,
  routePath: string,
  body: unknown,
  cookies: CookieJar,
): Promise<{ status: number; data: unknown; cookies: CookieJar }> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    // better-auth rejects a state-changing POST whose Origin is not among its
    // trusted origins (which default to its base URL). The browser reaches the
    // hub through the preview proxy, which rewrites Origin to the base URL;
    // this direct server-side call sets the same trusted Origin itself.
    Origin: hubURL,
  };
  if (cookies.length > 0) headers["Cookie"] = cookies.join("; ");

  const res = await fetch(`${hubURL}${routePath}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
    redirect: "manual",
  });

  const nextCookies = [...cookies];
  for (const setCookie of res.headers.getSetCookie()) {
    const name = setCookie.split("=")[0];
    const value = setCookie.split(";")[0];
    if (name === undefined || value === undefined) continue;
    const idx = nextCookies.findIndex((c) => c.startsWith(`${name}=`));
    if (idx >= 0) nextCookies[idx] = value;
    else nextCookies.push(value);
  }

  let data: unknown = null;
  if ((res.headers.get("content-type") ?? "").includes("json")) {
    data = await res.json();
  }
  return { status: res.status, data, cookies: nextCookies };
}

/**
 * Resolve the current `refs/heads/main` commit of the seeded workflow-source
 * asset over the asset smart-HTTP endpoint. There is no JSON surface for an
 * asset's head commit, so this authenticates a `git ls-remote` with a
 * short-lived read git-token embedded as the basic-auth password, matching the
 * seed's own push convention.
 *
 * @throws if `git ls-remote` reports no `refs/heads/main` commit.
 */
async function resolveWorkflowHeadCommit(
  hubURL: string,
  tenantId: string,
  tokenSecret: string,
): Promise<string> {
  const remote = new URL(
    `${hubURL}/api/tenants/${tenantId}/assets/workflow/${WORKFLOW_ASSET_NAME}.git`,
  );
  remote.username = "x-access-token";
  remote.password = tokenSecret;

  const work = fs.mkdtempSync(path.join(os.tmpdir(), "intx-e2e-lsremote-"));
  const askpass = path.join(work, "askpass.sh");
  try {
    fs.writeFileSync(
      askpass,
      `#!/bin/sh\nprintf '%s\\n' '${tokenSecret.replace(/'/g, "'\\''")}'\n`,
      "utf-8",
    );
    fs.chmodSync(askpass, 0o755);
    const out = await $({
      cwd: work,
      env: {
        ...process.env,
        GIT_ASKPASS: askpass,
        GIT_TERMINAL_PROMPT: "0",
      },
    })`git -c credential.helper= ls-remote ${remote.toString()} refs/heads/main`;
    const firstLine = out.stdout.trim().split("\n")[0] ?? "";
    const commitSha = firstLine.split(/\s+/)[0] ?? "";
    if (!/^[0-9a-f]{40}$/.test(commitSha)) {
      throw new Error(
        `git ls-remote returned no refs/heads/main commit for ${WORKFLOW_ASSET_NAME}: ${JSON.stringify(out.stdout)}`,
      );
    }
    return commitSha;
  } finally {
    fs.rmSync(work, { recursive: true, force: true });
  }
}

/**
 * After the seed runs, discover the concrete inputs the live deploy spec needs:
 * the Acme tenant id, the seeded workflow asset's id, and that asset's head
 * commit. The ids are hub-minted (not deterministic), so they are read back
 * through the same authenticated API an operator would use.
 */
async function discoverSeededWorkflow(hubURL: string): Promise<{
  tenantId: string;
  assetId: string;
  commitSha: string;
  credentialId: string;
}> {
  const signIn = await hubFetch(
    hubURL,
    "POST",
    "/api/auth/sign-in/email",
    { email: SEED_LOGIN_EMAIL, password: SEED_LOGIN_PASSWORD },
    [],
  );
  if (signIn.status !== 200) {
    throw new Error(
      `e2e discovery: sign-in as ${SEED_LOGIN_EMAIL} failed with ${signIn.status}`,
    );
  }
  const cookies = signIn.cookies;

  const principalsRes = await hubFetch(
    hubURL,
    "GET",
    "/api/me/principals",
    undefined,
    cookies,
  );
  const principals = PrincipalsResponse(principalsRes.data);
  if (principals instanceof type.errors) {
    throw new Error(`e2e discovery: invalid principals: ${principals.summary}`);
  }
  const acme = principals.data.find((p) => p.tenantSlug === SEED_TENANT_SLUG);
  if (acme === undefined) {
    throw new Error(
      `e2e discovery: no ${SEED_TENANT_SLUG} tenant among the seeded principals`,
    );
  }
  const tenantId = acme.tenantId;

  const assetsRes = await hubFetch(
    hubURL,
    "GET",
    `/api/tenants/${tenantId}/assets?kind=workflow`,
    undefined,
    cookies,
  );
  const assets = AssetsResponse(assetsRes.data);
  if (assets instanceof type.errors) {
    throw new Error(`e2e discovery: invalid assets: ${assets.summary}`);
  }
  const workflowAsset = assets.find((a) => a.name === WORKFLOW_ASSET_NAME);
  if (workflowAsset === undefined) {
    throw new Error(
      `e2e discovery: seeded workflow asset ${WORKFLOW_ASSET_NAME} not found`,
    );
  }

  // The source-ref deploy resolves the source credential by id, so discover the
  // seeded tenant-owned credential. `owner=org` lists only tenant-owned
  // (principalId IS NULL) credentials, which is what the deploy can resolve.
  const credsRes = await hubFetch(
    hubURL,
    "GET",
    `/api/tenants/${tenantId}/credentials?owner=org`,
    undefined,
    cookies,
  );
  const creds = CredentialsResponse(credsRes.data);
  if (creds instanceof type.errors) {
    throw new Error(`e2e discovery: invalid credentials: ${creds.summary}`);
  }
  const credential = creds.data.find((c) => c.name === SEED_CREDENTIAL_NAME);
  if (credential === undefined) {
    throw new Error(
      `e2e discovery: seeded credential ${SEED_CREDENTIAL_NAME} not found`,
    );
  }

  const tokenExpiresAt = new Date(Date.now() + 10 * 60 * 1000).toISOString();
  const tokenRes = await hubFetch(
    hubURL,
    "POST",
    `/api/tenants/${tenantId}/git-tokens`,
    {
      name: "e2e-live-discovery",
      resource: "asset:*",
      refPattern: "**",
      actions: ["can_read"],
      expiresAt: tokenExpiresAt,
    },
    cookies,
  );
  if (tokenRes.status !== 201) {
    throw new Error(
      `e2e discovery: minting a read git-token failed with ${tokenRes.status}`,
    );
  }
  const token = GitTokenResponse(tokenRes.data);
  if (token instanceof type.errors) {
    throw new Error(`e2e discovery: invalid git-token: ${token.summary}`);
  }

  const commitSha = await resolveWorkflowHeadCommit(
    hubURL,
    tenantId,
    token.secret,
  );
  return {
    tenantId,
    assetId: workflowAsset.id,
    commitSha,
    credentialId: credential.id,
  };
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
    const { database, hubEnv, dbEnv } = parseProvisionResult(
      provisionOutput.stdout,
    );
    acquired.database = database;

    // The DB connection the seed writes through. It uses the table-owning
    // migration role, whose password
    // is concrete — the hub role may authenticate under trust auth with an
    // empty password that those bin scripts reject. PG_SCHEMA is stripped for
    // the same reason the hub env strips it: migrations land in `public`.
    const dbSpawnEnv: NodeJS.ProcessEnv = { ...process.env, ...dbEnv };
    delete dbSpawnEnv["PG_SCHEMA"];

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
      [
        "bun",
        "run",
        "--conditions=intx-src",
        "tests/admin-ui-e2e/harness/hub.ts",
      ],
      hubSpawnEnv,
      REPO_ROOT,
    );
    acquired.hubProc = hubProc;

    // Poll the hub directly, not through the preview proxy: the proxy is
    // not up yet, and a direct probe isolates a hub-start failure from a
    // proxy misconfiguration.
    const hubURL = betterAuthBaseURL;
    await waitForHTTP(`${hubURL}/api/auth/get-session`, HUB_READY_TIMEOUT_MS);

    // Seed the running hub: users, tenants, model catalog, and a deployable
    // workflow-source asset pushed over the asset smart-HTTP route. The seed
    // targets the hub via HUB_URL and projects its workflow definition through
    // the same table-owning DB connection the provision step used. A non-zero
    // exit throws.
    await $({
      cwd: REPO_ROOT,
      env: { ...dbSpawnEnv, HUB_URL: hubURL },
    })`bun --conditions=intx-src ${SEED}`;

    // Read back the hub-minted tenant id, workflow asset id, and the asset's
    // head commit, then publish them (alongside the seeded login) so the live
    // deploy spec drives the picker against real, deployable inputs.
    const seeded = await discoverSeededWorkflow(hubURL);
    process.env["E2E_WORKFLOW_TENANT_ID"] = seeded.tenantId;
    process.env["E2E_WORKFLOW_ASSET_ID"] = seeded.assetId;
    process.env["E2E_WORKFLOW_COMMIT_SHA"] = seeded.commitSha;
    process.env["E2E_WORKFLOW_CREDENTIAL_ID"] = seeded.credentialId;
    process.env["E2E_WORKFLOW_ENTRY"] = WORKFLOW_ENTRY;
    process.env["E2E_LOGIN_EMAIL"] = SEED_LOGIN_EMAIL;
    process.env["E2E_LOGIN_PASSWORD"] = SEED_LOGIN_PASSWORD;

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
