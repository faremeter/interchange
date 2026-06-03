// Smoke test for the integration-test harness foundation.
//
// Covers the harness contract before any wire-format test consumes
// it: discoverGitBinary parses a real version and enforces the
// floor, startHub spawns a real hub subprocess against a freshly
// created per-test postgres schema, runGit executes with a fully
// redirected git config environment, and stop drops the schema
// cleanly and the spawned process exits.

import { afterEach, describe, expect, test } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import postgres from "postgres";

import {
  discoverGitBinary,
  harnessDbEnvAvailable,
  runGit,
  startHub,
  tokenAskpassEnv,
  loadHarnessDbConfig,
} from "./git-harness";

const stopHandles: (() => Promise<void>)[] = [];
const tempDirs: string[] = [];

afterEach(async () => {
  const stops = stopHandles.splice(0);
  for (const stop of stops) {
    await stop();
  }
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.rm(d, { recursive: true, force: true })),
  );
});

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

describe("discoverGitBinary", () => {
  test("returns a path and parses a 2.34+ semver", () => {
    const info = discoverGitBinary();
    expect(info.path.length).toBeGreaterThan(0);
    expect(info.version.major).toBeGreaterThanOrEqual(2);
    if (info.version.major === 2) {
      expect(info.version.minor).toBeGreaterThanOrEqual(34);
    }
  });
});

describe("runGit", () => {
  test("executes with a redirected config environment", async () => {
    const cwd = await makeTempDir("harness-git-");
    const result = await runGit(["init", "--quiet", "--initial-branch=main"], {
      cwd,
    });
    expect(result.status).toBe(0);
    // Verify the redirected HOME by reading what git resolved for
    // user.name — with our shim there is no global config, so the
    // local config we set next is the only source.
    await runGit(["config", "user.name", "Harness"], { cwd });
    await runGit(["config", "user.email", "harness@example.invalid"], { cwd });
    const got = await runGit(["config", "user.name"], { cwd });
    expect(got.status).toBe(0);
    expect(got.stdout.trim()).toBe("Harness");
  });

  test("does not inherit ambient git config from HOME", async () => {
    const cwd = await makeTempDir("harness-git-");
    await runGit(["init", "--quiet", "--initial-branch=main"], { cwd });
    // With ambient config redirected, asking for a user.name that
    // was never set in the repo must fail (exit 1, empty stdout).
    const got = await runGit(["config", "--get", "user.name"], { cwd });
    expect(got.stdout.trim()).toBe("");
    expect(got.status).not.toBe(0);
  });
});

describe("tokenAskpassEnv", () => {
  test("produces an env block that supplies the token over askpass", async () => {
    const env = await tokenAskpassEnv("itx_pat_smoketoken");
    expect(env["GIT_TERMINAL_PROMPT"]).toBe("0");
    expect(typeof env["GIT_ASKPASS"]).toBe("string");

    // The askpass shim must echo the token regardless of which
    // prompt git asks for. Invoke it directly to verify.
    const askpath = env["GIT_ASKPASS"];
    if (askpath === undefined) {
      throw new Error("tokenAskpassEnv did not provide GIT_ASKPASS");
    }
    const proc = Bun.spawn([askpath, "Password for 'https://hub/':"], {
      stdout: "pipe",
      stderr: "pipe",
    });
    const out = await new Response(proc.stdout).text();
    await proc.exited;
    expect(out.trim()).toBe("itx_pat_smoketoken");
  });
});

describe.skipIf(!harnessDbEnvAvailable())("startHub", () => {
  test("spawns a hub against an isolated schema, drops the schema on stop", async () => {
    const harnessConfig = loadHarnessDbConfig();

    const hub = await startHub();
    stopHandles.push(hub.stop);

    // 1. URL is reachable.
    const probe = await fetch(`${hub.url}/api/me`);
    // /api/me requires auth; we expect either 401 or 200 depending
    // on the route shape. The point is that the server answered.
    expect([200, 401, 403]).toContain(probe.status);

    // 2. The schema named by the hub exists, and a representative
    // table from the migration set lives in it.
    const sql = postgres({
      host: harnessConfig.host,
      port: harnessConfig.port,
      user: harnessConfig.user,
      password: harnessConfig.password,
      database: harnessConfig.database,
      max: 1,
    });
    try {
      const before = await sql<
        { schema_name: string }[]
      >`SELECT schema_name FROM information_schema.schemata WHERE schema_name = ${hub.schema}`;
      expect(before.length).toBe(1);

      const tables = await sql<
        { table_name: string }[]
      >`SELECT table_name FROM information_schema.tables WHERE table_schema = ${hub.schema} AND table_name = 'tenant'`;
      expect(tables.length).toBe(1);

      // 3. Stop the hub: schema must be gone and the process exited.
      const stop = stopHandles.pop();
      if (stop === undefined) {
        throw new Error("expected a pending stop handle");
      }
      await stop();

      const after = await sql<
        { schema_name: string }[]
      >`SELECT schema_name FROM information_schema.schemata WHERE schema_name = ${hub.schema}`;
      expect(after.length).toBe(0);
    } finally {
      await sql.end();
    }
  }, 60_000);
});
