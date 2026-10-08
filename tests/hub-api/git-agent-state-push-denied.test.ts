// `git push -v` against the per-run agent-state URL returns 403 at the
// advertise step. The receive-pack denial routes run before the bearer
// middleware, so no token is needed: an unauthenticated push still parses
// the pkt-line ERR record cleanly. The raw advertise body (the locked
// `ERR agent-state is read-only over HTTP\n` payload) is verified via a
// direct fetch alongside the push.

import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import {
  harnessHubEnvAvailable,
  runGit,
  startHub,
  type HubHandle,
} from "./lib/git-harness";
import { createTenant, signUpUser } from "./lib/git-asset-fixtures";

const stops: (() => Promise<void>)[] = [];
const tempDirs: string[] = [];

afterEach(async () => {
  for (const stop of stops.splice(0)) {
    await stop();
  }
  await Promise.all(
    tempDirs.splice(0).map((d) => fs.rm(d, { recursive: true, force: true })),
  );
});

async function mkTemp(prefix: string): Promise<string> {
  const d = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

async function startHubTracked(): Promise<HubHandle> {
  const hub = await startHub();
  stops.push(hub.stop);
  return hub;
}

/**
 * Prepare a local non-bare repo with a single commit on
 * `refs/heads/deploy` so `git push` has a ref + content to send; the
 * deny middleware rejects advertise before any pack is uploaded.
 */
async function prepareLocalPushSource(workDir: string): Promise<void> {
  await runGit(["init", "--initial-branch=deploy", workDir], { cwd: workDir });
  await runGit(["config", "user.name", "push-tester"], { cwd: workDir });
  await runGit(["config", "user.email", "push@example.invalid"], {
    cwd: workDir,
  });
  await runGit(["config", "commit.gpgsign", "false"], { cwd: workDir });
  await fs.mkdir(path.join(workDir, "deploy"), { recursive: true });
  await fs.writeFile(
    path.join(workDir, "deploy", "prompt.md"),
    "# local\n",
    "utf-8",
  );
  await runGit(["add", "deploy/prompt.md"], { cwd: workDir });
  const r = await runGit(["commit", "-m", "Local commit for push test"], {
    cwd: workDir,
  });
  if (r.status !== 0) {
    throw new Error(`local commit failed: ${r.stderr}`);
  }
}

function runStateGitUrl(
  hubUrl: string,
  tenantId: string,
  runId: string,
): string {
  return `${hubUrl}/api/tenants/${tenantId}/workflows/runs/${runId}/state.git`;
}

describe.skipIf(!harnessHubEnvAvailable())("agent-state push denied", () => {
  test("advertise deny body names read-only", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);
    const tenant = await createTenant(hub.url, user);

    // The receive-pack advertise deny runs ahead of the bearer middleware
    // and the resolver, so an unauthenticated probe on a bogus run id
    // still yields the locked 403 body verbatim.
    const res = await fetch(
      `${runStateGitUrl(hub.url, tenant.tenantId, "run_doesnotexist")}/info/refs?service=git-receive-pack`,
    );
    expect(res.status).toBe(403);
    const body = await res.text();
    expect(body).toContain("agent-state is read-only over HTTP");
  }, 90_000);

  test("git push -v against per-run URL returns 403 with read-only message", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);
    const tenant = await createTenant(hub.url, user);

    // The receive-pack deny runs before the bearer middleware and the
    // resolver, so even unauthenticated pushes against a bogus run id get
    // a clean protocol-level rejection before any DB lookup.
    const workDir = await mkTemp("agent-state-push-ins-");
    await prepareLocalPushSource(workDir);

    const remote = runStateGitUrl(hub.url, tenant.tenantId, "run_fakefakefake");
    const push = await runGit(
      [
        "-c",
        "credential.helper=",
        "push",
        "-v",
        remote,
        "refs/heads/deploy:refs/heads/deploy",
      ],
      { cwd: workDir },
    );
    expect(push.status).not.toBe(0);
    const combined = `${push.stdout}\n${push.stderr}`.toLowerCase();
    // Stock git surfaces an HTTP 403 advertise denial as `the requested
    // url returned error: 403`; the `read-only` substring is best-effort
    // because git strips most of the body. The advertise body is verified
    // by the direct-fetch test above.
    expect(combined).toMatch(/error: 403|http 403|forbidden/);
  }, 90_000);
});
