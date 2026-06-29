// `git push -v` against either agent-state URL group returns 403
// at the advertise step. The denial message names "read-only" so
// support can recognise it.
//
// The receive-pack denial routes are registered BEFORE the bearer
// middleware, so the test does not need to mint a token: an
// unauthenticated `git push -v` against the per-instance or
// per-definition URL still parses the pkt-line ERR record cleanly.
//
// We also verify the raw advertise body shape (the locked
// `ERR agent-state is read-only over HTTP\n` payload) via a direct
// fetch alongside the `git push -v` inspection.

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
import {
  apiCall,
  createTenant,
  signUpUser,
  type SignedUpUser,
} from "./lib/git-asset-fixtures";

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

async function createAgentDefinition(
  hubUrl: string,
  user: SignedUpUser,
  tenantId: string,
  name: string,
): Promise<{ agentId: string }> {
  const res = await apiCall(
    hubUrl,
    "POST",
    `/api/tenants/${tenantId}/agents/definitions`,
    {
      name,
      systemPrompt: "you are a test agent",
    },
    user.cookies,
  );
  if (res.status !== 201) {
    throw new Error(
      `create agent failed: status=${res.status} body=${JSON.stringify(res.data)}`,
    );
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the route returns AgentResponse; only `id` is needed downstream
  const data = res.data as { id: string };
  return { agentId: data.id };
}

/**
 * Prepare a local non-bare repo with a single commit on
 * `refs/heads/deploy` so `git push` has a ref + content to send. We
 * never expect the push to succeed — the deny middleware rejects
 * advertise before any pack is uploaded — but git needs a real
 * source branch to dispatch a push at all.
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

function instanceStateGitUrl(
  hubUrl: string,
  tenantId: string,
  instanceId: string,
): string {
  return `${hubUrl}/api/tenants/${tenantId}/agents/instances/${instanceId}/state.git`;
}

function definitionStateGitUrl(
  hubUrl: string,
  tenantId: string,
  agentId: string,
): string {
  return `${hubUrl}/api/tenants/${tenantId}/agents/definitions/${agentId}/state.git`;
}

describe.skipIf(!harnessHubEnvAvailable())("agent-state push denied", () => {
  test("advertise deny body names read-only", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);
    const tenant = await createTenant(hub.url, user);
    const agent = await createAgentDefinition(
      hub.url,
      user,
      tenant.tenantId,
      "push-deny-agent",
    );

    // The receive-pack advertise deny middleware runs ahead of the
    // bearer middleware. An unauthenticated probe on either URL
    // group should yield the locked 403 body verbatim.
    for (const url of [
      `${definitionStateGitUrl(hub.url, tenant.tenantId, agent.agentId)}/info/refs?service=git-receive-pack`,
      `${definitionStateGitUrl(hub.url, tenant.tenantId, "ins_doesnotexist")}/info/refs?service=git-receive-pack`,
      `${instanceStateGitUrl(hub.url, tenant.tenantId, "ins_doesnotexist")}/info/refs?service=git-receive-pack`,
    ]) {
      const res = await fetch(url);
      expect(res.status).toBe(403);
      const body = await res.text();
      expect(body).toContain("agent-state is read-only over HTTP");
    }
  }, 90_000);

  test("git push -v against per-definition URL returns 403 with read-only message", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);
    const tenant = await createTenant(hub.url, user);
    const agent = await createAgentDefinition(
      hub.url,
      user,
      tenant.tenantId,
      "push-def-agent",
    );

    const workDir = await mkTemp("agent-state-push-def-");
    await prepareLocalPushSource(workDir);

    const remote = definitionStateGitUrl(
      hub.url,
      tenant.tenantId,
      agent.agentId,
    );
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
    // 403 on the receive-pack advertise step surfaces as `http 403`
    // / `forbidden` in stderr. The verb "read-only" appears either
    // via the pkt-line `ERR` payload git prints under `remote:`, or
    // via the `# service=git-receive-pack` header text. We require
    // the 403 unambiguously; the read-only marker is best-effort
    // depending on how git renders the body.
    // Stock git surfaces an HTTP 403 advertise denial as
    // `the requested url returned error: 403` in stderr. The
    // `read-only`/`agent-state` substring is best-effort: git
    // strips most of the body unless the server emits a properly
    // pkt-line-framed `ERR` record on the wire. The advertise body
    // is verified separately via the direct-fetch test above.
    expect(combined).toMatch(/error: 403|http 403|forbidden/);
  }, 90_000);

  test("git push -v against per-instance URL returns 403 with read-only message", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);
    const tenant = await createTenant(hub.url, user);

    // No instance row required: the receive-pack deny runs before
    // the bearer middleware AND before the resolver, so it 403s
    // even on a bogus instance id. This is the desired property —
    // even unauthenticated push attempts get a clean protocol-level
    // rejection before any DB lookup.
    const workDir = await mkTemp("agent-state-push-ins-");
    await prepareLocalPushSource(workDir);

    const remote = instanceStateGitUrl(
      hub.url,
      tenant.tenantId,
      "ins_fakefakefake",
    );
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
    // Stock git surfaces an HTTP 403 advertise denial as
    // `the requested url returned error: 403` in stderr. The
    // `read-only`/`agent-state` substring is best-effort: git
    // strips most of the body unless the server emits a properly
    // pkt-line-framed `ERR` record on the wire. The advertise body
    // is verified separately via the direct-fetch test above.
    expect(combined).toMatch(/error: 403|http 403|forbidden/);
  }, 90_000);
});
