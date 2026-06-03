// End-to-end clone of an agent-state per-definition repo against
// the real `/usr/bin/git`.
//
// The per-definition repo stores hub-written deploy artifacts under
// `deploy/`. In production this is populated by `writeDeployTree`
// during instance launch; we pre-stage the repo on disk under the
// hub's data dir so the clone has real content to fetch without
// requiring a connected sidecar.
//
// Three scenarios:
//   1. Definition creator (the tenant owner) clones and observes
//      `deploy/prompt.md` history under `refs/remotes/origin/deploy`.
//   2. Admin (owner role `*:*`) clones successfully.
//   3. A token bound to a separate tenant is denied at advertise
//      with `http 403`.
//
// The advertise layer projects HEAD as a symref via the
// `symref=HEAD:<target>` capability, so stock `git clone` lands on a
// real branch and HEAD is born against refs/heads/deploy.

import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import {
  harnessDbEnvAvailable,
  runGit,
  startHub,
  type HubHandle,
} from "./lib/git-harness";
import {
  apiCall,
  createTenant,
  mintTenantGitToken,
  signUpUser,
  tokenEnv,
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

type CreatedAgent = { agentId: string };

async function createAgentDefinition(
  hubUrl: string,
  user: SignedUpUser,
  tenantId: string,
  name: string,
): Promise<CreatedAgent> {
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
 * Initialize the per-definition agent-state repo at
 * `<hubDataDir>/agents/<agentId>` with a single `deploy/prompt.md`
 * commit on `refs/heads/deploy`. Mirrors what `writeDeployTree` +
 * `createDeployPack` would produce at instance launch, minus the
 * SSHSIG. The hub's RepoStore reads the directory by name; the
 * branch list is returned by `git.listBranches` from
 * `isomorphic-git`, so a plain git-CLI init + commit is sufficient.
 */
async function seedDefinitionDeployHistory(
  dataDir: string,
  agentId: string,
  promptBody: string,
): Promise<void> {
  const repoDir = path.join(dataDir, "agents", agentId);
  await fs.mkdir(repoDir, { recursive: true });

  await runGit(["init", "--initial-branch=deploy", repoDir], {
    cwd: repoDir,
  });
  await runGit(["config", "user.name", "interchange-hub"], { cwd: repoDir });
  await runGit(["config", "user.email", "hub@interchange.local"], {
    cwd: repoDir,
  });
  await runGit(["config", "commit.gpgsign", "false"], { cwd: repoDir });

  await fs.mkdir(path.join(repoDir, "deploy"), { recursive: true });
  await fs.writeFile(
    path.join(repoDir, "deploy", "prompt.md"),
    promptBody,
    "utf-8",
  );
  await fs.writeFile(path.join(repoDir, ".gitignore"), "keys/\n", "utf-8");

  await runGit(["add", "deploy/prompt.md", ".gitignore"], { cwd: repoDir });
  const commit = await runGit(
    ["commit", "-m", "Seed deploy prompt for test fixture"],
    { cwd: repoDir },
  );
  if (commit.status !== 0) {
    throw new Error(
      `seed commit failed: status=${commit.status} stderr=${commit.stderr}`,
    );
  }
}

function definitionStateGitUrl(
  hubUrl: string,
  tenantId: string,
  agentId: string,
): string {
  return `${hubUrl}/api/tenants/${tenantId}/agents/definitions/${agentId}/state.git`;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "agent-state per-definition clone",
  () => {
    test("creator clones deploy/prompt.md history", async () => {
      const hub = await startHubTracked();
      const user = await signUpUser(hub.url);
      const tenant = await createTenant(hub.url, user);
      const agent = await createAgentDefinition(
        hub.url,
        user,
        tenant.tenantId,
        "def-creator-agent",
      );
      const promptBody = "# Test deploy prompt\n\nseeded by test fixture\n";
      await seedDefinitionDeployHistory(hub.dataDir, agent.agentId, promptBody);

      const token = await mintTenantGitToken(hub.url, user, tenant, {
        resource: `agent-state:${agent.agentId}`,
        refPattern: "**",
        actions: ["can_read"],
      });
      const env = await tokenEnv(token.secret);
      const cwd = await mkTemp("agent-state-def-creator-");
      const cloneTarget = path.join(cwd, "repo");
      const remote = definitionStateGitUrl(
        hub.url,
        tenant.tenantId,
        agent.agentId,
      );
      const result = await runGit(
        ["-c", "credential.helper=", "clone", remote, cloneTarget],
        { cwd, env },
      );
      if (result.status !== 0) {
        throw new Error(
          `git clone exited ${result.status}: ${result.stderr}\nstdout: ${result.stdout}`,
        );
      }

      // HEAD is advertised as a symref pointing at refs/heads/deploy,
      // so the clone has a born HEAD. Assert it matches the remote
      // tracking ref before reading deploy/prompt.md from HEAD.
      const headRev = await runGit(["rev-parse", "HEAD"], { cwd: cloneTarget });
      if (headRev.status !== 0) {
        throw new Error(`rev-parse HEAD failed: ${headRev.stderr}`);
      }
      const originRev = await runGit(
        ["rev-parse", "refs/remotes/origin/deploy"],
        { cwd: cloneTarget },
      );
      if (originRev.status !== 0) {
        throw new Error(`rev-parse origin/deploy failed: ${originRev.stderr}`);
      }
      expect(headRev.stdout.trim()).toBe(originRev.stdout.trim());

      const showBlob = await runGit(["show", "HEAD:deploy/prompt.md"], {
        cwd: cloneTarget,
      });
      if (showBlob.status !== 0) {
        throw new Error(
          `show blob failed: status=${showBlob.status} stderr=${showBlob.stderr}`,
        );
      }
      expect(showBlob.stdout).toBe(promptBody);
    }, 90_000);

    test("admin (owner *:*) clones the per-definition repo", async () => {
      // The tenant owner role holds `*:*`. As in the per-instance
      // tests, the creator IS the owner in this fixture; the admin
      // grant path overlaps with the creator-seed path on the same
      // grant chain. We assert the clone succeeds via that chain.
      const hub = await startHubTracked();
      const user = await signUpUser(hub.url);
      const tenant = await createTenant(hub.url, user);
      const agent = await createAgentDefinition(
        hub.url,
        user,
        tenant.tenantId,
        "def-admin-agent",
      );
      await seedDefinitionDeployHistory(
        hub.dataDir,
        agent.agentId,
        "# admin path\n",
      );

      const token = await mintTenantGitToken(hub.url, user, tenant, {
        resource: `agent-state:${agent.agentId}`,
        refPattern: "**",
        actions: ["can_read"],
      });
      const env = await tokenEnv(token.secret);
      const cwd = await mkTemp("agent-state-def-admin-");
      const remote = definitionStateGitUrl(
        hub.url,
        tenant.tenantId,
        agent.agentId,
      );
      const result = await runGit(
        ["-c", "credential.helper=", "clone", remote, path.join(cwd, "repo")],
        { cwd, env },
      );
      if (result.status !== 0) {
        throw new Error(
          `git clone exited ${result.status}: ${result.stderr}\nstdout: ${result.stdout}`,
        );
      }
    }, 90_000);

    test("non-tenant token is denied at advertise", async () => {
      // Same pattern as the per-instance clone test: a separate
      // tenant's token cannot be used against this tenant's
      // definition URL. The bearer middleware emits 403
      // tenant_mismatch.
      const hub = await startHubTracked();
      const userA = await signUpUser(hub.url);
      const tenantA = await createTenant(hub.url, userA);
      const agent = await createAgentDefinition(
        hub.url,
        userA,
        tenantA.tenantId,
        "def-denied-agent",
      );
      await seedDefinitionDeployHistory(
        hub.dataDir,
        agent.agentId,
        "# denied path\n",
      );

      const userB = await signUpUser(hub.url);
      const tenantB = await createTenant(hub.url, userB);
      const tokenB = await mintTenantGitToken(hub.url, userB, tenantB, {
        resource: `agent-state:${agent.agentId}`,
        refPattern: "**",
        actions: ["can_read"],
      });

      const advertiseUrl = `${definitionStateGitUrl(
        hub.url,
        tenantA.tenantId,
        agent.agentId,
      )}/info/refs?service=git-upload-pack`;
      const res = await fetch(advertiseUrl, {
        headers: {
          Authorization: `Bearer ${tokenB.secret}`,
        },
      });
      expect(res.status).toBe(403);
    }, 90_000);
  },
);
