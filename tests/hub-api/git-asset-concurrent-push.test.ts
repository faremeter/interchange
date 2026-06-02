// Concurrent-push race: two pushes against the same ref are launched
// in parallel. One wins; the other observes a non-fast-forward
// rejection on stderr. The remote tip after the race matches the
// winner's commit.

import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { runGit, startHub, type HubHandle } from "./lib/git-harness";
import {
  assetSmartHttpUrl,
  createAsset,
  createTenant,
  mintTenantGitToken,
  signUpUser,
  tokenEnv,
} from "./lib/git-asset-fixtures";

const stops: (() => Promise<void>)[] = [];
const tempDirs: string[] = [];

afterEach(async () => {
  for (const stop of stops.splice(0)) await stop();
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

function withBasicAuth(url: string, user: string, pass: string): string {
  const u = new URL(url);
  u.username = encodeURIComponent(user);
  u.password = encodeURIComponent(pass);
  return u.toString();
}

async function setupWorker(
  remote: string,
  readToken: string,
  cwdPrefix: string,
  skillName: string,
): Promise<{ cloneDir: string }> {
  const cwd = await mkTemp(cwdPrefix);
  const cloneDir = path.join(cwd, "repo");
  const clone = await runGit(
    ["-c", "credential.helper=", "clone", remote, cloneDir],
    { cwd, env: await tokenEnv(readToken) },
  );
  if (clone.status !== 0)
    throw new Error(`clone ${cwdPrefix}: ${clone.stderr}`);

  for (const [k, v] of Object.entries({
    "user.name": `Concurrent ${skillName}`,
    "user.email": `${skillName}@example.invalid`,
  })) {
    await runGit(["config", k, v], { cwd: cloneDir });
  }
  await runGit(["checkout", "-B", "main", "refs/remotes/origin/main"], {
    cwd: cloneDir,
  });
  await fs.mkdir(path.join(cloneDir, skillName), { recursive: true });
  await fs.writeFile(
    path.join(cloneDir, skillName, "SKILL.md"),
    `---\nname: ${skillName}\ndescription: ${skillName}\n---\n\nbody for ${skillName}\n`,
    "utf-8",
  );
  await runGit(["add", `${skillName}/SKILL.md`], { cwd: cloneDir });
  await runGit(["commit", "-m", `add ${skillName}`], { cwd: cloneDir });
  return { cloneDir };
}

describe("concurrent push race", () => {
  test("one push wins; the other reports non-fast-forward", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);
    const tenant = await createTenant(hub.url, user);
    const asset = await createAsset(hub.url, user, tenant);
    const readToken = await mintTenantGitToken(hub.url, user, tenant, {
      refPattern: "**",
      actions: ["can_read"],
    });
    const writeToken = await mintTenantGitToken(hub.url, user, tenant, {
      refPattern: "**",
      actions: ["can_read", "can_push"],
    });
    const remote = assetSmartHttpUrl(hub.url, asset);

    const alpha = await setupWorker(
      remote,
      readToken.secret,
      "race-a-",
      "alpha",
    );
    const beta = await setupWorker(remote, readToken.secret, "race-b-", "beta");

    const pushRemote = withBasicAuth(remote, "x", writeToken.secret);
    const pushArgs = [
      "-c",
      "credential.helper=",
      "push",
      pushRemote,
      "refs/heads/main:refs/heads/main",
    ];

    const [aRes, bRes] = await Promise.all([
      runGit(pushArgs, { cwd: alpha.cloneDir }),
      runGit(pushArgs, { cwd: beta.cloneDir }),
    ]);

    const aOk = aRes.status === 0;
    const bOk = bRes.status === 0;
    // Exactly one must succeed.
    if (aOk === bOk) {
      throw new Error(
        `expected exactly one push to succeed; alpha=${aRes.status} beta=${bRes.status}\n` +
          `alpha stderr:\n${aRes.stderr}\nbeta stderr:\n${bRes.stderr}`,
      );
    }
    const loser = aOk ? bRes : aRes;
    const winnerCloneDir = aOk ? alpha.cloneDir : beta.cloneDir;
    const combined = `${loser.stdout}\n${loser.stderr}`;
    expect(combined).toMatch(/non-fast-forward|remote rejected/);

    const winnerHead = await runGit(["rev-parse", "HEAD"], {
      cwd: winnerCloneDir,
    });
    if (winnerHead.status !== 0) {
      throw new Error(`winner rev-parse: ${winnerHead.stderr}`);
    }

    const lsRemote = await runGit(
      ["ls-remote", pushRemote, "refs/heads/main"],
      { cwd: alpha.cloneDir },
    );
    if (lsRemote.status !== 0) {
      throw new Error(`ls-remote: ${lsRemote.stderr}`);
    }
    expect(lsRemote.stdout.split(/\s+/)[0]).toBe(winnerHead.stdout.trim());
  }, 120_000);
});
