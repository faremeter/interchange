// Multi-commit push: a real `git push` carrying multiple commits in
// a single pack stream exercises the substrate's `git.indexPack`
// path with realistic delta references. The receive-pack pipeline
// must accept whatever pack stock git produces; the test confirms
// the resulting refs are reachable and `git fsck` is clean.

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

describe("multi-commit push", () => {
  test("a pack carrying multiple delta-related commits indexes cleanly", async () => {
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

    const cwd = await mkTemp("tp-multi-");
    const repoDir = path.join(cwd, "repo");
    const clone = await runGit(
      ["-c", "credential.helper=", "clone", remote, repoDir],
      { cwd, env: await tokenEnv(readToken.secret) },
    );
    if (clone.status !== 0) throw new Error(`clone: ${clone.stderr}`);
    for (const [k, v] of Object.entries({
      "user.name": "MC",
      "user.email": "mc@example.invalid",
    })) {
      await runGit(["config", k, v], { cwd: repoDir });
    }
    await runGit(["checkout", "-B", "main", "refs/remotes/origin/main"], {
      cwd: repoDir,
    });

    // Build a chain of commits that touch the same skill subtree with
    // small edits. Successive edits give stock git a natural delta to
    // express in the pack — that's the path the substrate's indexer
    // must accept.
    const skill = "evolving";
    await fs.mkdir(path.join(repoDir, skill), { recursive: true });
    for (let i = 0; i < 4; i += 1) {
      const body =
        `---\nname: ${skill}\ndescription: revision ${i.toString()}\n---\n\n` +
        `Body revision ${i.toString()}.\n${"x".repeat(40 + i * 10)}\n`;
      await fs.writeFile(path.join(repoDir, skill, "SKILL.md"), body, "utf-8");
      await runGit(["add", `${skill}/SKILL.md`], { cwd: repoDir });
      await runGit(["commit", "-m", `revision ${i.toString()}`], {
        cwd: repoDir,
      });
    }

    const pushRemote = withBasicAuth(remote, "x", writeToken.secret);
    const push = await runGit(
      [
        "-c",
        "credential.helper=",
        "push",
        pushRemote,
        "refs/heads/main:refs/heads/main",
      ],
      { cwd: repoDir },
    );
    if (push.status !== 0) {
      throw new Error(
        `push: status=${push.status}\nstderr:\n${push.stderr}\nstdout:\n${push.stdout}`,
      );
    }

    // Re-clone fresh and fsck the result; if the indexer accepted a
    // pack with broken delta references the new clone would not
    // reconstruct cleanly.
    const verifyCwd = await mkTemp("tp-verify-");
    const verify = path.join(verifyCwd, "repo");
    const reclone = await runGit(
      ["-c", "credential.helper=", "clone", remote, verify],
      { cwd: verifyCwd, env: await tokenEnv(readToken.secret) },
    );
    if (reclone.status !== 0) throw new Error(`reclone: ${reclone.stderr}`);
    const fsck = await runGit(["fsck", "--strict", "--no-dangling"], {
      cwd: verify,
    });
    if (fsck.status !== 0) {
      throw new Error(`fsck: ${fsck.stderr}`);
    }
    const localHead = await runGit(["rev-parse", "HEAD"], { cwd: repoDir });
    const remoteHead = await runGit(["rev-parse", "refs/remotes/origin/main"], {
      cwd: verify,
    });
    expect(remoteHead.stdout.trim()).toBe(localHead.stdout.trim());
  }, 120_000);
});
