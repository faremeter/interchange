// End-to-end push scenarios against the real `/usr/bin/git`.
//
// Three scenarios:
//   1. A push that satisfies the token's refPattern and action set
//      succeeds; the remote ref advances and `ls-remote` reflects
//      the new tip.
//   2. A push that targets a ref outside the token's refPattern is
//      rejected with `(forbidden)` on stderr.
//   3. A push using a read-only token (no `receivePack` action) is
//      rejected with `(forbidden)` on stderr.

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

async function setupSignedCommit(
  cwd: string,
  remoteUrl: string,
): Promise<{ cloneTarget: string }> {
  const cloneTarget = path.join(cwd, "repo");
  const clone = await runGit(
    ["-c", "credential.helper=", "clone", remoteUrl, cloneTarget],
    { cwd },
  );
  if (clone.status !== 0) {
    throw new Error(`clone failed: ${clone.stderr}`);
  }
  // Identify the worker; checkout the remote tip locally so push has
  // a parent reachable from the server side.
  for (const [k, v] of Object.entries({
    "user.name": "Test User",
    "user.email": "test@example.invalid",
  })) {
    const c = await runGit(["config", k, v], { cwd: cloneTarget });
    if (c.status !== 0) throw new Error(`config ${k} failed: ${c.stderr}`);
  }
  const checkout = await runGit(
    ["checkout", "-B", "main", "refs/remotes/origin/main"],
    { cwd: cloneTarget },
  );
  if (checkout.status !== 0) {
    throw new Error(`checkout main failed: ${checkout.stderr}`);
  }
  await fs.writeFile(
    path.join(cloneTarget, "README.md"),
    "# updated\n",
    "utf-8",
  );
  const add = await runGit(["add", "README.md"], { cwd: cloneTarget });
  if (add.status !== 0) throw new Error(`git add failed: ${add.stderr}`);
  const commit = await runGit(["commit", "-m", "Update README"], {
    cwd: cloneTarget,
  });
  if (commit.status !== 0) {
    throw new Error(`git commit failed: ${commit.stderr}`);
  }
  return { cloneTarget };
}

function withBasicAuth(url: string, user: string, pass: string): string {
  const u = new URL(url);
  u.username = encodeURIComponent(user);
  u.password = encodeURIComponent(pass);
  return u.toString();
}

describe("authorized push", () => {
  test("fast-forward push succeeds against a permissive token", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);
    const tenant = await createTenant(hub.url, user);
    const asset = await createAsset(hub.url, user, tenant);

    // Two tokens: a read-scoped one for the clone (so listRefs is
    // gated by ** and succeeds), and a write-scoped one for the push.
    // The push token must include `receivePack` plus a refPattern that
    // matches refs/heads/main.
    const readToken = await mintTenantGitToken(hub.url, user, tenant, {
      refPattern: "**",
      actions: ["can_read"],
    });
    const writeToken = await mintTenantGitToken(hub.url, user, tenant, {
      refPattern: "**",
      actions: ["can_read", "can_push"],
    });

    const cwd = await mkTemp("push-ok-");
    const remoteForRead = assetSmartHttpUrl(hub.url, asset);
    const cloneEnv = await tokenEnv(readToken.secret);
    const cloneRes = await runGit(
      [
        "-c",
        "credential.helper=",
        "clone",
        remoteForRead,
        path.join(cwd, "repo"),
      ],
      { cwd, env: cloneEnv },
    );
    if (cloneRes.status !== 0) {
      throw new Error(`clone failed: ${cloneRes.stderr}`);
    }
    // Embed the write token in the remote URL for the push step.
    const pushRemote = withBasicAuth(
      assetSmartHttpUrl(hub.url, asset),
      "x",
      writeToken.secret,
    );
    const cloneTarget = path.join(cwd, "repo");
    for (const [k, v] of Object.entries({
      "user.name": "Push Test",
      "user.email": "push@example.invalid",
    })) {
      const c = await runGit(["config", k, v], { cwd: cloneTarget });
      if (c.status !== 0) throw new Error(`config ${k} failed: ${c.stderr}`);
    }
    const checkout = await runGit(
      ["checkout", "-B", "main", "refs/remotes/origin/main"],
      { cwd: cloneTarget },
    );
    if (checkout.status !== 0) {
      throw new Error(`checkout failed: ${checkout.stderr}`);
    }
    // Skill kind requires every top-level directory to be a skill
    // subtree containing a SKILL.md with matching frontmatter.
    await fs.mkdir(path.join(cloneTarget, "greet"), { recursive: true });
    await fs.writeFile(
      path.join(cloneTarget, "greet", "SKILL.md"),
      "---\nname: greet\ndescription: Greeting\n---\n\nbody\n",
      "utf-8",
    );
    const add = await runGit(["add", "greet/SKILL.md"], { cwd: cloneTarget });
    if (add.status !== 0) throw new Error(`add failed: ${add.stderr}`);
    const commit = await runGit(["commit", "-m", "Add greet skill"], {
      cwd: cloneTarget,
    });
    if (commit.status !== 0) throw new Error(`commit failed: ${commit.stderr}`);

    const push = await runGit(
      [
        "-c",
        "credential.helper=",
        "push",
        pushRemote,
        "refs/heads/main:refs/heads/main",
      ],
      { cwd: cloneTarget },
    );
    if (push.status !== 0) {
      throw new Error(
        `push failed: status=${push.status}\nstderr:\n${push.stderr}\nstdout:\n${push.stdout}`,
      );
    }

    // Confirm the remote tip advanced.
    const lsRemote = await runGit(
      ["ls-remote", pushRemote, "refs/heads/main"],
      { cwd },
    );
    if (lsRemote.status !== 0) {
      throw new Error(`ls-remote failed: ${lsRemote.stderr}`);
    }
    const localHead = await runGit(["rev-parse", "HEAD"], { cwd: cloneTarget });
    if (localHead.status !== 0) {
      throw new Error(`rev-parse failed: ${localHead.stderr}`);
    }
    expect(lsRemote.stdout.split(/\s+/)[0]).toBe(localHead.stdout.trim());
  }, 90_000);
});

describe("refPattern-forbidden push", () => {
  test("push to refs/heads/secret is rejected as (forbidden)", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);
    const tenant = await createTenant(hub.url, user);
    const asset = await createAsset(hub.url, user, tenant);
    const readToken = await mintTenantGitToken(hub.url, user, tenant, {
      refPattern: "**",
      actions: ["can_read"],
    });
    const writeToken = await mintTenantGitToken(hub.url, user, tenant, {
      refPattern: "refs/heads/main",
      actions: ["can_read", "can_push"],
    });

    const cwd = await mkTemp("push-pattern-");
    const remote = assetSmartHttpUrl(hub.url, asset);
    const cloneRes = await runGit(
      ["-c", "credential.helper=", "clone", remote, path.join(cwd, "repo")],
      { cwd, env: await tokenEnv(readToken.secret) },
    );
    if (cloneRes.status !== 0) {
      throw new Error(`clone failed: ${cloneRes.stderr}`);
    }
    const cloneTarget = path.join(cwd, "repo");
    for (const [k, v] of Object.entries({
      "user.name": "Pattern Test",
      "user.email": "pat@example.invalid",
    })) {
      await runGit(["config", k, v], { cwd: cloneTarget });
    }
    // Create a local branch named "secret" off the remote tip.
    const co = await runGit(
      ["checkout", "-B", "secret", "refs/remotes/origin/main"],
      { cwd: cloneTarget },
    );
    if (co.status !== 0) {
      throw new Error(`checkout secret failed: ${co.stderr}`);
    }
    await fs.mkdir(path.join(cloneTarget, "secret-skill"), { recursive: true });
    await fs.writeFile(
      path.join(cloneTarget, "secret-skill", "SKILL.md"),
      "---\nname: secret-skill\ndescription: secret\n---\n\nbody\n",
      "utf-8",
    );
    await runGit(["add", "secret-skill/SKILL.md"], { cwd: cloneTarget });
    await runGit(["commit", "-m", "secret branch"], { cwd: cloneTarget });

    const pushRemote = withBasicAuth(remote, "x", writeToken.secret);
    const push = await runGit(
      [
        "-c",
        "credential.helper=",
        "push",
        pushRemote,
        "refs/heads/secret:refs/heads/secret",
      ],
      { cwd: cloneTarget },
    );
    expect(push.status).not.toBe(0);
    const combined = `${push.stdout}\n${push.stderr}`;
    expect(combined).toMatch(/remote rejected/);
    expect(combined.toLowerCase()).toContain("forbidden");
  }, 90_000);
});

describe("read-only token push", () => {
  test("push with a token lacking receivePack is denied", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);
    const tenant = await createTenant(hub.url, user);
    const asset = await createAsset(hub.url, user, tenant);
    const readToken = await mintTenantGitToken(hub.url, user, tenant, {
      refPattern: "**",
      actions: ["can_read"],
    });

    const cwd = await mkTemp("push-ro-");
    const remote = assetSmartHttpUrl(hub.url, asset);
    const cloneRes = await runGit(
      ["-c", "credential.helper=", "clone", remote, path.join(cwd, "repo")],
      { cwd, env: await tokenEnv(readToken.secret) },
    );
    if (cloneRes.status !== 0) {
      throw new Error(`clone failed: ${cloneRes.stderr}`);
    }
    const cloneTarget = path.join(cwd, "repo");
    for (const [k, v] of Object.entries({
      "user.name": "RO Test",
      "user.email": "ro@example.invalid",
    })) {
      await runGit(["config", k, v], { cwd: cloneTarget });
    }
    await runGit(["checkout", "-B", "main", "refs/remotes/origin/main"], {
      cwd: cloneTarget,
    });
    await fs.mkdir(path.join(cloneTarget, "ro-skill"), { recursive: true });
    await fs.writeFile(
      path.join(cloneTarget, "ro-skill", "SKILL.md"),
      "---\nname: ro-skill\ndescription: ro\n---\n\nbody\n",
      "utf-8",
    );
    await runGit(["add", "ro-skill/SKILL.md"], { cwd: cloneTarget });
    await runGit(["commit", "-m", "Update"], { cwd: cloneTarget });

    // Use the read-only token to push. The middleware short-circuits on
    // the action gate before any wire-format exchange.
    const pushRemote = withBasicAuth(remote, "x", readToken.secret);
    const push = await runGit(
      [
        "-c",
        "credential.helper=",
        "push",
        pushRemote,
        "refs/heads/main:refs/heads/main",
      ],
      { cwd: cloneTarget },
    );
    expect(push.status).not.toBe(0);
    const combined = `${push.stdout}\n${push.stderr}`.toLowerCase();
    // The smart-HTTP layer rejects the receive-pack info/refs request
    // with HTTP 403 before any wire-format exchange takes place. Stock
    // git surfaces that as `rpc failed; http 403` in stderr.
    expect(combined).toMatch(/http 403|forbidden/);
  }, 90_000);

  void setupSignedCommit;
});
