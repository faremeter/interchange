// Clone → edit → commit → push → fresh clone roundtrip.
//
// Asserts the on-disk content and the HEAD SHA match between the two
// clones, demonstrating that the upload-pack/receive-pack pair
// round-trips both objects and refs through the smart-HTTP layer.

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

describe("clone → push → re-clone roundtrip", () => {
  test("the second clone observes the pushed commit byte-for-byte", async () => {
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
    const cwd1 = await mkTemp("rt-c1-");
    const c1 = path.join(cwd1, "repo");
    const clone1 = await runGit(
      ["-c", "credential.helper=", "clone", remote, c1],
      { cwd: cwd1, env: await tokenEnv(readToken.secret) },
    );
    if (clone1.status !== 0) throw new Error(`clone1: ${clone1.stderr}`);

    for (const [k, v] of Object.entries({
      "user.name": "RT",
      "user.email": "rt@example.invalid",
    })) {
      await runGit(["config", k, v], { cwd: c1 });
    }
    // HEAD is advertised as a symref pointing at refs/heads/main, so
    // stock `git clone` already left a born HEAD checked out on the
    // local main branch. The explicit checkout below is a no-op on the
    // happy path and would diverge if the symref ever regresses; assert
    // HEAD's pre-checkout state matches refs/remotes/origin/main first.
    {
      const headRev = await runGit(["rev-parse", "HEAD"], { cwd: c1 });
      if (headRev.status !== 0) {
        throw new Error(`rev-parse HEAD failed in c1: ${headRev.stderr}`);
      }
      const originRev = await runGit(
        ["rev-parse", "refs/remotes/origin/main"],
        { cwd: c1 },
      );
      if (originRev.status !== 0) {
        throw new Error(
          `rev-parse origin/main failed in c1: ${originRev.stderr}`,
        );
      }
      expect(headRev.stdout.trim()).toBe(originRev.stdout.trim());
    }
    await runGit(["checkout", "-B", "main", "refs/remotes/origin/main"], {
      cwd: c1,
    });
    await fs.mkdir(path.join(c1, "greet"), { recursive: true });
    const skillBody =
      "---\nname: greet\ndescription: Greeting skill\n---\n\n# greet\n";
    await fs.writeFile(path.join(c1, "greet", "SKILL.md"), skillBody, "utf-8");
    await runGit(["add", "greet/SKILL.md"], { cwd: c1 });
    await runGit(["commit", "-m", "Add greet skill"], { cwd: c1 });

    const pushRemote = withBasicAuth(remote, "x", writeToken.secret);
    const push = await runGit(
      [
        "-c",
        "credential.helper=",
        "push",
        pushRemote,
        "refs/heads/main:refs/heads/main",
      ],
      { cwd: c1 },
    );
    if (push.status !== 0) throw new Error(`push: ${push.stderr}`);

    const head1 = await runGit(["rev-parse", "HEAD"], { cwd: c1 });
    if (head1.status !== 0) throw new Error(`rev-parse: ${head1.stderr}`);
    const head1Sha = head1.stdout.trim();

    // Fresh clone into a different dir.
    const cwd2 = await mkTemp("rt-c2-");
    const c2 = path.join(cwd2, "repo");
    const clone2 = await runGit(
      ["-c", "credential.helper=", "clone", remote, c2],
      { cwd: cwd2, env: await tokenEnv(readToken.secret) },
    );
    if (clone2.status !== 0) throw new Error(`clone2: ${clone2.stderr}`);

    const head2 = await runGit(["rev-parse", "refs/remotes/origin/main"], {
      cwd: c2,
    });
    if (head2.status !== 0) throw new Error(`rev-parse2: ${head2.stderr}`);
    expect(head2.stdout.trim()).toBe(head1Sha);

    // The advertise layer projects HEAD as a symref, so the fresh
    // clone also has a born HEAD that resolves to the same SHA.
    const head2Head = await runGit(["rev-parse", "HEAD"], { cwd: c2 });
    if (head2Head.status !== 0) {
      throw new Error(`rev-parse HEAD failed in c2: ${head2Head.stderr}`);
    }
    expect(head2Head.stdout.trim()).toBe(head1Sha);

    // Verify the skill body matches via ls-tree against the remote-
    // tracking ref; both this path and HEAD resolve to the same tree.
    const lsTree = await runGit(
      ["ls-tree", "-r", "refs/remotes/origin/main", "greet/SKILL.md"],
      { cwd: c2 },
    );
    if (lsTree.status !== 0) throw new Error(`ls-tree: ${lsTree.stderr}`);
    const treeFields = lsTree.stdout.trim().split(/\s+/);
    const blobSha = treeFields[2];
    if (blobSha === undefined || blobSha.length === 0) {
      throw new Error(`ls-tree produced no blob entry: ${lsTree.stdout}`);
    }
    const catBlob = await runGit(["cat-file", "-p", blobSha], { cwd: c2 });
    if (catBlob.status !== 0) throw new Error(`cat-file: ${catBlob.stderr}`);
    expect(catBlob.stdout).toBe(skillBody);
  }, 120_000);
});
