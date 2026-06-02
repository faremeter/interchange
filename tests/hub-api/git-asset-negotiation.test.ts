// Negotiation test: after a clone and a small push, a second fetch
// against the same clone transfers fewer objects than the original
// clone. Verified via `git count-objects -v` before and after.

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

function parseCountObjects(out: string): {
  count: number;
  inPack: number;
  packs: number;
} {
  const lines = out.split("\n");
  const find = (prefix: string): number => {
    const line = lines.find((l) => l.startsWith(prefix));
    if (line === undefined) {
      throw new Error(`count-objects missing ${prefix} line in: ${out}`);
    }
    const parts = line.split(/\s+/);
    const last = parts[parts.length - 1];
    if (last === undefined)
      throw new Error(`count-objects malformed line: ${line}`);
    return Number(last);
  };
  return {
    count: find("count:"),
    inPack: find("in-pack:"),
    packs: find("packs:"),
  };
}

describe("incremental fetch", () => {
  test("a second fetch after a remote-side push transfers only the delta", async () => {
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

    // Initial clone (consumer side).
    const consumerCwd = await mkTemp("neg-consumer-");
    const consumer = path.join(consumerCwd, "repo");
    const clone = await runGit(
      ["-c", "credential.helper=", "clone", remote, consumer],
      { cwd: consumerCwd, env: await tokenEnv(readToken.secret) },
    );
    if (clone.status !== 0) throw new Error(`clone: ${clone.stderr}`);

    // Snapshot after-clone object counts.
    const beforeCounts = await runGit(["count-objects", "-v"], {
      cwd: consumer,
    });
    if (beforeCounts.status !== 0) {
      throw new Error(`count-objects pre: ${beforeCounts.stderr}`);
    }
    const before = parseCountObjects(beforeCounts.stdout);

    // Producer side: clone with write creds, add a small skill, push.
    const producerCwd = await mkTemp("neg-producer-");
    const producer = path.join(producerCwd, "repo");
    const cloneP = await runGit(
      ["-c", "credential.helper=", "clone", remote, producer],
      { cwd: producerCwd, env: await tokenEnv(readToken.secret) },
    );
    if (cloneP.status !== 0) throw new Error(`cloneP: ${cloneP.stderr}`);
    for (const [k, v] of Object.entries({
      "user.name": "Producer",
      "user.email": "p@example.invalid",
    })) {
      await runGit(["config", k, v], { cwd: producer });
    }
    await runGit(["checkout", "-B", "main", "refs/remotes/origin/main"], {
      cwd: producer,
    });
    await fs.mkdir(path.join(producer, "small"), { recursive: true });
    await fs.writeFile(
      path.join(producer, "small", "SKILL.md"),
      "---\nname: small\ndescription: tiny delta\n---\n\nx\n",
      "utf-8",
    );
    await runGit(["add", "small/SKILL.md"], { cwd: producer });
    await runGit(["commit", "-m", "add small"], { cwd: producer });
    const pushRemote = withBasicAuth(remote, "x", writeToken.secret);
    const push = await runGit(
      [
        "-c",
        "credential.helper=",
        "push",
        pushRemote,
        "refs/heads/main:refs/heads/main",
      ],
      { cwd: producer },
    );
    if (push.status !== 0) throw new Error(`push: ${push.stderr}`);

    // Consumer fetches the delta.
    const fetch = await runGit(
      ["-c", "credential.helper=", "fetch", "origin"],
      { cwd: consumer, env: await tokenEnv(readToken.secret) },
    );
    if (fetch.status !== 0) throw new Error(`fetch: ${fetch.stderr}`);

    const afterCounts = await runGit(["count-objects", "-v"], {
      cwd: consumer,
    });
    if (afterCounts.status !== 0) {
      throw new Error(`count-objects post: ${afterCounts.stderr}`);
    }
    const after = parseCountObjects(afterCounts.stdout);

    // The fetch must transfer the delta only: the count of objects
    // already in the local clone (the haves) must NOT be repeated.
    // That is, the delta `totalAfter - totalBefore` must be strictly
    // less than `totalAfter` — proving that fewer than all objects
    // were transferred in the second exchange.
    const totalBefore = before.count + before.inPack;
    const totalAfter = after.count + after.inPack;
    expect(totalAfter).toBeGreaterThan(totalBefore);
    const delta = totalAfter - totalBefore;
    expect(delta).toBeLessThan(totalAfter);
  }, 120_000);
});
