// REST-create-then-clone roundtrip: a fresh asset is created over
// the REST endpoint and immediately cloned over smart-HTTP. The
// clone must return a signed genesis on refs/heads/main and pass
// `git fsck`.

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

describe.skipIf(!harnessDbEnvAvailable())(
  "REST create → immediate clone",
  () => {
    test("the clone returns a signed genesis on refs/heads/main", async () => {
      const hub = await startHubTracked();
      const user = await signUpUser(hub.url);
      const tenant = await createTenant(hub.url, user);
      const asset = await createAsset(hub.url, user, tenant, {
        kind: "skill",
        name: "freshly-created",
      });
      const token = await mintTenantGitToken(hub.url, user, tenant, {
        refPattern: "**",
        actions: ["can_read"],
      });

      const cwd = await mkTemp("create-rt-");
      const target = path.join(cwd, "repo");
      const clone = await runGit(
        [
          "-c",
          "credential.helper=",
          "clone",
          assetSmartHttpUrl(hub.url, asset),
          target,
        ],
        { cwd, env: await tokenEnv(token.secret) },
      );
      if (clone.status !== 0) {
        throw new Error(`clone: ${clone.stderr}`);
      }

      const fsck = await runGit(["fsck", "--strict", "--no-dangling"], {
        cwd: target,
      });
      if (fsck.status !== 0) {
        throw new Error(`fsck: ${fsck.stderr}\nstdout: ${fsck.stdout}`);
      }

      // HEAD is advertised as a symref pointing at refs/heads/main, so
      // the clone has a born HEAD. Assert it matches the remote tracking
      // ref before reading the genesis commit via HEAD.
      const headRev = await runGit(["rev-parse", "HEAD"], { cwd: target });
      if (headRev.status !== 0) {
        throw new Error(`rev-parse HEAD: ${headRev.stderr}`);
      }
      const originRev = await runGit(
        ["rev-parse", "refs/remotes/origin/main"],
        {
          cwd: target,
        },
      );
      if (originRev.status !== 0) {
        throw new Error(`rev-parse origin/main: ${originRev.stderr}`);
      }
      expect(headRev.stdout.trim()).toBe(originRev.stdout.trim());

      const catFile = await runGit(["cat-file", "commit", "HEAD"], {
        cwd: target,
      });
      if (catFile.status !== 0) {
        throw new Error(`cat-file: ${catFile.stderr}`);
      }
      expect(catFile.stdout).toContain("gpgsig ");
      expect(catFile.stdout).toContain("BEGIN SSH SIGNATURE");
      expect(catFile.stdout).toContain("interchange-hub");
    }, 90_000);
  },
);
