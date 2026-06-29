// Capability advertisement compatibility against real `git`.
//
// Stock git's `-c protocol.version=2` invocation negotiates protocol
// v2 with the server. For HTTP smart transports, the server may
// refuse v2 and fall back to v0/v1. We assert that the negotiation
// still terminates and that the advertised v0 capabilities the asset
// route emits are accepted by the real git binary (it does not error
// out with `unrecognized capability` or similar).

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

describe.skipIf(!harnessHubEnvAvailable())(
  "smart-HTTP capability advertisement",
  () => {
    test("protocol.version=2 fetch terminates and surfaces the advertised refs", async () => {
      const hub = await startHubTracked();
      const user = await signUpUser(hub.url);
      const tenant = await createTenant(hub.url, user);
      const asset = await createAsset(hub.url, user, tenant);
      const token = await mintTenantGitToken(hub.url, user, tenant, {
        refPattern: "**",
        actions: ["can_read"],
      });
      const url = assetSmartHttpUrl(hub.url, asset);

      // `git ls-remote` under protocol.version=2 emits a Git-Protocol
      // request header. The hub's advertise-refs handler returns the
      // v0 advertisement regardless; stock git falls back gracefully
      // when the response is not a v2 capability advertisement.
      const env = await tokenEnv(token.secret);
      const cwd = await mkTemp("proto-compat-");
      const lsRemote = await runGit(
        [
          "-c",
          "protocol.version=2",
          "-c",
          "credential.helper=",
          "ls-remote",
          url,
        ],
        { cwd, env },
      );
      if (lsRemote.status !== 0) {
        throw new Error(
          `ls-remote (v2 attempt) failed: ${lsRemote.stderr}\nstdout: ${lsRemote.stdout}`,
        );
      }
      // ls-remote prints `<sha>\t<ref>` per line; refs/heads/main must
      // be advertised.
      expect(lsRemote.stdout).toMatch(/refs\/heads\/main/);

      // Direct probe of info/refs to confirm the capability tail of the
      // first ref line carries the advertised v0 capabilities.
      const info = await fetch(`${url}/info/refs?service=git-upload-pack`, {
        headers: {
          Authorization: `Basic ${Buffer.from(`x:${token.secret}`).toString("base64")}`,
        },
      });
      expect(info.status).toBe(200);
      const text = await info.text();
      // First ref line: `<sha> <ref>\0<caps>\n`. Look for the NUL
      // separator and the capability tail.
      const nulIdx = text.indexOf("\0");
      if (nulIdx === -1) {
        throw new Error(`info/refs response missing NUL: ${text}`);
      }
      const capsTail = text.slice(nulIdx + 1, text.indexOf("\n", nulIdx));
      expect(capsTail).toContain("side-band-64k");
      expect(capsTail).toContain("ofs-delta");
      expect(capsTail).toContain("object-format=sha1");
      expect(capsTail).toMatch(/\bagent=interchange-hub\//);

      // And confirm `git fetch -c protocol.version=2` against the same
      // url completes against a fresh local repo.
      const target = path.join(cwd, "fetched");
      const initRes = await runGit(["init", "--initial-branch=main", target], {
        cwd,
      });
      if (initRes.status !== 0) throw new Error(`init: ${initRes.stderr}`);
      const fetchRes = await runGit(
        [
          "-c",
          "protocol.version=2",
          "-c",
          "credential.helper=",
          "fetch",
          url,
          "refs/heads/main:refs/remotes/origin/main",
        ],
        { cwd: target, env },
      );
      if (fetchRes.status !== 0) {
        throw new Error(
          `fetch v2 failed: ${fetchRes.stderr}\nstdout: ${fetchRes.stdout}`,
        );
      }
      const remoteMain = await runGit(
        ["rev-parse", "refs/remotes/origin/main"],
        {
          cwd: target,
        },
      );
      expect(remoteMain.stdout.trim().length).toBe(40);
    }, 90_000);
  },
);
