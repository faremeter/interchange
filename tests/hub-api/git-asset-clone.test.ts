// End-to-end clone of an asset repo against the real `/usr/bin/git`.
//
// Three scenarios:
//   1. Anonymous clone fails with 401 + `WWW-Authenticate: Basic`.
//   2. Authorized clone succeeds; the resulting working tree passes
//      `git fsck --strict --no-dangling`.
//   3. The genesis commit reports a verifiable signature under
//      `git log --show-signature` against an allowed-signers entry
//      built from the public key embedded in the SSHSIG envelope.

import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import {
  installSshAllowedSigner,
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

// SSHSIG ASCII-armor begin/end markers per draft-miller-ssh-pamphlet.
const SSHSIG_BEGIN = "-----BEGIN SSH SIGNATURE-----";
const SSHSIG_END = "-----END SSH SIGNATURE-----";

function readU32(buf: Uint8Array, off: number): [number, number] {
  if (off + 4 > buf.length) throw new Error("truncated u32");
  const view = new DataView(buf.buffer, buf.byteOffset + off, 4);
  return [view.getUint32(0), off + 4];
}

function readString(buf: Uint8Array, off: number): [Uint8Array, number] {
  const [len, after] = readU32(buf, off);
  if (after + len > buf.length) throw new Error("truncated string");
  return [buf.slice(after, after + len), after + len];
}

/**
 * Pull the embedded public key out of an armored SSH signature and
 * render it back into the OpenSSH `ssh-ed25519 <base64>` text form
 * suitable for an allowed-signers entry.
 */
function extractOpenSshPublicKey(armored: string): string {
  const begin = armored.indexOf(SSHSIG_BEGIN);
  const end = armored.indexOf(SSHSIG_END);
  if (begin === -1 || end === -1) {
    throw new Error("armored SSH signature is missing begin/end markers");
  }
  const body = armored
    .slice(begin + SSHSIG_BEGIN.length, end)
    .replace(/\s+/g, "");
  const blob = new Uint8Array(Buffer.from(body, "base64"));

  let off = 0;
  // magic "SSHSIG"
  if (
    blob.length < 6 ||
    new TextDecoder().decode(blob.slice(0, 6)) !== "SSHSIG"
  ) {
    throw new Error("not an SSHSIG envelope");
  }
  off = 6;
  // version
  const [, afterVersion] = readU32(blob, off);
  off = afterVersion;
  // publickey blob: string("ssh-ed25519") || string(32-byte-key)
  const [pubKeyBlob, afterPubKey] = readString(blob, off);
  off = afterPubKey;
  void off;
  const [keyType] = readString(pubKeyBlob, 0);
  if (new TextDecoder().decode(keyType) !== "ssh-ed25519") {
    throw new Error("only ssh-ed25519 keys are supported");
  }
  return `ssh-ed25519 ${Buffer.from(pubKeyBlob).toString("base64")}`;
}

/**
 * Rewrite an `http://host:port/...` URL to embed the supplied basic-auth
 * credentials so git can issue an authenticated request without having
 * to read a credential off a TTY.
 */
function withBasicAuth(
  url: string,
  username: string,
  password: string,
): string {
  const u = new URL(url);
  u.username = encodeURIComponent(username);
  u.password = encodeURIComponent(password);
  return u.toString();
}

describe("anonymous clone", () => {
  test("rejects with 401 + WWW-Authenticate Basic", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);
    const tenant = await createTenant(hub.url, user);
    const asset = await createAsset(hub.url, user, tenant);

    const cwd = await mkTemp("anon-clone-");
    const cloneTarget = path.join(cwd, "repo");
    // Embed an obviously bogus credential pair in the URL so git sends
    // a request the hub can answer with 401 + WWW-Authenticate, rather
    // than aborting locally on the credential prompt.
    const remote = withBasicAuth(
      assetSmartHttpUrl(hub.url, asset),
      "anon",
      "itx_pat_invalid",
    );
    const result = await runGit(
      ["-c", "credential.helper=", "clone", remote, cloneTarget],
      { cwd },
    );

    expect(result.status).not.toBe(0);
    expect(result.stderr).toMatch(/401|Authentication failed|unauthorized/i);

    // Confirm the WWW-Authenticate header is present at the source.
    const probe = await fetch(
      `${assetSmartHttpUrl(hub.url, asset)}/info/refs?service=git-upload-pack`,
    );
    expect(probe.status).toBe(401);
    const challenge = probe.headers.get("WWW-Authenticate");
    expect(challenge).not.toBeNull();
    expect(challenge ?? "").toMatch(/Basic\s+realm="Interchange"/);
  }, 90_000);
});

describe("authorized clone", () => {
  test("clone succeeds and fsck reports a clean tree", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);
    const tenant = await createTenant(hub.url, user);
    const asset = await createAsset(hub.url, user, tenant);
    const token = await mintTenantGitToken(hub.url, user, tenant, {
      refPattern: "**",
      actions: ["can_read"],
    });

    const env = await tokenEnv(token.secret);
    const cwd = await mkTemp("auth-clone-");
    const cloneTarget = path.join(cwd, "repo");
    const result = await runGit(
      [
        "-c",
        "credential.helper=",
        "clone",
        assetSmartHttpUrl(hub.url, asset),
        cloneTarget,
      ],
      { cwd, env },
    );
    if (result.status !== 0) {
      throw new Error(
        `git clone exited ${result.status}: ${result.stderr}\nstdout: ${result.stdout}`,
      );
    }

    const fsck = await runGit(["fsck", "--strict", "--no-dangling"], {
      cwd: cloneTarget,
    });
    if (fsck.status !== 0) {
      throw new Error(
        `git fsck failed: status=${fsck.status} stderr=${fsck.stderr} stdout=${fsck.stdout}`,
      );
    }
  }, 90_000);

  test("git log --show-signature accepts the genesis signature", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);
    const tenant = await createTenant(hub.url, user);
    const asset = await createAsset(hub.url, user, tenant);
    const token = await mintTenantGitToken(hub.url, user, tenant, {
      refPattern: "**",
      actions: ["can_read"],
    });

    const env = await tokenEnv(token.secret);
    const cwd = await mkTemp("sig-clone-");
    const cloneTarget = path.join(cwd, "repo");
    const clone = await runGit(
      [
        "-c",
        "credential.helper=",
        "clone",
        assetSmartHttpUrl(hub.url, asset),
        cloneTarget,
      ],
      { cwd, env },
    );
    if (clone.status !== 0) {
      throw new Error(`git clone failed: ${clone.stderr}`);
    }

    // Pull the gpgsig out of the raw commit object so we can derive an
    // allowed-signers entry that matches the embedded key. The
    // advertise-refs layer does not currently project HEAD as a
    // separate ref (no `symref=HEAD:refs/heads/main` capability), so
    // the cloned tree has an unborn HEAD; resolve refs/heads/main
    // directly to reach the genesis commit.
    // The advertise-refs layer does not project HEAD as a
    // symref, so the local clone has no `refs/heads/main` checkout;
    // the remote tip lives under `refs/remotes/origin/main`.
    const catFile = await runGit(
      ["cat-file", "commit", "refs/remotes/origin/main"],
      { cwd: cloneTarget },
    );
    if (catFile.status !== 0) {
      throw new Error(
        `cat-file refs/remotes/origin/main failed: ${catFile.stderr}\nclone stderr:\n${clone.stderr}`,
      );
    }
    const sigStart = catFile.stdout.indexOf("gpgsig ");
    if (sigStart === -1) {
      throw new Error(`commit has no gpgsig header:\n${catFile.stdout}`);
    }
    // gpgsig spans every subsequent line that starts with a space.
    const lines = catFile.stdout.slice(sigStart).split("\n");
    let sigText = lines[0]?.slice("gpgsig ".length) ?? "";
    let consumed = 1;
    while (consumed < lines.length) {
      const line = lines[consumed];
      if (line === undefined) break;
      if (line.startsWith(" ")) {
        sigText += "\n" + line.slice(1);
        consumed += 1;
        continue;
      }
      break;
    }
    const openSshKey = extractOpenSshPublicKey(sigText);
    await installSshAllowedSigner(cloneTarget, openSshKey, "interchange-hub");

    const showSig = await runGit(
      ["log", "--show-signature", "-n", "1", "refs/remotes/origin/main"],
      { cwd: cloneTarget },
    );
    if (showSig.status !== 0) {
      throw new Error(
        `git log --show-signature failed: status=${showSig.status} stderr=${showSig.stderr}`,
      );
    }
    // git emits the verdict on stderr.
    const verdict = `${showSig.stdout}\n${showSig.stderr}`;
    if (!/Good "git" signature/.test(verdict)) {
      throw new Error(
        `expected a Good "git" signature verdict; got:\n${verdict}`,
      );
    }
  }, 90_000);
});
