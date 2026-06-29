import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import {
  generateKeyPair,
  createSSHSignature,
  verifySSHSignature,
} from "@intx/crypto";
import { initRepo } from "./init";
import type { CommitSigner } from "./signer";

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "interchange-test-"),
  );
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.promises.rm(d, { recursive: true, force: true })),
  );
});

describe("initRepo unsigned default", () => {
  test("produces an unsigned genesis commit authored as harness", async () => {
    const dir = await tempDir();
    await initRepo(dir);

    const [entry] = await git.log({ fs, dir, depth: 1 });
    if (entry === undefined) throw new Error("no commit in log");

    expect(entry.commit.author.name).toBe("interchange-harness");
    expect(entry.commit.author.email).toBe("harness@interchange.local");
    expect(entry.commit.gpgsig).toBeUndefined();
    expect(entry.commit.message.trim()).toBe("Initialize repository");
  });

  test("points HEAD at main", async () => {
    const dir = await tempDir();
    await initRepo(dir);

    const head = await fs.promises.readFile(
      path.join(dir, ".git", "HEAD"),
      "utf-8",
    );
    expect(head.trim()).toBe("ref: refs/heads/main");

    const branch = await git.currentBranch({ fs, dir });
    expect(branch).toBe("main");
  });

  test("is idempotent on a directory that already contains a repo", async () => {
    const dir = await tempDir();
    await initRepo(dir);
    const before = await git.log({ fs, dir, depth: 10 });

    await initRepo(dir);
    const after = await git.log({ fs, dir, depth: 10 });

    expect(after.length).toBe(before.length);
    expect(after[0]?.oid).toBe(before[0]?.oid);
  });

  test("writes the default gitignore body when no override is supplied", async () => {
    const dir = await tempDir();
    await initRepo(dir);
    const body = await fs.promises.readFile(
      path.join(dir, ".gitignore"),
      "utf-8",
    );
    expect(body).toBe("keys/\n");
  });
});

describe("initRepo gitignore override", () => {
  test("writes the supplied gitignore body into the genesis tree", async () => {
    const dir = await tempDir();
    const customBody =
      ".DS_Store\n.idea/\nnode_modules/\nkeys/\ndist/\nbuild/\n";
    await initRepo(dir, { gitignore: customBody });

    const onDisk = await fs.promises.readFile(
      path.join(dir, ".gitignore"),
      "utf-8",
    );
    expect(onDisk).toBe(customBody);

    const [entry] = await git.log({ fs, dir, depth: 1 });
    if (entry === undefined) throw new Error("no commit in log");
    const { tree } = await git.readTree({
      fs,
      dir,
      oid: entry.commit.tree,
    });
    const gitignoreEntry = tree.find((e) => e.path === ".gitignore");
    if (gitignoreEntry === undefined) {
      throw new Error(".gitignore not staged in genesis tree");
    }
    const { blob } = await git.readBlob({
      fs,
      dir,
      oid: gitignoreEntry.oid,
    });
    expect(new TextDecoder().decode(blob)).toBe(customBody);
  });
});

describe("initRepo with signing callback", () => {
  test("produces a signed genesis commit authored as interchange-hub", async () => {
    const keyPair = await generateKeyPair();
    const signer: CommitSigner = async (payload) =>
      createSSHSignature(payload, keyPair.privateKey, keyPair.publicKey);

    const dir = await tempDir();
    await initRepo(dir, { signer });

    const [entry] = await git.log({ fs, dir, depth: 1 });
    if (entry === undefined) throw new Error("no commit in log");

    expect(entry.commit.author.name).toBe("interchange-hub");
    expect(entry.commit.author.email).toBe("hub@interchange.local");
    expect(entry.commit.gpgsig).toBeDefined();
  });

  test("the signed genesis verifies against the hub's public key", async () => {
    const keyPair = await generateKeyPair();
    const signer: CommitSigner = async (payload) =>
      createSSHSignature(payload, keyPair.privateKey, keyPair.publicKey);

    const dir = await tempDir();
    await initRepo(dir, { signer });

    const [entry] = await git.log({ fs, dir, depth: 1 });
    if (entry === undefined) throw new Error("no commit in log");

    const signature = entry.commit.gpgsig;
    if (signature === undefined) throw new Error("commit was not signed");

    const { object } = await git.readObject({
      fs,
      dir,
      oid: entry.oid,
      format: "content",
    });
    if (!(object instanceof Uint8Array)) {
      throw new Error("expected raw commit content as Uint8Array");
    }
    const content = new TextDecoder().decode(object);

    const gpgsigIdx = content.indexOf("\ngpgsig ");
    let endIdx = gpgsigIdx + 1;
    while (endIdx < content.length) {
      const nlIdx = content.indexOf("\n", endIdx);
      if (nlIdx === -1) break;
      endIdx = nlIdx + 1;
      if (endIdx < content.length && content[endIdx] !== " ") break;
    }
    const payload =
      content.substring(0, gpgsigIdx) + "\n" + content.substring(endIdx);

    expect(
      await verifySSHSignature(payload, signature, keyPair.publicKey),
    ).toBe(true);
  });

  test("points HEAD at main when signing", async () => {
    const keyPair = await generateKeyPair();
    const signer: CommitSigner = async (payload) =>
      createSSHSignature(payload, keyPair.privateKey, keyPair.publicKey);

    const dir = await tempDir();
    await initRepo(dir, { signer });

    const head = await fs.promises.readFile(
      path.join(dir, ".git", "HEAD"),
      "utf-8",
    );
    expect(head.trim()).toBe("ref: refs/heads/main");
  });

  test("is idempotent on a directory that already contains a signed repo", async () => {
    const keyPair = await generateKeyPair();
    const signer: CommitSigner = async (payload) =>
      createSSHSignature(payload, keyPair.privateKey, keyPair.publicKey);

    const dir = await tempDir();
    await initRepo(dir, { signer });
    const before = await git.log({ fs, dir, depth: 10 });

    await initRepo(dir, { signer });
    const after = await git.log({ fs, dir, depth: 10 });

    expect(after.length).toBe(before.length);
    expect(after[0]?.oid).toBe(before[0]?.oid);
  });
});
