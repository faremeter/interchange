import { afterEach, describe, expect, spyOn, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import {
  generateKeyPair,
  createSSHSignature,
  verifySSHSignature,
} from "@intx/crypto";
import { initAgentRepo } from "./node";
import { createIsogitStorage } from "./index";
import {
  applyPack,
  removeStagedPacks,
  stagePack,
  type CommitVerifier,
  type StagedPack,
} from "./node";
import { collectReachableObjects } from "./node";
import { createNodeIsogitRuntime } from "./node-runtime";

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

async function createPackFromRepo(
  sourceDir: string,
  oids: string[],
): Promise<Uint8Array> {
  const result = await git.packObjects({
    fs,
    dir: sourceDir,
    oids,
    write: false,
  });
  if (result.packfile === undefined) {
    throw new Error("packObjects returned no packfile");
  }
  return result.packfile;
}

async function makeSourceRepo(): Promise<{
  dir: string;
  commitSha: string;
  oids: string[];
}> {
  const dir = await tempDir();
  await git.init({ fs, dir, defaultBranch: "main" });

  const filePath = path.join(dir, "deploy", "prompt.txt");
  await fs.promises.mkdir(path.join(dir, "deploy"), { recursive: true });
  await fs.promises.writeFile(filePath, "You are a helpful agent.");
  await git.add({ fs, dir, filepath: "deploy/prompt.txt" });

  const commitSha = await git.commit({
    fs,
    dir,
    message: "Initial deploy",
    author: { name: "Test", email: "test@test.dev" },
  });

  const walkResult = await git.log({ fs, dir, depth: 1 });
  const entry = walkResult[0];
  if (entry === undefined) throw new Error("no commit");

  const oids = await collectReachableObjects(dir, commitSha);
  return { dir, commitSha, oids };
}

describe("applyPack", () => {
  test("applies a packfile and updates the ref", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await applyPack(
      targetDir,
      pack,
      "refs/heads/deploy",
      source.commitSha,
      "test-transfer-1",
    );

    const resolved = await git.resolveRef({
      fs,
      dir: targetDir,
      ref: "refs/heads/deploy",
    });
    expect(resolved).toBe(source.commitSha);
  });

  test("retains pack and index in objects/pack after success", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await applyPack(
      targetDir,
      pack,
      "refs/heads/deploy",
      source.commitSha,
      "cleanup-test",
    );

    const packPath = path.join(
      targetDir,
      ".git",
      "objects",
      "pack",
      "pack-recv-cleanup-test.pack",
    );
    const idxPath = path.join(
      targetDir,
      ".git",
      "objects",
      "pack",
      "pack-recv-cleanup-test.idx",
    );

    // Pack and index are retained so git can read objects from them.
    await fs.promises.access(packPath);
    await fs.promises.access(idxPath);
  });

  test("checks out files to the working tree", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await applyPack(
      targetDir,
      pack,
      "refs/heads/deploy",
      source.commitSha,
      "checkout-test",
    );

    const content = await fs.promises.readFile(
      path.join(targetDir, "deploy", "prompt.txt"),
      "utf-8",
    );
    expect(content).toBe("You are a helpful agent.");
  });

  test("preserves executable mode on checked-out files", async () => {
    const sourceDir = await tempDir();
    await git.init({ fs, dir: sourceDir, defaultBranch: "main" });

    const scriptPath = path.join(sourceDir, "deploy", "run.sh");
    await fs.promises.mkdir(path.join(sourceDir, "deploy"), {
      recursive: true,
    });
    await fs.promises.writeFile(scriptPath, "#!/bin/sh\necho hello\n", {
      mode: 0o755,
    });
    await git.add({ fs, dir: sourceDir, filepath: "deploy/run.sh" });

    const commitSha = await git.commit({
      fs,
      dir: sourceDir,
      message: "Add executable script",
      author: { name: "Test", email: "test@test.dev" },
    });

    const oids = await collectReachableObjects(sourceDir, commitSha);
    const pack = await createPackFromRepo(sourceDir, oids);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await applyPack(
      targetDir,
      pack,
      "refs/heads/deploy",
      commitSha,
      "exec-test",
    );

    const stat = await fs.promises.stat(
      path.join(targetDir, "deploy", "run.sh"),
    );
    const isExecutable = (stat.mode & 0o111) !== 0;
    expect(isExecutable).toBe(true);
  });

  test("removes stale files when a second deploy drops content", async () => {
    // First commit: two state subtrees (turns + responses).
    const sourceDir = await tempDir();
    await git.init({ fs, dir: sourceDir, defaultBranch: "main" });

    const turnsDir = path.join(sourceDir, "state", "turns");
    const responsesDir = path.join(sourceDir, "state", "responses");
    await fs.promises.mkdir(turnsDir, { recursive: true });
    await fs.promises.mkdir(responsesDir, { recursive: true });
    await fs.promises.writeFile(
      path.join(turnsDir, "turn-1.json"),
      '{"id":"turn-1"}',
    );
    await fs.promises.writeFile(
      path.join(responsesDir, "response-1.json"),
      '{"id":"response-1"}',
    );
    await git.add({
      fs,
      dir: sourceDir,
      filepath: "state/turns/turn-1.json",
    });
    await git.add({
      fs,
      dir: sourceDir,
      filepath: "state/responses/response-1.json",
    });
    const sha1 = await git.commit({
      fs,
      dir: sourceDir,
      message: "Two state subtrees",
      author: { name: "Test", email: "test@test.dev" },
    });

    const oids1 = await collectReachableObjects(sourceDir, sha1);
    const pack1 = await createPackFromRepo(sourceDir, oids1);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await applyPack(targetDir, pack1, "refs/heads/deploy", sha1, "deploy-v1");

    // Verify both subtrees exist after first deploy.
    await fs.promises.access(
      path.join(targetDir, "state", "responses", "response-1.json"),
    );

    // Second commit: remove responses, keep turns.
    await fs.promises.rm(responsesDir, { recursive: true });
    await git.remove({
      fs,
      dir: sourceDir,
      filepath: "state/responses/response-1.json",
    });
    const sha2 = await git.commit({
      fs,
      dir: sourceDir,
      message: "Remove responses",
      author: { name: "Test", email: "test@test.dev" },
    });

    const oids2 = await collectReachableObjects(sourceDir, sha2);
    const pack2 = await createPackFromRepo(sourceDir, oids2);

    await applyPack(targetDir, pack2, "refs/heads/deploy", sha2, "deploy-v2");

    // Turns should still exist.
    const turnContent = await fs.promises.readFile(
      path.join(targetDir, "state", "turns", "turn-1.json"),
      "utf-8",
    );
    expect(turnContent).toBe('{"id":"turn-1"}');

    // Responses should be gone — stale files must not linger.
    const responsesExist = await fs.promises
      .access(path.join(targetDir, "state", "responses", "response-1.json"))
      .then(() => true)
      .catch(() => false);
    expect(responsesExist).toBe(false);
  });

  test("removes stale top-level directories absent from new tree", async () => {
    // First commit: deploy/ and config/ at the top level.
    const sourceDir = await tempDir();
    await git.init({ fs, dir: sourceDir, defaultBranch: "main" });

    await fs.promises.mkdir(path.join(sourceDir, "deploy"), {
      recursive: true,
    });
    await fs.promises.mkdir(path.join(sourceDir, "config"), {
      recursive: true,
    });
    await fs.promises.writeFile(
      path.join(sourceDir, "deploy", "prompt.md"),
      "hello",
    );
    await fs.promises.writeFile(
      path.join(sourceDir, "config", "settings.json"),
      "{}",
    );
    await git.add({ fs, dir: sourceDir, filepath: "deploy/prompt.md" });
    await git.add({ fs, dir: sourceDir, filepath: "config/settings.json" });
    const sha1 = await git.commit({
      fs,
      dir: sourceDir,
      message: "Deploy and config",
      author: { name: "Test", email: "test@test.dev" },
    });

    const oids1 = await collectReachableObjects(sourceDir, sha1);
    const pack1 = await createPackFromRepo(sourceDir, oids1);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await applyPack(targetDir, pack1, "refs/heads/deploy", sha1, "tld-v1");

    // Verify both top-level dirs exist.
    await fs.promises.access(path.join(targetDir, "config", "settings.json"));

    // Second commit: remove config/ entirely.
    await fs.promises.rm(path.join(sourceDir, "config"), { recursive: true });
    await git.remove({
      fs,
      dir: sourceDir,
      filepath: "config/settings.json",
    });
    const sha2 = await git.commit({
      fs,
      dir: sourceDir,
      message: "Remove config",
      author: { name: "Test", email: "test@test.dev" },
    });

    const oids2 = await collectReachableObjects(sourceDir, sha2);
    const pack2 = await createPackFromRepo(sourceDir, oids2);

    await applyPack(targetDir, pack2, "refs/heads/deploy", sha2, "tld-v2");

    // deploy/ should still exist.
    const promptContent = await fs.promises.readFile(
      path.join(targetDir, "deploy", "prompt.md"),
      "utf-8",
    );
    expect(promptContent).toBe("hello");

    // config/ should be gone.
    const configExists = await fs.promises
      .access(path.join(targetDir, "config"))
      .then(() => true)
      .catch(() => false);
    expect(configExists).toBe(false);
  });

  test("throws on sha mismatch", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await expect(
      applyPack(
        targetDir,
        pack,
        "refs/heads/deploy",
        "0000000000000000000000000000000000000000",
        "mismatch-test",
      ),
    ).rejects.toThrow("sha_mismatch");
  });

  test("cleans up pack files after failure", async () => {
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    // Write garbage as a pack — indexPack will fail
    const garbagePack = new Uint8Array([1, 2, 3, 4]);

    await expect(
      applyPack(
        targetDir,
        garbagePack,
        "refs/heads/deploy",
        "abc123",
        "fail-cleanup",
      ),
    ).rejects.toThrow();

    const packPath = path.join(
      targetDir,
      ".git",
      "objects",
      "pack",
      "pack-recv-fail-cleanup.pack",
    );
    const idxPath = path.join(
      targetDir,
      ".git",
      "objects",
      "pack",
      "pack-recv-fail-cleanup.idx",
    );
    await expect(fs.promises.access(packPath)).rejects.toThrow();
    await expect(fs.promises.access(idxPath)).rejects.toThrow();
  });
});

describe("stagePack", () => {
  function stagingDirOf(targetDir: string, transferId: string): string {
    return path.join(targetDir, ".git", "objects", "pack-staging", transferId);
  }

  async function packDirEntries(targetDir: string): Promise<string[]> {
    return fs.promises.readdir(path.join(targetDir, ".git", "objects", "pack"));
  }

  test("the view reads the staged objects and the repo's own fs does not", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const staged = await stagePack(targetDir, pack, "view");
    try {
      expect(staged.oids).toContain(source.commitSha);
      const viaView = await git.readCommit({
        fs: staged.fs,
        dir: targetDir,
        oid: source.commitSha,
      });
      expect(viaView.oid).toBe(source.commitSha);
      await expect(
        git.readCommit({ fs, dir: targetDir, oid: source.commitSha }),
      ).rejects.toThrow();
      expect(await packDirEntries(targetDir)).toEqual([]);
    } finally {
      await staged.discard();
    }
  });

  test("the view finds the staged pack through a relative gitdir", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const staged = await stagePack(targetDir, pack, "relative");
    try {
      const viaView = await git.readCommit({
        fs: staged.fs,
        gitdir: path.relative(process.cwd(), path.join(targetDir, ".git")),
        oid: source.commitSha,
      });
      expect(viaView.oid).toBe(source.commitSha);
    } finally {
      await staged.discard();
    }
  });

  test("publish makes the objects readable through the repo's own fs", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const staged = await stagePack(targetDir, pack, "publish");
    await staged.publish();

    const commit = await git.readCommit({
      fs,
      dir: targetDir,
      oid: source.commitSha,
    });
    expect(commit.oid).toBe(source.commitSha);
    expect((await packDirEntries(targetDir)).sort()).toEqual([
      "pack-recv-publish.idx",
      "pack-recv-publish.pack",
    ]);
    await expect(
      fs.promises.access(stagingDirOf(targetDir, "publish")),
    ).rejects.toThrow();
    // The view passes through after publish, so it lists the pack once.
    const viaView = await git.readCommit({
      fs: staged.fs,
      dir: targetDir,
      oid: source.commitSha,
    });
    expect(viaView.oid).toBe(source.commitSha);
  });

  test("a cache that loaded the staged pack through the view reuses it after publish", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);
    const cache = {};

    const staged = await stagePack(targetDir, pack, "cache-key");
    await git.readCommit({
      fs: staged.fs,
      dir: targetDir,
      cache,
      oid: source.commitSha,
    });
    await staged.publish();

    const packDir = path.join(targetDir, ".git", "objects", "pack");
    const spy = spyOn(fs.promises, "readFile");
    try {
      await git.readCommit({
        fs,
        dir: targetDir,
        cache,
        oid: source.commitSha,
      });
      const packReads = spy.mock.calls.filter(([target]) =>
        String(target).startsWith(packDir),
      );
      expect(packReads).toEqual([]);
    } finally {
      spy.mockRestore();
    }
  });

  test("discard leaves no staging directory and nothing in objects/pack", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const staged = await stagePack(targetDir, pack, "discard");
    await staged.discard();
    await staged.discard();

    await expect(
      fs.promises.access(stagingDirOf(targetDir, "discard")),
    ).rejects.toThrow();
    expect(await packDirEntries(targetDir)).toEqual([]);
    await expect(
      git.readCommit({ fs: staged.fs, dir: targetDir, oid: source.commitSha }),
    ).rejects.toThrow();
    await expect(staged.publish()).rejects.toThrow(/cannot be published/);
    expect(await packDirEntries(targetDir)).toEqual([]);
  });

  test("discard after publish leaves the published pack in place", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const staged = await stagePack(targetDir, pack, "published");
    await staged.publish();
    await staged.discard();

    expect((await packDirEntries(targetDir)).sort()).toEqual([
      "pack-recv-published.idx",
      "pack-recv-published.pack",
    ]);
    expect(
      (await git.readCommit({ fs, dir: targetDir, oid: source.commitSha })).oid,
    ).toBe(source.commitSha);
  });

  test("removeStagedPacks removes every staged pack", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await stagePack(targetDir, pack, "left-one");
    await stagePack(targetDir, pack, "left-two");
    await removeStagedPacks(targetDir);

    await expect(
      fs.promises.access(
        path.join(targetDir, ".git", "objects", "pack-staging"),
      ),
    ).rejects.toThrow();
    expect(await packDirEntries(targetDir)).toEqual([]);
  });

  test("publish renames the completed pack before its index", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const staged = await stagePack(targetDir, pack, "rename-order");
    const originalRename = fs.promises.rename.bind(fs.promises);
    const renames: { from: string; to: string }[] = [];
    const renameSpy = spyOn(fs.promises, "rename").mockImplementation(
      async (from, to) => {
        await originalRename(from, to);
        renames.push({ from: from.toString(), to: to.toString() });
      },
    );

    try {
      await staged.publish();
    } finally {
      renameSpy.mockRestore();
    }

    const finalPack = path.join(
      targetDir,
      ".git",
      "objects",
      "pack",
      "pack-recv-rename-order.pack",
    );
    const finalIdx = finalPack.replace(/\.pack$/, ".idx");
    expect(renames).toEqual([
      {
        from: path.join(stagingDirOf(targetDir, "rename-order"), "pack.pack"),
        to: finalPack,
      },
      {
        from: path.join(stagingDirOf(targetDir, "rename-order"), "pack.idx"),
        to: finalIdx,
      },
    ]);
    await fs.promises.access(finalPack);
    await fs.promises.access(finalIdx);
    await expect(
      fs.promises.access(stagingDirOf(targetDir, "rename-order")),
    ).rejects.toThrow();
  });

  test.each(["pack", "idx"] as const)(
    "does not overwrite an existing final %s",
    async (extension) => {
      const source = await makeSourceRepo();
      const pack = await createPackFromRepo(source.dir, source.oids);
      const targetDir = await tempDir();
      await initAgentRepo(targetDir);

      const packDir = path.join(targetDir, ".git", "objects", "pack");
      const existingPath = path.join(
        packDir,
        `pack-recv-duplicate.${extension}`,
      );
      await fs.promises.mkdir(packDir, { recursive: true });
      await fs.promises.writeFile(existingPath, "existing");

      await expect(stagePack(targetDir, pack, "duplicate")).rejects.toThrow(
        /already published/,
      );
      expect(await fs.promises.readFile(existingPath, "utf8")).toBe("existing");
    },
  );

  test("rejects concurrent stages using the same transfer ID", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const originalIndexPack = git.indexPack.bind(git);
    let signalIndexingStarted: () => void = () => undefined;
    const indexingStarted = new Promise<void>((resolve) => {
      signalIndexingStarted = resolve;
    });
    let releaseIndexing: () => void = () => undefined;
    const indexingReleased = new Promise<void>((resolve) => {
      releaseIndexing = resolve;
    });
    let blockNextIndex = true;
    const indexPackSpy = spyOn(git, "indexPack").mockImplementation(
      async (args) => {
        if (blockNextIndex) {
          blockNextIndex = false;
          signalIndexingStarted();
          await indexingReleased;
        }
        return originalIndexPack(args);
      },
    );

    let firstStage: Promise<StagedPack> | undefined;
    try {
      firstStage = stagePack(targetDir, pack, "concurrent");
      await indexingStarted;

      await expect(stagePack(targetDir, pack, "concurrent")).rejects.toThrow(
        /already published or in progress/,
      );
      expect(indexPackSpy).toHaveBeenCalledTimes(1);

      releaseIndexing();
      await (await firstStage).publish();
    } finally {
      releaseIndexing();
      await firstStage?.catch(() => undefined);
      indexPackSpy.mockRestore();
    }
  });

  test("cleans up when index publication fails after the pack rename", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const staged = await stagePack(targetDir, pack, "partial");
    const originalRename = fs.promises.rename.bind(fs.promises);
    let renameCount = 0;
    const renameSpy = spyOn(fs.promises, "rename").mockImplementation(
      async (from, to) => {
        renameCount += 1;
        if (renameCount === 2) throw new Error("index publish failed");
        await originalRename(from, to);
      },
    );

    try {
      await expect(staged.publish()).rejects.toThrow("index publish failed");
    } finally {
      renameSpy.mockRestore();
    }

    const finalPack = path.join(
      targetDir,
      ".git",
      "objects",
      "pack",
      "pack-recv-partial.pack",
    );
    await expect(fs.promises.access(finalPack)).rejects.toThrow();
    await expect(
      fs.promises.access(finalPack.replace(/\.pack$/, ".idx")),
    ).rejects.toThrow();
    await expect(
      fs.promises.access(stagingDirOf(targetDir, "partial")),
    ).rejects.toThrow();
  });
});

async function makeSignedSourceRepo(keyPair: {
  privateKey: Uint8Array;
  publicKey: Uint8Array;
}): Promise<{
  dir: string;
  commitSha: string;
  oids: string[];
}> {
  const dir = await tempDir();
  await git.init({ fs, dir, defaultBranch: "main" });

  const filePath = path.join(dir, "deploy", "prompt.txt");
  await fs.promises.mkdir(path.join(dir, "deploy"), { recursive: true });
  await fs.promises.writeFile(filePath, "You are a helpful agent.");
  await git.add({ fs, dir, filepath: "deploy/prompt.txt" });

  const commitSha = await git.commit({
    fs,
    dir,
    message: "Signed deploy",
    author: { name: "Test", email: "test@test.dev" },
    signingKey: "sshsig",
    onSign: async ({ payload }) => ({
      signature: await createSSHSignature(
        payload,
        keyPair.privateKey,
        keyPair.publicKey,
      ),
    }),
  });

  const oids = await collectReachableObjects(dir, commitSha);
  return { dir, commitSha, oids };
}

describe("applyPack signature verification", () => {
  test("accepts a correctly signed pack", async () => {
    const keyPair = await generateKeyPair();
    const source = await makeSignedSourceRepo(keyPair);
    const pack = await createPackFromRepo(source.dir, source.oids);

    const verifier: CommitVerifier = (payload, signature) =>
      verifySSHSignature(payload, signature, keyPair.publicKey);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await applyPack(
      targetDir,
      pack,
      "refs/heads/deploy",
      source.commitSha,
      "signed-ok",
      verifier,
    );

    const resolved = await git.resolveRef({
      fs,
      dir: targetDir,
      ref: "refs/heads/deploy",
    });
    expect(resolved).toBe(source.commitSha);
  });

  test("rejects a pack signed with the wrong key", async () => {
    const signerKey = await generateKeyPair();
    const verifierKey = await generateKeyPair();
    const source = await makeSignedSourceRepo(signerKey);
    const pack = await createPackFromRepo(source.dir, source.oids);

    const verifier: CommitVerifier = (payload, signature) =>
      verifySSHSignature(payload, signature, verifierKey.publicKey);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await expect(
      applyPack(
        targetDir,
        pack,
        "refs/heads/deploy",
        source.commitSha,
        "wrong-key",
        verifier,
      ),
    ).rejects.toThrow("signature_invalid");
  });

  test("rejects an unsigned commit when verifier is provided", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);

    const keyPair = await generateKeyPair();
    const verifier: CommitVerifier = (payload, signature) =>
      verifySSHSignature(payload, signature, keyPair.publicKey);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await expect(
      applyPack(
        targetDir,
        pack,
        "refs/heads/deploy",
        source.commitSha,
        "unsigned",
        verifier,
      ),
    ).rejects.toThrow("signature_unsigned");
  });

  test("skips verification when no verifier is provided", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await applyPack(
      targetDir,
      pack,
      "refs/heads/deploy",
      source.commitSha,
      "no-verify",
    );

    const resolved = await git.resolveRef({
      fs,
      dir: targetDir,
      ref: "refs/heads/deploy",
    });
    expect(resolved).toBe(source.commitSha);
  });
});

// Helpers used by the rejection and flush tests below. The pack
// filename layout is derived from `transferId` per
// `publishedPackPaths` in pack-receive.ts; replicate the derivation
// here so the assertions stay decoupled from that helper's accessors
// (no internals exported).
function publishedPackFile(targetDir: string, transferId: string): string {
  return path.join(
    targetDir,
    ".git",
    "objects",
    "pack",
    `pack-recv-${transferId}.pack`,
  );
}

function publishedIdxFile(targetDir: string, transferId: string): string {
  return path.join(
    targetDir,
    ".git",
    "objects",
    "pack",
    `pack-recv-${transferId}.idx`,
  );
}

async function fileExists(p: string): Promise<boolean> {
  try {
    await fs.promises.access(p);
    return true;
  } catch {
    return false;
  }
}

function createStorageFailingPromotionFlush(
  targetDir: string,
  ref: string,
  expectedSha: string,
) {
  let failureInjected = false;
  return createIsogitStorage({
    ...createNodeIsogitRuntime(),
    flush: async () => {
      const currentSha = await git
        .resolveRef({ fs, dir: targetDir, ref })
        .catch(() => null);
      if (!failureInjected && currentSha === expectedSha) {
        failureInjected = true;
        throw new Error("injected post-promotion flush failure");
      }
    },
  });
}

describe("post-promotion flush failures", () => {
  test("apply retains the durable pack and materialized tree for the promoted ref", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    const ref = "refs/heads/deploy";
    const transferId = "apply-flush-failure";
    const storage = createStorageFailingPromotionFlush(
      targetDir,
      ref,
      source.commitSha,
    );
    await storage.initAgentRepo(targetDir);

    await expect(
      storage.applyPack(targetDir, pack, ref, source.commitSha, transferId),
    ).rejects.toThrow("injected post-promotion flush failure");

    expect(await fileExists(publishedPackFile(targetDir, transferId))).toBe(
      true,
    );
    expect(await fileExists(publishedIdxFile(targetDir, transferId))).toBe(
      true,
    );
    expect(await git.resolveRef({ fs, dir: targetDir, ref })).toBe(
      source.commitSha,
    );
    expect(
      await fs.promises.readFile(
        path.join(targetDir, "deploy", "prompt.txt"),
        "utf8",
      ),
    ).toBe("You are a helpful agent.");
  });
});

describe("applyPack keeps a rejected pack out of objects/pack", () => {
  test("sha_mismatch publishes nothing and leaves no staged pack", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);

    const targetDir = await tempDir();
    let flushes = 0;
    const storage = createIsogitStorage({
      ...createNodeIsogitRuntime(),
      flush: () => {
        flushes += 1;
        return Promise.resolve();
      },
    });
    await storage.initAgentRepo(targetDir);
    const flushesBefore = flushes;

    const bogusExpected = "0".repeat(40);
    await expect(
      storage.applyPack(
        targetDir,
        pack,
        "refs/heads/deploy",
        bogusExpected,
        "apply-rejected-sha",
      ),
    ).rejects.toThrow("sha_mismatch");

    expect(
      await fileExists(publishedPackFile(targetDir, "apply-rejected-sha")),
    ).toBe(false);
    expect(
      await fileExists(publishedIdxFile(targetDir, "apply-rejected-sha")),
    ).toBe(false);
    expect(
      await fileExists(
        path.join(
          targetDir,
          ".git",
          "objects",
          "pack-staging",
          "apply-rejected-sha",
        ),
      ),
    ).toBe(false);
    // Nothing reached objects/pack, so there is nothing to make durable.
    expect(flushes).toBe(flushesBefore);
  });
});
