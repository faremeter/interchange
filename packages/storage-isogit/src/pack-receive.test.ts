import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import {
  generateKeyPair,
  createSshSignature,
  verifySshSignature,
} from "@intx/crypto-node";
import { initAgentRepo } from "./init";
import {
  applyPack,
  receivePackObjects,
  type CommitVerifier,
  type TreeValidator,
} from "./pack-receive";
import { collectReachableObjects } from "./object-walk";

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
    // First commit: two skills (greet + farewell).
    const sourceDir = await tempDir();
    await git.init({ fs, dir: sourceDir, defaultBranch: "main" });

    const greetDir = path.join(sourceDir, "deploy", "skills", "greet");
    const farewellDir = path.join(sourceDir, "deploy", "skills", "farewell");
    await fs.promises.mkdir(greetDir, { recursive: true });
    await fs.promises.mkdir(farewellDir, { recursive: true });
    await fs.promises.writeFile(
      path.join(greetDir, "tool.json"),
      '{"name":"greet"}',
    );
    await fs.promises.writeFile(
      path.join(farewellDir, "tool.json"),
      '{"name":"farewell"}',
    );
    await git.add({
      fs,
      dir: sourceDir,
      filepath: "deploy/skills/greet/tool.json",
    });
    await git.add({
      fs,
      dir: sourceDir,
      filepath: "deploy/skills/farewell/tool.json",
    });
    const sha1 = await git.commit({
      fs,
      dir: sourceDir,
      message: "Two skills",
      author: { name: "Test", email: "test@test.dev" },
    });

    const oids1 = await collectReachableObjects(sourceDir, sha1);
    const pack1 = await createPackFromRepo(sourceDir, oids1);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await applyPack(targetDir, pack1, "refs/heads/deploy", sha1, "deploy-v1");

    // Verify both skills exist after first deploy.
    await fs.promises.access(
      path.join(targetDir, "deploy", "skills", "farewell", "tool.json"),
    );

    // Second commit: remove farewell, keep greet.
    await fs.promises.rm(farewellDir, { recursive: true });
    await git.remove({
      fs,
      dir: sourceDir,
      filepath: "deploy/skills/farewell/tool.json",
    });
    const sha2 = await git.commit({
      fs,
      dir: sourceDir,
      message: "Remove farewell",
      author: { name: "Test", email: "test@test.dev" },
    });

    const oids2 = await collectReachableObjects(sourceDir, sha2);
    const pack2 = await createPackFromRepo(sourceDir, oids2);

    await applyPack(targetDir, pack2, "refs/heads/deploy", sha2, "deploy-v2");

    // Greet should still exist.
    const greetContent = await fs.promises.readFile(
      path.join(targetDir, "deploy", "skills", "greet", "tool.json"),
      "utf-8",
    );
    expect(greetContent).toBe('{"name":"greet"}');

    // Farewell should be gone — stale files must not linger.
    const farewellExists = await fs.promises
      .access(path.join(targetDir, "deploy", "skills", "farewell", "tool.json"))
      .then(() => true)
      .catch(() => false);
    expect(farewellExists).toBe(false);
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
      signature: createSshSignature(
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
      verifySshSignature(payload, signature, keyPair.publicKey);

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
      verifySshSignature(payload, signature, verifierKey.publicKey);

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
      verifySshSignature(payload, signature, keyPair.publicKey);

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

async function makeRepoWithPaths(
  paths: { filepath: string; content: string }[],
): Promise<{
  dir: string;
  commitSha: string;
  oids: string[];
}> {
  const dir = await tempDir();
  await git.init({ fs, dir, defaultBranch: "main" });

  for (const { filepath, content } of paths) {
    const fullPath = path.join(dir, filepath);
    await fs.promises.mkdir(path.dirname(fullPath), { recursive: true });
    await fs.promises.writeFile(fullPath, content);
    await git.add({ fs, dir, filepath });
  }

  const commitSha = await git.commit({
    fs,
    dir,
    message: "Test tree",
    author: { name: "Test", email: "test@test.dev" },
  });

  const oids = await collectReachableObjects(dir, commitSha);
  return { dir, commitSha, oids };
}

describe("receivePackObjects tree validation", () => {
  test("accepts a state-only tree when validator requires state", async () => {
    const source = await makeRepoWithPaths([
      { filepath: "state/turns.jsonl", content: "" },
    ]);
    const pack = await createPackFromRepo(source.dir, source.oids);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const validator: TreeValidator = (paths) =>
      paths.every((p) => p === "state");

    await receivePackObjects(
      targetDir,
      pack,
      "refs/heads/state",
      source.commitSha,
      "state-ok",
      validator,
    );

    const resolved = await git.resolveRef({
      fs,
      dir: targetDir,
      ref: "refs/heads/state",
    });
    expect(resolved).toBe(source.commitSha);
  });

  test("rejects a tree with deploy/ when validator requires state only", async () => {
    const source = await makeRepoWithPaths([
      { filepath: "state/turns.jsonl", content: "" },
      { filepath: "deploy/prompt.md", content: "evil" },
    ]);
    const pack = await createPackFromRepo(source.dir, source.oids);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const validator: TreeValidator = (paths) =>
      paths.every((p) => p === "state");

    await expect(
      receivePackObjects(
        targetDir,
        pack,
        "refs/heads/state",
        source.commitSha,
        "state-bad",
        validator,
      ),
    ).rejects.toThrow("path_violation");
  });

  test("accepts any tree when no validator is provided", async () => {
    const source = await makeRepoWithPaths([
      { filepath: "state/turns.jsonl", content: "" },
      { filepath: "deploy/prompt.md", content: "anything" },
    ]);
    const pack = await createPackFromRepo(source.dir, source.oids);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await receivePackObjects(
      targetDir,
      pack,
      "refs/heads/mixed",
      source.commitSha,
      "no-validate",
    );

    const resolved = await git.resolveRef({
      fs,
      dir: targetDir,
      ref: "refs/heads/mixed",
    });
    expect(resolved).toBe(source.commitSha);
  });
});
