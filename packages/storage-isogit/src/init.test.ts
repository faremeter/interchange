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
import { initRepo } from "./node";
import { initRepo as initRepoOn } from "./init";
import { createNodeIsogitRuntime } from "./node-runtime";
import { normalizeRuntime, promiseFsMethods } from "./runtime";
import { recordingRuntime, treeOf } from "./sync.test-helpers";
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

describe("initRepo durability", () => {
  // What a build that crashed leaves in `building`: its mark, an empty
  // `objects/`, and an empty file for each of `files`.
  async function leaveCrashedBuild(building: string, files: string[]) {
    await fs.promises.mkdir(path.join(building, "objects"), {
      recursive: true,
    });
    await fs.promises.writeFile(
      path.join(building, "intx-init"),
      "@intx/storage-isogit initRepo builds a repository here\n",
    );
    for (const file of files) {
      await fs.promises.writeFile(path.join(building, file), "");
    }
  }

  const syncsAfterRename = (steps: readonly string[]) =>
    steps.slice(steps.findIndex((step) => step.startsWith("rename ")) + 1);

  test("a call after a failed flush flushes the repository it finds", async () => {
    const root = await tempDir();
    const node = createNodeIsogitRuntime();
    const dir = path.join(root, "repo");
    // Counts only flushes after .git exists; the build flushes earlier too.
    let flushes = 0;
    const runtime = normalizeRuntime({
      ...node,
      flush: async () => {
        if (!fs.existsSync(path.join(dir, ".git"))) return;
        flushes += 1;
        if (flushes === 1) throw new Error("flush failed");
      },
    });

    await expect(initRepoOn(runtime, dir)).rejects.toThrow("flush failed");
    await initRepoOn(runtime, dir);

    expect(flushes).toBe(2);
    await initRepoOn(runtime, dir);
    expect(flushes).toBe(2);
  });

  test("on a runtime that only flushes, makes the mark and the build durable before each step that needs them", async () => {
    const root = await tempDir();
    const node = createNodeIsogitRuntime();
    const building = path.join(root, ".git-init");
    await leaveCrashedBuild(building, ["HEAD"]);
    const steps: string[] = [];
    const runtime = normalizeRuntime({
      fs: node.fs,
      path: node.path,
      rename: node.rename,
      flush: async () => {
        steps.push(
          `flush ${fs.existsSync(building) ? (await fs.promises.readdir(building)).sort().join() : "-"}`,
        );
      },
    });

    await initRepoOn(runtime, root);

    expect(steps.slice(0, 3)).toEqual([
      "flush intx-init",
      "flush intx-init",
      expect.stringMatching(/^flush .*HEAD.*intx-init.*objects/),
    ]);
  });

  test("syncs the whole repository before it renames it to .git, and the directories that hold it after", async () => {
    const root = await tempDir();
    const { runtime, steps } = recordingRuntime(root);
    const dir = path.join(root, "repos", "one");

    await initRepoOn(runtime, dir);

    const rename = steps.findIndex((step) => step.startsWith("rename "));
    expect(rename).toBeGreaterThan(0);
    const gitdir = path.join(dir, ".git");
    for (const entry of await treeOf(gitdir, gitdir)) {
      const built = path.join("repos", "one", ".git-init", entry);
      expect(steps.slice(0, rename)).toContain(`sync ${built}`);
    }
    expect(syncsAfterRename(steps).sort()).toEqual(
      ["", "repos", path.join("repos", "one")].map((d) => `sync ${d}`).sort(),
    );
  });

  test("syncs every directory up to the durable root's holder, though an earlier call made them", async () => {
    const root = await tempDir();
    const dataDir = path.join(root, "data");
    const dir = path.join(dataDir, "repos", "one");
    // What a call that crashed before it synced these directories leaves.
    await fs.promises.mkdir(dir, { recursive: true });
    const { runtime, steps } = recordingRuntime(root);

    await initRepoOn(runtime, dir, { durableRoot: dataDir });

    expect(syncsAfterRename(steps).sort()).toEqual(
      [
        "",
        "data",
        path.join("data", "repos"),
        path.join("data", "repos", "one"),
      ]
        .map((d) => `sync ${d}`)
        .sort(),
    );
  });

  test("refuses a durable root that does not hold the repository", async () => {
    const root = await tempDir();

    await expect(
      initRepo(path.join(root, "one"), {
        durableRoot: path.join(root, "other"),
      }),
    ).rejects.toThrow("is not under");
    expect(fs.existsSync(path.join(root, "one"))).toBe(false);
  });

  test("syncs the holders a durable root adds after a call without one", async () => {
    const root = await tempDir();
    const dataDir = path.join(root, "data");
    const dir = path.join(dataDir, "repos", "one");
    await initRepo(dir);
    const { runtime, steps } = recordingRuntime(root);

    await initRepoOn(runtime, dir);
    expect(steps).toEqual([`sync ${path.join("data", "repos", "one")}`]);

    steps.length = 0;
    await initRepoOn(runtime, dir, { durableRoot: dataDir });
    expect(steps.sort()).toEqual(
      [
        "",
        "data",
        path.join("data", "repos"),
        path.join("data", "repos", "one"),
      ]
        .map((d) => `sync ${d}`)
        .sort(),
    );
  });

  test("replaces a .git-init that a crash left", async () => {
    const root = await tempDir();
    await leaveCrashedBuild(path.join(root, ".git-init"), ["HEAD"]);

    await initRepo(root);

    expect(fs.existsSync(path.join(root, ".git-init"))).toBe(false);
    const [entry] = await git.log({ fs, dir: root, depth: 1 });
    expect(entry?.commit.message).toBe("Initialize repository\n");
  });

  test("refuses a .git-init whose cut-short mark has other files beside it", async () => {
    const root = await tempDir();
    const theirs = path.join(root, ".git-init", "notes.txt");
    await fs.promises.mkdir(path.dirname(theirs));
    await fs.promises.writeFile(path.join(root, ".git-init", "intx-init"), "");
    await fs.promises.writeFile(theirs, "not a repository");

    await expect(initRepo(root)).rejects.toThrow(
      "is not a repository that initRepo was building",
    );
    expect(await fs.promises.readFile(theirs, "utf8")).toBe("not a repository");
  });

  test("makes its mark durable before it writes any Git file", async () => {
    const root = await tempDir();
    const node = createNodeIsogitRuntime();
    const building = path.join(root, ".git-init");
    const synced: { path: string; holds: string[] }[] = [];
    const runtime = normalizeRuntime({
      ...node,
      sync: async (p) => {
        synced.push({
          path: path.relative(root, p),
          holds: p === building ? await fs.promises.readdir(p) : [],
        });
        await node.sync?.(p);
      },
    });

    await initRepoOn(runtime, root);

    expect(synced.slice(0, 2)).toEqual([
      { path: path.join(".git-init", "intx-init"), holds: [] },
      { path: ".git-init", holds: ["intx-init"] },
    ]);
  });

  test("removes a crash's .git-init mark last, so a removal that fails part way leaves one the next call replaces", async () => {
    const root = await tempDir();
    const building = path.join(root, ".git-init");
    await leaveCrashedBuild(building, ["left"]);
    const node = createNodeIsogitRuntime();
    const base = promiseFsMethods(node.fs);
    const failure = { unlink: true };
    const steps: string[] = [];
    const runtime = normalizeRuntime({
      ...node,
      fs: {
        promises: {
          ...base,
          // The mark listed first, so a removal in listing order would
          // reach it before the file whose removal fails.
          readdir: async (...args: unknown[]) => {
            const names: unknown = await base.readdir(...args);
            if (String(args[0]) !== building || !Array.isArray(names)) {
              return names;
            }
            return [
              ...names.filter((name) => name === "intx-init"),
              ...names.filter((name) => name !== "intx-init"),
            ];
          },
          unlink: async (...args: unknown[]) => {
            const target = String(args[0]);
            if (failure.unlink && target === path.join(building, "left")) {
              throw new Error("unlink failed");
            }
            await base.unlink(...args);
            steps.push(`unlink ${path.relative(root, target)}`);
          },
        },
      },
      sync: async (p) => {
        if (p === building) {
          steps.push(`sync holding ${(await fs.promises.readdir(p)).join()}`);
        }
        await node.sync?.(p);
      },
    });

    await expect(initRepoOn(runtime, root)).rejects.toThrow("unlink failed");
    expect(fs.existsSync(path.join(building, "intx-init"))).toBe(true);

    failure.unlink = false;
    await initRepoOn(runtime, root);
    const unlinkMark = steps.indexOf(
      `unlink ${path.join(".git-init", "intx-init")}`,
    );
    expect(unlinkMark).toBeGreaterThan(0);
    expect(steps[unlinkMark - 1]).toBe("sync holding intx-init");
    expect(fs.existsSync(building)).toBe(false);
    expect(fs.existsSync(path.join(root, ".git"))).toBe(true);
  });

  test("syncs the holders again for a repository built anew at a path whose build failed after the rename", async () => {
    const root = await tempDir();
    const dir = path.join(root, "repos", "one");
    const { runtime, steps, failing } = recordingRuntime(root);
    await initRepoOn(runtime, dir);
    await fs.promises.rm(path.join(dir, ".git"), { recursive: true });

    failing.sync = (p) => p === dir;
    await expect(initRepoOn(runtime, dir)).rejects.toThrow("sync failed");
    expect(fs.existsSync(path.join(dir, ".git"))).toBe(true);

    failing.sync = () => false;
    steps.length = 0;
    await initRepoOn(runtime, dir);
    expect(steps).toEqual([`sync ${path.join("repos", "one")}`]);
  });

  test("replaces an empty .git-init", async () => {
    const root = await tempDir();
    await fs.promises.mkdir(path.join(root, ".git-init"));

    await initRepo(root);

    expect(fs.existsSync(path.join(root, ".git-init"))).toBe(false);
    expect(fs.existsSync(path.join(root, ".git"))).toBe(true);
  });

  test("refuses a .git-init it did not make, and leaves it", async () => {
    const root = await tempDir();
    const theirs = path.join(root, ".git-init", "notes.txt");
    await fs.promises.mkdir(path.dirname(theirs));
    await fs.promises.writeFile(theirs, "not a repository");

    await expect(initRepo(root)).rejects.toThrow(
      "is not a repository that initRepo was building",
    );
    expect(await fs.promises.readFile(theirs, "utf8")).toBe("not a repository");
    expect(fs.existsSync(path.join(root, ".git"))).toBe(false);
  });

  test("syncs the holders of a repository it finds once, then nothing", async () => {
    const root = await tempDir();
    const dataDir = path.join(root, "data");
    const dir = path.join(dataDir, "repos", "one");
    // Made through another runtime, so this one has no record of syncing
    // its holders.
    await initRepo(dir);
    const { runtime, steps } = recordingRuntime(root);

    await initRepoOn(runtime, dir, { durableRoot: dataDir });
    expect(steps.sort()).toEqual(
      [
        "",
        "data",
        path.join("data", "repos"),
        path.join("data", "repos", "one"),
      ]
        .map((d) => `sync ${d}`)
        .sort(),
    );

    steps.length = 0;
    await initRepoOn(runtime, dir, { durableRoot: dataDir });
    expect(steps).toEqual([]);
  });
});
