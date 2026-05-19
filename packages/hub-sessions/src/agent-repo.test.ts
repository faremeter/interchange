import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { generateKeyPair } from "@intx/crypto-node";
import { createDeployPack } from "@intx/storage-isogit";
import { createAgentRepoStore } from "./agent-repo";
import type { KeyPair } from "@intx/types/runtime";

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

let signingKey: KeyPair;

beforeAll(async () => {
  signingKey = await generateKeyPair();
});

afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fs.promises.rm(d, { recursive: true, force: true }).catch((_e) => {
      /* best effort cleanup */
    });
  }
});

describe("AgentRepoStore", () => {
  test("writeDeployTree creates a commit with prompt and skills", async () => {
    const dataDir = await makeTempDir("agent-repo-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    const { commitSha } = await store.writeDeployTree("agent-1", {
      systemPrompt: "You are a test agent.",
      skills: [
        {
          name: "greet",
          definition: {
            name: "greet",
            description: "Greet someone",
            inputSchema: { type: "object", properties: {} },
          },
        },
      ],
    });

    expect(commitSha).toMatch(/^[0-9a-f]{40}$/);

    const repoDir = path.join(dataDir, "agents", "agent-1");
    const prompt = await fs.promises.readFile(
      path.join(repoDir, "deploy", "prompt.md"),
      "utf-8",
    );
    expect(prompt).toBe("You are a test agent.");

    const toolJson = await fs.promises.readFile(
      path.join(repoDir, "deploy", "skills", "greet", "tool.json"),
      "utf-8",
    );
    const toolParsed = JSON.parse(toolJson);
    if (
      toolParsed === null ||
      typeof toolParsed !== "object" ||
      !("name" in toolParsed) ||
      typeof toolParsed.name !== "string"
    ) {
      throw new Error("tool.json missing expected name field");
    }
    expect(toolParsed.name).toBe("greet");

    const ref = await git.resolveRef({
      fs,
      dir: repoDir,
      ref: "refs/heads/deploy",
    });
    expect(ref).toBe(commitSha);
  });

  test("writeDeployTree removes stale skills from the index", async () => {
    const dataDir = await makeTempDir("agent-repo-stale-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    await store.writeDeployTree("agent-stale", {
      systemPrompt: "V1",
      skills: [
        { name: "old-skill", definition: { name: "old-skill" } },
        { name: "keep-skill", definition: { name: "keep-skill" } },
      ],
    });

    const { commitSha } = await store.writeDeployTree("agent-stale", {
      systemPrompt: "V2",
      skills: [{ name: "keep-skill", definition: { name: "keep-skill" } }],
    });

    const repoDir = path.join(dataDir, "agents", "agent-stale");
    const { commit } = await git.readCommit({
      fs,
      dir: repoDir,
      oid: commitSha,
    });
    const { tree } = await git.readTree({ fs, dir: repoDir, oid: commit.tree });

    const deployEntry = tree.find((e) => e.path === "deploy");
    expect(deployEntry).toBeDefined();
    if (!deployEntry) throw new Error("unreachable");

    const { tree: deployTree } = await git.readTree({
      fs,
      dir: repoDir,
      oid: deployEntry.oid,
    });
    const skillsEntry = deployTree.find((e) => e.path === "skills");
    expect(skillsEntry).toBeDefined();
    if (!skillsEntry) throw new Error("unreachable");

    const { tree: skillsTree } = await git.readTree({
      fs,
      dir: repoDir,
      oid: skillsEntry.oid,
    });
    const skillNames = skillsTree.map((e) => e.path);
    expect(skillNames).toContain("keep-skill");
    expect(skillNames).not.toContain("old-skill");
  });

  test("writeDeployTree does not advance refs/heads/main", async () => {
    const dataDir = await makeTempDir("agent-repo-ref-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    await store.writeDeployTree("agent-ref", {
      systemPrompt: "Ref test.",
      skills: [],
    });

    const repoDir = path.join(dataDir, "agents", "agent-ref");
    const mainLog = await git.log({ fs, dir: repoDir, ref: "refs/heads/main" });
    const deployLog = await git.log({
      fs,
      dir: repoDir,
      ref: "refs/heads/deploy",
    });

    // main should only have the init commit
    expect(mainLog.length).toBe(1);
    // deploy should have 2: init parent + deploy commit
    expect(deployLog.length).toBe(2);
  });

  test("createDeployPack produces a valid packfile", async () => {
    const dataDir = await makeTempDir("agent-repo-pack-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    await store.writeDeployTree("agent-2", {
      systemPrompt: "Pack test.",
      skills: [],
    });

    const { pack, commitSha, ref } = await store.createDeployPack("agent-2");

    expect(pack).toBeInstanceOf(Uint8Array);
    expect(pack.length).toBeGreaterThan(0);
    expect(commitSha).toMatch(/^[0-9a-f]{40}$/);
    expect(ref).toBe("refs/heads/deploy");
  });

  test("receiveStatePack indexes objects and updates ref", async () => {
    const dataDir = await makeTempDir("agent-repo-state-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    await store.writeDeployTree("agent-3", {
      systemPrompt: "State test.",
      skills: [],
    });

    const sourceDir = await makeTempDir("state-source-");
    await git.init({ fs, dir: sourceDir, defaultBranch: "main" });
    await fs.promises.mkdir(path.join(sourceDir, "state"), { recursive: true });
    await fs.promises.writeFile(
      path.join(sourceDir, "state", "turns.jsonl"),
      '{"messages":[]}',
    );
    await git.add({ fs, dir: sourceDir, filepath: "state/turns.jsonl" });
    const stateCommit = await git.commit({
      fs,
      dir: sourceDir,
      message: "State snapshot",
      author: { name: "test", email: "test@test" },
    });

    const { pack } = await createDeployPack(sourceDir, "refs/heads/main");

    const stateRef = "refs/instances/test-instance";
    await store.receiveStatePack("agent-3", pack, stateRef, stateCommit);

    const repoDir = path.join(dataDir, "agents", "agent-3");
    const resolved = await git.resolveRef({
      fs,
      dir: repoDir,
      ref: stateRef,
    });
    expect(resolved).toBe(stateCommit);
  });

  test("receiveStatePack accepts packs with .gitignore alongside state", async () => {
    const dataDir = await makeTempDir("agent-repo-gitignore-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    await store.writeDeployTree("agent-gi", {
      systemPrompt: "Gitignore test.",
      skills: [],
    });

    const sourceDir = await makeTempDir("gitignore-source-");
    await git.init({ fs, dir: sourceDir, defaultBranch: "main" });
    await fs.promises.writeFile(path.join(sourceDir, ".gitignore"), "keys/\n");
    await fs.promises.mkdir(path.join(sourceDir, "state"), { recursive: true });
    await fs.promises.writeFile(
      path.join(sourceDir, "state", "turns.jsonl"),
      "{}",
    );
    await git.add({ fs, dir: sourceDir, filepath: ".gitignore" });
    await git.add({ fs, dir: sourceDir, filepath: "state/turns.jsonl" });
    const stateCommit = await git.commit({
      fs,
      dir: sourceDir,
      message: "State with gitignore",
      author: { name: "test", email: "test@test" },
    });

    const { pack } = await createDeployPack(sourceDir, "refs/heads/main");
    const stateRef = "refs/instances/gi-test";
    await store.receiveStatePack("agent-gi", pack, stateRef, stateCommit);

    const repoDir = path.join(dataDir, "agents", "agent-gi");
    const resolved = await git.resolveRef({
      fs,
      dir: repoDir,
      ref: stateRef,
    });
    expect(resolved).toBe(stateCommit);
  });

  test("receiveStatePack rejects packs with only .gitignore and no state/", async () => {
    const dataDir = await makeTempDir("agent-repo-gitignore-only-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    await store.writeDeployTree("agent-gio", {
      systemPrompt: "Gitignore-only test.",
      skills: [],
    });

    const sourceDir = await makeTempDir("gitignore-only-source-");
    await git.init({ fs, dir: sourceDir, defaultBranch: "main" });
    await fs.promises.writeFile(path.join(sourceDir, ".gitignore"), "keys/\n");
    await git.add({ fs, dir: sourceDir, filepath: ".gitignore" });
    const badCommit = await git.commit({
      fs,
      dir: sourceDir,
      message: "Only gitignore",
      author: { name: "test", email: "test@test" },
    });

    const { pack } = await createDeployPack(sourceDir, "refs/heads/main");

    await expect(
      store.receiveStatePack(
        "agent-gio",
        pack,
        "refs/instances/test",
        badCommit,
      ),
    ).rejects.toThrow("path_violation");
  });

  test("receiveStatePack rejects packs with paths outside state/", async () => {
    const dataDir = await makeTempDir("agent-repo-confined-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    await store.writeDeployTree("agent-confined", {
      systemPrompt: "Confinement test.",
      skills: [],
    });

    const sourceDir = await makeTempDir("confined-source-");
    await git.init({ fs, dir: sourceDir, defaultBranch: "main" });
    await fs.promises.mkdir(path.join(sourceDir, "state"), { recursive: true });
    await fs.promises.mkdir(path.join(sourceDir, "deploy"), {
      recursive: true,
    });
    await fs.promises.writeFile(
      path.join(sourceDir, "state", "turns.jsonl"),
      "{}",
    );
    await fs.promises.writeFile(
      path.join(sourceDir, "deploy", "prompt.md"),
      "evil",
    );
    await git.add({ fs, dir: sourceDir, filepath: "state/turns.jsonl" });
    await git.add({ fs, dir: sourceDir, filepath: "deploy/prompt.md" });
    const badCommit = await git.commit({
      fs,
      dir: sourceDir,
      message: "Escaped confinement",
      author: { name: "test", email: "test@test" },
    });

    const { pack } = await createDeployPack(sourceDir, "refs/heads/main");

    await expect(
      store.receiveStatePack(
        "agent-confined",
        pack,
        "refs/instances/test",
        badCommit,
      ),
    ).rejects.toThrow("path_violation");
  });

  test("writeDeployTree is idempotent on the repo", async () => {
    const dataDir = await makeTempDir("agent-repo-idem-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    const first = await store.writeDeployTree("agent-4", {
      systemPrompt: "Version 1",
      skills: [],
    });

    const second = await store.writeDeployTree("agent-4", {
      systemPrompt: "Version 2",
      skills: [
        {
          name: "search",
          definition: { name: "search", description: "Search" },
        },
      ],
    });

    expect(second.commitSha).not.toBe(first.commitSha);

    const repoDir = path.join(dataDir, "agents", "agent-4");
    const prompt = await fs.promises.readFile(
      path.join(repoDir, "deploy", "prompt.md"),
      "utf-8",
    );
    expect(prompt).toBe("Version 2");
  });

  test("hub repo does not contain state/ scaffolding", async () => {
    const dataDir = await makeTempDir("agent-repo-nostate-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    await store.writeDeployTree("agent-5", {
      systemPrompt: "No state test.",
      skills: [],
    });

    const repoDir = path.join(dataDir, "agents", "agent-5");
    const stateExists = await fs.promises
      .stat(path.join(repoDir, "state"))
      .then(() => true)
      .catch(() => false);
    expect(stateExists).toBe(false);
  });

  test("rejects agent IDs with path traversal characters", () => {
    const dataDir = "/tmp/never-created";
    const store = createAgentRepoStore({ dataDir, signingKey });

    expect(() =>
      store.writeDeployTree("../../evil", {
        systemPrompt: "x",
        skills: [],
      }),
    ).toThrow("agentId contains unsafe characters");

    expect(() =>
      store.writeDeployTree("agent@domain", {
        systemPrompt: "x",
        skills: [],
      }),
    ).toThrow("agentId contains unsafe characters");
  });
});
