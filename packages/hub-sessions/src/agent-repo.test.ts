import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { generateKeyPair } from "@intx/crypto";
import { createDeployPack } from "@intx/storage-isogit/node";
import { createAgentRepoStore, DEFAULT_GC_KINDS } from "./agent-repo";
import { applyWorkflowRunPack } from "./agent-repo.test-helpers";
import type { WorkflowRunSupervisorPrincipal } from "./workflow-run-kind";
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

describe("AgentRepoStore GC scope", () => {
  test("workflow-run is excluded from GC so expunged mailbox blobs survive in history", () => {
    // The warm-agent mailbox physically expunges <uid>.eml blobs from the
    // live tree; the raw bytes then persist only through the parent commit,
    // and survive ONLY because a workflow-run repo's objects are never
    // pruned. This tripwire fails if a future change makes workflow-run
    // GC-eligible, which would silently destroy the mailbox audit trail.
    const kinds: readonly string[] = DEFAULT_GC_KINDS;
    expect(kinds).not.toContain("workflow-run");
    expect(kinds).toEqual(["agent-state"]);
  });
});

describe("AgentRepoStore", () => {
  test("writeDeployTree creates a commit with the system prompt", async () => {
    const dataDir = await makeTempDir("agent-repo-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    const { commitSha } = await store.writeDeployTree("agent-1", {
      systemPrompt: "You are a test agent.",
    });

    expect(commitSha).toMatch(/^[0-9a-f]{40}$/);

    const repoDir = path.join(dataDir, "agents", "agent-1");
    const prompt = await fs.promises.readFile(
      path.join(repoDir, "deploy", "prompt.md"),
      "utf-8",
    );
    expect(prompt).toBe("You are a test agent.");

    const ref = await git.resolveRef({
      fs,
      dir: repoDir,
      ref: "refs/heads/deploy",
    });
    expect(ref).toBe(commitSha);
  });

  test("writeDeployTree does not advance refs/heads/main", async () => {
    const dataDir = await makeTempDir("agent-repo-ref-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    await store.writeDeployTree("agent-ref", {
      systemPrompt: "Ref test.",
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
    });

    const { pack, commitSha, ref } = await store.createDeployPack("agent-2");

    expect(pack).toBeInstanceOf(Uint8Array);
    expect(pack.length).toBeGreaterThan(0);
    expect(commitSha).toMatch(/^[0-9a-f]{40}$/);
    expect(ref).toBe("refs/heads/deploy");
  });

  test("receiveAgentStatePack indexes objects and updates ref", async () => {
    const dataDir = await makeTempDir("agent-repo-state-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    await store.writeDeployTree("agent-3", {
      systemPrompt: "State test.",
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
    await store.receiveAgentStatePack(
      { kind: "agent-state", id: "agent-3" },
      pack,
      stateRef,
      stateCommit,
    );

    const repoDir = path.join(dataDir, "agents", "agent-3");
    const resolved = await git.resolveRef({
      fs,
      dir: repoDir,
      ref: stateRef,
    });
    expect(resolved).toBe(stateCommit);
  });

  test("receiveAgentStatePack accepts packs with .gitignore alongside state", async () => {
    const dataDir = await makeTempDir("agent-repo-gitignore-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    await store.writeDeployTree("agent-gi", {
      systemPrompt: "Gitignore test.",
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
    await store.receiveAgentStatePack(
      { kind: "agent-state", id: "agent-gi" },
      pack,
      stateRef,
      stateCommit,
    );

    const repoDir = path.join(dataDir, "agents", "agent-gi");
    const resolved = await git.resolveRef({
      fs,
      dir: repoDir,
      ref: stateRef,
    });
    expect(resolved).toBe(stateCommit);
  });

  test("receiveAgentStatePack rejects packs with only .gitignore and no state/", async () => {
    const dataDir = await makeTempDir("agent-repo-gitignore-only-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    await store.writeDeployTree("agent-gio", {
      systemPrompt: "Gitignore-only test.",
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
      store.receiveAgentStatePack(
        { kind: "agent-state", id: "agent-gio" },
        pack,
        "refs/instances/test",
        badCommit,
      ),
    ).rejects.toThrow("path_violation");
  });

  test("receiveAgentStatePack rejects packs with paths outside state/", async () => {
    const dataDir = await makeTempDir("agent-repo-confined-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    await store.writeDeployTree("agent-confined", {
      systemPrompt: "Confinement test.",
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
      store.receiveAgentStatePack(
        { kind: "agent-state", id: "agent-confined" },
        pack,
        "refs/instances/test",
        badCommit,
      ),
    ).rejects.toThrow("path_violation");
  });

  test("writeDeployTree produces a fresh commit when content changes", async () => {
    const dataDir = await makeTempDir("agent-repo-idem-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    const first = await store.writeDeployTree("agent-4", {
      systemPrompt: "Version 1",
    });

    const second = await store.writeDeployTree("agent-4", {
      systemPrompt: "Version 2",
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
    });

    const repoDir = path.join(dataDir, "agents", "agent-5");
    const stateExists = await fs.promises
      .stat(path.join(repoDir, "state"))
      .then(() => true)
      .catch(() => false);
    expect(stateExists).toBe(false);
  });

  test("prepareWorkflowRunPack and commit index a workflow-run genesis pack and update the ref", async () => {
    const dataDir = await makeTempDir("agent-repo-wfr-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    // Construct a workflow-run pack carrying a `.gitignore`-only
    // genesis tree (the workflow-run kind handler explicitly accepts
    // this as the initial commit so deploy-time init can land before
    // any run produces an event).
    const sourceDir = await makeTempDir("wfr-source-");
    await git.init({ fs, dir: sourceDir, defaultBranch: "main" });
    await fs.promises.writeFile(path.join(sourceDir, ".gitignore"), "");
    await git.add({ fs, dir: sourceDir, filepath: ".gitignore" });
    const wfrCommit = await git.commit({
      fs,
      dir: sourceDir,
      message: "Workflow-run genesis",
      author: { name: "test", email: "test@test" },
    });

    const { pack } = await createDeployPack(sourceDir, "refs/heads/main");

    const wfrRef = "refs/heads/events";
    const anchorRunId = "dep-wfr-happy";
    const prepared = await store.prepareWorkflowRunPack(
      { kind: "workflow-run", id: anchorRunId },
      pack,
      wfrRef,
      wfrCommit,
    );
    if (prepared.kind !== "prepared") throw new Error("expected prepared");
    expect(await prepared.commit()).toEqual({
      kind: "applied",
      newlyTerminalRuns: [],
    });

    const repoDir = path.join(dataDir, "workflow-runs", anchorRunId);
    const resolved = await git.resolveRef({
      fs,
      dir: repoDir,
      ref: wfrRef,
    });
    expect(resolved).toBe(wfrCommit);
  });

  describe("a workflow-run receive with a tip the ref already holds", () => {
    // Commits on one line over a `.gitignore`-only tree, which the
    // workflow-run kind handler accepts; `packAt` packs the line up to
    // one of them.
    async function sourceLine(
      count: number,
      root = "",
    ): Promise<{
      commits: string[];
      packAt: (commitSha: string) => Promise<Uint8Array>;
    }> {
      const sourceDir = await makeTempDir("wfr-line-");
      await git.init({ fs, dir: sourceDir, defaultBranch: "main" });
      await fs.promises.writeFile(path.join(sourceDir, ".gitignore"), root);
      await git.add({ fs, dir: sourceDir, filepath: ".gitignore" });
      const commits: string[] = [];
      for (let i = 0; i < count; i++) {
        commits.push(
          await git.commit({
            fs,
            dir: sourceDir,
            message: `Commit ${String(i)}`,
            author: { name: "test", email: "test@test" },
          }),
        );
      }
      return {
        commits,
        packAt: async (commitSha) => {
          // Every commit, tree and blob from the root up to
          // `commitSha`, so the pack carries its whole history.
          const oids = new Set<string>();
          for (const entry of await git.log({
            fs,
            dir: sourceDir,
            ref: commitSha,
          })) {
            oids.add(entry.oid);
            oids.add(entry.commit.tree);
            const { tree } = await git.readTree({
              fs,
              dir: sourceDir,
              oid: entry.commit.tree,
            });
            for (const e of tree) oids.add(e.oid);
          }
          const { packfile } = await git.packObjects({
            fs,
            dir: sourceDir,
            oids: [...oids],
          });
          if (packfile === undefined) throw new Error("packObjects");
          return packfile;
        },
      };
    }

    async function packFiles(repoDir: string): Promise<string[]> {
      const entries = await fs.promises.readdir(
        path.join(repoDir, ".git", "objects", "pack"),
      );
      return entries.filter((e) => e.endsWith(".pack")).sort();
    }

    function supervisorOf(anchorRunId: string): WorkflowRunSupervisorPrincipal {
      return { kind: "supervisor", anchorRunId };
    }

    const wfrRef = "refs/heads/events";

    test("an older pack that lands after a newer one does not move the ref back", async () => {
      const dataDir = await makeTempDir("agent-repo-wfr-stale-");
      const store = createAgentRepoStore({ dataDir, signingKey });
      const repoId = { kind: "workflow-run" as const, id: "dep-wfr-stale" };
      const repoDir = path.join(dataDir, "workflow-runs", repoId.id);
      const line = await sourceLine(3);
      const [first, , last] = line.commits;
      if (first === undefined || last === undefined) throw new Error("line");

      const olderPack = await line.packAt(first);
      const newerPack = await line.packAt(last);
      await applyWorkflowRunPack(store, repoId, olderPack, wfrRef, first);
      await applyWorkflowRunPack(store, repoId, newerPack, wfrRef, last);
      const packsBefore = await packFiles(repoDir);

      const runs = await applyWorkflowRunPack(
        store,
        repoId,
        olderPack,
        wfrRef,
        first,
      );

      expect(runs).toEqual([]);
      expect(await git.resolveRef({ fs, dir: repoDir, ref: wfrRef })).toBe(
        last,
      );
      expect(await packFiles(repoDir)).toEqual(packsBefore);
    });

    test("a re-push of the current tip publishes no pack", async () => {
      const dataDir = await makeTempDir("agent-repo-wfr-same-");
      const store = createAgentRepoStore({ dataDir, signingKey });
      const repoId = { kind: "workflow-run" as const, id: "dep-wfr-same" };
      const repoDir = path.join(dataDir, "workflow-runs", repoId.id);
      const line = await sourceLine(2);
      const tip = line.commits[1];
      if (tip === undefined) throw new Error("line");

      const pack = await line.packAt(tip);
      await applyWorkflowRunPack(store, repoId, pack, wfrRef, tip);
      const packsBefore = await packFiles(repoDir);

      const runs = await applyWorkflowRunPack(store, repoId, pack, wfrRef, tip);

      expect(runs).toEqual([]);
      expect(await git.resolveRef({ fs, dir: repoDir, ref: wfrRef })).toBe(tip);
      expect(await packFiles(repoDir)).toEqual(packsBefore);
    });

    test("a known commit that the ref no longer reaches still moves the ref", async () => {
      // The sidecar is authoritative for its history, so a push of a
      // new line replaces the hub's copy; a later push of the old line
      // is not one the ref holds, so it replaces the copy again.
      const dataDir = await makeTempDir("agent-repo-wfr-replaced-");
      const store = createAgentRepoStore({ dataDir, signingKey });
      const repoId = { kind: "workflow-run" as const, id: "dep-wfr-replaced" };
      const repoDir = path.join(dataDir, "workflow-runs", repoId.id);
      const oldLine = await sourceLine(1);
      const newLine = await sourceLine(1, "other\n");
      const oldTip = oldLine.commits[0];
      const newTip = newLine.commits[0];
      if (oldTip === undefined || newTip === undefined) throw new Error("line");

      const oldPack = await oldLine.packAt(oldTip);
      await applyWorkflowRunPack(store, repoId, oldPack, wfrRef, oldTip);
      await applyWorkflowRunPack(
        store,
        repoId,
        await newLine.packAt(newTip),
        wfrRef,
        newTip,
      );
      await applyWorkflowRunPack(store, repoId, oldPack, wfrRef, oldTip);

      expect(await git.resolveRef({ fs, dir: repoDir, ref: wfrRef })).toBe(
        oldTip,
      );
    });

    test("a held tip is answered at prepare, with nothing staged", async () => {
      const dataDir = await makeTempDir("agent-repo-wfr-held-prepare-");
      const store = createAgentRepoStore({ dataDir, signingKey });
      const repoId = { kind: "workflow-run" as const, id: "dep-wfr-held" };
      const repoDir = path.join(dataDir, "workflow-runs", repoId.id);
      const line = await sourceLine(3);
      const [, middle, last] = line.commits;
      if (middle === undefined || last === undefined) throw new Error("line");

      await applyWorkflowRunPack(
        store,
        repoId,
        await line.packAt(last),
        wfrRef,
        last,
      );
      const prepared = await store.prepareWorkflowRunPack(
        repoId,
        await line.packAt(middle),
        wfrRef,
        middle,
      );

      expect(prepared.kind).toBe("already_applied");
      expect(await git.resolveRef({ fs, dir: repoDir, ref: wfrRef })).toBe(
        last,
      );
      expect(
        await fs.promises
          .readdir(path.join(repoDir, ".git", "objects", "pack-staging"))
          .catch(() => []),
      ).toEqual([]);
    });

    test("a held tip is answered at commit when another receive reached it first", async () => {
      const dataDir = await makeTempDir("agent-repo-wfr-held-commit-");
      const store = createAgentRepoStore({ dataDir, signingKey });
      const repoId = { kind: "workflow-run" as const, id: "dep-wfr-raced" };
      const repoDir = path.join(dataDir, "workflow-runs", repoId.id);
      const line = await sourceLine(3);
      const [first, , last] = line.commits;
      if (first === undefined || last === undefined) throw new Error("line");
      const olderPack = await line.packAt(first);
      const newerPack = await line.packAt(last);

      const prepared = await store.prepareWorkflowRunPack(
        repoId,
        olderPack,
        wfrRef,
        first,
      );
      if (prepared.kind !== "prepared") throw new Error("expected prepared");
      await applyWorkflowRunPack(store, repoId, newerPack, wfrRef, last);
      const packsBefore = await packFiles(repoDir);

      expect(await prepared.commit()).toEqual({ kind: "already_applied" });
      expect(await git.resolveRef({ fs, dir: repoDir, ref: wfrRef })).toBe(
        last,
      );
      expect(await packFiles(repoDir)).toEqual(packsBefore);
    });

    test("a held tip is recognized by a store that starts on existing data", async () => {
      const dataDir = await makeTempDir("agent-repo-wfr-cold-");
      const repoId = { kind: "workflow-run" as const, id: "dep-wfr-cold" };
      const repoDir = path.join(dataDir, "workflow-runs", repoId.id);
      const line = await sourceLine(3);
      const [first, , last] = line.commits;
      if (first === undefined || last === undefined) throw new Error("line");

      await applyWorkflowRunPack(
        createAgentRepoStore({ dataDir, signingKey }),
        repoId,
        await line.packAt(last),
        wfrRef,
        last,
      );
      const runs = await applyWorkflowRunPack(
        createAgentRepoStore({ dataDir, signingKey }),
        repoId,
        await line.packAt(first),
        wfrRef,
        first,
      );

      expect(runs).toEqual([]);
      expect(await git.resolveRef({ fs, dir: repoDir, ref: wfrRef })).toBe(
        last,
      );
    });

    test("a one-call receive with its expected old tip still moves the ref to an older tip", async () => {
      const dataDir = await makeTempDir("agent-repo-wfr-no-opt-in-");
      const store = createAgentRepoStore({ dataDir, signingKey });
      const repoId = { kind: "workflow-run" as const, id: "dep-wfr-plain" };
      const repoDir = path.join(dataDir, "workflow-runs", repoId.id);
      const line = await sourceLine(3);
      const [first, , last] = line.commits;
      if (first === undefined || last === undefined) throw new Error("line");

      await applyWorkflowRunPack(
        store,
        repoId,
        await line.packAt(last),
        wfrRef,
        last,
      );
      await store.repoStore.receivePack(
        supervisorOf(repoId.id),
        repoId,
        wfrRef,
        await line.packAt(first),
        first,
        last,
      );

      expect(await git.resolveRef({ fs, dir: repoDir, ref: wfrRef })).toBe(
        first,
      );
    });
  });

  test("prepareWorkflowRunPack rejects packs whose repoId.kind is not workflow-run", async () => {
    const dataDir = await makeTempDir("agent-repo-wfr-wrong-kind-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    const sourceDir = await makeTempDir("wfr-wrong-kind-source-");
    await git.init({ fs, dir: sourceDir, defaultBranch: "main" });
    await fs.promises.writeFile(path.join(sourceDir, ".gitignore"), "");
    await git.add({ fs, dir: sourceDir, filepath: ".gitignore" });
    const commit = await git.commit({
      fs,
      dir: sourceDir,
      message: "Wrong kind",
      author: { name: "test", email: "test@test" },
    });

    const { pack } = await createDeployPack(sourceDir, "refs/heads/main");

    await expect(
      store.prepareWorkflowRunPack(
        { kind: "agent-state", id: "agent-x" },
        pack,
        "refs/heads/events",
        commit,
      ),
    ).rejects.toThrow(
      /prepareWorkflowRunPack requires repoId\.kind === "workflow-run"/,
    );
  });

  test("receiveAgentStatePack rejects packs whose repoId.kind is not agent-state", async () => {
    const dataDir = await makeTempDir("agent-repo-state-wrong-kind-");
    const store = createAgentRepoStore({ dataDir, signingKey });

    const sourceDir = await makeTempDir("state-wrong-kind-source-");
    await git.init({ fs, dir: sourceDir, defaultBranch: "main" });
    await fs.promises.mkdir(path.join(sourceDir, "state"), { recursive: true });
    await fs.promises.writeFile(
      path.join(sourceDir, "state", "turns.jsonl"),
      "{}",
    );
    await git.add({ fs, dir: sourceDir, filepath: "state/turns.jsonl" });
    const commit = await git.commit({
      fs,
      dir: sourceDir,
      message: "Misrouted",
      author: { name: "test", email: "test@test" },
    });

    const { pack } = await createDeployPack(sourceDir, "refs/heads/main");

    await expect(
      store.receiveAgentStatePack(
        { kind: "workflow-run", id: "dep-misrouted" },
        pack,
        "refs/heads/state",
        commit,
      ),
    ).rejects.toThrow(
      /receiveAgentStatePack requires repoId\.kind === "agent-state"/,
    );
  });

  test("rejects agent IDs with path traversal characters", () => {
    const dataDir = "/tmp/never-created";
    const store = createAgentRepoStore({ dataDir, signingKey });

    expect(() =>
      store.writeDeployTree("../../evil", {
        systemPrompt: "x",
      }),
    ).toThrow("repo_id_invalid: ../../evil");

    expect(() =>
      store.writeDeployTree("agent@domain", {
        systemPrompt: "x",
      }),
    ).toThrow("repo_id_invalid: agent@domain");
  });
});
