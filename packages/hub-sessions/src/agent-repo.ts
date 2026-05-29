import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";
import { createSshSignature } from "@intx/crypto-node";
import {
  initRepo,
  createDeployPack,
  receivePackObjects,
} from "@intx/storage-isogit";
import { hasCode } from "@intx/types";

const AUTHOR = {
  name: "interchange-hub",
  email: "hub@interchange.local",
};

const SAFE_AGENT_ID = /^[a-zA-Z0-9_-]+$/;

const DEPLOY_REF = "refs/heads/deploy";

export type DeployContent = {
  systemPrompt: string;
  skills: { name: string; definition: Record<string, unknown> }[];
};

export type AgentRepoStore = {
  /**
   * Write deploy content into the agent's hub-side repo and commit on
   * refs/heads/deploy. Creates the repo if it doesn't exist.
   *
   * The caller is responsible for serializing calls per agent.
   */
  writeDeployTree(
    agentId: string,
    content: DeployContent,
  ): Promise<{ commitSha: string }>;

  /**
   * Produce a packfile from the agent's current deploy ref.
   */
  createDeployPack(
    agentId: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }>;

  /**
   * Receive and store a state pack from a sidecar. Indexes the pack
   * objects and updates the ref without materializing a working tree.
   */
  receiveStatePack(
    agentId: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
  ): Promise<void>;

  /** Resolve the current deploy ref SHA, or null if no deploy exists. */
  getDeployRef(agentId: string): Promise<string | null>;

  /** Raw 32-byte Ed25519 public key used to sign deploy commits. */
  getSigningPublicKey(): Uint8Array;
};

export function createAgentRepoStore(config: {
  dataDir: string;
  signingKey: { privateKey: Uint8Array; publicKey: Uint8Array };
}): AgentRepoStore {
  const { dataDir, signingKey } = config;

  function validateAgentId(agentId: string): void {
    if (!SAFE_AGENT_ID.test(agentId)) {
      throw new Error(
        `agentId contains unsafe characters: ${JSON.stringify(agentId)}`,
      );
    }
  }

  function repoDir(agentId: string): string {
    validateAgentId(agentId);
    return path.join(dataDir, "agents", agentId);
  }

  async function ensureRepo(agentId: string): Promise<string> {
    const dir = repoDir(agentId);
    await initRepo(dir);
    return dir;
  }

  /**
   * Collect all files tracked in the git index under a given prefix.
   */
  async function indexedPaths(dir: string, prefix: string): Promise<string[]> {
    const matrix = await git.statusMatrix({ fs, dir });
    return matrix
      .filter(([filepath]) => filepath.startsWith(prefix))
      .map(([filepath]) => filepath as string);
  }

  async function writeDeployTree(
    agentId: string,
    content: DeployContent,
  ): Promise<{ commitSha: string }> {
    const dir = await ensureRepo(agentId);

    // Remove all tracked deploy/ paths from the index so stale entries
    // from a previous writeDeployTree call don't survive.
    const oldPaths = await indexedPaths(dir, "deploy/");
    for (const filepath of oldPaths) {
      await git.remove({ fs, dir, filepath });
    }

    const deployDir = path.join(dir, "deploy");
    await fs.promises.rm(deployDir, { recursive: true, force: true });
    await fs.promises.mkdir(deployDir, { recursive: true });

    await fs.promises.writeFile(
      path.join(deployDir, "prompt.md"),
      content.systemPrompt,
    );
    await git.add({ fs, dir, filepath: "deploy/prompt.md" });

    for (const skill of content.skills) {
      const skillDir = path.join(deployDir, "skills", skill.name);
      await fs.promises.mkdir(skillDir, { recursive: true });
      await fs.promises.writeFile(
        path.join(skillDir, "tool.json"),
        JSON.stringify(skill.definition, null, 2),
      );
      await git.add({
        fs,
        dir,
        filepath: `deploy/skills/${skill.name}/tool.json`,
      });
    }

    // Resolve the parent from the deploy ref if it exists, otherwise
    // fall back to HEAD. This keeps deploy history on its own lineage
    // separate from refs/heads/main.
    let parent: string[];
    try {
      parent = [await git.resolveRef({ fs, dir, ref: DEPLOY_REF })];
    } catch {
      parent = [await git.resolveRef({ fs, dir, ref: "HEAD" })];
    }

    const commitSha = await git.commit({
      fs,
      dir,
      message: "Update deploy tree",
      author: AUTHOR,
      parent,
      ref: DEPLOY_REF,
      signingKey: "sshsig",
      onSign: async ({ payload }) => ({
        signature: createSshSignature(
          payload,
          signingKey.privateKey,
          signingKey.publicKey,
        ),
      }),
    });

    return { commitSha };
  }

  async function makeDeployPack(
    agentId: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }> {
    const dir = repoDir(agentId);
    const { pack, commitSha } = await createDeployPack(dir, DEPLOY_REF);
    return { pack, commitSha, ref: DEPLOY_REF };
  }

  async function receiveStatePack(
    agentId: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
  ): Promise<void> {
    const dir = await ensureRepo(agentId);
    const transferId = crypto.randomUUID().replace(/-/g, "");
    // Allow the per-cycle working-tree files written by the reactor at the
    // repository root alongside `state/` and `.gitignore`. Anything outside
    // this allowlist (notably `deploy/`) is rejected. The pack must contain
    // at least one state-bearing path beyond `.gitignore`.
    const allowedTopLevel = new Set([
      "state",
      ".gitignore",
      "turns.jsonl",
      "prompt.jsonl",
      "response.jsonl",
      "manifest.jsonl",
      "metadata.json",
      "tool-output",
    ]);
    await receivePackObjects(
      dir,
      pack,
      ref,
      commitSha,
      transferId,
      (paths) =>
        paths.every((p) => allowedTopLevel.has(p)) &&
        paths.some((p) => p !== ".gitignore"),
    );
  }

  async function getDeployRef(agentId: string): Promise<string | null> {
    const dir = repoDir(agentId);
    try {
      return await git.resolveRef({ fs, dir, ref: DEPLOY_REF });
    } catch (err: unknown) {
      if (hasCode(err) && err.code === "NotFoundError") {
        return null;
      }
      throw err;
    }
  }

  return {
    writeDeployTree,
    createDeployPack: makeDeployPack,
    receiveStatePack,
    getDeployRef,
    getSigningPublicKey: () => signingKey.publicKey,
  };
}
