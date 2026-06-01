import { createRepoStore, SAFE_REPO_ID } from "./repo-store";
import type { RepoId } from "./repo-store";
import {
  agentStateKindHandler,
  agentStateAuthorize,
  AGENT_STATE_DEPLOY_REF,
  type AgentStateHubPrincipal,
  type AgentStateSidecarPrincipal,
} from "./agent-state-kind";

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
   *
   * `repoId.kind` must be `"agent-state"` — this store is per-agent and
   * does not generalize across kinds. The `repoId.id` is used as the
   * agent address internally.
   */
  receiveStatePack(
    repoId: RepoId,
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

  const store = createRepoStore({
    dataDir,
    signingKey,
    handlers: { "agent-state": agentStateKindHandler },
    authorize: agentStateAuthorize,
  });

  const hub: AgentStateHubPrincipal = { kind: "hub" };

  // The substrate enforces the same SAFE_REPO_ID rule with a different
  // error prefix; we throw the legacy message here so existing callers
  // and tests that match on it continue to work.
  function repoId(agentId: string): RepoId {
    if (!SAFE_REPO_ID.test(agentId)) {
      throw new Error(
        `agentId contains unsafe characters: ${JSON.stringify(agentId)}`,
      );
    }
    return { kind: "agent-state", id: agentId };
  }

  return {
    async writeDeployTree(agentId, content) {
      const id = repoId(agentId);
      const files: Record<string, string> = {
        "deploy/prompt.md": content.systemPrompt,
      };
      for (const skill of content.skills) {
        files[`deploy/skills/${skill.name}/tool.json`] = JSON.stringify(
          skill.definition,
          null,
          2,
        );
      }
      return store.writeTree(hub, id, AGENT_STATE_DEPLOY_REF, {
        files,
        clearPrefix: "deploy/",
        message: "Update deploy tree",
      });
    },

    async createDeployPack(agentId) {
      return store.createPack(hub, repoId(agentId), AGENT_STATE_DEPLOY_REF);
    },

    async receiveStatePack(incomingRepoId, pack, ref, commitSha) {
      if (incomingRepoId.kind !== "agent-state") {
        throw new Error(
          `AgentRepoStore.receiveStatePack requires repoId.kind === "agent-state", got ${JSON.stringify(incomingRepoId.kind)}`,
        );
      }
      const agentId = incomingRepoId.id;
      const id = repoId(agentId);
      const principal: AgentStateSidecarPrincipal = {
        kind: "sidecar",
        agentId,
      };
      await store.receivePack(principal, id, ref, pack, commitSha);
    },

    async getDeployRef(agentId) {
      return store.resolveRef(hub, repoId(agentId), AGENT_STATE_DEPLOY_REF);
    },

    getSigningPublicKey() {
      return signingKey.publicKey;
    },
  };
}
