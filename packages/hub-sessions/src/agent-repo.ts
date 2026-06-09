import { createSSHSignature } from "@intx/crypto-node";
import type { ToolPackageManifest } from "@intx/types/tool-packages";

import { createRepoStore } from "./repo-store";
import type { AuthorizeFn, RepoId, RepoStore } from "./repo-store";
import {
  agentStateKindHandler,
  agentStateAuthorize,
  AGENT_STATE_DEPLOY_REF,
  type AgentStateHubPrincipal,
  type AgentStateSidecarPrincipal,
} from "./agent-state-kind";
import { skillKindHandler, skillAuthorize } from "./skill-kind";
import {
  packageRegistryKindHandler,
  packageRegistryAuthorize,
} from "./package-registry-kind";

export type DeployContent = {
  systemPrompt: string;
  /**
   * Optional. When present, written to
   * `deploy/tool-packages-manifest.json` so the sidecar's loader can
   * materialize the pinned tool-package closure on apply.
   */
  toolPackageManifest?: ToolPackageManifest;
  /**
   * Optional. `assetId` → workspace-relative mount path for every
   * asset id referenced by a `kind: "asset"` entry in
   * `toolPackageManifest`. When present, written to
   * `deploy/asset-mounts.json`; the sidecar's loader reads it back via
   * `readDeployTree` and resolves asset-sourced tarballs against it.
   * Empty maps and absent values produce no file on disk — both shapes
   * read back as an empty mount table.
   */
  assetMounts?: ReadonlyMap<string, string>;
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

  /**
   * Underlying kind-keyed substrate. Exposed so callers that need to
   * operate on non-agent-state kinds (e.g. the asset service writing
   * skill repos) can share the same on-disk root and signing key
   * without spinning up a parallel RepoStore.
   */
  readonly repoStore: RepoStore;
};

export function createAgentRepoStore(config: {
  dataDir: string;
  signingKey: { privateKey: Uint8Array; publicKey: Uint8Array };
}): AgentRepoStore {
  const { dataDir, signingKey } = config;

  const authorize: AuthorizeFn = (principal, incomingRepoId, ref, action) => {
    switch (incomingRepoId.kind) {
      case "agent-state":
        return agentStateAuthorize(principal, incomingRepoId, ref, action);
      case "skill":
        return skillAuthorize(principal, incomingRepoId, ref, action);
      case "package-registry":
        return packageRegistryAuthorize(principal, incomingRepoId, ref, action);
      default: {
        const _exhaustive: never = incomingRepoId.kind;
        return {
          allowed: false,
          reason: `no authorize registered for kind: ${String(_exhaustive)}`,
        };
      }
    }
  };

  // The substrate's signingCallback bridges the agent-repo store's
  // raw Ed25519 keypair to the storage layer's per-payload SSHSIG
  // signer. Skill asset genesis commits and agent-state deploy
  // commits both flow through this signer so that signed commits
  // round-trip through the smart-HTTP layer and verify under
  // `git log --show-signature` and `git verify-commit`.
  const signer = async (payload: string) =>
    createSSHSignature(payload, signingKey.privateKey, signingKey.publicKey);

  const store = createRepoStore({
    dataDir,
    signingKey,
    handlers: {
      "agent-state": agentStateKindHandler,
      skill: skillKindHandler,
      "package-registry": packageRegistryKindHandler,
    },
    authorize,
    signingCallback: () => signer,
  });

  const hub: AgentStateHubPrincipal = { kind: "hub" };

  function repoId(agentId: string): RepoId {
    return { kind: "agent-state", id: agentId };
  }

  return {
    async writeDeployTree(agentId, content) {
      const id = repoId(agentId);
      const files: Record<string, string> = {
        "deploy/prompt.md": content.systemPrompt,
      };
      if (content.toolPackageManifest !== undefined) {
        files["deploy/tool-packages-manifest.json"] = JSON.stringify(
          content.toolPackageManifest,
          null,
          2,
        );
      }
      if (content.assetMounts !== undefined && content.assetMounts.size > 0) {
        files["deploy/asset-mounts.json"] = JSON.stringify(
          { assetMounts: Object.fromEntries(content.assetMounts) },
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
      const expectedOldSha = await store.resolveRef(principal, id, ref);
      await store.receivePack(
        principal,
        id,
        ref,
        pack,
        commitSha,
        expectedOldSha,
      );
    },

    async getDeployRef(agentId) {
      return store.resolveRef(hub, repoId(agentId), AGENT_STATE_DEPLOY_REF);
    },

    getSigningPublicKey() {
      return signingKey.publicKey;
    },

    repoStore: store,
  };
}
