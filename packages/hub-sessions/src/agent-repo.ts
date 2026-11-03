import { createSSHSignature } from "@intx/crypto";

import { createRepoStore } from "./repo-store";
import type {
  AuthorizeFn,
  LocalRepoStore,
  PreparedReceivePack,
  RepoId,
} from "./repo-store";
import { agentStateKindHandler, agentStateAuthorize } from "./agent-state-kind";
import { skillKindHandler, skillAuthorize } from "./skill-kind";
import {
  packageRegistryKindHandler,
  packageRegistryAuthorize,
} from "./package-registry-kind";
import { workflowKindHandler, workflowAuthorize } from "./workflow-kind";
import {
  workflowRunKindHandler,
  workflowRunAuthorize,
  type WorkflowRunSupervisorPrincipal,
} from "./workflow-run-kind";

export type AgentRepoStore = {
  /**
   * Stage and validate a workflow-run pack from a sidecar, and return the
   * prepared receive. The validation holds no repo lock, so a caller can
   * take a lock of its own around `commit()` alone. `commit()` takes the
   * repo lock, publishes the pack, and moves the ref without materializing
   * a working tree.
   *
   * The sidecar's pushes can land out of order, so a pack whose tip the
   * ref already holds (its tip or a first-parent ancestor) moves no ref:
   * prepare or `commit()` answers `already_applied`, and the ref keeps
   * the newer tip. When the store lost objects of that tip, such a pack
   * must bring them, or it is rejected as `missing_objects`, and
   * `commit()` publishes them. Any other pack goes through the usual
   * receive checks, and one on a new line of history is not rejected for
   * that.
   *
   * `repoId.kind` must be `"workflow-run"`. Stamps a
   * `WorkflowRunSupervisorPrincipal` whose `anchorRunId` equals
   * `repoId.id`; the workflow-run kind handler denies sidecar-kind
   * writes by design, so the supervisor branch is the correct path for
   * sidecar-originated run-event commits (the supervisor lives on the
   * sidecar and is the actual signer of the run events).
   *
   * An applied `commit()` returns the runs the pack drove terminal,
   * surfaced from the substrate's per-commit validation so a caller can
   * flip the run's DB state without re-deriving terminal-ness.
   */
  prepareWorkflowRunPack(
    repoId: RepoId,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
  ): Promise<PreparedReceivePack>;

  /**
   * Underlying kind-keyed substrate. Exposed so callers that need to
   * operate on non-agent-state kinds (e.g. the asset service writing
   * skill repos) can share the same on-disk root and signing key
   * without spinning up a parallel RepoStore.
   */
  readonly repoStore: LocalRepoStore;
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
      case "workflow":
        return workflowAuthorize(principal, incomingRepoId, ref, action);
      case "workflow-run":
        return workflowRunAuthorize(principal, incomingRepoId, ref, action);
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
      workflow: workflowKindHandler,
      "workflow-run": workflowRunKindHandler,
    },
    authorize,
    signingCallback: () => signer,
  });

  async function prepareWorkflowRunPack(
    incomingRepoId: RepoId,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
  ): Promise<PreparedReceivePack> {
    if (incomingRepoId.kind !== "workflow-run") {
      throw new Error(
        `AgentRepoStore.prepareWorkflowRunPack requires repoId.kind === "workflow-run", got ${JSON.stringify(incomingRepoId.kind)}`,
      );
    }
    const principal: WorkflowRunSupervisorPrincipal = {
      kind: "supervisor",
      anchorRunId: incomingRepoId.id,
    };
    return store.prepareReceivePack(
      principal,
      incomingRepoId,
      ref,
      pack,
      commitSha,
    );
  }

  return {
    prepareWorkflowRunPack,

    repoStore: store,
  };
}
