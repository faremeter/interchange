// A real RepoStore for hub-api route tests: it signs with the given key and
// serves the skill, package-registry and workflow asset kinds. The caller
// owns `dataDir` and removes it.

import { createSSHSignature } from "@intx/crypto";
import {
  createRepoStore,
  packageRegistryAuthorize,
  packageRegistryKindHandler,
  skillAuthorize,
  skillKindHandler,
  workflowAuthorize,
  workflowKindHandler,
  type AuthorizeFn,
  type RepoStore,
} from "@intx/hub-sessions";
import type { KeyPair } from "@intx/types/runtime";

export type CreateTestRepoStoreArgs = {
  dataDir: string;
  signingKey: KeyPair;
};

export function createTestRepoStore({
  dataDir,
  signingKey,
}: CreateTestRepoStoreArgs): RepoStore {
  const signer = async (payload: string) =>
    createSSHSignature(payload, signingKey.privateKey, signingKey.publicKey);
  const authorize: AuthorizeFn = (principal, repoId, ref, action) => {
    if (repoId.kind === "skill") {
      return skillAuthorize(principal, repoId, ref, action);
    }
    if (repoId.kind === "package-registry") {
      return packageRegistryAuthorize(principal, repoId, ref, action);
    }
    if (repoId.kind === "workflow") {
      return workflowAuthorize(principal, repoId, ref, action);
    }
    return { allowed: false, reason: `no authorize for ${repoId.kind}` };
  };
  return createRepoStore({
    dataDir,
    signingKey,
    handlers: {
      skill: skillKindHandler,
      "package-registry": packageRegistryKindHandler,
      workflow: workflowKindHandler,
    },
    authorize,
    signingCallback: () => signer,
  });
}
