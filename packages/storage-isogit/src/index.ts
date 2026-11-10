import type { ContextStore, AuditStore } from "@intx/types/runtime";
import {
  createAndSwitchBranch,
  currentBranch,
  listBranches,
  logHistory,
  switchBranch,
} from "./history";
import { initAgentRepo, initRepo } from "./init";
import {
  removeOrphanPackIndexes,
  removeStagedPacks,
  stagePack,
} from "./pack-receive";
import { createDeployPack, createNegotiatedPack } from "./pack-send";
import { collectReachableObjects } from "./object-walk";
import { createObjectPresence } from "./object-presence";
import { writeTreeToDisk } from "./write-tree";
import { indexPackIntoGitDir } from "./index-pack";
import {
  IsogitStore,
  type DurableMirrorReads,
  type ParkedReplyTargetBuffer,
} from "./store";
import type { CommitSigner } from "./signer";
import {
  normalizeRuntime,
  type IsogitRuntime,
  type StorageRuntime,
} from "./runtime";

export type { ContextStore, AuditStore, CommitSigner };
export type { StagedPack } from "./pack-receive";
export type { IncludeShaPredicate } from "./pack-send";
export type { ObjectPresence } from "./object-presence";
export type { DurableMirrorReads, ParkedReplyTargetBuffer };
export type { InitRepoOpts } from "./init";
export type { IsogitPath, IsogitRuntime } from "./runtime";
export { writeTreeToDisk } from "./write-tree";
export { indexPackIntoGitDir } from "./index-pack";
export { HUB_REF_NAMESPACE } from "./hub-refs";
export {
  DEFAULT_PACK_MATERIALIZATION_LIMITS,
  type PackMaterializationLimits,
} from "./materialization-limits";

function bindRepoDir<TArgs extends readonly unknown[], TResult>(
  runtime: StorageRuntime,
  operation: (runtime: StorageRuntime, dir: string, ...args: TArgs) => TResult,
): (dir: string, ...args: TArgs) => TResult {
  return (dir, ...args) =>
    operation(runtime, runtime.path.resolve(dir), ...args);
}

/** Bind the complete storage API to one filesystem runtime. */
export function createIsogitStorage(runtime: IsogitRuntime) {
  const storageRuntime = normalizeRuntime(runtime);
  return {
    storageRuntime,
    createIsogitStore: async (
      dir: string,
      signer?: CommitSigner,
    ): Promise<
      ContextStore & AuditStore & DurableMirrorReads & ParkedReplyTargetBuffer
    > => {
      const resolvedDir = storageRuntime.path.resolve(dir);
      await initAgentRepo(storageRuntime, resolvedDir);
      return new IsogitStore(storageRuntime, resolvedDir, signer);
    },
    initRepo: bindRepoDir(storageRuntime, initRepo),
    initAgentRepo: bindRepoDir(storageRuntime, initAgentRepo),
    switchBranch: bindRepoDir(storageRuntime, switchBranch),
    createAndSwitchBranch: bindRepoDir(storageRuntime, createAndSwitchBranch),
    currentBranch: bindRepoDir(storageRuntime, currentBranch),
    listBranches: bindRepoDir(storageRuntime, listBranches),
    logHistory: bindRepoDir(storageRuntime, logHistory),
    stagePack: bindRepoDir(storageRuntime, stagePack),
    removeStagedPacks: bindRepoDir(storageRuntime, removeStagedPacks),
    removeOrphanPackIndexes: bindRepoDir(
      storageRuntime,
      removeOrphanPackIndexes,
    ),
    createDeployPack: bindRepoDir(storageRuntime, createDeployPack),
    createNegotiatedPack: bindRepoDir(storageRuntime, createNegotiatedPack),
    collectReachableObjects: bindRepoDir(
      storageRuntime,
      collectReachableObjects,
    ),
    createObjectPresence: bindRepoDir(storageRuntime, createObjectPresence),
    writeTreeToDisk: bindRepoDir(storageRuntime, writeTreeToDisk),
    indexPackIntoGitDir: bindRepoDir(storageRuntime, indexPackIntoGitDir),
  };
}
