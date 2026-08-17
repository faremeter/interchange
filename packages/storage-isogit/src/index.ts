import type { ContextStore, AuditStore } from "@intx/types/runtime";
import {
  createAndSwitchBranch,
  currentBranch,
  listBranches,
  logHistory,
  switchBranch,
} from "./history";
import { initAgentRepo, initRepo } from "./init";
import { applyPack, receivePackObjects } from "./pack-receive";
import { createDeployPack, createNegotiatedPack } from "./pack-send";
import { collectReachableObjects } from "./object-walk";
import { writeTreeToDisk } from "./write-tree";
import {
  countLooseObjects,
  countPackFiles,
  gitBytes,
  listRepoRefs,
  repoDiskUsage,
} from "./repo-disk";
import { IsogitStore, type DurableMirrorReads } from "./store";
import type { CommitSigner } from "./signer";
import { maybeGC, runGC, type GCPolicy } from "./gc";
import { createMailAuditStore, listMail } from "./mail-store";
import {
  normalizeRuntime,
  type IsogitRuntime,
  type StorageRuntime,
} from "./runtime";

export type { ContextStore, AuditStore, CommitSigner };
export type {
  CommitVerifier,
  TreeValidator,
  TreeValidatorResult,
} from "./pack-receive";
export type { IncludeShaPredicate } from "./pack-send";
export type { DurableMirrorReads };
export type { InitRepoOpts } from "./init";
export type { RetentionPolicy, GCResult, GCPolicy } from "./gc";
export type { RepoDiskUsage } from "./repo-disk";
export type {
  MailAuditStore,
  MailCommitOptions,
  MailCommitResult,
  MailDirection,
  MailEntry,
} from "./mail-store";
export type { IsogitPath, IsogitRuntime } from "./runtime";
export { writeTreeToDisk } from "./write-tree";

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
      gcPolicy?: GCPolicy,
    ): Promise<ContextStore & AuditStore & DurableMirrorReads> => {
      const resolvedDir = storageRuntime.path.resolve(dir);
      await initAgentRepo(storageRuntime, resolvedDir);
      return new IsogitStore(storageRuntime, resolvedDir, signer, gcPolicy);
    },
    initRepo: bindRepoDir(storageRuntime, initRepo),
    initAgentRepo: bindRepoDir(storageRuntime, initAgentRepo),
    switchBranch: bindRepoDir(storageRuntime, switchBranch),
    createAndSwitchBranch: bindRepoDir(storageRuntime, createAndSwitchBranch),
    currentBranch: bindRepoDir(storageRuntime, currentBranch),
    listBranches: bindRepoDir(storageRuntime, listBranches),
    logHistory: bindRepoDir(storageRuntime, logHistory),
    applyPack: bindRepoDir(storageRuntime, applyPack),
    receivePackObjects: bindRepoDir(storageRuntime, receivePackObjects),
    createDeployPack: bindRepoDir(storageRuntime, createDeployPack),
    createNegotiatedPack: bindRepoDir(storageRuntime, createNegotiatedPack),
    collectReachableObjects: bindRepoDir(
      storageRuntime,
      collectReachableObjects,
    ),
    repoDiskUsage: bindRepoDir(storageRuntime, repoDiskUsage),
    countLooseObjects: bindRepoDir(storageRuntime, countLooseObjects),
    countPackFiles: bindRepoDir(storageRuntime, countPackFiles),
    gitBytes: bindRepoDir(storageRuntime, gitBytes),
    listRepoRefs: bindRepoDir(storageRuntime, listRepoRefs),
    runGC: bindRepoDir(storageRuntime, runGC),
    maybeGC: bindRepoDir(storageRuntime, maybeGC),
    createMailAuditStore: bindRepoDir(storageRuntime, createMailAuditStore),
    listMail: bindRepoDir(storageRuntime, listMail),
    writeTreeToDisk: bindRepoDir(storageRuntime, writeTreeToDisk),
  };
}
