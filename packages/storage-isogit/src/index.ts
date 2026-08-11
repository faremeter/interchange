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
import { normalizeRuntime, type IsogitRuntime } from "./runtime";

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
      await initAgentRepo(storageRuntime, dir);
      return new IsogitStore(storageRuntime, dir, signer, gcPolicy);
    },
    initRepo: (dir: string, opts?: Parameters<typeof initRepo>[2]) =>
      initRepo(storageRuntime, dir, opts),
    initAgentRepo: (dir: string) => initAgentRepo(storageRuntime, dir),
    switchBranch: (dir: string, ref: string) =>
      switchBranch(storageRuntime, dir, ref),
    createAndSwitchBranch: (dir: string, name: string) =>
      createAndSwitchBranch(storageRuntime, dir, name),
    currentBranch: (dir: string) => currentBranch(storageRuntime, dir),
    listBranches: (dir: string) => listBranches(storageRuntime, dir),
    logHistory: (dir: string, limit?: number) =>
      logHistory(storageRuntime, dir, limit),
    applyPack: (...args: DropFirst<Parameters<typeof applyPack>>) =>
      applyPack(storageRuntime, ...args),
    receivePackObjects: (
      ...args: DropFirst<Parameters<typeof receivePackObjects>>
    ) => receivePackObjects(storageRuntime, ...args),
    createDeployPack: (
      ...args: DropFirst<Parameters<typeof createDeployPack>>
    ) => createDeployPack(storageRuntime, ...args),
    createNegotiatedPack: (
      ...args: DropFirst<Parameters<typeof createNegotiatedPack>>
    ) => createNegotiatedPack(storageRuntime, ...args),
    collectReachableObjects: (
      ...args: DropFirst<Parameters<typeof collectReachableObjects>>
    ) => collectReachableObjects(storageRuntime, ...args),
    repoDiskUsage: (dir: string) => repoDiskUsage(storageRuntime, dir),
    countLooseObjects: (dir: string) => countLooseObjects(storageRuntime, dir),
    countPackFiles: (dir: string) => countPackFiles(storageRuntime, dir),
    gitBytes: (dir: string) => gitBytes(storageRuntime, dir),
    listRepoRefs: (dir: string) => listRepoRefs(storageRuntime, dir),
    runGC: (...args: DropFirst<Parameters<typeof runGC>>) =>
      runGC(storageRuntime, ...args),
    maybeGC: (...args: DropFirst<Parameters<typeof maybeGC>>) =>
      maybeGC(storageRuntime, ...args),
    createMailAuditStore: (
      ...args: DropFirst<Parameters<typeof createMailAuditStore>>
    ) => createMailAuditStore(storageRuntime, ...args),
    listMail: (dir: string) => listMail(storageRuntime, dir),
  };
}

type DropFirst<T extends readonly unknown[]> = T extends readonly [
  unknown,
  ...infer R,
]
  ? R
  : never;
