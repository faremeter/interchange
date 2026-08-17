import type { AuditStore, ContextStore } from "@intx/types/runtime";

import {
  createIsogitStorage,
  type CommitSigner,
  type DurableMirrorReads,
  type GCPolicy,
} from "./index";
import { createNodeIsogitRuntime } from "./node-runtime";
import { IsogitStore as RuntimeIsogitStore } from "./store";
export { createNodeIsogitRuntime } from "./node-runtime";
export {
  countLooseObjects,
  countPackFiles,
  gitBytes,
  repoDiskUsage,
} from "./node-metrics";

export {
  createIsogitStorage,
  type CommitSigner,
  type DurableMirrorReads,
  type GCPolicy,
  type ContextStore,
  type AuditStore,
  type CommitVerifier,
  type TreeValidator,
  type TreeValidatorResult,
  type IncludeShaPredicate,
  type InitRepoOpts,
  type RetentionPolicy,
  type GCResult,
  type RepoDiskUsage,
  type MailAuditStore,
  type MailCommitOptions,
  type MailCommitResult,
  type MailDirection,
  type MailEntry,
  type IsogitPath,
  type IsogitRuntime,
} from "./index";

export const runtime = createNodeIsogitRuntime();
const storage = createIsogitStorage(runtime);
const { storageRuntime } = storage;

export const {
  applyPack,
  collectReachableObjects,
  createAndSwitchBranch,
  createDeployPack,
  createMailAuditStore,
  createNegotiatedPack,
  currentBranch,
  initAgentRepo,
  initRepo,
  listBranches,
  listMail,
  listRepoRefs,
  logHistory,
  maybeGC,
  receivePackObjects,
  runGC,
  switchBranch,
  writeTreeToDisk,
  indexPackIntoGitDir,
} = storage;

export class IsogitStore extends RuntimeIsogitStore {
  constructor(dir: string, signer?: CommitSigner, gcPolicy?: GCPolicy) {
    super(storageRuntime, storageRuntime.path.resolve(dir), signer, gcPolicy);
  }
}

export async function createIsogitStore(
  dir: string,
  signer?: CommitSigner,
  gcPolicy?: GCPolicy,
): Promise<ContextStore & AuditStore & DurableMirrorReads> {
  return storage.createIsogitStore(dir, signer, gcPolicy);
}
