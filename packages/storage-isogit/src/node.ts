import type { AuditStore, ContextStore } from "@intx/types/runtime";

import {
  createIsogitStorage,
  type CommitSigner,
  type DurableMirrorReads,
  type GCPolicy,
} from "./index";
import { createNodeIsogitRuntime } from "./node-runtime";
import { IsogitStore as RuntimeIsogitStore } from "./store";
import { assertPackInflationWithinBounds } from "./pack-inflation-guard";
import type { PackMaterializationLimits } from "./materialization-limits";
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
  DEFAULT_PACK_MATERIALIZATION_LIMITS,
  type PackMaterializationLimits,
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
  indexPackIntoGitDir: rawIndexPackIntoGitDir,
} = storage;

/**
 * The node build guards `indexPackIntoGitDir` with a per-object inflation scan
 * (`assertPackInflationWithinBounds`) before it indexes, so every node caller
 * that materializes an untrusted pushed pack is protected against a zip bomb
 * without having to remember a separate call. The browser build indexes only
 * its own repos and needs no such guard.
 */
export async function indexPackIntoGitDir(
  gitDir: string,
  pack: Uint8Array,
  commitSha: string,
  limits: PackMaterializationLimits,
): Promise<void> {
  await assertPackInflationWithinBounds(pack, limits);
  await rawIndexPackIntoGitDir(gitDir, pack, commitSha, limits);
}

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
