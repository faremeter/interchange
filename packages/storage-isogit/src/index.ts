import type { ContextStore, AuditStore } from "@intx/types/runtime";
import { initAgentRepo } from "./init";
import { IsogitStore, type DurableMirrorReads } from "./store";
import type { CommitSigner } from "./signer";
import type { GCPolicy } from "./gc";

export type { ContextStore, AuditStore, CommitSigner };
export type {
  CommitVerifier,
  TreeValidator,
  TreeValidatorResult,
} from "./pack-receive";
export { IsogitStore };
export type { DurableMirrorReads };
export {
  switchBranch,
  createAndSwitchBranch,
  currentBranch,
  listBranches,
  logHistory,
} from "./history";
export { initRepo, initAgentRepo, type InitRepoOpts } from "./init";
export { applyPack, receivePackObjects } from "./pack-receive";
export {
  createDeployPack,
  createNegotiatedPack,
  type IncludeShaPredicate,
} from "./pack-send";
export { collectReachableObjects } from "./object-walk";
export {
  repoDiskUsage,
  listRepoRefs,
  gitBytes,
  countLooseObjects,
  countPackFiles,
  type RepoDiskUsage,
} from "./repo-disk";
export {
  runGC,
  maybeGC,
  type RetentionPolicy,
  type GCResult,
  type GCPolicy,
} from "./gc";
export {
  createMailAuditStore,
  listMail,
  type MailAuditStore,
  type MailCommitOptions,
  type MailDirection,
  type MailCommitResult,
  type MailEntry,
} from "./mail-store";

/**
 * Initialize an agent repository at `dir` and return a store backed by that
 * repository. The returned object implements both ContextStore (inference
 * state) and AuditStore (tool authorization records).
 *
 * When `gcPolicy` is supplied, each commit reclaims the repo on the write
 * path once it crosses the policy's thresholds.
 */
export async function createIsogitStore(
  dir: string,
  signer?: CommitSigner,
  gcPolicy?: GCPolicy,
): Promise<ContextStore & AuditStore & DurableMirrorReads> {
  await initAgentRepo(dir);
  return new IsogitStore(dir, signer, gcPolicy);
}
