// Per-agent on-disk repo operations for supervised deployments.
//
// The in-process session runtime -- harness construction, agent
// provisioning, disk restore, and per-agent mail audit -- has been
// retired: every agent now runs as a supervised workflow-process child
// on the workflow-run substrate. What remains here is the thin
// serialization layer over the agent repo store that the deploy path
// and the hub-link still call: deploy/asset-pack applies, state-pack
// reads, deploy-ref reads, and directory teardown, each run
// one-at-a-time per agent so a teardown never races an in-flight git op.

import path from "node:path";

import type { AgentRepoStore } from "./agent-repo-store";
import { applyAssetPack as applyAssetPackFn } from "./apply-asset-pack";

export type SessionManagerConfig = {
  repoStore: AgentRepoStore;
};

export type SessionManager = {
  /**
   * Initialize the on-disk deploy-tree repo for an address. A single-step
   * workflow deploy uses this at the head so the follow-up deploy-pack
   * apply has a repo to apply into.
   */
  initRepo(address: string): Promise<void>;
  /**
   * Apply a deploy pack to the agent's repo. Thin wrapper around
   * AgentRepoStore for callers that already have a SessionManager handle.
   */
  applyDeployPack(
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    transferId: string,
    verifyCommit?: (payload: string, signature: string) => Promise<boolean>,
  ): Promise<void>;
  /**
   * Materialize an asset pack at `<workspaceRoot>/<mountPath>/` for the
   * agent. The workspace root is per-agent; this is distinct from the
   * agent's deploy git tree. Asset packs are unsigned in v1 -- no
   * `verifyCommit` parameter.
   */
  applyAssetPack(
    agentAddress: string,
    mountPath: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
  ): Promise<void>;
  createStatePack(
    agentAddress: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }>;
  deleteAgentDir(agentAddress: string): Promise<void>;
  getDeployRef(agentAddress: string): Promise<string | null>;
  /**
   * Session addresses this manager hosts. The in-process session runtime
   * is retired, so this is always empty; the hub-link ships it in the
   * register frame alongside the sidecar's workflow-deployment addresses.
   */
  getAddresses(): string[];
  /**
   * Session id for an address' outbound mail forwarding. Always undefined
   * now that no in-process sessions exist; the hub-link tolerates a
   * missing id and forwards the mail without one.
   */
  getSessionId(agentAddress: string): string | undefined;
};

export function createSessionManager(
  config: SessionManagerConfig,
): SessionManager {
  const { repoStore } = config;

  // Per-agent promise chain that serializes the operations against an agent's
  // on-disk directory -- state-pack and deploy-ref reads and deploy/asset-pack
  // applies all run one-at-a-time per agent. The chain exists for teardown:
  // drainRepoOps awaits it before deleting the directory, so an operation that
  // was valid when it started never runs against a path that has since
  // vanished underneath it. Serializing additionally avoids corruption for the
  // members that share the agent's `.git/` object store (state-pack and
  // deploy-ref reads, deploy-pack applies), which isogit, lacking a
  // cross-process lock, would otherwise let interleave. Asset-pack applies are
  // on the chain only for the teardown reason -- they materialize into a
  // workspace subtree, not the agent repo's object store.
  const repoOpQueues = new Map<string, Promise<void>>();

  function runRepoOp<T>(
    agentAddress: string,
    fn: () => Promise<T>,
  ): Promise<T> {
    const prev = repoOpQueues.get(agentAddress) ?? Promise.resolve();
    // Run fn once prev settles. prev is either the initial Promise.resolve()
    // or the rejection-swallowing tail stored below, so it never rejects;
    // passing fn as both the fulfilled and rejected handler keeps this op
    // independent of that detail and guarantees fn runs exactly once after the
    // previous op completes.
    const result = prev.then(fn, fn);
    // Store a rejection-swallowing tail so one failed op does not poison the
    // chain for the next caller. The caller still observes this op's own
    // result or rejection through `result`.
    repoOpQueues.set(
      agentAddress,
      result.then(
        () => undefined,
        () => undefined,
      ),
    );
    return result;
  }

  // Await the agent's current operation chain so teardown removes the
  // directory only after in-flight git work finishes. Capturing the tail and
  // clearing the entry means an op enqueued AFTER this point starts a fresh
  // chain this drain does not await. That is safe only because every caller
  // invokes runRepoOp synchronously, before its first await, inside the
  // serialized frame dispatch -- so by the time a later agent.undeploy frame
  // reaches deleteAgentDir, every racing op is already on the chain. A handler
  // that deferred its runRepoOp call past an await would reopen the
  // delete-under-in-flight-op race.
  async function drainRepoOps(agentAddress: string): Promise<void> {
    const inflight = repoOpQueues.get(agentAddress);
    repoOpQueues.delete(agentAddress);
    if (inflight !== undefined) await inflight;
  }

  async function applyDeployPack(
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    transferId: string,
    verifyCommit?: (payload: string, signature: string) => Promise<boolean>,
  ): Promise<void> {
    const args =
      verifyCommit !== undefined
        ? {
            address: agentAddress,
            pack,
            ref,
            commitSha,
            transferId,
            verifyCommit,
          }
        : { address: agentAddress, pack, ref, commitSha, transferId };
    await runRepoOp(agentAddress, () => repoStore.applyDeployPack(args));
  }

  async function applyAssetPack(
    agentAddress: string,
    mountPath: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
  ): Promise<void> {
    const workspaceRoot = path.join(
      repoStore.getAgentDir(agentAddress),
      "workspace",
    );
    await runRepoOp(agentAddress, () =>
      applyAssetPackFn({
        workspaceRoot,
        mountPath,
        pack,
        ref,
        commitSha,
      }),
    );
  }

  async function createStatePack(
    agentAddress: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }> {
    return runRepoOp(agentAddress, () =>
      repoStore.createStatePack(agentAddress),
    );
  }

  async function deleteAgentDir(agentAddress: string): Promise<void> {
    await drainRepoOps(agentAddress);
    await repoStore.remove(agentAddress);
  }

  async function getDeployRef(agentAddress: string): Promise<string | null> {
    return runRepoOp(agentAddress, () => repoStore.getDeployRef(agentAddress));
  }

  return {
    initRepo: (address: string) => repoStore.initRepo(address),
    applyDeployPack,
    applyAssetPack,
    createStatePack,
    deleteAgentDir,
    getDeployRef,
    getAddresses: () => [],
    getSessionId: () => undefined,
  };
}
