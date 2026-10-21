import type { WorkflowDefinitionArtifactStore } from "@intx/db";
import { getLogger } from "@intx/log";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type { ToolPackageManifest } from "@intx/types/tool-packages";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";

import type { WorkflowProbeResult } from "./ws/sidecar-handler";

// Bump when probe semantics or the stored result shape changes. Probe workers
// must use compatible evaluator code and environments within this version.
const WORKFLOW_PROBE_CACHE_VERSION = 4;

export type WorkflowProbeLeaseTiming = {
  readonly leaseMs: number;
  readonly renewMs: number;
  readonly waitMs: number;
};

export const DEFAULT_PROBE_LEASE_TIMING: WorkflowProbeLeaseTiming = {
  leaseMs: 60_000,
  renewMs: 15_000,
  waitMs: 250,
};

export type WorkflowProbeCache = {
  readonly store: WorkflowDefinitionArtifactStore;
  readonly tenantId: string;
  readonly registryKey: string;
  readonly lease: WorkflowProbeLeaseTiming;
};

const logger = getLogger(["hub-sessions", "workflow-probe-cache"]);

type WorkflowRevisionSelector = {
  readonly assetId: string;
  readonly assetCommitSha: string;
  readonly source: WorkflowDefinitionSource;
  readonly entry: string;
  readonly pin?: string;
};

/**
 * A database lease coordinates probes across Hub processes without holding a
 * transaction open during provisioning. Dependency ranges resolve once per
 * source revision, and a probe that does not pass deletes that revision so the
 * next request resolves them again. Only results that pass `publishable` are
 * stored, and `artifactId` is set only when the result came from, or was
 * published to, the cache.
 */
export async function withCachedWorkflowProbe<T>(args: {
  cache: WorkflowProbeCache;
  revision: WorkflowRevisionSelector;
  resolveClosure(): Promise<ToolPackageManifest>;
  probe(closure: ToolPackageManifest): Promise<WorkflowProbeResult>;
  validate(
    result: WorkflowProbeResult,
    closure: ToolPackageManifest,
  ): Promise<T>;
  publishable(value: T): boolean;
}): Promise<{ value: T; artifactId?: string }> {
  const { store, lease, tenantId, registryKey } = args.cache;
  const { revision } = args;
  // Bump schemaVersion when the revision inputs change shape, so old rows are
  // never read under the new meaning.
  const revisionId = await computeWireDefinitionHash({
    schemaVersion: 1,
    tenantId,
    assetId: revision.assetId,
    assetCommitSha: revision.assetCommitSha,
    source: revision.source,
    entry: revision.entry,
    pin: revision.pin,
    registryKey,
  });
  const id = await computeWireDefinitionHash({
    schemaVersion: WORKFLOW_PROBE_CACHE_VERSION,
    revisionId,
  });

  async function ensureRevision() {
    if (await store.hasRevision(revisionId)) return;
    await store.createRevision({
      id: revisionId,
      tenantId,
      assetId: revision.assetId,
      assetCommitSha: revision.assetCommitSha,
      source: revision.source,
      entry: revision.entry,
      closure: await args.resolveClosure(),
    });
  }

  // A deleted revision can be created again under the same id with another
  // closure. An artifact row keeps its revision, so read the closure only
  // while one is held.
  async function pinnedClosure() {
    const stored = await store.getRevision(revisionId);
    if (stored === null) {
      throw new Error(
        `Workflow definition revision ${revisionId} is missing while artifact ${id} references it`,
      );
    }
    return stored.closure;
  }

  await ensureRevision();
  const owner = crypto.randomUUID();
  let awaitedOwner: string | null = null;
  let closure: ToolPackageManifest;
  for (;;) {
    const artifact = await store.get(id);
    if (artifact?.result != null) {
      return {
        value: await args.validate(artifact.result, await pinnedClosure()),
        artifactId: id,
      };
    }
    // The owner we waited on deleted its claim after a failed probe, or lost
    // it after a crash. Either way it will not publish.
    const awaitedGone =
      awaitedOwner !== null && artifact?.leaseOwner !== awaitedOwner;
    const claim = await store.claim({
      id,
      revisionId,
      owner,
      leaseMs: lease.leaseMs,
    });
    if (claim === "claimed") {
      closure = await pinnedClosure();
      break;
    }
    if (claim === "revision_missing") {
      await ensureRevision();
      continue;
    }
    if (awaitedGone) {
      // Another request already took over the retry. Probe here instead of
      // queueing behind an attempt that may fail the same way. Its claim keeps
      // the revision unless it was released in the meantime.
      const stored = await store.getRevision(revisionId);
      if (stored === null) continue;
      return {
        value: await args.validate(
          await args.probe(stored.closure),
          stored.closure,
        ),
      };
    }
    awaitedOwner = artifact?.leaseOwner ?? null;
    await new Promise<void>((resolve) => setTimeout(resolve, lease.waitMs));
  }

  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let renewal = Promise.resolve();
  let lostLease = false;
  function scheduleRenewal() {
    timer = setTimeout(() => {
      renewal = store
        .renew(id, owner, lease.leaseMs)
        .then(
          (owned) => {
            lostLease = !owned;
          },
          (error: unknown) => {
            // Retry on the next tick. A lease that expires meanwhile is
            // reported by that renewal, not guessed from this error.
            logger.warn`Workflow probe artifact ${id} lease renewal failed: ${String(error)}`;
          },
        )
        .then(() => {
          if (!stopped && !lostLease) scheduleRenewal();
        });
    }, lease.renewMs);
  }
  async function stopRenewal() {
    stopped = true;
    clearTimeout(timer);
    await renewal;
  }
  async function abandon() {
    try {
      await store.abandon(id, owner);
    } catch {
      // An unavailable database leaves a claim that another request can
      // recover when its lease expires.
    }
  }
  // Freezing exists to keep a validated closure stable. A closure that never
  // passed is dropped so a fixed dependency release can be picked up.
  async function abandonRevision() {
    await abandon();
    try {
      await store.deleteRevision(revisionId);
    } catch (error) {
      logger.warn`Workflow definition revision ${revisionId} was kept after a failed probe: ${String(error)}`;
    }
  }

  scheduleRenewal();
  try {
    const result = await args.probe(closure);
    const value = await args.validate(result, closure);
    await stopRenewal();
    if (!args.publishable(value)) {
      await abandonRevision();
      return { value };
    }
    // Publication is an optimization. A validated result still deploys when
    // the cache cannot record it.
    let reason = "lease lost before publication";
    if (!lostLease) {
      try {
        if (await store.complete(id, owner, result)) {
          return { value, artifactId: id };
        }
      } catch (error) {
        reason = String(error);
      }
    }
    logger.warn`Workflow probe artifact ${id} was not published: ${reason}`;
    await abandon();
    return { value };
  } catch (error) {
    await abandonRevision();
    throw error;
  } finally {
    await stopRenewal();
  }
}
