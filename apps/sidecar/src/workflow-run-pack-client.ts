// Workflow-run pack push client.
//
// Sits between the boot-edge substrate facade and the hub-link's
// `pushWorkflowRunPack` wire surface. The facade fires this client
// after a successful supervisor-authored `writeTreePreservingPrefix`
// against a `workflow-run` repo; the client builds the new pack via
// `RepoStore.createPack` under the supervisor principal and ships it
// over the hub link. The hub routes the pack to its workflow-run
// receiver because `repoId.kind === "workflow-run"`.
//
// The client does NOT mint a fresh signing key, transferId, or
// principal kind. The principal is the same `WorkflowRunSupervisorPrincipal`
// shape the supervisor uses for `writeTreePreservingPrefix`; the
// transferId is minted inside `HubLink.pushWorkflowRunPack`.

import { type } from "arktype";

import { getLogger } from "@intx/log";
import { SourcesUpdatedData } from "@intx/workflow-host";
import type { InferenceSource } from "@intx/types/runtime";
import type {
  RepoId,
  RepoStore,
  WorkflowRunSupervisorPrincipal,
} from "@intx/hub-sessions";
import type { HubLink } from "@intx/hub-agent";

const logger = getLogger([
  "interchange",
  "sidecar",
  "workflow-run-pack-client",
]);

export type WorkflowRunPackClient = {
  /**
   * Build a pack of the workflow-run repo at `ref` and ship it to the
   * hub. Resolves on the hub's `repo.pack.ack`; rejects on
   * `repo.pack.reject`, on a disconnect that cancels the in-flight
   * transfer, or on a substrate-side `createPack` failure. The push
   * failure shape is intentionally loud per the project's
   * defensive-coding rule.
   */
  push(opts: {
    agentAddress: string;
    repoId: RepoId;
    ref: string;
  }): Promise<void>;
};

export type CreateWorkflowRunPackClientOpts = {
  substrate: RepoStore;
  hubLink: Pick<HubLink, "pushWorkflowRunPack">;
};

export function createWorkflowRunPackClient(
  opts: CreateWorkflowRunPackClientOpts,
): WorkflowRunPackClient {
  const { substrate, hubLink } = opts;
  return {
    async push({ agentAddress, repoId, ref }) {
      if (repoId.kind !== "workflow-run") {
        throw new Error(
          `workflow-run pack client: repoId.kind must be "workflow-run", got ${JSON.stringify(repoId.kind)}`,
        );
      }
      const principal: WorkflowRunSupervisorPrincipal = {
        kind: "supervisor",
        deploymentId: repoId.id,
      };
      const { pack, commitSha } = await substrate.createPack(
        principal,
        repoId,
        ref,
      );
      await hubLink.pushWorkflowRunPack({
        agentAddress,
        repoId,
        pack,
        ref,
        commitSha,
      });
    },
  };
}

/**
 * Mapping registry the boot-edge substrate facade consults to resolve
 * `repoId.id` (the workflow-run deploymentId, which the deploy router
 * derives by slugging the agent's mail address) back into the
 * agentAddress carried on every outbound pack frame. Populated by the
 * deploy router as each `agent.deploy` frame lands.
 */
export type DeploymentAddressRegistry = {
  record(deploymentId: string, agentAddress: string): void;
  resolve(deploymentId: string): string | null;
  unregister(deploymentId: string): void;
};

export function createDeploymentAddressRegistry(): DeploymentAddressRegistry {
  const table = new Map<string, string>();
  return {
    record(deploymentId, agentAddress) {
      table.set(deploymentId, agentAddress);
    },
    resolve(deploymentId) {
      return table.get(deploymentId) ?? null;
    },
    unregister(deploymentId) {
      table.delete(deploymentId);
    },
  };
}

/**
 * Handler the multi-step deploy router installs on the
 * `MultistepMailRouter` after a supervisor's `spawn` succeeds. The
 * handler hands a delivered inbound mail off to the per-deployment
 * supervisor's `routeInbound`, which dispatches into the workflow-host
 * mail-bus the multi-step child's `awaitSignal` subscribes against.
 */
export type MultistepMailHandler = (message: Uint8Array) => void;

/**
 * Per-deployment-address mail handler registry the sidecar hub-link
 * consults before falling back to `transport.deliver`. The multi-step
 * deploy router registers a handler against the deployment's mail
 * address after `wired.supervisor.spawn` succeeds, so an inbound
 * `mail.inbound` frame for that address dispatches into the
 * supervisor's mail-bus subscription rather than the
 * never-provisioned-for-this-address transport mailbox.
 *
 * The registry is owned at the sidecar's host layer (not inside the
 * workflow-host library) because the routing decision is between
 * "legacy single-agent path" and "supervisor mail-bus path" -- two
 * concrete sidecar host concerns. The workflow-host package stays
 * agnostic to which transport surface its mail-bus rides on.
 */
export type MultistepMailRouter = {
  register(address: string, handler: MultistepMailHandler): void;
  unregister(address: string): void;
  tryRoute(address: string, message: Uint8Array): boolean;
};

export function createMultistepMailRouter(): MultistepMailRouter {
  const handlers = new Map<string, MultistepMailHandler>();
  return {
    register(address, handler) {
      handlers.set(address, handler);
    },
    unregister(address) {
      handlers.delete(address);
    },
    tryRoute(address, message) {
      const handler = handlers.get(address);
      if (handler === undefined) return false;
      handler(message);
      return true;
    },
  };
}

/**
 * Per-deployment signal-delivery handler the multi-step deploy router
 * installs against the `MultistepSignalRouter` after a supervisor's
 * `spawn` succeeds. The handler hands the signal off to the supervisor's
 * `deliverSignal`, which sends a `signal.deliver` control IPC frame to
 * the workflow-process child. Routing every workflow-run signal through
 * the child keeps the workflow-run repo's single-writer invariant
 * intact -- the child is the only writer of `runs/<runId>/events/` on
 * the sidecar side, so the pack-push pipeline that propagates the
 * commit to the hub never races against a concurrent host-side write.
 */
export type MultistepSignalHandler = (args: {
  runId: string;
  signalName: string;
  signalId: string;
  payload: unknown;
}) => Promise<void>;

/**
 * Per-deployment-address signal handler registry the sidecar hub-link
 * consults on every inbound `signal.deliver` frame. The deploy router
 * registers a handler against the deployment's mail address after
 * `wired.supervisor.spawn` succeeds, for single-step and multi-step
 * deployments alike; the handler dispatches the signal into the
 * supervisor's `deliverSignal`.
 *
 * The registry lives at the sidecar's host layer (not inside the
 * workflow-host library) for the same boundary reason as
 * `MultistepMailRouter`: the routing decision is a concrete sidecar
 * host concern, and the workflow-host package stays agnostic to which
 * transport surface its supervisor handle rides on.
 */
export type MultistepSignalRouter = {
  register(address: string, handler: MultistepSignalHandler): void;
  unregister(address: string): void;
  tryRoute(frame: {
    type: "signal.deliver";
    agentAddress: string;
    runId: string;
    signalName: string;
    signalId: string;
    payload: unknown;
  }): Promise<boolean>;
};

export function createMultistepSignalRouter(): MultistepSignalRouter {
  const handlers = new Map<string, MultistepSignalHandler>();
  return {
    register(address, handler) {
      handlers.set(address, handler);
    },
    unregister(address) {
      handlers.delete(address);
    },
    async tryRoute(frame) {
      const handler = handlers.get(frame.agentAddress);
      if (handler === undefined) return false;
      await handler({
        runId: frame.runId,
        signalName: frame.signalName,
        signalId: frame.signalId,
        payload: frame.payload,
      });
      return true;
    },
  };
}

/**
 * Per-deployment drain handler the multi-step deploy router installs
 * against the `MultistepDrainRouter` after a supervisor's `spawn`
 * succeeds. The handler hands the drain opts off to the supervisor's
 * `drain`, which sends a `drain` control IPC frame to the
 * workflow-process child and arms one `drainTimeout` accumulator per
 * in-flight run. Cancel-mode in-flight steps abort as the child's
 * controller signal flips; wait-mode steps continue. Each accumulator
 * commits a signed `CancelRequested{origin: "supervisor-drain"}`
 * against the workflow-run repo when the deadline expires.
 */
export type MultistepDrainHandler = (args: {
  deadlineMs: number;
}) => Promise<void>;

/**
 * Per-deployment-address drain handler registry the sidecar hub-link
 * consults on every inbound `drain.deliver` frame. The deploy router
 * registers a handler against the deployment's mail address after
 * `wired.supervisor.spawn` succeeds, for single-step and multi-step
 * deployments alike; the handler dispatches into the supervisor's
 * `drain`.
 *
 * The registry lives at the sidecar's host layer (not inside the
 * workflow-host library) for the same boundary reason as
 * `MultistepMailRouter` / `MultistepSignalRouter`: the routing decision
 * is a concrete sidecar host concern, and the workflow-host package
 * stays agnostic to which transport surface its supervisor handle
 * rides on.
 */
export type MultistepDrainRouter = {
  register(address: string, handler: MultistepDrainHandler): void;
  unregister(address: string): void;
  tryRoute(frame: {
    type: "drain.deliver";
    agentAddress: string;
    deadlineMs: number;
  }): Promise<boolean>;
};

export function createMultistepDrainRouter(): MultistepDrainRouter {
  const handlers = new Map<string, MultistepDrainHandler>();
  return {
    register(address, handler) {
      handlers.set(address, handler);
    },
    unregister(address) {
      handlers.delete(address);
    },
    async tryRoute(frame) {
      const handler = handlers.get(frame.agentAddress);
      if (handler === undefined) return false;
      await handler({ deadlineMs: frame.deadlineMs });
      return true;
    },
  };
}

/**
 * Per-deployment sources-rotation handler the deploy router installs
 * against the `MultistepSourcesRouter` after a supervisor's `spawn`
 * succeeds -- but ONLY for a single-step (warm launched-agent)
 * deployment. The handler hands the rotated list off to the supervisor's
 * `deliverSources`, which sends a `sources-updated` control IPC frame to
 * the workflow-process child, where the warm agent's live sources are
 * swapped in place. A multi-step deployment has no single warm agent to
 * rotate, so the router registers no handler for it and an inbound
 * `sources.update` for a multi-step address is unrouted.
 */
export type MultistepSourcesHandler = (args: {
  sources: InferenceSource[];
  defaultSource: string;
}) => Promise<void>;

/**
 * Per-deployment-address sources-rotation handler registry. Only a
 * single-step warm deployment registers a handler (after
 * `wired.supervisor.spawn` succeeds); a multi-step deployment never
 * does, so `tryRoute` resolves a rotation only for a registered
 * single-step address and returns `false` for any other.
 *
 * The registry lives at the sidecar's host layer for the same boundary
 * reason as the mail/signal/drain routers: the routing decision is a
 * concrete sidecar host concern, and the workflow-host package stays
 * agnostic to which transport surface its supervisor handle rides on.
 */
export type MultistepSourcesRouter = {
  register(address: string, handler: MultistepSourcesHandler): void;
  unregister(address: string): void;
  tryRoute(frame: {
    type: "sources.update";
    agentAddress: string;
    sources: InferenceSource[];
    defaultSource: string;
  }): Promise<boolean>;
};

export function createMultistepSourcesRouter(): MultistepSourcesRouter {
  const handlers = new Map<string, MultistepSourcesHandler>();
  return {
    register(address, handler) {
      handlers.set(address, handler);
    },
    unregister(address) {
      handlers.delete(address);
    },
    async tryRoute(frame) {
      const handler = handlers.get(frame.agentAddress);
      // Registration check first: an unregistered (multi-step or torn-down)
      // address is unrouted -- reported as `false`, its payload never
      // inspected, because it would not be acted on regardless.
      if (handler === undefined) return false;
      // Validate the rotation BEFORE dispatch. This is the only inbound
      // router that validates its frame, and deliberately so: a bad list
      // (duplicate ids, or a default that is not the head element) would
      // reach the child's control-channel receiver and crash it on
      // `SourcesUpdatedData`'s narrow -- the sources-updated frame is the
      // only inbound frame carrying a crash-on-invalid narrow downstream,
      // and the only one that is request/ack. Rejecting here throws, and
      // the hub-link turns the throw into a truthful `session.error`
      // instead of acking and detonating the child.
      const validated = SourcesUpdatedData({
        sources: frame.sources,
        defaultSource: frame.defaultSource,
      });
      if (validated instanceof type.errors) {
        throw new Error(validated.summary);
      }
      await handler({
        sources: frame.sources,
        defaultSource: frame.defaultSource,
      });
      return true;
    },
  };
}

/**
 * Boot-edge facade around the substrate-shaped `RepoStore`. Forwards
 * every method to the underlying store; intercepts the
 * `writeTreePreservingPrefix` return path so a successful write
 * against a `workflow-run` repo schedules a workflow-run pack push.
 * Writes against any other `repoId.kind` (today, only `agent-state`
 * via the deploy-applier path) flow through unchanged.
 *
 * Pack-push coalescing: the facade returns from
 * `writeTreePreservingPrefix` as soon as the LOCAL commit lands and
 * schedules an asynchronous pack push for `(repoId.id, ref)`. At
 * most one push per (repoId, ref) is in flight at a time. Writes
 * that arrive while a push is in flight are NOT enqueued as
 * additional pushes; instead they mark the slot as "dirty", and the
 * loop runs one more push after the current one settles. This means
 * a burst of N writes against the same ref produces at most 2
 * pushes (the one already running when the burst starts, plus one
 * more for everything that arrived during it), rather than N
 * serial round-trips' worth of hub-ack latency. The push body
 * captures the current local ref tip at the moment it runs, so the
 * single pack it builds covers every commit landed since the prior
 * shipped tip -- the substrate's incremental `createPack` already
 * walks the chain from the prior `lastPackedTip` forward, so the
 * receiver still sees every commit transition.
 *
 * Single-writer + FIFO correctness: the underlying substrate
 * serialises local writes via `withRepoLock`, so commits land on
 * disk in submission order. The hub's `receivePack` validates each
 * commit's parent against its existing-commits set; as long as the
 * pack carries the full chain from prior shipped tip to current
 * tip (which `createPack` does for `workflow-run` repos), every
 * intermediate commit is validated by the receiver. Coalescing
 * multiple local commits into one network push therefore preserves
 * the receive-time CAS invariant while collapsing N hub round-trips
 * into 1.
 *
 * Failure surfacing: a failed push latches its error on the
 * per-(repoId, ref) slot's `lastError` field. The next call to
 * `writeTreePreservingPrefix` on that (repoId, ref) re-throws the
 * latched error before doing its own work, keeping failures loud
 * rather than swallowed by the fire-and-forget pipeline. The
 * defensive-coding rule says errors must surface; this is how they
 * surface from a coalescing writer.
 *
 * Flush: callers that need a hub-visible barrier (shutdown,
 * integration tests that read hub-side state) call
 * `flushWorkflowRunPushes(repoId, ref)` to await the per-(repoId,
 * ref) slot to drain (both the in-flight push and any follow-up
 * triggered by writes that arrived during it).
 */
export type WorkflowRunPackPushingRepoStoreOpts = {
  underlying: RepoStore;
  packClient: WorkflowRunPackClient;
  registry: DeploymentAddressRegistry;
};

/**
 * The wrapped store plus a side-channel API for waiting on the
 * per-(repoId.id, ref) pack-push pipeline to drain. The `RepoStore`
 * shape is unchanged so call sites that consume `RepoStore` keep
 * working; `flushWorkflowRunPushes` is opt-in for code that
 * genuinely needs hub-side visibility (shutdown, integration tests,
 * end-to-end benchmarks). Call sites that don't need it pay zero
 * cost.
 */
export type WorkflowRunPackPushingRepoStore = RepoStore & {
  /**
   * Await the pack-push pipeline for `(repoId.id, ref)` to drain.
   * Resolves once no push is in flight and no follow-up push is
   * pending; rejects if the most recent push failed (the same
   * latched error the next `writeTreePreservingPrefix` call would
   * surface).
   */
  flushWorkflowRunPushes: (repoId: RepoId, ref: string) => Promise<void>;
};

export function createWorkflowRunPackPushingRepoStore(
  opts: WorkflowRunPackPushingRepoStoreOpts,
): WorkflowRunPackPushingRepoStore {
  const { underlying, packClient, registry } = opts;

  type Slot = {
    agentAddress: string;
    inFlight: Promise<void> | null;
    dirty: boolean;
    lastError: Error | null;
    settled: (() => void)[];
  };
  const slots = new Map<string, Slot>();
  function slotKey(repoId: RepoId, ref: string): string {
    return `${repoId.kind}/${repoId.id}/${ref}`;
  }

  function notifySettled(slot: Slot): void {
    const callbacks = slot.settled;
    slot.settled = [];
    for (const cb of callbacks) cb();
  }

  function startLoop(slot: Slot, repoId: RepoId, ref: string): void {
    if (slot.inFlight !== null) return;
    slot.inFlight = (async () => {
      while (slot.dirty) {
        slot.dirty = false;
        try {
          await packClient.push({
            agentAddress: slot.agentAddress,
            repoId,
            ref,
          });
          slot.lastError = null;
        } catch (cause) {
          const msg = cause instanceof Error ? cause.message : String(cause);
          logger.warn`workflow-run pack push failed for deployment ${repoId.id} (${slot.agentAddress}): ${msg}`;
          slot.lastError =
            cause instanceof Error ? cause : new Error(String(cause));
        }
      }
      slot.inFlight = null;
      notifySettled(slot);
    })();
  }

  function schedulePush(
    agentAddress: string,
    repoId: RepoId,
    ref: string,
  ): void {
    const key = slotKey(repoId, ref);
    let slot = slots.get(key);
    if (slot === undefined) {
      slot = {
        agentAddress,
        inFlight: null,
        dirty: false,
        lastError: null,
        settled: [],
      };
      slots.set(key, slot);
    } else {
      // The agentAddress is derived from a stable per-deployment
      // mapping; refreshing it on every call keeps the slot in sync
      // if the registry ever re-resolves the same deploymentId to
      // a different address (today it does not, but the contract is
      // "look up at push time", not "cache forever").
      slot.agentAddress = agentAddress;
    }
    slot.dirty = true;
    startLoop(slot, repoId, ref);
  }

  function takeLatchedError(repoId: RepoId, ref: string): Error | null {
    const slot = slots.get(slotKey(repoId, ref));
    if (slot === undefined) return null;
    const err = slot.lastError;
    if (err !== null) slot.lastError = null;
    return err;
  }

  async function flushWorkflowRunPushes(
    repoId: RepoId,
    ref: string,
  ): Promise<void> {
    const slot = slots.get(slotKey(repoId, ref));
    if (slot === undefined) return;
    if (slot.inFlight === null && !slot.dirty) {
      if (slot.lastError !== null) {
        const err = slot.lastError;
        slot.lastError = null;
        throw err;
      }
      return;
    }
    await new Promise<void>((resolve) => {
      slot.settled.push(resolve);
    });
    if (slot.lastError !== null) {
      const err = slot.lastError;
      slot.lastError = null;
      throw err;
    }
  }

  const wrapped: WorkflowRunPackPushingRepoStore = {
    initRepo: underlying.initRepo.bind(underlying),
    writeTree: underlying.writeTree.bind(underlying),
    receivePack: underlying.receivePack.bind(underlying),
    createPack: underlying.createPack.bind(underlying),
    resolveRef: underlying.resolveRef.bind(underlying),
    listRefs: underlying.listRefs.bind(underlying),
    resolveHead: underlying.resolveHead.bind(underlying),
    getRepoDir: underlying.getRepoDir.bind(underlying),
    subscribe: underlying.subscribe.bind(underlying),
    flushWorkflowRunPushes,
    async writeTreePreservingPrefix(principal, repoId, ref, args) {
      if (repoId.kind === "workflow-run") {
        const latched = takeLatchedError(repoId, ref);
        if (latched !== null) {
          throw latched;
        }
      }
      const result = await underlying.writeTreePreservingPrefix(
        principal,
        repoId,
        ref,
        args,
      );
      if (repoId.kind !== "workflow-run") {
        return result;
      }
      const agentAddress = registry.resolve(repoId.id);
      if (agentAddress === null) {
        throw new Error(
          `workflow-run pack push: no agent address registered for deployment ${repoId.id}; the deploy router must record the mapping before the supervisor commits run events`,
        );
      }
      schedulePush(agentAddress, repoId, ref);
      return result;
    },
    async writeTreeDelta(principal, repoId, ref, args) {
      if (repoId.kind === "workflow-run") {
        const latched = takeLatchedError(repoId, ref);
        if (latched !== null) {
          throw latched;
        }
      }
      const result = await underlying.writeTreeDelta(
        principal,
        repoId,
        ref,
        args,
      );
      if (repoId.kind !== "workflow-run") {
        return result;
      }
      const agentAddress = registry.resolve(repoId.id);
      if (agentAddress === null) {
        throw new Error(
          `workflow-run pack push: no agent address registered for deployment ${repoId.id}; the deploy router must record the mapping before the supervisor commits run events`,
        );
      }
      schedulePush(agentAddress, repoId, ref);
      return result;
    },
  };
  return wrapped;
}
