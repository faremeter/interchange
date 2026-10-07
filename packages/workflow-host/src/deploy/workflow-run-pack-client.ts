// Workflow-run pack push client: sits between the boot-edge substrate
// facade and the hub-link's `pushWorkflowRunPack` wire surface. After a
// supervisor-authored `writeTreePreservingPrefix` against a `workflow-run`
// repo, it builds the new pack via `RepoStore.createPack` and ships it to
// the hub.
//
// It does NOT mint a fresh signing key, transferId, or principal kind: the
// principal is the same `WorkflowRunSupervisorPrincipal` shape the
// supervisor uses, and the transferId is minted inside
// `HubLink.pushWorkflowRunPack`.

import { type } from "arktype";

import { getLogger } from "@intx/log";
import { SourcesUpdatedData } from "../ipc/control-channel";
import type { InferenceSource } from "@intx/types/runtime";
import {
  CredentialDelivery,
  type SenderIdentity,
  type WorkflowRunRefTips,
} from "@intx/types/sidecar";
import {
  WORKFLOW_RUN_RESTORE_REFS,
  type RepoId,
  type RepoStore,
  type WorkflowRunSupervisorPrincipal,
} from "@intx/hub-sessions/substrate";

const logger = getLogger([
  "interchange",
  "sidecar",
  "workflow-run-pack-client",
]);

export type WorkflowRunPackClient = {
  /**
   * Build a pack of the workflow-run repo at `ref` and ship it to the
   * hub. Resolves on the hub's `repo.pack.ack`; rejects on `repo.pack.reject`,
   * on a disconnect cancelling the transfer, or on a substrate-side
   * `createPack` failure.
   */
  push(opts: {
    agentAddress: string;
    repoId: RepoId;
    ref: string;
  }): Promise<void>;
  /**
   * Seed the acknowledged-tip state after the Hub restores a ref into a fresh
   * worker, so a reconnect does not re-send an empty delta at the restored tip.
   */
  markRestored(repoId: RepoId, ref: string, commitSha: string): void;
};

export type CreateWorkflowRunPackClientOpts = {
  substrate: RepoStore;
  hubLink: {
    pushWorkflowRunPack(opts: {
      agentAddress: string;
      repoId: RepoId;
      pack: Uint8Array;
      ref: string;
      commitSha: string;
    }): Promise<void>;
  };
};

export function createWorkflowRunPackClient(
  opts: CreateWorkflowRunPackClientOpts,
): WorkflowRunPackClient {
  const { substrate, hubLink } = opts;

  // Shadow of the substrate's shipped-tip cursor, keyed by `(repoId.id, ref)`.
  // The client is the SOLE caller of `commitPackedTip` for the workflow-run
  // kind, so this record of "the last commitSha I acked" cannot drift from the
  // substrate cursor. It answers a question `createPack` cannot: is the ref
  // tip already shipped? An empty-delta pack whose declared tip is not in it
  // is rejected by the hub as `sha_mismatch`, so skipping the send makes a
  // re-drive of an already-shipped tip a clean no-op. Load-bearing: the
  // reconnect re-drive can fire for a slot whose commits already landed.
  const lastAckedSha = new Map<string, string>();
  function ackKey(repoId: RepoId, ref: string): string {
    return `${repoId.id}/${ref}`;
  }

  return {
    markRestored(repoId, ref, commitSha) {
      if (repoId.kind !== "workflow-run") {
        throw new Error(
          `workflow-run pack client: restored repoId.kind must be "workflow-run", got ${JSON.stringify(repoId.kind)}`,
        );
      }
      substrate.commitPackedTip(repoId, ref, commitSha);
      lastAckedSha.set(ackKey(repoId, ref), commitSha);
    },
    async push({ agentAddress, repoId, ref }) {
      if (repoId.kind !== "workflow-run") {
        throw new Error(
          `workflow-run pack client: repoId.kind must be "workflow-run", got ${JSON.stringify(repoId.kind)}`,
        );
      }
      const principal: WorkflowRunSupervisorPrincipal = {
        kind: "supervisor",
        anchorRunId: repoId.id,
      };
      // Local tip already shipped: the commits landed on a prior push, so
      // return without a wire send (an empty-delta pack would be rejected).
      const tip = await substrate.resolveRef(principal, repoId, ref);
      if (tip !== null && tip === lastAckedSha.get(ackKey(repoId, ref))) {
        return;
      }
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
      // Advance the shipped-tip cursor only after the ack (never at build
      // time): a rejected or reconnect-cancelled push throws before this
      // line, so the cursor stays put and the next `createPack` re-includes
      // the un-acked commits.
      substrate.commitPackedTip(repoId, ref, commitSha);
      lastAckedSha.set(ackKey(repoId, ref), commitSha);
    },
  };
}

/**
 * Maps `repoId.id` (the workflow-run runId, derived by slugging the agent's
 * mail address) to the agentAddress carried on every outbound pack frame.
 * Populated by the deploy router as each `agent.deploy` frame lands.
 */
export type DeploymentAddressRegistry = {
  record(runId: string, agentAddress: string): void;
  resolve(runId: string): string | null;
  unregister(runId: string): void;
};

export function createDeploymentAddressRegistry(): DeploymentAddressRegistry {
  const table = new Map<string, string>();
  return {
    record(runId, agentAddress) {
      table.set(runId, agentAddress);
    },
    resolve(runId) {
      return table.get(runId) ?? null;
    },
    unregister(runId) {
      table.delete(runId);
    },
  };
}

/**
 * Handler the deploy router installs after `spawn` succeeds: hands a
 * delivered inbound mail to the supervisor's `routeInbound`, which
 * dispatches into the mail-bus the child's `awaitSignal` subscribes against.
 */
export type MultistepMailHandler = (message: Uint8Array) => Promise<void>;

/**
 * Per-deployment-address mail handler registry the hub-link consults before
 * falling back to `transport.deliver`. An inbound `mail.inbound` frame for a
 * registered address dispatches into the supervisor's mail-bus subscription
 * rather than a never-provisioned transport mailbox.
 *
 * Owned at the sidecar host layer, not inside the workflow-host library:
 * the routing decision is between the "legacy single-agent path" and the
 * "supervisor mail-bus path", two concrete sidecar host concerns.
 */
export type MultistepMailRouter = {
  register(address: string, handler: MultistepMailHandler): void;
  unregister(address: string): void;
  /**
   * Dispatch `message` to the handler for `address`, or `null` when none is
   * registered. The returned promise resolves when the message is durably
   * accepted and rejects when it was not, so the caller acks only on
   * resolution.
   */
  tryRoute(address: string, message: Uint8Array): Promise<void> | null;
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
      if (handler === undefined) return null;
      return handler(message);
    },
  };
}

/**
 * Per-deployment signal-delivery handler the deploy router installs after
 * `spawn` succeeds: hands the signal to the supervisor's `deliverSignal`,
 * which sends a `signal.deliver` control frame to the child. Routing every
 * signal through the child keeps the workflow-run repo's single-writer
 * invariant -- the child is the only sidecar-side writer of
 * `runs/<runId>/events/`, so the pack-push pipeline never races a host-side
 * write.
 */
export type MultistepSignalHandler = (args: {
  runId: string;
  signalName: string;
  signalId: string;
  payload: unknown;
}) => Promise<void>;

/**
 * Per-deployment-address signal handler registry the hub-link consults on
 * every inbound `signal.deliver` frame. Registered after `spawn` succeeds
 * for single- and multi-step deployments alike.
 *
 * Lives at the sidecar host layer for the same boundary reason as
 * `MultistepMailRouter`: the routing decision is a concrete sidecar host
 * concern.
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
 * Per-deployment grants-write handler the deploy router installs after
 * `spawn` succeeds: writes the run's grants to `runs/<runId>/grants.json`
 * in the workflow-run repo, awaited so a frame's FIFO completion means the
 * grants are durable before the next frame.
 *
 * `senderIdentities` are the run's authorized senders' hub-vouched keys,
 * co-delivered on the same frame. Each is cached before the grants write, so
 * a durable grant is never missing the key its recipient verifies against.
 */
export type MultistepGrantsHandler = (args: {
  runId: string;
  stepGrants: readonly unknown[];
  senderIdentities?: readonly SenderIdentity[];
}) => Promise<void>;

/**
 * Per-deployment-address grants handler registry the hub-link consults on
 * every inbound `run.grants` frame. Registered after `spawn` succeeds for
 * single- and multi-step deployments alike.
 *
 * Lives at the sidecar host layer for the same boundary reason as the other
 * routers: the routing decision is a concrete sidecar host concern.
 */
export type MultistepGrantsRouter = {
  register(address: string, handler: MultistepGrantsHandler): void;
  unregister(address: string): void;
  tryRoute(frame: {
    type: "run.grants";
    agentAddress: string;
    runId: string;
    stepGrants: readonly unknown[];
    senderIdentities?: readonly SenderIdentity[];
  }): Promise<boolean>;
};

export function createMultistepGrantsRouter(): MultistepGrantsRouter {
  const handlers = new Map<string, MultistepGrantsHandler>();
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
        stepGrants: frame.stepGrants,
        ...(frame.senderIdentities !== undefined
          ? { senderIdentities: frame.senderIdentities }
          : {}),
      });
      return true;
    },
  };
}

/**
 * Per-deployment drain handler the deploy router installs after `spawn`
 * succeeds: hands the drain opts to the supervisor's `drain`, which sends a
 * `drain` control frame to the child and arms one `drainTimeout` accumulator
 * per in-flight run. Cancel-mode in-flight steps abort; wait-mode steps
 * continue. Each accumulator commits a signed
 * `CancelRequested{origin: "supervisor-drain"}` when the deadline expires.
 */
export type MultistepDrainHandler = (args: {
  deadlineMs: number;
}) => Promise<void>;

/**
 * Per-deployment-address drain handler registry the hub-link consults on
 * every inbound `drain.deliver` frame. Registered after `spawn` succeeds for
 * single- and multi-step deployments alike.
 *
 * Lives at the sidecar host layer for the same boundary reason as the other
 * routers: the routing decision is a concrete sidecar host concern.
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
 * Per-deployment sources-rotation handler the deploy router installs after
 * `spawn` succeeds -- but ONLY for a single-step (warm launched-agent)
 * deployment. The handler hands the rotated list to the supervisor's
 * `deliverSources`, which sends a `sources-updated` control frame to the
 * child, where the warm agent's live sources are swapped in place. A
 * multi-step deployment has no single warm agent to rotate, so no handler is
 * registered for it.
 */
export type MultistepSourcesHandler = (args: {
  sources: InferenceSource[];
  defaultSource: string;
}) => Promise<void>;

/**
 * Per-deployment-address sources-rotation handler registry. Only a
 * single-step warm deployment registers a handler, so `tryRoute` resolves a
 * rotation only for a registered single-step address and returns `false`
 * otherwise.
 *
 * Lives at the sidecar host layer for the same boundary reason as the other
 * routers: the routing decision is a concrete sidecar host concern.
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
      // An unregistered (multi-step or torn-down) address is unrouted.
      if (handler === undefined) return false;
      // Validate BEFORE dispatch: a bad list (duplicate ids, or a default
      // that is not the head element) would crash the child's control-channel
      // receiver on `SourcesUpdatedData`'s narrow. Rejecting here throws, and
      // the hub-link turns the throw into a truthful `session.error` instead
      // of acking and detonating the child.
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
 * Per-deployment credential-delivery handler the deploy router installs after
 * `spawn` succeeds: hands the delivery to the supervisor's
 * `deliverCredentials`, which sends a `credentials-updated` control frame to
 * the child, where the material cell is swapped in place.
 *
 * Unlike sources rotation, this registers for ANY deployment: the material
 * cell is per-child and read by every step's tool capabilities. No durable
 * persist -- credential material never touches disk (the hub re-resolves it
 * on reconnect).
 */
export type MultistepCredentialsHandler = (args: {
  delivery: CredentialDelivery;
  revoke?: string[];
}) => Promise<void>;

/**
 * Per-deployment-address credential-delivery handler registry. Mirrors
 * `MultistepSourcesRouter`: `credentials.update` is a REQUEST/ACK frame, so a
 * registered address that throws surfaces as a `session.error` and an
 * unregistered one returns `false`. Lives at the sidecar host layer for the
 * same boundary reason as the other routers.
 */
export type MultistepCredentialsRouter = {
  register(address: string, handler: MultistepCredentialsHandler): void;
  unregister(address: string): void;
  tryRoute(frame: {
    type: "credentials.update";
    agentAddress: string;
    delivery: CredentialDelivery;
    revoke?: string[];
  }): Promise<boolean>;
};

export function createMultistepCredentialsRouter(): MultistepCredentialsRouter {
  const handlers = new Map<string, MultistepCredentialsHandler>();
  return {
    register(address, handler) {
      handlers.set(address, handler);
    },
    unregister(address) {
      handlers.delete(address);
    },
    async tryRoute(frame) {
      const handler = handlers.get(frame.agentAddress);
      // Registration check first: an unregistered (torn-down) address is
      // unrouted -- reported as `false`, its payload never inspected.
      if (handler === undefined) return false;
      // Validate BEFORE dispatch: a malformed delivery would crash the
      // child's control-channel receiver on `CredentialsUpdateFrame`'s
      // narrow. Rejecting here throws, and the hub-link turns the throw into
      // a truthful `session.error`.
      const validated = CredentialDelivery(frame.delivery);
      if (validated instanceof type.errors) {
        throw new Error(validated.summary);
      }
      await handler({
        delivery: frame.delivery,
        ...(frame.revoke !== undefined ? { revoke: frame.revoke } : {}),
      });
      return true;
    },
  };
}

/**
 * Boot-edge facade around the substrate-shaped `RepoStore`. Forwards every
 * method to the underlying store; intercepts the `writeTreePreservingPrefix`
 * (and `writeTreeDelta`) return path so a successful write against a
 * `workflow-run` repo schedules a pack push. Other kinds flow through
 * unchanged.
 *
 * Coalescing: at most one push per (repoId, ref) is in flight; writes that
 * arrive during a push mark the slot dirty and trigger one follow-up push, so
 * a burst of N writes costs at most 2 hub round-trips. The single pack covers
 * every commit since the prior ACKED tip, which the receiver validates as a
 * full chain.
 *
 * Reconnect-safe: the shipped-tip cursor advances only on the ack, so a
 * transfer a reconnect cancels is re-shipped with the un-acked commits.
 *
 * Failure surfacing: a failed push latches its error on the slot; the next
 * write on that (repoId, ref) re-throws it rather than swallowing it in the
 * fire-and-forget pipeline.
 *
 * Flush: `flushWorkflowRunPushes(repoId, ref)` awaits the slot to drain for
 * callers that need a hub-visible barrier (shutdown, hub-side reads).
 */
export type WorkflowRunPackPushingRepoStoreOpts = {
  underlying: RepoStore;
  packClient: Pick<WorkflowRunPackClient, "push">;
  registry: DeploymentAddressRegistry;
  deriveWorkflowRunRepoId: (agentAddress: string) => string;
};

/**
 * The wrapped store plus a side-channel API for waiting on the
 * per-(repoId.id, ref) pack-push pipeline to drain. The `RepoStore` shape is
 * unchanged; `flushWorkflowRunPushes` is opt-in for code that needs
 * hub-side visibility.
 */
export type WorkflowRunPackPushingRepoStore = RepoStore & {
  /**
   * Await the pack-push pipeline for `(repoId.id, ref)` to drain: resolves
   * once no push is in flight and no follow-up is pending, rejects on the
   * latched failure.
   */
  flushWorkflowRunPushes: (repoId: RepoId, ref: string) => Promise<void>;
  /**
   * Re-drive workflow-run pushes for `agentAddress` that a disconnect
   * cancelled, fired when the hub-link sees the address routable again after
   * an authenticated reconnect. Re-arms the coalescing loop for slots with a
   * latched failure so a fresh `createPack` re-ships the un-acked commits --
   * the liveness path a synchronous single-step run lacks (no later local
   * write to re-set `dirty`).
   *
   * A re-ship that fails again re-latches without self-retrying, so an
   * unrecoverable failure still surfaces on the next local write.
   */
  notifyAddressRoutable: (agentAddress: string) => void;
  /**
   * Block workflow-run pushes for `agentAddress` until the next
   * `notifyAddressRoutable` or a stop reports the address's tips. Called on a
   * WS drop so pushes wait for the reconnect instead of queueing on a link
   * that cannot deliver them.
   */
  markAddressUnroutable: (agentAddress: string) => void;
  /**
   * Read the local tip of each authoritative ref (`null` for an absent ref)
   * and schedule a push of every existing ref without waiting. Ships commits
   * written outside this facade's write hooks, such as run grants, and lifts
   * a disconnect's block: a stopped deployment is never announced again, so
   * no reconnect would lift it.
   */
  reportWorkflowRunRefTips: (
    agentAddress: string,
  ) => Promise<WorkflowRunRefTips>;
};

export function createWorkflowRunPackPushingRepoStore(
  opts: WorkflowRunPackPushingRepoStoreOpts,
): WorkflowRunPackPushingRepoStore {
  const { underlying, packClient, registry, deriveWorkflowRunRepoId } = opts;

  type Slot = {
    agentAddress: string;
    repoId: RepoId;
    ref: string;
    inFlight: Promise<void> | null;
    dirty: boolean;
    lastError: Error | null;
    settled: (() => void)[];
  };
  const slots = new Map<string, Slot>();
  function slotKey(repoId: RepoId, ref: string): string {
    return `${repoId.kind}/${repoId.id}/${ref}`;
  }

  // Addresses whose hub route dropped and has not been re-established by an
  // authenticated reconnect. Added on `markAddressUnroutable` (WS disconnect),
  // removed on `notifyAddressRoutable` (reconnect sent) or when a stop reports
  // its tips. A push for a blocked address is held: the loop pauses with
  // `dirty` still set, and the reconnect re-drives it.
  const blockedAddresses = new Set<string>();

  function notifySettled(slot: Slot): void {
    const callbacks = slot.settled;
    slot.settled = [];
    for (const cb of callbacks) cb();
  }

  function startLoop(slot: Slot, repoId: RepoId, ref: string): void {
    if (slot.inFlight !== null) return;
    // Hold the push while the address is unroutable; `notifyAddressRoutable`
    // clears the block and re-arms the loop.
    if (blockedAddresses.has(slot.agentAddress)) return;
    slot.inFlight = (async () => {
      while (slot.dirty) {
        // Re-check routability each iteration: a disconnect mid-drain pauses
        // the loop with `dirty` still set so the resume re-ships.
        if (blockedAddresses.has(slot.agentAddress)) break;
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
        repoId,
        ref,
        inFlight: null,
        dirty: false,
        lastError: null,
        settled: [],
      };
      slots.set(key, slot);
    } else {
      // Refresh the address on every call: the contract is "look up at push
      // time", not "cache forever".
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

  function markAddressUnroutable(agentAddress: string): void {
    // The hub route for this address just dropped (WS disconnect). Block its
    // pushes until the reconnect re-routes it; an in-flight push rejects via
    // `packSender.cancelAll` and latches its error.
    blockedAddresses.add(agentAddress);
  }

  function notifyAddressRoutable(agentAddress: string): void {
    // The reconnect re-routed this address. Clear the block and re-drive so a
    // cancelled or held push ships now -- the liveness path a synchronous
    // single-step run lacks, since it has no later local write to re-arm the
    // loop.
    blockedAddresses.delete(agentAddress);
    for (const slot of slots.values()) {
      if (slot.agentAddress !== agentAddress) continue;
      // Re-drive a slot with pending work (`dirty`) or a latched failure;
      // skip a clean, already-acked slot to avoid a pointless loop spin.
      // Re-arming is safe against double-ship: `startLoop` no-ops when a push
      // is in flight, and the hub-link serializes transfers per (repoId, ref).
      if (!slot.dirty && slot.lastError === null) continue;
      slot.dirty = true;
      startLoop(slot, slot.repoId, slot.ref);
    }
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

  async function reportWorkflowRunRefTips(
    agentAddress: string,
  ): Promise<WorkflowRunRefTips> {
    const repoId: RepoId = {
      kind: "workflow-run",
      id: deriveWorkflowRunRepoId(agentAddress),
    };
    const principal: WorkflowRunSupervisorPrincipal = {
      kind: "supervisor",
      anchorRunId: repoId.id,
    };
    blockedAddresses.delete(agentAddress);
    const tips: WorkflowRunRefTips = {};
    for (const ref of WORKFLOW_RUN_RESTORE_REFS) {
      const tip = await underlying.resolveRef(principal, repoId, ref);
      tips[ref] = tip;
      if (tip !== null) schedulePush(agentAddress, repoId, ref);
    }
    return tips;
  }

  const wrapped: WorkflowRunPackPushingRepoStore = {
    initRepo: underlying.initRepo.bind(underlying),
    writeTree: underlying.writeTree.bind(underlying),
    receivePack: underlying.receivePack.bind(underlying),
    createPack: underlying.createPack.bind(underlying),
    commitPackedTip: underlying.commitPackedTip.bind(underlying),
    resolveRef: underlying.resolveRef.bind(underlying),
    listRefs: underlying.listRefs.bind(underlying),
    resolveHead: underlying.resolveHead.bind(underlying),
    getRepoDir: underlying.getRepoDir.bind(underlying),
    openCommittedReads: underlying.openCommittedReads.bind(underlying),
    openCommittedReadsAtCommit:
      underlying.openCommittedReadsAtCommit.bind(underlying),
    subscribe: underlying.subscribe.bind(underlying),
    flushWorkflowRunPushes,
    notifyAddressRoutable,
    markAddressUnroutable,
    reportWorkflowRunRefTips,
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
          `workflow-run pack push: no run address registered for deployment ${repoId.id}; the deploy router must record the mapping before the supervisor commits run events`,
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
          `workflow-run pack push: no run address registered for deployment ${repoId.id}; the deploy router must record the mapping before the supervisor commits run events`,
        );
      }
      schedulePush(agentAddress, repoId, ref);
      return result;
    },
  };
  return wrapped;
}
