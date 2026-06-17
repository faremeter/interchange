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

import { getLogger } from "@intx/log";
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
 * `repoId.id` (the workflow-run deploymentId, which the trivial branch
 * derives by slugging the agent's mail address) back into the
 * agentAddress carried on every outbound pack frame. Populated by the
 * deploy router as each `agent.deploy` frame lands.
 */
export type DeploymentAddressRegistry = {
  record(deploymentId: string, agentAddress: string): void;
  resolve(deploymentId: string): string | null;
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
 * consults before falling back to `transport.deliver` /
 * `sessions.commitInboundMail`. The trivial deploy path never registers
 * a handler -- its mail flows through the legacy session path. The
 * multi-step deploy router registers a handler against the deployment's
 * mail address after `wired.supervisor.spawn` succeeds, so an inbound
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
 * consults on every inbound `signal.deliver` frame. The trivial deploy
 * path never registers a handler. The multi-step deploy router
 * registers a handler against the deployment's mail address after
 * `wired.supervisor.spawn` succeeds; the handler dispatches the signal
 * into the supervisor's `deliverSignal`.
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
 * Boot-edge facade around the substrate-shaped `RepoStore`. Forwards
 * every method to the underlying store; intercepts the
 * `writeTreePreservingPrefix` return path so a successful write
 * against a `workflow-run` repo fires the workflow-run pack push.
 * Writes against any other `repoId.kind` (today, only `agent-state`
 * via the deploy-applier path) flow through unchanged. The hook is
 * fire-and-await: the original `writeTreePreservingPrefix` Promise
 * does not resolve until the hub has acked the push, so a
 * downstream `recordRunEvent` does not observe a fast-forward against
 * a not-yet-shipped commit.
 */
export type WorkflowRunPackPushingRepoStoreOpts = {
  underlying: RepoStore;
  packClient: WorkflowRunPackClient;
  registry: DeploymentAddressRegistry;
};

export function createWorkflowRunPackPushingRepoStore(
  opts: WorkflowRunPackPushingRepoStoreOpts,
): RepoStore {
  const { underlying, packClient, registry } = opts;
  const wrapped: RepoStore = {
    initRepo: underlying.initRepo.bind(underlying),
    writeTree: underlying.writeTree.bind(underlying),
    receivePack: underlying.receivePack.bind(underlying),
    createPack: underlying.createPack.bind(underlying),
    resolveRef: underlying.resolveRef.bind(underlying),
    listRefs: underlying.listRefs.bind(underlying),
    resolveHead: underlying.resolveHead.bind(underlying),
    getRepoDir: underlying.getRepoDir.bind(underlying),
    subscribe: underlying.subscribe.bind(underlying),
    async writeTreePreservingPrefix(principal, repoId, ref, args) {
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
      try {
        await packClient.push({ agentAddress, repoId, ref });
      } catch (cause) {
        const msg = cause instanceof Error ? cause.message : String(cause);
        logger.warn`workflow-run pack push failed for deployment ${repoId.id} (${agentAddress}): ${msg}`;
        throw cause;
      }
      return result;
    },
  };
  return wrapped;
}
