// Typed event emitter for the sidecar router.
//
// The router emits events at the points where wire-layer frame handling
// completes and a host-side decision or side effect is required. Two
// emission shapes are exposed:
//
// - `emit(type, payload)` — notification semantics. Each listener runs
//   inside its own try/catch; a thrown error is logged and does not
//   affect other listeners or the wire layer. Used for events whose
//   outcome does not feed back into protocol behavior.
//
// - `emitAndAwait(type, payload)` — sequential await semantics.
//   Listeners run in registration order; the first rejection propagates
//   to the caller and stops the chain. Used for events whose outcome
//   affects subsequent wire-layer state (e.g. reconnect rollback).
//
// The TSDoc on each entry in `SidecarEventMap` records which semantic
// applies. Mixing the two on a single event is intentional: today's
// wire layer already has both behaviors, and pretending otherwise
// would silently change failure handling.

import type {
  DeployApplyErrorCategory,
  PackRejectReason,
  RepoId,
} from "@intx/types/sidecar";
import type { ConnectorThreadState } from "@intx/types/runtime";
import { getLogger } from "@intx/log";

const logger = getLogger(["hub", "ws", "sidecar", "events"]);

export type SidecarMailPersistedRow = {
  id: string;
  createdAt: Date;
  direction: "inbound" | "outbound";
  instanceId: string | null;
  address: string;
};

export type SidecarMailPersistedPayload = SidecarMailPersistedRow & {
  raw: Uint8Array;
};

export type SidecarEventMap = {
  /** Notification. Emitted for every agent.event frame the wire layer
   * decodes. The wire layer also forwards the event to in-process agent
   * subscribers registered via `router.subscribeAgent`; this event is
   * the host-side observation point. */
  "agent.event": {
    agentAddress: string;
    sessionId: string;
    event: unknown;
  };

  /** Notification. Emitted once when a sidecar's connection closes,
   * carrying every address the connection owned -- challenged session
   * addresses and hub-minted workflow-substrate deployment addresses
   * alike -- so lifecycle teardown covers both. */
  "sidecar.disconnect": {
    ownedAddresses: string[];
  };

  /** Notification. Emitted when a mail.outbound frame from a sidecar
   * names recipients that the wire layer could not deliver locally and
   * could not enqueue for a disconnected agent. The host is free to
   * relay it onto an external transport or drop it. */
  "mail.outbound.undelivered": {
    rawMessage: string;
    recipients: string[];
  };

  /** Notification. Emitted once per row produced by the host's
   * `persistMail` lookup. The wire layer calls `persistMail` to obtain
   * the rows; this event fires for each so subscribers can react
   * per-row (e.g. dispatch a delivered event). */
  "mail.persisted": SidecarMailPersistedPayload;

  /** Awaited. Emitted when an agent.deploy.ack frame arrives. Rejection
   * fails the pending deploy with the listener's error. */
  "agent.deploy.ack": {
    agentAddress: string;
    publicKey: string;
  };

  /** Notification. Emitted when the sidecar reports a change to an
   * agent's connector-thread state. The wire layer caches the state
   * per agent so the host can read it via
   * `router.getConnectorState(agentAddress)`; this event is for hosts
   * that want to observe transitions directly. `connectorState` is
   * `null` when the agent has no active connector thread. */
  "connector.state.changed": {
    agentAddress: string;
    connectorState: ConnectorThreadState | null;
  };

  /** Awaited. Emitted per address after challenge verification
   * succeeds and before the disconnect queue is flushed. Rejection
   * rolls that address back from the routing table; earlier listeners
   * in registration order have already executed and their side effects
   * are not undone. A subsequent reconnect arriving mid-flight may
   * supersede this one, so listeners must be idempotent. */
  "agent.reconnected": {
    agentAddress: string;
  };

  /** Awaited. Emitted per address after the wire layer has confirmed
   * the sidecar's deploy ref is stale relative to the hub's current
   * ref. The listener's job is to push a fresh deploy pack. The wire
   * layer fires this only when staleness is confirmed; subscribing
   * without a `lookupDeployRef` configured on the router will never
   * deliver. */
  "deploy.ref.stale": {
    agentAddress: string;
  };

  /** Notification. Emitted when the sidecar reports that its
   * tool-package apply pipeline rejected a deploy. `category` matches
   * the closed `DeployApplyErrorCategory` enum; `package` is set
   * when the failure implicates a specific manifest entry.
   * `previousDeployId` is the atomicity contract — it is always the
   * deploy id the instance was running before the rejected attempt
   * (the instance keeps running that deploy untouched). Listeners
   * are responsible for surfacing the failure to operators and/or
   * the deploy lifecycle subsystem; the wire layer only delivers.
   */
  "deploy.apply.error": {
    agentAddress: string;
    attemptId: string;
    previousDeployId: string;
    category: DeployApplyErrorCategory;
    message: string;
    package?: { name: string; version: string };
    occurredAt: string;
  };
};

export type SidecarEventType = keyof SidecarEventMap;

export type SidecarEventListener<T extends SidecarEventType> = (
  payload: SidecarEventMap[T],
) => void | Promise<void>;

export type SidecarEventEmitter = {
  on<T extends SidecarEventType>(
    type: T,
    listener: SidecarEventListener<T>,
  ): () => void;
  emit<T extends SidecarEventType>(type: T, payload: SidecarEventMap[T]): void;
  emitAndAwait<T extends SidecarEventType>(
    type: T,
    payload: SidecarEventMap[T],
  ): Promise<void>;
  /** Number of listeners registered for `type`. Wire-layer callers use
   * this to skip an `await` when nothing is listening, preserving the
   * synchronous scheduling of unconfigured-handler paths. */
  listenerCount(type: SidecarEventType): number;
};

export function createSidecarEmitter(): SidecarEventEmitter {
  const listeners: { [K in SidecarEventType]: Set<SidecarEventListener<K>> } = {
    "agent.event": new Set(),
    "sidecar.disconnect": new Set(),
    "mail.outbound.undelivered": new Set(),
    "mail.persisted": new Set(),
    "agent.deploy.ack": new Set(),
    "agent.reconnected": new Set(),
    "deploy.ref.stale": new Set(),
    "deploy.apply.error": new Set(),
    "connector.state.changed": new Set(),
  };

  function on<T extends SidecarEventType>(
    type: T,
    listener: SidecarEventListener<T>,
  ): () => void {
    listeners[type].add(listener);
    return () => {
      listeners[type].delete(listener);
    };
  }

  function emit<T extends SidecarEventType>(
    type: T,
    payload: SidecarEventMap[T],
  ): void {
    const set = listeners[type];
    if (set.size === 0) return;
    for (const listener of [...set]) {
      try {
        const result = listener(payload);
        if (result instanceof Promise) {
          result.catch((err: unknown) => {
            logger.warn`Listener for ${type} threw: ${
              err instanceof Error ? err.message : String(err)
            }`;
          });
        }
      } catch (err) {
        logger.warn`Listener for ${type} threw: ${
          err instanceof Error ? err.message : String(err)
        }`;
      }
    }
  }

  async function emitAndAwait<T extends SidecarEventType>(
    type: T,
    payload: SidecarEventMap[T],
  ): Promise<void> {
    const set = listeners[type];
    if (set.size === 0) return;
    for (const listener of [...set]) {
      await listener(payload);
    }
  }

  function listenerCount(type: SidecarEventType): number {
    return listeners[type].size;
  }

  return { on, emit, emitAndAwait, listenerCount };
}

export type SidecarLookups = {
  /** Returns the hex-encoded Ed25519 public key stored for the address,
   * or `null` if the address is unknown. Used during the reconnect
   * challenge to verify the sidecar's signature. */
  lookupPublicKey?: (agentAddress: string) => Promise<string | null>;

  /** Returns the hub's current deploy ref for the address, or `null` if
   * no deploy state is tracked. The wire layer compares this against
   * the sidecar's reported ref during reconnect and emits
   * `deploy.ref.stale` only on mismatch. */
  lookupDeployRef?: (agentAddress: string) => Promise<string | null>;

  /** Persists a delivered outbound mail frame. Returns one row per
   * persisted record; the wire layer attaches `raw` to each row and
   * emits a `mail.persisted` event. */
  persistMail?: (args: {
    senderAddress: string;
    recipients: string[];
    raw: Uint8Array;
  }) => Promise<SidecarMailPersistedRow[]>;

  /** Ingests a received agent-state pack and returns whether the wire
   * layer should ack or reject the pack to the sidecar. `repoId.kind`
   * is `"agent-state"` and `repoId.id` is the agent address. The wire
   * layer dispatches on `repoId.kind` against the receive lookups
   * before calling either; this lookup must reject any pack whose
   * `repoId.kind` is not `"agent-state"`. */
  receiveAgentStatePack?: (
    repoId: RepoId,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
  ) => Promise<
    { accepted: true } | { accepted: false; reason: PackRejectReason }
  >;

  /** Ingests a received workflow-run pack and returns whether the wire
   * layer should ack or reject the pack to the sidecar. `repoId.kind`
   * is `"workflow-run"` and `repoId.id` is the deployment id (which the
   * hub-side substrate maps to a `WorkflowRunSupervisorPrincipal`
   * during the receivePack call). The wire layer dispatches on
   * `repoId.kind` against the receive lookups before calling either;
   * this lookup must reject any pack whose `repoId.kind` is not
   * `"workflow-run"`. */
  receiveWorkflowRunPack?: (
    repoId: RepoId,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
  ) => Promise<
    { accepted: true } | { accepted: false; reason: PackRejectReason }
  >;
};
