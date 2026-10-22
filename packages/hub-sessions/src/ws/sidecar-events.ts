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
  PackRejectReason,
  RunGrantsFrame,
  WorkflowRunRefTips,
} from "@intx/types/sidecar";
import type { CredentialDelivery } from "@intx/types/credential-delivery";
import type { RepoId } from "@intx/types/repo";
import type {
  ApprovalSnapshot,
  ConnectorThreadState,
} from "@intx/types/runtime";
import type { SignalKind } from "@intx/types";
import type { MailAcceptCoordinate, ResolvedSenderPrincipal } from "@intx/db";
import { getLogger } from "@intx/log";

const logger = getLogger(["hub", "ws", "sidecar", "events"]);

export type SidecarMailPersistedRow = {
  id: string;
  createdAt: Date;
  direction: "inbound" | "outbound";
  runId: string | null;
  address: string;
};

export type SidecarMailPersistedPayload = SidecarMailPersistedRow & {
  raw: Uint8Array;
};

/** Authenticated connection scope attached to a workflow-run pack. */
export type WorkflowRunPackSource = {
  readonly kind: "allocated";
  readonly agentAddress: string;
  readonly allocationId: string;
  readonly anchorRunId: string;
  readonly generation: number;
};

// The code a mail-transport admission denial rejects under. Distinct from
// `insufficient_grants` (a declared requirement's authority is missing) and
// `workflow_run_terminal` (the run is settled): this one means the run's
// accept-policy does not admit the authenticated sender.
export const MAIL_ADMISSION_DENIED_CODE = "mail_admission_denied";

/**
 * Outcome of reserving a mail-triggered run's grants. `skip` means the
 * recipient names no workflow deployment; `rejected` means the deployment's
 * stable run is terminal, its requirements cannot be authorized, or the
 * sender gate refused the mail (`MAIL_ADMISSION_DENIED_CODE`: the sender is
 * not admitted, or the deployment has no mail trigger); `notReady` means
 * credential resolution is not recorded yet, so nothing was committed and
 * the caller should retry after initialization; `materialized` carries the
 * canonical persisted wire rows.
 */
export type MailTriggeredRunGrantsResult =
  | { outcome: "skip" }
  | { outcome: "rejected"; status: 403 | 409; code: string; message: string }
  | { outcome: "notReady" }
  | {
      outcome: "materialized";
      stepGrants: RunGrantsFrame["stepGrants"];
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

  /** Notification. Emitted when a sidecar's connection closes, and when
   * one allocation leaves a connection that stays open for others,
   * carrying every workflow address that lost its route: deployment
   * addresses and the step addresses staged for them. */
  "sidecar.disconnect": {
    ownedAddresses: string[];
    /** Allocation generations whose current connection this was. */
    allocated: {
      allocationId: string;
      generation: number;
    }[];
  };

  /** Notification after the exact authenticated allocation generation registers. */
  "sidecar.allocated.connected": {
    allocationId: string;
    generation: number;
  };

  /**
   * Notification after the sidecar reports that the current generation's
   * deployment stopped though the Hub did not stop it.
   */
  "deployment.stopped": {
    allocationId: string;
    generation: number;
  };

  /** Notification. Emitted for mail the Hub gives up on: recipients no
   * connection routes, un-acked mail it stops redelivering that no
   * dispatch row stands behind, and mail held for a sender key that never
   * arrived. The host is free to relay it onto an external transport or
   * drop it. */
  "mail.outbound.undelivered": {
    rawMessage: string;
    recipients: string[];
  };

  /** Notification. Emitted once per row produced by the host's
   * `persistMail` lookup. The wire layer calls `persistMail` to obtain
   * the rows; this event fires for each so subscribers can react
   * per-row (e.g. dispatch a delivered event). */
  "mail.persisted": SidecarMailPersistedPayload;

  /** Notification after a sidecar confirms a mail trigger is in its durable
   * local inbox. For a provisioned worker, `allocated` identifies the exact
   * generation that acknowledged the message. This is not workflow
   * settlement; the Hub retains the payload until the Git claim-check records
   * consumption. */
  "mail.inbound.acknowledged": {
    agentAddress: string;
    messageId: string;
    allocated?: {
      allocationId: string;
      anchorRunId: string;
      generation: number;
    };
  };

  /** Awaited. Emitted when an agent.deploy.ack frame arrives. Rejection
   * fails the pending deploy with the listener's error. */
  "agent.deploy.ack": {
    agentAddress: string;
    publicKey: string;
    allocated?: {
      allocationId: string;
      anchorRunId: string;
      generation: number;
    };
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
    "sidecar.allocated.connected": new Set(),
    "deployment.stopped": new Set(),
    "mail.outbound.undelivered": new Set(),
    "mail.persisted": new Set(),
    "mail.inbound.acknowledged": new Set(),
    "agent.deploy.ack": new Set(),
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
  /** Returns the hub's current deploy ref for the address, or `null` if
   * no deploy state is tracked. The wire layer compares this against
   * the sidecar's reported ref during reconnect and emits
   * `deploy.ref.stale` only on mismatch. */
  lookupDeployRef?: (agentAddress: string) => Promise<string | null>;

  /** Re-resolves a reconnecting deployment's current credential delivery and
   * reconciles the child, so a credential revoked, deleted, or rotated while
   * the sidecar was disconnected is applied on reconnect (closing the offline
   * window). Keyed by the run address. Fire-and-forget: the wire layer does not
   * await it and it never throws -- a failure is logged and dropped, like the
   * best-effort source-update pushes. No-ops for a run that persisted no
   * credential refs. */
  resyncCredentials?: (agentAddress: string) => void;

  /** Loads a deployment run's hub credentials (its run tokens) by run id, or
   * null when it has none. The wire layer attaches the result to each
   * `run.grants` barrier it sends ahead of a trigger mail, so a replayed
   * barrier carries them again. Absent: barriers carry no hub credentials.
   * It may throw; the wire layer logs a fault and sends the barrier without
   * them, so a load failure never drops the mail it precedes. */
  loadRunCredentials?: (runId: string) => Promise<CredentialDelivery | null>;

  /** Persists a delivered outbound mail frame. Returns one row per
   * persisted record; the wire layer attaches `raw` to each row and
   * emits a `mail.persisted` event. */
  persistMail?: (args: {
    senderAddress: string;
    recipients: string[];
    raw: Uint8Array;
  }) => Promise<SidecarMailPersistedRow[]>;

  /** Resolves the hub-held public key a sender address signs with, hex-encoded,
   * or `null` when the sender has no resolvable key (a run whose deploy is not
   * yet acked, an address matching no known principal). The `mail.inbound` frame
   * carries the result so a recipient verifies the signature locally against the
   * hub-resolved key rather than the message's own spoofable From. Returns only
   * the key string, not its source, so the recipient cannot branch on sender
   * kind.
   *
   * Best-effort: this NEVER throws. A resolution fault degrades to `null`
   * (logged at ERROR by the resolver), so a key-resolution problem can never
   * block mail delivery. The strict, throwing resolver is `resolveSenderKey`
   * in `@intx/db`; the reconnect reconciliation consumes it through
   * `resolveSenderKeyStrict` below, which needs to tell a fault from a genuine
   * absence. */
  resolveSenderKey?: (address: string) => Promise<string | null>;

  /** Strict sibling of `resolveSenderKey` for the reconnect reconciliation. The
   * `hello` handler re-resolves each cached sender a hello reports and acts
   * on the three-way outcome the best-effort resolver collapses:
   *   - returns the hex key when the sender resolves -> push `sender.key.refresh`;
   *   - returns `null` for a CONFIRMED absence (no matching principal = a deleted
   *     sender) -> push `sender.key.evict`;
   *   - THROWS on a fault (ambiguous address, a keyless-principal invariant
   *     break, a DB error) -> keep the stale cached key, evict nothing.
   * Never evicting on a throw is load-bearing: dropping a live key on a
   * transient DB fault would be worse than doing nothing. Wraps the strict
   * `resolveSenderKey` in `@intx/db` (unwrapped to the key string). Note the
   * naming inversion: in `@intx/db` the STRICT resolver is `resolveSenderKey`
   * and the best-effort one is `resolveFrameSenderKey`, whereas on this lookup
   * type the best-effort field is `resolveSenderKey` and this is the strict
   * one. */
  resolveSenderKeyStrict?: (address: string) => Promise<string | null>;

  /** Resolves an authenticated mail sender to its durable identity -- its
   * principal and tenant, and its admission coordinates. Distinct from
   * `resolveSenderKey`: that one is best-effort (degrade-to-null) and yields only
   * a signing key for co-delivery; this one is the STRICT, coordinate-carrying
   * resolution admission needs, and it must NOT be sourced from the
   * degrade-to-null path.
   *
   * Returns `null` when the sender does not resolve to a concrete principal --
   * an address matching no principal, or a run whose deploy is acked but whose
   * principal is not yet minted (a principal-less signer). THROWS on a genuine
   * fault (an ambiguous address, a data-integrity break). Both a `null` and a
   * throw leave the sender with no admission coordinates at the seam, so the
   * gate never admits it. Wraps the strict `resolveSenderKey` in `@intx/db`
   * WITHOUT unwrapping to the key, keeping `senderCoordinates`' fail-closed on a
   * principal-less run.
   */
  resolveSenderPrincipal?: (
    address: string,
  ) => Promise<ResolvedSenderPrincipal | null>;

  /** Co-writes the `signal_correlation` routing row and the `approval` row
   * for a suspending workflow agent step, in one transaction. Called from
   * the `signal.correlation.register` frame handler after the wire layer has
   * confirmed the sending sidecar owns `agentAddress`. Idempotent: a
   * redelivered frame (reconnect, workflow-log replay, supervisor restart
   * re-emitting) is a no-op, not an error. The wire layer does not carry
   * `signalName`; the host derives it from `correlationId`. Resolves the
   * tenancy from the workflow deployment the address names. */
  registerSignalCorrelation?: (args: {
    correlationId: string;
    runId: string;
    anchorRunId: string;
    agentAddress: string;
    kind: SignalKind;
    approvalSnapshot: ApprovalSnapshot;
  }) => Promise<void>;

  /** Reserves a mail-triggered workflow run's grants from the receiving
   * deployment's definition, returning a discriminated result the
   * `mail.outbound` handler orders against delivery. Called for each recipient
   * that is a workflow deployment. The `runId` is the deployment's stable
   * address-derived run id.
   *
   * On `materialized`, `stepGrants` are already persisted and the caller sends
   * them ahead of the inbound mail. Reservation is idempotent on the runId, so
   * a redelivered inbound mail neither double-mints nor throws. On `skip` the
   * address names no deployed workflow deployment, so no grants are sent and
   * the mail still forwards. On `rejected` the stable run is terminal, a
   * declared requirement's authority is insufficient, or the sender gate
   * refused the mail (`mail_admission_denied`: the sender is not admitted, or
   * the deployment has no mail trigger); the caller fails the mail closed for
   * that recipient.
   *
   * `senderCoordinates` are the authenticated sender rendered as the
   * `mail.accept` admission coordinates, resolved ONCE at the seam via
   * `resolveSenderPrincipal`. The sender is not the run's invoker:
   * invoker-sourced requirements resolve against the deployment's deployer.
   * The materializer gates delivery on them: a start (first fire) is admitted
   * only when the run's staged `mail.accept` rows admit the sender, and a
   * delivery to an already-committed run is admitted only when the run's
   * collected grants do. `null` means the sender did not resolve to a concrete
   * principal, which the admission gate treats as an unknown sender and never
   * admits. */
  materializeMailTriggeredRunGrants?: (args: {
    agentAddress: string;
    runId: string;
    senderCoordinates: MailAcceptCoordinate[] | null;
  }) => Promise<MailTriggeredRunGrantsResult>;

  /**
   * Records that a live run mailed one or more parties, so their replies are
   * admitted: the dynamic `correspondent` relation. Called once per outbound
   * mail with all its recipients. It mints a `mail.accept:principal:<id>` grant
   * on the SENDING run for each recipient, only when the sending definition
   * authored the `correspondent` relation and the sending run is live; an
   * unresolvable recipient or a principal-less/terminal sender mints nothing.
   * Best-effort -- it never throws, so a mint fault never breaks delivery. */
  mintCorrespondentGrant?: (args: {
    senderAddress: string;
    recipientAddresses: string[];
  }) => Promise<void>;

  /** Ingests a received agent-state pack and returns whether the wire
   * layer should ack or reject the pack to the sidecar. `repoId.kind`
   * is `"agent-state"` and `repoId.id` is the run address. The wire
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
   * layer should ack or reject the pack to the sidecar. `source` is derived
   * from the authenticated socket, never from the frame. The lookup must
   * revalidate that source against durable deployment/allocation ownership
   * before advancing the Git ref. */
  receiveWorkflowRunPack?: (
    repoId: RepoId,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    source: WorkflowRunPackSource,
  ) => Promise<
    { accepted: true } | { accepted: false; reason: PackRejectReason }
  >;

  /** Reads the Hub's tip of each authoritative ref of the deployment's
   * workflow-run repo, `null` for a ref it has not received. A stop is
   * confirmed only once the worker reports the same tips. */
  readWorkflowRunRefTips?: (
    agentAddress: string,
  ) => Promise<WorkflowRunRefTips>;
  /** The terminal anchor's admission cutoff is closed and earlier receives settled. */
  isWorkflowRunHistoryFinal?: (
    source: WorkflowRunPackSource,
  ) => Promise<boolean>;
};
