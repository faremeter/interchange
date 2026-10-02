// Hub-side websocket handler for sidecar connections.
//
// Accepts websocket upgrades, processes hello frames, maintains a routing
// table of agentAddress → sidecar connection, and dispatches frames between
// sidecars and the hub's internal systems.

import { getLogger } from "@intx/log";
import {
  WorkflowRunNotExecutableError,
  type WorkflowRunExecutionTarget,
} from "@intx/db";
import { chunkPack, createPackReceiver } from "@intx/pack-transport";
import {
  base64Decode,
  deriveMessageId,
  deriveWorkflowRunId,
  isRunAddress,
} from "@intx/types";
import type { GrantWalkSnapshot } from "@intx/types";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";
import { type } from "arktype";
import {
  MAX_MAIL_OUTBOUND_BODY_BYTES,
  SidecarFrame,
  WORKFLOW_CONTROL_INITIALIZING_ERROR,
  type AgentDeployAckFrame,
  type AgentDeployErrorFrame,
  type AgentDeployFrame,
  type AgentUndeployAckFrame,
  type AgentUndeployErrorFrame,
  type HostedIncarnation,
  type PackAckFrame,
  type HubFrame,
  type MailInboundFrame,
  type PackPushFrame,
  type PackDoneFrame,
  type PackRejectFrame,
  type RunGrantsFrame,
  type WorkflowControlFrame,
  type WorkflowControlAckFrame,
  type WorkflowRunRefTips,
  type SignalCorrelationRegisterFrame,
  type WorkflowSourceAssetMount,
  type WorkflowProjectionDefinition,
} from "@intx/types/sidecar";
import { RepoId } from "@intx/types/repo";
import type { CredentialDelivery } from "@intx/types/credential-delivery";
import type {
  ConnectorThreadState,
  HarnessConfig,
  InferenceSource,
} from "@intx/types/runtime";
import type {
  SidecarCredentialIdentity,
  SidecarCredentials,
  SidecarIdentityUse,
} from "../sidecar-allocation/contracts";
import type { ToolPackageManifest } from "@intx/types/tool-packages";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";
import {
  createSidecarEmitter,
  type SidecarEventEmitter,
  type SidecarLookups,
  type SidecarMailPersistedRow,
} from "./sidecar-events";
import {
  PendingTracker,
  type PendingEntry,
  type WsHandle,
} from "./pending-tracker";
import { workflowRunRepoIdForAddress } from "../workflow-run-kind";

const logger = getLogger(["hub", "ws", "sidecar"]);

/**
 * A deploy-frame send failure, tagged with whether the `agent.deploy` frame
 * reached the wire. `frameSent: false` means the send was refused before
 * `conn.send` (a guard failed, or the send threw synchronously) -- the deploy
 * provably never started, so a caller may safely roll back anything it staged.
 * `frameSent: true` means the frame was sent and the failure came afterward (ack
 * timeout, sidecar disconnect), so the sidecar may hold a live agent.
 */
export interface DeployFrameFailure extends Error {
  readonly frameSent: boolean;
}

function deployFrameFailure(
  message: string,
  frameSent: boolean,
  cause?: unknown,
): DeployFrameFailure {
  return Object.assign(new Error(message, { cause }), { frameSent });
}

export function isDeployFrameFailure(err: unknown): err is DeployFrameFailure {
  return (
    err instanceof Error &&
    "frameSent" in err &&
    typeof err.frameSent === "boolean"
  );
}

/**
 * Identity validation failed or remained pending at the connection deadline.
 * Readiness is unknown: the worker may be healthy behind the lookup, so
 * callers must retry rather than treat this as a missed connection deadline.
 */
export class SidecarIdentityValidationError extends Error {
  constructor(allocationId: string, generation: number, cause?: unknown) {
    super(
      `Cannot validate sidecar identity for allocation ${allocationId} generation ${String(generation)}`,
      { cause },
    );
    this.name = "SidecarIdentityValidationError";
  }
}

/**
 * This Hub holds no current connection for the allocation generation, or the
 * connection closed before the worker answered. The worker may be healthy
 * behind another connection, so callers must retry rather than escalate.
 */
export class WorkflowControlUnreachableError extends Error {
  constructor(message: string, cause?: unknown) {
    super(message, { cause });
    this.name = "WorkflowControlUnreachableError";
  }
}

/** A connected worker is still deploying and cannot process control yet. */
export class WorkflowControlInitializingError extends Error {
  constructor() {
    super("Workflow deployment is still being initialized");
    this.name = "WorkflowControlInitializingError";
  }
}

/**
 * The worker acknowledged the control command, but the Hub could not process
 * the acknowledgement in time. The worker answered, so this never counts as
 * the worker failing to comply.
 */
export class WorkflowControlUnconfirmedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "WorkflowControlUnconfirmedError";
  }
}

/** A live worker connection did not answer the control command in time. */
export class WorkflowControlTimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "WorkflowControlTimeoutError";
  }
}

/** A live worker answered the control command with an error. */
export class WorkflowControlRejectedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "WorkflowControlRejectedError";
  }
}

/**
 * The worker stopped, but the Hub does not hold the history it reported. The
 * stop stays unconfirmed and unfenced, so that history can still land.
 */
export class WorkflowControlHistoryPendingError extends Error {
  constructor(message: string) {
    super(`Workflow history has not reached the Hub: ${message}`);
    this.name = "WorkflowControlHistoryPendingError";
  }
}

export type SidecarConnection = {
  sidecarId: string;
  /**
   * The probe and allocation generations this sidecar currently hosts, keyed
   * by allocation id. A provisioner may place several on one sidecar; each
   * keeps its own generation fence.
   */
  bindings: Map<string, SidecarAuthIdentity>;
  // Allocated deployment routes (including first deploy and reconnect) and
  // transient step routes, each mapped to the allocation that owns it, so one
  // allocation's routes can leave a connection that stays open for the
  // others. `handleClose` cleans them out of `addressIndex`.
  workflowAddresses: Map<string, string>;
  // Deploy frames the worker has not answered, by request id to their
  // address, kept past the Hub's own deploy timeout. The worker handles an
  // address's frames in order, so a control frame sent behind one of these
  // for its address cannot start until the worker answers it.
  unansweredDeploys: Map<string, string>;
  /**
   * Undeploys sent on this connection that the sidecar has not answered yet,
   * by address. The copy one removes can still be sending under the address
   * until then, so a deploy of the address waits for the answer.
   */
  undeploying: Map<string, UndeployInFlight>;
  send(frame: HubFrame): void;
};

type UndeployInFlight = {
  /** The latest undeploy of the address sent on the connection. */
  requestId: string;
  answered: Promise<void>;
  settle(): void;
};

/** The allocation that owns a workflow address routed on this connection. */
function owningAllocation(
  conn: SidecarConnection,
  address: string,
): Extract<SidecarAuthIdentity, { kind: "allocated" }> | undefined {
  const allocationId = conn.workflowAddresses.get(address);
  const binding =
    allocationId === undefined ? undefined : conn.bindings.get(allocationId);
  return binding?.kind === "allocated" ? binding : undefined;
}

/**
 * The allocation this connection hosts whose deployment address is
 * `address`, whether or not the address is routed.
 */
function deploymentBinding(
  conn: SidecarConnection,
  address: string,
): Extract<SidecarAuthIdentity, { kind: "allocated" }> | undefined {
  return allocationBindings(conn).find(
    (binding) => binding.workflowRunAddress === address,
  );
}

/**
 * Whether a sidecar frame names the incarnation this connection routes for
 * its address. A sidecar can still be draining a generation the Hub has
 * fenced, so a frame naming any other generation is never credited to the
 * routed one.
 */
function namesRoutedIncarnation(
  conn: SidecarConnection,
  frame: { agentAddress: string; generation: number },
): boolean {
  return (
    owningAllocation(conn, frame.agentAddress)?.generation === frame.generation
  );
}

/**
 * Bind pack writes to the repository implied by the authenticated
 * incarnation: an allocation may only write its own deployment's workflow-run
 * repository, never a standalone agent-state repository, and only as the
 * generation this connection hosts. It authorizes the write even when the
 * address is not routed: a stopped worker is not routed, but its stop is
 * confirmed only once its remaining history reaches the Hub.
 */
function connCanPushRepo(
  conn: SidecarConnection,
  frame: { agentAddress: string; generation: number; repoId: RepoId },
): boolean {
  const binding = deploymentBinding(conn, frame.agentAddress);
  if (binding?.generation !== frame.generation) return false;
  return (
    frame.repoId.kind === "workflow-run" &&
    frame.repoId.id === deriveWorkflowRunRepoId(frame.agentAddress)
  );
}

function allocationBindings(
  conn: SidecarConnection,
): Extract<SidecarAuthIdentity, { kind: "allocated" }>[] {
  return [...conn.bindings.values()].flatMap((binding) =>
    binding.kind === "allocated" ? [binding] : [],
  );
}

export type SendPackOptions = {
  /**
   * Repo-relative mount path under the sidecar's per-agent workspace.
   * When set, the receiving sidecar materializes the pack as plain
   * files at `<workspaceRoot>/<mountPath>/` and does NOT apply it to
   * the agent's deploy git tree. Absent for agent-state deploy/state
   * packs, which continue to apply to the deploy tree.
   */
  mountPath?: string;
  /**
   * Override the `repoId` emitted on the wire. The agent-state flow
   * defaults to `{ kind: "agent-state", id: agentAddress }`; asset
   * packs must pass the SOURCE asset's id so audit can correlate the
   * pack back to its hub-side origin. Workflow-run restoration uses a
   * dedicated allocation-bound sender that supplies its derived repo id.
   */
  repoId?: RepoId;
};

/**
 * Everything a `sendProbe` caller supplies to populate the outbound
 * `workflow.probe.request` frame: where the definition's bytes come from, the
 * frozen dependency closure the hub already resolved, and the
 * `interchange.workflow` entry-module path whose evaluation produces the
 * `WorkflowDefinition`. The `requestId` is minted inside `sendProbe`, not
 * supplied here.
 */
export type SendProbeArgs = {
  source: WorkflowDefinitionSource;
  closure: ToolPackageManifest;
  entry: string;
  /** Hub assets a `kind:"asset"` closure entry reads from, delivered inline. */
  assets?: WorkflowSourceAssetMount[];
};

/**
 * The payload a `sendProbe` promise resolves with, lifted off the sidecar's
 * `workflow.probe.result` frame: the inert needs-surface projection of the
 * probed workflow, the inert grant set derived from it, the un-flattened grant
 * walk snapshot the set is derived from, and the projection's content hash.
 */
export type WorkflowProbeResult = {
  projection: WorkflowProjectionDefinition;
  grants: string[];
  grantWalkSnapshot: GrantWalkSnapshot;
  wireHash: string;
};

/**
 * The result of a run-address sender's deploy, reported to the router once the
 * deploy's key write is durable. `recorded` carries the sender's now-persisted
 * public key and wakes the sender's parked pre-ack mail for redelivery;
 * `failed` carries a failure reason and drains that mail to
 * `mail.outbound.undelivered`. Modeled as a discriminated result so a settle is
 * unambiguous about which side of the deploy it reports.
 */
export type SenderDeploySettledOutcome =
  | { recorded: string }
  | { failed: string };

export type AllocatedSenderDeployAttempt = AllocatedSidecarTarget & {
  readonly leaseId: string;
};

export type SidecarRouter = {
  handleOpen(ws: WsHandle): void;
  handleMessage(ws: WsHandle, data: string): void;
  handleClose(ws: WsHandle): void;

  routeMail(
    agentAddress: string,
    rawMessage: string,
    authenticatedSender: string,
    messageId?: string,
    runGrants?: {
      runId: string;
      stepGrants: RunGrantsFrame["stepGrants"];
      senderIdentities?: RunGrantsFrame["senderIdentities"];
    },
  ): Promise<boolean>;
  /**
   * Update a run's authorization grants independently of mail. For a trigger,
   * pass `runGrants` to `routeMail` so both frames share one admission decision.
   * Sends on the live connection, stamped with the incarnation the Hub routes
   * for the address, and returns `false` whenever the address is unroutable;
   * the caller keeps any stable-run grant reservation so a later
   * first-delivery attempt reuses it.
   *
   * `senderIdentities` co-delivers the run's authorized senders' resolved keys
   * on the same barrier as the grant, so a recipient that caches from this
   * frame binds each sender address to the hub-vouched key. The caller passes
   * `undefined` when there is no sender to co-deliver (a standing-grant refresh)
   * or the sender has no resolvable key; a null key is never carried.
   */
  sendRunGrants(
    agentAddress: string,
    runId: string,
    stepGrants: RunGrantsFrame["stepGrants"],
    senderIdentities: RunGrantsFrame["senderIdentities"],
  ): boolean;
  /**
   * Report that a run-address sender's deploy has settled, driving any mail the
   * sender parked while its public key was not yet recorded. A `recorded`
   * outcome wakes the parked mail and re-drives its delivery now that the key
   * co-delivers; a `failed` outcome drains it to `mail.outbound.undelivered`.
   * Allocated callbacks name their exact attempt; recovery may settle the
   * previous attempt by generation after claiming its reconciliation lease.
   * An address-only settlement belongs to a non-allocated deployment and cannot
   * settle an allocated attempt. Stale or repeated settlements are no-ops.
   */
  noteSenderDeploySettled(
    sender: string | AllocatedSidecarTarget | AllocatedSenderDeployAttempt,
    outcome: SenderDeploySettledOutcome,
  ): void;
  /**
   * Mark a run-address sender's ALLOCATED deploy as mid-flight, before the deploy
   * emit and its anchor-key update. An allocated run records its key later than
   * the deploy ack clears `pendingDeploys`, so this marker covers the allocated
   * pre-ack window that `pendingDeploys` alone under-covers. `noteSenderDeploySettled`
   * clears it only when the durable outcome is known. Cancellation leaves it
   * pending for recovery, so a lost publication response cannot discard mail.
   */
  noteSenderDeployStarted(
    address: string,
    attempt: AllocatedSenderDeployAttempt,
  ): void;
  /**
   * Returns the current connector-thread state for the named agent, or
   * `null` if the agent has no active connector thread (or if the
   * sidecar has not yet reported any state — e.g. mid-reconnect, before
   * the harness has loaded its context store). The state is cached
   * from `connector.state.changed` frames; callers should treat `null`
   * as "no threading info available" and fall through to whatever
   * default the calling path uses.
   */
  getConnectorState(agentAddress: string): ConnectorThreadState | null;
  sendAgentUndeploy(agentAddress: string, reason: string): Promise<void>;
  sendSourcesUpdate(
    agentAddress: string,
    sources: InferenceSource[],
    defaultSource: string,
  ): Promise<void>;
  sendCredentialsUpdate(
    agentAddress: string,
    delivery: CredentialDelivery,
    revoke?: string[],
  ): Promise<void>;
  /**
   * Deliver a workflow-run signal to the sidecar that hosts the named
   * deployment-level mail address. The sidecar's hub-link routes the
   * frame through its `signalInboundRouter` into the deployment's
   * supervisor, which sends a `signal.deliver` control IPC frame to
   * the workflow-process child. The child commits the resulting
   * `SignalReceived` event through its own substrate -- the single
   * writer of the workflow-run repo on the sidecar side -- so the
   * pack-push pipeline that propagates the commit to the hub never
   * sees a concurrent writer at the same ref.
   *
   * Throws when no sidecar is registered for `agentAddress`; the
   * caller is responsible for ensuring the deployment is live.
   */
  sendSignalDeliver(opts: {
    agentAddress: string;
    runId: string;
    signalName: string;
    signalId: string;
    payload: unknown;
  }): Promise<void>;
  /**
   * Deliver a workflow-host drain control payload to the sidecar that
   * hosts the named deployment-level mail address. The sidecar's
   * hub-link routes the frame through its `drainInboundRouter` into
   * the deployment's supervisor, which sends a `drain` control IPC
   * frame to the workflow-process child and arms one `drainTimeout`
   * accumulator per in-flight run. Cancel-mode steps abort on the
   * child side; wait-mode steps continue. Accumulators commit a
   * signed `CancelRequested{origin: "supervisor-drain"}` against the
   * workflow-run repo when the deadline expires.
   *
   * Throws when no sidecar is registered for `agentAddress`; the
   * caller is responsible for ensuring the deployment is live.
   */
  sendDrain(opts: { agentAddress: string; deadlineMs: number }): void;

  subscribeAgent(
    agentAddress: string,
    callback: (event: unknown) => void,
  ): () => void;
  dispatchAgentEvent(agentAddress: string, event: unknown): void;

  getConnectedSidecars(): string[];
  getRoutableAddresses(): string[];

  /** Typed event emitter for the receiver-dispatch surface. See
   * `sidecar-events.ts` for the event map and emission semantics. */
  events: SidecarEventEmitter;
};

/**
 * One probe or allocation generation a sidecar hosts, as
 * `resolveSidecarBindings` reads it for the sidecar the WebSocket handshake
 * authenticated. The `sidecarId` is that authenticated id, not the untrusted
 * `sidecarId` claimed on the hello frame.
 */
export type SidecarAuthIdentity = SidecarCredentialIdentity;

export type AllocatedSidecarTarget = {
  readonly allocationId: string;
  readonly generation: number;
};

export type SidecarAllocationRouter = {
  sendWorkflowControl(
    target: AllocatedSidecarTarget,
    command: Omit<WorkflowControlFrame, "type" | "requestId" | "generation">,
    timeoutMs: number,
  ): Promise<void>;
  /** Advance the in-memory trust boundary before provisioning a generation. */
  fenceAllocation(allocationId: string, generation: number): void;
  /**
   * Remove an exact generation's fence after its durable owner becomes
   * terminal. Durable identity validation rejects later stale reconnects.
   */
  retireAllocation(target: AllocatedSidecarTarget): void;
  /**
   * Resolve once the exact authenticated allocation generation is connected.
   * Throws `SidecarIdentityValidationError` when readiness cannot be
   * determined; only confirmed absence surfaces as a connection timeout.
   * `onValidation` observes notification lookups that may outlive this wait.
   * Cancellation removes a parked waiter and discards pending validation
   * results.
   */
  waitForAllocatedSidecar(
    target: AllocatedSidecarTarget,
    timeoutMs: number,
    onValidation?: (validation: Promise<boolean>) => void,
    signal?: AbortSignal,
  ): Promise<void>;
  /**
   * Check exact allocated readiness without parking a reconciliation worker.
   * Throws `SidecarIdentityValidationError` when identity validation fails;
   * `false` means the worker is confirmed absent or stale.
   */
  isAllocatedSidecarReady(target: AllocatedSidecarTarget): Promise<boolean>;
  /**
   * Whether a connection already holds the allocation's binding at this
   * generation, without validating it. A probe binding the allocation adopted
   * is not that binding: only a sync or a registration attaches the allocation
   * over it.
   */
  holdsAllocatedBinding(target: AllocatedSidecarTarget): boolean;
  /**
   * Whether the generation's current connection routes its deployment. Throws
   * when the generation has no current connection or identity validation
   * fails: a sidecar that is only cut off may still hold the deployment.
   */
  isAllocatedWorkflowActive(target: AllocatedSidecarTarget): Promise<boolean>;
  /** Probe a workflow on the exact provisioned allocation generation. */
  sendProbeToAllocation(
    target: AllocatedSidecarTarget,
    args: SendProbeArgs,
  ): Promise<WorkflowProbeResult>;
  /**
   * Remove an exact generation from its sidecar's connection before changing
   * its durable owner. The sidecar is told to undeploy an allocation's
   * deployment, and the socket stays open while the sidecar hosts other
   * bindings.
   */
  detachAllocation(target: AllocatedSidecarTarget): void;
  /**
   * Bring a connected sidecar's bindings up to date with the database after
   * a probe or allocation was placed on it, or left it, while it stayed
   * connected. A sidecar that is registering, or not connected, picks its
   * bindings up when its registration completes. The promise can settle
   * before a binding attaches, so callers wait for its readiness instead.
   * Cancellation prevents queued work or a pending binding read from attaching.
   */
  syncSidecar(sidecarId: string, signal?: AbortSignal): Promise<void>;
  sendAgentDeployToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    config: HarnessConfig,
    workflow?: AgentDeployFrame["workflow"],
    signal?: AbortSignal,
    beforeSend?: () => Promise<void>,
  ): Promise<{ publicKey: string }>;
  sendPackToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    options?: SendPackOptions,
  ): Promise<void>;
  /**
   * Restore one Hub-authoritative workflow-run ref onto the exact allocation
   * generation before its deployment address is routed or supervisor spawned.
   */
  sendWorkflowRunPackToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    signal?: AbortSignal,
  ): Promise<void>;
  bindAllocatedStepRoute(
    target: AllocatedSidecarTarget,
    stepAddress: string,
  ): Promise<void>;
  unbindAllocatedStepRoute(
    target: AllocatedSidecarTarget,
    stepAddress: string,
  ): void;
  sendProvisionStepToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    config: HarnessConfig,
  ): Promise<void>;
  /**
   * Deliver one durable workflow trigger to the exact allocation generation.
   * Grants and mail are written to the same websocket in FIFO order. The
   * returned promise proves only that both frames were sent; the sidecar's
   * durable-inbox acknowledgement is surfaced separately through
   * `mail.inbound.acknowledged`.
   */
  sendWorkflowRunDispatchToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    runId: string,
    stepGrants: RunGrantsFrame["stepGrants"],
    rawMessage: string,
    authenticatedSender: string,
    messageId: string,
    signal?: AbortSignal,
  ): Promise<void>;
  /** Deliver an idempotent signal to the exact provisioned generation. */
  sendSignalDeliverToAllocation(
    target: AllocatedSidecarTarget,
    opts: {
      agentAddress: string;
      runId: string;
      signalName: string;
      signalId: string;
      payload: unknown;
    },
    signal?: AbortSignal,
  ): Promise<void>;
};

/**
 * Resolves the credentials a sidecar presents on the handshake to the
 * verified sidecar, or `null` when they are not recognized. A sidecar that
 * hosts nothing current still resolves, so its handshake can undeploy what it
 * reports before turning it away. The claimed `sidecarId` is an
 * unauthenticated hint; the authenticator derives the trusted identity from
 * the `token` and the returned `sidecarId` is what the router keys connection
 * state off of. What the sidecar hosts is read through
 * `resolveSidecarBindings` once the handshake has claimed the sidecar.
 */
export type SidecarAuthenticator = (claim: {
  sidecarId: string;
  token: string;
}) => Promise<SidecarCredentials | null>;

export type SidecarRouterConfig = {
  requestTimeoutMs?: number;
  /** Hex-encoded 32-byte Ed25519 public key for signing deploy commits.
   * Included in agent.deploy frames so sidecars can verify pack signatures. */
  hubPublicKey?: string;
  /** Resolves each hello handshake to a verified sidecar
   * identity. Required: without it a connection could route on an
   * unverified frame claim. Return null to reject the handshake. */
  authenticateSidecar: SidecarAuthenticator;
  /** Revalidate durable identity at registration and routing boundaries. */
  validateSidecarIdentity: (
    identity: SidecarAuthIdentity,
    use: SidecarIdentityUse,
  ) => Promise<boolean>;
  /** Lock current allocation/lifecycle state while the synchronous send runs. */
  withExecutableWorkflowRun: (
    target: WorkflowRunExecutionTarget,
    send: () => boolean,
    signal?: AbortSignal,
  ) => Promise<boolean>;
  /** The bindings a sidecar currently hosts, for `syncSidecar`. */
  resolveSidecarBindings: (
    sidecarId: string,
  ) => Promise<readonly SidecarAuthIdentity[]>;
  /** Timeout for a `sendProbe` round-trip. A probe materializes a workflow's
   * dependency closure and evaluates it on the sidecar, so it can run longer
   * than a routine `sendRequest`; it gets its own timeout rather than sharing
   * the request timeout. */
  probeTimeoutMs?: number;
  /** How long un-acknowledged mail and mail awaiting its sender's key are
   * held for a delivery before the Hub gives up on it. */
  mailHoldTTLMs?: number;
  pingTimeoutMs?: number;
  /** Interval between redelivery attempts of a connected-window `mail.inbound`
   * the sidecar has not yet acknowledged with `mail.inbound.ack`. */
  mailAckRetryIntervalMs?: number;
  /**
   * Arms the mail-redelivery retry and the connection-liveness timers, and
   * returns each one's canceller. Defaults to the global timer, which is what
   * production wants.
   *
   * The intervals beside it say how long until something should happen; this
   * says what makes it happen. With only the intervals injectable, a test had
   * to shorten one and then sleep past it, which turns an assertion about
   * WHETHER something happened into a bet on how much the machine got through
   * -- and, for the liveness deadline, on a pause landing inside a window
   * rather than past it.
   *
   * REQUIRED of the returned canceller: calling it more than once must be
   * harmless. A pending-mail entry outlives a disconnect, so the disconnect
   * path cancels its retry and a later reconnect can cancel the same one
   * again. `clearTimeout` on an already-cleared timer is a no-op, which is
   * what makes the default satisfy this; a substitute must arrange the same,
   * typically by flipping a flag.
   */
  scheduleTimeout?: (handler: () => void, ms: number) => () => void;
  /** Maximum redelivery attempts before the hub stops retrying an un-acked
   * connected-window `mail.inbound`. Bounds the retry so a sidecar that never
   * acks does not accumulate an unbounded timer per delivery. */
  mailAckMaxRetries?: number;
  /** Query handlers the wire layer issues during frame processing. */
  lookups?: SidecarLookups;
};

// Re-exported so existing consumers keep importing the handle type from the
// router module; the definition now lives in `pending-tracker.ts`, which also
// operates on it.
export type { WsHandle };

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
// A probe fetches a workflow's dependency closure from a registry and
// evaluates it on the sidecar, so it runs longer than a routine request; its
// default timeout is correspondingly wider than DEFAULT_REQUEST_TIMEOUT_MS.
export const DEFAULT_PROBE_TIMEOUT_MS = 60_000;
const DEFAULT_MAIL_HOLD_TTL_MS = 5 * 60 * 1000;
const DEFAULT_PING_TIMEOUT_MS = 60_000;
const DEFAULT_MAIL_ACK_RETRY_INTERVAL_MS = 10_000;
const DEFAULT_MAIL_ACK_MAX_RETRIES = 5;
const INCARNATION_VALIDATION_CONCURRENCY = 8;

const MalformedPackDone = type({
  type: "'repo.pack.done'",
  transferId: "string > 0",
  agentAddress: "string > 0",
  repoId: RepoId,
});

// The hub re-resolves and re-pushes a key for each rotatable sender a sidecar
// reports on (re)connect. A legitimate sidecar caches keys for tens, maybe low
// hundreds of distinct user senders, so this cap sits well above ten times that
// ceiling: it NEVER truncates a real report -- dropping a genuine sender would
// leave its key stale, the exact failure this refresh exists to prevent. It
// bounds only a hostile or buggy sidecar, since a compromised authenticated
// sidecar could otherwise report an unbounded set and drive that many sequential
// DB resolves on every reconnect. The cap lives in the handler, not on the
// arktype frame schema, on purpose: rejecting an over-cap frame at parse would
// fail the whole reconnect (a hard outage) rather than degrade gracefully to a
// bounded refresh.
export const MAX_RESYNC_SENDER_ADDRESSES = 2048;

export function createSidecarRouter(
  config: SidecarRouterConfig,
): SidecarRouter & SidecarAllocationRouter {
  const {
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
    probeTimeoutMs = DEFAULT_PROBE_TIMEOUT_MS,
    hubPublicKey: hubPublicKeyHex,
    authenticateSidecar,
    validateSidecarIdentity,
    withExecutableWorkflowRun,
    resolveSidecarBindings,
    mailHoldTTLMs = DEFAULT_MAIL_HOLD_TTL_MS,
    pingTimeoutMs = DEFAULT_PING_TIMEOUT_MS,
    mailAckRetryIntervalMs = DEFAULT_MAIL_ACK_RETRY_INTERVAL_MS,
    scheduleTimeout = (handler: () => void, ms: number) => {
      const handle = setTimeout(handler, ms);
      return () => {
        clearTimeout(handle);
      };
    },
    mailAckMaxRetries = DEFAULT_MAIL_ACK_MAX_RETRIES,
    lookups = {},
  } = config;

  // Receiver-dispatch surface. Wire-layer callsites emit events here;
  // host code subscribes via `router.events`.
  const events = createSidecarEmitter();

  async function withAllocationWorkAdmission(
    ws: WsHandle,
    binding: Extract<SidecarAuthIdentity, { kind: "allocated" }>,
    send: () => boolean,
    signal?: AbortSignal,
  ): Promise<boolean> {
    return withExecutableWorkflowRun(
      binding,
      () => {
        signal?.throwIfAborted();
        if (
          allocationFences.get(binding.allocationId) !== binding.generation ||
          allocatedConnections.get(binding.allocationId)?.ws !== ws ||
          !connections.has(ws)
        )
          throw new Error(
            `Workflow connection changed for allocation ${binding.allocationId}`,
          );
        return send();
      },
      signal,
    );
  }

  async function withWorkflowWorkAdmission(
    ws: WsHandle,
    conn: SidecarConnection,
    agentAddress: string,
    send: () => boolean,
    signal?: AbortSignal,
  ): Promise<boolean> {
    const binding = owningAllocation(conn, agentAddress);
    const changed = () =>
      new Error(
        `Workflow connection changed before delivery to ${agentAddress}`,
      );
    if (binding === undefined) throw changed();
    return withAllocationWorkAdmission(
      ws,
      binding,
      () => {
        if (
          addressIndex.get(agentAddress) !== ws ||
          conn.workflowAddresses.get(agentAddress) !== binding.allocationId
        )
          throw changed();
        return send();
      },
      signal,
    );
  }

  // ws handle → registered connection
  const connections = new Map<WsHandle, SidecarConnection>();
  // sidecar id → its current socket
  const sidecarSockets = new Map<string, WsHandle>();
  // sidecar id → the socket whose handshake is registering it. The previous
  // socket keeps serving until that registration takes over, but syncs for
  // the sidecar queue behind the registration instead.
  const sidecarClaims = new Map<string, WsHandle>();
  // A handshake still authenticating when its socket closes must not
  // register the socket afterwards.
  const closedSockets = new WeakSet<WsHandle>();
  // Sockets whose sidecar was sent `welcome`, and so is registered.
  const welcomedSockets = new WeakSet<WsHandle>();
  const allocatedConnections = new Map<
    string,
    {
      ws: WsHandle;
      identity: SidecarAuthIdentity;
    }
  >();
  const allocationFences = new Map<string, number>();
  // allocationId -> generation whose worker acknowledged a workflow stop. Its
  // later workflow-run packs are refused, including after a reconnect.
  const stoppedAllocations = new Map<string, number>();
  type AllocationWaiter = {
    generation: number;
    resolve(): void;
    reject(error: Error): void;
    timer: ReturnType<typeof setTimeout>;
    validationFailure?: SidecarIdentityValidationError;
    validations: Set<Promise<boolean>>;
    onValidation?: (validation: Promise<boolean>) => void;
  };
  const allocationWaiters = new Map<string, Set<AllocationWaiter>>();
  // agentAddress → ws handle (routing table)
  const addressIndex = new Map<string, WsHandle>();
  // requestId → pending promise (resolved by session.ack, rejected by
  // session.error), carrying the target address so an allocation leaving a
  // shared connection rejects only its own requests. `PendingTracker` owns the
  // register/timeout/settle/sweep lifecycle shared by all five pending
  // round-trips below; each entry's resolve/reject closures carry the
  // per-round-trip cleanup.
  const pendingRequests = new PendingTracker<string, void, string>();
  // agentAddress → the deploy in flight for it, answered by the
  // agent.deploy.ack or agent.deploy.error carrying its request id and
  // generation. One deploy per address is in flight at a time; a reply that
  // names another request answers a deploy the Hub already gave up on.
  type PendingDeploy = { requestId: string; generation: number };
  const pendingDeploys = new PendingTracker<string, string, PendingDeploy>();
  // Run addresses whose ALLOCATED deploy is mid-flight -- key-record has been
  // started but not yet committed. pendingDeploys clears at the deploy ack, but an
  // allocated run's key is recorded LATER by session-service's anchor-key update,
  // so pendingDeploys alone under-covers the allocated pre-ack window. session-
  // service brackets this marker across its deploy try/catch: set before the
  // deploy emit, cleared by noteSenderDeploySettled on record or failure.
  const allocatedKeyRecordInFlight = new Map<
    string,
    AllocatedSenderDeployAttempt
  >();
  // agentAddress → messageId → connected-window mail awaiting a
  // `mail.inbound.ack`. A `mail.inbound` delivered over a LIVE connection with
  // a hub-minted messageId is tracked here and redelivered -- identical bytes,
  // same messageId -- on a timer until the sidecar acknowledges its durable
  // inbox write, so a frame silently dropped in the connected window (a socket
  // that half-died before the sidecar wrote the message) is recovered rather
  // than lost. The sidecar inbox is idempotent on messageId, so a redelivery
  // of a message the sidecar already wrote is deduped there: at-least-once
  // redelivery is effectively-once.
  type PendingMailEntry = {
    agentAddress: string;
    messageId: string;
    frame: MailInboundFrame;
    attempts: number;
    /**
     * Disarms this entry's redelivery retry. Safe to call more than once:
     * `scheduleTimeout` requires an idempotent canceller. The second call is
     * reached when a reconnect drops a generation-local entry the disconnect
     * had already cancelled -- the disconnect keeps the entry for replay, so
     * its spent canceller is still on it.
     */
    cancelRetry: () => void;
    /**
     * The retry loop that owns the entry's redelivery, replaced when a
     * redelivery rearms the retry. Cancelling a timer cannot stop a retry
     * already awaiting its replay, so a retry whose loop was replaced neither
     * sends nor rearms.
     */
    retryLoop: symbol;
    // When this mail triggers a workflow run, the run's already-materialized
    // grants ride alongside it. Redelivery replays this snapshot as a
    // `run.grants` frame AHEAD of the mail so the redelivered trigger lands on
    // a sidecar that has the run's grants, rather than failing its onRunStart
    // barrier closed. Re-materializing at redelivery time is unsafe (it carries
    // commit/authority semantics); replaying the same bytes is not. The
    // co-delivered `senderIdentities` ride the same snapshot, so a sidecar that
    // first learns the grant on the reconnect replay also learns the key.
    runGrants?: {
      runId: string;
      stepGrants: RunGrantsFrame["stepGrants"];
      senderIdentities?: RunGrantsFrame["senderIdentities"];
    };
    /** The incarnation the mail was delivered to, which alone may ack it. */
    allocatedTarget: AllocatedSidecarTarget;
    /**
     * Whether a durable dispatch row stands behind the mail. The row delivers
     * it again to whichever generation replaces the one it went to, so the
     * entry covers only that generation. Mail with no row behind it follows
     * the deployment to the generation routed next instead.
     */
    dispatchBacked: boolean;
  };
  const pendingMail = new Map<string, Map<string, PendingMailEntry>>();
  // agentAddress → retention TTL timer for un-acked pending mail held across a
  // disconnect. On close the per-entry retry timers are cleared (the socket is
  // gone) but the entries are RETAINED so a verified reconnect can redeliver
  // them; this timer bounds that retention so a sidecar that never reconnects
  // does not leak entries. Cleared when the address reconnects (redelivery) or
  // its last pending entry is acked.
  const pendingMailRetention = new Map<string, ReturnType<typeof setTimeout>>();
  // sender address → pre-ack mail parked while the sender's public key is not
  // yet recorded. A run mints its signing keypair locally at the sidecar and can
  // send mail before the hub has recorded its `public_key`; such mail resolves a
  // null sender key, so no key co-delivers and a strict recipient drops it as an
  // unknown sender. Each entry holds what a re-drive of `handleMailOutbound`
  // needs -- the raw message and its recipients -- plus a per-entry TTL timer.
  // This is a SIBLING of `pendingMail`, keyed by SENDER: it means "the SENDER's
  // key is missing," not that the recipient is unreachable. Here the
  // recipient is live; only the sender's key is absent. `noteSenderDeploySettled`
  // drives an entry to delivery once the key lands or to
  // `mail.outbound.undelivered` when the deploy fails; the TTL backstops the case
  // where neither happens.
  type DeferredSenderMailEntry = {
    authenticatedSender: string;
    rawMessage: string;
    recipients: string[];
    timer: ReturnType<typeof setTimeout>;
  };
  const deferredSenderMail = new Map<string, Set<DeferredSenderMailEntry>>();
  // Recipient address → mail held because that deployment has not recorded
  // credential resolution. A sibling of `deferredSenderMail`: keyed by the
  // RECIPIENT. A recorded settle re-drives with the sender key bound when
  // the mail was accepted, not the recipient's key. `rejected` is the
  // wrong outcome here -- it drops the mail.
  type DeferredRecipientResolutionMailEntry = {
    recipient: string;
    authenticatedSender: string;
    rawMessage: string;
    recipients: string[];
    // The sender key bound for this message before the hold. Null means
    // that resolve found no key. A redrive passes this value so a later
    // key cannot replace it; omitting it would resolve again.
    recordedSenderKey: string | null;
    // Armed when the attempt finishes parked with no wake. Null while the
    // attempt is in flight, and after the hold is claimed.
    cancelTimer: (() => void) | null;
    // True until the attempt that parked this entry finishes. A settle during
    // that await records `wake` and does not re-drive yet: the attempt may
    // still route, and a re-drive beside it would deliver the mail twice.
    inflight: boolean;
    wake: SenderDeploySettledOutcome | null;
  };
  const deferredRecipientResolutionMail = new Map<
    string,
    Set<DeferredRecipientResolutionMailEntry>
  >();
  // agentAddress → set of subscriber callbacks for agent events
  const agentSubscribers = new Map<string, Set<(event: unknown) => void>>();
  // agentAddress → cached connector-thread state, populated by
  // connector.state.changed frames. Hub-side mail composition reads this
  // to set threading headers on user-originated mail. Absent entries mean
  // "no state reported yet" (e.g. mid-reconnect); callers must treat that
  // identically to a null entry (no active thread).
  const connectorStates = new Map<string, ConnectorThreadState | null>();
  // ws handle → liveness timer (reset on each ping from the sidecar)
  const livenessTimers = new Map<WsHandle, () => void>();

  // Per-ws serialization chain for QUEUE-class frames (see `frameBypassesQueue`
  // for the split and the invariant behind it). A frame that establishes or
  // reads routing waits for earlier queued frames on the same ws to complete,
  // so it observes their finished effects -- most importantly a hello's
  // routing write, which would otherwise land after a following
  // connector.state.changed / mail / pack frame and silently drop it. It holds
  // a single in-flight promise per ws (replaced each queued frame), cleared on
  // close.
  const messageChains = new Map<WsHandle, Promise<void>>();

  // transferId → pending pack transfer (resolved by repo.pack.ack, rejected
  // by repo.pack.reject). The entry carries the send-site agentAddress and
  // repoId so an ack/reject is honored only when it comes from the
  // connection that owns the transfer for the same repo. Transfer ids are
  // random, so an answer to a transfer from before a Hub restart never
  // matches a new one.
  type PackTransferMeta = { agentAddress: string; repoId: RepoId };
  const pendingPacks = new PendingTracker<string, void, PackTransferMeta>();

  // agentAddress → the undeploy a caller awaits for it, answered by the
  // agent.undeploy.ack or agent.undeploy.error carrying its request id.
  const pendingUndeploys = new PendingTracker<string, void, string>();
  const pendingWorkflowControls = new PendingTracker<
    string,
    void,
    AllocatedSidecarTarget &
      Pick<WorkflowControlFrame, "agentAddress" | "action"> & {
        fail(error: Error): void;
        received(): void;
      }
  >(scheduleTimeout);

  // requestId → pending workflow probe (resolved by workflow.probe.result,
  // rejected by workflow.probe.error). Result-carrying, unlike the other
  // trackers (which resolve void): a probe returns the sidecar's inert
  // projection + grant set + wire hash. The probe runs in the sidecar's
  // pre-deploy state and enters no address map, so each entry carries its
  // probe id: the ws-keyed sweeps -- the whole connection closing, or that
  // probe leaving a connection other bindings keep open -- are its only
  // disconnect cleanup.
  const pendingProbes = new PendingTracker<
    string,
    WorkflowProbeResult,
    string
  >();

  // Receives agent-state packs pushed from sidecars. The wire frames
  // (`repo.pack.push` / `repo.pack.done`) are shared with the
  // workflow-run flow; dispatch on `repoId.kind` picks which receiver
  // observes the chunks. The two receivers maintain independent
  // in-flight pack state and independent cancel-by-agent semantics so a
  // pending workflow-run transfer cannot disturb a concurrent agent-
  // state transfer for the same agent and vice versa.
  const agentStatePackReceiver = createPackReceiver();
  const workflowRunPackReceiver = createPackReceiver();

  let requestCounter = 0;

  // Arm a redelivery-retry timer for a tracked pending mail. Wraps the async
  // `retryPendingMail` so a rejection -- a socket write that throws once the
  // sidecar is gone -- is logged rather than floating out of the timer as an
  // unhandled rejection.
  function scheduleMailRetry(
    agentAddress: string,
    messageId: string,
    retryLoop: symbol,
  ): () => void {
    return scheduleTimeout(() => {
      void retryPendingMail(agentAddress, messageId, retryLoop).catch(
        (err: unknown) => {
          logger.warn`Redelivery retry for mail ${messageId} to ${agentAddress} failed: ${err instanceof Error ? err.message : String(err)}`;
        },
      );
    }, mailAckRetryIntervalMs);
  }

  // Track a connected-window `mail.inbound` for redelivery until the sidecar
  // acks its durable inbox write. Replaces any prior entry for the same
  // (agentAddress, messageId) -- clearing its timer first so no timer leaks --
  // which keeps a re-sent delivery from arming a second concurrent retry loop.
  function trackPendingMail(
    agentAddress: string,
    messageId: string,
    frame: MailInboundFrame,
    allocatedTarget: AllocatedSidecarTarget,
    dispatchBacked: boolean,
    runGrants?: {
      runId: string;
      stepGrants: RunGrantsFrame["stepGrants"];
      senderIdentities?: RunGrantsFrame["senderIdentities"];
    },
  ): void {
    let byId = pendingMail.get(agentAddress);
    if (byId === undefined) {
      byId = new Map();
      pendingMail.set(agentAddress, byId);
    }
    const existing = byId.get(messageId);
    if (existing !== undefined) existing.cancelRetry();
    const retryLoop = Symbol("mail-retry");
    byId.set(messageId, {
      agentAddress,
      messageId,
      frame,
      attempts: 0,
      cancelRetry: scheduleMailRetry(agentAddress, messageId, retryLoop),
      retryLoop,
      allocatedTarget,
      dispatchBacked,
      ...(runGrants !== undefined ? { runGrants } : {}),
    });
  }

  // A pending mail the Hub stops redelivering. Mail a dispatch row stands
  // behind is left to the row: it delivers the mail again once the deployment
  // is next ready, unless the deployment ended, which abandons the row. Any
  // other mail is surfaced as undelivered.
  function abandonPendingMail(entry: PendingMailEntry, reason: string): void {
    if (entry.dispatchBacked) {
      logger.warn`Stopped redelivering mail ${entry.messageId} to ${entry.agentAddress} and left it to its dispatch row: ${reason}`;
      return;
    }
    events.emit("mail.outbound.undelivered", {
      rawMessage: entry.frame.rawMessage,
      recipients: [entry.agentAddress],
    });
    logger.warn`Dropping un-acked mail ${entry.messageId} for ${entry.agentAddress}: ${reason}`;
  }

  // A pending mail whose generation no longer holds its address. Mail a
  // dispatch row stands behind is left to the row, which delivers it to the
  // generation routed now. Any other mail follows the deployment there,
  // restamped for that generation, or is dropped when no generation holds
  // the address. Returns whether the entry is still pending.
  function followRoutedGeneration(
    byId: Map<string, PendingMailEntry>,
    entry: PendingMailEntry,
    routed: Extract<SidecarAuthIdentity, { kind: "allocated" }> | undefined,
  ): boolean {
    if (entry.dispatchBacked) {
      entry.cancelRetry();
      deletePendingMail(byId, entry.agentAddress, entry.messageId);
      logger.info`Leaving mail ${entry.messageId} for ${entry.agentAddress} to its dispatch row: generation ${String(entry.allocatedTarget.generation)} no longer holds the address`;
      return false;
    }
    if (routed === undefined) {
      entry.cancelRetry();
      deletePendingMail(byId, entry.agentAddress, entry.messageId);
      logger.warn`Dropping un-acked mail ${entry.messageId} for ${entry.agentAddress}: no live connection to redeliver over`;
      return false;
    }
    entry.allocatedTarget = {
      allocationId: routed.allocationId,
      generation: routed.generation,
    };
    entry.frame = { ...entry.frame, generation: routed.generation };
    return true;
  }

  // Resolve the frame that must precede a redelivery of a trigger mail on the
  // FIFO socket, re-resolving a keyless run sender's key so it still
  // co-delivers. Returns:
  //   - a `run.grants` frame when the entry carries run grants (the redelivered
  //     run resolves its onRunStart barrier instead of failing closed on
  //     missing grants);
  //   - a bare `sender.key.refresh` frame when the entry carries NO run grants
  //     but its run sender's key was never co-delivered, so the recipient still
  //     caches the key ahead of the mail;
  //   - `undefined` when nothing must precede the mail.
  //
  // The re-resolve is KIND-GATED to run-address senders only. A run's
  // deployment key is immutable once acked, so the re-resolved key equals the
  // signing-time key -- safe. A user (non-run) sender's key may have rotated
  // since it signed, so re-resolving would check the fixed signed bytes against
  // a newer key and turn a valid message into a false `invalid`; such a sender
  // lacking a captured key stays keyless (an honest `unknown`). An entry that
  // captured `senderIdentities` at track time replays that snapshot as-is: it
  // holds the signing-time key and is never re-resolved.
  //
  // Awaits any key resolve so the caller sends the returned frame and the mail
  // back-to-back with no await between them, keeping the co-delivered key ahead
  // of the mail on the FIFO socket.
  async function resolveReplayLeadFrame(
    entry: PendingMailEntry,
  ): Promise<HubFrame | undefined> {
    const authenticatedSender = entry.frame.authenticatedSender;
    const senderIsRun = isRunAddress(authenticatedSender);

    if (entry.runGrants === undefined) {
      if (!senderIsRun) return undefined;
      const key = await reresolveRunSenderKey(authenticatedSender);
      if (key === null) return undefined;
      return {
        type: "sender.key.refresh",
        address: authenticatedSender,
        publicKey: key,
      };
    }

    let senderIdentities = entry.runGrants.senderIdentities;
    if (senderIdentities === undefined && senderIsRun) {
      const key = await reresolveRunSenderKey(authenticatedSender);
      senderIdentities = senderIdentitiesFromKey(authenticatedSender, key);
    }
    return {
      type: "run.grants",
      agentAddress: entry.agentAddress,
      generation: entry.frame.generation,
      runId: entry.runGrants.runId,
      stepGrants: entry.runGrants.stepGrants,
      ...(senderIdentities !== undefined ? { senderIdentities } : {}),
    };
  }

  // Best-effort re-resolve of a run sender's hub-held key at replay time. Only
  // called for a run-address sender, whose deployment key is immutable once
  // acked, so the current key equals the signing-time key. Returns null when no
  // resolver is wired or the sender has no durable key.
  //
  // This relies on `lookups.resolveSenderKey` being the BEST-EFFORT,
  // NEVER-THROWS resolver (the contract at sidecar-events.ts:274-279, wired to
  // resolveFrameSenderKey, which swallows faults to null). That contract is
  // load-bearing here: `redeliverPendingMail` clears the retention TTL up-front
  // and re-arms each entry's per-entry timer only on a successful send, so a
  // resolver that THREW would abort the redeliver loop and strand the
  // not-yet-processed entries with no timer and no TTL until a process restart.
  // A strict/throwing resolver must NOT be wired here. Do not add a try/catch:
  // the boundary owns the never-throws contract; duplicating it here would
  // violate that ownership. The dispatch-time resolveSenderKey call
  // (sendWorkflowRunDispatchToAllocation path) carries the same dependency
  // note.
  async function reresolveRunSenderKey(
    authenticatedSender: string,
  ): Promise<string | null> {
    const resolveSenderKey = lookups.resolveSenderKey;
    if (resolveSenderKey === undefined) return null;
    return resolveSenderKey(authenticatedSender);
  }

  // Replay a pending mail's lead frame (its run grants or a re-resolved sender
  // key) and then the mail itself over `conn`. Awaits the resolve FIRST, then
  // sends the lead frame and the mail back-to-back with NO await between them,
  // so the co-delivered key always precedes the mail on the FIFO socket.
  // Returns whether the mail was sent and arms its retry inside admission,
  // before an acknowledgement can remove the pending entry. A retry passes its
  // loop, and sends nothing once a redelivery has replaced that loop; a
  // redelivery passes none and starts the entry's one retry loop afresh.
  async function replaySendPendingMail(
    conn: SidecarConnection,
    entry: PendingMailEntry,
    retryLoop: symbol | undefined,
  ): Promise<boolean> {
    const lead = await resolveReplayLeadFrame(entry);
    // The resolve above may have awaited real I/O; during that gap a queued
    // `mail.inbound.ack` can advance and run `resolvePendingMail` (delete +
    // clearTimeout) on this entry. `mail.inbound.ack` is a QUEUED frame
    // (frameBypassesQueue returns false for it), so it interleaves only with a
    // replay that runs off the owning ws's message chain: the retry, an
    // independent setTimeout macrotask (retryPendingMail). On the hello's
    // redelivery the ack cannot interleave -- it queues behind the
    // still-running registration on the same ws. Re-confirm it is still the
    // tracked entry before sending, or a post-ack redelivery would arm a retry
    // timer on a detached entry.
    if (pendingMail.get(entry.agentAddress)?.get(entry.messageId) !== entry) {
      return false;
    }
    if (retryLoop !== undefined && entry.retryLoop !== retryLoop) return false;
    // The same gap can span a disconnect or a takeover that moves the address
    // off `conn`. Sending on the stale conn would write to a dead socket and
    // re-arm a retry that later drops a still-retained entry. Skip so the entry
    // survives for the reconnect redelivery.
    const ws = addressIndex.get(entry.agentAddress);
    if (ws === undefined || connections.get(ws) !== conn) return false;
    const stillOwned = (): boolean =>
      pendingMail.get(entry.agentAddress)?.get(entry.messageId) === entry &&
      (retryLoop === undefined || entry.retryLoop === retryLoop);
    // Mail tracked since the address was routed here still has its retry
    // armed, and a retry of it may still be awaiting its replay; one retry
    // loop per entry, so a redelivery replaces the loop.
    const rearm = (attempts: number): void => {
      entry.cancelRetry();
      entry.attempts = attempts;
      if (retryLoop === undefined) entry.retryLoop = Symbol("mail-retry");
      entry.cancelRetry = scheduleMailRetry(
        entry.agentAddress,
        entry.messageId,
        entry.retryLoop,
      );
    };
    try {
      return await withWorkflowWorkAdmission(
        ws,
        conn,
        entry.agentAddress,
        () => {
          if (!stillOwned()) return false;
          if (lead !== undefined) conn.send(lead);
          conn.send(entry.frame);
          // Arm before releasing the database locks: an ack may arrive while
          // the transaction finishes and must be able to cancel this timer.
          rearm(retryLoop === undefined ? 0 : entry.attempts + 1);
          return true;
        },
      );
    } catch (error) {
      if (error instanceof WorkflowRunNotExecutableError) {
        logger.warn`Dropping un-acked mail ${entry.messageId} for ${entry.agentAddress}: its workflow run can no longer execute`;
        resolvePendingMail(
          entry.agentAddress,
          entry.messageId,
          entry.allocatedTarget.generation,
        );
      } else {
        // A transient database or socket failure must leave replay retryable,
        // including during the registration handler's reconnect replay.
        if (stillOwned() && addressIndex.has(entry.agentAddress)) {
          rearm(entry.attempts + 1);
        }
        logger.warn`Mail replay failed for ${entry.agentAddress}: ${error instanceof Error ? error.message : String(error)}`;
      }
      return false;
    }
  }

  function deletePendingMail(
    byId: Map<string, PendingMailEntry>,
    agentAddress: string,
    messageId: string,
  ): void {
    byId.delete(messageId);
    if (byId.size === 0) {
      pendingMail.delete(agentAddress);
      // The retention TTL guards a non-empty pending set; drop it once the set
      // is empty so it never outlives the entries it was bounding.
      const retention = pendingMailRetention.get(agentAddress);
      if (retention !== undefined) {
        clearTimeout(retention);
        pendingMailRetention.delete(agentAddress);
      }
    }
  }

  async function retryPendingMail(
    agentAddress: string,
    messageId: string,
    retryLoop: symbol,
  ): Promise<void> {
    const byId = pendingMail.get(agentAddress);
    if (byId === undefined) return;
    const entry = byId.get(messageId);
    if (entry?.retryLoop !== retryLoop) return;

    if (entry.attempts >= mailAckMaxRetries) {
      // The sidecar never acked within the retry budget. The ack is withheld
      // precisely because the sidecar's durable inbox write failed, so the
      // mail was NOT delivered. Drop the pending entry so its timer does not
      // leak.
      deletePendingMail(byId, agentAddress, messageId);
      abandonPendingMail(
        entry,
        `no acknowledgement after ${String(entry.attempts)} redelivery attempt(s)`,
      );
      return;
    }

    // Redeliver over the address's CURRENT owner: a verified reconnect may have
    // moved the address to a new connection since the original delivery.
    const ws = addressIndex.get(agentAddress);
    const conn = ws !== undefined ? connections.get(ws) : undefined;
    if (conn === undefined) {
      deletePendingMail(byId, agentAddress, messageId);
      logger.warn`Dropping un-acked mail ${messageId} for ${agentAddress}: no live connection to redeliver over`;
      return;
    }
    const allocated = allocatedConnections.get(
      entry.allocatedTarget.allocationId,
    );
    const targetStillOwnsAddress =
      allocated !== undefined &&
      allocated.identity.generation === entry.allocatedTarget.generation &&
      allocated.ws === ws;
    if (
      !targetStillOwnsAddress &&
      !followRoutedGeneration(byId, entry, owningAllocation(conn, agentAddress))
    ) {
      return;
    }

    await replaySendPendingMail(conn, entry, retryLoop);
  }

  function resolvePendingMail(
    agentAddress: string,
    messageId: string,
    generation: number,
  ): void {
    const byId = pendingMail.get(agentAddress);
    if (byId === undefined) return;
    const entry = byId.get(messageId);
    if (entry?.allocatedTarget.generation !== generation) return;
    entry.cancelRetry();
    deletePendingMail(byId, agentAddress, messageId);
  }

  // Hold an address's un-acked pending mail across a disconnect. The per-entry
  // retry timers are cleared -- retrying over the dead socket is pointless --
  // but the entries are KEPT so a verified reconnect can redeliver them. A
  // retention TTL bounds the hold so a sidecar that never reconnects does not
  // leak; on expiry the still-un-acked entries are given up, since a withheld
  // ack means the sidecar's durable write never landed.
  function retainPendingMailForAddress(agentAddress: string): void {
    const byId = pendingMail.get(agentAddress);
    if (byId === undefined) return;
    for (const entry of byId.values()) entry.cancelRetry();
    const existing = pendingMailRetention.get(agentAddress);
    if (existing !== undefined) clearTimeout(existing);
    const timer = setTimeout(() => {
      pendingMailRetention.delete(agentAddress);
      const expired = pendingMail.get(agentAddress);
      pendingMail.delete(agentAddress);
      for (const entry of expired?.values() ?? []) {
        abandonPendingMail(
          entry,
          "no reconnect redelivered it within the retention time",
        );
      }
    }, mailHoldTTLMs);
    pendingMailRetention.set(agentAddress, timer);
  }

  // Redeliver an address's retained un-acked pending mail once a `hello`
  // routes the address again.
  // Replays identical bytes (same messageId) over that connection, so the
  // sidecar's inbox dedups a message it already wrote (effectively-once) and
  // processes one it had dropped (no loss). Re-arms the connected-window retry
  // over that connection with a fresh per-generation budget, so a redelivery
  // that is itself dropped before its ack is retried.
  async function redeliverPendingMail(
    agentAddress: string,
    conn: SidecarConnection,
  ): Promise<void> {
    // A handshake that took the sidecar over meanwhile redelivers what it
    // routes, and mail it does not route keeps its retention.
    const routedOn = addressIndex.get(agentAddress);
    if (routedOn === undefined || connections.get(routedOn) !== conn) return;
    const retention = pendingMailRetention.get(agentAddress);
    if (retention !== undefined) {
      clearTimeout(retention);
      pendingMailRetention.delete(agentAddress);
    }
    const byId = pendingMail.get(agentAddress);
    if (byId === undefined) return;
    let redelivered = 0;
    for (const entry of [...byId.values()]) {
      const routed = owningAllocation(conn, agentAddress);
      if (routed === undefined) {
        // The route went while an earlier entry awaited its replay. The rest
        // waits for the generation routed next, as a detach leaves it, unless
        // another connection already routes the address and delivers it there.
        if (!addressIndex.has(agentAddress)) {
          retainPendingMailForAddress(agentAddress);
        }
        return;
      }
      if (
        (routed.allocationId !== entry.allocatedTarget.allocationId ||
          routed.generation !== entry.allocatedTarget.generation) &&
        !followRoutedGeneration(byId, entry, routed)
      ) {
        continue;
      }
      if (!(await replaySendPendingMail(conn, entry, undefined))) continue;
      redelivered += 1;
    }
    if (redelivered > 0) {
      logger.info`Redelivered ${String(redelivered)} un-acked message(s) to ${agentAddress}`;
    }
  }

  function resetLivenessTimer(ws: WsHandle): void {
    const existing = livenessTimers.get(ws);
    if (existing !== undefined) existing();

    const cancel = scheduleTimeout(() => {
      livenessTimers.delete(ws);
      logger.warn`Sidecar ping timeout, closing connection`;
      ws.close();
    }, pingTimeoutMs);
    livenessTimers.set(ws, cancel);
  }

  function handlePing(ws: WsHandle): void {
    resetLivenessTimer(ws);
    // Always respond with pong, even before the hello is answered. The
    // sidecar's ping timer starts on open, which may fire before the async
    // registration handshake finishes.
    ws.send(JSON.stringify({ type: "pong" }));
  }

  function handleOpen(ws: WsHandle): void {
    // Connection is not usable until a hello frame arrives.
    // Start the liveness timer immediately — a sidecar that connects
    // but never sends a ping will be reaped.
    resetLivenessTimer(ws);
  }

  function handleMessage(ws: WsHandle, data: string): void {
    let raw: unknown;
    try {
      raw = JSON.parse(data) as unknown;
    } catch {
      logger.warn`Unparseable frame from sidecar connection`;
      return;
    }
    const validated = SidecarFrame(raw);
    if (validated instanceof type.errors) {
      logger.warn`Invalid sidecar frame: ${validated.summary}`;
      // An invalid hello cannot be answered with `welcome`, so the socket
      // closes rather than leave the sidecar waiting on it.
      if (
        typeof raw === "object" &&
        raw !== null &&
        "type" in raw &&
        raw.type === "hello"
      ) {
        handleClose(ws);
        ws.close();
      }
      const done = MalformedPackDone(raw);
      if (!(done instanceof type.errors)) {
        // Behind the frames the sidecar sent before it, such as the
        // transfer's own chunks, so the rejection cannot overtake them.
        enqueueOnConnection(ws, async () => {
          rejectMalformedPackDone(ws, done);
        }).catch((err: unknown) => {
          logger.warn`Rejecting a malformed repo.pack.done failed: ${err instanceof Error ? err.message : String(err)}`;
        });
      }
      return;
    }
    const frame = validated;

    // Bypass frames (liveness + terminal responses to outbound requests)
    // dispatch immediately: they resolve the very promises a queued handler
    // may be blocked on, so queuing them would deadlock the round-trip.
    // A queued stop acknowledgement may wait behind slow pack ingestion. The
    // worker has answered, so its silence timeout stops here.
    if (frame.type === "workflow.control.ack") {
      const entry = pendingWorkflowControls.get(frame.requestId);
      if (entry?.ws === ws) entry.meta.received();
    }
    if (frameBypassesQueue(frame)) {
      // Guard so a bypass handler's failure -- a synchronous throw or an async
      // ack handler's rejection -- is logged rather than floating out of the
      // immediate dispatch. The async wrapper turns a synchronous throw into a
      // rejection too, matching the queue path's .then/.catch coverage.
      void (async () => dispatchFrame(ws, frame))().catch((err: unknown) => {
        logger.warn`Frame handler failed for ${frame.type}: ${err instanceof Error ? err.message : String(err)}`;
      });
      return;
    }
    // Everything else serializes per ws so a frame that establishes or reads
    // routing observes earlier queued frames' completed effects.
    const prev = messageChains.get(ws) ?? Promise.resolve();
    const next = prev
      .then(() => dispatchFrame(ws, frame))
      .catch((err: unknown) => {
        logger.warn`Frame handler failed for ${frame.type}: ${err instanceof Error ? err.message : String(err)}`;
      });
    messageChains.set(ws, next);
  }

  // Run Hub-initiated connection work on the same per-ws chain as inbound
  // frames, so it observes a completed registration and a registration
  // observes its effects. The caller sees the task's own outcome; the chain
  // only needs it settled.
  function enqueueOnConnection<T>(
    ws: WsHandle,
    task: () => Promise<T>,
  ): Promise<T> {
    const prev = messageChains.get(ws) ?? Promise.resolve();
    const result = prev.then(task);
    messageChains.set(
      ws,
      result.then(
        () => undefined,
        () => undefined,
      ),
    );
    return result;
  }

  function assertNever(x: never): never {
    throw new Error(`Unclassified sidecar frame type: ${JSON.stringify(x)}`);
  }

  // Whether `frame` bypasses the per-ws serialization chain. Invariant: a frame
  // bypasses IFF it is liveness (ping) OR a terminal response to an
  // already-issued outbound request -- correlated purely by
  // requestId/transferId/agentAddress in the pending maps, touching no routing
  // state. Such a frame has no ordering obligation against new inbound frames
  // (a response cannot resolve "too early" for a request that already went
  // out), and it is exactly what in-flight queued handlers block on, so it MUST
  // run out of band. Every other frame establishes or reads routing, or carries
  // an inbound payload whose order matters, so it queues. A workflow stop
  // acknowledgement queues too: it ends the worker's history, so the packs the
  // worker sent before it must land before the stop resolves and later packs
  // are fenced. The exhaustive switch + assertNever makes adding a SidecarFrame
  // variant without classifying it a compile error, not a latent deadlock or a
  // silent bypass hole.
  function frameBypassesQueue(frame: SidecarFrame): boolean {
    switch (frame.type) {
      case "ping":
      case "session.ack":
      case "session.error":
      case "agent.deploy.ack":
      case "agent.deploy.error":
      case "agent.undeploy.ack":
      case "agent.undeploy.error":
      case "repo.pack.ack":
      case "repo.pack.reject":
      case "workflow.probe.result":
      case "workflow.probe.error":
        return true;
      case "hello":
      case "mail.outbound":
      case "agent.event":
      case "connector.state.changed":
      case "mail.inbound.ack":
      case "signal.correlation.register":
      case "workflow.control.ack":
      case "repo.pack.push":
      case "repo.pack.done":
        return false;
      default:
        return assertNever(frame);
    }
  }

  // Runs one frame's handler. Returns the handler's promise for async handlers
  // so the per-ws chain can await bounded completion; sync handlers return
  // void. Never awaits a promise that resolves on a later same-ws frame.
  function dispatchFrame(
    ws: WsHandle,
    frame: SidecarFrame,
  ): void | Promise<void> {
    switch (frame.type) {
      case "hello": {
        const { incarnations } = frame;
        const cachedSenderAddresses = frame.cachedSenderAddresses ?? [];
        // A hello is answered with `welcome` or with the socket closing, never
        // left waiting: a registration that fails before its welcome closes
        // the socket, and the sidecar registers again on a new one. One that
        // fails after it is only reported, since registering again would meet
        // the same failure and resend the sidecar's pending mail each time.
        return authenticateHandshake(ws, frame, (sidecarId) =>
          handleRegistration(
            ws,
            sidecarId,
            incarnations,
            cachedSenderAddresses,
          ),
        ).catch((err: unknown) => {
          const message = err instanceof Error ? err.message : String(err);
          if (welcomedSockets.has(ws)) {
            logger.error`Registration of sidecar ${frame.sidecarId} failed after its welcome: ${message}`;
            return;
          }
          logger.error`Registration of sidecar ${frame.sidecarId} failed; closing its connection: ${message}`;
          handleClose(ws);
          ws.close();
        });
      }
      case "agent.deploy.ack":
        return handleDeployAck(ws, frame);
      case "workflow.control.ack":
        return handleWorkflowControlAck(ws, frame);
      case "agent.deploy.error":
        rejectDeployPendingFromFrame(ws, frame);
        return;
      case "agent.undeploy.ack":
        settleUndeploy(ws, frame);
        resolveUndeployPending(ws, frame);
        return;
      case "agent.undeploy.error":
        settleUndeploy(ws, frame);
        rejectUndeployPending(ws, frame);
        return;
      case "ping":
        handlePing(ws);
        return;
      case "mail.outbound": {
        const conn = connections.get(ws);
        if (conn === undefined) return;
        if (
          !namesRoutedIncarnation(conn, {
            agentAddress: frame.senderAddress,
            generation: frame.generation,
          })
        ) {
          logger.warn`Dropping mail.outbound from ${frame.senderAddress} generation ${String(frame.generation)}: this sidecar does not route that incarnation`;
          return;
        }
        // The DoS backstop and the trust boundary for an untrusted sidecar's
        // mail body: measure the true byte cost (a hostile sidecar can send
        // multi-byte UTF-8, so `.length` would undercount) and drop an over-cap
        // frame here before either delivery path allocates on it. The socket's
        // maxPayloadLength has already closed the connection for a truly huge
        // frame; this catches one between the mail cap and that ceiling.
        const bodyBytes = Buffer.byteLength(frame.rawMessage, "utf8");
        if (bodyBytes > MAX_MAIL_OUTBOUND_BODY_BYTES) {
          logger.warn`Dropping mail.outbound from ${frame.senderAddress}: rawMessage of ${String(bodyBytes)} bytes exceeds the ${String(MAX_MAIL_OUTBOUND_BODY_BYTES)}-byte cap`;
          return;
        }
        if (frame.delivered !== true) {
          // frame.senderAddress is the sender this connection was just gated
          // on above -- a hub-verified value. Thread it so the relayed
          // inbound frame is stamped with it, not the MIME From.
          return handleMailOutbound(
            frame.rawMessage,
            frame.senderAddress,
            frame.recipients,
          );
        }
        if (lookups.persistMail) {
          return handleMailPersist(
            lookups.persistMail,
            frame.rawMessage,
            frame.senderAddress,
            frame.recipients,
          );
        }
        logger.warn`Dropping delivered mail.outbound frame: no persistMail lookup configured`;
        return;
      }
      case "agent.event": {
        const conn = connections.get(ws);
        if (conn === undefined) return;
        if (!namesRoutedIncarnation(conn, frame)) {
          logger.debug`Dropping agent.event for ${frame.agentAddress} generation ${String(frame.generation)}: this sidecar does not route that incarnation`;
          return;
        }
        events.emit("agent.event", {
          agentAddress: frame.agentAddress,
          sessionId: frame.sessionId,
          event: frame.event,
        });
        dispatchToSubscribers(frame.agentAddress, frame.event);
        return;
      }
      case "connector.state.changed":
        // Gate the cache write on the sending sidecar actually owning
        // the named agent. A misbehaving sidecar that knows another
        // agent's address could otherwise poison the cached state.
        if (addressIndex.get(frame.agentAddress) !== ws) {
          logger.warn`Dropping connector.state.changed for ${frame.agentAddress}: not registered to this sidecar`;
          return;
        }
        connectorStates.set(frame.agentAddress, frame.connectorState);
        events.emit("connector.state.changed", {
          agentAddress: frame.agentAddress,
          connectorState: frame.connectorState,
        });
        return;
      case "mail.inbound.ack": {
        // Terminal receipt for a connected-window `mail.inbound`: the sidecar
        // has durably written the message to the inbox of the incarnation the
        // ack names. Only the incarnation this connection routes may settle
        // it: an ack from a generation the Hub fenced is for an inbox that may
        // be gone, and crediting it would settle mail the routed generation
        // never received.
        const conn = connections.get(ws);
        if (conn === undefined) return;
        if (!namesRoutedIncarnation(conn, frame)) {
          logger.warn`Dropping mail.inbound.ack for ${frame.agentAddress} generation ${String(frame.generation)}: this sidecar does not route that incarnation`;
          return;
        }
        resolvePendingMail(
          frame.agentAddress,
          frame.messageId,
          frame.generation,
        );
        const acknowledging = owningAllocation(conn, frame.agentAddress);
        events.emit("mail.inbound.acknowledged", {
          agentAddress: frame.agentAddress,
          messageId: frame.messageId,
          ...(acknowledging !== undefined
            ? {
                allocated: {
                  allocationId: acknowledging.allocationId,
                  anchorRunId: acknowledging.anchorRunId,
                  generation: acknowledging.generation,
                },
              }
            : {}),
        });
        return;
      }
      case "signal.correlation.register":
        return handleSignalCorrelationRegister(ws, frame);
      case "session.ack":
        if (pendingRequests.get(frame.requestId)?.ws !== ws) return;
        pendingRequests.resolve(frame.requestId);
        return;
      case "session.error":
        if (pendingRequests.get(frame.requestId)?.ws !== ws) return;
        pendingRequests.reject(frame.requestId, frame.error);
        return;
      case "repo.pack.ack":
        resolvePackPending(ws, frame);
        return;
      case "repo.pack.reject":
        rejectPackPending(ws, frame);
        return;
      case "repo.pack.push":
        handlePackPush(ws, frame);
        return;
      case "repo.pack.done":
        return handlePackDone(ws, frame);
      case "workflow.probe.result":
        resolveProbe(ws, frame.requestId, {
          projection: frame.projection,
          grants: frame.grants,
          grantWalkSnapshot: frame.grantWalkSnapshot,
          wireHash: frame.wireHash,
        });
        return;
      case "workflow.probe.error":
        rejectProbe(ws, frame.requestId, frame.error);
        return;
      default:
        return assertNever(frame);
    }
  }

  // Authenticate a hello handshake exactly once, then run the
  // frame's handler with the verified sidecar id. The claimed `sidecarId` on
  // the frame is an unauthenticated hint: it is logged if it disagrees with
  // the verified id but never trusted -- routing keys off the verified id.
  // Fails closed by closing the connection when the authenticator rejects
  // (returns null) or throws (e.g. a database failure), so a handshake never
  // proceeds on unverified credentials.
  async function authenticateHandshake(
    ws: WsHandle,
    frame: { type: string; sidecarId: string; token: string },
    run: (sidecarId: string) => Promise<void>,
  ): Promise<void> {
    let credentials: SidecarCredentials | null;
    try {
      credentials = await authenticateSidecar({
        sidecarId: frame.sidecarId,
        token: frame.token,
      });
    } catch (err) {
      logger.error`Rejected ${frame.type} from claimed sidecar ${frame.sidecarId}: authenticator failed: ${err instanceof Error ? err.message : String(err)}`;
      ws.close();
      return;
    }
    if (credentials === null) {
      logger.warn`Rejected ${frame.type} from claimed sidecar ${frame.sidecarId}: invalid token`;
      ws.close();
      return;
    }
    const sidecarId = credentials.sidecarId;
    if (sidecarId !== frame.sidecarId) {
      logger.warn`Sidecar ${frame.type} claimed id ${frame.sidecarId} but token verifies as ${sidecarId}; keying off the verified id`;
    }
    await run(sidecarId);
  }

  async function notifyAllocationWaiters(allocationId: string): Promise<void> {
    const waiters = allocationWaiters.get(allocationId);
    const current = allocatedConnections.get(allocationId);
    if (waiters === undefined || current === undefined) return;
    const matchingWaiters = [...waiters].filter(
      (waiter) => waiter.generation === current.identity.generation,
    );
    if (matchingWaiters.length === 0) return;
    const validation = Promise.resolve().then(() =>
      validateSidecarIdentity(current.identity, "readiness"),
    );
    for (const waiter of matchingWaiters) {
      waiter.validations.add(validation);
      waiter.onValidation?.(validation);
    }
    let identityCurrent: boolean;
    try {
      identityCurrent = await validation;
    } catch (cause) {
      // A failed revalidation leaves the waiters parked: a later hello
      // revalidates, and at expiry the wait reports the failure rather than a
      // missed deadline. Registration itself was already gated, so this must
      // not fail the connection that just registered.
      const validationFailure = new SidecarIdentityValidationError(
        allocationId,
        current.identity.generation,
        cause,
      );
      for (const waiter of matchingWaiters) {
        waiter.validationFailure = validationFailure;
      }
      return;
    } finally {
      for (const waiter of matchingWaiters) {
        waiter.validations.delete(validation);
      }
    }
    // A clean validation supersedes earlier failures: expiry must report the
    // current reading, not a stale transient.
    for (const waiter of matchingWaiters) {
      delete waiter.validationFailure;
    }
    if (!identityCurrent || allocatedConnections.get(allocationId) !== current)
      return;

    for (const waiter of matchingWaiters) {
      if (!waiters.delete(waiter)) continue;
      clearTimeout(waiter.timer);
      waiter.resolve();
    }
    if (waiters.size === 0 && allocationWaiters.get(allocationId) === waiters)
      allocationWaiters.delete(allocationId);
  }

  function isFencedAsCurrent(binding: SidecarAuthIdentity): boolean {
    return allocationFences.get(binding.allocationId) === binding.generation;
  }

  // Make `bindings` the connection's current set. A binding the in-memory
  // fence no longer accepts is skipped; one this socket held that is absent
  // or superseded is detached; one still attached to another sidecar's socket
  // moves here. Returns the attached bindings and, of those, the ones this
  // socket did not already hold at the same generation and kind. A binding it
  // already holds that way keeps its entry: readiness and routing checks read
  // the entry, await its validation, and treat a replaced entry as a changed
  // connection, so a sync that changes nothing must not replace it. The socket
  // stays open even when nothing attaches -- the caller decides. A newer
  // generation attaches at once even while the sidecar is still tearing an
  // older one of the same deployment down: every frame names its generation,
  // so nothing the older one still sends is credited to the newer one.
  function attachBindings(
    ws: WsHandle,
    conn: SidecarConnection,
    bindings: readonly SidecarAuthIdentity[],
  ): { attached: SidecarAuthIdentity[]; added: SidecarAuthIdentity[] } {
    const current = bindings.filter(isFencedAsCurrent);
    for (const binding of bindings) {
      if (!current.includes(binding)) {
        logger.warn`Sidecar ${conn.sidecarId} skipped ${binding.kind} ${binding.allocationId} generation ${String(binding.generation)}: it is not fenced as current`;
      }
    }
    for (const [allocationId, held] of [...conn.bindings]) {
      const next = current.find(
        (binding) => binding.allocationId === allocationId,
      );
      if (next === undefined || next.generation !== held.generation) {
        detachBinding(ws, allocationId, "Its binding is no longer current", {
          keepOpen: true,
        });
      }
    }
    const added: SidecarAuthIdentity[] = [];
    for (const binding of current) {
      const elsewhere = allocatedConnections.get(binding.allocationId);
      if (elsewhere !== undefined && elsewhere.ws !== ws) {
        detachBinding(
          elsewhere.ws,
          binding.allocationId,
          `It moved to sidecar ${conn.sidecarId}`,
        );
      }
      const held = conn.bindings.get(binding.allocationId);
      const unchanged =
        held?.generation === binding.generation && held.kind === binding.kind;
      if (
        unchanged &&
        allocatedConnections.get(binding.allocationId)?.ws === ws
      )
        continue;
      if (!unchanged) added.push(binding);
      conn.bindings.set(binding.allocationId, binding);
      allocatedConnections.set(binding.allocationId, { ws, identity: binding });
    }
    return { attached: current, added };
  }

  // Remove one binding from its connection: drop its routes and in-flight
  // work, tell the sidecar to undeploy the deployment it no longer hosts, and
  // report the lost routes. The socket closes once it hosts nothing current,
  // unless the caller is about to attach replacements.
  function detachBinding(
    ws: WsHandle,
    allocationId: string,
    reason: string,
    { keepOpen = false }: { keepOpen?: boolean } = {},
  ): void {
    const conn = connections.get(ws);
    const binding = conn?.bindings.get(allocationId);
    if (conn === undefined || binding === undefined) return;
    conn.bindings.delete(allocationId);
    const allocated: { allocationId: string; generation: number }[] = [];
    if (allocatedConnections.get(allocationId)?.ws === ws) {
      allocatedConnections.delete(allocationId);
      if (binding.kind === "allocated") {
        allocated.push({ allocationId, generation: binding.generation });
      }
    }
    const addresses = [...conn.workflowAddresses].flatMap(([address, owner]) =>
      owner === allocationId ? [address] : [],
    );
    for (const address of addresses) {
      conn.workflowAddresses.delete(address);
      if (addressIndex.get(address) === ws) {
        addressIndex.delete(address);
        connectorStates.delete(address);
        retainPendingMailForAddress(address);
      }
      agentStatePackReceiver.cancelByAgent(address);
      workflowRunPackReceiver.cancelByAgent(address);
    }
    const deploymentAddress =
      binding.kind === "allocated" ? binding.workflowRunAddress : undefined;
    // An own-repository transfer needs no route, so the loop above can miss
    // it.
    if (deploymentAddress !== undefined)
      workflowRunPackReceiver.cancelByAgent(deploymentAddress);
    const error = `${binding.kind === "probe" ? "Probe" : "Allocation"} ${allocationId} left sidecar ${conn.sidecarId}: ${reason}`;
    const released = new Set(addresses);
    if (deploymentAddress !== undefined) released.add(deploymentAddress);
    pendingRequests.rejectForWs(ws, (entry) => released.has(entry.meta), error);
    pendingDeploys.rejectForWs(ws, (entry) => released.has(entry.key), error);
    pendingPacks.rejectForWs(
      ws,
      (entry) => released.has(entry.meta.agentAddress),
      error,
    );
    pendingUndeploys.rejectForWs(ws, (entry) => released.has(entry.key), error);
    pendingProbes.rejectForWs(
      ws,
      (entry) => entry.meta === allocationId,
      error,
    );
    pendingWorkflowControls.rejectForWs(
      ws,
      (entry) => entry.meta.allocationId === allocationId,
      error,
    );
    // The Hub cannot tell whether the sidecar still holds the deployment: a
    // deploy that timed out or failed has already dropped its route and
    // pending entry, yet the sidecar may have installed it. The undeploy
    // names this generation, so it never removes a newer one, and a sidecar
    // that does not hold it acknowledges it anyway.
    if (deploymentAddress !== undefined) {
      sendUndeploy(
        conn,
        { address: deploymentAddress, generation: binding.generation },
        reason,
        { waitable: true },
      );
    }
    if (addresses.length > 0 || allocated.length > 0) {
      events.emit("sidecar.disconnect", {
        ownedAddresses: addresses,
        allocated,
      });
    }
    if (!keepOpen && !hostsWork(conn)) {
      handleClose(ws);
      ws.close();
    }
  }

  // Only a later deploy of the address on this connection waits on the
  // answer, bounded by the request timeout. What the sidecar still sends for
  // the incarnation names its generation and is dropped, and an incarnation
  // still held after an undeploy lost with its connection is in the sidecar's
  // next `hello`, which undeploys it again. An undeploy no deploy can wait on,
  // such as one of an address the sidecar reports but no binding of it
  // deploys, is not kept, so a sidecar cannot make the Hub keep any number of
  // them.
  function sendUndeploy(
    conn: Pick<SidecarConnection, "sidecarId" | "send" | "undeploying">,
    incarnation: { address: string; generation: number },
    reason: string,
    { waitable }: { waitable: boolean },
  ): void {
    const requestId = nextRequestId();
    try {
      conn.send({
        type: "agent.undeploy",
        requestId,
        agentAddress: incarnation.address,
        generation: incarnation.generation,
        reason,
      });
    } catch (err) {
      logger.warn`Failed to ask sidecar ${conn.sidecarId} to undeploy ${incarnation.address} generation ${String(incarnation.generation)}: ${err instanceof Error ? err.message : String(err)}`;
      return;
    }
    if (waitable) noteUndeploying(conn, incarnation.address, requestId);
  }

  function noteUndeploying(
    conn: Pick<SidecarConnection, "undeploying">,
    agentAddress: string,
    requestId: string,
  ): void {
    const inFlight = conn.undeploying.get(agentAddress);
    if (inFlight !== undefined) {
      // The sidecar answers an address's undeploys in the order it got them,
      // so whoever waits on the earlier one waits for this later one.
      inFlight.requestId = requestId;
      return;
    }
    let settle = (): void => undefined;
    const answered = new Promise<void>((resolve) => {
      settle = resolve;
    });
    conn.undeploying.set(agentAddress, { requestId, answered, settle });
  }

  // Settled behind the frames the sidecar sent before its answer, so all the
  // removed copy still sent is handled, and dropped, before a deploy waiting
  // on the answer routes the address again.
  function settleUndeploy(
    ws: WsHandle,
    frame: { agentAddress: string; requestId: string },
  ): void {
    void enqueueOnConnection(ws, async () => {
      const conn = connections.get(ws);
      const inFlight = conn?.undeploying.get(frame.agentAddress);
      if (conn === undefined || inFlight?.requestId !== frame.requestId) {
        return;
      }
      conn.undeploying.delete(frame.agentAddress);
      inFlight.settle();
    });
  }

  // Resolves once no undeploy of `agentAddress` is unanswered on `conn`. Fails
  // when the answer takes longer than a request may, or the caller aborts.
  async function undeployAnswered(
    conn: SidecarConnection,
    agentAddress: string,
    signal?: AbortSignal,
  ): Promise<void> {
    const inFlight = conn.undeploying.get(agentAddress);
    if (inFlight === undefined) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let onAbort = (): void => undefined;
    try {
      await Promise.race([
        inFlight.answered,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            reject(
              new Error(
                `${agentAddress} is still being undeployed on sidecar ${conn.sidecarId} after ${String(requestTimeoutMs)}ms`,
              ),
            );
          }, requestTimeoutMs);
          onAbort = () => {
            reject(
              signal?.reason instanceof Error
                ? signal.reason
                : new Error("The deploy was aborted"),
            );
          };
          signal?.addEventListener("abort", onAbort, { once: true });
        }),
      ]);
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    }
  }

  function hostsWork(conn: SidecarConnection): boolean {
    return conn.bindings.size > 0;
  }

  async function announceAttached(
    ws: WsHandle,
    bindings: readonly SidecarAuthIdentity[],
  ): Promise<void> {
    for (const binding of bindings) {
      await notifyAllocationWaiters(binding.allocationId);
      // The socket can close or the binding move on while the waiters are
      // notified, and a connected event after its disconnect would wake the
      // allocation as if its sidecar were back.
      const attached = allocatedConnections.get(binding.allocationId);
      if (
        attached?.ws !== ws ||
        attached.identity.generation !== binding.generation
      )
        continue;
      if (binding.kind === "allocated") {
        events.emit("sidecar.allocated.connected", {
          allocationId: binding.allocationId,
          generation: binding.generation,
        });
      }
    }
  }

  // What a registering sidecar hosts: the bindings current for it, the
  // reported incarnations that take their route back, stopped copies kept
  // for inspection, live copies kept under retention, and reported copies
  // the Hub does not keep. Null once registration has been rejected and its
  // socket closed.
  async function readHostedWork(
    ws: WsHandle,
    sidecarId: string,
    existing: SidecarConnection | undefined,
    incarnations: readonly HostedIncarnation[],
  ): Promise<{
    bindings: SidecarAuthIdentity[];
    reclaimed: Map<string, Extract<SidecarAuthIdentity, { kind: "allocated" }>>;
    retained: Map<string, Extract<SidecarAuthIdentity, { kind: "allocated" }>>;
    unkept: HostedIncarnation[];
  } | null> {
    let bindings: SidecarAuthIdentity[];
    try {
      const resolved = await resolveSidecarBindings(sidecarId);
      const current = await Promise.all(
        resolved.map((binding) =>
          validateSidecarIdentity(binding, "registration"),
        ),
      );
      bindings = resolved.filter((_, index) => current[index]);
    } catch (err) {
      logger.error`Rejected registration from sidecar ${sidecarId}: cannot resolve what it hosts: ${err instanceof Error ? err.message : String(err)}`;
      handleClose(ws);
      ws.close();
      return null;
    }
    if (bindings.length === 0) {
      logger.warn`Rejected registration from sidecar ${sidecarId}: it hosts nothing current`;
      // Nothing it reports is the Hub's any more, yet it may still be running
      // some of it, and no later handshake of a sidecar hosting nothing gets
      // further than this.
      const rejected = {
        sidecarId,
        undeploying: new Map<string, UndeployInFlight>(),
        send(frame: HubFrame) {
          ws.send(JSON.stringify(frame));
        },
      };
      for (const incarnation of incarnations) {
        sendUndeploy(
          rejected,
          incarnation,
          "The sidecar hosts nothing current for the Hub",
          { waitable: false },
        );
      }
      handleClose(ws);
      ws.close();
      return null;
    }

    // A reported incarnation takes its route back only when it is live, it is
    // the generation of a binding current for this sidecar, that deployment's
    // first deploy has completed, and its run has not ended. A stopped one of
    // a current binding, and a live one whose run ended on its own, stay
    // unrouted, their local state kept for inspection until the Hub releases
    // the deployment. Every other one is undeployed: an earlier generation, a
    // live one of a run the Hub cancelled, a deployment that left this sidecar
    // while it was disconnected, one still deploying or tearing down, or one
    // whose deploy is still uncertain. The reconciler fails a deployment whose
    // current generation the sidecar no longer reports.
    const reclaimed = new Map<
      string,
      Extract<SidecarAuthIdentity, { kind: "allocated" }>
    >();
    const retained = new Map<
      string,
      Extract<SidecarAuthIdentity, { kind: "allocated" }>
    >();
    const unkept: HostedIncarnation[] = [];
    async function readIncarnation(incarnation: HostedIncarnation) {
      const address = incarnation.address;
      const binding = bindings.find(
        (candidate) =>
          candidate.kind === "allocated" &&
          candidate.workflowRunAddress === address &&
          candidate.generation === incarnation.generation,
      );
      if (
        binding?.kind !== "allocated" ||
        (incarnation.state !== "live" && incarnation.state !== "stopped")
      ) {
        return { incarnation, kind: "unkept" } as const;
      }
      if (incarnation.state === "stopped") {
        return { incarnation, binding, kind: "stopped" } as const;
      }
      const alreadyRouted =
        existing?.workflowAddresses.get(address) === binding.allocationId &&
        existing.bindings.get(binding.allocationId)?.generation ===
          binding.generation &&
        addressIndex.get(address) === ws;
      let reclaimable: boolean;
      let kept = false;
      try {
        reclaimable =
          alreadyRouted || (await validateSidecarIdentity(binding, "reclaim"));
        if (!reclaimable)
          kept = await validateSidecarIdentity(binding, "retention");
      } catch (err) {
        logger.error`Rejected registration from sidecar ${sidecarId}: cannot validate reported ${address} generation ${String(incarnation.generation)}: ${err instanceof Error ? err.message : String(err)}`;
        throw err;
      }
      if (reclaimable)
        return { incarnation, binding, kind: "reclaimed" } as const;
      if (kept) return { incarnation, binding, kind: "retained" } as const;
      return { incarnation, kind: "unkept" } as const;
    }
    // Each validation reads the database. Bound the fan-out while avoiding a
    // serial round trip for every deployment before the welcome deadline.
    // Apply results in inventory order; no routes are published until every
    // batch succeeds and handleRegistration rechecks the connection/fences.
    try {
      for (
        let offset = 0;
        offset < incarnations.length;
        offset += INCARNATION_VALIDATION_CONCURRENCY
      ) {
        const results = await Promise.all(
          incarnations
            .slice(offset, offset + INCARNATION_VALIDATION_CONCURRENCY)
            .map(readIncarnation),
        );
        for (const result of results) {
          const { incarnation } = result;
          switch (result.kind) {
            case "reclaimed":
              reclaimed.set(incarnation.address, result.binding);
              break;
            case "retained":
              retained.set(incarnation.address, result.binding);
              break;
            case "stopped":
              retained.set(incarnation.address, result.binding);
              break;
            case "unkept":
              unkept.push(incarnation);
              break;
          }
        }
      }
    } catch {
      handleClose(ws);
      ws.close();
      return null;
    }
    return { bindings, reclaimed, retained, unkept };
  }

  async function handleRegistration(
    ws: WsHandle,
    sidecarId: string,
    incarnations: readonly HostedIncarnation[],
    cachedSenderAddresses: string[],
  ): Promise<void> {
    const existing = connections.get(ws);
    if (existing !== undefined && existing.sidecarId !== sidecarId) {
      logger.warn`Rejected sidecar ${sidecarId}: socket is registered as sidecar ${existing.sidecarId}`;
      handleClose(ws);
      ws.close();
      return;
    }

    if (closedSockets.has(ws)) return;

    // Claim the sidecar before reading what it hosts. A sync for it queues
    // behind this registration from here on, and a sync that ran earlier
    // followed a placement this read sees, so work placed on the sidecar
    // while it connects is attached either way. The claim ends with the read:
    // the takeover below runs without yielding, or the registration stops.
    sidecarClaims.set(sidecarId, ws);
    let hosted: Awaited<ReturnType<typeof readHostedWork>>;
    let claimHeld: boolean;
    try {
      hosted = await readHostedWork(ws, sidecarId, existing, incarnations);
      // Every binding the read saw advanced or was released while it ran.
      // Work placed on the sidecar meanwhile is only in a newer read, so take
      // one rather than turn the sidecar away.
      if (
        hosted !== null &&
        !hosted.bindings.some(isFencedAsCurrent) &&
        sidecarClaims.get(sidecarId) === ws
      ) {
        hosted = await readHostedWork(ws, sidecarId, existing, incarnations);
      }
    } finally {
      claimHeld = sidecarClaims.get(sidecarId) === ws;
      if (claimHeld) sidecarClaims.delete(sidecarId);
    }
    if (hosted === null) return;
    if (!claimHeld) {
      logger.info`Dropping a registration of sidecar ${sidecarId}: its socket closed, or a newer handshake took the sidecar over`;
      // A socket already serving the sidecar keeps serving until that
      // handshake takes its bindings over.
      if (existing === undefined) {
        handleClose(ws);
        ws.close();
      }
      return;
    }
    const { bindings, reclaimed, retained, unkept } = hosted;

    // A sidecar reconnecting on a new socket takes its bindings along. Move
    // them off the previous socket first so its close reports only the
    // bindings that did not follow.
    const previous = sidecarSockets.get(sidecarId);
    if (previous !== undefined && previous !== ws) {
      for (const binding of bindings.filter(isFencedAsCurrent)) {
        if (allocatedConnections.get(binding.allocationId)?.ws === previous)
          allocatedConnections.delete(binding.allocationId);
      }
      handleClose(previous);
      previous.close();
    }

    const conn: SidecarConnection = existing ?? {
      sidecarId,
      bindings: new Map(),
      workflowAddresses: new Map(),
      unansweredDeploys: new Map(),
      undeploying: new Map(),
      send(frame: HubFrame) {
        ws.send(JSON.stringify(frame));
      },
    };
    connections.set(ws, conn);
    sidecarSockets.set(sidecarId, ws);

    const deployable = new Set(
      bindings.flatMap((binding) =>
        binding.kind === "allocated" ? [binding.workflowRunAddress] : [],
      ),
    );
    for (const incarnation of unkept) {
      logger.warn`Sidecar ${sidecarId} reported ${incarnation.address} generation ${String(incarnation.generation)} ${incarnation.state}, which the Hub does not keep there; asking it to undeploy`;
      sendUndeploy(
        conn,
        incarnation,
        "The Hub does not keep this incarnation on this sidecar",
        { waitable: deployable.has(incarnation.address) },
      );
    }
    const { attached } = attachBindings(ws, conn, bindings);
    if (!hostsWork(conn)) {
      logger.warn`Rejected sidecar ${sidecarId}: none of its bindings is fenced as current`;
      for (const [address, binding] of [...reclaimed, ...retained]) {
        sendUndeploy(
          conn,
          { address, generation: binding.generation },
          "The deployment is not current on this sidecar",
          { waitable: false },
        );
      }
      handleClose(ws);
      ws.close();
      return;
    }

    const newlyRoutedAddresses = new Set<string>();
    const routed: { address: string; generation: number }[] = [];
    for (const [address, binding] of reclaimed) {
      if (
        conn.bindings.get(binding.allocationId)?.generation !==
        binding.generation
      ) {
        sendUndeploy(
          conn,
          { address, generation: binding.generation },
          "The deployment is not current on this sidecar",
          { waitable: true },
        );
        continue;
      }
      if (addressIndex.get(address) !== ws) newlyRoutedAddresses.add(address);
      conn.workflowAddresses.set(address, binding.allocationId);
      addressIndex.set(address, ws);
      routed.push({ address, generation: binding.generation });
    }
    // A copy kept unrouted whose binding did not attach was released
    // while this hello was read, and no later release reaches it.
    for (const [address, binding] of retained) {
      if (
        conn.bindings.get(binding.allocationId)?.generation !==
        binding.generation
      ) {
        sendUndeploy(
          conn,
          { address, generation: binding.generation },
          "The deployment is not current on this sidecar",
          { waitable: true },
        );
      }
    }
    // Routing is settled, so the sidecar can stop holding back what must be
    // delivered and re-drive what it owes each routed incarnation.
    conn.send({ type: "welcome", routed });
    welcomedSockets.add(ws);
    logger.info`Provisioned sidecar ${sidecarId} registered for ${attached.map((binding) => `${binding.kind} ${binding.allocationId} generation ${String(binding.generation)}`).join(", ")}`;
    // Replay can wait on database admission without delaying connection readiness.
    await announceAttached(ws, attached);
    if (connections.get(ws) !== conn) return;
    for (const address of newlyRoutedAddresses) {
      await redeliverPendingMail(address, conn);
    }
    // Reconcile a reconnecting deployment's credentials, closing the offline
    // window: a credential revoked, deleted, or rotated while the sidecar was
    // disconnected is applied to the child now. Fire-and-forget so
    // registration is not blocked; the lookup no-ops for a run that persisted
    // no credential refs.
    const resyncCredentials = lookups.resyncCredentials;
    if (resyncCredentials !== undefined) {
      for (const address of newlyRoutedAddresses) {
        if (!isRunAddress(address)) continue;
        resyncCredentials(address);
      }
    }
    // Reconcile the sidecar's cached sender keys, closing the offline window: a
    // user-principal key that rotated while the sidecar was disconnected is
    // re-resolved and re-pushed, and a sender whose principal was DELETED while
    // the sidecar was disconnected is evicted, so the recipient stops verifying
    // either against a key the hub no longer vouches for. Only allocated
    // sidecars host a sender cache worth reconciling. Resolve and push
    // SEQUENTIALLY in one detached task: registration is never blocked, and a
    // large cache cannot fan out into one concurrent DB query per reported
    // sender on every reconnect.
    const resolveSenderKeyStrict = lookups.resolveSenderKeyStrict;
    if (
      allocationBindings(conn).length > 0 &&
      resolveSenderKeyStrict !== undefined
    ) {
      const rotatableSenders = new Set(cachedSenderAddresses);
      // Resolve-don't-trust applied to input SIZE: bound the reported set before
      // acting on it. Run addresses count toward the cap by design -- the
      // isRunAddress skip below is inside the loop, so the iteration, and thus
      // the DB resolves, can never exceed the cap regardless of the run/non-run
      // mix. Over the cap, reconcile the first MAX_RESYNC_SENDER_ADDRESSES and
      // log the overflow so a misbehaving sidecar is detectable.
      let sendersToResync = [...rotatableSenders];
      if (sendersToResync.length > MAX_RESYNC_SENDER_ADDRESSES) {
        logger.warn`Sidecar ${conn.sidecarId} reported ${String(sendersToResync.length)} cached sender addresses, over the ${String(MAX_RESYNC_SENDER_ADDRESSES)} resync cap; reconciling the first ${String(MAX_RESYNC_SENDER_ADDRESSES)} and ignoring the rest`;
        sendersToResync = sendersToResync.slice(0, MAX_RESYNC_SENDER_ADDRESSES);
      }
      void (async () => {
        for (const address of sendersToResync) {
          // The sidecar already reports only non-run senders, but do not trust
          // the report: a run sender's key is the immutable
          // workflow_run.public_key and is never refreshed or evicted, so skip
          // it here too rather than couple correctness to the sidecar's filter.
          if (isRunAddress(address)) continue;
          // Tri-state, deleted-vs-fault distinguished by the STRICT resolver:
          //   - resolves to a key -> refresh the sidecar's cached key;
          //   - CONFIRMED null (no matching principal = a deleted sender) ->
          //     evict it;
          //   - THROWS (fault: ambiguous address, keyless-principal invariant
          //     break, DB error) -> keep the stale key, evict nothing.
          // Never evicting on a fault is the load-bearing property: dropping a
          // live key on a transient DB fault would be worse than doing nothing.
          // Only the resolve is guarded here; conn.send stays outside so a
          // socket-gone throw propagates to the outer catch and stops the loop.
          let publicKey: string | null;
          try {
            publicKey = await resolveSenderKeyStrict(address);
          } catch (cause) {
            const message =
              cause instanceof Error ? cause.message : String(cause);
            logger.error`Keeping the stale cached key for ${address}: resolving it faulted (a fault, not a deleted sender): ${message}`;
            continue;
          }
          if (publicKey !== null) {
            conn.send({ type: "sender.key.refresh", address, publicKey });
          } else {
            conn.send({ type: "sender.key.evict", address });
          }
        }
      })().catch((cause) => {
        // The per-address resolve is guarded above, so the only throw reaching
        // here is conn.send (JSON.stringify + the socket write) once the sidecar
        // is gone. That means the connection left, so stop -- the remaining
        // sends would fail the same way.
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.warn`Sender-key resync for sidecar ${conn.sidecarId} stopped: ${message}`;
      });
    }
  }

  // The socket a sync for the sidecar runs on: the one registering it while
  // a handshake holds its claim, otherwise the one it is connected on.
  function syncTarget(sidecarId: string): WsHandle | undefined {
    return sidecarClaims.get(sidecarId) ?? sidecarSockets.get(sidecarId);
  }

  async function syncSidecar(
    sidecarId: string,
    signal?: AbortSignal,
  ): Promise<void> {
    signal?.throwIfAborted();
    const ws = syncTarget(sidecarId);
    if (ws === undefined) return;
    await enqueueOnConnection(ws, async () => {
      signal?.throwIfAborted();
      // The registration this queued behind may have been rejected, leaving
      // the sidecar on its previous socket, or superseded.
      if (syncTarget(sidecarId) !== ws) {
        redirectSync(sidecarId, signal);
        return;
      }
      const conn = connections.get(ws);
      if (conn === undefined) return;
      const bindings = await resolveSidecarBindings(sidecarId);
      signal?.throwIfAborted();
      if (syncTarget(sidecarId) !== ws) {
        redirectSync(sidecarId, signal);
        return;
      }
      if (connections.get(ws) !== conn) return;
      const { added } = attachBindings(ws, conn, bindings);
      if (!hostsWork(conn)) {
        logger.info`Sidecar ${sidecarId} hosts nothing current; closing its connection`;
        handleClose(ws);
        ws.close();
        return;
      }
      await announceAttached(ws, added);
    });
  }

  // Not awaited: the socket the sync moves to may have work queued behind the
  // one that redirects it.
  function redirectSync(sidecarId: string, signal?: AbortSignal): void {
    syncSidecar(sidecarId, signal).catch((err: unknown) => {
      if (signal?.aborted) return;
      logger.warn`Failed to sync the bindings of sidecar ${sidecarId}: ${err instanceof Error ? err.message : String(err)}`;
    });
  }

  // Park a pre-ack sender's mail synchronously and return its entry. Registering
  // the entry BEFORE the caller awaits `resolveSenderKey` is the interlock that
  // guarantees a settle landing during the resolve has an entry to find: the
  // event loop is single-threaded, so no settle can interleave between this
  // synchronous registration and the caller's first await.
  function parkDeferredSenderMail(
    authenticatedSender: string,
    rawMessage: string,
    recipients: string[],
  ): DeferredSenderMailEntry {
    let parked = deferredSenderMail.get(authenticatedSender);
    if (parked === undefined) {
      parked = new Set();
      deferredSenderMail.set(authenticatedSender, parked);
    }
    const entry: DeferredSenderMailEntry = {
      authenticatedSender,
      rawMessage,
      recipients,
      timer: setTimeout(() => {
        // TTL backstop for the case where a settle never arrives (the sender's
        // deploy never acked and never failed loudly). Claim the entry and
        // surface it as undelivered so the mail is not held forever.
        if (!claimDeferredSenderEntry(entry)) return;
        events.emit("mail.outbound.undelivered", {
          rawMessage: entry.rawMessage,
          recipients: entry.recipients,
        });
        logger.warn`Dropping mail from ${entry.authenticatedSender}: its sender key was not recorded before the deferred-mail TTL expired`;
      }, mailHoldTTLMs),
    };
    parked.add(entry);
    return entry;
  }

  // Remove one parked entry by identity, clearing its TTL timer. Returns whether
  // THIS call removed it. The inline-deliver path, a settle, and the TTL all
  // race to claim the same entry; only the claimer acts on it, so a claim that
  // finds nothing (already claimed) is a no-op. This is the idempotent
  // remove-by-key that keeps a settle and the inline non-null branch from both
  // delivering the same message.
  function claimDeferredSenderEntry(entry: DeferredSenderMailEntry): boolean {
    const parked = deferredSenderMail.get(entry.authenticatedSender);
    if (parked === undefined) return false;
    const claimed = parked.delete(entry);
    if (!claimed) return false;
    clearTimeout(entry.timer);
    if (parked.size === 0) deferredSenderMail.delete(entry.authenticatedSender);
    return true;
  }

  // Claim every entry parked for a sender, clearing their TTL timers. A later
  // settle or TTL for the same sender then finds nothing.
  function claimAllDeferredSenderMail(
    authenticatedSender: string,
  ): DeferredSenderMailEntry[] {
    const parked = deferredSenderMail.get(authenticatedSender);
    if (parked === undefined) return [];
    deferredSenderMail.delete(authenticatedSender);
    const entries = [...parked];
    for (const entry of entries) clearTimeout(entry.timer);
    return entries;
  }

  function drainDeferredSenderMail(
    authenticatedSender: string,
    reason: string,
  ): void {
    const entries = claimAllDeferredSenderMail(authenticatedSender);
    if (entries.length === 0) return;
    for (const entry of entries) {
      events.emit("mail.outbound.undelivered", {
        rawMessage: entry.rawMessage,
        recipients: entry.recipients,
      });
    }
    logger.warn`Dropping ${String(entries.length)} deferred message(s) from ${authenticatedSender}: ${reason}`;
  }

  function noteSenderDeployStarted(
    address: string,
    attempt: AllocatedSenderDeployAttempt,
  ): void {
    if (allocatedKeyRecordInFlight.has(address)) {
      throw new Error(`Sender deployment ${address} has an unresolved attempt`);
    }
    allocatedKeyRecordInFlight.set(address, attempt);
  }

  function noteSenderDeploySettled(
    sender: string | AllocatedSidecarTarget | AllocatedSenderDeployAttempt,
    outcome: SenderDeploySettledOutcome,
  ): void {
    if (typeof sender !== "string") {
      for (const [address, attempt] of [...allocatedKeyRecordInFlight]) {
        if (
          attempt.allocationId !== sender.allocationId ||
          attempt.generation !== sender.generation ||
          ("leaseId" in sender && attempt.leaseId !== sender.leaseId)
        ) {
          continue;
        }
        allocatedKeyRecordInFlight.delete(address);
        settleSenderMail(address, outcome);
      }
      return;
    }
    if (allocatedKeyRecordInFlight.has(sender)) return;
    settleSenderMail(sender, outcome);
  }

  function parkDeferredRecipientResolutionMail(
    recipient: string,
    rawMessage: string,
    authenticatedSender: string,
    recordedSenderKey: string | null,
  ): DeferredRecipientResolutionMailEntry {
    let parked = deferredRecipientResolutionMail.get(recipient);
    if (parked === undefined) {
      parked = new Set();
      deferredRecipientResolutionMail.set(recipient, parked);
    }
    // The hold owns this recipient alone. Its settle, TTL, and drain report
    // `recipients`, and a recorded settle re-drives only that list.
    const entry: DeferredRecipientResolutionMailEntry = {
      recipient,
      authenticatedSender,
      rawMessage,
      recipients: [recipient],
      recordedSenderKey,
      inflight: true,
      wake: null,
      cancelTimer: null,
    };
    parked.add(entry);
    return entry;
  }

  function claimDeferredRecipientResolutionEntry(
    entry: DeferredRecipientResolutionMailEntry,
  ): boolean {
    const parked = deferredRecipientResolutionMail.get(entry.recipient);
    if (parked === undefined) return false;
    const claimed = parked.delete(entry);
    if (!claimed) return false;
    if (entry.cancelTimer !== null) {
      entry.cancelTimer();
      entry.cancelTimer = null;
    }
    if (parked.size === 0)
      deferredRecipientResolutionMail.delete(entry.recipient);
    return true;
  }

  function redriveRecipientResolutionMail(
    entry: DeferredRecipientResolutionMailEntry,
  ): void {
    // Re-drive with the sender key bound when this mail was accepted. The
    // recipient's recorded key authenticates the deployment, not the
    // sender, so it is not the fourth argument.
    void Promise.resolve()
      .then(() =>
        handleMailOutbound(
          entry.rawMessage,
          entry.authenticatedSender,
          entry.recipients,
          entry.recordedSenderKey,
        ),
      )
      .catch((err: unknown) => {
        logger.error`Re-driving deferred mail to ${entry.recipient} failed: ${err instanceof Error ? err.message : String(err)}`;
      });
  }

  function undeliverRecipientResolutionEntry(
    entry: DeferredRecipientResolutionMailEntry,
    reason: string,
  ): void {
    events.emit("mail.outbound.undelivered", {
      rawMessage: entry.rawMessage,
      recipients: entry.recipients,
    });
    logger.warn`Dropping mail to ${entry.recipient}: ${reason}`;
  }

  // The in-flight attempt is about to route. Drop the hold, including a
  // settle that arrived during the await, so that settle does not route too.
  function takeInFlightRecipientDelivery(
    entry: DeferredRecipientResolutionMailEntry,
  ): boolean {
    entry.inflight = false;
    entry.wake = null;
    return claimDeferredRecipientResolutionEntry(entry);
  }

  // The TTL starts once the attempt has finished parked. Arming it at
  // park time can claim the hold while materialize is still in flight,
  // which drops a recorded wake and supersedes a return that is ready
  // to route. A drain during that await detaches the entry; arming then
  // would leak a timer whose callback no-ops.
  function armRecipientResolutionTtl(
    entry: DeferredRecipientResolutionMailEntry,
  ): void {
    if (entry.cancelTimer !== null) return;
    const parked = deferredRecipientResolutionMail.get(entry.recipient);
    if (parked === undefined || !parked.has(entry)) return;
    entry.cancelTimer = scheduleTimeout(() => {
      if (!claimDeferredRecipientResolutionEntry(entry)) return;
      events.emit("mail.outbound.undelivered", {
        rawMessage: entry.rawMessage,
        recipients: entry.recipients,
      });
      logger.warn`Dropping mail to ${entry.recipient}: credential resolution was not recorded before the deferred-mail TTL expired`;
    }, disconnectQueueTTLMs);
  }

  // The in-flight attempt did not route. A settle that arrived during the
  // await owns the next step; otherwise the entry stays parked for a later
  // settle or the TTL.
  function finishParkedRecipientResolution(
    entry: DeferredRecipientResolutionMailEntry,
  ): void {
    entry.inflight = false;
    const wake = entry.wake;
    if (wake === null) {
      armRecipientResolutionTtl(entry);
      return;
    }
    if (!claimDeferredRecipientResolutionEntry(entry)) return;
    if ("failed" in wake) {
      undeliverRecipientResolutionEntry(
        entry,
        `recipient deploy failed: ${wake.failed}`,
      );
      return;
    }
    redriveRecipientResolutionMail(entry);
  }

  // A thrown attempt fails closed. Claim the hold so the TTL does not later
  // report the original recipient list as undelivered.
  function abandonInFlightRecipientResolution(
    entry: DeferredRecipientResolutionMailEntry,
  ): void {
    entry.inflight = false;
    entry.wake = null;
    claimDeferredRecipientResolutionEntry(entry);
  }

  function settleRecipientResolutionMail(
    address: string,
    outcome: SenderDeploySettledOutcome,
  ): void {
    // A failed attempt has not recorded resolution and does not prove a
    // later attempt will not. Claiming the hold drops mail the next
    // recorded settle can still deliver. The TTL undelivers if none does.
    if ("failed" in outcome) return;
    const parked = deferredRecipientResolutionMail.get(address);
    if (parked === undefined) return;
    const idle: DeferredRecipientResolutionMailEntry[] = [];
    for (const entry of parked) {
      if (entry.inflight) {
        entry.wake = outcome;
      } else {
        idle.push(entry);
      }
    }
    for (const entry of idle) {
      if (!claimDeferredRecipientResolutionEntry(entry)) continue;
      redriveRecipientResolutionMail(entry);
    }
  }

  function settleSenderMail(
    address: string,
    outcome: SenderDeploySettledOutcome,
  ): void {
    settleRecipientResolutionMail(address, outcome);
    if ("failed" in outcome) {
      drainDeferredSenderMail(
        address,
        `sender deploy failed: ${outcome.failed}`,
      );
      return;
    }
    for (const entry of claimAllDeferredSenderMail(address)) {
      // Re-drive delivery as its OWN task, off the settle's stack, so delivery
      // work never runs on the deploy-ack handler's stack. Carry the confirmed
      // key: another attempt may start before this task runs, and must not
      // capture this mail or change the key that authenticates it.
      void Promise.resolve()
        .then(() =>
          handleMailOutbound(
            entry.rawMessage,
            entry.authenticatedSender,
            entry.recipients,
            outcome.recorded,
          ),
        )
        .catch((err: unknown) => {
          logger.error`Re-driving deferred mail from ${entry.authenticatedSender} failed: ${err instanceof Error ? err.message : String(err)}`;
        });
    }
  }

  function senderIdentitiesFromKey(
    address: string,
    publicKey: string | null,
  ): RunGrantsFrame["senderIdentities"] {
    return publicKey !== null ? [{ address, publicKey }] : undefined;
  }

  // Resolve the co-delivered sender identities for a message, applying the
  // register-before-read interlock for a pre-ack run sender. Returns either
  // `deliver: true` with the resolved identities (undefined when there is no
  // resolvable key), or `deliver: false` when the message is parked and will be
  // driven later by a settle (`noteSenderDeploySettled`) or the TTL.
  async function resolveSenderIdentitiesOrPark(
    rawMessage: string,
    authenticatedSender: string,
    recipients: string[],
  ): Promise<
    | { deliver: true; senderIdentities: RunGrantsFrame["senderIdentities"] }
    | { deliver: false }
  > {
    const resolveSenderKey = lookups.resolveSenderKey;
    if (resolveSenderKey === undefined)
      return { deliver: true, senderIdentities: undefined };

    // The co-delivered key is consumed only by a run recipient caching it from the
    // run.grants frame. Purely external/federated mail never uses it and is never
    // locally verified, so resolve nothing and never park it.
    if (!recipients.some(isRunAddress))
      return { deliver: true, senderIdentities: undefined };

    // A stable-key (non-run) sender has no pre-ack window; resolve inline.
    if (!isRunAddress(authenticatedSender)) {
      const key = await resolveSenderKey(authenticatedSender);
      return {
        deliver: true,
        senderIdentities: senderIdentitiesFromKey(authenticatedSender, key),
      };
    }

    // Park a run sender ONLY while a key-record settle is guaranteed to arrive -- a
    // deploy is in flight. Without one, a null resolve is a transient fault or a
    // genuine absence on an already-settled run: no settle is coming, so parking
    // would strand the mail to the TTL. Deliver on the normal path instead.
    const settleGuaranteed =
      pendingDeploys.has(authenticatedSender) ||
      allocatedKeyRecordInFlight.has(authenticatedSender);
    if (!settleGuaranteed) {
      const key = await resolveSenderKey(authenticatedSender);
      return {
        deliver: true,
        senderIdentities: senderIdentitiesFromKey(authenticatedSender, key),
      };
    }

    // Register-before-read: park a waiter entry synchronously (NO await) so a
    // settle that lands while we resolve below has an entry to find, THEN
    // resolve. The single-threaded event loop cannot interleave a settle between
    // this registration and the await.
    const entry = parkDeferredSenderMail(
      authenticatedSender,
      rawMessage,
      recipients,
    );
    const key = await resolveSenderKey(authenticatedSender);
    if (key === null) {
      // Not recorded yet. Leave the entry parked; a settle or the TTL drives it.
      return { deliver: false };
    }
    // The key was already recorded before we parked. Claim our entry and deliver
    // inline -- unless a concurrent settle already claimed it and is re-driving
    // this message, in which case claiming fails and we must NOT deliver again.
    if (!claimDeferredSenderEntry(entry)) {
      return { deliver: false };
    }
    return {
      deliver: true,
      senderIdentities: senderIdentitiesFromKey(authenticatedSender, key),
    };
  }

  async function handleMailOutbound(
    rawMessage: string,
    authenticatedSender: string,
    recipients: string[],
    // undefined resolves now. null is a finished resolve that found no
    // key, and a string is the key already bound for this message. null
    // and undefined are different: treating null as missing resolves again
    // and can co-deliver a key this message was not accepted under.
    recordedSenderKey?: string | null,
  ): Promise<void> {
    // A mail addressed to more than one workflow deployment would birth a
    // run per recipient from a single inbound mail. The stable runId
    // removed the Message-ID collision that originally forced this guard --
    // each recipient now derives its own per-deployment runId (its mail
    // address), so it is no longer a runId-collision guard. It stays a
    // deliberate one-workflow-recipient-per-mail restriction because the
    // fan-out is not verified end-to-end: per-recipient grants
    // materialization, consumed-tracking, and reply-addressing all assume a
    // single workflow recipient today. Lifting it means proving those three
    // hold per recipient, not just relaxing this check -- so fail loudly
    // rather than materialize a partial set. The guard only applies when a
    // materializer is wired -- absent one, no run is born from the mail, so
    // there is nothing to restrict.
    if (lookups.materializeMailTriggeredRunGrants !== undefined) {
      // A workflow recipient is one this hub owns: its address parses as a run
      // address. An external/federated address does not, and is not ours to
      // materialize a run for.
      const workflowRecipients = recipients.filter(isRunAddress);
      if (workflowRecipients.length > 1) {
        throw new Error(
          `mail addressed to multiple workflow-derived recipients (${workflowRecipients.join(", ")}); materializing a run for more than one workflow deployment from a single mail is unsupported`,
        );
      }
    }

    // Resolve the sender's hub-held key ONCE for the whole message, ahead of the
    // recipient fan-out and any grant materialization, so every recipient in
    // this fan-out binds the same key snapshot. A run-address sender may be
    // pre-ack -- it minted its keypair locally and can send before the hub
    // records its public key. The register-before-read interlock holds such mail
    // until the key lands rather than delivering it keyless, which a strict
    // recipient drops as an unknown sender. A parked message returns here and is
    // re-driven later by a settle or the TTL.
    const resolution =
      recordedSenderKey === undefined
        ? await resolveSenderIdentitiesOrPark(
            rawMessage,
            authenticatedSender,
            recipients,
          )
        : {
            deliver: true as const,
            senderIdentities: senderIdentitiesFromKey(
              authenticatedSender,
              recordedSenderKey,
            ),
          };
    if (!resolution.deliver) return;
    const senderIdentities = resolution.senderIdentities;
    const boundIdentity = senderIdentities?.[0];
    const boundSenderKey =
      boundIdentity === undefined ? null : boundIdentity.publicKey;

    // Route each recipient over the connection that routes its address.
    const unrouted: string[] = [];
    for (const recipient of recipients) {
      // Each recipient is isolated: a materialization failure or a
      // fail-closed rejection for one must not drop the mail for its
      // co-recipients. The catch fails THIS recipient closed (its run never
      // starts under-authorized) and continues to the rest.
      // Register the hold before awaiting materialization. A settle that
      // lands during that await finds the entry and records a wake; it does
      // not re-drive until this attempt finishes, so a ready attempt routes
      // once and a not-ready attempt hands the mail to that wake.
      let parked: DeferredRecipientResolutionMailEntry | undefined;
      try {
        parked =
          lookups.materializeMailTriggeredRunGrants !== undefined &&
          isRunAddress(recipient)
            ? parkDeferredRecipientResolutionMail(
                recipient,
                rawMessage,
                authenticatedSender,
                boundSenderKey,
              )
            : undefined;
        const outcome = await deliverMailToRecipient(
          recipient,
          rawMessage,
          authenticatedSender,
          senderIdentities,
          parked,
        );
        if (
          parked !== undefined &&
          (outcome === "parked" || outcome === "superseded")
        ) {
          // This hold is only this recipient. Its settle, TTL, and drain
          // report that recipient, so the rest of this attempt still runs.
          if (outcome === "parked") finishParkedRecipientResolution(parked);
          continue;
        }
        if (outcome === "unrouted") unrouted.push(recipient);
      } catch (err) {
        if (parked !== undefined) abandonInFlightRecipientResolution(parked);
        logger.error`Failed to deliver mail to ${recipient}: ${err instanceof Error ? err.message : String(err)}`;
      }
    }

    // Anything not routed locally is emitted as a notification. The
    // host decides whether to relay onto an external transport, log,
    // or drop. The wire layer takes no stance.
    if (unrouted.length > 0) {
      events.emit("mail.outbound.undelivered", {
        rawMessage,
        recipients: unrouted,
      });
    }
  }

  // Deliver an inbound mail to one recipient, materializing a
  // mail-triggered run's grants first when the recipient is a workflow
  // deployment. Returns:
  //   - `routed`: the mail reached the connection that routes the recipient,
  //     or pending redelivery after a transient admission failure.
  //   - `unrouted`: the mail was locally undeliverable and should be
  //     relayed externally by the host.
  //   - `failed-closed`: the run's grants could not be materialized safely,
  //     so the mail is deliberately DROPPED for this recipient (not relayed)
  //     to keep its run from starting under-authorized.
  //   - `parked`: credential resolution is not recorded yet. The caller holds
  //     the mail and retries after initialization; it is not relayed.
  //   - `superseded`: something else claimed the hold during this attempt.
  //     The caller must not also route.
  //
  // A workflow deployment is the only recipient whose inbound mail can first
  // fire its stable run. Its grants are reserved, and the `run.grants` frame is
  // sent BEFORE the mail. Same-address FIFO guarantees it lands ahead of the
  // mail that dispatches the run, so the run's `onRunStart` barrier resolves its
  // grants rather than failing closed. Reservation happens before routing so
  // concurrent first deliveries cannot send different snapshots; a routing
  // failure leaves a grants-only, still-unfired run.
  async function deliverMailToRecipient(
    recipient: string,
    rawMessage: string,
    authenticatedSender: string,
    senderIdentities: RunGrantsFrame["senderIdentities"],
    hold?: DeferredRecipientResolutionMailEntry,
  ): Promise<
    "routed" | "unrouted" | "failed-closed" | "parked" | "superseded"
  > {
    if (
      lookups.materializeMailTriggeredRunGrants !== undefined &&
      isRunAddress(recipient)
    ) {
      const runId = deriveWorkflowRunId(recipient);
      // This does NOT let mail mutate a run's authorization. First delivery
      // reserves and commits the run's grants (the mail IS the trigger);
      // every later delivery only RE-READS the current committed grants
      // (`loadCommittedRunGrants`) and re-asserts them ahead of the dispatch.
      // The committed rows already carry any standing-approval change (an
      // approve/reject-with-`always` resolution mutates them through its own
      // path), so this re-send is idempotent -- it re-establishes the run's
      // current floor on the sidecar, self-healing a `grants.json` a sidecar
      // may have lost, and never overwrites it with anything staler.
      const result = await lookups.materializeMailTriggeredRunGrants({
        agentAddress: recipient,
        runId,
      });
      if (result.outcome === "notReady") return "parked";
      // Claim before any send. A settle that arrived during the await must
      // not route beside this attempt.
      if (hold !== undefined && !takeInFlightRecipientDelivery(hold)) {
        return "superseded";
      }
      if (result.outcome === "rejected") {
        // The run's grants could not be materialized with sufficient
        // authority or it is already terminal. Fail the mail closed for this
        // recipient: routing or external relay would bypass that decision.
        logger.error`Refusing mail-triggered run ${runId} for ${recipient}: grant materialization rejected (${result.code}): ${result.message}`;
        return "failed-closed";
      }
      if (result.outcome === "materialized") {
        // The sender's hub-held key was resolved ONCE in handleMailOutbound,
        // ahead of this fan-out, and threaded in as `senderIdentities`. Co-
        // deliver it on the run's grants barrier so a recipient that caches from
        // the `run.grants` frame binds the sender address to the key and can
        // verify the sender's mail locally. A null key is never carried (the
        // list is undefined then), so the "authorized-with-a-key implies key
        // cached" invariant holds; a recipient with no cached key resolves such
        // mail as `unknown`, which its admission policy rejects by default (a
        // workflow may relax `unknown` to admit).
        // Finish asynchronous preparation before sending the grants and mail
        // together, keeping another delivery's key out of the gap between them.
        const messageId = await deriveMessageId(base64Decode(rawMessage));
        // Route through the messageId handshake `routeMail` -- NOT a
        // fire-and-forget send. This branch COMMITS a run, so a mail dropped in
        // the connected window (a socket that half-dies before the sidecar's
        // durable-write ack) would otherwise leave the run row "running"
        // forever with no body and no error. `routeMail` tracks the delivery
        // and redelivers identical bytes on reconnect, bringing the mail-relay
        // run-trigger to parity with the HTTP-trigger path. The messageId is the
        // mail's own id (derived over the same bytes the sidecar derives), so a
        // redelivery replays identically and the downstream RunStarted /
        // stable-runId dedup makes it effectively-once.
        const outcome: "routed" | "unrouted" = (await routeMail(
          recipient,
          rawMessage,
          authenticatedSender,
          messageId,
          {
            runId,
            stepGrants: result.stepGrants,
            ...(senderIdentities !== undefined ? { senderIdentities } : {}),
          },
        ))
          ? "routed"
          : "unrouted";
        return outcome;
      }
      // `skip`: the address named no deployed workflow deployment. Forward
      // the mail without grants -- the run, if any, is not ours to
      // authorize. No run is committed here, so no ack handshake is needed.
    }

    return (await routeMail(recipient, rawMessage, authenticatedSender))
      ? "routed"
      : "unrouted";
  }

  async function handleMailPersist(
    persist: NonNullable<SidecarLookups["persistMail"]>,
    rawMessage: string,
    senderAddress: string,
    recipients: string[],
  ): Promise<void> {
    let results: SidecarMailPersistedRow[];
    let raw: Uint8Array;
    try {
      raw = Uint8Array.from(atob(rawMessage), (c) => c.charCodeAt(0));
      results = await persist({
        senderAddress,
        recipients,
        raw,
      });
    } catch (err) {
      logger.error`Failed to persist mail from ${senderAddress}: ${err instanceof Error ? err.message : String(err)}`;
      return;
    }

    for (const result of results) {
      events.emit("mail.persisted", {
        id: result.id,
        raw,
        createdAt: result.createdAt,
        direction: result.direction,
        runId: result.runId,
        address: result.address,
      });
    }
  }

  async function handleSignalCorrelationRegister(
    ws: WsHandle,
    frame: SignalCorrelationRegisterFrame,
  ): Promise<void> {
    // Only the incarnation this connection routes may register a correlation
    // for its address; otherwise a misbehaving sidecar that knows another
    // deployment's address could register a spurious one against it.
    const conn = connections.get(ws);
    if (conn === undefined) return;
    if (!namesRoutedIncarnation(conn, frame)) {
      logger.warn`Dropping signal.correlation.register for ${frame.agentAddress} generation ${String(frame.generation)}: this sidecar does not route that incarnation`;
      return;
    }

    const register = lookups.registerSignalCorrelation;
    if (register === undefined) {
      logger.warn`Dropping signal.correlation.register for ${frame.agentAddress}: no registerSignalCorrelation lookup configured`;
      return;
    }

    try {
      await register({
        correlationId: frame.correlationId,
        runId: frame.runId,
        anchorRunId: frame.anchorRunId,
        agentAddress: frame.agentAddress,
        kind: frame.kind,
        approvalSnapshot: frame.snapshot,
      });
      // The co-write resolves only when a row exists -- freshly inserted or
      // already present (both stores are idempotent on the correlationId). Ack
      // so the sidecar's link stops retrying a register whose frame may have
      // been lost on an open socket. A thrown co-write (undeployed deployment,
      // id mismatch) means no row, so no ack: the sidecar keeps retrying and
      // the reconnect re-emit remains the ultimate backstop.
      conn.send({
        type: "signal.correlation.register.ack",
        agentAddress: frame.agentAddress,
        correlationId: frame.correlationId,
      });
    } catch (err) {
      logger.error`Failed to register signal correlation ${frame.correlationId} for ${frame.agentAddress}: ${err instanceof Error ? err.message : String(err)}`;
    }
  }

  function handleClose(ws: WsHandle): void {
    closedSockets.add(ws);
    for (const [sidecarId, claimant] of sidecarClaims) {
      if (claimant === ws) sidecarClaims.delete(sidecarId);
    }

    // Cancel the liveness timer for this connection.
    const cancelLiveness = livenessTimers.get(ws);
    if (cancelLiveness !== undefined) {
      cancelLiveness();
      livenessTimers.delete(ws);
    }

    // Drop the per-ws serialization chain; no more frames will queue on it.
    messageChains.delete(ws);

    const conn = connections.get(ws);
    if (conn === undefined) return;
    const allocated: { allocationId: string; generation: number }[] = [];
    // A deploy waiting on an answer this socket will not carry goes on and
    // finds the connection gone.
    for (const inFlight of conn.undeploying.values()) inFlight.settle();
    conn.undeploying.clear();

    // Remove this connection's workflow-substrate routes. The sidecar reports
    // what it still holds in its next `hello`, and its in-flight run state is
    // reconstructed sidecar-locally, not from a hub-side queue. Only routes
    // this connection still owns are removed, so a takeover by a newer ws is
    // not clobbered by the prior owner's close.
    for (const addr of conn.workflowAddresses.keys()) {
      if (addressIndex.get(addr) === ws) {
        addressIndex.delete(addr);
        connectorStates.delete(addr);
        // Retain un-acked trigger mail across the disconnect: its in-flight
        // retry timers target a dead socket (cleared), but the entries are
        // held so the next registration that routes the same incarnation
        // redelivers them. Bounded by a retention TTL.
        retainPendingMailForAddress(addr);
      }
    }
    for (const [allocationId, binding] of conn.bindings) {
      if (allocatedConnections.get(allocationId)?.ws !== ws) continue;
      allocatedConnections.delete(allocationId);
      if (binding.kind === "allocated") {
        allocated.push({ allocationId, generation: binding.generation });
      }
    }
    if (sidecarSockets.get(conn.sidecarId) === ws) {
      sidecarSockets.delete(conn.sidecarId);
    }
    connections.delete(ws);

    // Reject any in-flight requests that were sent to this sidecar. Each
    // entry's reject closure runs its own per-site cleanup (the deploy and
    // undeploy closures roll routing back), exactly as a frame-error
    // rejection would.
    pendingRequests.rejectAllForWs(
      ws,
      `Sidecar ${conn.sidecarId} disconnected`,
    );
    // Reject every deploy issued on this socket.
    pendingDeploys.rejectAllForWs(ws, `Sidecar ${conn.sidecarId} disconnected`);
    // Reject any in-flight pack transfers for this sidecar.
    pendingPacks.rejectAllForWs(ws, `Sidecar ${conn.sidecarId} disconnected`);
    // Reject any in-flight undeploys for this sidecar.
    pendingUndeploys.rejectAllForWs(
      ws,
      `Sidecar ${conn.sidecarId} disconnected`,
    );
    // Reject any in-flight probes sent to this sidecar. A probe never enters
    // the address maps, so this ws-keyed sweep is its ONLY disconnect cleanup:
    // without it a probe whose sidecar drops mid-flight would hang until its
    // own timeout instead of failing fast on the disconnect.
    pendingProbes.rejectAllForWs(ws, `Sidecar ${conn.sidecarId} disconnected`);
    pendingWorkflowControls.rejectAllForWs(
      ws,
      `Sidecar ${conn.sidecarId} disconnected`,
    );

    // Cancel any in-flight inbound pack transfers from this sidecar
    // across both receivers. The two receivers track their own in-
    // flight transferIds, so a pending workflow-run transfer for an
    // agent that just disconnected won't outlive the connection just
    // because the agent-state receiver has nothing to cancel.
    const owned = new Set(conn.workflowAddresses.keys());
    for (const addr of owned) {
      agentStatePackReceiver.cancelByAgent(addr);
      workflowRunPackReceiver.cancelByAgent(addr);
    }
    // An own-repository transfer needs no route, so the owned addresses above
    // can miss it. A successor for these allocations closes this connection
    // while registering, before handling any frame of its own, so this cannot
    // cancel the successor's transfers.
    for (const binding of allocationBindings(conn))
      workflowRunPackReceiver.cancelByAgent(binding.workflowRunAddress);

    events.emit("sidecar.disconnect", {
      ownedAddresses: [...owned],
      allocated,
    });

    logger.info`Sidecar ${conn.sidecarId} disconnected`;
  }

  function nextRequestId(): string {
    return `req-${++requestCounter}`;
  }

  // The connection and incarnation the Hub routes `agentAddress` to, or
  // undefined when it routes none. Frames sent through it name the generation
  // of this incarnation.
  function routedIncarnation(agentAddress: string):
    | {
        ws: WsHandle;
        conn: SidecarConnection;
        binding: Extract<SidecarAuthIdentity, { kind: "allocated" }>;
      }
    | undefined {
    const ws = addressIndex.get(agentAddress);
    const conn = ws === undefined ? undefined : connections.get(ws);
    const binding =
      conn === undefined ? undefined : owningAllocation(conn, agentAddress);
    if (ws === undefined || conn === undefined || binding === undefined)
      return undefined;
    return { ws, conn, binding };
  }

  function sendRequest(
    agentAddress: string,
    buildFrame: (requestId: string, generation: number) => HubFrame,
  ): Promise<void> {
    const routed = routedIncarnation(agentAddress);
    if (routed === undefined) {
      return Promise.reject(
        new Error(`No sidecar connected for agent "${agentAddress}"`),
      );
    }
    const { ws, conn, binding } = routed;

    const requestId = nextRequestId();
    const frame = buildFrame(requestId, binding.generation);

    return new Promise<void>((resolve, reject) => {
      pendingRequests.register(
        requestId,
        ws,
        {
          timeoutMs: requestTimeoutMs,
          timeoutMessage: `Request ${requestId} timed out after ${requestTimeoutMs}ms`,
          resolve,
          reject(error: string) {
            reject(new Error(error));
          },
        },
        agentAddress,
      );

      conn.send(frame);
    });
  }

  function packResponseMatches(
    entry: PendingEntry<string, void, PackTransferMeta>,
    ws: WsHandle,
    frame: PackAckFrame | PackRejectFrame,
  ): boolean {
    return (
      entry.ws === ws &&
      entry.meta.agentAddress === frame.agentAddress &&
      entry.meta.repoId.kind === frame.repoId.kind &&
      entry.meta.repoId.id === frame.repoId.id
    );
  }

  function resolvePackPending(ws: WsHandle, frame: PackAckFrame): void {
    const entry = pendingPacks.get(frame.transferId);
    if (entry === undefined) return;
    if (!packResponseMatches(entry, ws, frame)) {
      logger.warn`Ignoring repo.pack.ack for transfer ${frame.transferId} from a connection that does not own the pending transfer`;
      return;
    }
    pendingPacks.resolve(frame.transferId);
  }

  function rejectPackPending(ws: WsHandle, frame: PackRejectFrame): void {
    const entry = pendingPacks.get(frame.transferId);
    if (entry === undefined) return;
    if (!packResponseMatches(entry, ws, frame)) {
      logger.warn`Ignoring repo.pack.reject for transfer ${frame.transferId} from a connection that does not own the pending transfer`;
      return;
    }
    // Surface the receiver's specific cause when it carried one, so the awaiting
    // push sees "corrupt: <detail>" rather than only the coarse reason. The
    // "Pack rejected:" prefix is applied here rather than in the entry's
    // reject closure because a TIMEOUT rejection must not carry it.
    pendingPacks.reject(
      frame.transferId,
      `Pack rejected: ${
        frame.detail !== undefined
          ? `${frame.reason}: ${frame.detail}`
          : frame.reason
      }`,
    );
  }

  function resolveUndeployPending(
    ws: WsHandle,
    frame: AgentUndeployAckFrame,
  ): void {
    const req = pendingUndeploys.get(frame.agentAddress);
    if (req?.ws !== ws || req.meta !== frame.requestId) {
      logger.debug`agent.undeploy.ack for ${frame.agentAddress} generation ${String(frame.generation)} answers an undeploy no request waits on`;
      return;
    }
    pendingUndeploys.resolve(frame.agentAddress);
  }

  function rejectUndeployPending(
    ws: WsHandle,
    frame: AgentUndeployErrorFrame,
  ): void {
    const req = pendingUndeploys.get(frame.agentAddress);
    if (req?.ws === ws && req.meta === frame.requestId) {
      pendingUndeploys.reject(frame.agentAddress, frame.error);
      return;
    }
    // No caller waits on this undeploy, so the failure surfaces only here.
    logger.error`Sidecar failed to undeploy ${frame.agentAddress} generation ${String(frame.generation)}: ${frame.error}`;
  }

  function resolveProbe(
    ws: WsHandle,
    requestId: string,
    result: WorkflowProbeResult,
  ): void {
    const req = pendingProbes.get(requestId);
    if (req === undefined || req.ws !== ws) return;
    pendingProbes.resolve(requestId, result);
  }

  function rejectProbe(ws: WsHandle, requestId: string, error: string): void {
    const req = pendingProbes.get(requestId);
    if (req === undefined || req.ws !== ws) return;
    pendingProbes.reject(requestId, error);
  }

  // Routing rule: pick the receiver dedicated to the repoId.kind the
  // frame carries. The receivers' in-flight state is independent, so a
  // workflow-run transferId can never collide with or evict an
  // agent-state transferId for the same agentAddress.
  function pickPackReceiver(
    repoId: RepoId,
  ): { receiver: ReturnType<typeof createPackReceiver> } | null {
    switch (repoId.kind) {
      case "agent-state":
        return { receiver: agentStatePackReceiver };
      case "workflow-run":
        return { receiver: workflowRunPackReceiver };
      // The remaining kinds in `RepoKind` (`skill`, `package-registry`,
      // `workflow`) have no sidecar->hub pack flow today. A frame
      // arriving with those kinds is malformed at this layer.
      default:
        return null;
    }
  }

  function pickReceivePackLookup(
    repoId: RepoId,
  ): SidecarLookups["receiveWorkflowRunPack"] | undefined {
    switch (repoId.kind) {
      case "agent-state":
        // The agent-state lookup ignores the `source` argument the workflow-run
        // lookup takes; the two are otherwise the same contract.
        return lookups.receiveAgentStatePack;
      case "workflow-run":
        return lookups.receiveWorkflowRunPack;
      default:
        return undefined;
    }
  }

  // The generation whose workflow stop the Hub confirmed, when the pack is
  // for its workflow-run repository. That history is final.
  function stoppedPackSender(
    conn: SidecarConnection,
    frame: PackPushFrame | PackDoneFrame,
  ): SidecarAuthIdentity | undefined {
    const binding = deploymentBinding(conn, frame.agentAddress);
    return frame.repoId.kind === "workflow-run" &&
      binding !== undefined &&
      stoppedAllocations.get(binding.allocationId) === binding.generation
      ? binding
      : undefined;
  }

  // The sender holds a transfer open until it is answered, so a done that
  // fails validation is still rejected when it names its transfer.
  function rejectMalformedPackDone(
    ws: WsHandle,
    done: typeof MalformedPackDone.infer,
  ): void {
    const conn = connections.get(ws);
    if (conn === undefined) return;
    pickPackReceiver(done.repoId)?.receiver.cancel(done.transferId);
    conn.send({
      type: "repo.pack.reject",
      agentAddress: done.agentAddress,
      repoId: done.repoId,
      transferId: done.transferId,
      reason: "corrupt",
    });
  }

  function handlePackPush(ws: WsHandle, frame: PackPushFrame): void {
    const conn = connections.get(ws);
    if (conn === undefined) return;
    // A chunk of a transfer the connection may not make is dropped: the
    // transfer is answered once, when its `repo.pack.done` is rejected.
    if (
      stoppedPackSender(conn, frame) !== undefined ||
      !connCanPushRepo(conn, frame)
    )
      return;

    const picked = pickPackReceiver(frame.repoId);
    if (picked === null) {
      logger.warn`Received repo.pack.push with unsupported repoId.kind ${frame.repoId.kind}`;
      conn.send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason: "corrupt",
      });
      return;
    }

    const reason = picked.receiver.handlePush(frame);
    if (reason !== null) {
      conn.send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason,
      });
    }
  }

  async function handlePackDone(
    ws: WsHandle,
    frame: PackDoneFrame,
  ): Promise<void> {
    const conn = connections.get(ws);
    if (conn === undefined) return;
    const stopped = stoppedPackSender(conn, frame);
    if (stopped !== undefined) {
      logger.warn`Rejected repo.pack.done from allocation ${stopped.allocationId} after its workflow stop`;
      conn.send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason: "path_violation",
      });
      return;
    }
    const identity = deploymentBinding(conn, frame.agentAddress);
    if (identity === undefined || !connCanPushRepo(conn, frame)) {
      // Answer rather than drop: the sender holds the transfer open until it
      // is answered, and later pushes to the same repository wait behind it.
      logger.warn`Rejected repo.pack.done for ${frame.agentAddress} generation ${String(frame.generation)} outside sidecar ${conn.sidecarId}'s routed repositories`;
      conn.send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason: "path_violation",
      });
      return;
    }

    const picked = pickPackReceiver(frame.repoId);
    if (picked === null) {
      logger.warn`Received repo.pack.done with unsupported repoId.kind ${frame.repoId.kind}`;
      conn.send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason: "corrupt",
      });
      return;
    }

    const result = picked.receiver.handleDone(frame);
    if (result === null) {
      conn.send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason: "corrupt",
      });
      return;
    }

    const receivePackLookup = pickReceivePackLookup(frame.repoId);
    if (receivePackLookup === undefined) {
      conn.send({
        type: "repo.pack.ack",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
      });
      return;
    }

    let verdict: Awaited<ReturnType<typeof receivePackLookup>>;
    try {
      verdict = await receivePackLookup(
        frame.repoId,
        result.pack,
        result.ref,
        result.commitSha,
        {
          kind: "allocated",
          agentAddress: frame.agentAddress,
          allocationId: identity.allocationId,
          anchorRunId: identity.anchorRunId,
          generation: identity.generation,
        },
      );
    } catch (err) {
      // A lookup that throws still owes the sender its answer: the socket
      // stays open for the sidecar's other deployments, so nothing else
      // settles the transfer, and later pushes to the repository wait on it.
      logger.error`Receiving the pack for ${frame.agentAddress} generation ${String(frame.generation)} failed: ${err instanceof Error ? err.message : String(err)}`;
      verdict = { accepted: false, reason: "corrupt" };
    }

    // Connection may have closed during async verification.
    const currentConn = connections.get(ws);
    if (currentConn === undefined) return;

    if (verdict.accepted) {
      currentConn.send({
        type: "repo.pack.ack",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
      });
    } else {
      currentConn.send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason: verdict.reason,
      });
    }
  }

  /**
   * Bind a per-step workflow-substrate address to a sidecar for the staging
   * window of a multi-step deploy, so `sendPack` can route the step's deploy
   * and asset packs before the deployment-level frame spawns the child.
   *
   * The address is Hub-minted and workflow-derived, so it enters the
   * `workflowAddresses` set and is
   * torn down by `unbindStepRoute` once the
   * step's packs land. `handleClose` reclaims it if the sidecar drops
   * mid-stage. Per-step addresses are not runtime-routed (mail, signals, and
   * drains use the deployment address), so the binding is transient: no
   * `hello` reports it and no reconnect restores it.
   */
  function fenceAllocation(allocationId: string, generation: number): void {
    const existing = allocationFences.get(allocationId);
    if (existing !== undefined && generation < existing) {
      throw new Error(
        `Cannot move allocation ${allocationId} fence backward from ${String(existing)} to ${String(generation)}`,
      );
    }
    allocationFences.set(allocationId, generation);
    const stopped = stoppedAllocations.get(allocationId);
    if (stopped !== undefined && stopped < generation)
      stoppedAllocations.delete(allocationId);

    // A durable generation advance resolves unfinished initialization as failed.
    // This also covers a cleanup transaction whose response was lost: the next
    // reconciliation rebuilds this fence before it can start a replacement.
    for (const attempt of [...allocatedKeyRecordInFlight.values()]) {
      if (
        attempt.allocationId === allocationId &&
        attempt.generation < generation
      ) {
        noteSenderDeploySettled(attempt, {
          failed: `Allocation ${allocationId} advanced beyond the deployment attempt`,
        });
      }
    }

    const current = allocatedConnections.get(allocationId);
    if (current !== undefined && current.identity.generation !== generation) {
      detachBinding(
        current.ws,
        allocationId,
        `Generation ${String(generation)} superseded it`,
      );
    }

    const waiters = allocationWaiters.get(allocationId);
    if (waiters === undefined) return;
    for (const waiter of [...waiters]) {
      if (waiter.generation === generation) continue;
      clearTimeout(waiter.timer);
      waiters.delete(waiter);
      waiter.reject(
        new Error(
          `Allocation ${allocationId} advanced to generation ${String(generation)}`,
        ),
      );
    }
    if (waiters.size === 0) allocationWaiters.delete(allocationId);
  }

  function retireAllocation(target: AllocatedSidecarTarget): void {
    if (allocationFences.get(target.allocationId) !== target.generation) return;

    detachAllocation(target);
    allocationFences.delete(target.allocationId);
    stoppedAllocations.delete(target.allocationId);

    // The fence is gone, so a lingering attempt can never settle normally.
    // Fail it here rather than leaving a marker that blocks the address.
    for (const attempt of [...allocatedKeyRecordInFlight.values()]) {
      if (
        attempt.allocationId === target.allocationId &&
        attempt.generation <= target.generation
      ) {
        noteSenderDeploySettled(attempt, {
          failed: `Allocation ${target.allocationId} generation ${String(target.generation)} retired`,
        });
      }
    }

    const waiters = allocationWaiters.get(target.allocationId);
    if (waiters === undefined) return;
    allocationWaiters.delete(target.allocationId);
    for (const waiter of waiters) {
      clearTimeout(waiter.timer);
      waiter.reject(
        new Error(
          `Allocation ${target.allocationId} generation ${String(target.generation)} retired`,
        ),
      );
    }
  }

  async function getProvisionedConnection(
    target: AllocatedSidecarTarget,
    use: "readiness" | "routing",
  ): Promise<{
    ws: WsHandle;
    conn: SidecarConnection;
    binding: SidecarAuthIdentity;
  }> {
    if (allocationFences.get(target.allocationId) !== target.generation) {
      throw new Error(
        `Allocation ${target.allocationId} generation ${String(target.generation)} is not current`,
      );
    }
    const current = allocatedConnections.get(target.allocationId);
    if (
      current === undefined ||
      current.identity.generation !== target.generation
    ) {
      throw new Error(
        `Allocated sidecar is not connected for allocation ${target.allocationId} generation ${String(target.generation)}`,
      );
    }
    let identityCurrent: boolean;
    try {
      identityCurrent = await validateSidecarIdentity(current.identity, use);
    } catch (cause) {
      throw new SidecarIdentityValidationError(
        target.allocationId,
        target.generation,
        cause,
      );
    }
    if (!identityCurrent) {
      if (allocatedConnections.get(target.allocationId) === current) {
        detachBinding(
          current.ws,
          target.allocationId,
          "Its identity is no longer current",
        );
      }
      throw new Error(
        `Allocated sidecar identity is no longer current for allocation ${target.allocationId}`,
      );
    }
    if (allocatedConnections.get(target.allocationId) !== current) {
      throw new Error(
        `Allocated sidecar connection changed for allocation ${target.allocationId}`,
      );
    }
    const conn = connections.get(current.ws);
    const binding = conn?.bindings.get(target.allocationId);
    if (
      conn === undefined ||
      binding === undefined ||
      binding.generation !== target.generation
    ) {
      throw new Error(
        `Allocated sidecar is not connected for allocation ${target.allocationId}`,
      );
    }
    return { ws: current.ws, conn, binding };
  }

  async function getAllocatedConnection(
    target: AllocatedSidecarTarget,
    use: "readiness" | "routing",
  ): Promise<{
    ws: WsHandle;
    conn: SidecarConnection;
    binding: Extract<SidecarAuthIdentity, { kind: "allocated" }>;
  }> {
    const { ws, conn, binding } = await getProvisionedConnection(target, use);
    if (binding.kind !== "allocated") {
      throw new Error(
        `Allocation ${target.allocationId} is connected as probe capacity`,
      );
    }
    return { ws, conn, binding };
  }

  async function isAllocatedSidecarReady(
    target: AllocatedSidecarTarget,
  ): Promise<boolean> {
    try {
      await getProvisionedConnection(target, "readiness");
      return true;
    } catch (error) {
      // A failed validation is unknown, not absent: the worker may be healthy
      // behind a failed lookup, so report it distinctly instead of answering
      // `false` and letting the caller release a live worker.
      if (error instanceof SidecarIdentityValidationError) throw error;
      return false;
    }
  }

  function holdsAllocatedBinding(target: AllocatedSidecarTarget): boolean {
    const held = allocatedConnections.get(target.allocationId)?.identity;
    return held?.kind === "allocated" && held.generation === target.generation;
  }

  async function isAllocatedWorkflowActive(
    target: AllocatedSidecarTarget,
  ): Promise<boolean> {
    const { conn, binding } = await getAllocatedConnection(target, "readiness");
    return (
      conn.workflowAddresses.get(binding.workflowRunAddress) ===
      binding.allocationId
    );
  }

  async function waitForAllocatedSidecar(
    target: AllocatedSidecarTarget,
    timeoutMs: number,
    onValidation?: (validation: Promise<boolean>) => void,
    signal?: AbortSignal,
  ): Promise<void> {
    signal?.throwIfAborted();
    // An indeterminable worker waits out the unknown while time remains: only
    // confirmed absence may surface as a connection timeout. At expiry the
    // wait reports the validation failure rather than a missed deadline, so
    // the caller retries instead of releasing a worker that may be healthy.
    let validationFailure: SidecarIdentityValidationError | undefined;
    let ready = false;
    try {
      ready = await isAllocatedSidecarReady(target);
    } catch (error) {
      if (!(error instanceof SidecarIdentityValidationError)) throw error;
      validationFailure = error;
    }
    signal?.throwIfAborted();
    if (ready) return;
    if (allocationFences.get(target.allocationId) !== target.generation) {
      throw new Error(
        `Allocation ${target.allocationId} generation ${String(target.generation)} is not current`,
      );
    }
    if (timeoutMs <= 0) {
      if (validationFailure !== undefined) throw validationFailure;
      throw new Error(
        `Timed out waiting for allocated sidecar ${target.allocationId}`,
      );
    }

    await new Promise<void>((resolve, reject) => {
      const cleanUp = () => {
        clearTimeout(waiter.timer);
        signal?.removeEventListener("abort", onAbort);
        const current = allocationWaiters.get(target.allocationId);
        current?.delete(waiter);
        if (current?.size === 0) allocationWaiters.delete(target.allocationId);
      };
      const onAbort = () => {
        waiter.reject(
          signal?.reason instanceof Error
            ? signal.reason
            : new Error("Sidecar connection wait cancelled"),
        );
      };
      const waiter: AllocationWaiter = {
        generation: target.generation,
        validations: new Set(),
        ...(onValidation !== undefined ? { onValidation } : {}),
        resolve() {
          cleanUp();
          resolve();
        },
        reject(error) {
          cleanUp();
          reject(error);
        },
        timer: setTimeout(() => {
          waiter.reject(
            waiter.validationFailure ??
              (waiter.validations.size > 0
                ? new SidecarIdentityValidationError(
                    target.allocationId,
                    target.generation,
                  )
                : new Error(
                    `Timed out waiting for allocated sidecar ${target.allocationId} generation ${String(target.generation)}`,
                  )),
          );
        }, timeoutMs),
        ...(validationFailure !== undefined ? { validationFailure } : {}),
      };
      let waiters = allocationWaiters.get(target.allocationId);
      if (waiters === undefined) {
        waiters = new Set();
        allocationWaiters.set(target.allocationId, waiters);
      }
      waiters.add(waiter);
      signal?.addEventListener("abort", onAbort, { once: true });
      void notifyAllocationWaiters(target.allocationId);
    });
  }

  async function bindAllocatedStepRoute(
    target: AllocatedSidecarTarget,
    stepAddress: string,
  ): Promise<void> {
    const { ws, conn } = await getAllocatedConnection(target, "routing");
    const existing = addressIndex.get(stepAddress);
    if (existing !== undefined && existing !== ws) {
      throw new Error(
        `Workflow step ${stepAddress} is already routed to another sidecar`,
      );
    }
    const owner = conn.workflowAddresses.get(stepAddress);
    if (owner !== undefined && owner !== target.allocationId) {
      throw new Error(
        `Workflow step ${stepAddress} is already routed to allocation ${owner}`,
      );
    }
    conn.workflowAddresses.set(stepAddress, target.allocationId);
    addressIndex.set(stepAddress, ws);
  }

  function unbindAllocatedStepRoute(
    target: AllocatedSidecarTarget,
    stepAddress: string,
  ): void {
    const current = allocatedConnections.get(target.allocationId);
    if (
      current === undefined ||
      current.identity.generation !== target.generation
    ) {
      return;
    }
    if (addressIndex.get(stepAddress) !== current.ws) return;
    const conn = connections.get(current.ws);
    if (conn?.workflowAddresses.get(stepAddress) !== target.allocationId)
      return;
    conn.workflowAddresses.delete(stepAddress);
    addressIndex.delete(stepAddress);
  }

  // Pack transfers may take longer than session requests due to data volume.
  const PACK_TIMEOUT_MS = requestTimeoutMs * 4;

  function sendPackOnConnection(
    ws: WsHandle,
    conn: SidecarConnection,
    agentAddress: string,
    generation: number,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    options?: SendPackOptions,
  ): Promise<void> {
    const transferId = `pack-${crypto.randomUUID()}`;
    // For the agent-state flow the destination agent and the source repo
    // are the same entity, so `repoId.id === agentAddress`. Asset packs
    // override this with the SOURCE asset's id so audit can correlate
    // the pack back to its hub-side origin.
    const repoId: RepoId = options?.repoId ?? {
      kind: "agent-state",
      id: agentAddress,
    };
    const mountPath = options?.mountPath;

    // Register pending entry before sending frames so that a synchronous
    // repo.pack.ack (e.g. in tests or loopback transports) resolves correctly.
    return new Promise<void>((resolve, reject) => {
      pendingPacks.register(
        transferId,
        ws,
        {
          timeoutMs: PACK_TIMEOUT_MS,
          timeoutMessage: `Pack transfer ${transferId} timed out after ${PACK_TIMEOUT_MS}ms`,
          resolve,
          reject(error: string) {
            reject(new Error(error));
          },
        },
        { agentAddress, repoId },
      );

      // Send chunks
      for (const chunk of chunkPack(pack)) {
        conn.send({
          type: "repo.pack.push",
          agentAddress,
          generation,
          repoId,
          transferId,
          seq: chunk.seq,
          data: chunk.data,
        });
      }

      // Send done
      conn.send({
        type: "repo.pack.done",
        agentAddress,
        generation,
        repoId,
        transferId,
        ref,
        commitSha,
        ...(mountPath !== undefined ? { mountPath } : {}),
      });
    });
  }

  async function sendPackToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    options?: SendPackOptions,
  ): Promise<void> {
    const { ws, conn } = await getAllocatedConnection(target, "routing");
    if (
      addressIndex.get(agentAddress) !== ws ||
      conn.workflowAddresses.get(agentAddress) !== target.allocationId
    ) {
      throw new Error(
        `Address ${agentAddress} is not routed on allocation ${target.allocationId}`,
      );
    }
    let transfer: Promise<void> | undefined;
    await withWorkflowWorkAdmission(ws, conn, agentAddress, () => {
      transfer = sendPackOnConnection(
        ws,
        conn,
        agentAddress,
        target.generation,
        pack,
        ref,
        commitSha,
        options,
      );
      // The acknowledgement can reject before admission's transaction ends.
      void transfer.catch(() => undefined);
      return true;
    });
    if (transfer === undefined) throw new Error("Workflow pack was not sent");
    return transfer;
  }

  async function sendWorkflowRunPackToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    signal?: AbortSignal,
  ): Promise<void> {
    signal?.throwIfAborted();
    const { ws, conn, binding } = await getAllocatedConnection(
      target,
      "routing",
    );
    signal?.throwIfAborted();
    if (agentAddress !== binding.workflowRunAddress) {
      throw new Error(
        `Allocation ${target.allocationId} cannot restore unrelated address ${agentAddress}`,
      );
    }
    let transfer: Promise<void> | undefined;
    await withAllocationWorkAdmission(
      ws,
      binding,
      () => {
        if (conn.workflowAddresses.has(agentAddress)) {
          throw new Error(
            `Allocation ${target.allocationId} already hosts active workflow ${agentAddress}; refusing to overwrite its run history`,
          );
        }
        transfer = sendPackOnConnection(
          ws,
          conn,
          agentAddress,
          target.generation,
          pack,
          ref,
          commitSha,
          { repoId: workflowRunRepoIdForAddress(agentAddress) },
        );
        // The acknowledgement can reject before admission's transaction ends.
        void transfer.catch(() => undefined);
        return true;
      },
      signal,
    );
    if (transfer === undefined)
      throw new Error("Workflow restore was not sent");
    return transfer;
  }

  async function routeMail(
    agentAddress: string,
    rawMessage: string,
    authenticatedSender: string,
    messageId?: string,
    runGrants?: {
      runId: string;
      stepGrants: RunGrantsFrame["stepGrants"];
      senderIdentities?: RunGrantsFrame["senderIdentities"];
    },
  ): Promise<boolean> {
    // `authenticatedSender` is hub-assigned by the caller from a hub-verified
    // value (the ownership-gated sender of a relayed mail, or the triggering
    // principal's address) -- never the message's own MIME `From`. It rides
    // the frame as the hub-verified sender of record, so a recipient can take
    // the sender from it rather than the forgeable `From`. The recipient's
    // signature check reads it as the sender of record -- resolving the
    // sender's key from its local cache to verify the signature -- and its
    // admission policy gates delivery on the verdict.
    //
    // Carry the hub-minted messageId on the frame so the sidecar's durable-
    // receipt ack (`mail.inbound.ack`) keys on the same id the hub tracks, and
    // a redelivery replays identical bytes for the downstream RunStarted dedup.
    // Optional: the workflow-trigger and session-conversation callers supply
    // it (they participate in the ack/retry handshake); a caller without a
    // hub-minted id omits it and the delivery is not tracked for redelivery.
    const routed = routedIncarnation(agentAddress);
    if (routed === undefined) return false;
    const { ws, conn, binding } = routed;
    const frame: MailInboundFrame = {
      type: "mail.inbound",
      agentAddress,
      generation: binding.generation,
      rawMessage,
      authenticatedSender,
      ...(messageId !== undefined ? { messageId } : {}),
    };
    const grantsFrame: HubFrame | undefined =
      runGrants === undefined
        ? undefined
        : {
            type: "run.grants",
            agentAddress,
            generation: binding.generation,
            runId: runGrants.runId,
            stepGrants: runGrants.stepGrants,
            ...(runGrants.senderIdentities !== undefined
              ? { senderIdentities: runGrants.senderIdentities }
              : {}),
          };
    const target = {
      allocationId: binding.allocationId,
      generation: binding.generation,
    };
    try {
      return await withWorkflowWorkAdmission(ws, conn, agentAddress, () => {
        if (grantsFrame !== undefined) conn.send(grantsFrame);
        conn.send(frame);
        // Track the delivery for redelivery until the sidecar acks its durable
        // inbox write. Only mail carrying a hub-minted messageId participates
        // in the ack handshake; relayed agent-to-agent mail omits it and is
        // delivered fire-and-forget as before. A mail that triggered a workflow
        // run carries the run's grants so redelivery can replay them ahead of
        // the mail.
        if (messageId !== undefined) {
          trackPendingMail(
            agentAddress,
            messageId,
            frame,
            target,
            false,
            runGrants,
          );
        }
        return true;
      });
    } catch (error) {
      if (error instanceof WorkflowRunNotExecutableError) throw error;
      logger.warn`Mail admission failed for ${agentAddress}: ${error instanceof Error ? error.message : String(error)}`;
      if (messageId === undefined) return false;
      // Admission may have awaited a disconnect and missed handleClose's
      // pending-mail retention. Record the mail now, then either retry on
      // the current owner or retain it for the next verified reconnect.
      trackPendingMail(
        agentAddress,
        messageId,
        frame,
        target,
        false,
        runGrants,
      );
      const current = addressIndex.get(agentAddress);
      if (current === undefined || !connections.has(current))
        retainPendingMailForAddress(agentAddress);
      return true;
    }
  }

  function sendRunGrants(
    agentAddress: string,
    runId: string,
    stepGrants: RunGrantsFrame["stepGrants"],
    senderIdentities: RunGrantsFrame["senderIdentities"],
  ): boolean {
    const routed = routedIncarnation(agentAddress);
    if (routed === undefined) return false;
    routed.conn.send({
      type: "run.grants",
      agentAddress,
      generation: routed.binding.generation,
      runId,
      stepGrants,
      ...(senderIdentities !== undefined ? { senderIdentities } : {}),
    });
    return true;
  }

  async function sendWorkflowRunDispatchToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    runId: string,
    stepGrants: RunGrantsFrame["stepGrants"],
    rawMessage: string,
    authenticatedSender: string,
    messageId: string,
    signal?: AbortSignal,
  ): Promise<void> {
    signal?.throwIfAborted();
    const { ws, conn } = await getAllocatedConnection(target, "routing");
    signal?.throwIfAborted();
    if (
      addressIndex.get(agentAddress) !== ws ||
      conn.workflowAddresses.get(agentAddress) !== target.allocationId
    ) {
      throw new Error(
        `Address ${agentAddress} is not routed on allocation ${target.allocationId}`,
      );
    }
    // authenticatedSender is the sender persisted at enqueue on the dispatch
    // row (the triggering principal's hub-verified address); the caller reads
    // it from that row. It is never the message's MIME From.
    //
    // Resolve its key here, at dispatch (redelivery) time, from that persisted
    // address, to co-deliver on the run's grants barrier so the recipient
    // caches the sender's current hub-held key. A run sender's deployment key
    // is immutable once acked; a user sender's key rotating mid-flight would
    // leave the fixed signed bytes checked against the new key, which the
    // recipient logs as unverifiable. Null when unresolvable (no resolver
    // wired, or the sender has no durable key). The lookup contract (see
    // SidecarLookups.resolveSenderKey) is best-effort and never throws, so
    // resolving ahead of the run.grants send cannot block it.
    const authenticatedSenderPublicKey =
      lookups.resolveSenderKey !== undefined
        ? await lookups.resolveSenderKey(authenticatedSender)
        : null;
    signal?.throwIfAborted();
    // Co-deliver the resolved key on the run's grants barrier, omitting a null
    // key so it is never cached (see deliverMailToRecipient). The same list
    // rides the pending-mail entry so the reconnect replay carries it too.
    const senderIdentities =
      authenticatedSenderPublicKey !== null
        ? [
            {
              address: authenticatedSender,
              publicKey: authenticatedSenderPublicKey,
            },
          ]
        : undefined;
    const runGrants = {
      runId,
      stepGrants,
      ...(senderIdentities !== undefined ? { senderIdentities } : {}),
    };
    const frame: MailInboundFrame = {
      type: "mail.inbound",
      agentAddress,
      generation: target.generation,
      rawMessage,
      authenticatedSender,
      messageId,
    };
    await withWorkflowWorkAdmission(
      ws,
      conn,
      agentAddress,
      () => {
        conn.send({
          type: "run.grants",
          agentAddress,
          generation: target.generation,
          ...runGrants,
        });
        conn.send(frame);
        trackPendingMail(
          agentAddress,
          messageId,
          frame,
          target,
          true,
          runGrants,
        );
        return true;
      },
      signal,
    );
  }

  // The deploy in flight for a reply's address on this connection, when the
  // reply answers it: the one with the reply's request id and generation.
  function deployAnsweredBy(
    ws: WsHandle,
    frame: AgentDeployAckFrame | AgentDeployErrorFrame,
  ): PendingEntry<string, string, PendingDeploy> | undefined {
    const req = pendingDeploys.get(frame.agentAddress);
    return req?.ws === ws &&
      req.meta.requestId === frame.requestId &&
      req.meta.generation === frame.generation
      ? req
      : undefined;
  }

  async function handleDeployAck(
    ws: WsHandle,
    frame: AgentDeployAckFrame,
  ): Promise<void> {
    connections.get(ws)?.unansweredDeploys.delete(frame.requestId);
    const req = deployAnsweredBy(ws, frame);
    if (req === undefined) {
      logger.warn`Ignoring agent.deploy.ack ${frame.requestId} for ${frame.agentAddress} generation ${String(frame.generation)}: it answers no deploy in flight on this connection`;
      return;
    }
    // The listeners below are awaited, and the deploy this ack answers can be
    // settled meanwhile (a timeout, the allocation leaving) and the address
    // deployed again. Only the deploy the ack answers may be settled by it.
    const stillAnswered = (): boolean => {
      if (pendingDeploys.get(frame.agentAddress) === req) return true;
      logger.warn`Dropping agent.deploy.ack ${frame.requestId} for ${frame.agentAddress} generation ${String(frame.generation)}: its deploy was settled while the ack's listeners ran`;
      return false;
    };

    if (events.listenerCount("agent.deploy.ack") > 0) {
      try {
        const conn = connections.get(ws);
        const identity =
          conn === undefined
            ? undefined
            : owningAllocation(conn, frame.agentAddress);
        await events.emitAndAwait("agent.deploy.ack", {
          agentAddress: frame.agentAddress,
          publicKey: frame.publicKey,
          ...(identity !== undefined
            ? {
                allocated: {
                  allocationId: identity.allocationId,
                  anchorRunId: identity.anchorRunId,
                  generation: identity.generation,
                },
              }
            : {}),
        });
      } catch (err) {
        if (!stillAnswered()) return;
        pendingDeploys.reject(
          frame.agentAddress,
          `Failed to store public key: ${err instanceof Error ? err.message : String(err)}`,
        );
        return;
      }
      if (!stillAnswered()) return;
    }
    pendingDeploys.resolve(frame.agentAddress, frame.publicKey);
  }

  function rejectDeployPendingFromFrame(
    ws: WsHandle,
    frame: AgentDeployErrorFrame,
  ): void {
    connections.get(ws)?.unansweredDeploys.delete(frame.requestId);
    if (deployAnsweredBy(ws, frame) === undefined) {
      logger.warn`Ignoring agent.deploy.error ${frame.requestId} for ${frame.agentAddress} generation ${String(frame.generation)}: it answers no deploy in flight on this connection: ${frame.error}`;
      return;
    }
    pendingDeploys.reject(frame.agentAddress, frame.error);
  }

  function sendAgentDeployOnConnection(
    ws: WsHandle,
    conn: SidecarConnection,
    target: AllocatedSidecarTarget,
    agentAddress: string,
    harnessConfig: HarnessConfig,
    workflow?: AgentDeployFrame["workflow"],
  ): Promise<{ publicKey: string }> {
    if (hubPublicKeyHex === undefined) {
      throw deployFrameFailure(
        "Hub signing key is required for agent deployment",
        false,
      );
    }

    if (pendingDeploys.has(agentAddress)) {
      throw deployFrameFailure(
        `Deploy already in progress for agent "${agentAddress}"`,
        false,
      );
    }

    conn.workflowAddresses.set(agentAddress, target.allocationId);
    addressIndex.set(agentAddress, ws);

    const requestId = nextRequestId();
    const response = Promise.withResolvers<{ publicKey: string }>();
    // Timeout and frame-error rejections share this closure, so the routing
    // rollback and the `frameSent: true` tag live in one place.
    pendingDeploys.register(
      agentAddress,
      ws,
      {
        timeoutMs: requestTimeoutMs,
        timeoutMessage: `Deploy of "${agentAddress}" timed out after ${requestTimeoutMs}ms`,
        resolve(publicKey) {
          response.resolve({ publicKey });
        },
        reject(error: string) {
          if (addressIndex.get(agentAddress) === ws) {
            conn.workflowAddresses.delete(agentAddress);
            addressIndex.delete(agentAddress);
          }
          // The deployment's owner settles any pre-ack sender mail parked on
          // this address once it knows the outcome.
          response.reject(deployFrameFailure(error, true));
        },
      },
      { requestId, generation: target.generation },
    );

    try {
      conn.send({
        type: "agent.deploy",
        requestId,
        agentAddress,
        generation: target.generation,
        agentId: harnessConfig.agentId,
        config: harnessConfig,
        hubPublicKey: hubPublicKeyHex,
        ...(workflow !== undefined ? { workflow } : {}),
      });
      conn.unansweredDeploys.set(requestId, agentAddress);
    } catch (cause) {
      // Throw synchronously on a proven-unsent frame. Returning the response
      // promise below is the caller's evidence that the send took place.
      pendingDeploys.delete(agentAddress);
      if (addressIndex.get(agentAddress) === ws) {
        conn.workflowAddresses.delete(agentAddress);
        addressIndex.delete(agentAddress);
      }
      throw deployFrameFailure(
        `Deploy of "${agentAddress}" failed to send: ${cause instanceof Error ? cause.message : String(cause)}`,
        false,
        cause,
      );
    }
    return response.promise;
  }

  async function sendAgentDeployToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    harnessConfig: HarnessConfig,
    workflow?: AgentDeployFrame["workflow"],
    signal?: AbortSignal,
    beforeSend?: () => Promise<void>,
  ): Promise<{ publicKey: string }> {
    let response: Promise<{ publicKey: string }> | undefined;
    try {
      signal?.throwIfAborted();
      const { ws, conn, binding } = await getAllocatedConnection(
        target,
        "routing",
      );
      signal?.throwIfAborted();
      if (agentAddress !== binding.workflowRunAddress) {
        throw new Error(
          `Allocation ${target.allocationId} cannot deploy unrelated address ${agentAddress}`,
        );
      }
      const existing = addressIndex.get(agentAddress);
      if (existing !== undefined && existing !== ws) {
        throw new Error(
          `Deployment ${agentAddress} is already routed to another sidecar`,
        );
      }
      if (hubPublicKeyHex === undefined)
        throw new Error("Hub signing key is required for agent deployment");
      if (pendingDeploys.has(agentAddress))
        throw new Error(
          `Deploy already in progress for agent "${agentAddress}"`,
        );
      // The sidecar may still be tearing down a copy of this address it was
      // told to undeploy, and anything that copy sends names the same address.
      // Routing the address back before the answer would credit it all here.
      await undeployAnswered(conn, agentAddress, signal);
      signal?.throwIfAborted();
      await beforeSend?.();
      await withAllocationWorkAdmission(
        ws,
        binding,
        () => {
          response = sendAgentDeployOnConnection(
            ws,
            conn,
            target,
            agentAddress,
            harnessConfig,
            workflow,
          );
          // Own an early ack rejection while the admission transaction ends.
          void response.catch(() => undefined);
          return true;
        },
        signal,
      );
      if (response === undefined)
        throw new Error("Workflow deploy was not sent");
    } catch (cause) {
      // A failed transaction response cannot roll back a frame already sent.
      throw deployFrameFailure(
        cause instanceof Error ? cause.message : String(cause),
        response !== undefined,
        cause,
      );
    }
    // Never hold the lifecycle locks while waiting for the worker's reply.
    return response;
  }

  /**
   * Provision one step of a multi-step deploy on the sidecar WITHOUT
   * spawning: the sidecar initializes the step's agent-state repo and
   * records the hub key, so the follow-up deploy pack applies into a repo
   * and verifies against the recorded key -- but no supervisor or child is
   * constructed. The deployment-level workflow frame, sent once after every
   * step is provisioned, spawns the child.
   *
   * The step address must already be bound via `bindStepRoute`, which
   * resolves and records the sidecar; this reuses that route. Waits for the
   * sidecar's `agent.deploy.ack`
   * so the caller can safely deliver the deploy pack afterward. On failure
   * the caller owns tearing the route down via `unbindStepRoute`.
   */
  function sendProvisionStepOnConnection(
    ws: WsHandle,
    conn: SidecarConnection,
    generation: number,
    agentAddress: string,
    harnessConfig: HarnessConfig,
  ): Promise<void> {
    if (hubPublicKeyHex === undefined) {
      throw new Error("Hub signing key is required for step provisioning");
    }
    if (pendingDeploys.has(agentAddress)) {
      throw new Error(`Deploy already in progress for agent "${agentAddress}"`);
    }

    const hubKey = hubPublicKeyHex;
    const requestId = nextRequestId();
    return new Promise<void>((resolve, reject) => {
      // The sidecar's `agent.deploy.ack` resolves this through
      // `pendingDeploys.resolve`. The per-step address is workflow-derived
      // and records no hub-side key, so the ack's public key is not needed
      // and this resolves void.
      pendingDeploys.register(
        agentAddress,
        ws,
        {
          timeoutMs: requestTimeoutMs,
          timeoutMessage: `Step provision of "${agentAddress}" timed out after ${requestTimeoutMs}ms`,
          resolve(_publicKey) {
            resolve();
          },
          reject(error: string) {
            reject(new Error(error));
          },
        },
        { requestId, generation },
      );

      conn.send({
        type: "agent.deploy",
        requestId,
        agentAddress,
        generation,
        agentId: harnessConfig.agentId,
        config: harnessConfig,
        hubPublicKey: hubKey,
        provisionStep: true,
      });
      conn.unansweredDeploys.set(requestId, agentAddress);
    });
  }

  async function sendProvisionStepToAllocation(
    target: AllocatedSidecarTarget,
    agentAddress: string,
    harnessConfig: HarnessConfig,
  ): Promise<void> {
    const { ws, conn } = await getAllocatedConnection(target, "routing");
    if (
      addressIndex.get(agentAddress) !== ws ||
      conn.workflowAddresses.get(agentAddress) !== target.allocationId
    ) {
      throw new Error(
        `Step route ${agentAddress} is not bound to allocation ${target.allocationId}`,
      );
    }
    let provisioned: Promise<void> | undefined;
    await withWorkflowWorkAdmission(ws, conn, agentAddress, () => {
      provisioned = sendProvisionStepOnConnection(
        ws,
        conn,
        target.generation,
        agentAddress,
        harnessConfig,
      );
      // The acknowledgement can reject before admission's transaction ends.
      void provisioned.catch(() => undefined);
      return true;
    });
    if (provisioned === undefined)
      throw new Error("Workflow step was not provisioned");
    return provisioned;
  }

  function sendProbeOnConnection(
    ws: WsHandle,
    conn: SidecarConnection,
    probeId: string,
    args: SendProbeArgs,
  ): Promise<WorkflowProbeResult> {
    const requestId = nextRequestId();

    return new Promise<WorkflowProbeResult>((resolve, reject) => {
      pendingProbes.register(
        requestId,
        ws,
        {
          timeoutMs: probeTimeoutMs,
          timeoutMessage: `Probe ${requestId} timed out after ${probeTimeoutMs}ms`,
          resolve,
          reject(error: string) {
            reject(new Error(error));
          },
        },
        probeId,
      );

      conn.send({
        type: "workflow.probe.request",
        requestId,
        source: args.source,
        closure: args.closure,
        entry: args.entry,
        ...(args.assets !== undefined ? { assets: args.assets } : {}),
      });
    });
  }

  async function sendProbeToAllocation(
    target: AllocatedSidecarTarget,
    args: SendProbeArgs,
  ): Promise<WorkflowProbeResult> {
    const { ws, conn } = await getProvisionedConnection(target, "routing");
    return sendProbeOnConnection(ws, conn, target.allocationId, args);
  }

  function detachAllocation(target: AllocatedSidecarTarget): void {
    const current = allocatedConnections.get(target.allocationId);
    if (
      current === undefined ||
      current.identity.generation !== target.generation
    ) {
      return;
    }
    detachBinding(current.ws, target.allocationId, "It was released");
  }

  async function handleWorkflowControlAck(
    ws: WsHandle,
    frame: WorkflowControlAckFrame,
  ): Promise<void> {
    const entry = pendingWorkflowControls.get(frame.requestId);
    if (entry === undefined || entry.ws !== ws) return;
    const fail = (error: Error) => {
      pendingWorkflowControls.delete(frame.requestId);
      entry.meta.fail(error);
    };
    try {
      const current = await getAllocatedConnection(entry.meta, "routing");
      if (current.ws !== ws) return;
    } catch (cause) {
      // A failed lookup leaves the stop unknown, not refused. The caller
      // retries, and the worker acknowledges a repeated stop.
      fail(
        cause instanceof SidecarIdentityValidationError
          ? cause
          : new WorkflowControlUnreachableError(
              "Workflow control allocation changed",
              cause,
            ),
      );
      return;
    }
    if (frame.error !== undefined)
      fail(
        frame.error === WORKFLOW_CONTROL_INITIALIZING_ERROR
          ? new WorkflowControlInitializingError()
          : new WorkflowControlRejectedError(frame.error),
      );
    else {
      if (entry.meta.action === "stop") {
        // Confirming the stop fences the worker's packs, so it must wait until
        // the Hub holds the history the worker reported. A generation already
        // confirmed is fenced, so the history it holds is final.
        const unreceived =
          stoppedAllocations.get(entry.meta.allocationId) ===
          entry.meta.generation
            ? null
            : await findUnreceivedWorkflowHistory(
                entry.meta.agentAddress,
                frame.refTips,
              );
        if (unreceived !== null) {
          fail(new WorkflowControlHistoryPendingError(unreceived));
          return;
        }
        stoppedAllocations.set(entry.meta.allocationId, entry.meta.generation);
        removeRoute(ws, entry.meta.agentAddress);
      }
      pendingWorkflowControls.resolve(frame.requestId, undefined);
    }
  }

  // Describes the first ref whose reported tip the Hub does not hold, or
  // returns null when the Hub holds the worker's whole history.
  async function findUnreceivedWorkflowHistory(
    agentAddress: string,
    reported: WorkflowRunRefTips | undefined,
  ): Promise<string | null> {
    if (reported === undefined) return "the worker did not report its ref tips";
    if (lookups.readWorkflowRunRefTips === undefined)
      return "the Hub cannot read workflow history";
    let received: WorkflowRunRefTips;
    try {
      received = await lookups.readWorkflowRunRefTips(agentAddress);
    } catch (cause) {
      return `the Hub cannot read workflow history: ${cause instanceof Error ? cause.message : String(cause)}`;
    }
    for (const [ref, tip] of Object.entries(received)) {
      const reportedTip = reported[ref];
      if (reportedTip !== tip)
        return `${ref} is at ${tip ?? "no commit"} on the Hub and ${reportedTip ?? "no commit"} on the worker`;
    }
    return null;
  }

  async function sendWorkflowControl(
    target: AllocatedSidecarTarget,
    command: Omit<WorkflowControlFrame, "type" | "requestId" | "generation">,
    timeoutMs: number,
  ): Promise<void> {
    let connection: Awaited<ReturnType<typeof getAllocatedConnection>>;
    try {
      connection = await getAllocatedConnection(target, "routing");
    } catch (cause) {
      if (cause instanceof SidecarIdentityValidationError) throw cause;
      throw new WorkflowControlUnreachableError(
        cause instanceof Error ? cause.message : String(cause),
        cause,
      );
    }
    const { ws, conn, binding } = connection;
    if (
      command.agentAddress !== binding.workflowRunAddress ||
      command.runId !== binding.anchorRunId
    ) {
      throw new Error(
        "Workflow control does not target the allocation's anchor run",
      );
    }
    const requestId = nextRequestId();
    const timeoutMessage = `Workflow control ${requestId} timed out`;
    const unconfirmedMessage = `Workflow control ${requestId} was acknowledged but not processed in time`;
    return new Promise<void>((resolve, reject) => {
      let cancelProcessing: (() => void) | undefined;
      const settle = () => {
        cancelProcessing?.();
      };
      pendingWorkflowControls.register(
        requestId,
        ws,
        {
          timeoutMs,
          timeoutMessage,
          resolve: () => {
            settle();
            resolve();
          },
          // The tracker reports timeouts, disconnect sweeps, send failures,
          // and the processing limit as strings. Only a timeout shows the live
          // connection stayed silent.
          reject: (error) => {
            settle();
            reject(
              error === timeoutMessage
                ? [...conn.unansweredDeploys.values()].includes(
                    command.agentAddress,
                  )
                  ? new WorkflowControlInitializingError()
                  : new WorkflowControlTimeoutError(error)
                : error === unconfirmedMessage
                  ? new WorkflowControlUnconfirmedError(error)
                  : new WorkflowControlUnreachableError(error),
            );
          },
        },
        {
          ...target,
          agentAddress: command.agentAddress,
          action: command.action,
          fail: (error) => {
            settle();
            reject(error);
          },
          received: () => {
            const entry = pendingWorkflowControls.get(requestId);
            if (entry === undefined || cancelProcessing !== undefined) return;
            entry.cancelTimeout();
            cancelProcessing = scheduleTimeout(() => {
              pendingWorkflowControls.reject(requestId, unconfirmedMessage);
            }, timeoutMs);
          },
        },
      );
      try {
        conn.send({
          type: "workflow.control",
          requestId,
          generation: target.generation,
          ...command,
        });
      } catch (error) {
        pendingWorkflowControls.reject(
          requestId,
          error instanceof Error ? error.message : String(error),
        );
      }
    });
  }

  function sendAgentUndeploy(
    agentAddress: string,
    reason: string,
  ): Promise<void> {
    const routed = routedIncarnation(agentAddress);
    if (routed === undefined) {
      return Promise.reject(
        new Error(`No sidecar connected for agent "${agentAddress}"`),
      );
    }
    const { ws, conn, binding } = routed;
    const requestId = nextRequestId();

    return new Promise<void>((resolve, reject) => {
      // Timeout, ack, and error rejection share one closure so the routing
      // teardown runs exactly once no matter how the round-trip settles.
      pendingUndeploys.register(
        agentAddress,
        ws,
        {
          timeoutMs: requestTimeoutMs,
          timeoutMessage: `Undeploy of "${agentAddress}" timed out after ${requestTimeoutMs}ms`,
          resolve() {
            removeRoute(ws, agentAddress);
            resolve();
          },
          reject(error: string) {
            removeRoute(ws, agentAddress);
            reject(new Error(error));
          },
        },
        requestId,
      );

      conn.send({
        type: "agent.undeploy",
        requestId,
        agentAddress,
        generation: binding.generation,
        reason,
      });
      noteUndeploying(conn, agentAddress, requestId);
    });
  }

  function removeRoute(ws: WsHandle, agentAddress: string): void {
    if (addressIndex.get(agentAddress) === ws)
      addressIndex.delete(agentAddress);
    connections.get(ws)?.workflowAddresses.delete(agentAddress);
  }

  function dispatchToSubscribers(agentAddress: string, event: unknown): void {
    const subs = agentSubscribers.get(agentAddress);
    if (subs === undefined) return;
    for (const cb of [...subs]) {
      try {
        cb(event);
      } catch (err) {
        logger.warn`Agent subscriber threw: ${err instanceof Error ? err.message : String(err)}`;
      }
    }
  }

  function subscribeAgent(
    agentAddress: string,
    callback: (event: unknown) => void,
  ): () => void {
    let subs = agentSubscribers.get(agentAddress);
    if (subs === undefined) {
      subs = new Set();
      agentSubscribers.set(agentAddress, subs);
    }
    subs.add(callback);
    return () => {
      const current = agentSubscribers.get(agentAddress);
      if (current === undefined) return;
      current.delete(callback);
      if (current.size === 0) {
        agentSubscribers.delete(agentAddress);
      }
    };
  }

  function getConnectedSidecars(): string[] {
    return Array.from(connections.values()).map((c) => c.sidecarId);
  }

  function getRoutableAddresses(): string[] {
    return Array.from(addressIndex.keys());
  }

  function getConnectorState(
    agentAddress: string,
  ): ConnectorThreadState | null {
    return connectorStates.get(agentAddress) ?? null;
  }

  async function sendSourcesUpdate(
    agentAddress: string,
    sources: InferenceSource[],
    defaultSource: string,
  ): Promise<void> {
    await sendRequest(agentAddress, (requestId, generation) => ({
      type: "sources.update",
      requestId,
      agentAddress,
      generation,
      sources,
      defaultSource,
    }));
  }

  async function sendCredentialsUpdate(
    agentAddress: string,
    delivery: CredentialDelivery,
    revoke?: string[],
  ): Promise<void> {
    await sendRequest(agentAddress, (requestId, generation) => ({
      type: "credentials.update",
      requestId,
      agentAddress,
      generation,
      delivery,
      ...(revoke !== undefined ? { revoke } : {}),
    }));
  }

  async function sendSignalDeliver(opts: {
    agentAddress: string;
    runId: string;
    signalName: string;
    signalId: string;
    payload: unknown;
  }): Promise<void> {
    const routed = routedIncarnation(opts.agentAddress);
    if (routed === undefined) {
      throw new Error(
        `No sidecar connected for deployment "${opts.agentAddress}"`,
      );
    }
    const { ws, conn, binding } = routed;
    await withWorkflowWorkAdmission(ws, conn, opts.agentAddress, () => {
      conn.send({
        type: "signal.deliver",
        agentAddress: opts.agentAddress,
        generation: binding.generation,
        runId: opts.runId,
        signalName: opts.signalName,
        signalId: opts.signalId,
        payload: opts.payload,
      });
      return true;
    });
  }

  async function sendSignalDeliverToAllocation(
    target: AllocatedSidecarTarget,
    opts: {
      agentAddress: string;
      runId: string;
      signalName: string;
      signalId: string;
      payload: unknown;
    },
    signal?: AbortSignal,
  ): Promise<void> {
    signal?.throwIfAborted();
    const { ws, conn } = await getAllocatedConnection(target, "routing");
    signal?.throwIfAborted();
    if (
      addressIndex.get(opts.agentAddress) !== ws ||
      conn.workflowAddresses.get(opts.agentAddress) !== target.allocationId
    ) {
      throw new Error(
        `Address ${opts.agentAddress} is not routed on allocation ${target.allocationId}`,
      );
    }
    await withWorkflowWorkAdmission(
      ws,
      conn,
      opts.agentAddress,
      () => {
        conn.send({
          type: "signal.deliver",
          ...opts,
          generation: target.generation,
        });
        return true;
      },
      signal,
    );
  }

  function sendDrain(opts: { agentAddress: string; deadlineMs: number }): void {
    const routed = routedIncarnation(opts.agentAddress);
    if (routed === undefined) {
      throw new Error(
        `No sidecar connected for deployment "${opts.agentAddress}"`,
      );
    }
    routed.conn.send({
      type: "drain.deliver",
      agentAddress: opts.agentAddress,
      generation: routed.binding.generation,
      deadlineMs: opts.deadlineMs,
    });
  }

  return {
    handleOpen,
    handleMessage,
    handleClose,
    routeMail,
    sendRunGrants,
    noteSenderDeployStarted,
    noteSenderDeploySettled,
    sendProbeToAllocation,
    detachAllocation,
    syncSidecar,
    sendAgentUndeploy,
    sendWorkflowControl,
    sendSourcesUpdate,
    sendCredentialsUpdate,
    sendPackToAllocation,
    sendWorkflowRunPackToAllocation,
    fenceAllocation,
    retireAllocation,
    waitForAllocatedSidecar,
    isAllocatedSidecarReady,
    holdsAllocatedBinding,
    isAllocatedWorkflowActive,
    sendAgentDeployToAllocation,
    bindAllocatedStepRoute,
    unbindAllocatedStepRoute,
    sendProvisionStepToAllocation,
    sendWorkflowRunDispatchToAllocation,
    sendSignalDeliver,
    sendSignalDeliverToAllocation,
    sendDrain,
    subscribeAgent,
    dispatchAgentEvent: dispatchToSubscribers,
    getConnectedSidecars,
    getRoutableAddresses,
    getConnectorState,
    events,
  };
}
