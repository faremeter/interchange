// SidecarOrchestrator: constructs and wires every package-side piece of the
// sidecar runtime -- stores, SessionManager, HubLink -- and returns a single
// start/close handle the host driver uses. The host supplies policy (data
// directory, crypto primitives, hub credentials, deploy-router factory); the
// orchestrator does the composition. The multi-step deploy path forwards a
// spawned child's verified InferenceEvents to the hub through a sink the
// orchestrator owns: it points at a no-op closure until HubLink is
// constructed, then is rewired to hubLink.sendEvent, containing the
// cross-reference inside this module.

import { getLogger } from "@intx/log";
import type { HubTransport } from "@intx/mail-memory";
import type { SignalKind } from "@intx/types";
import type {
  ApprovalSnapshot,
  CryptoProvider,
  InferenceEvent,
  KeyPair,
} from "@intx/types/runtime";

import { createAgentKeyStore, type AgentKeyStore } from "./agent-key-store";
import { createAgentRepoStore, type AgentRepoStore } from "./agent-repo-store";
import { createSessionManager, type SessionManager } from "./session-manager";
import {
  createHubLink,
  type DeployRouter,
  type HubLink,
  type MailInboundRouter,
  type SignalInboundRouter,
  type DrainInboundRouter,
  type GrantsInboundRouter,
  type SourcesInboundRouter,
  type CredentialsInboundRouter,
  type WorkflowRunPackApplier,
  type WorkflowProbeExecutor,
  type ReconnectScheduler,
} from "./ws/hub-link";
import type { ResolvedInboundMailPolicy } from "./ws/inbound-signature";

const log = getLogger(["interchange", "hub-agent", "orchestrator"]);

export type SidecarCryptoOps = {
  generateKeyPair(): Promise<KeyPair>;
  verifySSHSig(
    payload: string,
    signature: string,
    publicKey: Uint8Array,
  ): Promise<boolean>;
};

/**
 * Factory the orchestrator invokes once `sessions` and `keyStore` are
 * constructed; the returned `DeployRouter` routes every inbound
 * `agent.deploy` frame on the link. The host closes over any other state
 * the router needs at the call site.
 */
export type CreateDeployRouter = (deps: {
  sessions: SessionManager;
  keyStore: AgentKeyStore;
  /**
   * Per-event sink the multi-step branch routes a spawned child's verified
   * `InferenceEvent`s through, keyed by the deployment's agent address and
   * the deploy's session id. Wired to the same hub-link `agent.event` sink
   * the in-process path's `onEvent` uses. `sessionId` is optional because a
   * deploy frame need not carry one; the sink drops a sessionless event
   * rather than guessing a session.
   */
  publishWorkflowInferenceEvent: (
    agentAddress: string,
    event: InferenceEvent,
    sessionId: string | undefined,
  ) => void;
  /**
   * Control-plane suspension sink the multi-step branch routes a
   * supervisor's `park.notify` registration through. Wired to the hub-link's
   * `sendSignalCorrelationRegister` so a parked run's correlation is
   * registered at the hub (routing + approval rows). Mirrors
   * `publishWorkflowInferenceEvent`: a no-op until HubLink is constructed,
   * then swapped to the link's sink.
   */
  publishWorkflowSuspension: (registration: {
    correlationId: string;
    runId: string;
    anchorRunId: string;
    agentAddress: string;
    kind: SignalKind;
    approvalSnapshot?: ApprovalSnapshot;
  }) => void;
}) => DeployRouter;

export type SidecarOrchestratorConfig = {
  hubURL: string;
  sidecarId: string;
  token: string;
  dataDir: string;
  transport: HubTransport;
  cryptoOps: SidecarCryptoOps;
  /**
   * Resolves a sender address to the crypto that verifies its inbound mail.
   * The host builds this over the sidecar's sender-key cache; forwarded
   * unchanged to `createHubLink`.
   */
  resolveSenderCrypto: (address: string) => CryptoProvider | undefined;
  /**
   * Resolves a recipient deployment address to its total inbound-mail
   * admission policy. The host builds this over the per-address policy
   * registry; forwarded unchanged to `createHubLink`.
   */
  lookupInboundMailPolicy: (address: string) => ResolvedInboundMailPolicy;
  /**
   * Persists the hub-vouched public key for a sender address. The host builds
   * it over the sender-key cache; forwarded unchanged to `createHubLink`,
   * where an inbound `sender.key.refresh` frame drives it.
   */
  cacheSenderKey: (address: string, publicKey: string) => Promise<void>;
  /**
   * Durably removes a sender's cached key. The host builds it over the
   * sender-key cache; forwarded unchanged to `createHubLink`, where an
   * inbound `sender.key.evict` frame drives it.
   */
  evictSenderKey: (address: string) => Promise<void>;
  /** Host-injected `DeployRouter` factory; called once after `sessions` and `keyStore` are built. */
  createDeployRouter: CreateDeployRouter;
  /**
   * Optional pre-fallback mail dispatcher the link consults on every
   * inbound `mail.inbound` frame. Production wires the multi-step
   * deployment mail handler registry so a deployment-address inbound flows
   * into the supervisor's mail-bus subscription. Forwarded unchanged.
   */
  mailInboundRouter?: MailInboundRouter;
  /**
   * Optional pre-fallback signal dispatcher the link consults on every
   * inbound `signal.deliver` frame. Production wires the multi-step
   * deployment signal handler registry so a deployment-address signal
   * flows into the supervisor's `deliverSignal`. Forwarded unchanged.
   */
  signalInboundRouter?: SignalInboundRouter;
  /**
   * Optional pre-fallback drain dispatcher the link consults on every
   * inbound `drain.deliver` frame. Production wires the multi-step
   * deployment drain handler registry so a deployment-address drain flows
   * into the supervisor's `drain`. Forwarded unchanged.
   */
  drainInboundRouter?: DrainInboundRouter;
  /**
   * Optional inbound grants dispatcher the link consults on every inbound
   * `run.grants` frame. Production wires the multi-step deployment grants
   * handler registry so a deployment-address grants frame flows into the
   * deployment's wiring, which writes the run's grants to its `workflow-run`
   * repo. Forwarded unchanged.
   */
  grantsInboundRouter?: GrantsInboundRouter;
  /**
   * Optional inbound sources-rotation dispatcher the link consults on every
   * inbound `sources.update` frame. Production wires the single-step
   * deployment sources handler registry so a rotation flows into the
   * supervisor's `deliverSources`. Forwarded unchanged.
   */
  sourcesInboundRouter?: SourcesInboundRouter;
  /** Apply Hub-authoritative workflow-run refs before replacement deploy. */
  applyWorkflowRunPack: WorkflowRunPackApplier;
  /**
   * Optional inbound credential-delivery dispatcher the link consults on
   * every inbound `credentials.update` frame. Production wires the
   * per-deployment credential handler registry so a delivery flows into
   * the supervisor's `deliverCredentials`. Forwarded unchanged.
   */
  credentialsInboundRouter?: CredentialsInboundRouter;
  /**
   * Optional workflow-probe executor, forwarded unchanged to `createHubLink`.
   * Omitted, the link falls back to its rejecting placeholder so a probe is
   * answered with an error rather than dropped.
   */
  workflowProbeExecutor?: WorkflowProbeExecutor;
  /**
   * Returns the workflow-substrate deployment addresses this sidecar
   * currently hosts; the link announces them on every (re)connect so the
   * hub re-registers them for routing. Omitted, the link announces none.
   */
  getWorkflowAddresses?: () => string[];
  /**
   * Returns the rotatable (non-run) sender addresses this sidecar holds cached
   * keys for; the link reports them on every (re)connect so the hub
   * re-resolves and re-pushes each key. Omitted, the link reports none.
   */
  getCachedSenderAddresses?: () => string[];
  /**
   * Invoked with the workflow-substrate addresses the link just announced in
   * an authenticated reconnect, so the workflow-run pack pusher can re-drive
   * a push a disconnect cancelled. Omitted, the link fires nothing.
   */
  onWorkflowAddressesRoutable?: (addresses: string[]) => void;
  /**
   * Invoked on WS disconnect with the workflow-substrate addresses the link
   * hosts, so the workflow-run pack pusher blocks their pushes until the
   * authenticated reconnect re-routes them. Paired with
   * `onWorkflowAddressesRoutable`. Omitted, the link fires nothing.
   */
  onWorkflowAddressesUnroutable?: (addresses: string[]) => void;
  pingIntervalMs?: number;
  reconnectDelayMs?: number;
  scheduleReconnect?: ReconnectScheduler;
};

export type SidecarOrchestrator = {
  /** Open the hub connection and put the runtime into service. */
  start(): void;
  /** Tear the runtime down: close the hub connection. */
  close(): void;
  /** The store handles, for callers that need to inspect them. */
  readonly repoStore: AgentRepoStore;
  readonly keyStore: AgentKeyStore;
  readonly sessions: SessionManager;
  readonly hubLink: HubLink;
};

export function createSidecarOrchestrator(
  config: SidecarOrchestratorConfig,
): SidecarOrchestrator {
  const {
    hubURL,
    sidecarId,
    token,
    dataDir,
    transport,
    cryptoOps,
    resolveSenderCrypto,
    lookupInboundMailPolicy,
    cacheSenderKey,
    evictSenderKey,
    createDeployRouter,
    mailInboundRouter,
    signalInboundRouter,
    drainInboundRouter,
    grantsInboundRouter,
    sourcesInboundRouter,
    credentialsInboundRouter,
    applyWorkflowRunPack,
    workflowProbeExecutor,
    getWorkflowAddresses,
    getCachedSenderAddresses,
    onWorkflowAddressesRoutable,
    onWorkflowAddressesUnroutable,
    pingIntervalMs,
    reconnectDelayMs,
    scheduleReconnect,
  } = config;

  const repoStore = createAgentRepoStore({ dataDir });
  const keyStore = createAgentKeyStore({
    dataDir,
    generateKeyPair: cryptoOps.generateKeyPair,
    verifySSHSig: cryptoOps.verifySSHSig,
  });

  // Sink the multi-step deploy path routes a spawned child's verified
  // InferenceEvents through. A no-op until HubLink is constructed below, then
  // swapped to the link's sendEvent method.
  let dispatchEvent: (
    agentAddress: string,
    sessionId: string,
    event: InferenceEvent,
  ) => void = () => {
    /* replaced after HubLink construction */
  };

  // Sink the multi-step deploy path routes a supervisor's `park.notify`
  // suspension registration through. A no-op until HubLink is constructed,
  // then swapped to the link's sendSignalCorrelationRegister method.
  let dispatchSuspension: (registration: {
    correlationId: string;
    runId: string;
    anchorRunId: string;
    agentAddress: string;
    kind: SignalKind;
    approvalSnapshot?: ApprovalSnapshot;
  }) => void = () => {
    /* replaced after HubLink construction */
  };

  const sessions = createSessionManager({ repoStore });

  const deployRouter = createDeployRouter({
    sessions,
    keyStore,
    // Route a spawned child's verified InferenceEvents up the same hub-link
    // `agent.event` sink the in-process path uses. `dispatchEvent` is a no-op
    // until HubLink is constructed below; the closure reads it lazily so the
    // post-construction swap is observed. A sessionless event is dropped
    // rather than guessed onto an arbitrary session -- the hub timeline is
    // session-keyed and a forged session id would mis-route the event.
    publishWorkflowInferenceEvent: (agentAddress, event, sessionId) => {
      if (sessionId === undefined) {
        log.warn(
          "Dropping workflow inference event for {agentAddress}: deploy carried no sessionId",
          { agentAddress },
        );
        return;
      }
      dispatchEvent(agentAddress, sessionId, event);
    },
    // Route a supervisor's suspension registration up the hub-link so the
    // hub co-writes the parked run's routing + approval rows.
    // `dispatchSuspension` is a no-op until HubLink is constructed below;
    // the closure reads it lazily so the post-construction swap is observed.
    publishWorkflowSuspension: (registration) => {
      dispatchSuspension(registration);
    },
  });

  const hubLink = createHubLink({
    hubURL,
    sidecarId,
    token,
    transport,
    sessions,
    keyStore,
    resolveSenderCrypto,
    lookupInboundMailPolicy,
    cacheSenderKey,
    evictSenderKey,
    deployRouter,
    applyWorkflowRunPack,
    ...(mailInboundRouter !== undefined ? { mailInboundRouter } : {}),
    ...(signalInboundRouter !== undefined ? { signalInboundRouter } : {}),
    ...(drainInboundRouter !== undefined ? { drainInboundRouter } : {}),
    ...(grantsInboundRouter !== undefined ? { grantsInboundRouter } : {}),
    ...(sourcesInboundRouter !== undefined ? { sourcesInboundRouter } : {}),
    ...(credentialsInboundRouter !== undefined
      ? { credentialsInboundRouter }
      : {}),
    ...(workflowProbeExecutor !== undefined ? { workflowProbeExecutor } : {}),
    ...(getWorkflowAddresses !== undefined ? { getWorkflowAddresses } : {}),
    ...(getCachedSenderAddresses !== undefined
      ? { getCachedSenderAddresses }
      : {}),
    ...(onWorkflowAddressesRoutable !== undefined
      ? { onWorkflowAddressesRoutable }
      : {}),
    ...(onWorkflowAddressesUnroutable !== undefined
      ? { onWorkflowAddressesUnroutable }
      : {}),
    ...(pingIntervalMs !== undefined ? { pingIntervalMs } : {}),
    ...(reconnectDelayMs !== undefined ? { reconnectDelayMs } : {}),
    ...(scheduleReconnect !== undefined ? { scheduleReconnect } : {}),
  });

  dispatchEvent = hubLink.sendEvent;
  dispatchSuspension = hubLink.sendSignalCorrelationRegister;

  function start(): void {
    hubLink.connect();
    log.info("Sidecar {sidecarId} connecting to {hubURL}", {
      sidecarId,
      hubURL,
    });
  }

  function close(): void {
    hubLink.close();
  }

  return { start, close, repoStore, keyStore, sessions, hubLink };
}
