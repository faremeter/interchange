// SidecarOrchestrator: constructs and wires every package-side piece
// of the sidecar runtime — stores, SessionManager, HubLink — and
// returns a single start/close handle the host driver uses.
//
// The host supplies policy (the data directory, the low-level crypto
// primitives, the hub credentials, the deploy-router factory); the
// orchestrator does the composition. The multi-step deploy path
// forwards a spawned child's verified InferenceEvents to the hub
// through a sink the orchestrator owns: it points at a no-op closure
// until HubLink is constructed, then is rewired to hubLink.sendEvent,
// so the cross-reference is contained inside this module rather than
// leaking up to the host entry point.

import { getLogger } from "@intx/log";
import type { HubTransport } from "@intx/mail-memory";
import type { SignalKind } from "@intx/types";
import type {
  ApprovalSnapshot,
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
  type ReconnectScheduler,
} from "./ws/hub-link";

const log = getLogger(["interchange", "hub-agent", "orchestrator"]);

export type SidecarCryptoOps = {
  generateKeyPair(): Promise<KeyPair>;
  signEd25519(privateKey: Uint8Array, payload: Uint8Array): Promise<Uint8Array>;
  verifySSHSig(
    payload: string,
    signature: string,
    publicKey: Uint8Array,
  ): Promise<boolean>;
};

/**
 * Factory the orchestrator invokes once `sessions` and `keyStore`
 * are constructed. The host returns the `DeployRouter` the link
 * routes every inbound `agent.deploy` through; production wires
 * this against the sidecar's workflow-run deploy router. The host is
 * responsible for closing over any other state the router needs
 * (transport, substrate handle, signing keys) at the call site.
 */
export type CreateDeployRouter = (deps: {
  sessions: SessionManager;
  keyStore: AgentKeyStore;
  /**
   * Per-event sink the multi-step branch routes a spawned child's
   * verified `InferenceEvent`s through, keyed by the deployment's agent
   * address and the deploy's session id. Wired to the same hub-link
   * `agent.event` sink the in-process path's `onEvent` uses, so a step
   * agent's events reach the hub timeline keyed to the right session.
   * The `sessionId` is optional because a deploy frame need not carry
   * one (a headless deployment); the sink drops a sessionless event
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
    deploymentId: string;
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
   * Host-injected `DeployRouter` factory. The orchestrator calls it
   * once after `sessions` and `keyStore` are constructed; the
   * returned router routes every `agent.deploy` frame on the link.
   */
  createDeployRouter: CreateDeployRouter;
  /**
   * Optional pre-fallback mail dispatcher the link consults on every
   * inbound `mail.inbound` frame. Production wires this against the
   * sidecar's multi-step deployment mail handler registry so a
   * deployment-address inbound flows into the supervisor's mail-bus
   * subscription instead of the legacy session path. The orchestrator
   * forwards the binding unchanged to `createHubLink`.
   */
  mailInboundRouter?: MailInboundRouter;
  /**
   * Optional pre-fallback signal dispatcher the link consults on every
   * inbound `signal.deliver` frame. Production wires this against the
   * sidecar's multi-step deployment signal handler registry so a
   * deployment-address signal flows into the supervisor's
   * `deliverSignal`. The orchestrator forwards the binding unchanged
   * to `createHubLink`.
   */
  signalInboundRouter?: SignalInboundRouter;
  /**
   * Optional pre-fallback drain dispatcher the link consults on every
   * inbound `drain.deliver` frame. Production wires this against the
   * sidecar's multi-step deployment drain handler registry so a
   * deployment-address drain flows into the supervisor's `drain`. The
   * orchestrator forwards the binding unchanged to `createHubLink`.
   */
  drainInboundRouter?: DrainInboundRouter;
  /**
   * Optional inbound grants dispatcher the link consults on every inbound
   * `run.grants` frame. Production wires this against the sidecar's
   * multi-step deployment grants handler registry so a deployment-address
   * grants frame flows into the deployment's wiring, which writes the
   * run's grants to its `workflow-run` repo. The orchestrator forwards
   * the binding unchanged to `createHubLink`.
   */
  grantsInboundRouter?: GrantsInboundRouter;
  /**
   * Optional inbound sources-rotation dispatcher the link consults on
   * every inbound `sources.update` frame. Production wires this against
   * the sidecar's single-step deployment sources handler registry so a
   * deployment-address rotation flows into the supervisor's
   * `deliverSources`. The orchestrator forwards the binding unchanged to
   * `createHubLink`.
   */
  sourcesInboundRouter?: SourcesInboundRouter;
  /**
   * Returns the workflow-substrate deployment addresses this sidecar
   * currently hosts. Forwarded to the hub link, which announces them on
   * every (re)connect so the hub re-registers them for routing without a
   * challenge. Production wires this to the deploy router's
   * `activeAddresses`; omitted, the link announces none.
   */
  getWorkflowAddresses?: () => string[];
  /**
   * Invoked with the workflow-substrate addresses the link just answered a
   * reconnect challenge for. Forwarded to the hub link, which fires it once
   * per challenge so the workflow-run pack pusher can re-drive a push a
   * disconnect cancelled -- gated on the address becoming routable again.
   * Production wires this to the boot-edge pack-pushing store's
   * "address routable" notifier; omitted, the link fires nothing.
   */
  onWorkflowAddressesRoutable?: (addresses: string[]) => void;
  /**
   * Invoked on WS disconnect with the workflow-substrate addresses the link
   * hosts, so the workflow-run pack pusher blocks their pushes until the
   * reconnect challenge re-routes them. Paired with
   * `onWorkflowAddressesRoutable`. Production wires this to the boot-edge
   * pack-pushing store's block notifier; omitted, the link fires nothing.
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
    createDeployRouter,
    mailInboundRouter,
    signalInboundRouter,
    drainInboundRouter,
    grantsInboundRouter,
    sourcesInboundRouter,
    getWorkflowAddresses,
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
    signEd25519: cryptoOps.signEd25519,
    verifySSHSig: cryptoOps.verifySSHSig,
  });

  // Sink the multi-step deploy path routes a spawned child's verified
  // InferenceEvents through. It points at a no-op until HubLink is
  // constructed below, at which point it is swapped to the link's
  // sendEvent method.
  let dispatchEvent: (
    agentAddress: string,
    sessionId: string,
    event: InferenceEvent,
  ) => void = () => {
    /* replaced after HubLink construction */
  };

  // Sink the multi-step deploy path routes a supervisor's `park.notify`
  // suspension registration through. Points at a no-op until HubLink is
  // constructed below, at which point it is swapped to the link's
  // sendSignalCorrelationRegister method.
  let dispatchSuspension: (registration: {
    correlationId: string;
    runId: string;
    deploymentId: string;
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
    // Route a spawned child's verified InferenceEvents up the same
    // hub-link `agent.event` sink the in-process path uses, so step
    // agent events reach the hub timeline keyed to the deploy's
    // session. `dispatchEvent` is a no-op until HubLink is constructed
    // below; the closure reads it lazily so the post-construction swap
    // is observed. A sessionless event is dropped rather than guessed
    // onto an arbitrary session -- the hub timeline is session-keyed and
    // a forged session id would mis-route the event.
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
    // `dispatchSuspension` is a no-op until HubLink is constructed below; the
    // closure reads it lazily so the post-construction swap is observed.
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
    deployRouter,
    ...(mailInboundRouter !== undefined ? { mailInboundRouter } : {}),
    ...(signalInboundRouter !== undefined ? { signalInboundRouter } : {}),
    ...(drainInboundRouter !== undefined ? { drainInboundRouter } : {}),
    ...(grantsInboundRouter !== undefined ? { grantsInboundRouter } : {}),
    ...(sourcesInboundRouter !== undefined ? { sourcesInboundRouter } : {}),
    ...(getWorkflowAddresses !== undefined ? { getWorkflowAddresses } : {}),
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
