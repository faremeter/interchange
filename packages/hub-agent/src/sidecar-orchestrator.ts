// SidecarOrchestrator: constructs and wires every package-side piece
// of the sidecar runtime — stores, SessionManager, HubLink — and
// returns a single start/close handle the host driver uses.
//
// The host supplies policy (the data directory, the harness builder
// implementation, the per-agent crypto factory, the low-level crypto
// primitives, the hub credentials); the orchestrator does the
// composition. SessionManager and HubLink reference each other through
// SessionEventSink / ConnectorStateSink callbacks, but SessionManager
// is constructed first so the sinks initially point at no-op closures
// the orchestrator owns. After HubLink is constructed those closures
// are rewired to hubLink.sendEvent and hubLink.sendConnectorState, so
// the cross-reference is contained inside this module rather than
// leaking up to the host entry point.

import { getLogger } from "@intx/log";
import type { HubTransport } from "@intx/mail-memory";
import type { DeployApplyErrorFrame } from "@intx/types/sidecar";
import type {
  ConnectorThreadState,
  CryptoProvider,
  InferenceEvent,
  KeyPair,
} from "@intx/types/runtime";

import { createAgentKeyStore, type AgentKeyStore } from "./agent-key-store";
import { createAgentRepoStore, type AgentRepoStore } from "./agent-repo-store";
import { createSessionManager, type SessionManager } from "./session-manager";
import type { HarnessBuilder } from "./harness-builder";
import {
  createHubLink,
  type DeployRouter,
  type HubLink,
  type MailInboundRouter,
  type SignalInboundRouter,
  type DrainInboundRouter,
  type ReconnectScheduler,
} from "./ws/hub-link";

const log = getLogger(["interchange", "hub-agent", "orchestrator"]);

export type SidecarCryptoOps = {
  generateKeyPair(): Promise<KeyPair>;
  signEd25519(privateKey: Uint8Array, payload: Uint8Array): Uint8Array;
  verifySSHSig(
    payload: string,
    signature: string,
    publicKey: Uint8Array,
  ): boolean;
};

/**
 * Factory the orchestrator invokes once `sessions` and `keyStore`
 * are constructed. The host returns the `DeployRouter` the link
 * routes every inbound `agent.deploy` through; production wires
 * this against a workflow-host supervisor whose `trivialLaunch`
 * closes over `sessions.provisionAgent`. The host is responsible
 * for closing over any other state the router needs (transport,
 * substrate handle, signing keys) at the call site.
 *
 * `onAgentEvent` is the per-agent InferenceEvent subscription seam
 * the trivial-launch closure uses to bracket the workflow-run event
 * chain against the existing reactor moments
 * (`message.run.started` / `inference.start` / `message.run.ended`).
 * The orchestrator hands it through unchanged so the supervisor's
 * `recordRunEvent` callback fires for the right address.
 */
export type CreateDeployRouter = (deps: {
  sessions: SessionManager;
  keyStore: AgentKeyStore;
  onAgentEvent: SessionManager["onAgentEvent"];
}) => DeployRouter;

export type SidecarOrchestratorConfig = {
  hubURL: string;
  sidecarId: string;
  token: string;
  dataDir: string;
  transport: HubTransport;
  buildHarness: HarnessBuilder;
  createAgentCrypto: (keyPair: KeyPair) => CryptoProvider;
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
    buildHarness,
    createAgentCrypto,
    cryptoOps,
    createDeployRouter,
    mailInboundRouter,
    signalInboundRouter,
    drainInboundRouter,
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

  // Pre-declare the sinks. SessionManager dispatches events into them
  // synchronously; the closures point at no-ops until HubLink is
  // constructed below, at which point they are swapped to the link's
  // sendEvent / sendConnectorState methods.
  let dispatchEvent: (
    agentAddress: string,
    sessionId: string,
    event: InferenceEvent,
  ) => void = () => {
    /* replaced after HubLink construction */
  };
  let dispatchConnectorState: (
    agentAddress: string,
    state: ConnectorThreadState | null,
  ) => void = () => {
    /* replaced after HubLink construction */
  };
  let dispatchDeployApplyError: (
    agentAddress: string,
    payload: Omit<DeployApplyErrorFrame, "type" | "agentAddress">,
  ) => void = () => {
    /* replaced after HubLink construction */
  };

  const sessions = createSessionManager({
    transport,
    repoStore,
    keyStore,
    buildHarness,
    createAgentCrypto,
    onEvent(agentAddress, sessionId, event) {
      dispatchEvent(agentAddress, sessionId, event);
    },
    onConnectorStateChanged(agentAddress, state) {
      dispatchConnectorState(agentAddress, state);
    },
    onDeployApplyError(agentAddress, payload) {
      dispatchDeployApplyError(agentAddress, payload);
    },
  });

  const deployRouter = createDeployRouter({
    sessions,
    keyStore,
    onAgentEvent: sessions.onAgentEvent,
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
    ...(pingIntervalMs !== undefined ? { pingIntervalMs } : {}),
    ...(reconnectDelayMs !== undefined ? { reconnectDelayMs } : {}),
    ...(scheduleReconnect !== undefined ? { scheduleReconnect } : {}),
  });

  dispatchEvent = hubLink.sendEvent;
  dispatchConnectorState = hubLink.sendConnectorState;
  dispatchDeployApplyError = hubLink.sendDeployApplyError;

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
