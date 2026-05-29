// SidecarOrchestrator: constructs and wires every package-side piece
// of the sidecar runtime — stores, SessionManager, HubLink — and
// returns a single start/close handle the host driver uses.
//
// The host supplies policy (the data directory, the harness builder
// implementation, the per-agent crypto factory, the low-level crypto
// primitives, the hub credentials); the orchestrator does the
// composition. The forwardEvent / forwardConnectorState callback shim
// that lived in the host entry point before this commit is now
// encapsulated here: SessionManager's event sinks dispatch to a
// closure the orchestrator points at hubLink.sendEvent and
// hubLink.sendConnectorState immediately after both components exist.

import { getLogger } from "@intx/log";
import type { InMemoryTransport } from "@intx/mail-memory";
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
  type HubLink,
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

export type SidecarOrchestratorConfig = {
  hubURL: string;
  sidecarId: string;
  token: string;
  dataDir: string;
  transport: InMemoryTransport;
  buildHarness: HarnessBuilder;
  createAgentCrypto: (keyPair: KeyPair) => CryptoProvider;
  cryptoOps: SidecarCryptoOps;
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
  // constructed, at which point they're swapped to the link's
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
  });

  const hubLink = createHubLink({
    hubURL,
    sidecarId,
    token,
    transport,
    sessions,
    keyStore,
    ...(pingIntervalMs !== undefined ? { pingIntervalMs } : {}),
    ...(reconnectDelayMs !== undefined ? { reconnectDelayMs } : {}),
    ...(scheduleReconnect !== undefined ? { scheduleReconnect } : {}),
  });

  dispatchEvent = hubLink.sendEvent;
  dispatchConnectorState = hubLink.sendConnectorState;

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
