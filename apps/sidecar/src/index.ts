import { setup, getLogger } from "@intx/log";
import { createInMemoryTransport } from "@intx/mail-memory";
import type { ConnectorThreadState, InferenceEvent } from "@intx/types/runtime";
import { createNodeCrypto, generateKeyPair } from "@intx/crypto-node";
import {
  createAgentKeyStore,
  createAgentRepoStore,
  createSessionManager,
} from "@intx/hub-agent";

import { createDefaultHarnessBuilder } from "./default-harness";
import { createWsClient } from "./ws-client";

await setup();

const log = getLogger(["sidecar"]);

const hubUrl = process.env["HUB_WS_URL"];
if (hubUrl === undefined) {
  throw new Error("HUB_WS_URL environment variable is required");
}

const sidecarId = process.env["SIDECAR_ID"];
if (sidecarId === undefined) {
  throw new Error("SIDECAR_ID environment variable is required");
}

const token = process.env["SIDECAR_TOKEN"];
if (token === undefined) {
  throw new Error("SIDECAR_TOKEN environment variable is required");
}

const dataDir = process.env["SIDECAR_DATA_DIR"];
if (dataDir === undefined) {
  throw new Error("SIDECAR_DATA_DIR environment variable is required");
}

const transport = createInMemoryTransport();

// Break the circular dependency between session manager and ws-client:
// the session manager fires events through this mutable reference,
// which gets pointed at the real client after both are constructed.
let forwardEvent: (a: string, s: string, e: InferenceEvent) => void = (
  _a,
  _s,
  _e,
) => {
  // Replaced after ws-client is constructed.
};
let forwardConnectorState: (
  a: string,
  state: ConnectorThreadState | null,
) => void = (_a, _state) => {
  // Replaced after ws-client is constructed.
};

const repoStore = createAgentRepoStore({ dataDir });
const keyStore = createAgentKeyStore({ dataDir, generateKeyPair });
const buildHarness = createDefaultHarnessBuilder();

const sessions = createSessionManager({
  transport,
  repoStore,
  keyStore,
  buildHarness,
  createAgentCrypto: createNodeCrypto,
  onEvent(agentAddress, sessionId, event) {
    forwardEvent(agentAddress, sessionId, event);
  },
  onConnectorStateChanged(agentAddress, state) {
    forwardConnectorState(agentAddress, state);
  },
});

const client = createWsClient({
  hubUrl,
  sidecarId,
  token,
  transport,
  sessions,
});

forwardEvent = client.sendEvent;
forwardConnectorState = client.sendConnectorState;
client.connect();

log.info("Sidecar {sidecarId} connecting to {hubUrl}", { sidecarId, hubUrl });
