import { sign as nodeSign } from "node:crypto";
import { setup, getLogger } from "@intx/log";
import { createInMemoryTransport } from "@intx/mail-memory";
import type { ConnectorThreadState, InferenceEvent } from "@intx/types/runtime";
import {
  createNodeCrypto,
  generateKeyPair,
  importPrivateKeyBytes,
  verifySshSignature,
} from "@intx/crypto-node";
import {
  createAgentKeyStore,
  createAgentRepoStore,
  createHubLink,
  createSessionManager,
  type HubLinkLookups,
} from "@intx/hub-agent";

import { createDefaultHarnessBuilder } from "./default-harness";

await setup();

const log = getLogger(["sidecar"]);

const hubURL = process.env["HUB_WS_URL"];
if (hubURL === undefined) {
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

// Break the circular dependency between SessionManager and HubLink:
// the session manager fires events through this mutable reference,
// which gets pointed at the real link after both are constructed.
let forwardEvent: (a: string, s: string, e: InferenceEvent) => void = (
  _a,
  _s,
  _e,
) => {
  // Replaced after HubLink is constructed.
};
let forwardConnectorState: (
  a: string,
  state: ConnectorThreadState | null,
) => void = (_a, _state) => {
  // Replaced after HubLink is constructed.
};

const lookups: HubLinkLookups = {
  signEd25519(privateKey, payload) {
    const key = importPrivateKeyBytes(privateKey);
    return new Uint8Array(nodeSign(null, payload, key));
  },
  verifySshSig(payload, signature, publicKey) {
    return verifySshSignature(payload, signature, publicKey);
  },
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

const hubLink = createHubLink({
  hubURL,
  sidecarId,
  token,
  transport,
  sessions,
  lookups,
});

forwardEvent = hubLink.sendEvent;
forwardConnectorState = hubLink.sendConnectorState;
hubLink.connect();

log.info("Sidecar {sidecarId} connecting to {hubURL}", { sidecarId, hubURL });
