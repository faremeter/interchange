import { sign as nodeSign } from "node:crypto";
import { setup } from "@intx/log";
import { createInMemoryTransport } from "@intx/mail-memory";
import {
  createNodeCrypto,
  generateKeyPair,
  importPrivateKeyBytes,
  verifySSHSignature,
} from "@intx/crypto-node";
import { createSidecarOrchestrator } from "@intx/hub-agent";

import { createDefaultHarnessBuilder } from "./default-harness";

await setup();

function requireEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined) {
    throw new Error(`${name} environment variable is required`);
  }
  return value;
}

const orchestrator = createSidecarOrchestrator({
  hubURL: requireEnv("HUB_WS_URL"),
  sidecarId: requireEnv("SIDECAR_ID"),
  token: requireEnv("SIDECAR_TOKEN"),
  dataDir: requireEnv("SIDECAR_DATA_DIR"),
  transport: createInMemoryTransport(),
  buildHarness: createDefaultHarnessBuilder(),
  createAgentCrypto: createNodeCrypto,
  cryptoOps: {
    generateKeyPair,
    signEd25519(privateKey, payload) {
      const key = importPrivateKeyBytes(privateKey);
      return new Uint8Array(nodeSign(null, payload, key));
    },
    verifySSHSig: verifySSHSignature,
  },
});

orchestrator.start();
