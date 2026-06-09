import { sign as nodeSign } from "node:crypto";
import path from "node:path";
import { setup } from "@intx/log";
import { createInMemoryTransport } from "@intx/mail-memory";
import {
  createNodeCrypto,
  generateKeyPair,
  importPrivateKeyBytes,
  verifySSHSignature,
} from "@intx/crypto-node";
import { createSidecarOrchestrator } from "@intx/hub-agent";
import { createTarballCache } from "@intx/tool-packaging";

import { readCacheMaxBytes, readRegistryMaxTarballBytes } from "./config";
import { createDefaultHarnessBuilder } from "./default-harness";

await setup();

function requireEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined) {
    throw new Error(`${name} environment variable is required`);
  }
  return value;
}

const dataDir = requireEnv("SIDECAR_DATA_DIR");

// Resolve cache configuration at the boot edge so the per-apply loader
// inside the harness builder receives a concrete path and cap rather
// than re-reading env at non-boundary call sites.
const sidecarCacheDir = process.env["SIDECAR_CACHE_DIR"];
const cacheRoot =
  sidecarCacheDir !== undefined && sidecarCacheDir.trim() !== ""
    ? sidecarCacheDir
    : path.join(dataDir, "cache", "tarballs");
const cacheMaxBytes = readCacheMaxBytes();
const registryMaxTarballBytes = readRegistryMaxTarballBytes();

// Sweep any tmp staging directories left behind by a `put` or
// `extractTarball` that crashed between staging and the final rename
// on a previous boot. Running here, before the orchestrator starts
// accepting apply work, keeps the cache root from accumulating
// orphans for the lifetime of the sidecar's data directory.
await createTarballCache({
  rootDir: cacheRoot,
  maxBytes: cacheMaxBytes,
}).sweepOrphans();

const orchestrator = createSidecarOrchestrator({
  hubURL: requireEnv("HUB_WS_URL"),
  sidecarId: requireEnv("SIDECAR_ID"),
  token: requireEnv("SIDECAR_TOKEN"),
  dataDir,
  transport: createInMemoryTransport(),
  buildHarness: createDefaultHarnessBuilder({
    cacheRoot,
    cacheMaxBytes,
    registryMaxTarballBytes,
  }),
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
