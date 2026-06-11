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
import type { RepoStore } from "@intx/hub-sessions";
import { createTarballCache } from "@intx/tool-packaging";

import { readCacheMaxBytes, readRegistryMaxTarballBytes } from "./config";
import { createDefaultHarnessBuilder } from "./default-harness";
// Pull the workflow-host wiring factory into the sidecar's module
// graph so the supervisor surface is reachable from this binary.
// `createSidecarDeployRouter` is the production routing the
// orchestrator hands to the link's `agent.deploy` handler; every
// inbound frame flows through a freshly-constructed workflow-host
// supervisor whose trivial branch calls back into the sidecar's
// existing single-agent provisioning surface.
import {
  createSidecarDeployRouter,
  createSidecarWorkflowSupervisor,
} from "./workflow-host-wiring";

await setup();

/**
 * Substrate-RepoStore placeholder for the trivial-only routing the
 * production sidecar exercises today. The workflow-host supervisor
 * accepts a substrate `RepoStore` in its bindings; the trivial
 * branch never reaches into it (no `requestCancel`, no `spawn`).
 * The proxy below throws on any access so a future code path that
 * does reach in surfaces a precise failure rather than a silent
 * miss. Substrate plumbing for the multi-step branch lands in a
 * separate commit.
 */
function createTrivialOnlyRepoStorePlaceholder(): RepoStore {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- placeholder; every access throws via the proxy
  return new Proxy({} as RepoStore, {
    get(_target, prop) {
      return () => {
        throw new Error(
          `sidecar trivial-only RepoStore placeholder: ${String(prop)} invoked; multi-step substrate plumbing not yet wired`,
        );
      };
    },
  });
}

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

const transport = createInMemoryTransport();

const orchestrator = createSidecarOrchestrator({
  hubURL: requireEnv("HUB_WS_URL"),
  sidecarId: requireEnv("SIDECAR_ID"),
  token: requireEnv("SIDECAR_TOKEN"),
  dataDir,
  transport,
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
  createDeployRouter: ({ sessions, keyStore, onAgentEvent }) =>
    createSidecarDeployRouter({
      sessions,
      keyStore,
      onAgentEvent,
      transport,
      // Trivial-branch routing does not touch the substrate
      // RepoStore (the workflow-host supervisor only calls into
      // it on the multi-step `spawn` / `requestCancel` paths).
      // The production sidecar does not yet wire a substrate
      // handle into its boot edge; the placeholder below throws
      // on any access so a future code path that depends on it
      // surfaces a precise error rather than a silent miss. The
      // multi-step branch lands with the substrate-handle plumb
      // in a separate commit.
      repoStore: createTrivialOnlyRepoStorePlaceholder(),
      signingKeySeed: new Uint8Array(32),
    }),
});

orchestrator.start();

// Keep `createSidecarWorkflowSupervisor` reachable from this entry
// point so a deploy handler that branches on workflow kind can
// instantiate a supervisor per active deployment without forking the
// boot path. The full wiring -- agent.deploy → kind detect →
// supervisor.spawn -- threads through the deploy handler in a later
// commit. The reference here is the seam.
export { createSidecarWorkflowSupervisor };
