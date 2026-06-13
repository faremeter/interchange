import fs from "node:fs/promises";
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
import { createSidecarOrchestrator, type HubLink } from "@intx/hub-agent";
import { createAgentRepoStore } from "@intx/hub-sessions";
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
import {
  createDeploymentAddressRegistry,
  createWorkflowRunPackClient,
  createWorkflowRunPackPushingRepoStore,
} from "./workflow-run-pack-client";

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

// Load or mint the sidecar's local Ed25519 keypair. The supervisor
// principal signs every workflow-run commit with this key; the
// substrate's `signingCallback` signs every SSH-signed commit with
// it; the workflow-host child's substrate factory re-uses it via the
// `SIDECAR_SIGNING_*` spawn-time env vars. One key, one identity for
// the sidecar process.
const SIDECAR_SIGNING_DIR = path.join(dataDir, ".sidecar-signing");
const SIDECAR_PRIVATE_KEY_PATH = path.join(
  SIDECAR_SIGNING_DIR,
  "ed25519.private",
);
const SIDECAR_PUBLIC_KEY_PATH = path.join(
  SIDECAR_SIGNING_DIR,
  "ed25519.public",
);

async function loadOrMintSidecarKeypair(): Promise<{
  publicKey: Uint8Array;
  privateKey: Uint8Array;
}> {
  let havePriv = false;
  let havePub = false;
  try {
    await fs.access(SIDECAR_PRIVATE_KEY_PATH);
    havePriv = true;
  } catch {
    havePriv = false;
  }
  try {
    await fs.access(SIDECAR_PUBLIC_KEY_PATH);
    havePub = true;
  } catch {
    havePub = false;
  }
  if (havePriv !== havePub) {
    throw new Error(
      `sidecar signing keypair under ${SIDECAR_SIGNING_DIR} is partial: privateKey=${String(havePriv)} publicKey=${String(havePub)}; remove the directory to reset`,
    );
  }
  if (havePriv && havePub) {
    const [priv, pub] = await Promise.all([
      fs.readFile(SIDECAR_PRIVATE_KEY_PATH),
      fs.readFile(SIDECAR_PUBLIC_KEY_PATH),
    ]);
    return {
      privateKey: new Uint8Array(priv),
      publicKey: new Uint8Array(pub),
    };
  }
  const keyPair = await generateKeyPair();
  await fs.mkdir(SIDECAR_SIGNING_DIR, { recursive: true });
  await Promise.all([
    fs.writeFile(SIDECAR_PRIVATE_KEY_PATH, keyPair.privateKey, {
      mode: 0o600,
    }),
    fs.writeFile(SIDECAR_PUBLIC_KEY_PATH, keyPair.publicKey),
  ]);
  return keyPair;
}

const sidecarSigningKey = await loadOrMintSidecarKeypair();

// Construct the substrate-backed RepoStore at the boot edge. The
// supervisor consumes this through the deploy router; the trivial
// branch's `recordRunEvent` reaches `writeTreePreservingPrefix` on
// this store. The boot-edge facade below wraps the store so a
// successful workflow-run write fires the pack push hook before its
// Promise resolves.
const agentRepoStore = createAgentRepoStore({
  dataDir,
  signingKey: sidecarSigningKey,
});

// The deploy router records `(deploymentId -> agentAddress)` here on
// every inbound `agent.deploy`; the facade resolves the mapping when
// firing the pack push so the outbound frames carry the right
// agentAddress for hub-side routing.
const deploymentAddressRegistry = createDeploymentAddressRegistry();

const transport = createInMemoryTransport();

// The pack-push client closes over the substrate (for `createPack`)
// and a lazy hub-link binding (for `pushWorkflowRunPack`). The link
// reference is set once the orchestrator is constructed below; the
// closure here is consulted lazily because the
// `createSidecarOrchestrator` factory calls `createDeployRouter`
// during its constructor, before the orchestrator handle is bound.
let resolvedHubLink: HubLink | null = null;
const workflowRunPackClient = createWorkflowRunPackClient({
  substrate: agentRepoStore.repoStore,
  hubLink: {
    pushWorkflowRunPack(opts) {
      if (resolvedHubLink === null) {
        throw new Error(
          "sidecar boot: workflow-run pack push attempted before hub link was constructed",
        );
      }
      return resolvedHubLink.pushWorkflowRunPack(opts);
    },
  },
});

// Wrap the substrate's RepoStore with the boot-edge facade so a
// successful supervisor write against a workflow-run repo fires the
// pack push hook before its Promise resolves. Non-workflow-run writes
// (today, the agent-state deploy-applier path) flow through
// unchanged.
const wrappedRepoStore = createWorkflowRunPackPushingRepoStore({
  underlying: agentRepoStore.repoStore,
  packClient: workflowRunPackClient,
  registry: deploymentAddressRegistry,
});

const hubWsUrl = requireEnv("HUB_WS_URL");
const sidecarId = requireEnv("SIDECAR_ID");
const sidecarToken = requireEnv("SIDECAR_TOKEN");

// Multi-step substrate-config the deploy router threads into the
// workflow-process child's spawn-time env. The child's substrate
// factory consumes these via the typed `SubstrateConfig` validator so
// the per-step pack-push wrap can identify the deployment's hub-side
// trust anchors. Today the IPC bridge in `pack.push.request` carries
// the pack; the WebSocket-connection keys are reserved for a future
// child-local hub link without redoing the boot-edge wiring.
//
// `PATH`, `HOME`, and `TMPDIR` are propagated from the boot edge's own
// environment so the child's `#!/usr/bin/env bun` shebang can resolve
// `bun`, agent code can find a writable home, and tmp-file APIs land
// on the same temp root the host uses. The substrate-config validator
// ignores undeclared keys, so these are visible to the OS for binary
// lookup but invisible to the typed `SubstrateConfig` shape.
const multistepSubstrateEnv: Record<string, string> = {
  SIDECAR_DATA_DIR: dataDir,
  SIDECAR_SIGNING_PUBLIC_KEY: Buffer.from(sidecarSigningKey.publicKey).toString(
    "hex",
  ),
  SIDECAR_SIGNING_PRIVATE_KEY: Buffer.from(
    sidecarSigningKey.privateKey,
  ).toString("hex"),
  HUB_WS_URL: hubWsUrl,
  SIDECAR_ID: sidecarId,
  SIDECAR_TOKEN: sidecarToken,
  PATH: requireEnv("PATH"),
};
const hostHome = process.env["HOME"];
if (hostHome !== undefined) {
  multistepSubstrateEnv["HOME"] = hostHome;
}
const hostTmpdir = process.env["TMPDIR"];
if (hostTmpdir !== undefined) {
  multistepSubstrateEnv["TMPDIR"] = hostTmpdir;
}

const orchestrator = createSidecarOrchestrator({
  hubURL: hubWsUrl,
  sidecarId,
  token: sidecarToken,
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
      repoStore: wrappedRepoStore,
      signingKeySeed: sidecarSigningKey.privateKey,
      registerDeployment: ({ deploymentId, agentAddress }) => {
        deploymentAddressRegistry.record(deploymentId, agentAddress);
      },
      multistepSubstrateEnv,
      // The multi-step supervisor forwards `pack.push.request` upstream
      // control frames into this closure; the boot edge resolves them
      // through the same `HubLink.pushWorkflowRunPack` the
      // trivial-path facade consults. The closure mirrors the lazy
      // pattern used by `workflowRunPackClient` so a deploy that lands
      // before the orchestrator's `hubLink` is bound surfaces a
      // structured error rather than a `null` deref.
      multistepPushWorkflowRunPack: (opts) => {
        if (resolvedHubLink === null) {
          throw new Error(
            "sidecar boot: multi-step workflow-run pack push attempted before hub link was constructed",
          );
        }
        return resolvedHubLink.pushWorkflowRunPack(opts);
      },
    }),
});

resolvedHubLink = orchestrator.hubLink;

orchestrator.start();

// Keep `createSidecarWorkflowSupervisor` reachable from this entry
// point so a deploy handler that branches on workflow kind can
// instantiate a supervisor per active deployment without forking the
// boot path. The full wiring -- agent.deploy → kind detect →
// supervisor.spawn -- threads through the deploy handler in a later
// commit. The reference here is the seam.
export { createSidecarWorkflowSupervisor };
