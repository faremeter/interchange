import { appendFileSync } from "node:fs";
import path from "node:path";
import { setup } from "@intx/log";
import { createInMemoryTransport } from "@intx/mail-memory";
import {
  createEd25519Crypto,
  createEnvKeyCredentialCipher,
  generateKeyPair,
  verifySSHSignature,
} from "@intx/crypto";
import {
  createSenderKeyCache,
  createSenderCryptoResolver,
  createInboundMailPolicyRegistry,
  createInboundMailPolicyLookup,
  createSidecarOrchestrator,
  MAX_INLINE_ASSET_PAYLOAD_BYTES,
  materializeWorkflowAssets,
  resolveInboundMailPolicy,
  sourceAssetGitDir,
  type HubLink,
} from "@intx/hub-agent";
import { hexDecode, hexEncode } from "@intx/types";
import {
  createAgentRepoStore,
  parseAgentId,
  workflowSourceAssetMountPath,
} from "@intx/hub-sessions";
import {
  applyFrozenWorkflowClosure,
  createTarballCache,
  createWorkflowClosureMaterializer,
} from "@intx/tool-packaging";
import {
  deriveWorkflowRunRepoId,
  inertFlatNamespaceStepIds,
} from "@intx/workflow-deploy";

import { loadAdapterRegistry } from "@intx/inference/providers";

import {
  parseReconnectDelayMs,
  readAdapterManifest,
  readCacheMaxBytes,
  readCredentialEncryptionKey,
  readRegistryMaxTarballBytes,
} from "./config";
import { createDefaultHarnessBuilder } from "./default-harness";
// Production `agent.deploy` routing: every inbound deploy frame stages
// through the workflow-run substrate under a supervised child process.
import type { DispatchTimingMark } from "@intx/workflow-host";
import {
  createDeploymentAddressRegistry,
  createMultistepCredentialsRouter,
  createMultistepDrainRouter,
  createMultistepGrantsRouter,
  createMultistepMailRouter,
  createMultistepSignalRouter,
  createMultistepSourcesRouter,
  createSidecarDeployRouter,
  createWorkflowRunPackClient,
  createWorkflowRunPackPushingRepoStore,
  createWorkflowRunPackRestorer,
  removeFileAtomicDurable,
  writeFileAtomicDurable,
  type SidecarDeployRouter,
} from "@intx/workflow-host/deploy";
import { createWorkflowProbeExecutor } from "@intx/workflow-host/probe";

import {
  readRegistries,
  resolveHostPlatform,
} from "./sidecar-materialization-config";
import { loadOrMintSidecarKeypair } from "./signing-keypair";
import {
  defaultSubprocessSpawner,
  SIDECAR_WORKFLOW_CHILD_BINARY,
} from "./workflow-child-spawner";
import {
  defaultProbeChildSpawner,
  SIDECAR_WORKFLOW_PROBE_CHILD_BINARY,
} from "./workflow-probe-spawner";

await setup();

function requireEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined) {
    throw new Error(`${name} environment variable is required`);
  }
  return value;
}

const dataDir = requireEnv("SIDECAR_DATA_DIR");

// Seals at-rest credential material (run-record apiKeys, tool credential
// store) under this operator key. Built here so a missing or malformed
// key fails at boot, not at first persist.
const credentialCipher = createEnvKeyCredentialCipher(
  readCredentialEncryptionKey(),
);

// Resolve cache config at the boot edge so the workflow child's loader
// gets concrete path and cap via its spawn env, not a re-read of env.
const sidecarCacheDir = process.env["SIDECAR_CACHE_DIR"];
const cacheRoot =
  sidecarCacheDir !== undefined && sidecarCacheDir.trim() !== ""
    ? sidecarCacheDir
    : path.join(dataDir, "cache", "tarballs");
const cacheMaxBytes = readCacheMaxBytes();
const registryMaxTarballBytes = readRegistryMaxTarballBytes();

// Custom inference adapters resolved once at boot: `loadAdapterRegistry`
// merges the statically-linked built-ins with any custom adapters the
// manifest names, importing each eagerly so a bad specifier fails here.
// The same registry backs the deploy router's source-admission check;
// the child rebuilds an equivalent one from the manifest serialized into
// its spawn env (see `multistepSubstrateEnv`), since the object cannot
// cross the fork. Specifiers are operator-config-only.
const adapterManifest = readAdapterManifest();
const adapters = await loadAdapterRegistry(adapterManifest);

// Phase 4.7 latency-gate hook: when `SIDECAR_LATENCY_BENCH_FILE` names a
// path, the deploy router wires the supervisor's `onDispatchTiming`
// observer to append one parseable line per per-message dispatch
// boundary. A file (not stdout) is the channel because the spawn fixture
// caps its stdout drain buffer. Unset leaves the observer unwired;
// observability-only, no control-flow effect. The synchronous append is
// the raw measurement channel: not formatted or level-gated, and a
// failed append surfaces rather than yielding an empty result set. Two
// line shapes, discriminated by `mark.kind`, both lead with the mail's
// Message-ID:
//   roundtrip:  `<messageId> <marker> <atMs>`
//   leg:        `<messageId> leg <leg> <phase> <atMs> [runsFanOut consumedFanOut looseObjects gitBytes]`
//               (trailing counters present only on the `end` phase). The
//               leg shape is a strict superset prefixed with the literal
//               `leg` token, so a reader can branch on field 2.
const latencyBenchFile = process.env["SIDECAR_LATENCY_BENCH_FILE"];
const onDispatchTiming: ((mark: DispatchTimingMark) => void) | undefined =
  latencyBenchFile !== undefined && latencyBenchFile.trim() !== ""
    ? (mark) => {
        let line: string;
        if (mark.kind === "roundtrip") {
          line = `${mark.messageId} ${mark.marker} ${mark.atMs.toFixed(3)}\n`;
        } else {
          const counters =
            mark.counters !== undefined
              ? ` ${String(mark.counters.runsFanOut)} ${String(mark.counters.consumedFanOut)} ${String(mark.counters.looseObjects)} ${String(mark.counters.gitBytes)}`
              : "";
          line = `${mark.messageId} leg ${mark.leg} ${mark.phase} ${mark.atMs.toFixed(3)}${counters}\n`;
        }
        appendFileSync(latencyBenchFile, line);
      }
    : undefined;

// D2 §10c forced-repack A/B toggle. When `SIDECAR_REPACK_EVERY_MESSAGES`
// names a positive integer, the supervisor forces a `git gc`/repack of the
// workflow-run repo every Nth dispatched message (under the single-writer
// discipline). Resolved at the boot edge; absent or non-positive => no
// `git gc` forks. Measurement-only: exists to discriminate pack-growth from
// tree-fan-out, never to run in production.
const repackEveryRaw = process.env["SIDECAR_REPACK_EVERY_MESSAGES"];
let repackEveryMessages: { everyMessages: number } | undefined;
if (repackEveryRaw !== undefined && repackEveryRaw.trim() !== "") {
  const parsed = Number.parseInt(repackEveryRaw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(
      `SIDECAR_REPACK_EVERY_MESSAGES must be a positive integer, got ${repackEveryRaw}`,
    );
  }
  repackEveryMessages = { everyMessages: parsed };
}

// Consumed-dedup retention horizon (ms), threaded into every supervisor
// the deploy router constructs; absent or empty => the supervisor's 24h
// default. The longest window in which the same message could
// legitimately be re-submitted and still be caught as a duplicate (the
// consumed/dedup index retains at least this long before the retention
// watermark prunes it). Must be >= the maximum redelivery window of any
// at-least-once mail source if one is ever added.
const consumedRetentionRaw = process.env["CONSUMED_RETENTION_MS"];
let consumedRetentionMs: number | undefined;
if (consumedRetentionRaw !== undefined && consumedRetentionRaw.trim() !== "") {
  const parsed = Number.parseInt(consumedRetentionRaw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(
      `CONSUMED_RETENTION_MS must be a positive integer (milliseconds), got ${consumedRetentionRaw}`,
    );
  }
  consumedRetentionMs = parsed;
}

// Bound on the child's spawn-time `ready` handshake, threaded to every
// per-deployment supervisor. On expiry the supervisor kills the child and
// rejects the spawn, so a child that spawns but never signals ready fails
// the deploy (or is skipped by boot-time restore) instead of hanging it.
// Absent => the supervisor's 30s default.
const readyTimeoutRaw = process.env["CHILD_READY_TIMEOUT_MS"];
let readyTimeoutMs: number | undefined;
if (readyTimeoutRaw !== undefined && readyTimeoutRaw.trim() !== "") {
  const parsed = Number.parseInt(readyTimeoutRaw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(
      `CHILD_READY_TIMEOUT_MS must be a positive integer (milliseconds), got ${readyTimeoutRaw}`,
    );
  }
  readyTimeoutMs = parsed;
}

// Hub-link reconnect backoff, resolved at the boot edge and forwarded to
// the orchestrator's hub link below. `parseReconnectDelayMs` owns the
// validation rule and the absent-means-link-default semantics.
const reconnectDelayMs = parseReconnectDelayMs(
  process.env["SIDECAR_RECONNECT_DELAY_MS"],
);

// Sweep tmp staging dirs left by a `put`/`extractTarball` that crashed
// before the final rename on a previous boot, before the orchestrator
// accepts apply work, so the cache root accumulates no orphans.
await createTarballCache({
  rootDir: cacheRoot,
  maxBytes: cacheMaxBytes,
}).sweepOrphans();

// Load or mint the sidecar's local Ed25519 keypair: the supervisor signs
// workflow-run commits with it, the substrate signs SSH commits, and the
// child's substrate factory reuses it via the `SIDECAR_SIGNING_*` spawn
// env. One key, one identity for the sidecar process.
const SIDECAR_SIGNING_DIR = path.join(dataDir, ".sidecar-signing");

const sidecarSigningKey = await loadOrMintSidecarKeypair(SIDECAR_SIGNING_DIR);

// Construct the substrate-backed RepoStore at the boot edge. The supervisor
// consumes this through the deploy router; the supervisor's
// conversation-state writes reach `writeTreePreservingPrefix` on this store.
// The boot-edge facade below wraps the store so a successful workflow-run
// write fires the pack push hook before its Promise resolves.
const agentRepoStore = createAgentRepoStore({
  dataDir,
  signingKey: sidecarSigningKey,
});

// Cache of hub-vouched public keys of senders this sidecar's deployments
// are authorized to receive mail from. The grants handler writes each key
// the hub co-delivers on a `run.grants` frame; inbound-mail verify reads
// it back. Loaded at boot so a restart keeps previously-cached keys. The
// injected durable-write primitive makes the cache write atomic and
// fsynced -- at least as durable as the run-grants write it gates.
const senderKeyCache = await createSenderKeyCache({
  dataDir,
  writeFileDurable: (filePath, contents) =>
    writeFileAtomicDurable(filePath, contents, { mode: 0o600 }),
  removeFileDurable: (filePath) => removeFileAtomicDurable(filePath),
});

// Read side of the same cache: inbound signature verify resolves a sender
// address to the crypto that verifies its mail. Built here so the hub
// link stays source-opaque.
const resolveSenderCrypto = createSenderCryptoResolver(senderKeyCache);

// Per-recipient-address registry of resolved inbound-mail admission
// policies. The deploy wiring registers each deployment's authored
// `inboundMailPolicy` beside its mail-router registration and removes it
// on teardown; the read side hands the hub link a total policy per
// address, keeping the link policy-source-opaque.
const inboundMailPolicyRegistry = createInboundMailPolicyRegistry();
const lookupInboundMailPolicy = createInboundMailPolicyLookup(
  inboundMailPolicyRegistry,
);

// The deploy router records `(runId -> agentAddress)` here on every inbound
// `agent.deploy`; the facade resolves the mapping when firing the pack push
// so the outbound frames carry the right agentAddress for hub-side routing.
const deploymentAddressRegistry = createDeploymentAddressRegistry();

// Per-deployment-address handler registries, one per inbound frame kind.
// Each is registered against a deployment's address once its supervisor
// spawns and forwards that frame kind into the supervisor; an address
// with no registered handler leaves the frame unrouted (logged/dropped).
// The deploy router's multi-step branch registers `wired.routeInbound`
// here; the hub link's `mail.inbound` handler calls `tryRoute` so an
// inbound deployment-address message lands on the mail-bus subscription.
const multistepMailRouter = createMultistepMailRouter();

// Forwards `signal.deliver` into the supervisor's `deliverSignal` (a
// `signal.deliver` control IPC frame to the child); the child commits the
// resulting `SignalReceived` event through its own substrate -- the
// single writer of the workflow-run repo on the sidecar side.
const multistepSignalRouter = createMultistepSignalRouter();

// Forwards `drain.deliver` into the supervisor's `drain` (a `drain` IPC
// frame to the child) and arms one drainTimeout accumulator per in-flight
// run. Cancel-mode in-flight steps abort on the child side; wait-mode
// steps continue. Accumulators commit a signed `CancelRequested{origin:
// "supervisor-drain"}` against the workflow-run repo when the deadline
// expires.
const multistepDrainRouter = createMultistepDrainRouter();

// Writes the run's grants to `runs/<runId>/grants.json`, sibling to that
// run's `runs/<runId>/events/` subtree; the write is awaited so the
// frame's FIFO completion means the grants are durable on disk.
const multistepGrantsRouter = createMultistepGrantsRouter();

// Only a single-step warm deployment registers one; the handler forwards
// the rotated list into the supervisor's `deliverSources` (a
// `sources-updated` control IPC frame to the child), swapping the warm
// agent's live sources in place. A multi-step deployment registers none,
// so a rotation against its address is unrouted.
const multistepSourcesRouter = createMultistepSourcesRouter();
// Any deployment with a supervisor registers its handler after `spawn`;
// a `credentials.update` for an unregistered address is unrouted.
const multistepCredentialsRouter = createMultistepCredentialsRouter();

const transport = createInMemoryTransport();

// Pack-push client over the substrate (createPack) with a lazy hub-link
// binding (pushWorkflowRunPack): the link is set below because
// `createSidecarOrchestrator` builds the deploy router during its
// constructor, before the orchestrator handle exists.
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
const restoreWorkflowRunPack = createWorkflowRunPackRestorer({
  // Restore into the unwrapped substrate; the push facade would echo the
  // pack straight back to the Hub as a new supervisor write.
  substrate: agentRepoStore.repoStore,
  markRestored: workflowRunPackClient.markRestored,
  deriveWorkflowRunRepoId,
});

// Wrap the substrate's RepoStore with the pack-push facade so a
// successful workflow-run write fires the push hook before its Promise
// resolves. Non-workflow-run writes (the agent-state deploy-applier path)
// flow through unchanged.
const wrappedRepoStore = createWorkflowRunPackPushingRepoStore({
  underlying: agentRepoStore.repoStore,
  packClient: workflowRunPackClient,
  registry: deploymentAddressRegistry,
  deriveWorkflowRunRepoId,
});

const hubWsUrl = requireEnv("HUB_WS_URL");
const sidecarId = requireEnv("SIDECAR_ID");
const sidecarToken = requireEnv("SIDECAR_TOKEN");

// Substrate config threaded into the workflow child's spawn env: the
// child's factory consumes it via the typed `SubstrateConfig` validator
// so the per-step pack-push wrap can identify the deployment's hub-side
// trust anchors (the IPC bridge in `pack.push.request` carries the pack
// today; the connection keys are reserved for a future child-local link).
// `PATH`/`HOME`/`TMPDIR` propagate from this process's env so the
// `#!/usr/bin/env bun` shebang resolves, agent code finds a writable
// home, and tmp APIs land on the host's temp root; the validator ignores
// undeclared keys, so they stay invisible to the typed shape.
const multistepSubstrateEnv: Record<string, string> = {
  SIDECAR_DATA_DIR: dataDir,
  SIDECAR_SIGNING_PUBLIC_KEY: hexEncode(sidecarSigningKey.publicKey),
  SIDECAR_SIGNING_PRIVATE_KEY: hexEncode(sidecarSigningKey.privateKey),
  HUB_WS_URL: hubWsUrl,
  SIDECAR_ID: sidecarId,
  SIDECAR_TOKEN: sidecarToken,
  PATH: requireEnv("PATH"),
  // Tool-loader caps for the child's per-step tool materialization,
  // threaded in so the child does not re-read env.
  SIDECAR_CACHE_MAX_BYTES: String(cacheMaxBytes),
  SIDECAR_REGISTRY_MAX_TARBALL_BYTES: String(registryMaxTarballBytes),
  // Serialize the already-validated manifest object, not the raw env
  // string, so the child rebuilds the same adapter set. Always present
  // (defaults to "[]"); the child treats a missing key as a
  // serialization bug and re-validates the shape before importing, since
  // the child env is operator-controlled via Bun.spawn.
  SIDECAR_ADAPTER_MANIFEST: JSON.stringify(adapterManifest),
};
const hostHome = process.env["HOME"];
if (hostHome !== undefined) {
  multistepSubstrateEnv["HOME"] = hostHome;
}
const hostTmpdir = process.env["TMPDIR"];
if (hostTmpdir !== undefined) {
  multistepSubstrateEnv["TMPDIR"] = hostTmpdir;
}

// The deploy router's source-admission gate reuses this exact
// `canBuildSource` predicate, against the one adapter registry, rather
// than a second copy of the check.
const buildHarness = createDefaultHarnessBuilder({ adapters });

// Airlocked workflow-probe executor injected through the orchestrator so
// the live sidecar answers `workflow.probe.request` with a real inert
// projection instead of the hub link's rejecting placeholder. The
// host-side `MaterializeWorkflowClosure` lays a probe frame's frozen
// closure under a per-probe scratch dir rooted here (shares the data
// dir's lifecycle); the executor spawns the one-shot child that evaluates
// the workflow entry against it. Probe source assets ride inline in one
// frame; a git-sourced asset past the shared inline-payload cap is the
// signal to move probe delivery to the streamed transfer the deploy path
// uses.
const workflowProbeExecutor = createWorkflowProbeExecutor({
  binaryPath: SIDECAR_WORKFLOW_PROBE_CHILD_BINARY,
  spawnProbeChild: defaultProbeChildSpawner,
  materialize: createWorkflowClosureMaterializer({
    cacheRoot,
    cacheMaxBytes,
    registryMaxTarballBytes,
    maxAssetPayloadBytes: MAX_INLINE_ASSET_PAYLOAD_BYTES,
    registries: readRegistries(),
    scratchRoot: path.join(dataDir, "workflow-probe", "closures"),
    host: resolveHostPlatform(),
    materializeAssets: materializeWorkflowAssets,
  }),
});

// Set by the `createDeployRouter` callback below (invoked synchronously
// during construction) so the boot edge can drive the router's restore
// pass before `orchestrator.start()` connects to the hub.
let sidecarDeployRouter: SidecarDeployRouter | undefined;

const orchestrator = createSidecarOrchestrator({
  hubURL: hubWsUrl,
  sidecarId,
  token: sidecarToken,
  dataDir,
  transport,
  cryptoOps: {
    generateKeyPair,
    verifySSHSig: verifySSHSignature,
  },
  resolveSenderCrypto,
  lookupInboundMailPolicy,
  // Write peer of `resolveSenderCrypto`: an inbound `sender.key.refresh`
  // frame re-pushes a rotated sender key here. Decode and persist through
  // the same cache the read side serves from; `put` owns the 32-byte
  // length check and `hexDecode` owns hex validity, so both faults
  // surface to the link's handler.
  cacheSenderKey: (address, publicKey) =>
    senderKeyCache.put(address, hexDecode(publicKey)),
  // Evicting peer of `cacheSenderKey`: an inbound `sender.key.evict` frame
  // durably removes a revoked sender's cached key through the same cache.
  evictSenderKey: (address) => senderKeyCache.evict(address),
  mailInboundRouter: multistepMailRouter,
  signalInboundRouter: multistepSignalRouter,
  drainInboundRouter: multistepDrainRouter,
  grantsInboundRouter: multistepGrantsRouter,
  sourcesInboundRouter: multistepSourcesRouter,
  credentialsInboundRouter: multistepCredentialsRouter,
  applyWorkflowRunPack: restoreWorkflowRunPack,
  workflowProbeExecutor,
  // Test-only override of the hub-link reconnect backoff; unset in
  // production, where the link applies its 3s default.
  ...(reconnectDelayMs !== undefined ? { reconnectDelayMs } : {}),
  // Announces on every (re)connect the workflow deployments this sidecar
  // hosts so the hub re-registers their routes. `createDeployRouter` runs
  // synchronously during construction, so the router is captured before
  // the link connects; assert rather than optional-chain so a wiring
  // regression fails loud instead of announcing none.
  getWorkflowAddresses: () => {
    if (sidecarDeployRouter === undefined) {
      throw new Error(
        "sidecar boot: deploy router was not constructed before the hub link requested workflow addresses",
      );
    }
    return sidecarDeployRouter.activeAddresses();
  },
  // Reports the cached rotatable senders on every (re)connect so the hub
  // re-resolves and re-pushes each current key, catching a rotation that
  // landed while the sidecar was disconnected. The cache owns the
  // "only user senders rotate" filter (a run sender's key is the
  // immutable workflow_run.public_key); the link stays source-opaque.
  getCachedSenderAddresses: () => senderKeyCache.rotatableAddresses(),
  // When the hub-link re-announces a deployment address in an
  // authenticated reconnect, re-drive any workflow-run pack the disconnect
  // cancelled. The link fires this AFTER sending the reconnect frame, so
  // the hub routes the address before it sees the re-shipped pack (both
  // frame families queue on the hub's per-connection chain). Liveness half
  // of reconnect recovery: without it, a synchronous single-step run whose
  // only pack was interrupted mid-transfer never re-ships, because it has
  // no later local write to re-arm the coalescing loop.
  onWorkflowAddressesRoutable: (addresses) => {
    // The deploy router is captured synchronously during orchestrator
    // construction, so a routable callback can only fire once it exists;
    // assert (like `getWorkflowAddresses`) rather than optional-chain.
    if (sidecarDeployRouter === undefined) {
      throw new Error(
        "sidecar boot: deploy router was not constructed before a reconnect made workflow addresses routable",
      );
    }
    for (const address of addresses) {
      wrappedRepoStore.notifyAddressRoutable(address);
      // Trigger B: re-register the deployment's parked correlations. A
      // long hub outage can evict a `signal.correlation.register` frame
      // from the link's bounded send queue; re-driving it on reconnect
      // recovers the parked run's approvability. Safe because the hub-
      // side co-write's `status = "deployed"` row was written at DEPLOY
      // time and survives the outage (the reconnect re-routes the address
      // but writes no DB status), and a sidecar restart re-emits the set
      // twice (child re-establishment fires Trigger A too) but the
      // co-write dedups on the `correlationId` PK via
      // `onConflictDoNothing`. The non-transactional status-check window
      // this widened concurrency exercises is tracked in INTR-338.
      sidecarDeployRouter.reEmitParkedCorrelations(address);
    }
  },
  // On disconnect, block the deployment addresses' workflow-run pushes
  // until the authenticated reconnect above re-routes them; without the
  // block, the pusher re-ships onto the not-yet-registered connection and
  // the hub drops the frames as "unrouted".
  onWorkflowAddressesUnroutable: (addresses) => {
    for (const address of addresses) {
      wrappedRepoStore.markAddressUnroutable(address);
    }
  },
  createDeployRouter: ({
    sessions,
    keyStore,
    publishWorkflowInferenceEvent,
    publishWorkflowSuspension,
  }) => {
    const router = createSidecarDeployRouter({
      sessions,
      keyStore,
      senderKeyCache,
      transport,
      repoStore: wrappedRepoStore,
      signingKeySeed: sidecarSigningKey.privateKey,
      credentialCipher,
      createAgentCrypto: createEd25519Crypto,
      assertSourceBuildable: buildHarness.canBuildSource,
      registerDeployment: ({ runId, agentAddress }) => {
        deploymentAddressRegistry.record(runId, agentAddress);
      },
      unregisterDeployment: ({ runId }) => {
        deploymentAddressRegistry.unregister(runId);
      },
      reportDeploymentRefTips: (agentAddress) =>
        wrappedRepoStore.reportWorkflowRunRefTips(agentAddress),
      multistepMailRouter,
      inboundMailPolicyRegistry,
      multistepSignalRouter,
      multistepDrainRouter,
      multistepGrantsRouter,
      multistepSourcesRouter,
      multistepCredentialsRouter,
      multistepSubstrateEnv,
      multistepSubprocessSpawner: defaultSubprocessSpawner,
      multistepBinaryPath: SIDECAR_WORKFLOW_CHILD_BINARY,
      applyFrozenWorkflowClosure,
      readRegistries,
      resolveHostPlatform,
      resolveInboundMailPolicy,
      materializeWorkflowAssets,
      maxInlineAssetPayloadBytes: MAX_INLINE_ASSET_PAYLOAD_BYTES,
      deriveWorkflowRunRepoId,
      inertFlatNamespaceStepIds,
      workflowSourceAssetMountPath,
      sourceAssetGitDir,
      parseAgentId,
      publishWorkflowInferenceEvent,
      publishWorkflowSuspension,
      ...(onDispatchTiming !== undefined ? { onDispatchTiming } : {}),
      ...(repackEveryMessages !== undefined ? { repackEveryMessages } : {}),
      ...(consumedRetentionMs !== undefined ? { consumedRetentionMs } : {}),
      ...(readyTimeoutMs !== undefined ? { readyTimeoutMs } : {}),
    });
    // Capture the router so the boot edge can drive its restore pass
    // before connecting. `createDeployRouter` runs synchronously during
    // orchestrator construction (exactly once), so this is populated by
    // the time the restore call below runs.
    sidecarDeployRouter = router;
    return router;
  },
});

resolvedHubLink = orchestrator.hubLink;

// Re-establish the workflow deployments a prior sidecar process persisted,
// BEFORE opening the hub connection: each single-step head must have its
// mailbox/transport registration live before the hub can route to it.
// Assert the router was captured rather than optional-chaining it, so a
// future refactor that made `createDeployRouter` fire lazily would fail
// loud here instead of silently skipping restore.
if (sidecarDeployRouter === undefined) {
  throw new Error(
    "sidecar boot: deploy router was not constructed before workflow-deployment restore",
  );
}
await sidecarDeployRouter.restoreWorkflowRuns();

orchestrator.start();
