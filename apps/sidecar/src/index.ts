import { appendFileSync } from "node:fs";
import path from "node:path";
import { setup } from "@intx/log";
import { createInMemoryTransport } from "@intx/mail-memory";
import {
  createEd25519Crypto,
  generateKeyPair,
  signEd25519,
  verifySSHSignature,
} from "@intx/crypto";
import { createSidecarOrchestrator, type HubLink } from "@intx/hub-agent";
import { hexEncode } from "@intx/types";
import { createAgentRepoStore } from "@intx/hub-sessions";
import { createTarballCache } from "@intx/tool-packaging";

import { loadAdapterRegistry } from "@intx/inference/providers";

import {
  readAdapterManifest,
  readCacheMaxBytes,
  readRegistryMaxTarballBytes,
} from "./config";
import { createDefaultHarnessBuilder } from "./default-harness";
// Pull the workflow-host wiring factory into the sidecar's module
// graph so the supervisor surface is reachable from this binary.
// `createSidecarDeployRouter` is the production routing the
// orchestrator hands to the link's `agent.deploy` handler; every
// inbound frame stages through the workflow-run substrate, spawning a
// supervised workflow-process child for a workflow deploy.
import type { DispatchTimingMark } from "@intx/workflow-host";

import {
  createSidecarDeployRouter,
  createSidecarWorkflowSupervisor,
  type SidecarDeployRouter,
} from "./workflow-host-wiring";
import {
  createDeploymentAddressRegistry,
  createMultistepDrainRouter,
  createMultistepMailRouter,
  createMultistepSignalRouter,
  createMultistepSourcesRouter,
  createWorkflowRunPackClient,
  createWorkflowRunPackPushingRepoStore,
} from "./workflow-run-pack-client";
import { loadOrMintSidecarKeypair } from "./signing-keypair";

await setup();

function requireEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined) {
    throw new Error(`${name} environment variable is required`);
  }
  return value;
}

const dataDir = requireEnv("SIDECAR_DATA_DIR");

// Resolve cache configuration at the boot edge so the workflow-child's
// per-apply loader receives a concrete path and cap through its spawn
// env rather than re-reading env at non-boundary call sites.
const sidecarCacheDir = process.env["SIDECAR_CACHE_DIR"];
const cacheRoot =
  sidecarCacheDir !== undefined && sidecarCacheDir.trim() !== ""
    ? sidecarCacheDir
    : path.join(dataDir, "cache", "tarballs");
const cacheMaxBytes = readCacheMaxBytes();
const registryMaxTarballBytes = readRegistryMaxTarballBytes();

// Operator-configured custom inference adapters, resolved once at the
// boot edge. `loadAdapterRegistry` merges the statically-linked
// built-ins with any custom adapters the manifest names, importing each
// custom module eagerly here so a bad specifier fails the sidecar at
// boot rather than at first inference. The SAME registry backs the
// deploy router's source-admission check below; the workflow child
// cannot receive this object across the fork, so the validated
// manifest is serialized into the child's spawn env (see
// `multistepSubstrateEnv`) and the child rebuilds an equivalent
// registry from it. Specifiers are operator-config-only — the agent
// deploy tree never contributes one.
const adapterManifest = readAdapterManifest();
const adapters = await loadAdapterRegistry(adapterManifest);

// Phase 4.7 latency-gate hook. When `SIDECAR_LATENCY_BENCH_FILE` names a
// path, the deploy router wires the supervisor's `onDispatchTiming`
// observer to append a parseable line per per-message dispatch boundary
// to that file, which the benchmark harness reads after the run. A file
// (rather than stdout) is the channel because the spawn fixture caps its
// stdout drain buffer, and a few-hundred-message run emits more lines
// than that cap holds. Resolved here at the boot edge (the one layer
// that reads env) so no non-boundary site re-decides what the absent
// value means; an unset path leaves the observer unwired and the
// supervisor's dispatch path untouched. This is observability-only -- no
// control-flow effect -- and is the single benchmark-side instrumentation
// hook the 4.7 gate adds. The synchronous append (not the logger) is the
// raw measurement channel: it is not application logging and must not be
// formatted or level-gated. A failed append surfaces (it is not
// swallowed) so a broken benchmark channel does not silently yield an
// empty result set.
// Two line shapes share the channel, discriminated by `mark.kind`:
//   roundtrip:  `<runId> <marker> <atMs>`          (4.7 latency gate)
//   leg:        `<runId> leg <leg> <phase> <atMs> [runsFanOut consumedFanOut looseObjects gitBytes]`
//               (D2 per-leg attribution; the trailing four counters are
//               present only on the `end` phase). The leg shape is a
//               strict superset prefixed with the literal `leg` token, so
//               a reader can split on whitespace and branch on field 2.
const latencyBenchFile = process.env["SIDECAR_LATENCY_BENCH_FILE"];
const onDispatchTiming: ((mark: DispatchTimingMark) => void) | undefined =
  latencyBenchFile !== undefined && latencyBenchFile.trim() !== ""
    ? (mark) => {
        let line: string;
        if (mark.kind === "roundtrip") {
          line = `${mark.runId} ${mark.marker} ${mark.atMs.toFixed(3)}\n`;
        } else {
          const counters =
            mark.counters !== undefined
              ? ` ${String(mark.counters.runsFanOut)} ${String(mark.counters.consumedFanOut)} ${String(mark.counters.looseObjects)} ${String(mark.counters.gitBytes)}`
              : "";
          line = `${mark.runId} leg ${mark.leg} ${mark.phase} ${mark.atMs.toFixed(3)}${counters}\n`;
        }
        appendFileSync(latencyBenchFile, line);
      }
    : undefined;

// D2 §10c forced-repack A/B toggle. When `SIDECAR_REPACK_EVERY_MESSAGES`
// names a positive integer, the supervisor forces a `git gc`/repack of
// the workflow-run repo every Nth dispatched message (under the
// single-writer discipline). Resolved here at the boot edge alongside the
// timing channel; absent or non-positive => the supervisor forks no
// `git gc` and the dispatch path is untouched. Measurement-only: this
// exists to discriminate pack-growth from tree-fan-out, never to run in
// production.
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

// Consumed-dedup retention horizon (ms). Resolved here at the boot edge
// -- the single layer that owns operator config -- and threaded into
// every supervisor the deploy router constructs. Absent or empty =>
// the supervisor applies its 24h default. This is the OPERATOR-policy
// value: the longest window in which the same message could
// legitimately be re-submitted and still must be caught as a duplicate
// (the consumed/ dedup index retains at least this long before the
// retention watermark prunes it). It must be >= the maximum redelivery
// window of any at-least-once mail source if one is ever added.
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

// Bound on the child's spawn-time `ready` handshake. Threaded to every
// per-deployment supervisor; on expiry the supervisor kills the child and
// rejects the spawn, so a child that spawns but never signals ready fails
// the deploy (or is skipped by boot-time restore) instead of hanging it.
// Absent, the supervisor applies its 30s default.
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

const sidecarSigningKey = await loadOrMintSidecarKeypair(SIDECAR_SIGNING_DIR);

// Construct the substrate-backed RepoStore at the boot edge. The
// supervisor consumes this through the deploy router; the supervisor's
// conversation-state writes reach `writeTreePreservingPrefix` on this
// store. The boot-edge facade below wraps the store so a successful
// workflow-run write fires the pack push hook before its Promise
// resolves.
const agentRepoStore = createAgentRepoStore({
  dataDir,
  signingKey: sidecarSigningKey,
});

// The deploy router records `(deploymentId -> agentAddress)` here on
// every inbound `agent.deploy`; the facade resolves the mapping when
// firing the pack push so the outbound frames carry the right
// agentAddress for hub-side routing.
const deploymentAddressRegistry = createDeploymentAddressRegistry();

// Per-deployment-address mail handler registry the hub-link consults on
// every inbound `mail.inbound` frame. The deploy router's multi-step
// branch registers `wired.routeInbound` against the deployment's mail
// address once its supervisor spawns; the hub-link's `mail.inbound`
// handler calls `tryRoute` so an inbound deployment-address message
// lands on the supervisor's mail-bus subscription. Mail for an address
// with no registered handler has no receiver and is logged-and-dropped.
const multistepMailRouter = createMultistepMailRouter();

// Per-deployment-address signal handler registry the hub-link consults
// on every inbound `signal.deliver` frame. The multi-step deploy
// router registers a handler against the deployment's mail address
// once its supervisor spawns; the handler forwards the signal into the
// supervisor's `deliverSignal`, which sends a `signal.deliver` control
// IPC frame to the workflow-process child. The child commits the
// resulting `SignalReceived` event through its own substrate -- the
// single writer of the workflow-run repo on the sidecar side.
const multistepSignalRouter = createMultistepSignalRouter();

// Per-deployment-address drain handler registry the hub-link consults
// on every inbound `drain.deliver` frame. The multi-step deploy router
// registers a handler against the deployment's mail address once its
// supervisor spawns; the handler forwards the drain into the
// supervisor's `drain`, which sends a `drain` control IPC frame to the
// workflow-process child and arms one drainTimeout accumulator per
// in-flight run. Cancel-mode in-flight steps abort on the child side;
// wait-mode steps continue. Accumulators commit a signed
// `CancelRequested{origin: "supervisor-drain"}` against the
// workflow-run repo when the deadline expires.
const multistepDrainRouter = createMultistepDrainRouter();

// Per-deployment-address sources-rotation handler registry. Only a
// single-step warm deployment registers a handler once its supervisor
// spawns; the handler forwards the rotated list into the supervisor's
// `deliverSources`, which sends a `sources-updated` control IPC frame to
// the child, where the warm agent's live sources are swapped in place. A
// multi-step deployment registers none, so a rotation resolved against
// its address is unrouted.
const multistepSourcesRouter = createMultistepSourcesRouter();

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
  SIDECAR_SIGNING_PUBLIC_KEY: hexEncode(sidecarSigningKey.publicKey),
  SIDECAR_SIGNING_PRIVATE_KEY: hexEncode(sidecarSigningKey.privateKey),
  HUB_WS_URL: hubWsUrl,
  SIDECAR_ID: sidecarId,
  SIDECAR_TOKEN: sidecarToken,
  PATH: requireEnv("PATH"),
  // Tool-loader caps the child's per-step tool materialization uses.
  // Resolved once at the boot edge and threaded into the child through
  // the substrate config so the child does not re-read env at a
  // non-boundary site.
  SIDECAR_CACHE_MAX_BYTES: String(cacheMaxBytes),
  SIDECAR_REGISTRY_MAX_TARBALL_BYTES: String(registryMaxTarballBytes),
  // Serialize the parent's ALREADY-VALIDATED manifest (the object
  // `readAdapterManifest` returned), not the raw env string, so the
  // child rebuilds the same custom-adapter set. Always present (defaults
  // to "[]" when no custom adapters are configured); the child treats a
  // missing key as a serialization bug and fails loud. The child
  // re-validates the shape before importing any module — defense in
  // depth at the deserialization boundary, since the child env is
  // operator-controlled via Bun.spawn.
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

// Set by the `createDeployRouter` callback below (invoked synchronously
// during construction) so the boot edge can drive the router's restore pass
// before `orchestrator.start()` connects to the hub.
let sidecarDeployRouter: SidecarDeployRouter | undefined;

const orchestrator = createSidecarOrchestrator({
  hubURL: hubWsUrl,
  sidecarId,
  token: sidecarToken,
  dataDir,
  transport,
  cryptoOps: {
    generateKeyPair,
    signEd25519,
    verifySSHSig: verifySSHSignature,
  },
  mailInboundRouter: multistepMailRouter,
  signalInboundRouter: multistepSignalRouter,
  drainInboundRouter: multistepDrainRouter,
  sourcesInboundRouter: multistepSourcesRouter,
  // The hub link calls this on every (re)connect to announce the workflow
  // deployments this sidecar hosts so the hub re-registers their routes.
  // `createDeployRouter` runs synchronously during construction (below), so
  // the router is captured before the link ever connects; assert rather than
  // optional-chain so a wiring regression fails loud instead of silently
  // announcing no deployments.
  getWorkflowAddresses: () => {
    if (sidecarDeployRouter === undefined) {
      throw new Error(
        "sidecar boot: deploy router was not constructed before the hub link requested workflow addresses",
      );
    }
    return sidecarDeployRouter.activeAddresses();
  },
  createDeployRouter: ({
    sessions,
    keyStore,
    publishWorkflowInferenceEvent,
  }) => {
    const router = createSidecarDeployRouter({
      sessions,
      keyStore,
      transport,
      repoStore: wrappedRepoStore,
      signingKeySeed: sidecarSigningKey.privateKey,
      createAgentCrypto: createEd25519Crypto,
      assertSourceBuildable: buildHarness.canBuildSource,
      registerDeployment: ({ deploymentId, agentAddress }) => {
        deploymentAddressRegistry.record(deploymentId, agentAddress);
      },
      unregisterDeployment: ({ deploymentId }) => {
        deploymentAddressRegistry.unregister(deploymentId);
      },
      multistepMailRouter,
      multistepSignalRouter,
      multistepDrainRouter,
      multistepSourcesRouter,
      multistepSubstrateEnv,
      publishWorkflowInferenceEvent,
      ...(onDispatchTiming !== undefined ? { onDispatchTiming } : {}),
      ...(repackEveryMessages !== undefined ? { repackEveryMessages } : {}),
      ...(consumedRetentionMs !== undefined ? { consumedRetentionMs } : {}),
      ...(readyTimeoutMs !== undefined ? { readyTimeoutMs } : {}),
    });
    // Capture the router so the boot edge can drive its restore pass before
    // connecting. `createDeployRouter` runs synchronously during
    // `createSidecarOrchestrator` construction (exactly once, before the
    // handle returns), so `sidecarDeployRouter` is populated by the time the
    // restore call below runs.
    sidecarDeployRouter = router;
    return router;
  },
});

resolvedHubLink = orchestrator.hubLink;

// Re-establish the workflow deployments a prior sidecar process persisted,
// BEFORE opening the hub connection: each single-step head must have its
// mailbox/transport registration live before the hub can route to it.
// Assert the router was captured rather than optional-chaining it, so a
// future refactor that made `createDeployRouter` fire lazily would fail loud
// here instead of silently skipping restore.
if (sidecarDeployRouter === undefined) {
  throw new Error(
    "sidecar boot: deploy router was not constructed before workflow-deployment restore",
  );
}
await sidecarDeployRouter.restoreWorkflowDeployments();

orchestrator.start();

// Keep `createSidecarWorkflowSupervisor` reachable from this entry
// point so a deploy handler that branches on workflow kind can
// instantiate a supervisor per active deployment without forking the
// boot path. The full wiring -- agent.deploy → kind detect →
// supervisor.spawn -- threads through the deploy handler in a later
// commit. The reference here is the seam.
export { createSidecarWorkflowSupervisor };
