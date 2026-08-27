import {
  createDB,
  createGrantStore,
  createSidecarAllocationStore,
  createWorkflowRunDispatchStore,
} from "@intx/db";
import { createEnvKeyCredentialCipher } from "@intx/crypto";
import { hexDecode } from "@intx/types";
import {
  createApp,
  createAuth,
  createMailTriggeredRunGrantsMaterializer,
} from "@intx/hub-api";
import {
  createAgentRepoStore,
  createAssetService,
  createEventCollectorRegistry,
  createHubSessionLookups,
  createHubSessionOrchestrator,
  createSessionService,
  createSidecarAllocationReconciler,
  createSidecarPluginRegistry,
  createSidecarRouter,
  createSidecarCredentialResolver,
  createWorkflowAllocationService,
  createWorkflowDispatchService,
  pushCredentialReconcile,
  WORKSPACE_BUILTINS_REGISTRY,
  type SidecarLookups,
  type WsHandle,
} from "@intx/hub-sessions";
import { generateKeyPair } from "@intx/crypto";
import { hexEncode } from "@intx/types";
import { upgradeWebSocket, websocket } from "hono/bun";
import { setup, getLogger } from "@intx/log";

await setup();

const log = getLogger(["hub"]);
const port = Number(process.env["PORT"] ?? 3000);

// PG_SCHEMA pins the hub to a specific postgres schema. The
// integration-test harness sets this so each spawned hub gets a
// dedicated, droppable schema. Production deployments leave it
// unset and run against postgres' default search_path.
const pgSchema = process.env["PG_SCHEMA"];
const { db } = createDB({
  host: process.env["DB_HOST"] ?? "localhost",
  port: Number(process.env["DB_PORT"] ?? 5432),
  user: process.env["DB_USER"] ?? "postgres",
  password: process.env["DB_PASSWORD"] ?? "postgres",
  database: process.env["DB_NAME"] ?? "interchange",
  ...(pgSchema !== undefined && { schema: pgSchema }),
});

const auth = createAuth(db);

const hubDataDir = process.env["HUB_DATA_DIR"];
if (!hubDataDir) {
  throw new Error("HUB_DATA_DIR environment variable is required");
}

// Credential secrets are encrypted at rest under this operator-provided key.
// Required at boot: a missing or wrong-length key fails loudly here rather than
// letting the hub run and store secrets it cannot protect. 32 bytes, hex --
// e.g. `openssl rand -hex 32`, the same shape as BETTER_AUTH_SECRET.
const credentialEncryptionKeyHex = process.env["CREDENTIAL_ENCRYPTION_KEY"];
if (
  credentialEncryptionKeyHex === undefined ||
  credentialEncryptionKeyHex.trim() === ""
) {
  throw new Error("CREDENTIAL_ENCRYPTION_KEY environment variable is required");
}
const credentialCipher = createEnvKeyCredentialCipher(
  hexDecode(credentialEncryptionKeyHex),
);

// 10 MiB is the production cap for tool-package tarballs uploaded via
// the package-registry PUT endpoint. The npm registry's own per-tarball
// soft cap is several times this, but the substrate's tool packages are
// the curated subset the operator vets; an upload pushing past 10 MiB
// is far more likely to be misuse than a legitimate build. The
// HUB_MAX_TARBALL_BYTES env var lets an operator opt into a different
// cap without a code change.
const DEFAULT_HUB_MAX_TARBALL_BYTES = 10 * 1024 * 1024;
const hubMaxTarballBytesRaw = process.env["HUB_MAX_TARBALL_BYTES"];
const hubMaxTarballBytes =
  hubMaxTarballBytesRaw === undefined || hubMaxTarballBytesRaw.trim() === ""
    ? DEFAULT_HUB_MAX_TARBALL_BYTES
    : Number(hubMaxTarballBytesRaw);
if (!Number.isFinite(hubMaxTarballBytes) || hubMaxTarballBytes <= 0) {
  throw new Error(
    `HUB_MAX_TARBALL_BYTES must be a positive number; got ${JSON.stringify(hubMaxTarballBytesRaw)}`,
  );
}

const hubSigningKey = await generateKeyPair();
log.info("Generated hub deploy signing key");

// Write-path GC for the hub's agent-state repos. Each accepted state
// pack strands the prior tip's objects and adds a pack, and each deploy
// commit strands loose objects; left alone the repo grows without bound.
// The hub reclaims on the write path once a repo crosses
// HUB_AGENT_GC_PACK_THRESHOLD packs or HUB_AGENT_GC_LOOSE_THRESHOLD loose
// objects, and warns once it crosses HUB_AGENT_GC_WARN_BYTES. Retention
// is fixed to keep-history and not operator-configurable: the hub is the
// long-term archive of an agent's state graph, and tip-only would prune
// the commit ancestry the hub's subscriber-seq and history replay derive
// from git.log.
const DEFAULT_HUB_AGENT_GC_PACK_THRESHOLD = 64;
const DEFAULT_HUB_AGENT_GC_LOOSE_THRESHOLD = 2048;
const DEFAULT_HUB_AGENT_GC_WARN_BYTES = 256 * 1024 * 1024;

function readPositiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(
      `${name} must be a positive integer; got ${JSON.stringify(raw)}`,
    );
  }
  return value;
}

// Like `readPositiveIntEnv` but with no fallback: returns `undefined` when the
// var is unset so the caller can omit the field and let the consumer apply its
// own default, instead of duplicating that default here.
function readOptionalPositiveIntEnv(name: string): number | undefined {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") return undefined;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(
      `${name} must be a positive integer; got ${JSON.stringify(raw)}`,
    );
  }
  return value;
}

const agentRepoStore = createAgentRepoStore({
  dataDir: hubDataDir,
  signingKey: hubSigningKey,
  gc: {
    packThreshold: readPositiveIntEnv(
      "HUB_AGENT_GC_PACK_THRESHOLD",
      DEFAULT_HUB_AGENT_GC_PACK_THRESHOLD,
    ),
    looseThreshold: readPositiveIntEnv(
      "HUB_AGENT_GC_LOOSE_THRESHOLD",
      DEFAULT_HUB_AGENT_GC_LOOSE_THRESHOLD,
    ),
    warnBytes: readPositiveIntEnv(
      "HUB_AGENT_GC_WARN_BYTES",
      DEFAULT_HUB_AGENT_GC_WARN_BYTES,
    ),
    retention: "keep-history",
  },
});

// The asset service shares the agent-repo store's substrate so skill
// assets land under the same on-disk root and reuse the same signing
// key for commit signatures. It is consumed by the session service for
// per-attachment pack fan-out, by the smart-HTTP asset routes for clone
// and push, and by the mail-triggered run-grants materializer to hydrate
// a receiving deployment's definition. The E2E test seeds fixtures
// directly through this service object.
const httpRegistries = new Map([
  ["npmjs", { url: "https://registry.npmjs.org" }],
]);

const assetService = createAssetService({
  db,
  repoStore: agentRepoStore.repoStore,
  // Reserve every configured HTTP registry name so a `package-registry`
  // asset cannot silently shadow it at session launch. The session
  // service's per-launch registry assembly applies asset-wins-on-name-
  // collision (see `session-service.ts` `assetIndex` build), which
  // turns a same-named asset into an opaque reroute of the public
  // npm registry; rejecting at creation surfaces the collision at
  // intent time instead of debugging an unexpected reroute later.
  reservedPackageRegistryNames: new Set(httpRegistries.keys()),
});

// Materialize a mail-triggered workflow run's grants from the receiving
// deployment's definition, so a workflow->workflow mail run is born with
// the same authorization an externally-triggered run gets. Threaded into
// the sidecar router as a lookup its `mail.outbound` handler invokes for
// each workflow-deployment recipient.
const lookups: SidecarLookups = {
  ...createHubSessionLookups({ db, agentRepoStore }),
  materializeMailTriggeredRunGrants: createMailTriggeredRunGrantsMaterializer({
    db,
    grantStore: createGrantStore(db),
  }),
};

const sidecarCredentials = createSidecarCredentialResolver({ db });
// HUB_PROBE_TIMEOUT_MS widens the router's per-probe timeout for operators
// whose registries or definition evaluations run slow; unset, the router
// applies its own DEFAULT_PROBE_TIMEOUT_MS.
const probeTimeoutMs = readOptionalPositiveIntEnv("HUB_PROBE_TIMEOUT_MS");

const sidecarRouter = createSidecarRouter({
  hubPublicKey: hexEncode(hubSigningKey.publicKey),
  authenticateSidecar: async ({ token }) => sidecarCredentials.resolve(token),
  validateSidecarIdentity: sidecarCredentials.isCurrent,
  lookups,
  ...(probeTimeoutMs !== undefined ? { probeTimeoutMs } : {}),
});

// Wire the reconnect credential resync now that the router exists (the lookup
// calls back into it). The handler reads `lookups.resyncCredentials` lazily on
// each reconnect, so assigning it after construction is in time.
lookups.resyncCredentials = (agentAddress) => {
  void pushCredentialReconcile(
    db,
    sidecarRouter,
    agentAddress,
    credentialCipher,
  );
};

const eventCollectors = createEventCollectorRegistry({
  db,
  onTurnFinalized(agentAddress, turn) {
    sidecarRouter.dispatchAgentEvent(agentAddress, {
      type: "turn.committed",
      data: {
        turnId: turn.turnId,
        status: turn.status,
        text: turn.text,
        hadReply: turn.hadReply,
        hadError: turn.hadError,
        errors: turn.errors,
        toolCalls: turn.toolCalls,
        toolErrors: turn.toolErrors,
      },
    });
  },
});

createHubSessionOrchestrator({
  events: sidecarRouter.events,
  router: sidecarRouter,
  db,
  eventCollectors,
  agentRepoStore,
});

const sessionService = createSessionService({
  sidecarRouter,
  sidecarAllocationRouter: sidecarRouter,
  agentRepoStore,
  assetService,
  db,
  toolPackageRegistries: {
    httpRegistries,
    defaultRegistry: "npmjs",
    // The `workspace-builtins` package-registry asset hosts the
    // three in-tree tool packages (`@intx/tools-mail`,
    // `@intx/tools-posix`, `@intx/tools-lsp`). Routing the `@intx`
    // scope through it keeps an agent's pin set readable
    // (`{ name: "@intx/tools-mail" }`) without forcing every pin to
    // carry an explicit `registry` field. Operators who shadow this
    // asset at a child tenancy with their own `workspace-builtins`
    // asset get the closer-scope win for free, since the session
    // service builds the per-launch registry map leaf-to-root.
    scopeRouting: [{ scope: "@intx", registry: WORKSPACE_BUILTINS_REGISTRY }],
  },
});

// Provisioner plugins are injected at the application composition boundary.
// The in-tree Hub ships without an infrastructure backend.
const sidecarPlugins = createSidecarPluginRegistry({ provisioners: [] });
const workflowAllocationService = createWorkflowAllocationService({
  db,
  plugins: sidecarPlugins,
  preparedDeployer: sessionService,
  credentialCipher,
  allocationRouter: sidecarRouter,
});
const sidecarAllocationStore = createSidecarAllocationStore(db);
const workflowDispatchService = createWorkflowDispatchService({
  dispatchStore: createWorkflowRunDispatchStore(db),
  allocationStore: sidecarAllocationStore,
  router: sidecarRouter,
  resolveAnchorAddress: async (anchorRunId) => {
    const row = await db.query.workflowRun.findFirst({
      where: (run, { eq }) => eq(run.id, anchorRunId),
      columns: { address: true },
    });
    return row?.address ?? null;
  },
});
const sidecarAllocationReconciler = createSidecarAllocationReconciler({
  allocationStore: sidecarAllocationStore,
  plugins: sidecarPlugins,
  router: sidecarRouter,
  hubWebSocketUrl:
    process.env["HUB_SIDECAR_WEBSOCKET_URL"] ??
    `ws://127.0.0.1:${String(port)}/api/sidecars/ws`,
  onReady: async (allocation) => {
    await workflowAllocationService.deployReadyAllocation(allocation);
    await workflowDispatchService.requeueForReadyAllocation(
      allocation.anchorRunId,
    );
  },
});

await sidecarAllocationReconciler.initialize();
sidecarRouter.events.on("sidecar.disconnect", ({ allocated }) => {
  if (allocated === undefined) return;
  return sidecarAllocationReconciler.handleDisconnect(allocated);
});
sidecarRouter.events.on("sidecar.allocated.connected", (allocated) =>
  sidecarAllocationReconciler.handleConnected(allocated),
);
sidecarRouter.events.on(
  "mail.inbound.acknowledged",
  ({ messageId, allocated }) => {
    if (allocated === undefined) return;
    return workflowDispatchService.acknowledge({ ...allocated, messageId });
  },
);

const ALLOCATION_RECONCILIATION_INTERVAL_MS = 1_000;
const ALLOCATION_CONNECTION_REPAIR_INTERVAL_MS = 30_000;
let nextAllocationConnectionRepairAt =
  Date.now() + ALLOCATION_CONNECTION_REPAIR_INTERVAL_MS;
function scheduleAllocationReconciliation(delayMs: number): void {
  const timer = setTimeout(() => {
    void reconcileSidecarAllocations();
  }, delayMs);
  timer.unref?.();
}

async function reconcileSidecarAllocations(): Promise<void> {
  try {
    await sidecarAllocationReconciler.reconcileUntilIdle();
    await workflowDispatchService.reconcileUntilIdle();
    if (Date.now() >= nextAllocationConnectionRepairAt) {
      nextAllocationConnectionRepairAt =
        Date.now() + ALLOCATION_CONNECTION_REPAIR_INTERVAL_MS;
      await sidecarAllocationReconciler.repairUnscheduledConnections();
    }
  } catch (error) {
    log.error`Sidecar allocation reconciliation failed: ${error instanceof Error ? error.message : String(error)}`;
  } finally {
    scheduleAllocationReconciliation(ALLOCATION_RECONCILIATION_INTERVAL_MS);
  }
}

// Start after module initialization so the websocket endpoint can accept a
// newly provisioned sidecar while reconciliation waits for its connection.
scheduleAllocationReconciliation(0);

const app = createApp({
  getSession: async (headers) => {
    const result = await auth.api.getSession({ headers });
    return result ? { user: result.user, session: result.session } : null;
  },
  authHandler: (c) => auth.handler(c.req.raw),
  db,
  sidecarRouter,
  sessionService,
  workflowDispatchService,
  eventCollectors,
  credentialCipher,
  assetService,
  repoStore: agentRepoStore.repoStore,
  maxTarballBytes: hubMaxTarballBytes,
  sidecarWsHandler: upgradeWebSocket((_c) => {
    let handle: WsHandle;
    return {
      onOpen(_evt, ws) {
        handle = {
          send(data: string) {
            ws.send(data);
          },
          close() {
            ws.close();
          },
        };
        sidecarRouter.handleOpen(handle);
      },
      onMessage(evt, _ws) {
        if (typeof evt.data === "string") {
          sidecarRouter.handleMessage(handle, evt.data);
        }
      },
      onClose(_evt, _ws) {
        sidecarRouter.handleClose(handle);
      },
    };
  }),
});

log.info("Starting server on port {port}", { port });

export default {
  fetch: app.fetch,
  websocket,
  port,
  idleTimeout: 0,
};
