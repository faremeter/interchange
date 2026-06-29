import { createDB, createGrantStore } from "@intx/db";
import { createApp, createAuth } from "@intx/hub-api";
import {
  createAgentRepoStore,
  createAssetService,
  createEventCollectorRegistry,
  createHubSessionLookups,
  createHubSessionOrchestrator,
  createSessionService,
  createSidecarRouter,
  WORKSPACE_BUILTINS_REGISTRY,
  type WsHandle,
} from "@intx/hub-sessions";
import { generateKeyPair } from "@intx/crypto-node";
import { hexEncode } from "@intx/types";
import { upgradeWebSocket, websocket } from "hono/bun";
import { setup, getLogger } from "@intx/log";

await setup();

const log = getLogger(["hub"]);

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
const grantStore = createGrantStore(db);

const hubDataDir = process.env["HUB_DATA_DIR"];
if (!hubDataDir) {
  throw new Error("HUB_DATA_DIR environment variable is required");
}

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
// pack strands the prior tip's objects and adds a pack; left alone the
// repo grows without bound. The hub reclaims on the write path once a
// repo crosses HUB_AGENT_GC_PACK_THRESHOLD packs, and warns once it
// crosses HUB_AGENT_GC_WARN_BYTES. Retention is fixed to keep-history
// and not operator-configurable: the hub is the long-term archive of
// an agent's state graph, and tip-only would prune the ancestry its
// history replay derives from.
const DEFAULT_HUB_AGENT_GC_PACK_THRESHOLD = 64;
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

const agentRepoStore = createAgentRepoStore({
  dataDir: hubDataDir,
  signingKey: hubSigningKey,
  gc: {
    packThreshold: readPositiveIntEnv(
      "HUB_AGENT_GC_PACK_THRESHOLD",
      DEFAULT_HUB_AGENT_GC_PACK_THRESHOLD,
    ),
    warnBytes: readPositiveIntEnv(
      "HUB_AGENT_GC_WARN_BYTES",
      DEFAULT_HUB_AGENT_GC_WARN_BYTES,
    ),
    retention: "keep-history",
  },
});

const lookups = createHubSessionLookups({ db, agentRepoStore });

const sidecarRouter = createSidecarRouter({
  hubPublicKey: hexEncode(hubSigningKey.publicKey),
  lookups,
});

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
  grantStore,
  agentRepoStore,
});

// The asset service shares the agent-repo store's substrate so skill
// assets land under the same on-disk root and reuse the same signing
// key for commit signatures. It is consumed by the session service for
// per-attachment pack fan-out and by the smart-HTTP asset routes for
// clone and push. The E2E test seeds fixtures directly through this
// service object.
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

const sessionService = createSessionService({
  sidecarRouter,
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

const app = createApp({
  getSession: async (headers) => {
    const result = await auth.api.getSession({ headers });
    return result ? { user: result.user, session: result.session } : null;
  },
  authHandler: (c) => auth.handler(c.req.raw),
  db,
  sidecarRouter,
  sessionService,
  eventCollectors,
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

const port = Number(process.env["PORT"] ?? 3000);

log.info("Starting server on port {port}", { port });

export default {
  fetch: app.fetch,
  websocket,
  port,
  idleTimeout: 0,
};
