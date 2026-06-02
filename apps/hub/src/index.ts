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

const hubSigningKey = await generateKeyPair();
log.info("Generated hub deploy signing key");

const agentRepoStore = createAgentRepoStore({
  dataDir: hubDataDir,
  signingKey: hubSigningKey,
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
const assetService = createAssetService({
  db,
  repoStore: agentRepoStore.repoStore,
});

const sessionService = createSessionService({
  sidecarRouter,
  agentRepoStore,
  assetService,
  db,
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
