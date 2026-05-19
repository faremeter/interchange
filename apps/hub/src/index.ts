import { createDB, createGrantStore } from "@interchange/db";
import {
  createAgentRepoStore,
  createApp,
  createAuth,
  createEventCollectorRegistry,
  createHubSessionLookups,
  createHubSessionOrchestrator,
  createSessionService,
  createSidecarRouter,
} from "@interchange/hub";
import { generateKeyPair } from "@interchange/crypto-node";
import { upgradeWebSocket, websocket } from "hono/bun";
import { setup, getLogger } from "@interchange/log";

await setup();

const log = getLogger(["hub"]);

const { db } = createDB({
  host: process.env["DB_HOST"] ?? "localhost",
  port: Number(process.env["DB_PORT"] ?? 5432),
  user: process.env["DB_USER"] ?? "postgres",
  password: process.env["DB_PASSWORD"] ?? "postgres",
  database: process.env["DB_NAME"] ?? "interchange",
});

const auth = createAuth(db);
const grantStore = createGrantStore(db);

const hubDataDir = process.env["HUB_DATA_DIR"];
if (!hubDataDir) {
  throw new Error("HUB_DATA_DIR environment variable is required");
}

const hubSigningKey = await generateKeyPair();
log.info("Generated hub deploy signing key");

function hexEncode(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

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

const sessionService = createSessionService({
  sidecarRouter,
  agentRepoStore,
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
  sidecarWsHandler: upgradeWebSocket((_c) => {
    let handle: import("@interchange/hub").WsHandle;
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
