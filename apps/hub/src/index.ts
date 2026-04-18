import { createDB } from "@interchange/db";
import {
  createApp,
  createAuth,
  createEventCollectorRegistry,
  createSidecarRouter,
} from "@interchange/hub";
import type { InferenceEvent } from "@interchange/types/runtime";
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
const eventCollectors = createEventCollectorRegistry(db);

const sidecarRouter = createSidecarRouter({
  onAgentEvent(_agentAddress, sessionId, event) {
    eventCollectors.dispatch(sessionId, event as InferenceEvent);
  },
  onSidecarDisconnect(agentAddresses) {
    for (const addr of agentAddresses) {
      eventCollectors.abandonByAddress(addr);
    }
  },
});

const app = createApp({ auth, db, sidecarRouter, eventCollectors });

app.get(
  "/api/sidecars/ws",
  upgradeWebSocket((_c) => {
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
);

const port = Number(process.env["PORT"] ?? 3000);

log.info("Starting server on port {port}", { port });

export default {
  fetch: app.fetch,
  websocket,
  port,
  idleTimeout: 0,
};
