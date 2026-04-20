import { eq } from "drizzle-orm";
import { createDB, createGrantStore } from "@interchange/db";
import { agent } from "@interchange/db/schema";
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
const grantStore = createGrantStore(db);

function parseAgentId(agentAddress: string): string {
  const atIdx = agentAddress.indexOf("@");
  if (atIdx === -1) {
    throw new Error(`Invalid agent address: "${agentAddress}"`);
  }
  return agentAddress.substring(0, atIdx);
}

const sidecarRouter = createSidecarRouter({
  onAgentEvent(_agentAddress, sessionId, event) {
    eventCollectors.dispatch(sessionId, event as InferenceEvent);
  },
  onSidecarDisconnect(agentAddresses) {
    for (const addr of agentAddresses) {
      eventCollectors.abandonByAddress(addr);
    }
  },
  async onAgentDeployAck(agentAddress, publicKey) {
    const agentId = parseAgentId(agentAddress);
    const rows = await db
      .update(agent)
      .set({ publicKey })
      .where(eq(agent.id, agentId))
      .returning({ id: agent.id });
    if (rows.length === 0) {
      throw new Error(`Agent "${agentId}" not found in database`);
    }
  },
  async onAgentReconnected(agentAddress) {
    const agentId = parseAgentId(agentAddress);
    const row = await db.query.agent.findFirst({
      where: eq(agent.id, agentId),
    });
    if (!row) {
      throw new Error(
        `Agent "${agentAddress}" reconnected but not found in database`,
      );
    }
    if (!row.sessionId) {
      throw new Error(
        `Agent "${agentAddress}" reconnected but has no active session`,
      );
    }
    // Refresh grants before creating any local state. If this fails,
    // the address is rejected and nothing needs cleanup.
    const grants = await grantStore.collectGrants(
      row.principalId,
      row.tenantId,
    );
    await sidecarRouter.sendGrantsUpdate(agentAddress, grants);

    if (row.status !== "running") {
      await db
        .update(agent)
        .set({ status: "running", updatedAt: new Date() })
        .where(eq(agent.id, agentId));
    }
    if (!eventCollectors.has(row.sessionId)) {
      eventCollectors.create(row.sessionId, row.tenantId, agentAddress);
      log.info(
        "Restored event collector for reconnected agent {agentAddress} session {sessionId}",
        { agentAddress, sessionId: row.sessionId },
      );
    }
  },
  async lookupPublicKey(agentAddress) {
    const agentId = parseAgentId(agentAddress);
    const row = await db
      .select({ publicKey: agent.publicKey })
      .from(agent)
      .where(eq(agent.id, agentId))
      .limit(1)
      .then((rows) => rows[0]);
    return row?.publicKey ?? null;
  },
});

const app = createApp({
  auth,
  db,
  sidecarRouter,
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
