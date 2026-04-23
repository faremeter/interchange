import { eq } from "drizzle-orm";
import { createDB, createGrantStore } from "@interchange/db";
import { agent, agentInstance } from "@interchange/db/schema";
import {
  createAgentRepoStore,
  createApp,
  createAuth,
  createEventCollectorRegistry,
  createSessionService,
  createSidecarRouter,
} from "@interchange/hub";
import { generateKeyPair } from "@interchange/crypto-node";
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

function parseAgentId(agentAddress: string): string {
  const atIdx = agentAddress.indexOf("@");
  if (atIdx === -1) {
    throw new Error(`Invalid agent address: "${agentAddress}"`);
  }
  return agentAddress.substring(0, atIdx);
}

async function requireInstance(agentAddress: string) {
  const row = await db.query.agentInstance.findFirst({
    where: eq(agentInstance.address, agentAddress),
  });
  if (!row) {
    throw new Error(`No agent instance found for address "${agentAddress}"`);
  }
  return row;
}

const sidecarRouter = createSidecarRouter({
  hubPublicKey: hexEncode(hubSigningKey.publicKey),
  onAgentEvent(_agentAddress, sessionId, event) {
    eventCollectors.dispatch(sessionId, event as InferenceEvent);
  },
  onSidecarDisconnect(agentAddresses) {
    for (const addr of agentAddresses) {
      eventCollectors.abandonByAddress(addr);
    }
  },
  async onAgentDeployAck(agentAddress, publicKey) {
    const instance = await requireInstance(agentAddress);

    await db
      .update(agentInstance)
      .set({ publicKey })
      .where(eq(agentInstance.id, instance.id));

    // Dual-write: keep agent table in sync until routes are migrated
    await db
      .update(agent)
      .set({ publicKey })
      .where(eq(agent.id, instance.agentId));
  },
  async onAgentReconnected(agentAddress) {
    const instance = await requireInstance(agentAddress);

    if (!instance.sessionId) {
      throw new Error(
        `Agent "${agentAddress}" reconnected but has no active session`,
      );
    }
    const sessionId = instance.sessionId;

    // Refresh grants before creating any local state. If this fails,
    // the address is rejected and nothing needs cleanup.
    const grants = await grantStore.collectGrants(
      instance.principalId,
      instance.tenantId,
    );
    await sidecarRouter.sendGrantsUpdate(agentAddress, grants);

    const now = new Date();
    if (instance.status !== "running") {
      await db
        .update(agentInstance)
        .set({ status: "running", updatedAt: now })
        .where(eq(agentInstance.id, instance.id));
    }
    // Dual-write: always sync agent table since instance and agent
    // status can diverge during the migration
    const agentRow = await db.query.agent.findFirst({
      where: eq(agent.id, instance.agentId),
    });
    if (!agentRow) {
      log.warn(
        "Agent definition missing for instance {agentAddress}, skipping dual-write",
        { agentAddress },
      );
    } else if (agentRow.status !== "running") {
      await db
        .update(agent)
        .set({ status: "running", updatedAt: now })
        .where(eq(agent.id, instance.agentId));
    }
    if (!eventCollectors.has(sessionId)) {
      eventCollectors.create(sessionId, instance.tenantId, agentAddress);
      log.info(
        "Restored event collector for reconnected agent {agentAddress} session {sessionId}",
        { agentAddress, sessionId },
      );
    }
  },
  async onStatePackReceived(agentAddress, pack, ref, commitSha) {
    const agentId = parseAgentId(agentAddress);
    try {
      await agentRepoStore.receiveStatePack(agentId, pack, ref, commitSha);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.startsWith("path_violation")) {
        log.warn("State pack rejected for {agentAddress}: {msg}", {
          agentAddress,
          msg,
        });
        return { accepted: false, reason: "path_violation" as const };
      }
      throw err;
    }
    return { accepted: true };
  },
  async lookupDeployRef(agentAddress) {
    const agentId = parseAgentId(agentAddress);
    return agentRepoStore.getDeployRef(agentId);
  },
  async onDeployRefStale(agentAddress) {
    const agentId = parseAgentId(agentAddress);
    const { pack, commitSha, ref } =
      await agentRepoStore.createDeployPack(agentId);
    await sidecarRouter.sendPack(agentAddress, pack, ref, commitSha);
    log.info("Re-deployed stale agent {agentAddress}", { agentAddress });
  },
  async lookupPublicKey(agentAddress) {
    const row = await db
      .select({ publicKey: agentInstance.publicKey })
      .from(agentInstance)
      .where(eq(agentInstance.address, agentAddress))
      .limit(1)
      .then((rows) => rows[0]);
    if (!row) {
      return null;
    }
    return row.publicKey;
  },
});

const sessionService = createSessionService({
  sidecarRouter,
  agentRepoStore,
});

const app = createApp({
  auth,
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
