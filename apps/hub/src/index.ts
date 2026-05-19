import { eq, and, isNull } from "drizzle-orm";
import {
  createDB,
  createGrantStore,
  resolveInstanceProviders,
} from "@interchange/db";
import { agentInstance, sessionMail } from "@interchange/db/schema";
import {
  createAgentRepoStore,
  createApp,
  createAuth,
  createEventCollectorRegistry,
  createSessionService,
  createSidecarRouter,
  generateId,
} from "@interchange/hub";
import { generateKeyPair } from "@interchange/crypto-node";
import { parseMailToEmail } from "@interchange/mime";
import { parseInferenceEvent } from "@interchange/types/runtime";
import { upgradeWebSocket, websocket } from "hono/bun";
import { setup, getLogger } from "@interchange/log";
import { type } from "arktype";

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
    where: and(
      eq(agentInstance.address, agentAddress),
      isNull(agentInstance.endedAt),
    ),
  });
  if (!row) {
    throw new Error(`No active instance found for address "${agentAddress}"`);
  }
  return row;
}

const sidecarRouter = createSidecarRouter({
  hubPublicKey: hexEncode(hubSigningKey.publicKey),
  onAgentEvent(agentAddress, _sessionId, event) {
    const validated = parseInferenceEvent(event);
    if (validated instanceof type.errors) {
      log.warn("Received invalid agent event for {agentAddress}: {summary}", {
        agentAddress,
        summary: validated.summary,
      });
      return;
    }
    eventCollectors.dispatch(agentAddress, validated);
  },
  onSidecarDisconnect(agentAddresses) {
    for (const addr of agentAddresses) {
      eventCollectors.abandon(addr);
    }
  },
  async onAgentDeployAck(agentAddress, publicKey) {
    const instance = await requireInstance(agentAddress);

    await db
      .update(agentInstance)
      .set({ publicKey })
      .where(eq(agentInstance.id, instance.id));
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

    // Re-resolve and push credentials so the agent picks up any
    // rotations that happened while the sidecar was disconnected.
    // Fail-open: a stale credential causes runtime 401s, not a
    // security escalation, so we log rather than reject the reconnect.
    try {
      const providers = await resolveInstanceProviders(
        db,
        instance.tenantId,
        instance,
      );
      if (providers.length > 0) {
        await sidecarRouter.sendProvidersUpdate(agentAddress, providers);
      }
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      log.warn(
        "Failed to push credentials on reconnect for {agentAddress}: {msg}",
        { agentAddress, msg },
      );
    }

    const now = new Date();
    if (instance.status !== "running") {
      await db
        .update(agentInstance)
        .set({ status: "running", updatedAt: now })
        .where(eq(agentInstance.id, instance.id));
    }
    if (!eventCollectors.has(agentAddress)) {
      eventCollectors.create(
        agentAddress,
        instance.tenantId,
        sessionId,
        instance.id,
      );
      log.info(
        "Restored event collector for reconnected agent {agentAddress}",
        { agentAddress },
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
      .where(
        and(
          eq(agentInstance.address, agentAddress),
          isNull(agentInstance.endedAt),
        ),
      )
      .limit(1)
      .then((rows) => rows[0]);
    if (!row) {
      return null;
    }
    return row.publicKey;
  },
  async onMailPersist({ senderAddress, recipients, raw }) {
    const senderInstance = await requireInstance(senderAddress);
    if (!senderInstance.sessionId) {
      throw new Error(
        `Instance ${senderInstance.id} has no session for address "${senderAddress}"`,
      );
    }
    const createdAt = new Date();

    // Outbound record on the sender's session.
    const outboundId = generateId("sessionMail");
    const outboundRecord = {
      id: outboundId,
      sessionId: senderInstance.sessionId,
      instanceId: senderInstance.id,
      tenantId: senderInstance.tenantId,
      direction: "outbound" as const,
      status: "delivered" as const,
      raw,
      createdAt,
    };

    // Inbound records for each recipient that has an active agent instance.
    // Recipients that are not agent instances (e.g. human user addresses)
    // are skipped.
    const recipientResults = await Promise.all(
      recipients.map(async (addr) => {
        const row = await db.query.agentInstance.findFirst({
          where: and(
            eq(agentInstance.address, addr),
            isNull(agentInstance.endedAt),
          ),
        });
        if (row === undefined) {
          return null;
        }
        if (row.sessionId === null) {
          log.warn`Active instance ${row.id} for "${addr}" has no session; skipping inbound record`;
          return null;
        }
        return { addr, instance: row, sessionId: row.sessionId };
      }),
    );
    const recipientInstances = recipientResults.filter(
      (r): r is NonNullable<typeof r> => r !== null,
    );

    const inboundEntries = recipientInstances.map(
      ({ addr, instance, sessionId }) => {
        const id = generateId("sessionMail");
        return {
          record: {
            id,
            sessionId,
            instanceId: instance.id,
            tenantId: instance.tenantId,
            direction: "inbound" as const,
            status: "delivered" as const,
            raw,
            createdAt,
          },
          result: {
            id,
            direction: "inbound" as const,
            instanceId: instance.id,
            address: addr,
            createdAt,
          },
        };
      },
    );

    await db
      .insert(sessionMail)
      .values([outboundRecord, ...inboundEntries.map((e) => e.record)]);

    return [
      {
        id: outboundId,
        direction: "outbound" as const,
        instanceId: senderInstance.id,
        address: senderInstance.address,
        createdAt,
      },
      ...inboundEntries.map((e) => e.result),
    ];
  },
  onMailPersisted(row) {
    const parsed = parseMailToEmail(row.raw, row.id);
    sidecarRouter.dispatchAgentEvent(row.address, {
      type: "mail.delivered",
      data: {
        ...parsed,
        id: row.id,
        direction: row.direction,
        receivedAt: row.createdAt.toISOString(),
      },
    });
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
