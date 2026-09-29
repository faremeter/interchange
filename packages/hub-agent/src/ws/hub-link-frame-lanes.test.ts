import { afterAll, describe, expect, test } from "bun:test";
import { type } from "arktype";
import { Hono } from "hono";
import { upgradeWebSocket, websocket } from "hono/bun";

import { createInMemoryTransport } from "@intx/mail-memory";
import type { HarnessConfig } from "@intx/types/runtime";

import type { AgentKeyStore } from "../agent-key-store";
import type { SessionManager } from "../session-manager";
import { createHubLink, type HubLinkConfig } from "./hub-link";
import { resolveInboundMailPolicy } from "./inbound-signature";

// One sidecar link hosting several deployments. The fake hub records every
// frame the sidecar sends and answers its pings; each test holds one frame's
// handler open and checks what the link still handles meanwhile.

const ReceivedFrame = type({
  type: "string",
  "sidecarId?": "string",
  "agentAddress?": "string",
  "requestId?": "string",
});
type ReceivedFrame = typeof ReceivedFrame.infer;

type HubConnection = {
  send(frame: unknown): void;
  frame(match: (frame: ReceivedFrame) => boolean): Promise<ReceivedFrame>;
  received(): readonly ReceivedFrame[];
  isOpen(): boolean;
};

function startHub() {
  const connections = new Map<string, PromiseWithResolvers<HubConnection>>();
  function slot(sidecarId: string): PromiseWithResolvers<HubConnection> {
    const existing = connections.get(sidecarId);
    if (existing !== undefined) return existing;
    const created = Promise.withResolvers<HubConnection>();
    connections.set(sidecarId, created);
    return created;
  }

  const app = new Hono();
  app.get(
    "/ws",
    upgradeWebSocket(() => {
      const received: ReceivedFrame[] = [];
      const waiters: {
        match: (frame: ReceivedFrame) => boolean;
        resolve: (frame: ReceivedFrame) => void;
      }[] = [];
      let open = true;
      let registered = false;
      return {
        onMessage(evt, ws) {
          if (typeof evt.data !== "string") return;
          const frame = ReceivedFrame(JSON.parse(evt.data));
          if (frame instanceof type.errors) return;
          if (frame.type === "ping") ws.send(JSON.stringify({ type: "pong" }));
          received.push(frame);
          for (const waiter of [...waiters]) {
            if (!waiter.match(frame)) continue;
            waiters.splice(waiters.indexOf(waiter), 1);
            waiter.resolve(frame);
          }
          if (registered || frame.sidecarId === undefined) return;
          registered = true;
          slot(frame.sidecarId).resolve({
            send: (sent) => {
              ws.send(JSON.stringify(sent));
            },
            frame: (match) => {
              const found = received.find(match);
              if (found !== undefined) return Promise.resolve(found);
              const waiter = Promise.withResolvers<ReceivedFrame>();
              waiters.push({ match, resolve: waiter.resolve });
              return waiter.promise;
            },
            received: () => received,
            isOpen: () => open,
          });
        },
        onClose() {
          open = false;
        },
      };
    }),
  );
  const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });
  return {
    url: `ws://localhost:${String(server.port)}/ws`,
    connection: (sidecarId: string) => slot(sidecarId).promise,
    stop: () => server.stop(true),
  };
}

const hub = startHub();

afterAll(async () => {
  await hub.stop();
});

function config(agentAddress: string): HarnessConfig {
  return {
    sessionId: "ses-lanes",
    agentId: "workflow",
    tenantId: "tenant-1",
    principalId: "principal-1",
    agentAddress,
    systemPrompt: "test",
    tools: [],
    grants: [],
    sources: [
      {
        id: "test-source",
        provider: "test",
        baseURL: "https://api.example.test",
        credentialId: "test-credential",
        model: "test",
      },
    ],
    defaultSource: "test-source",
  };
}

let deploys = 0;
function deploy(agentAddress: string) {
  deploys += 1;
  return {
    type: "agent.deploy",
    requestId: `deploy-${String(deploys)}`,
    agentAddress,
    agentId: "workflow",
    config: config(agentAddress),
    hubPublicKey: "a".repeat(64),
  };
}

let probes = 0;
function probeRequest() {
  probes += 1;
  return {
    type: "workflow.probe.request",
    requestId: `probe-${String(probes)}`,
    source: { kind: "registry", registry: "npmjs" },
    closure: {
      schemaVersion: "1",
      topLevel: [{ name: "@acme/workflow", version: "1.0.0" }],
      entries: [
        {
          name: "@acme/workflow",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "npmjs",
            integrity: "sha512-deadbeef",
          },
        },
      ],
    },
    entry: "./workflow.js",
  };
}

function runGrants(agentAddress: string) {
  return {
    type: "run.grants",
    agentAddress,
    runId: `run-of-${agentAddress}`,
    stepGrants: [],
  };
}

function hold() {
  const released = Promise.withResolvers<boolean>();
  return { released: released.promise, release: () => released.resolve(true) };
}

function keyStore(): AgentKeyStore {
  return {
    async loadOrGenerateKey() {
      throw new Error("unused");
    },
    recordHubKey() {
      // These deployments verify no deploy commits.
    },
    async verifyDeployCommit() {
      return false;
    },
    forgetAgent() {
      // No key was ever cached for an address.
    },
  };
}

function sessions(): SessionManager {
  return {
    initRepo: () => Promise.resolve(),
    getAddresses: () => [],
    applyDeployPack: () => Promise.resolve(),
    applyAssetPack: () => Promise.resolve(),
    deleteAgentDir: () => Promise.resolve(),
    getSessionId: () => undefined,
  };
}

async function connectLink(
  sidecarId: string,
  overrides: Partial<HubLinkConfig>,
): Promise<{ link: ReturnType<typeof createHubLink>; hub: HubConnection }> {
  const link = createHubLink({
    hubURL: hub.url,
    sidecarId,
    token: "token",
    transport: createInMemoryTransport(),
    sessions: sessions(),
    keyStore: keyStore(),
    resolveSenderCrypto: () => undefined,
    lookupInboundMailPolicy: () => resolveInboundMailPolicy(undefined),
    cacheSenderKey: async () => undefined,
    evictSenderKey: async () => undefined,
    deployRouter: {
      async deploy() {
        return { publicKey: "ab".repeat(32) };
      },
    },
    ...overrides,
  });
  link.connect();
  return { link, hub: await hub.connection(sidecarId) };
}

const isAck = (agentAddress: string) => (frame: ReceivedFrame) =>
  frame.type === "agent.deploy.ack" && frame.agentAddress === agentAddress;

const SLOW = "run_slow@tenant.example";
const OTHER = "run_other@tenant.example";
const SENDER = "sender@tenant.example";

function senderKeyRefresh(keyByte: string) {
  return {
    type: "sender.key.refresh",
    address: SENDER,
    publicKey: keyByte.repeat(32),
  };
}

describe("a sidecar link hosting several deployments", () => {
  test("deploys one deployment while another's deploy is still running", async () => {
    const slow = hold();
    const events: string[] = [];
    const { link, hub: conn } = await connectLink("sc-lanes-deploy", {
      deployRouter: {
        async deploy(frame) {
          events.push(`deploy ${frame.agentAddress}`);
          if (frame.agentAddress === SLOW) await slow.released;
          return { publicKey: "ab".repeat(32) };
        },
      },
    });
    try {
      conn.send(deploy(SLOW));
      conn.send(deploy(OTHER));

      await conn.frame(isAck(OTHER));
      expect(events).toEqual([`deploy ${SLOW}`, `deploy ${OTHER}`]);
      expect(conn.received().some(isAck(SLOW))).toBe(false);

      slow.release();
      await conn.frame(isAck(SLOW));
    } finally {
      link.close();
    }
  });

  test("keeps answering the heartbeat while a deploy is running", async () => {
    const slow = hold();
    const { link, hub: conn } = await connectLink("sc-lanes-heartbeat", {
      pingIntervalMs: 100,
      deployRouter: {
        async deploy() {
          await slow.released;
          return { publicKey: "ab".repeat(32) };
        },
      },
    });
    try {
      conn.send(deploy(SLOW));

      // The link drops a connection at the second tick without a handled
      // pong, so a third ping on it means every pong was handled in time.
      let pings = 0;
      await conn.frame((frame) => frame.type === "ping" && ++pings === 3);
      expect(conn.isOpen()).toBe(true);
      expect(conn.received().some(isAck(SLOW))).toBe(false);

      slow.release();
      await conn.frame(isAck(SLOW));
    } finally {
      link.close();
    }
  });

  test("deploys while a probe is still running", async () => {
    const probing = hold();
    const { link, hub: conn } = await connectLink("sc-lanes-probe", {
      workflowProbeExecutor: {
        async probe() {
          await probing.released;
          throw new Error("probe released");
        },
      },
    });
    try {
      const request = probeRequest();
      conn.send(request);
      conn.send(deploy(OTHER));

      await conn.frame(isAck(OTHER));
      expect(
        conn.received().some((frame) => frame.requestId === request.requestId),
      ).toBe(false);

      probing.release();
      await conn.frame(
        (frame) =>
          frame.type === "workflow.probe.error" &&
          frame.requestId === request.requestId,
      );
    } finally {
      link.close();
    }
  });

  test("handles one deployment's frames in the order they arrived", async () => {
    const slow = hold();
    const sentinel = Promise.withResolvers<boolean>();
    const events: string[] = [];
    const { link, hub: conn } = await connectLink("sc-lanes-order", {
      deployRouter: {
        async deploy(frame) {
          events.push(`deploy ${frame.agentAddress}`);
          await slow.released;
          events.push(`deployed ${frame.agentAddress}`);
          return { publicKey: "ab".repeat(32) };
        },
        async undeploy(frame) {
          events.push(`undeploy ${frame.agentAddress}`);
        },
      },
      workflowProbeExecutor: {
        async probe() {
          events.push("probe");
          sentinel.resolve(true);
          throw new Error("sentinel");
        },
      },
    });
    try {
      conn.send(deploy(SLOW));
      conn.send({
        type: "agent.undeploy",
        requestId: "undeploy-slow",
        agentAddress: SLOW,
        reason: "test",
      });
      conn.send(probeRequest());

      await sentinel.promise;
      expect(events).toEqual([`deploy ${SLOW}`, "probe"]);

      slow.release();
      await conn.frame(
        (frame) =>
          frame.type === "agent.undeploy.ack" && frame.agentAddress === SLOW,
      );
      expect(events).toEqual([
        `deploy ${SLOW}`,
        "probe",
        `deployed ${SLOW}`,
        `undeploy ${SLOW}`,
      ]);
    } finally {
      link.close();
    }
  });

  test("answers a malformed deploy after the deploys that address received before it", async () => {
    const slow = hold();
    const sentinel = Promise.withResolvers<boolean>();
    const { link, hub: conn } = await connectLink("sc-lanes-malformed", {
      deployRouter: {
        async deploy() {
          await slow.released;
          return { publicKey: "ab".repeat(32) };
        },
      },
      workflowProbeExecutor: {
        async probe() {
          sentinel.resolve(true);
          throw new Error("sentinel");
        },
      },
    });
    try {
      conn.send(deploy(SLOW));
      conn.send({ ...deploy(SLOW), config: {} });
      conn.send(probeRequest());

      await sentinel.promise;
      expect(
        conn.received().some((frame) => frame.type === "agent.deploy.error"),
      ).toBe(false);

      slow.release();
      await conn.frame((frame) => frame.type === "agent.deploy.error");
      const replies = conn
        .received()
        .filter((frame) => frame.agentAddress === SLOW)
        .map((frame) => frame.type);
      expect(replies).toEqual(["agent.deploy.ack", "agent.deploy.error"]);
    } finally {
      link.close();
    }
  });

  test("caches a sender-key refresh while another deployment's deploy is still running", async () => {
    const slow = hold();
    const events: string[] = [];
    const { link, hub: conn } = await connectLink("sc-lanes-refresh", {
      deployRouter: {
        async deploy(frame) {
          events.push(`deploy ${frame.agentAddress}`);
          if (frame.agentAddress === SLOW) await slow.released;
          return { publicKey: "ab".repeat(32) };
        },
      },
      cacheSenderKey: async (address) => {
        events.push(`cache ${address}`);
      },
    });
    try {
      conn.send(deploy(SLOW));
      conn.send(senderKeyRefresh("cd"));
      conn.send(deploy(OTHER));

      await conn.frame(isAck(OTHER));
      expect(events).toEqual([
        `deploy ${SLOW}`,
        `cache ${SENDER}`,
        `deploy ${OTHER}`,
      ]);
      expect(conn.received().some(isAck(SLOW))).toBe(false);

      slow.release();
      await conn.frame(isAck(SLOW));
    } finally {
      link.close();
    }
  });

  test("writes one deployment's run grants while another's are still being written", async () => {
    const slow = hold();
    const otherRouted = Promise.withResolvers<boolean>();
    const events: string[] = [];
    const { link, hub: conn } = await connectLink("sc-lanes-grants", {
      grantsInboundRouter: {
        async tryRoute(frame) {
          events.push(`grants ${frame.agentAddress}`);
          if (frame.agentAddress === SLOW) await slow.released;
          else otherRouted.resolve(true);
          return true;
        },
      },
    });
    try {
      conn.send(runGrants(SLOW));
      conn.send(runGrants(OTHER));

      await otherRouted.promise;
      expect(events).toEqual([`grants ${SLOW}`, `grants ${OTHER}`]);
      slow.release();
    } finally {
      link.close();
    }
  });

  test("caches the sender keys run grants carry in arrival order, ahead of their deployment's earlier frames", async () => {
    const slow = hold();
    const refreshed = Promise.withResolvers<boolean>();
    const routed = Promise.withResolvers<boolean>();
    const events: string[] = [];
    const { link, hub: conn } = await connectLink("sc-lanes-grant-keys", {
      deployRouter: {
        async deploy() {
          await slow.released;
          return { publicKey: "ab".repeat(32) };
        },
      },
      cacheSenderKey: async (address, publicKey) => {
        events.push(`cache ${address} ${publicKey.slice(0, 2)}`);
        if (publicKey.startsWith("22")) refreshed.resolve(true);
      },
      grantsInboundRouter: {
        async tryRoute(frame, senderKeysCached) {
          events.push(
            `grants ${frame.agentAddress} keys cached ${String(senderKeysCached)}`,
          );
          routed.resolve(true);
          return true;
        },
      },
    });
    try {
      conn.send(deploy(SLOW));
      conn.send({
        ...runGrants(SLOW),
        senderIdentities: [{ address: SENDER, publicKey: "11".repeat(32) }],
      });
      conn.send(senderKeyRefresh("22"));

      await refreshed.promise;
      expect(events).toEqual([`cache ${SENDER} 11`, `cache ${SENDER} 22`]);

      slow.release();
      await routed.promise;
      expect(events).toEqual([
        `cache ${SENDER} 11`,
        `cache ${SENDER} 22`,
        `grants ${SLOW} keys cached true`,
      ]);
    } finally {
      link.close();
    }
  });

  test("tells the grants router when a sender key its grants carry did not land", async () => {
    const routed = Promise.withResolvers<boolean>();
    const { link, hub: conn } = await connectLink("sc-lanes-grant-key-fault", {
      cacheSenderKey: async () => {
        throw new Error("sender-key disk full");
      },
      grantsInboundRouter: {
        async tryRoute(_frame, senderKeysCached) {
          routed.resolve(senderKeysCached);
          return true;
        },
      },
    });
    try {
      conn.send({
        ...runGrants(OTHER),
        senderIdentities: [{ address: SENDER, publicKey: "11".repeat(32) }],
      });

      expect(await routed.promise).toBe(false);
    } finally {
      link.close();
    }
  });

  test("skips a malformed sender key its grants carry and caches the others", async () => {
    const routed = Promise.withResolvers<boolean>();
    const cached: string[] = [];
    const { link, hub: conn } = await connectLink("sc-lanes-grant-key-bad", {
      cacheSenderKey: async (address) => {
        cached.push(address);
      },
      grantsInboundRouter: {
        async tryRoute(_frame, senderKeysCached) {
          routed.resolve(senderKeysCached);
          return true;
        },
      },
    });
    try {
      conn.send({
        ...runGrants(OTHER),
        senderIdentities: [
          { address: "bad@tenant.example", publicKey: "11".repeat(16) },
          { address: "good@tenant.example", publicKey: "22".repeat(32) },
        ],
      });

      expect(await routed.promise).toBe(true);
      expect(cached).toEqual(["good@tenant.example"]);
    } finally {
      link.close();
    }
  });

  test("verifies a mail only once the sender-key writes that arrived before it are done", async () => {
    const write = hold();
    const verified = Promise.withResolvers<boolean>();
    const sentinel = Promise.withResolvers<boolean>();
    const events: string[] = [];
    const { link, hub: conn } = await connectLink("sc-lanes-mail-keys", {
      cacheSenderKey: async (address) => {
        events.push(`cache ${address}`);
        await write.released;
        events.push(`cached ${address}`);
      },
      resolveSenderCrypto: (address) => {
        events.push(`verify ${address}`);
        verified.resolve(true);
        return undefined;
      },
      workflowProbeExecutor: {
        async probe() {
          sentinel.resolve(true);
          throw new Error("sentinel");
        },
      },
    });
    try {
      conn.send(senderKeyRefresh("22"));
      conn.send({
        type: "mail.inbound",
        agentAddress: OTHER,
        rawMessage: btoa("Subject: test\r\n\r\nbody"),
        authenticatedSender: SENDER,
      });
      conn.send(probeRequest());

      await sentinel.promise;
      expect(events).toEqual([`cache ${SENDER}`]);

      write.release();
      await verified.promise;
      expect(events).toEqual([
        `cache ${SENDER}`,
        `cached ${SENDER}`,
        `verify ${SENDER}`,
      ]);
    } finally {
      link.close();
    }
  });

  test("drops a mail whose body is not base64 while its deployment's earlier frames still run", async () => {
    const slow = hold();
    const sentinel = Promise.withResolvers<boolean>();
    const { link, hub: conn } = await connectLink("sc-lanes-mail-bad-body", {
      deployRouter: {
        async deploy() {
          await slow.released;
          return { publicKey: "ab".repeat(32) };
        },
      },
      workflowProbeExecutor: {
        async probe() {
          sentinel.resolve(true);
          throw new Error("sentinel");
        },
      },
    });
    try {
      conn.send(deploy(SLOW));
      conn.send({
        type: "mail.inbound",
        agentAddress: SLOW,
        rawMessage: "not base64!",
        authenticatedSender: SENDER,
      });
      conn.send(probeRequest());

      await sentinel.promise;
      await new Promise((resolve) => setTimeout(resolve, 0));
      slow.release();
      await conn.frame(isAck(SLOW));
    } finally {
      link.close();
    }
  });

  test("verifies a mail against the sender keys as of its arrival while its deployment's earlier frames still run", async () => {
    const slow = hold();
    const rotated = Promise.withResolvers<boolean>();
    const cachedKeys = new Map<string, string>();
    const events: string[] = [];
    const { link, hub: conn } = await connectLink("sc-lanes-mail-arrival", {
      deployRouter: {
        async deploy() {
          await slow.released;
          return { publicKey: "ab".repeat(32) };
        },
      },
      cacheSenderKey: async (address, publicKey) => {
        cachedKeys.set(address, publicKey);
        events.push(`cache ${address} ${publicKey.slice(0, 2)}`);
        if (publicKey.startsWith("22")) rotated.resolve(true);
      },
      resolveSenderCrypto: (address) => {
        events.push(
          `verify ${address} with ${cachedKeys.get(address)?.slice(0, 2) ?? "no key"}`,
        );
        return undefined;
      },
    });
    try {
      conn.send(senderKeyRefresh("11"));
      conn.send(deploy(SLOW));
      conn.send({
        type: "mail.inbound",
        agentAddress: SLOW,
        rawMessage: btoa("Subject: test\r\n\r\nbody"),
        authenticatedSender: SENDER,
      });
      conn.send(senderKeyRefresh("22"));

      await rotated.promise;
      expect(events).toEqual([
        `cache ${SENDER} 11`,
        `verify ${SENDER} with 11`,
        `cache ${SENDER} 22`,
      ]);
      expect(conn.received().some(isAck(SLOW))).toBe(false);

      slow.release();
      await conn.frame(isAck(SLOW));
    } finally {
      link.close();
    }
  });
});
