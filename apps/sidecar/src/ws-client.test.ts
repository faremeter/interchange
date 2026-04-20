/* eslint-disable @typescript-eslint/no-non-null-assertion -- refs[0]! always follows expect(refs).toHaveLength(1) */
import { describe, test, expect, afterAll } from "bun:test";
import { Hono } from "hono";
import { upgradeWebSocket, websocket } from "hono/bun";
import {
  createSidecarRouter,
  type SidecarRouter,
  type WsHandle,
} from "@interchange/hub";
import { createInMemoryTransport } from "@interchange/message-memory";
import type { HarnessConfig, InboundMessage } from "@interchange/types/runtime";

import { createWsClient } from "./ws-client";
import type { SessionManager } from "./session-manager";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 2000,
): Promise<void> {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    }
    await new Promise((r) => setTimeout(r, 20));
  }
}

type DeliveredMessage = { agentAddress: string; message: InboundMessage };

function createMockSessionManager(): SessionManager & {
  created: HarnessConfig[];
  destroyed: string[];
  aborted: { address: string; reason: string }[];
  delivered: DeliveredMessage[];
  addresses: string[];
  shouldThrow: string | null;
} {
  const mock = {
    created: [] as HarnessConfig[],
    destroyed: [] as string[],
    aborted: [] as { address: string; reason: string }[],
    delivered: [] as DeliveredMessage[],
    addresses: [] as string[],
    shouldThrow: null as string | null,

    async createSession(config: HarnessConfig) {
      if (mock.shouldThrow !== null) throw new Error(mock.shouldThrow);
      mock.created.push(config);
      mock.addresses.push(config.agentAddress);
      return {
        sessionId: "mock-session-id",
        publicKey: "deadbeef",
        keyPair: {
          publicKey: new Uint8Array(32),
          privateKey: new Uint8Array(32),
        },
      };
    },
    destroySession(agentAddress: string): void {
      if (mock.shouldThrow !== null) throw new Error(mock.shouldThrow);
      mock.destroyed.push(agentAddress);
      mock.addresses = mock.addresses.filter((a) => a !== agentAddress);
    },
    abortSession(agentAddress: string, reason: string): void {
      if (mock.shouldThrow !== null) throw new Error(mock.shouldThrow);
      mock.aborted.push({ address: agentAddress, reason });
      mock.addresses = mock.addresses.filter((a) => a !== agentAddress);
    },
    deliverMessage(agentAddress: string, message: InboundMessage): void {
      if (mock.shouldThrow !== null) throw new Error(mock.shouldThrow);
      mock.delivered.push({ agentAddress, message });
    },
    hasSession(agentAddress: string): boolean {
      return mock.addresses.includes(agentAddress);
    },
    getAddresses(): string[] {
      return [...mock.addresses];
    },
    async restoreSessions() {
      return { restored: [], failed: [] };
    },
  };
  return mock;
}

const TEST_CONFIG: HarnessConfig = {
  sessionId: "ses_test-session-1",
  agentId: "agent-1",
  tenantId: "tenant-1",
  principalId: "prin_test-principal-1",
  agentAddress: "agent-1@test.interchange",
  systemPrompt: "You are a test agent",
  tools: [],
  grants: [],
  providers: [
    {
      provider: "anthropic",
      baseURL: "https://api.anthropic.com",
      apiKey: "sk-test",
    },
  ],
  defaultModel: "claude-sonnet-4-20250514",
};

const VALID_MESSAGE = new TextEncoder().encode(
  [
    "From: external@remote.interchange",
    "To: agent-1@test.interchange",
    "Date: Thu, 17 Apr 2026 12:00:00 +0000",
    "Message-ID: <test-1@remote.interchange>",
    "Subject: Hello from hub",
    "Content-Type: text/plain",
    "",
    "Test body",
  ].join("\r\n"),
);

function uint8ArrayToBase64(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

// ---------------------------------------------------------------------------
// Test server
// ---------------------------------------------------------------------------

type TestEnv = {
  server: ReturnType<typeof Bun.serve>;
  router: SidecarRouter;
  agentEvents: { addr: string; sid: string; event: unknown }[];
  outboundMail: { rawMessage: string; recipients: string[] }[];
};

function startTestServer(): TestEnv {
  const agentEvents: TestEnv["agentEvents"] = [];
  const outboundMail: TestEnv["outboundMail"] = [];

  const router = createSidecarRouter({
    requestTimeoutMs: 5000,
    onAgentEvent(addr, sid, event) {
      agentEvents.push({ addr, sid, event });
    },
    onMailOutbound(rawMessage, recipients) {
      outboundMail.push({ rawMessage, recipients });
    },
  });

  const app = new Hono();
  app.get(
    "/ws",
    upgradeWebSocket((_c) => {
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
          router.handleOpen(handle);
        },
        onMessage(evt, _ws) {
          if (typeof evt.data === "string") {
            router.handleMessage(handle, evt.data);
          }
        },
        onClose(_evt, _ws) {
          router.handleClose(handle);
        },
      };
    }),
  );

  const server = Bun.serve({
    fetch: app.fetch,
    websocket,
    port: 0,
  });

  return { server, router, agentEvents, outboundMail };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const env = startTestServer();

afterAll(() => {
  env.server.stop(true);
});

describe("sidecar↔hub integration", () => {
  test("sidecar registers with hub on connect", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const client = createWsClient({
      hubUrl: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "test-sidecar",
      token: "test-token",

      transport,
      sessions,
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getConnectedSidecars().includes("test-sidecar"),
      );
      expect(env.router.getConnectedSidecars()).toContain("test-sidecar");
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("test-sidecar"),
      );
    }
  });

  test("hub sends session.create and sidecar acks", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const client = createWsClient({
      hubUrl: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-create",
      token: "test-token",

      transport,
      sessions,
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getConnectedSidecars().includes("sc-create"),
      );

      await env.router.sendAgentDeploy("agent-1@test.interchange", TEST_CONFIG);

      expect(sessions.created).toHaveLength(1);
      expect(sessions.created[0]?.agentAddress).toBe(
        "agent-1@test.interchange",
      );
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-create"),
      );
    }
  });

  test("hub receives session.error when create fails", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    sessions.shouldThrow = "provider not configured";
    const client = createWsClient({
      hubUrl: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-fail",
      token: "test-token",

      transport,
      sessions,
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getConnectedSidecars().includes("sc-fail"),
      );

      await expect(
        env.router.sendAgentDeploy("fail-agent@test", TEST_CONFIG),
      ).rejects.toThrow("provider not configured");
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-fail"),
      );
    }
  });

  test("sidecar forwards agent events to hub", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const startLength = env.agentEvents.length;
    const client = createWsClient({
      hubUrl: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-events",
      token: "test-token",

      transport,
      sessions,
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getConnectedSidecars().includes("sc-events"),
      );

      client.sendEvent("agent-1@test.interchange", "sess-1", {
        type: "reactor.start",
        seq: 0,
        data: {},
      });

      await waitFor(() => env.agentEvents.length > startLength);
      const event = env.agentEvents[env.agentEvents.length - 1];
      expect(event?.addr).toBe("agent-1@test.interchange");
      expect(event?.sid).toBe("sess-1");
      expect(event?.event).toEqual({
        type: "reactor.start",
        seq: 0,
        data: {},
      });
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-events"),
      );
    }
  });

  test("hub routes mail inbound to sidecar", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    // The agent must be in the session manager's address list so the
    // register frame includes it in the routing table.
    sessions.addresses.push("agent-1@test.interchange");
    const { generateKeyPair, createNodeCrypto } = await import(
      "@interchange/crypto-node"
    );
    const kp = await generateKeyPair();
    transport.registerAgent("agent-1@test.interchange", createNodeCrypto(kp));

    const client = createWsClient({
      hubUrl: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-mail-in",
      token: "test-token",

      transport,
      sessions,
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getRoutableAddresses().includes("agent-1@test.interchange"),
      );

      const encoded = uint8ArrayToBase64(VALID_MESSAGE);
      const routed = env.router.routeMail("agent-1@test.interchange", encoded);
      expect(routed).toBe(true);

      // Wait for the message to be delivered to the agent's INBOX.
      const agentTransport = transport.getTransportForAgent(
        "agent-1@test.interchange",
      );
      await waitFor(async () => {
        const refs = await agentTransport.search("INBOX", {});
        return refs.length > 0;
      });

      const refs = await agentTransport.search("INBOX", {});
      expect(refs).toHaveLength(1);
      const headers = await agentTransport.fetchHeaders(refs[0]!);
      expect(headers.from).toBe("external@remote.interchange");
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-mail-in"),
      );
    }
  });

  test("sidecar forwards outbound mail to hub", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const { generateKeyPair, createNodeCrypto } = await import(
      "@interchange/crypto-node"
    );
    const kp = await generateKeyPair();
    transport.registerAgent("sender@test.interchange", createNodeCrypto(kp));

    const startLength = env.outboundMail.length;
    const client = createWsClient({
      hubUrl: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-mail-out",
      token: "test-token",

      transport,
      sessions,
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getConnectedSidecars().includes("sc-mail-out"),
      );

      const senderTransport = transport.getTransportForAgent(
        "sender@test.interchange",
      );
      await senderTransport.send({
        to: "remote@other.interchange",
        type: "conversation.message",
        content: "Hello from sidecar",
      });

      await waitFor(() => env.outboundMail.length > startLength);
      const mail = env.outboundMail[env.outboundMail.length - 1];
      expect(mail?.recipients).toEqual(["remote@other.interchange"]);
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-mail-out"),
      );
    }
  });

  test("mail routes between two sidecars via hub", async () => {
    const { generateKeyPair, createNodeCrypto } = await import(
      "@interchange/crypto-node"
    );

    // Sidecar A
    const transportA = createInMemoryTransport();
    const sessionsA = createMockSessionManager();
    sessionsA.addresses.push("alice@test.interchange");
    const kpA = await generateKeyPair();
    transportA.registerAgent("alice@test.interchange", createNodeCrypto(kpA));

    // Sidecar B
    const transportB = createInMemoryTransport();
    const sessionsB = createMockSessionManager();
    sessionsB.addresses.push("bob@test.interchange");
    const kpB = await generateKeyPair();
    transportB.registerAgent("bob@test.interchange", createNodeCrypto(kpB));

    const clientA = createWsClient({
      hubUrl: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-alice",
      token: "test-token",
      transport: transportA,
      sessions: sessionsA,
    });
    const clientB = createWsClient({
      hubUrl: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-bob",
      token: "test-token",
      transport: transportB,
      sessions: sessionsB,
    });

    clientA.connect();
    clientB.connect();
    try {
      await waitFor(() =>
        env.router.getRoutableAddresses().includes("alice@test.interchange"),
      );
      await waitFor(() =>
        env.router.getRoutableAddresses().includes("bob@test.interchange"),
      );

      // Alice sends a message to Bob.
      const aliceTransport = transportA.getTransportForAgent(
        "alice@test.interchange",
      );
      await aliceTransport.send({
        to: "bob@test.interchange",
        type: "conversation.message",
        content: "Hello Bob",
      });

      // Bob should receive it in his INBOX.
      const bobTransport = transportB.getTransportForAgent(
        "bob@test.interchange",
      );
      await waitFor(async () => {
        const refs = await bobTransport.search("INBOX", {});
        return refs.length > 0;
      });

      const refs = await bobTransport.search("INBOX", {});
      expect(refs).toHaveLength(1);
      const headers = await bobTransport.fetchHeaders(refs[0]!);
      expect(headers.from).toBe("alice@test.interchange");
    } finally {
      clientA.close();
      clientB.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-alice"),
      );
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-bob"),
      );
    }
  });

  test("hub sends message.send and sidecar delivers to session", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    sessions.addresses.push("agent-1@test.interchange");
    const client = createWsClient({
      hubUrl: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-msg-send",
      token: "test-token",

      transport,
      sessions,
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getRoutableAddresses().includes("agent-1@test.interchange"),
      );

      await env.router.sendMessage(
        "agent-1@test.interchange",
        "ses_test",
        "Hello agent",
      );

      expect(sessions.delivered).toHaveLength(1);
      const { agentAddress, message } = sessions.delivered[0]!;
      expect(agentAddress).toBe("agent-1@test.interchange");
      expect(message.content).toBe("Hello agent");
      expect(message.headers.to).toEqual(["agent-1@test.interchange"]);
      expect(message.headers.interchangeSessionId).toBe("ses_test");
      expect(message.signatureStatus).toBe("missing");
      expect(message.ref.mailbox).toBe("SYNTHETIC");
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-msg-send"),
      );
    }
  });

  test("message.send returns error when no session exists", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    sessions.addresses.push("agent-1@test.interchange");
    sessions.shouldThrow =
      'No session exists for agent "agent-1@test.interchange"';
    const client = createWsClient({
      hubUrl: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-msg-err",
      token: "test-token",

      transport,
      sessions,
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getRoutableAddresses().includes("agent-1@test.interchange"),
      );

      await expect(
        env.router.sendMessage("agent-1@test.interchange", "ses_test", "Hello"),
      ).rejects.toThrow("No session exists");

      expect(sessions.delivered).toHaveLength(0);
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-msg-err"),
      );
    }
  });

  test("disconnect cleans up routing table", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    sessions.addresses.push("tracked@test.interchange");

    const client = createWsClient({
      hubUrl: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-disconnect",
      token: "test-token",

      transport,
      sessions,
    });

    client.connect();
    await waitFor(() =>
      env.router.getConnectedSidecars().includes("sc-disconnect"),
    );
    expect(env.router.getRoutableAddresses()).toContain(
      "tracked@test.interchange",
    );

    client.close();
    await waitFor(
      () => !env.router.getConnectedSidecars().includes("sc-disconnect"),
    );
    expect(env.router.getRoutableAddresses()).not.toContain(
      "tracked@test.interchange",
    );
  });

  test("sidecar sends pings and hub responds with pongs", async () => {
    const pingEnv = startTestServer();

    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();

    const client = createWsClient({
      hubUrl: `ws://localhost:${pingEnv.server.port}/ws`,
      sidecarId: "sc-ping",
      token: "test-token",

      transport,
      sessions,
      pingIntervalMs: 100,
    });

    try {
      client.connect();
      await waitFor(() =>
        pingEnv.router.getConnectedSidecars().includes("sc-ping"),
      );

      // Wait long enough for at least one ping/pong round trip.
      await new Promise((r) => setTimeout(r, 250));

      // The sidecar should still be connected (pongs keep it alive).
      expect(pingEnv.router.getConnectedSidecars()).toContain("sc-ping");
    } finally {
      client.close();
      pingEnv.server.stop(true);
    }
  });
});
