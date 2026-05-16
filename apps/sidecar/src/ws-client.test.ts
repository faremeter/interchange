/* eslint-disable @typescript-eslint/no-non-null-assertion -- refs[0]! always follows expect(refs).toHaveLength(1) */
import { describe, test, expect, afterAll } from "bun:test";
import { Hono } from "hono";
import { upgradeWebSocket, websocket } from "hono/bun";
import {
  createSidecarRouter,
  type SidecarRouter,
  type WsHandle,
} from "@interchange/hub";
import { createInMemoryTransport } from "@interchange/mail-memory";
import type {
  HarnessConfig,
  InboundMessage,
  ProviderConfig,
} from "@interchange/types/runtime";
import type { GrantRule } from "@interchange/types/authz";

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
  provisioned: HarnessConfig[];
  started: string[];
  destroyed: string[];
  aborted: { address: string; reason: string }[];
  delivered: DeliveredMessage[];
  addresses: string[];
  provisionedAddresses: string[];
  shouldThrow: string | null;
} {
  const mock = {
    provisioned: [] as HarnessConfig[],
    started: [] as string[],
    destroyed: [] as string[],
    aborted: [] as { address: string; reason: string }[],
    delivered: [] as DeliveredMessage[],
    addresses: [] as string[],
    provisionedAddresses: [] as string[],
    shouldThrow: null as string | null,

    async provisionAgent(config: HarnessConfig) {
      if (mock.shouldThrow !== null) throw new Error(mock.shouldThrow);
      mock.provisioned.push(config);
      mock.provisionedAddresses.push(config.agentAddress);
      return {
        publicKey: "deadbeef",
        keyPair: {
          publicKey: new Uint8Array(32),
          privateKey: new Uint8Array(32),
        },
      };
    },
    async startSession(agentAddress: string): Promise<void> {
      if (mock.shouldThrow !== null) throw new Error(mock.shouldThrow);
      mock.started.push(agentAddress);
      mock.provisionedAddresses = mock.provisionedAddresses.filter(
        (a) => a !== agentAddress,
      );
      mock.addresses.push(agentAddress);
    },
    async destroySession(agentAddress: string): Promise<void> {
      if (mock.shouldThrow !== null) throw new Error(mock.shouldThrow);
      mock.destroyed.push(agentAddress);
      mock.addresses = mock.addresses.filter((a) => a !== agentAddress);
    },
    async abortSession(agentAddress: string, reason: string): Promise<void> {
      if (mock.shouldThrow !== null) throw new Error(mock.shouldThrow);
      mock.aborted.push({ address: agentAddress, reason });
      mock.addresses = mock.addresses.filter((a) => a !== agentAddress);
    },
    deliverMessage(agentAddress: string, message: InboundMessage): void {
      if (mock.shouldThrow !== null) throw new Error(mock.shouldThrow);
      mock.delivered.push({ agentAddress, message });
    },
    async updateGrants(
      _agentAddress: string,
      _grants: GrantRule[],
    ): Promise<void> {
      if (mock.shouldThrow !== null) throw new Error(mock.shouldThrow);
    },
    async updateProviders(
      _agentAddress: string,
      _providers: ProviderConfig[],
    ): Promise<void> {
      if (mock.shouldThrow !== null) throw new Error(mock.shouldThrow);
    },
    hasSession(agentAddress: string): boolean {
      return mock.addresses.includes(agentAddress);
    },
    isProvisioned(agentAddress: string): boolean {
      return mock.provisionedAddresses.includes(agentAddress);
    },
    getAddresses(): string[] {
      return [...mock.addresses];
    },
    async restoreSessions() {
      return { restored: [], failed: [] };
    },
    applyDeployPack: () => Promise.resolve(),
    createStatePack: () =>
      Promise.resolve({
        pack: new Uint8Array([1, 2, 3]),
        commitSha: "abc123",
        ref: "refs/heads/main",
      }),
    deleteAgentDir: () => Promise.resolve(),
    getDeployRef: (_agentAddress: string) => Promise.resolve(null),
    persistHubPublicKey: (_agentAddress: string, _hubPublicKey: string) =>
      Promise.resolve(),
    commitInboundMail: (_agentAddress: string, _rawMessage: Uint8Array) =>
      Promise.resolve(),
    getSessionId: (_agentAddress: string) => undefined,
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
    hubPublicKey: "a".repeat(64),
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

      expect(sessions.provisioned).toHaveLength(1);
      expect(sessions.provisioned[0]?.agentAddress).toBe(
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

  test("session.start starts the harness", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const client = createWsClient({
      hubUrl: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-start",
      token: "test-token",
      transport,
      sessions,
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getConnectedSidecars().includes("sc-start"),
      );

      await env.router.sendAgentDeploy(
        "start-agent@test.interchange",
        TEST_CONFIG,
      );
      expect(sessions.started).toHaveLength(0);

      await env.router.sendSessionStart("start-agent@test.interchange");
      expect(sessions.started).toHaveLength(1);
      expect(sessions.started[0]).toBe("start-agent@test.interchange");
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-start"),
      );
    }
  });

  test("session.start failure removes agent from routing table", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const client = createWsClient({
      hubUrl: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-start-fail",
      token: "test-token",
      transport,
      sessions,
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getConnectedSidecars().includes("sc-start-fail"),
      );

      await env.router.sendAgentDeploy(
        "start-fail@test.interchange",
        TEST_CONFIG,
      );

      // Make startSession throw on the next call.
      sessions.shouldThrow = "deploy tree missing";

      await expect(
        env.router.sendSessionStart("start-fail@test.interchange"),
      ).rejects.toThrow("deploy tree missing");

      // Failed session start should remove the agent from routing.
      expect(env.router.getRoutableAddresses()).not.toContain(
        "start-fail@test.interchange",
      );
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-start-fail"),
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

  test("pack.reject sent when applyDeployPack throws signature_invalid", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const client = createWsClient({
      hubUrl: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-pack-reject",
      token: "test-token",
      transport,
      sessions,
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getConnectedSidecars().includes("sc-pack-reject"),
      );

      await env.router.sendAgentDeploy(
        "pack-agent@test.interchange",
        TEST_CONFIG,
      );

      sessions.applyDeployPack = () => {
        throw new Error("signature_invalid: bad signature");
      };

      await expect(
        env.router.sendPack(
          "pack-agent@test.interchange",
          new Uint8Array([1, 2, 3]),
          "refs/heads/deploy",
          "a".repeat(40),
        ),
      ).rejects.toThrow("Pack rejected: signature_invalid");
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-pack-reject"),
      );
    }
  });

  test("signature_unsigned errors also map to pack.reject signature_invalid", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const client = createWsClient({
      hubUrl: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-pack-unsigned",
      token: "test-token",
      transport,
      sessions,
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getConnectedSidecars().includes("sc-pack-unsigned"),
      );

      await env.router.sendAgentDeploy(
        "unsigned-agent@test.interchange",
        TEST_CONFIG,
      );

      sessions.applyDeployPack = () => {
        throw new Error("signature_unsigned: no signature found");
      };

      await expect(
        env.router.sendPack(
          "unsigned-agent@test.interchange",
          new Uint8Array([1, 2, 3]),
          "refs/heads/deploy",
          "a".repeat(40),
        ),
      ).rejects.toThrow("Pack rejected: signature_invalid");
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-pack-unsigned"),
      );
    }
  });

  test("malformed hubPublicKey in deploy frame sends agent.error", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();

    // Stand up a hub router with an odd-length hex key to trigger hexDecode.
    const badRouter = createSidecarRouter({
      requestTimeoutMs: 5000,
      hubPublicKey: "abc", // odd length — hexDecode should throw
      onAgentEvent: () => undefined,
      onMailOutbound: () => undefined,
    });

    const badApp = new Hono();
    badApp.get(
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
            badRouter.handleOpen(handle);
          },
          onMessage(evt, _ws) {
            if (typeof evt.data === "string") {
              badRouter.handleMessage(handle, evt.data);
            }
          },
          onClose(_evt, _ws) {
            badRouter.handleClose(handle);
          },
        };
      }),
    );

    const badServer = Bun.serve({
      fetch: badApp.fetch,
      websocket,
      port: 0,
    });

    const client = createWsClient({
      hubUrl: `ws://localhost:${badServer.port}/ws`,
      sidecarId: "sc-bad-hex",
      token: "test-token",
      transport,
      sessions,
    });

    client.connect();
    try {
      await waitFor(() =>
        badRouter.getConnectedSidecars().includes("sc-bad-hex"),
      );

      // Deploy should fail because hexDecode throws on the odd-length key.
      await expect(
        badRouter.sendAgentDeploy("bad-hex@test.interchange", TEST_CONFIG),
      ).rejects.toThrow("odd-length");
    } finally {
      client.close();
      badServer.stop(true);
    }
  });

  test("reconnect restores hubPublicKey into hubKeys map", async () => {
    const { generateKeyPair, createSshSignature } = await import(
      "@interchange/crypto-node"
    );

    // Agent keypair — used for challenge/response signing.
    const agentKp = await generateKeyPair();
    // Hub keypair — the key whose public half the sidecar stores to verify
    // deploy commit signatures. Distinct from the agent keypair.
    const hubKp = await generateKeyPair();
    const fakeAddress = "restored@test.interchange";

    function hexEncode(bytes: Uint8Array): string {
      return Array.from(bytes)
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("");
    }

    const agentPublicKeyHex = hexEncode(agentKp.publicKey);
    const hubPublicKeyHex = hexEncode(hubKp.publicKey);

    // Stand up a hub that supports reconnect (lookupPublicKey configured).
    const reconnectRouter = createSidecarRouter({
      requestTimeoutMs: 5000,
      challengeTimeoutMs: 5000,
      hubPublicKey: hubPublicKeyHex,
      onAgentEvent: () => undefined,
      onMailOutbound: () => undefined,
      lookupPublicKey: async (addr) =>
        addr === fakeAddress ? agentPublicKeyHex : null,
    });

    const reconnectApp = new Hono();
    reconnectApp.get(
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
            reconnectRouter.handleOpen(handle);
          },
          onMessage(evt, _ws) {
            if (typeof evt.data === "string") {
              reconnectRouter.handleMessage(handle, evt.data);
            }
          },
          onClose(_evt, _ws) {
            reconnectRouter.handleClose(handle);
          },
        };
      }),
    );

    const reconnectServer = Bun.serve({
      fetch: reconnectApp.fetch,
      websocket,
      port: 0,
    });

    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();

    sessions.addresses.push(fakeAddress);
    sessions.restoreSessions = async () => ({
      restored: [
        {
          address: fakeAddress,
          keyPair: agentKp,
          config: { ...TEST_CONFIG, agentAddress: fakeAddress },
          hubPublicKey: hubPublicKeyHex,
        },
      ],
      failed: [],
    });
    sessions.getDeployRef = async () => "a".repeat(40);

    const client = createWsClient({
      hubUrl: `ws://localhost:${reconnectServer.port}/ws`,
      sidecarId: "sc-restore-key",
      token: "test-token",
      transport,
      sessions,
    });

    client.connect();
    try {
      await waitFor(
        () => reconnectRouter.getRoutableAddresses().includes(fakeAddress),
        5000,
      );

      // Capture the verifyCommit callback to prove the correct hub key
      // was restored — not just any key.
      let capturedVerifyCommit: ((p: string, s: string) => boolean) | undefined;
      sessions.applyDeployPack = async (
        _addr: string,
        _pack: Uint8Array,
        _ref: string,
        _sha: string,
        _tid: string,
        verifyCommit?: (payload: string, signature: string) => boolean,
      ) => {
        capturedVerifyCommit = verifyCommit;
      };

      await reconnectRouter.sendPack(
        fakeAddress,
        new Uint8Array([1, 2, 3]),
        "refs/heads/deploy",
        "b".repeat(40),
      );

      expect(capturedVerifyCommit).toBeFunction();

      // Create a real signature with the hub's private key and verify
      // it round-trips through the restored verifyCommit callback.
      const payload = "tree abc\nauthor t <t@t> 0 +0000\n\ntest\n";
      const sig = createSshSignature(
        payload,
        hubKp.privateKey,
        hubKp.publicKey,
      );
      expect(capturedVerifyCommit!(payload, sig)).toBe(true);

      // A signature from a different key must fail, proving the callback
      // is bound to the specific hub key that was restored.
      const wrongKp = await generateKeyPair();
      const wrongSig = createSshSignature(
        payload,
        wrongKp.privateKey,
        wrongKp.publicKey,
      );
      expect(capturedVerifyCommit!(payload, wrongSig)).toBe(false);
    } finally {
      client.close();
      reconnectServer.stop(true);
    }
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
