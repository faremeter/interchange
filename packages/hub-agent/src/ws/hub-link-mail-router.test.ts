// Pins the FIXED contract for the `mail.inbound` arm in
// `handleMessage`: a throwing `mailInboundRouter.tryRoute` must not
// wedge the per-connection `messageQueue` chain. The arm wraps the
// router call in try/catch (mirroring `signal.deliver` and
// `drain.deliver`), so subsequent frames -- including the heartbeat
// `pong` -- continue to dispatch through the same chain.
//
// The shape of the underlying bug: an unguarded `tryRoute` call
// rejects the chained promise (`messageQueue = messageQueue.then(()
// => handleMessage(...))`); subsequent `.then(...)` calls against
// the rejected chain never fire, silently dropping every later
// frame. This test exercises the patched arm end-to-end through the
// real hub-link WS surface to make the regression observable.

import { describe, test, expect, afterAll } from "bun:test";
import { Hono } from "hono";
import { upgradeWebSocket, websocket } from "hono/bun";
import {
  createSidecarRouter,
  type SidecarRouter,
  type WsHandle,
} from "@intx/hub-sessions";
import { createInMemoryTransport } from "@intx/mail-memory";
import { base64Encode } from "@intx/types";
import type {
  HarnessConfig,
  InboundMessage,
  InferenceSource,
  KeyPair,
} from "@intx/types/runtime";
import type { GrantRule } from "@intx/types/authz";
import { signEd25519, verifySSHSignature } from "@intx/crypto";
import { hexDecode } from "@intx/types";

import { createHubLink, type DeployRouter } from "./hub-link";
import type { AgentKeyStore } from "../agent-key-store";
import type { AgentEventListener, SessionManager } from "../session-manager";

function createTestKeyStore(): AgentKeyStore & {
  registerKey(address: string, kp: KeyPair): void;
} {
  const agentKeys = new Map<string, KeyPair>();
  const hubKeys = new Map<string, Uint8Array>();
  return {
    registerKey(address, kp) {
      agentKeys.set(address, kp);
    },
    async loadOrGenerateKey(address) {
      const existing = agentKeys.get(address);
      if (existing !== undefined) return { keyPair: existing, isNew: false };
      throw new Error(`No key registered for ${address} in test store`);
    },
    async scanKeys() {
      return [...agentKeys.entries()].map(([address, keyPair]) => ({
        address,
        keyPair,
      }));
    },
    async signChallenge(address, payload) {
      const kp = agentKeys.get(address);
      if (kp === undefined) return null;
      return await signEd25519(kp.privateKey, payload);
    },
    recordHubKey(address, hexHubPublicKey) {
      hubKeys.set(address, hexDecode(hexHubPublicKey));
    },
    verifyDeployCommit(address, payload, signature) {
      const hubKey = hubKeys.get(address);
      if (hubKey === undefined) {
        throw new Error(
          `signature_invalid: no hub public key for "${address}"`,
        );
      }
      return verifySSHSignature(payload, signature, hubKey);
    },
    forgetAgent(address) {
      agentKeys.delete(address);
      hubKeys.delete(address);
    },
  };
}

function createTestDeployRouter(
  sessions: SessionManager,
  keyStore: AgentKeyStore,
): DeployRouter {
  return {
    async deploy(frame) {
      const result = await sessions.provisionAgent(frame.config);
      keyStore.recordHubKey(frame.agentAddress, frame.hubPublicKey);
      await sessions.persistHubPublicKey(
        frame.agentAddress,
        frame.hubPublicKey,
      );
      return { publicKey: result.publicKey };
    },
  };
}

function withTestDeployBindings(sessions: SessionManager): {
  keyStore: AgentKeyStore & { registerKey(address: string, kp: KeyPair): void };
  deployRouter: DeployRouter;
} {
  const keyStore = createTestKeyStore();
  return {
    keyStore,
    deployRouter: createTestDeployRouter(sessions, keyStore),
  };
}

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
    async updateSources(
      _agentAddress: string,
      _sources: InferenceSource[],
      _defaultSource: string,
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
    applyAssetPack: () => Promise.resolve(),
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
    onAgentEvent:
      (_agentAddress: string, _listener: AgentEventListener) => () => {
        /* no-op disposer: this test does not exercise per-agent events */
      },
  };
  return mock;
}

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

type TestEnv = {
  server: ReturnType<typeof Bun.serve>;
  router: SidecarRouter;
};

function startTestServer(): TestEnv {
  const router = createSidecarRouter({
    requestTimeoutMs: 5000,
    hubPublicKey: "a".repeat(64),
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

  return { server, router };
}

const env = startTestServer();

afterAll(async () => {
  await env.server.stop(true);
});

describe("hub-link mail.inbound throwing router", () => {
  test("a throwing mailInboundRouter does not wedge subsequent frames", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deploymentAddress = "dep_wedge-1@integration.interchange";
    sessions.addresses.push(deploymentAddress);

    let calls = 0;
    const routedAfterThrow: Uint8Array[] = [];
    const mailInboundRouter = {
      tryRoute(_address: string, message: Uint8Array): boolean {
        calls += 1;
        if (calls === 1) {
          throw new Error("simulated mail router failure");
        }
        routedAfterThrow.push(message);
        return true;
      },
    };

    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-mail-wedge",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(sessions),
      mailInboundRouter,
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getRoutableAddresses().includes(deploymentAddress),
      );

      const encoded = base64Encode(VALID_MESSAGE);

      // First mail.inbound: the router throws. With the C4 fix in
      // place, the link's switch arm catches the throw and logs it
      // without rejecting the messageQueue chain.
      expect(env.router.routeMail(deploymentAddress, encoded)).toBe(true);

      // Second mail.inbound: the router accepts. With the fix in
      // place this frame still flows through; without the fix the
      // chain has been wedged by the prior rejection and the router
      // is never consulted.
      expect(env.router.routeMail(deploymentAddress, encoded)).toBe(true);

      await waitFor(() => routedAfterThrow.length > 0);
      expect(routedAfterThrow).toHaveLength(1);
      expect(calls).toBe(2);
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-mail-wedge"),
      );
    }
  });
});
