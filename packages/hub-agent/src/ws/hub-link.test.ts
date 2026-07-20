/* eslint-disable @typescript-eslint/no-non-null-assertion -- refs[0]! always follows expect(refs).toHaveLength(1) */
import { describe, test, expect, afterAll } from "bun:test";
import { Hono } from "hono";
import { upgradeWebSocket, websocket } from "hono/bun";
import {
  createSidecarRouter,
  type SidecarAuthenticator,
  type SidecarRouter,
  type WsHandle,
} from "@intx/hub-sessions";
import { createInMemoryTransport } from "@intx/mail-memory";
import { generateKeyPair, signEd25519, verifySSHSignature } from "@intx/crypto";
import { base64Encode, hexEncode } from "@intx/types";
import type { HarnessConfig } from "@intx/types/runtime";

import {
  answerMalformedRequestFrame,
  createHubLink,
  type DeployRouter,
  type ReconnectScheduler,
} from "./hub-link";
import type {
  AgentErrorFrame,
  PackRejectFrame,
  SessionErrorFrame,
} from "@intx/types/sidecar";
import type { AgentKeyStore } from "../agent-key-store";
import type { SessionManager } from "../session-manager";

// These tests exercise routing and the hub-link protocol, not handshake
// auth, so the router accepts any token and keys off the claimed id.
const acceptAnySidecar: SidecarAuthenticator = async ({ sidecarId }) => ({
  kind: "sidecar",
  sidecarId,
});

/**
 * Test-only deploy router modelling the current deploy path: record the
 * hub pairing key so `verifyDeployCommit` can accept the deployment's
 * packs, and surface a public key on the ack. Production stages the
 * deploy through the workflow-run substrate; the tests here exercise the
 * link's surface against the router directly.
 */
function createTestDeployRouter(keyStore: AgentKeyStore): DeployRouter {
  return {
    async deploy(frame) {
      keyStore.recordHubKey(frame.agentAddress, frame.hubPublicKey);
      return { publicKey: "aa".repeat(32) };
    },
  };
}

/**
 * Convenience spread for `createHubLink({ ... })` call sites that
 * use a freshly-constructed `createTestKeyStore()` and a sessions
 * mock: returns the `keyStore` and a matching `deployRouter` so the
 * call site does not have to name a temporary binding for the
 * keyStore-router pairing.
 */
function withTestDeployBindings(): {
  keyStore: AgentKeyStore & { registerKey(address: string, kp: KeyPair): void };
  deployRouter: DeployRouter;
} {
  const keyStore = createTestKeyStore();
  return {
    keyStore,
    deployRouter: createTestDeployRouter(keyStore),
  };
}
import type { KeyPair } from "@intx/types/runtime";
import { hexDecode } from "@intx/types";

// In-memory AgentKeyStore for tests. Tests that exercise challenge
// response or deploy-commit verification register keys via the public
// AgentKeyStore methods (loadOrGenerateKey, recordHubKey); the stub
// satisfies the interface and uses real @intx/crypto primitives so
// signatures round-trip through the production verify path.
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

function createMockSessionManager(): SessionManager {
  return {
    initRepo: (_address: string) => Promise.resolve(),
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
    getAddresses: () => [],
    getSessionId: (_agentAddress: string) => undefined,
  };
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
  sources: [
    {
      id: "anthropic:claude-sonnet-4-20250514",
      provider: "anthropic",
      baseURL: "https://api.anthropic.com",
      apiKey: "sk-test",
      model: "claude-sonnet-4-20250514",
    },
  ],
  defaultSource: "anthropic:claude-sonnet-4-20250514",
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

// ---------------------------------------------------------------------------
// Test server
// ---------------------------------------------------------------------------

type TestEnv = {
  server: ReturnType<typeof Bun.serve>;
  router: SidecarRouter;
  agentEvents: { addr: string; sid: string; event: unknown }[];
  outboundMail: { rawMessage: string; recipients: string[] }[];
  // address -> hex-encoded Ed25519 public key, backing the hub's
  // `lookupPublicKey`. A workflow deployment announced through the
  // challenged reconnect frame routes only after signing the hub's nonce
  // with the key registered here; tests populate it via
  // `provisionDeploymentKey`.
  deploymentKeys: Map<string, string>;
};

function startTestServer(): TestEnv {
  const agentEvents: TestEnv["agentEvents"] = [];
  const outboundMail: TestEnv["outboundMail"] = [];
  const deploymentKeys = new Map<string, string>();

  const router = createSidecarRouter({
    authenticateSidecar: acceptAnySidecar,
    requestTimeoutMs: 5000,
    hubPublicKey: "a".repeat(64),
    lookups: {
      lookupPublicKey: async (address) => deploymentKeys.get(address) ?? null,
    },
  });
  router.events.on("agent.event", ({ agentAddress, sessionId, event }) => {
    agentEvents.push({ addr: agentAddress, sid: sessionId, event });
  });
  router.events.on(
    "mail.outbound.undelivered",
    ({ rawMessage, recipients }) => {
      outboundMail.push({ rawMessage, recipients });
    },
  );

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

  return { server, router, agentEvents, outboundMail, deploymentKeys };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const env = startTestServer();

afterAll(async () => {
  await env.server.stop(true);
});

/**
 * Wire a workflow deployment for the challenged reconnect path: mint an
 * Ed25519 keypair, register it in the sidecar's keyStore (so `signChallenge`
 * can answer the hub's nonce) and in the hub's `deploymentKeys` lookup (so the
 * hub issues a challenge and verifies the signature). After this, the
 * deployment address named in `getWorkflowAddresses` routes once the
 * reconnect challenge round-trips -- the same proof a launched agent makes.
 */
async function provisionDeploymentKey(
  keyStore: ReturnType<typeof createTestKeyStore>,
  address: string,
): Promise<void> {
  const kp = await generateKeyPair();
  keyStore.registerKey(address, kp);
  env.deploymentKeys.set(address, hexEncode(kp.publicKey));
}

describe("sidecar↔hub integration", () => {
  test("sidecar registers with hub on connect", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "test-sidecar",
      token: "test-token",

      transport,
      sessions,
      ...withTestDeployBindings(),
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

  test("hub sends a deploy and the sidecar acks", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-create",
      token: "test-token",

      transport,
      sessions,
      ...withTestDeployBindings(),
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getConnectedSidecars().includes("sc-create"),
      );

      await env.router.sendAgentDeploy("agent-1@test.interchange", TEST_CONFIG);

      // The deploy resolves against the ack; the sidecar stays connected
      // after handling it.
      expect(env.router.getConnectedSidecars()).toContain("sc-create");
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-create"),
      );
    }
  });

  test("sidecar forwards agent events to hub", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const startLength = env.agentEvents.length;
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-events",
      token: "test-token",

      transport,
      sessions,
      ...withTestDeployBindings(),
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

  test("sidecar forwards outbound mail to hub", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const { generateKeyPair, createEd25519Crypto } = await import(
      "@intx/crypto"
    );
    const kp = await generateKeyPair();
    transport.register("sender@test.interchange", createEd25519Crypto(kp));

    const startLength = env.outboundMail.length;
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-mail-out",
      token: "test-token",

      transport,
      sessions,
      ...withTestDeployBindings(),
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getConnectedSidecars().includes("sc-mail-out"),
      );

      const senderTransport = transport.getTransportFor(
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

  test("disconnect cleans up routing table", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deploymentAddress = "ins_dep_disc1@integration.interchange";

    const bindings = withTestDeployBindings();
    await provisionDeploymentKey(bindings.keyStore, deploymentAddress);
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-disconnect",
      token: "test-token",
      transport,
      sessions,
      ...bindings,
      getWorkflowAddresses: () => [deploymentAddress],
    });

    client.connect();
    // Routability lags the connection: it lands only after the reconnect
    // challenge round-trips, so wait on the routable address directly.
    await waitFor(() =>
      env.router.getRoutableAddresses().includes(deploymentAddress),
    );

    client.close();
    await waitFor(
      () => !env.router.getConnectedSidecars().includes("sc-disconnect"),
    );
    expect(env.router.getRoutableAddresses()).not.toContain(deploymentAddress);
  });

  test("a failing deploy-ref read closes the socket instead of silently dropping the reconnect", async () => {
    const failEnv = startTestServer();

    // Capture the reconnect callback so no real timer fires during the
    // test. Its presence is also the proof we assert on: the re-announce
    // catch closes the socket, and the close handler schedules a reconnect
    // through this scheduler. Without the fix the deploy-ref rejection is
    // an unhandled promise, no close occurs, and this stays null.
    let pendingReconnect: (() => void) | null = null;
    const fakeScheduleReconnect: ReconnectScheduler = (cb) => {
      pendingReconnect = cb;
      return () => {
        pendingReconnect = null;
      };
    };

    const transport = createInMemoryTransport();
    const deploymentAddress = "ins_dep_reffail1@integration.interchange";
    // The deploy-ref read rejects the way a corrupt or unreadable ref
    // would. The re-announce IIFE must catch it and close the socket
    // rather than let the reconnect vanish with nothing logged.
    const sessions: SessionManager = {
      ...createMockSessionManager(),
      getDeployRef: () =>
        Promise.reject(new Error("simulated corrupt deploy ref")),
    };

    const client = createHubLink({
      hubURL: `ws://localhost:${failEnv.server.port}/ws`,
      sidecarId: "sc-ref-fail",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
      getWorkflowAddresses: () => [deploymentAddress],
      scheduleReconnect: fakeScheduleReconnect,
    });

    try {
      client.connect();
      // On open the register frame is sent, then the re-announce IIFE
      // reads the deploy ref and the read rejects. The catch closes the
      // socket, and the close handler schedules a reconnect through the
      // fake scheduler. Observing that pending callback is the proof the
      // socket was closed rather than the failure swallowed. Without the
      // fix the rejection is an unhandled promise, no close occurs, and
      // this stays null until the wait times out. (The connect→reject→
      // close cycle is faster than a poll interval, so the transient
      // connected state is deliberately not asserted -- the scheduled
      // reconnect is the durable signal.)
      await waitFor(() => pendingReconnect !== null);

      // No reconnect frame was ever sent, so the address never routed.
      expect(failEnv.router.getRoutableAddresses()).not.toContain(
        deploymentAddress,
      );
    } finally {
      client.close();
      await failEnv.server.stop(true);
    }
  });

  test("repo.pack.reject sent when applyDeployPack throws signature_invalid", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-pack-reject",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
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

  test("signature_unsigned errors also map to repo.pack.reject signature_invalid", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-pack-unsigned",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
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
      authenticateSidecar: acceptAnySidecar,
      requestTimeoutMs: 5000,
      hubPublicKey: "abc", // odd length — hexDecode should throw
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

    const client = createHubLink({
      hubURL: `ws://localhost:${badServer.port}/ws`,
      sidecarId: "sc-bad-hex",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
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
      await badServer.stop(true);
    }
  });

  test("a deploy records the hub key so verifyCommit is bound to it", async () => {
    const { generateKeyPair, createSSHSignature } = await import(
      "@intx/crypto"
    );

    // Hub keypair — the key whose public half the sidecar stores to verify
    // deploy-commit signatures.
    const hubKp = await generateKeyPair();
    const deployedAddress = "deployed@test.interchange";
    const hubPublicKeyHex = hexEncode(hubKp.publicKey);

    const deployHubRouter = createSidecarRouter({
      authenticateSidecar: acceptAnySidecar,
      requestTimeoutMs: 5000,
      challengeTimeoutMs: 5000,
      hubPublicKey: hubPublicKeyHex,
    });

    const deployApp = new Hono();
    deployApp.get(
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
            deployHubRouter.handleOpen(handle);
          },
          onMessage(evt, _ws) {
            if (typeof evt.data === "string") {
              deployHubRouter.handleMessage(handle, evt.data);
            }
          },
          onClose(_evt, _ws) {
            deployHubRouter.handleClose(handle);
          },
        };
      }),
    );

    const deployServer = Bun.serve({
      fetch: deployApp.fetch,
      websocket,
      port: 0,
    });

    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const keyStore = createTestKeyStore();

    const client = createHubLink({
      hubURL: `ws://localhost:${deployServer.port}/ws`,
      sidecarId: "sc-deploy-key",
      token: "test-token",
      transport,
      sessions,
      keyStore,
      deployRouter: createTestDeployRouter(keyStore),
    });

    client.connect();
    try {
      await waitFor(() =>
        deployHubRouter.getConnectedSidecars().includes("sc-deploy-key"),
      );

      // The hub's deploy frame carries hubPublicKeyHex; the deploy router
      // records it via keyStore.recordHubKey, so a later pack's verifyCommit
      // is bound to the hub key.
      await deployHubRouter.sendAgentDeploy(deployedAddress, {
        ...TEST_CONFIG,
        agentAddress: deployedAddress,
      });

      let capturedVerifyCommit:
        | ((p: string, s: string) => Promise<boolean>)
        | undefined;
      sessions.applyDeployPack = async (
        _addr: string,
        _pack: Uint8Array,
        _ref: string,
        _sha: string,
        _tid: string,
        verifyCommit?: (payload: string, signature: string) => Promise<boolean>,
      ) => {
        capturedVerifyCommit = verifyCommit;
      };

      await deployHubRouter.sendPack(
        deployedAddress,
        new Uint8Array([1, 2, 3]),
        "refs/heads/deploy",
        "b".repeat(40),
      );

      expect(capturedVerifyCommit).toBeFunction();

      // A signature from the hub's key round-trips through the recorded
      // verifyCommit callback.
      const payload = "tree abc\nauthor t <t@t> 0 +0000\n\ntest\n";
      const sig = await createSSHSignature(
        payload,
        hubKp.privateKey,
        hubKp.publicKey,
      );
      expect(await capturedVerifyCommit!(payload, sig)).toBe(true);

      // A signature from a different key fails, proving the callback is bound
      // to the specific hub key the deploy recorded.
      const wrongKp = await generateKeyPair();
      const wrongSig = await createSSHSignature(
        payload,
        wrongKp.privateKey,
        wrongKp.publicKey,
      );
      expect(await capturedVerifyCommit!(payload, wrongSig)).toBe(false);
    } finally {
      client.close();
      await deployServer.stop(true);
    }
  });

  test("sidecar sends pings and hub responds with pongs", async () => {
    const pingEnv = startTestServer();

    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();

    const client = createHubLink({
      hubURL: `ws://localhost:${pingEnv.server.port}/ws`,
      sidecarId: "sc-ping",
      token: "test-token",

      transport,
      sessions,
      ...withTestDeployBindings(),
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
      await pingEnv.server.stop(true);
    }
  });

  test("repo.pack.done with mountPath routes through applyAssetPack", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deployCalls: string[] = [];
    const assetCalls: { address: string; mountPath: string }[] = [];
    sessions.applyDeployPack = (addr: string) => {
      deployCalls.push(addr);
      return Promise.resolve();
    };
    sessions.applyAssetPack = (addr: string, mountPath: string) => {
      assetCalls.push({ address: addr, mountPath });
      return Promise.resolve();
    };

    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-asset-route",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getConnectedSidecars().includes("sc-asset-route"),
      );

      await env.router.sendAgentDeploy(
        "route-agent@test.interchange",
        TEST_CONFIG,
      );

      // Deploy pack (no mountPath) → applyDeployPack.
      await env.router.sendPack(
        "route-agent@test.interchange",
        new Uint8Array([1, 2, 3]),
        "refs/heads/deploy",
        "a".repeat(40),
      );
      expect(deployCalls).toEqual(["route-agent@test.interchange"]);
      expect(assetCalls).toEqual([]);

      // Asset pack (mountPath set) → applyAssetPack.
      await env.router.sendPack(
        "route-agent@test.interchange",
        new Uint8Array([4, 5, 6]),
        "refs/heads/main",
        "b".repeat(40),
        { mountPath: "skills/example/" },
      );
      expect(deployCalls).toEqual(["route-agent@test.interchange"]);
      expect(assetCalls).toEqual([
        {
          address: "route-agent@test.interchange",
          mountPath: "skills/example/",
        },
      ]);
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-asset-route"),
      );
    }
  });

  test("asset_materialization_failed reports as pack.reject corrupt", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    sessions.applyAssetPack = () => {
      throw new Error("asset_materialization_failed: pack index error");
    };

    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-asset-fail",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getConnectedSidecars().includes("sc-asset-fail"),
      );
      await env.router.sendAgentDeploy(
        "fail-asset@test.interchange",
        TEST_CONFIG,
      );

      await expect(
        env.router.sendPack(
          "fail-asset@test.interchange",
          new Uint8Array([1, 2, 3]),
          "refs/heads/main",
          "a".repeat(40),
          { mountPath: "skills/example/" },
        ),
      ).rejects.toThrow("Pack rejected: corrupt");
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-asset-fail"),
      );
    }
  });

  test("close cancels a pending reconnect scheduled by the previous disconnect", async () => {
    const reconnectEnv = startTestServer();

    // Inject a fake scheduler so we can observe the reconnect callback
    // directly rather than waiting for a real timer. The cancel
    // function nils the captured callback; after close() the callback
    // must be gone, otherwise the bug is present.
    let pendingReconnect: (() => void) | null = null;
    const fakeScheduleReconnect: ReconnectScheduler = (cb) => {
      pendingReconnect = cb;
      return () => {
        pendingReconnect = null;
      };
    };

    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const client = createHubLink({
      hubURL: `ws://localhost:${reconnectEnv.server.port}/ws`,
      sidecarId: "sc-reconnect-race",
      token: "test-token",

      transport,
      sessions,
      ...withTestDeployBindings(),
      scheduleReconnect: fakeScheduleReconnect,
    });

    try {
      client.connect();
      await waitFor(() =>
        reconnectEnv.router
          .getConnectedSidecars()
          .includes("sc-reconnect-race"),
      );

      // Force a disconnect by stopping the server. The WebSocket fires
      // its close event on the client, which schedules a reconnect
      // through the fake scheduler.
      await reconnectEnv.server.stop(true);
      await waitFor(() => pendingReconnect !== null);

      // close() must cancel the scheduled reconnect. Without the fix
      // the cancel function never runs and pendingReconnect stays
      // non-null.
      client.close();
      expect(pendingReconnect).toBeNull();
    } finally {
      client.close();
      await reconnectEnv.server.stop(true);
    }
  });

  test("mailInboundRouter claims an address and skips the legacy fallback", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    // The deployment address is what the hub routes mail to. The
    // sidecar puts it on the register frame's `agentAddresses` list
    // so the hub-side router accepts it as routable. Routing a
    // mail.inbound for it goes through the link's switch case, which
    // must consult mailInboundRouter first and -- on a `true` return
    // -- skip transport.deliver and sessions.commitInboundMail.
    const deploymentAddress = "ins_dep_mail1@integration.interchange";

    const routed: { address: string; bytes: Uint8Array }[] = [];
    const mailInboundRouter = {
      tryRoute(address: string, message: Uint8Array): boolean {
        routed.push({ address, bytes: message });
        return true;
      },
    };

    const bindings = withTestDeployBindings();
    await provisionDeploymentKey(bindings.keyStore, deploymentAddress);
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-multistep-mail",
      token: "test-token",
      transport,
      sessions,
      ...bindings,
      mailInboundRouter,
      getWorkflowAddresses: () => [deploymentAddress],
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getRoutableAddresses().includes(deploymentAddress),
      );

      const encoded = base64Encode(VALID_MESSAGE);
      const accepted = env.router.routeMail(deploymentAddress, encoded);
      expect(accepted).toBe(true);

      await waitFor(() => routed.length > 0);

      expect(routed).toHaveLength(1);
      expect(routed[0]?.address).toBe(deploymentAddress);
      expect(routed[0]?.bytes).toEqual(VALID_MESSAGE);
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-multistep-mail"),
      );
    }
  });

  test("drainInboundRouter dispatches an inbound drain.deliver frame", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deploymentAddress = "ins_dep_drain1@integration.interchange";

    const routed: { agentAddress: string; deadlineMs: number }[] = [];
    const drainInboundRouter = {
      async tryRoute(frame: {
        agentAddress: string;
        deadlineMs: number;
      }): Promise<boolean> {
        routed.push({
          agentAddress: frame.agentAddress,
          deadlineMs: frame.deadlineMs,
        });
        return true;
      },
    };

    const bindings = withTestDeployBindings();
    await provisionDeploymentKey(bindings.keyStore, deploymentAddress);
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-drain-router",
      token: "test-token",
      transport,
      sessions,
      ...bindings,
      drainInboundRouter,
      getWorkflowAddresses: () => [deploymentAddress],
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getRoutableAddresses().includes(deploymentAddress),
      );

      env.router.sendDrain({
        agentAddress: deploymentAddress,
        deadlineMs: 4_321,
      });

      await waitFor(() => routed.length > 0);

      expect(routed).toHaveLength(1);
      expect(routed[0]?.agentAddress).toBe(deploymentAddress);
      expect(routed[0]?.deadlineMs).toBe(4_321);
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-drain-router"),
      );
    }
  });

  const ROTATION_SOURCE = {
    id: "primary",
    provider: "anthropic",
    baseURL: "https://api.anthropic.com",
    apiKey: "sk-rotation",
    model: "claude-rotation",
  };

  test("sourcesInboundRouter acks an inbound sources.update round-trip", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deploymentAddress = "ins_dep_srcack@integration.interchange";

    const routed: { agentAddress: string }[] = [];
    const sourcesInboundRouter = {
      async tryRoute(frame: { agentAddress: string }): Promise<boolean> {
        routed.push({ agentAddress: frame.agentAddress });
        return true;
      },
    };

    const bindings = withTestDeployBindings();
    await provisionDeploymentKey(bindings.keyStore, deploymentAddress);
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-sources-ack",
      token: "test-token",
      transport,
      sessions,
      ...bindings,
      sourcesInboundRouter,
      getWorkflowAddresses: () => [deploymentAddress],
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getRoutableAddresses().includes(deploymentAddress),
      );
      // Resolves on the sidecar's session.ack. Without a reply this would
      // await the full request timeout, so a prompt resolution is the proof
      // the round-trip no longer hangs.
      await env.router.sendSourcesUpdate(
        deploymentAddress,
        [ROTATION_SOURCE],
        "primary",
      );
      expect(routed).toHaveLength(1);
      expect(routed[0]?.agentAddress).toBe(deploymentAddress);
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-sources-ack"),
      );
    }
  });

  test("an unrouted sources.update is answered with session.error", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deploymentAddress = "ins_dep_srcunrouted@integration.interchange";

    const sourcesInboundRouter = {
      async tryRoute(): Promise<boolean> {
        return false;
      },
    };

    const bindings = withTestDeployBindings();
    await provisionDeploymentKey(bindings.keyStore, deploymentAddress);
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-sources-unrouted",
      token: "test-token",
      transport,
      sessions,
      ...bindings,
      sourcesInboundRouter,
      getWorkflowAddresses: () => [deploymentAddress],
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getRoutableAddresses().includes(deploymentAddress),
      );
      await expect(
        env.router.sendSourcesUpdate(
          deploymentAddress,
          [ROTATION_SOURCE],
          "primary",
        ),
      ).rejects.toThrow(/no deployment registered/);
    } finally {
      client.close();
      await waitFor(
        () =>
          !env.router.getConnectedSidecars().includes("sc-sources-unrouted"),
      );
    }
  });

  test("a rejected sources.update surfaces the reason as session.error", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deploymentAddress = "ins_dep_srcreject@integration.interchange";

    const sourcesInboundRouter = {
      async tryRoute(): Promise<boolean> {
        throw new Error("supervisor is recycling");
      },
    };

    const bindings = withTestDeployBindings();
    await provisionDeploymentKey(bindings.keyStore, deploymentAddress);
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-sources-reject",
      token: "test-token",
      transport,
      sessions,
      ...bindings,
      sourcesInboundRouter,
      getWorkflowAddresses: () => [deploymentAddress],
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getRoutableAddresses().includes(deploymentAddress),
      );
      await expect(
        env.router.sendSourcesUpdate(
          deploymentAddress,
          [ROTATION_SOURCE],
          "primary",
        ),
      ).rejects.toThrow(/supervisor is recycling/);
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-sources-reject"),
      );
    }
  });

  test("a sources.update with no router wired is answered with session.error", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deploymentAddress = "ins_dep_srcnorouter@integration.interchange";

    const bindings = withTestDeployBindings();
    await provisionDeploymentKey(bindings.keyStore, deploymentAddress);
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-sources-norouter",
      token: "test-token",
      transport,
      sessions,
      ...bindings,
      // No sourcesInboundRouter: a request/ack frame must still be answered
      // or the hub hangs on its request timeout.
      getWorkflowAddresses: () => [deploymentAddress],
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getRoutableAddresses().includes(deploymentAddress),
      );
      await expect(
        env.router.sendSourcesUpdate(
          deploymentAddress,
          [ROTATION_SOURCE],
          "primary",
        ),
      ).rejects.toThrow(/no sourcesInboundRouter/);
    } finally {
      client.close();
      await waitFor(
        () =>
          !env.router.getConnectedSidecars().includes("sc-sources-norouter"),
      );
    }
  });

  // The hub-link's `pushWorkflowRunPack` retries the FIRST push to a
  // never-bootstrapped `(repoId, ref)` once on failure, absorbing the
  // hub-side `initRepo` CAS race. The retry guard is a Set keyed by
  // `(repoId, ref)`; the per-(repoId, ref) queue serializes pushes so
  // a second-from-this-sender push only fires after the first has
  // settled. Without the queue, two concurrent first-pushes from a
  // single sender could each observe `workflowRunPackBootstrapped.has`
  // as `false` and each fire its own bootstrap retry — a spurious
  // double retry the queue+flag combination is supposed to prevent.
  // This test pins the queue+retry interaction so a future change to
  // the bootstrap-retry surface cannot quietly introduce that
  // spurious retry.
  test("two concurrent first-pushes to the same (repoId, ref) fire exactly one bootstrap retry", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();

    // Fail the very first workflow-run pack push so the hub-link
    // observes a rejection on its first send and exercises the
    // bootstrap-retry arm. Every subsequent push (the retry, the
    // second concurrent push) succeeds.
    let receiveCount = 0;
    const receiveRecord: {
      transferIds: string[];
    } = { transferIds: [] };
    const wfrRouter = createSidecarRouter({
      authenticateSidecar: acceptAnySidecar,
      requestTimeoutMs: 5000,
      hubPublicKey: "a".repeat(64),
      lookups: {
        async receiveWorkflowRunPack(_repoId, _pack, _ref, _commitSha) {
          receiveCount += 1;
          if (receiveCount === 1) {
            // Mirror the hub's wire-level translation of an
            // `initRepo` non-fast-forward race: surface a `corrupt`
            // rejection so the sender's bootstrap arm runs once.
            return { accepted: false, reason: "corrupt" };
          }
          return { accepted: true };
        },
      },
    });

    const wfrApp = new Hono();
    wfrApp.get(
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
            wfrRouter.handleOpen(handle);
          },
          onMessage(evt, _ws) {
            if (typeof evt.data === "string") {
              // Capture every inbound `repo.pack.done` frame so the
              // test can assert how many sends the link actually
              // issued — one per (initial-attempt | retry | second
              // push). A spurious second retry adds a fourth entry.
              try {
                const parsed: unknown = JSON.parse(evt.data);
                if (
                  typeof parsed === "object" &&
                  parsed !== null &&
                  "type" in parsed &&
                  parsed.type === "repo.pack.done" &&
                  "transferId" in parsed &&
                  typeof parsed.transferId === "string"
                ) {
                  receiveRecord.transferIds.push(parsed.transferId);
                }
              } catch {
                /* not a JSON frame — ignore */
              }
              wfrRouter.handleMessage(handle, evt.data);
            }
          },
          onClose(_evt, _ws) {
            wfrRouter.handleClose(handle);
          },
        };
      }),
    );

    const wfrServer = Bun.serve({
      fetch: wfrApp.fetch,
      websocket,
      port: 0,
    });

    const client = createHubLink({
      hubURL: `ws://localhost:${wfrServer.port}/ws`,
      sidecarId: "sc-wfr-bootstrap-race",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
    });

    client.connect();
    try {
      await waitFor(() =>
        wfrRouter.getConnectedSidecars().includes("sc-wfr-bootstrap-race"),
      );

      // Deploy an agent so the sender's outbound pack frames carry a
      // routable address; otherwise the hub drops the push as
      // "unrouted agent" before it ever reaches `receiveWorkflowRunPack`.
      const agentAddress = "race-agent@test.interchange";
      await wfrRouter.sendAgentDeploy(agentAddress, TEST_CONFIG);
      await waitFor(() =>
        wfrRouter.getRoutableAddresses().includes(agentAddress),
      );

      const repoId = {
        kind: "workflow-run",
        id: "dep-bootstrap-race",
      } as const;
      const ref = "refs/heads/events";
      const commitSha = "a".repeat(40);
      const pack = new Uint8Array([1, 2, 3, 4, 5]);

      // Kick both pushes off in the same tick so they both observe the
      // queue's pre-A state and B genuinely chains through A's
      // promise.
      const pushA = client.pushWorkflowRunPack({
        agentAddress,
        repoId,
        pack,
        ref,
        commitSha,
      });
      const pushB = client.pushWorkflowRunPack({
        agentAddress,
        repoId,
        pack,
        ref,
        commitSha,
      });

      // Both pushes must resolve cleanly. If the bootstrap-retry path
      // double-fired, the second push's send would reuse a
      // transferId from a still-pending transfer and reject before
      // the hub ever responded.
      await Promise.all([pushA, pushB]);

      // The hub sees exactly three packs:
      //   1. A's first attempt — rejected with `corrupt`.
      //   2. A's bootstrap retry — accepted.
      //   3. B's single attempt — accepted (the flag is already set).
      // A spurious second retry would push the count to 4.
      expect(receiveCount).toBe(3);
      expect(receiveRecord.transferIds).toHaveLength(3);
      // Each send mints a fresh transferId; the queue+retry must not
      // collapse the two pushes into a shared in-flight id.
      const uniqueTransferIds = new Set(receiveRecord.transferIds);
      expect(uniqueTransferIds.size).toBe(3);
    } finally {
      client.close();
      await waitFor(
        () =>
          !wfrRouter.getConnectedSidecars().includes("sc-wfr-bootstrap-race"),
      );
      await wfrServer.stop(true);
    }
  });
});

describe("register + reconnect frames on connect", () => {
  test("ships an empty register then a challenged reconnect carrying the workflow addresses", async () => {
    const frames: string[] = [];
    const app = new Hono();
    app.get(
      "/ws",
      upgradeWebSocket((_c) => ({
        onMessage(evt, _ws) {
          if (typeof evt.data === "string") {
            frames.push(evt.data);
          }
        },
      })),
    );
    const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });

    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const workflowAddresses = ["ins_dep_reg1@integration.interchange"];

    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-register",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
      getWorkflowAddresses: () => workflowAddresses,
    });

    client.connect();
    try {
      await waitFor(() => {
        const types = frames.map((s) => JSON.parse(s).type);
        return types.includes("register") && types.includes("reconnect");
      });
      const parsed = frames.map((s) => JSON.parse(s));
      const registerFrames = parsed.filter(
        (f: { type: string }) => f.type === "register",
      );
      const reconnectFrames = parsed.filter(
        (f: { type: string }) => f.type === "reconnect",
      );
      // First-connect register carries no addresses and no workflow field:
      // a workflow deployment is announced only through the challenged
      // reconnect, never as a keyless register route.
      expect(registerFrames).toHaveLength(1);
      expect(registerFrames[0].agentAddresses).toEqual([]);
      expect(registerFrames[0].workflowAddresses).toBeUndefined();
      // The reconnect frame announces the workflow addresses in
      // agentAddresses, so each proves ownership via the Ed25519 challenge.
      expect(reconnectFrames).toHaveLength(1);
      expect(reconnectFrames[0].agentAddresses).toEqual(workflowAddresses);
    } finally {
      client.close();
      await server.stop(true);
    }
  });

  test("sendSignalCorrelationRegister throws on a registration with no snapshot", () => {
    // Regression cover for the existing fail-loud guard in the producer: the
    // ask rail always carries a snapshot, so a registration without one is a
    // wiring defect the producer refuses to send. The guard fires
    // synchronously before the frame is built, so no connection is needed.
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const client = createHubLink({
      hubURL: "ws://localhost:1/ws",
      sidecarId: "sc-throw",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
      getWorkflowAddresses: () => [],
    });

    try {
      expect(() =>
        client.sendSignalCorrelationRegister({
          correlationId: "corr-throw",
          runId: "run-1",
          deploymentId: "dep-1",
          agentAddress: "ins_dep@integration.interchange",
          kind: "approval",
        }),
      ).toThrow(/corr-throw/);
    } finally {
      client.close();
    }
  });

  test("sendSignalCorrelationRegister ships a register frame carrying the approval snapshot", async () => {
    const frames: string[] = [];
    const app = new Hono();
    app.get(
      "/ws",
      upgradeWebSocket((_c) => ({
        onMessage(evt, _ws) {
          if (typeof evt.data === "string") {
            frames.push(evt.data);
          }
        },
      })),
    );
    const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });

    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();

    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-register-snapshot",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
      getWorkflowAddresses: () => [],
    });

    const snapshot = {
      name: "charge_card",
      description: "Charge the customer's card",
      inputSchema: { type: "object" },
      arguments: { amount: 100 },
    };

    client.connect();
    try {
      await waitFor(() =>
        frames.some((s) => JSON.parse(s).type === "register"),
      );
      client.sendSignalCorrelationRegister({
        correlationId: "corr-1",
        runId: "run-1",
        deploymentId: "dep-1",
        agentAddress: "ins_dep_reg2@integration.interchange",
        kind: "approval",
        approvalSnapshot: snapshot,
      });
      await waitFor(() =>
        frames.some(
          (s) => JSON.parse(s).type === "signal.correlation.register",
        ),
      );
      const frame = frames
        .map((s) => JSON.parse(s))
        .find(
          (f: { type: string }) => f.type === "signal.correlation.register",
        );
      expect(frame.correlationId).toBe("corr-1");
      expect(frame.snapshot).toEqual(snapshot);
    } finally {
      client.close();
      await server.stop(true);
    }
  });

  test("an ack from the hub settles the register retry", async () => {
    const allFrames: string[] = [];
    const app = new Hono();
    app.get(
      "/ws",
      upgradeWebSocket((_c) => ({
        onMessage(evt, ws) {
          if (typeof evt.data !== "string") return;
          allFrames.push(evt.data);
          const frame: { type: string; correlationId?: string } = JSON.parse(
            evt.data,
          );
          if (
            frame.type === "signal.correlation.register" &&
            frame.correlationId !== undefined
          ) {
            ws.send(
              JSON.stringify({
                type: "signal.correlation.register.ack",
                agentAddress: "ins_dep_reg_ack@integration.interchange",
                correlationId: frame.correlationId,
              }),
            );
          }
        },
      })),
    );
    const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });

    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();

    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-register-ack",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
      getWorkflowAddresses: () => [],
      // A tight watchdog against a generous cap: an unacked register would
      // climb toward the cap, so a count that stabilizes far below it is the
      // ack having stopped the retry -- robust against a round-trip that races
      // one retry rather than asserting an exact timing-gated count.
      registerAckTimeoutMs: 30,
      registerAckMaxAttempts: 10,
    });

    const registers = (): string[] =>
      allFrames.filter(
        (s) => JSON.parse(s).type === "signal.correlation.register",
      );

    client.connect();
    try {
      await waitFor(() =>
        allFrames.some((s) => JSON.parse(s).type === "register"),
      );
      client.sendSignalCorrelationRegister({
        correlationId: "corr-ack",
        runId: "run-1",
        deploymentId: "dep-1",
        agentAddress: "ins_dep_reg_ack@integration.interchange",
        kind: "approval",
        approvalSnapshot: {
          name: "charge_card",
          description: "Charge the customer's card",
          inputSchema: { type: "object" },
          arguments: { amount: 100 },
        },
      });
      await waitFor(() => registers().length >= 1);
      // Let the ack settle and several watchdog windows pass, then confirm the
      // register count has stopped growing and stayed far below the cap -- the
      // ack settled the retry rather than the loop running to exhaustion.
      await new Promise((r) => setTimeout(r, 200));
      const settledCount = registers().length;
      await new Promise((r) => setTimeout(r, 120));
      expect(registers().length).toBe(settledCount);
      expect(settledCount).toBeLessThan(5);
    } finally {
      client.close();
      await server.stop(true);
    }
  });

  test("an unacked register is retried on the watchdog up to the cap", async () => {
    const allFrames: string[] = [];
    const app = new Hono();
    app.get(
      "/ws",
      upgradeWebSocket((_c) => ({
        // Deliberately never ack the register, so the client's watchdog fires.
        onMessage(evt, _ws) {
          if (typeof evt.data === "string") allFrames.push(evt.data);
        },
      })),
    );
    const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });

    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();

    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-register-retry",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
      getWorkflowAddresses: () => [],
      registerAckTimeoutMs: 40,
      registerAckMaxAttempts: 3,
    });

    const registers = (): string[] =>
      allFrames.filter(
        (s) => JSON.parse(s).type === "signal.correlation.register",
      );

    client.connect();
    try {
      await waitFor(() =>
        allFrames.some((s) => JSON.parse(s).type === "register"),
      );
      client.sendSignalCorrelationRegister({
        correlationId: "corr-retry",
        runId: "run-1",
        deploymentId: "dep-1",
        agentAddress: "ins_dep_reg_retry@integration.interchange",
        kind: "approval",
        approvalSnapshot: {
          name: "charge_card",
          description: "Charge the customer's card",
          inputSchema: { type: "object" },
          arguments: { amount: 100 },
        },
      });
      // Three sends total (initial + two retries), then the acker gives up.
      await waitFor(() => registers().length >= 3);
      await new Promise((r) => setTimeout(r, 150));
      expect(registers()).toHaveLength(3);
    } finally {
      client.close();
      await server.stop(true);
    }
  });
});

describe("answerMalformedRequestFrame", () => {
  test("answers a malformed sources.update with session.error carrying the requestId", () => {
    const sent: (SessionErrorFrame | AgentErrorFrame | PackRejectFrame)[] = [];
    // A structurally-invalid sources list (empty source object) that failed
    // the top-level parse but kept its type + requestId.
    const answered = answerMalformedRequestFrame(
      {
        type: "sources.update",
        requestId: "req-1",
        agentAddress: "ins_x@example.com",
        sources: [{}],
        defaultSource: "x",
      },
      "sources[0].id must be a string",
      (frame) => sent.push(frame),
    );
    expect(answered).toBe(true);
    expect(sent).toHaveLength(1);
    expect(sent[0]).toMatchObject({
      type: "session.error",
      requestId: "req-1",
      error: expect.stringMatching(/malformed sources.update frame/),
    });
  });

  test("answers a malformed agent.deploy with agent.error carrying the agentAddress", () => {
    const sent: (SessionErrorFrame | AgentErrorFrame | PackRejectFrame)[] = [];
    const answered = answerMalformedRequestFrame(
      {
        type: "agent.deploy",
        agentAddress: "ins_deploy@example.com",
        agentId: "x",
      },
      "config must be an object",
      (frame) => sent.push(frame),
    );
    expect(answered).toBe(true);
    expect(sent).toHaveLength(1);
    expect(sent[0]).toMatchObject({
      type: "agent.error",
      agentAddress: "ins_deploy@example.com",
      error: expect.stringMatching(/malformed agent.deploy frame/),
    });
  });

  test("drops an unhandled request-shaped frame instead of answering it", () => {
    // A request-shaped frame whose type is in none of the answerable sets
    // (SESSION_ERROR / AGENT_ERROR / PACK_REJECT) has no requester to
    // answer, so a malformed one is dropped rather than answered.
    for (const frameType of ["some.unhandled.request", "another.unknown"]) {
      const sent: (SessionErrorFrame | AgentErrorFrame | PackRejectFrame)[] =
        [];
      const answered = answerMalformedRequestFrame(
        {
          type: frameType,
          requestId: "req-2",
          agentAddress: "ins_x@example.com",
        },
        "payload must be valid",
        (frame) => sent.push(frame),
      );
      expect(answered).toBe(false);
      expect(sent).toHaveLength(0);
    }
  });

  test("answers a malformed agent.undeploy with agent.error", () => {
    const sent: (SessionErrorFrame | AgentErrorFrame | PackRejectFrame)[] = [];
    const answered = answerMalformedRequestFrame(
      { type: "agent.undeploy", agentAddress: "ins_undeploy@example.com" },
      "reason must be a string",
      (frame) => sent.push(frame),
    );
    expect(answered).toBe(true);
    expect(sent[0]).toMatchObject({
      type: "agent.error",
      agentAddress: "ins_undeploy@example.com",
      error: expect.stringMatching(/malformed agent.undeploy frame/),
    });
  });

  test("answers a malformed repo.pack frame with repo.pack.reject on its transferId", () => {
    for (const frameType of ["repo.pack.push", "repo.pack.done"]) {
      const sent: (SessionErrorFrame | AgentErrorFrame | PackRejectFrame)[] =
        [];
      const answered = answerMalformedRequestFrame(
        {
          type: frameType,
          agentAddress: "ins_pack@example.com",
          repoId: { kind: "workflow-run", id: "dep-1" },
          transferId: "xfer-1",
          seq: "not-a-number",
        },
        "a nested field is malformed",
        (frame) => sent.push(frame),
      );
      expect(answered).toBe(true);
      expect(sent[0]).toMatchObject({
        type: "repo.pack.reject",
        transferId: "xfer-1",
        reason: "corrupt",
      });
    }
  });

  test("does not answer a repo.pack frame with no recoverable transferId", () => {
    const sent: (SessionErrorFrame | AgentErrorFrame | PackRejectFrame)[] = [];
    const answered = answerMalformedRequestFrame(
      {
        type: "repo.pack.push",
        agentAddress: "ins_pack@example.com",
        repoId: { kind: "workflow-run", id: "dep-1" },
      },
      "bad",
      (frame) => sent.push(frame),
    );
    expect(answered).toBe(false);
    expect(sent).toHaveLength(0);
  });

  test("does not answer a repo.pack frame whose repoId is itself malformed", () => {
    const sent: (SessionErrorFrame | AgentErrorFrame | PackRejectFrame)[] = [];
    const answered = answerMalformedRequestFrame(
      {
        type: "repo.pack.push",
        agentAddress: "ins_pack@example.com",
        transferId: "xfer-3",
        repoId: { kind: "not-a-real-kind" },
      },
      "bad",
      (frame) => sent.push(frame),
    );
    expect(answered).toBe(false);
    expect(sent).toHaveLength(0);
  });

  test("recovers a non-pack frame that carries a malformed repoId-shaped field", () => {
    // repoId is validated only inside the pack branch, so a requestId- or
    // agentAddress-correlated frame that happens to carry a garbage
    // repoId still recovers through its own correlation key.
    const sent: (SessionErrorFrame | AgentErrorFrame | PackRejectFrame)[] = [];
    const answered = answerMalformedRequestFrame(
      {
        type: "sources.update",
        requestId: "req-9",
        repoId: { kind: "not-a-real-kind" },
        sources: [{}],
      },
      "sources invalid",
      (frame) => sent.push(frame),
    );
    expect(answered).toBe(true);
    expect(sent[0]).toMatchObject({
      type: "session.error",
      requestId: "req-9",
    });
  });

  test("does not answer a sources.update with no recoverable requestId", () => {
    const sent: (SessionErrorFrame | AgentErrorFrame | PackRejectFrame)[] = [];
    const answered = answerMalformedRequestFrame(
      { type: "sources.update", sources: [{}] },
      "bad",
      (frame) => sent.push(frame),
    );
    expect(answered).toBe(false);
    expect(sent).toHaveLength(0);
  });

  test("does not answer a fire-and-forget frame even with a requestId present", () => {
    const sent: (SessionErrorFrame | AgentErrorFrame | PackRejectFrame)[] = [];
    const answered = answerMalformedRequestFrame(
      { type: "signal.deliver", agentAddress: "x", requestId: "r" },
      "bad",
      (frame) => sent.push(frame),
    );
    expect(answered).toBe(false);
    expect(sent).toHaveLength(0);
  });

  test("does not answer a frame with no recognizable type", () => {
    const sent: (SessionErrorFrame | AgentErrorFrame | PackRejectFrame)[] = [];
    const answered = answerMalformedRequestFrame(
      { garbage: true },
      "bad",
      (frame) => sent.push(frame),
    );
    expect(answered).toBe(false);
    expect(sent).toHaveLength(0);
  });
});
