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
  type SidecarAuthenticator,
  type WsHandle,
} from "@intx/hub-sessions";
import { createInMemoryTransport } from "@intx/mail-memory";
import { base64Encode } from "@intx/types";
import type {
  HarnessConfig,
  InboundMessage,
  KeyPair,
} from "@intx/types/runtime";
import { generateKeyPair, verifySSHSignature } from "@intx/crypto";
import { hexDecode } from "@intx/types";

import { createHubLink, type DeployRouter } from "./hub-link";
import type { AgentKeyStore } from "../agent-key-store";
import type { SessionManager } from "../session-manager";

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

function createTestDeployRouter(keyStore: AgentKeyStore): DeployRouter {
  return {
    async deploy(frame) {
      keyStore.recordHubKey(frame.agentAddress, frame.hubPublicKey);
      return { publicKey: "aa".repeat(32) };
    },
  };
}

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

    initRepo: (_address: string) => Promise.resolve(),
    getAddresses(): string[] {
      return [...mock.addresses];
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
    getSessionId: (_agentAddress: string) => undefined,
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
  router: ReturnType<typeof createSidecarRouter>;
};

const identities = new Map<
  string,
  Exclude<Awaited<ReturnType<SidecarAuthenticator>>, null>
>();
function ensureIdentity(sidecarId: string) {
  const existing = identities.get(sidecarId);
  if (existing !== undefined) return existing;
  const identity = {
    kind: "allocated" as const,
    sidecarId,
    allocationId: `allocation-${sidecarId}`,
    tenantId: "tenant-test",
    anchorRunId: `anchor-${sidecarId}`,
    workflowRunAddress: "workflow",
    generation: 1,
  };
  identities.set(sidecarId, identity);
  return identity;
}
const acceptAnySidecar: SidecarAuthenticator = async ({ sidecarId }) =>
  ensureIdentity(sidecarId);

function startTestServer(): TestEnv {
  const router = createSidecarRouter({
    authenticateSidecar: acceptAnySidecar,
    validateSidecarIdentity: async () => true,
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
            // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test server parses the handshake frame emitted by the typed HubLink under test
            const frame = JSON.parse(evt.data) as {
              type?: string;
              sidecarId?: string;
              agentAddresses?: string[];
            };
            if (
              (frame.type === "register" || frame.type === "reconnect") &&
              frame.sidecarId !== undefined
            ) {
              router.fenceAllocation(`allocation-${frame.sidecarId}`, 1);
              const identity = ensureIdentity(frame.sidecarId);
              if (frame.agentAddresses?.length === 1) {
                Object.assign(identity, {
                  workflowRunAddress: frame.agentAddresses[0],
                });
              }
            }
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

/**
 * Wire a workflow deployment for the reconnect path: mint a keypair and
 * register it in the sidecar keyStore so the deploy path picks up the
 * pinned key. The deployment address then routes once the hub
 * re-registers it on (re)connect.
 */
async function provisionDeploymentKey(
  keyStore: ReturnType<typeof createTestKeyStore>,
  address: string,
): Promise<void> {
  const kp = await generateKeyPair();
  keyStore.registerKey(address, kp);
}

describe("hub-link mail.inbound throwing router", () => {
  test("a throwing mailInboundRouter does not wedge subsequent frames", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deploymentAddress = "run_wedge1@integration.interchange";
    sessions.addresses.push(deploymentAddress);

    let calls = 0;
    const routedAfterThrow: Uint8Array[] = [];
    const mailInboundRouter = {
      tryRoute(_address: string, message: Uint8Array): Promise<void> | null {
        calls += 1;
        if (calls === 1) {
          throw new Error("simulated mail router failure");
        }
        routedAfterThrow.push(message);
        return Promise.resolve();
      },
    };

    const bindings = withTestDeployBindings();
    await provisionDeploymentKey(bindings.keyStore, deploymentAddress);
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-mail-wedge",
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
