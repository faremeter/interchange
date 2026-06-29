// Pins H-A2: `workflowRunPackBootstrapped` must be pruned when the
// owning deployment is torn down. Without the prune the set grows
// unbounded over the link's lifetime, and a future workflow-run repo
// reset for the same `(kind, id, ref)` triple races the stale
// bootstrap flag -- the first push after the reset skips the
// bootstrap-retry arm and fails with `non_fast_forward`.
//
// The contract is asserted behaviorally through the wire surface:
//   1. Deploy agent A. Push a workflow-run pack; the hub rejects the
//      first attempt with `corrupt` and accepts the retry. The link
//      marks the `(workflow-run, dep-prune-X, ref)` key as bootstrapped.
//   2. Undeploy agent A. The fix prunes the bootstrap entry whose
//      `repoId.id` matches the agent's deploymentId.
//   3. Re-deploy agent A. Push another workflow-run pack to the same
//      `(repoId, ref)`; the hub rejects the first attempt again.
//   4. The push only succeeds because the link's bootstrap-retry arm
//      runs a second time -- the prune step in (2) reset the flag.
//      Without the fix the link would skip the retry on this push and
//      surface the rejection to the caller.

import { describe, test, expect, afterAll } from "bun:test";
import { Hono } from "hono";
import { upgradeWebSocket, websocket } from "hono/bun";
import {
  createSidecarRouter,
  type SidecarRouter,
  type WsHandle,
} from "@intx/hub-sessions";
import { createInMemoryTransport } from "@intx/mail-memory";
import { signEd25519, verifySSHSignature } from "@intx/crypto-node";
import type {
  HarnessConfig,
  InboundMessage,
  InferenceSource,
  KeyPair,
} from "@intx/types/runtime";
import type { GrantRule } from "@intx/types/authz";
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

type DeliveredMessage = { agentAddress: string; message: InboundMessage };

function createMockSessionManager(): SessionManager & {
  provisioned: HarnessConfig[];
  addresses: string[];
} {
  const mock = {
    provisioned: [] as HarnessConfig[],
    addresses: [] as string[],

    async provisionAgent(config: HarnessConfig) {
      mock.provisioned.push(config);
      mock.addresses.push(config.agentAddress);
      return {
        publicKey: "deadbeef",
        keyPair: {
          publicKey: new Uint8Array(32),
          privateKey: new Uint8Array(32),
        },
      };
    },
    async startSession(_agentAddress: string): Promise<void> {
      /* unused */
    },
    async destroySession(agentAddress: string): Promise<void> {
      mock.addresses = mock.addresses.filter((a) => a !== agentAddress);
    },
    async abortSession(_agentAddress: string, _reason: string): Promise<void> {
      /* unused */
    },
    deliverMessage(_agentAddress: string, _message: InboundMessage): void {
      /* unused */
    },
    async updateGrants(
      _agentAddress: string,
      _grants: GrantRule[],
    ): Promise<void> {
      /* unused */
    },
    async updateSources(
      _agentAddress: string,
      _sources: InferenceSource[],
      _defaultSource: string,
    ): Promise<void> {
      /* unused */
    },
    hasSession(agentAddress: string): boolean {
      return mock.addresses.includes(agentAddress);
    },
    isProvisioned(agentAddress: string): boolean {
      return mock.addresses.includes(agentAddress);
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
        /* unused */
      },
  } satisfies SessionManager & {
    provisioned: HarnessConfig[];
    addresses: string[];
  } & { delivered?: DeliveredMessage[] };
  return mock;
}

const TEST_CONFIG: HarnessConfig = {
  sessionId: "ses_test-session-prune",
  agentId: "agent-prune",
  tenantId: "tenant-prune",
  principalId: "prin_prune",
  agentAddress: "agent-prune@test.interchange",
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

type TestEnv = {
  server: ReturnType<typeof Bun.serve>;
  router: SidecarRouter;
  receiveCount: { value: number };
  rejectFirstOfEvery: { value: number };
};

function startTestServer(): TestEnv {
  const receiveCount = { value: 0 };
  // The hub rejects the FIRST pack push in every fresh "epoch" with
  // `corrupt` so the link's bootstrap-retry arm fires per epoch. An
  // epoch starts on every `rejectFirstOfEvery.value` increment.
  const rejectFirstOfEvery = { value: 0 };
  let attemptsThisEpoch = 0;
  let currentEpoch = 0;

  const router = createSidecarRouter({
    requestTimeoutMs: 5000,
    hubPublicKey: "a".repeat(64),
    lookups: {
      async receiveWorkflowRunPack(_repoId, _pack, _ref, _commitSha) {
        receiveCount.value += 1;
        if (currentEpoch !== rejectFirstOfEvery.value) {
          currentEpoch = rejectFirstOfEvery.value;
          attemptsThisEpoch = 0;
        }
        attemptsThisEpoch += 1;
        if (attemptsThisEpoch === 1) {
          return { accepted: false, reason: "corrupt" };
        }
        return { accepted: true };
      },
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

  return { server, router, receiveCount, rejectFirstOfEvery };
}

const env = startTestServer();

afterAll(() => {
  env.server.stop(true);
});

describe("hub-link workflow-run pack bootstrap prune", () => {
  test("undeploy prunes bootstrap entries so a subsequent reset retries", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const keyStore = createTestKeyStore();

    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-bootstrap-prune",
      token: "test-token",
      transport,
      sessions,
      keyStore,
      deployRouter: createTestDeployRouter(sessions, keyStore),
    });

    client.connect();
    try {
      await waitFor(() =>
        env.router.getConnectedSidecars().includes("sc-bootstrap-prune"),
      );

      const agentAddress = TEST_CONFIG.agentAddress;
      await env.router.sendAgentDeploy(agentAddress, TEST_CONFIG);
      await waitFor(() =>
        env.router.getRoutableAddresses().includes(agentAddress),
      );

      const repoId = {
        kind: "workflow-run" as const,
        id: "dep-prune-1",
      };
      const ref = "refs/heads/events";
      const commitSha = "a".repeat(40);
      const pack = new Uint8Array([1, 2, 3, 4, 5]);

      // Epoch 1: first push rejected with `corrupt`, link retries
      // once, retry accepts. The bootstrap flag for the key is set.
      env.rejectFirstOfEvery.value = 1;
      await client.pushWorkflowRunPack({
        agentAddress,
        repoId,
        pack,
        ref,
        commitSha,
      });
      expect(env.receiveCount.value).toBe(2);

      // Undeploy the agent. The fix prunes bootstrap entries owned by
      // this deployment.
      await env.router.sendAgentUndeploy(agentAddress, "test prune");
      await waitFor(
        () => !env.router.getRoutableAddresses().includes(agentAddress),
      );

      // Re-deploy with the same address so the deploymentId is
      // identical -- mirrors the disaster-recovery scenario where the
      // hub's workflow-run repo for `(kind, id, ref)` is reset.
      await env.router.sendAgentDeploy(agentAddress, TEST_CONFIG);
      await waitFor(() =>
        env.router.getRoutableAddresses().includes(agentAddress),
      );

      // Epoch 2: the hub rejects the first push of the new epoch with
      // `corrupt` again. Without the prune, the link skips the
      // bootstrap-retry arm (the flag from epoch 1 is still set) and
      // surfaces the rejection. With the prune, the link runs the
      // retry once more and the push succeeds.
      env.rejectFirstOfEvery.value = 2;
      await client.pushWorkflowRunPack({
        agentAddress,
        repoId,
        pack,
        ref,
        commitSha,
      });
      expect(env.receiveCount.value).toBe(4);
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes("sc-bootstrap-prune"),
      );
    }
  });
});
