/* eslint-disable @typescript-eslint/no-non-null-assertion -- refs[0]! always follows expect(refs).toHaveLength(1) */
import { describe, test, expect, afterAll } from "bun:test";
import { type } from "arktype";
import { Hono } from "hono";
import { upgradeWebSocket, websocket } from "hono/bun";
import {
  createSidecarRouter,
  type SidecarAuthIdentity,
  type SidecarRouterConfig,
  type WsHandle,
} from "@intx/hub-sessions";
import { createInMemoryTransport } from "@intx/mail-memory";
import {
  createEd25519Crypto,
  generateKeyPair,
  verifySSHSignature,
} from "@intx/crypto";
import { base64Encode, hexEncode } from "@intx/types";
import type { HarnessConfig } from "@intx/types/runtime";

import {
  answerMalformedRequestFrame,
  classifyAssetPackRejectReason,
  cleartextTransportWarning,
  createHubLink,
  type DeployRouter,
  type ReconnectScheduler,
} from "./hub-link";
import {
  MAX_DEPLOYMENT_ERROR_LENGTH,
  SidecarFrame,
  type AgentDeployErrorFrame,
  type AgentDeployFrame,
  type AgentUndeployErrorFrame,
  type HostedIncarnation,
  type PackRejectFrame,
  type SessionErrorFrame,
} from "@intx/types/sidecar";
import type { RepoId } from "@intx/types/repo";

type TestSendPackOptions = { mountPath?: string; repoId?: RepoId };
import type { AgentKeyStore } from "../agent-key-store";
import type { SessionManager } from "../session-manager";
import type { ResolvedInboundMailPolicy } from "./inbound-signature";

// These tests exercise routing and protocol, not admission policy, so they
// hand the seam a policy that admits every outcome -- inbound mail routes as it
// did before enforcement, keeping the tests focused on what they assert.
const admitAllInboundMailPolicy: ResolvedInboundMailPolicy = {
  clean: "admit",
  error: "admit",
  untrustedFrom: "admit",
  mismatchedFrom: "admit",
  absentFrom: "admit",
  invalid: "admit",
  missing: "admit",
  unknown: "admit",
};

// These tests exercise routing and the hub-link protocol, not handshake
// auth, so the router accepts any token and keys off the claimed id.
const testIdentities = new Map<string, SidecarAuthIdentity>();
function ensureTestIdentity(sidecarId: string) {
  const existing = testIdentities.get(sidecarId);
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
  testIdentities.set(sidecarId, identity);
  return identity;
}
const acceptAnySidecar = {
  authenticateSidecar: async ({ sidecarId }: { sidecarId: string }) => ({
    sidecarId,
  }),
  resolveSidecarBindings: async (sidecarId: string) => [
    ensureTestIdentity(sidecarId),
  ],
} satisfies Pick<
  SidecarRouterConfig,
  "authenticateSidecar" | "resolveSidecarBindings"
>;

function prepareAllocationFrame(
  router: ReturnType<typeof createSidecarRouter>,
  data: string,
): void {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test server parses the handshake frame emitted by the typed HubLink under test
  const frame = JSON.parse(data) as {
    type?: string;
    sidecarId?: string;
    incarnations?: { address: string }[];
  };
  if (frame.type !== "hello" || frame.sidecarId === undefined) return;
  router.fenceAllocation(`allocation-${frame.sidecarId}`, 1);
  const identity = ensureTestIdentity(frame.sidecarId);
  const reported = frame.incarnations?.[0];
  if (frame.incarnations?.length === 1 && reported !== undefined) {
    Object.assign(identity, { workflowRunAddress: reported.address });
  }
}

// One live incarnation of `address` at the generation the test allocation
// holds, for `getIncarnations`.
function liveIncarnation(address: string) {
  return [{ address, generation: 1, state: "live" as const }];
}

function allocationTargetFor(
  router: ReturnType<typeof createSidecarRouter>,
  agentAddress: string,
) {
  const sidecarId = router.getConnectedSidecars()[0];
  if (sidecarId === undefined) throw new Error("No test sidecar is connected");
  const identity = testIdentities.get(sidecarId);
  if (identity === undefined || identity.kind !== "allocated") {
    throw new Error(`No allocation identity for ${sidecarId}`);
  }
  Object.assign(identity, { workflowRunAddress: agentAddress });
  return {
    allocationId: identity.allocationId,
    generation: identity.generation,
  };
}

function sendAgentDeploy(
  router: ReturnType<typeof createSidecarRouter>,
  agentAddress: string,
  config: HarnessConfig,
  workflow?: AgentDeployFrame["workflow"],
) {
  return router.sendAgentDeployToAllocation(
    allocationTargetFor(router, agentAddress),
    agentAddress,
    config,
    workflow,
  );
}

function sendPack(
  router: ReturnType<typeof createSidecarRouter>,
  agentAddress: string,
  pack: Uint8Array,
  ref: string,
  commitSha: string,
  options?: TestSendPackOptions,
) {
  return router.sendPackToAllocation(
    allocationTargetFor(router, agentAddress),
    agentAddress,
    pack,
    ref,
    commitSha,
    options,
  );
}

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
  resolveSenderCrypto: () => undefined;
  lookupInboundMailPolicy: () => ResolvedInboundMailPolicy;
  cacheSenderKey: () => Promise<void>;
  evictSenderKey: () => Promise<void>;
} {
  const keyStore = createTestKeyStore();
  return {
    keyStore,
    deployRouter: createTestDeployRouter(keyStore),
    // These tests do not exercise the inbound signature compare, so the verify
    // resolves no cached key and the refresh/evict sinks are no-ops.
    resolveSenderCrypto: () => undefined,
    lookupInboundMailPolicy: () => admitAllInboundMailPolicy,
    cacheSenderKey: async () => undefined,
    evictSenderKey: async () => undefined,
  };
}
import type { KeyPair } from "@intx/types/runtime";
import { hexDecode } from "@intx/types";
import { waitUntil } from "@intx/types/testing";

// In-memory AgentKeyStore for tests. Tests that exercise deploy-commit
// verification register keys via the public AgentKeyStore methods
// (loadOrGenerateKey, recordHubKey); the stub satisfies the interface
// and uses real @intx/crypto primitives so signatures round-trip
// through the production verify path.
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function createMockSessionManager(): SessionManager {
  return {
    initRepo: (_address: string) => Promise.resolve(),
    applyDeployPack: () => Promise.resolve(),
    applyAssetPack: () => Promise.resolve(),
    deleteAgentDir: () => Promise.resolve(),
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
      id: "anthropic:claude-sonnet-5",
      provider: "anthropic",
      baseURL: "https://api.anthropic.com",
      credentialId: "sk-test",
      model: "claude-sonnet-5",
    },
  ],
  defaultSource: "anthropic:claude-sonnet-5",
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
  router: ReturnType<typeof createSidecarRouter>;
  agentEvents: { addr: string; sid: string; event: unknown }[];
  outboundMail: { rawMessage: string; recipients: string[] }[];
  /**
   * Every frame the sidecar sent, in arrival order. The router consumes frames
   * without reporting them, so a test whose subject is what the SIDECAR emits
   * -- the heartbeat is one -- has this to wait on instead of a duration.
   */
  sidecarFrames: string[];
};

function startTestServer(): TestEnv {
  const agentEvents: TestEnv["agentEvents"] = [];
  const outboundMail: TestEnv["outboundMail"] = [];
  const sidecarFrames: TestEnv["sidecarFrames"] = [];

  const router = createSidecarRouter({
    withExecutableWorkflowRun: async (_target, send) => send(),
    ...acceptAnySidecar,
    validateSidecarIdentity: async () => true,
    requestTimeoutMs: 5000,
    hubPublicKey: "a".repeat(64),
    lookups: { readWorkflowRunRefTips: async () => STOPPED_REF_TIPS },
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
            sidecarFrames.push(evt.data);
            prepareAllocationFrame(router, evt.data);
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

  return { server, router, agentEvents, outboundMail, sidecarFrames };
}

// A bare hub endpoint that welcomes every hello, answers pings, and records
// every frame the sidecar sends. `send` delivers a frame to the sidecar that
// connected last. `silence` stops answering that connection's pings, so the
// sidecar drops it the way it drops a Hub it can no longer reach.
function startWelcomingHub(): {
  server: ReturnType<typeof Bun.serve>;
  frames: string[];
  send(frame: object): void;
  silence(): void;
} {
  const frames: string[] = [];
  const sockets: { send(data: string): void; silenced: boolean }[] = [];
  const app = new Hono();
  app.get(
    "/ws",
    upgradeWebSocket((_c) => {
      const socket = {
        send(_data: string): void {
          throw new Error("The test hub socket is not open yet");
        },
        silenced: false,
      };
      return {
        onOpen(_evt, ws) {
          socket.send = (data) => {
            ws.send(data);
          };
          sockets.push(socket);
        },
        onMessage(evt, ws) {
          if (typeof evt.data !== "string") return;
          frames.push(evt.data);
          const frame: { type: string } = JSON.parse(evt.data);
          if (frame.type === "hello") {
            ws.send(JSON.stringify({ type: "welcome", routed: [] }));
          }
          if (frame.type === "ping" && !socket.silenced) {
            ws.send(JSON.stringify({ type: "pong" }));
          }
        },
      };
    }),
  );
  const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });
  const current = () => {
    const socket = sockets.at(-1);
    if (socket === undefined) {
      throw new Error("No sidecar is connected to the test hub");
    }
    return socket;
  };
  return {
    server,
    frames,
    send(frame) {
      current().send(JSON.stringify(frame));
    },
    silence() {
      current().silenced = true;
    },
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Longer than the runner's budget, so an acknowledgement, not the clock,
// settles each workflow control request.
const CONTROL_TIMEOUT_MS = 60_000;

// The ref tips a stopped test worker reports, which the test Hub holds.
const STOPPED_REF_TIPS = {
  "refs/heads/main": "c".repeat(40),
  "refs/heads/events": null,
};

const env = startTestServer();

afterAll(async () => {
  await env.server.stop(true);
});

/**
 * Wire a workflow deployment for the reconnect path: mint an Ed25519
 * keypair and register it in the sidecar's keyStore so the deploy path
 * picks up the pinned key. After this, the deployment address named in
 * `getIncarnations` routes once the hub's `welcome` routes it on
 * (re)connect.
 */
async function provisionDeploymentKey(
  keyStore: ReturnType<typeof createTestKeyStore>,
  address: string,
): Promise<void> {
  const kp = await generateKeyPair();
  keyStore.registerKey(address, kp);
}

describe("sidecar↔hub integration", () => {
  test("a cancellation finishing after reconnect does not reply on the new connection", async () => {
    const sockets: TestSocket[] = [];
    const stopped = Promise.withResolvers<undefined>();
    class TestSocket extends EventTarget {
      static readonly OPEN = 1;
      readyState = 1;
      readonly sent: string[] = [];
      constructor(_url: string | URL) {
        super();
        sockets.push(this);
      }
      send(raw: string) {
        this.sent.push(raw);
        const frame: unknown = JSON.parse(raw);
        if (
          typeof frame === "object" &&
          frame !== null &&
          "type" in frame &&
          frame.type === "workflow.control.ack" &&
          "requestId" in frame &&
          frame.requestId === "stop"
        ) {
          stopped.resolve(undefined);
        }
      }
      close() {
        this.readyState = 3;
        this.dispatchEvent(new Event("close"));
      }
      receive(frame: unknown) {
        this.dispatchEvent(
          new MessageEvent("message", { data: JSON.stringify(frame) }),
        );
      }
    }
    const originalWebSocket = globalThis.WebSocket;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- controlled transport double implements the WebSocket members this link uses
    globalThis.WebSocket = TestSocket as unknown as typeof WebSocket;
    const started = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const finished = Promise.withResolvers<undefined>();
    const reconnect = Promise.withResolvers<() => void>();
    const bindings = withTestDeployBindings();
    const client = createHubLink({
      hubURL: "ws://localhost/control-test",
      sidecarId: "control-reconnect",
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...bindings,
      scheduleReconnect(callback) {
        reconnect.resolve(callback);
        return () => undefined;
      },
      deployRouter: {
        ...bindings.deployRouter,
        async control(frame) {
          if (frame.action === "stop") return { refTips: STOPPED_REF_TIPS };
          started.resolve(undefined);
          await release.promise;
          finished.resolve(undefined);
          return {};
        },
      },
    });
    try {
      client.connect();
      const first = sockets[0];
      if (first === undefined) throw new Error("Missing first socket");
      first.dispatchEvent(new Event("open"));
      const command = {
        type: "workflow.control",
        runId: "run_control",
        agentAddress: "run_control@example.test",
        generation: 1,
        reason: "Stop",
      };
      first.receive({ ...command, action: "cancel", requestId: "cancel" });
      await started.promise;
      first.close();
      (await reconnect.promise)();
      const second = sockets[1];
      if (second === undefined) throw new Error("Missing second socket");
      second.dispatchEvent(new Event("open"));
      release.resolve(undefined);
      await finished.promise;
      second.receive({ ...command, action: "stop", requestId: "stop" });
      await stopped.promise;
      expect(second.sent.map((raw) => JSON.parse(raw))).toEqual([
        {
          type: "hello",
          sidecarId: "control-reconnect",
          token: "test-token",
          incarnations: [],
        },
        {
          type: "workflow.control.ack",
          requestId: "stop",
          refTips: STOPPED_REF_TIPS,
        },
      ]);
      expect(first.sent).toHaveLength(1);
    } finally {
      release.resolve(undefined);
      client.close();
      globalThis.WebSocket = originalWebSocket;
    }
  });

  test("forced stop passes a pending cancellation without releasing its acknowledgement early", async () => {
    const sidecarId = "control-preemption";
    const identity = ensureTestIdentity(sidecarId);
    if (identity.kind !== "allocated")
      throw new Error("Expected allocated identity");
    const connected = Promise.withResolvers<undefined>();
    env.router.events.on("sidecar.allocated.connected", ({ allocationId }) => {
      if (allocationId === identity.allocationId) connected.resolve(undefined);
    });
    const started = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const bindings = withTestDeployBindings();
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId,
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...bindings,
      deployRouter: {
        ...bindings.deployRouter,
        async control(frame) {
          if (frame.action === "stop") return { refTips: STOPPED_REF_TIPS };
          started.resolve(undefined);
          await release.promise;
          return {};
        },
      },
    });
    client.connect();
    await connected.promise;
    let cancellationAcknowledged = false;
    const cancel = env.router
      .sendWorkflowControl(
        identity,
        {
          runId: identity.anchorRunId,
          agentAddress: identity.workflowRunAddress,
          action: "cancel",
          reason: "Stop",
        },
        CONTROL_TIMEOUT_MS,
      )
      .then(() => {
        cancellationAcknowledged = true;
      });
    const cancelResult = cancel.catch((cause: unknown) => cause);
    try {
      await Promise.race([started.promise, cancelResult]);
      await env.router.sendWorkflowControl(
        identity,
        {
          runId: identity.anchorRunId,
          agentAddress: identity.workflowRunAddress,
          action: "stop",
          reason: "Grace expired",
        },
        CONTROL_TIMEOUT_MS,
      );
      expect(cancellationAcknowledged).toBe(false);
      release.resolve(undefined);
      expect(await cancelResult).toBeUndefined();
    } finally {
      release.resolve(undefined);
      await cancelResult;
      const disconnected = Promise.withResolvers<undefined>();
      const wasConnected = env.router
        .getConnectedSidecars()
        .includes(sidecarId);
      env.router.events.on("sidecar.disconnect", ({ allocated }) => {
        if (
          allocated.some(
            (binding) => binding.allocationId === identity.allocationId,
          )
        )
          disconnected.resolve(undefined);
      });
      client.close();
      if (wasConnected) await disconnected.promise;
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
  });

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
      await waitUntil(() =>
        env.router.getConnectedSidecars().includes("test-sidecar"),
      );
      expect(env.router.getConnectedSidecars()).toContain("test-sidecar");
    } finally {
      client.close();
      await waitUntil(
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
      await waitUntil(() =>
        env.router.getConnectedSidecars().includes("sc-create"),
      );

      await sendAgentDeploy(
        env.router,
        "agent-1@test.interchange",
        TEST_CONFIG,
      );

      // The deploy resolves against the ack; the sidecar stays connected
      // after handling it.
      expect(env.router.getConnectedSidecars()).toContain("sc-create");
    } finally {
      client.close();
      await waitUntil(
        () => !env.router.getConnectedSidecars().includes("sc-create"),
      );
    }
  });

  test("sidecar forwards agent events to hub", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const startLength = env.agentEvents.length;
    // Events are best-effort and go out only once the Hub has routed the
    // incarnation, which the welcome reports.
    const routed = Promise.withResolvers<string[]>();
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-events",
      token: "test-token",

      transport,
      sessions,
      ...withTestDeployBindings(),
      getIncarnations: () => liveIncarnation("agent-1@test.interchange"),
      onWorkflowAddressesRoutable: (addresses) => {
        routed.resolve(addresses);
      },
    });

    client.connect();
    try {
      expect(await routed.promise).toEqual(["agent-1@test.interchange"]);

      client.sendEvent("agent-1@test.interchange", 1, "sess-1", {
        type: "reactor.start",
        seq: 0,
        data: {},
      });

      await waitUntil(() => env.agentEvents.length > startLength);
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
      await waitUntil(
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
      getIncarnations: () => liveIncarnation("sender@test.interchange"),
    });

    client.connect();
    try {
      await waitUntil(() =>
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

      await waitUntil(() => env.outboundMail.length > startLength);
      const mail = env.outboundMail[env.outboundMail.length - 1];
      expect(mail?.recipients).toEqual(["remote@other.interchange"]);
    } finally {
      client.close();
      await waitUntil(
        () => !env.router.getConnectedSidecars().includes("sc-mail-out"),
      );
    }
  });

  test("disconnect cleans up routing table", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deploymentAddress = "run_disc1@integration.interchange";

    const bindings = withTestDeployBindings();
    await provisionDeploymentKey(bindings.keyStore, deploymentAddress);
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-disconnect",
      token: "test-token",
      transport,
      sessions,
      ...bindings,
      getIncarnations: () => liveIncarnation(deploymentAddress),
    });

    client.connect();
    // Routability lags the connection: it lands only after the hub routes
    // the incarnations the hello reports, so wait on the routable address
    // directly.
    await waitUntil(() =>
      env.router.getRoutableAddresses().includes(deploymentAddress),
    );

    client.close();
    await waitUntil(
      () => !env.router.getConnectedSidecars().includes("sc-disconnect"),
    );
    expect(env.router.getRoutableAddresses()).not.toContain(deploymentAddress);
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
      await waitUntil(() =>
        env.router.getConnectedSidecars().includes("sc-pack-reject"),
      );

      await sendAgentDeploy(
        env.router,
        "pack-agent@test.interchange",
        TEST_CONFIG,
      );

      sessions.applyDeployPack = () => {
        throw new Error("signature_invalid: bad signature");
      };

      await expect(
        sendPack(
          env.router,
          "pack-agent@test.interchange",
          new Uint8Array([1, 2, 3]),
          "refs/heads/deploy",
          "a".repeat(40),
        ),
      ).rejects.toThrow("Pack rejected: signature_invalid");
    } finally {
      client.close();
      await waitUntil(
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
      await waitUntil(() =>
        env.router.getConnectedSidecars().includes("sc-pack-unsigned"),
      );

      await sendAgentDeploy(
        env.router,
        "unsigned-agent@test.interchange",
        TEST_CONFIG,
      );

      sessions.applyDeployPack = () => {
        throw new Error("signature_unsigned: no signature found");
      };

      await expect(
        sendPack(
          env.router,
          "unsigned-agent@test.interchange",
          new Uint8Array([1, 2, 3]),
          "refs/heads/deploy",
          "a".repeat(40),
        ),
      ).rejects.toThrow("Pack rejected: signature_invalid");
    } finally {
      client.close();
      await waitUntil(
        () => !env.router.getConnectedSidecars().includes("sc-pack-unsigned"),
      );
    }
  });

  test("malformed hubPublicKey in deploy frame sends agent.deploy.error", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();

    // Stand up a hub router with an odd-length hex key to trigger hexDecode.
    const badRouter = createSidecarRouter({
      withExecutableWorkflowRun: async (_target, send) => send(),
      ...acceptAnySidecar,
      validateSidecarIdentity: async () => true,
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
              prepareAllocationFrame(badRouter, evt.data);
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
      await waitUntil(() =>
        badRouter.getConnectedSidecars().includes("sc-bad-hex"),
      );

      // Deploy should fail because hexDecode throws on the odd-length key.
      await expect(
        sendAgentDeploy(badRouter, "bad-hex@test.interchange", TEST_CONFIG),
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
      withExecutableWorkflowRun: async (_target, send) => send(),
      ...acceptAnySidecar,
      validateSidecarIdentity: async () => true,
      requestTimeoutMs: 5000,
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
              prepareAllocationFrame(deployHubRouter, evt.data);
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
      resolveSenderCrypto: () => undefined,
      lookupInboundMailPolicy: () => admitAllInboundMailPolicy,
      cacheSenderKey: async () => undefined,
      evictSenderKey: async () => undefined,
      deployRouter: createTestDeployRouter(keyStore),
    });

    client.connect();
    try {
      await waitUntil(() =>
        deployHubRouter.getConnectedSidecars().includes("sc-deploy-key"),
      );

      // The hub's deploy frame carries hubPublicKeyHex; the deploy router
      // records it via keyStore.recordHubKey, so a later pack's verifyCommit
      // is bound to the hub key.
      await sendAgentDeploy(deployHubRouter, deployedAddress, {
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

      await sendPack(
        deployHubRouter,
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
      await waitUntil(() =>
        pingEnv.router.getConnectedSidecars().includes("sc-ping"),
      );

      // The heartbeat closes the link at the first tick that finds no pong
      // within two ping intervals, so the SECOND tick is where a missing pong
      // takes the connection down. Waiting for a third ping therefore waits
      // past that point: the sidecar only reaches it by having been answered.
      const pings = () =>
        pingEnv.sidecarFrames.filter(
          (frame) => JSON.parse(frame).type === "ping",
        );
      await waitUntil(() => pings().length >= 3);

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
      await waitUntil(() =>
        env.router.getConnectedSidecars().includes("sc-asset-route"),
      );

      await sendAgentDeploy(
        env.router,
        "route-agent@test.interchange",
        TEST_CONFIG,
      );

      // Deploy pack (no mountPath) → applyDeployPack.
      await sendPack(
        env.router,
        "route-agent@test.interchange",
        new Uint8Array([1, 2, 3]),
        "refs/heads/deploy",
        "a".repeat(40),
      );
      expect(deployCalls).toEqual(["route-agent@test.interchange"]);
      expect(assetCalls).toEqual([]);

      // Asset pack (mountPath set) → applyAssetPack.
      await sendPack(
        env.router,
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
      await waitUntil(
        () => !env.router.getConnectedSidecars().includes("sc-asset-route"),
      );
    }
  });

  test("workflow-run packs route through the restore boundary instead of the deploy tree", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deployCalls: string[] = [];
    const restoreCalls: {
      agentAddress: string;
      repoId: RepoId;
      ref: string;
      commitSha: string;
      pack: Uint8Array;
    }[] = [];
    sessions.applyDeployPack = (address) => {
      deployCalls.push(address);
      return Promise.resolve();
    };
    const address = "run_restore@workflow.test";
    const repoId: RepoId = {
      kind: "workflow-run",
      id: "run_restore-workflow-test",
    };

    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-workflow-restore-route",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
      applyWorkflowRunPack: async (args) => {
        restoreCalls.push(args);
      },
    });

    client.connect();
    try {
      await waitUntil(() =>
        env.router.getConnectedSidecars().includes("sc-workflow-restore-route"),
      );
      await sendAgentDeploy(env.router, address, TEST_CONFIG);

      const pack = new Uint8Array([7, 8, 9]);
      await sendPack(
        env.router,
        address,
        pack,
        "refs/heads/events",
        "c".repeat(40),
        { repoId },
      );

      expect(deployCalls).toEqual([]);
      expect(restoreCalls).toEqual([
        {
          agentAddress: address,
          repoId,
          ref: "refs/heads/events",
          commitSha: "c".repeat(40),
          pack,
        },
      ]);
    } finally {
      client.close();
      await waitUntil(
        () =>
          !env.router
            .getConnectedSidecars()
            .includes("sc-workflow-restore-route"),
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
      await waitUntil(() =>
        env.router.getConnectedSidecars().includes("sc-asset-fail"),
      );
      await sendAgentDeploy(
        env.router,
        "fail-asset@test.interchange",
        TEST_CONFIG,
      );

      await expect(
        sendPack(
          env.router,
          "fail-asset@test.interchange",
          new Uint8Array([1, 2, 3]),
          "refs/heads/main",
          "a".repeat(40),
          { mountPath: "skills/example/" },
        ),
      ).rejects.toThrow("Pack rejected: corrupt");
    } finally {
      client.close();
      await waitUntil(
        () => !env.router.getConnectedSidecars().includes("sc-asset-fail"),
      );
    }
  });

  test("close cancels a pending reconnect scheduled by the previous disconnect", async () => {
    const reconnectEnv = startTestServer();

    // Inject a fake scheduler so we can observe the reconnect callback
    // and the delay it was scheduled with directly, rather than waiting
    // for a real timer. The cancel function nils the captured callback;
    // after close() the callback must be gone, otherwise the bug is
    // present.
    let pendingReconnect: (() => void) | null = null;
    const scheduledDelays: number[] = [];
    const fakeScheduleReconnect: ReconnectScheduler = (cb, delayMs) => {
      pendingReconnect = cb;
      scheduledDelays.push(delayMs);
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
      await waitUntil(() =>
        reconnectEnv.router
          .getConnectedSidecars()
          .includes("sc-reconnect-race"),
      );

      // Force a disconnect by stopping the server. The WebSocket fires
      // its close event on the client, which schedules a reconnect
      // through the fake scheduler.
      await reconnectEnv.server.stop(true);
      await waitUntil(() => pendingReconnect !== null);

      // This link supplies no `reconnectDelayMs`, so the delay the seam
      // receives is the link's own `DEFAULT_RECONNECT_DELAY_MS`. Reading
      // it off the injected scheduler is what pins that constant --
      // nothing here measures elapsed time.
      expect(scheduledDelays).toEqual([3_000]);

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

  test("a configured reconnectDelayMs is the delay the reconnect is scheduled with", async () => {
    const reconnectEnv = startTestServer();

    // The other half of the pin above: the sidecar's
    // `SIDECAR_RECONNECT_DELAY_MS` arrives here as `reconnectDelayMs`, so
    // this is where a link that drops the option -- or substitutes its own
    // default for a supplied one -- is caught. The scheduler seam reports
    // the delay as a number, so the assertion needs no clock.
    const configuredDelayMs = 250;
    const scheduledDelays: number[] = [];
    const fakeScheduleReconnect: ReconnectScheduler = (_cb, delayMs) => {
      scheduledDelays.push(delayMs);
      // The callback is never fired, so no second connect attempt runs and
      // there is nothing for close() to cancel.
      return () => undefined;
    };

    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const client = createHubLink({
      hubURL: `ws://localhost:${reconnectEnv.server.port}/ws`,
      sidecarId: "sc-reconnect-delay",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
      reconnectDelayMs: configuredDelayMs,
      scheduleReconnect: fakeScheduleReconnect,
    });

    try {
      client.connect();
      await waitUntil(() =>
        reconnectEnv.router
          .getConnectedSidecars()
          .includes("sc-reconnect-delay"),
      );

      await reconnectEnv.server.stop(true);
      await waitUntil(() => scheduledDelays.length > 0);

      expect(scheduledDelays).toEqual([configuredDelayMs]);
    } finally {
      client.close();
      await reconnectEnv.server.stop(true);
    }
  });

  test("mailInboundRouter claims an address and skips the legacy fallback", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    // The deployment address is what the hub routes mail to. The
    // sidecar reports it in its hello so the hub-side router routes
    // it. Routing a mail.inbound for it goes through the link's switch
    // case, which must consult mailInboundRouter first and -- on a
    // non-null return -- skip transport.deliver and
    // sessions.commitInboundMail.
    const deploymentAddress = "run_mail1@integration.interchange";

    const routed: { address: string; bytes: Uint8Array }[] = [];
    const mailInboundRouter = {
      tryRoute(address: string, message: Uint8Array): Promise<void> | null {
        routed.push({ address, bytes: message });
        return Promise.resolve();
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
      getIncarnations: () => liveIncarnation(deploymentAddress),
    });

    client.connect();
    try {
      await waitUntil(() =>
        env.router.getRoutableAddresses().includes(deploymentAddress),
      );

      const encoded = base64Encode(VALID_MESSAGE);
      const accepted = await env.router.routeMail(
        deploymentAddress,
        encoded,
        "user@integration.interchange",
      );
      expect(accepted).toBe(true);

      await waitUntil(() => routed.length > 0);

      expect(routed).toHaveLength(1);
      expect(routed[0]?.address).toBe(deploymentAddress);
      expect(routed[0]?.bytes).toEqual(VALID_MESSAGE);
    } finally {
      client.close();
      await waitUntil(
        () => !env.router.getConnectedSidecars().includes("sc-multistep-mail"),
      );
    }
  });

  test("drainInboundRouter dispatches an inbound drain.deliver frame", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deploymentAddress = "run_drain1@integration.interchange";

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
      getIncarnations: () => liveIncarnation(deploymentAddress),
    });

    client.connect();
    try {
      await waitUntil(() =>
        env.router.getRoutableAddresses().includes(deploymentAddress),
      );

      env.router.sendDrain({
        agentAddress: deploymentAddress,
        deadlineMs: 4_321,
      });

      await waitUntil(() => routed.length > 0);

      expect(routed).toHaveLength(1);
      expect(routed[0]?.agentAddress).toBe(deploymentAddress);
      expect(routed[0]?.deadlineMs).toBe(4_321);
    } finally {
      client.close();
      await waitUntil(
        () => !env.router.getConnectedSidecars().includes("sc-drain-router"),
      );
    }
  });

  const ROTATION_SOURCE = {
    id: "primary",
    provider: "anthropic",
    baseURL: "https://api.anthropic.com",
    credentialId: "sk-rotation",
    model: "claude-rotation",
  };

  test("sourcesInboundRouter acks an inbound sources.update round-trip", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deploymentAddress = "run_srcack@integration.interchange";

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
      getIncarnations: () => liveIncarnation(deploymentAddress),
    });

    client.connect();
    try {
      await waitUntil(() =>
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
      await waitUntil(
        () => !env.router.getConnectedSidecars().includes("sc-sources-ack"),
      );
    }
  });

  test("an unrouted sources.update is answered with session.error", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deploymentAddress = "run_srcunrouted@integration.interchange";

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
      getIncarnations: () => liveIncarnation(deploymentAddress),
    });

    client.connect();
    try {
      await waitUntil(() =>
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
      await waitUntil(
        () =>
          !env.router.getConnectedSidecars().includes("sc-sources-unrouted"),
      );
    }
  });

  test("a rejected sources.update surfaces the reason as session.error", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deploymentAddress = "run_srcreject@integration.interchange";

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
      getIncarnations: () => liveIncarnation(deploymentAddress),
    });

    client.connect();
    try {
      await waitUntil(() =>
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
      await waitUntil(
        () => !env.router.getConnectedSidecars().includes("sc-sources-reject"),
      );
    }
  });

  test("a sources.update with no router wired is answered with session.error", async () => {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deploymentAddress = "run_srcnorouter@integration.interchange";

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
      getIncarnations: () => liveIncarnation(deploymentAddress),
    });

    client.connect();
    try {
      await waitUntil(() =>
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
      await waitUntil(
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
      withExecutableWorkflowRun: async (_target, send) => send(),
      ...acceptAnySidecar,
      validateSidecarIdentity: async () => true,
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
              prepareAllocationFrame(wfrRouter, evt.data);
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

    const welcomed = Promise.withResolvers<boolean>();
    const client = createHubLink({
      hubURL: `ws://localhost:${wfrServer.port}/ws`,
      sidecarId: "sc-wfr-bootstrap-race",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
      onWorkflowAddressesRoutable: () => {
        welcomed.resolve(true);
      },
    });

    client.connect();
    try {
      await welcomed.promise;

      // Deploy an agent so the sender's outbound pack frames carry a
      // routable address; otherwise the hub drops the push as
      // "unrouted agent" before it ever reaches `receiveWorkflowRunPack`.
      const agentAddress = "race-agent@test.interchange";
      await sendAgentDeploy(wfrRouter, agentAddress, TEST_CONFIG);
      await waitUntil(() =>
        wfrRouter.getRoutableAddresses().includes(agentAddress),
      );

      const repoId = {
        kind: "workflow-run",
        // `deriveWorkflowRunRepoId(agentAddress)`; kept literal here so this
        // package's tests do not acquire a runtime dependency on the deployer.
        id: "race-agent-test-interchange",
      } as const;
      const ref = "refs/heads/events";
      const commitSha = "a".repeat(40);
      const pack = new Uint8Array([1, 2, 3, 4, 5]);

      // Kick both pushes off in the same tick so they both observe the
      // queue's pre-A state and B genuinely chains through A's
      // promise.
      const pushA = client.pushWorkflowRunPack({
        agentAddress,
        generation: 1,
        repoId,
        pack,
        ref,
        commitSha,
      });
      const pushB = client.pushWorkflowRunPack({
        agentAddress,
        generation: 1,
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
      await waitUntil(
        () =>
          !wfrRouter.getConnectedSidecars().includes("sc-wfr-bootstrap-race"),
      );
      await wfrServer.stop(true);
    }
  });

  test("a push the Hub rejects for any reason but corrupt is not resent", async () => {
    const doneFrames: string[] = [];
    const rejectingRouter = createSidecarRouter({
      withExecutableWorkflowRun: async (_target, send) => send(),
      ...acceptAnySidecar,
      validateSidecarIdentity: async () => true,
      requestTimeoutMs: 5000,
      hubPublicKey: "a".repeat(64),
      lookups: {
        async receiveWorkflowRunPack() {
          return { accepted: false, reason: "path_violation" };
        },
      },
    });
    const rejectingApp = new Hono();
    rejectingApp.get(
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
            rejectingRouter.handleOpen(handle);
          },
          onMessage(evt, _ws) {
            if (typeof evt.data !== "string") return;
            if (evt.data.includes('"repo.pack.done"'))
              doneFrames.push(evt.data);
            prepareAllocationFrame(rejectingRouter, evt.data);
            rejectingRouter.handleMessage(handle, evt.data);
          },
          onClose(_evt, _ws) {
            rejectingRouter.handleClose(handle);
          },
        };
      }),
    );
    const rejectingServer = Bun.serve({
      fetch: rejectingApp.fetch,
      websocket,
      port: 0,
    });
    const welcomed = Promise.withResolvers<boolean>();
    const client = createHubLink({
      hubURL: `ws://localhost:${rejectingServer.port}/ws`,
      sidecarId: "sc-wfr-no-retry",
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      onWorkflowAddressesRoutable: () => {
        welcomed.resolve(true);
      },
    });

    client.connect();
    try {
      await welcomed.promise;
      const agentAddress = "no-retry-agent@test.interchange";
      await sendAgentDeploy(rejectingRouter, agentAddress, TEST_CONFIG);
      await waitUntil(() =>
        rejectingRouter.getRoutableAddresses().includes(agentAddress),
      );

      const pushed = client.pushWorkflowRunPack({
        agentAddress,
        generation: 1,
        repoId: { kind: "workflow-run", id: "no-retry-agent-test-interchange" },
        pack: new Uint8Array([1, 2, 3]),
        ref: "refs/heads/events",
        commitSha: "b".repeat(40),
      });

      await expect(pushed).rejects.toThrow(/reason=path_violation/);
      expect(doneFrames).toHaveLength(1);
    } finally {
      client.close();
      await waitUntil(
        () =>
          !rejectingRouter.getConnectedSidecars().includes("sc-wfr-no-retry"),
      );
      await rejectingServer.stop(true);
    }
  });
});

describe("initial handshake on connect", () => {
  test("reports every held incarnation and holds reports until the welcome", async () => {
    const frames: string[] = [];
    const sockets: { send(data: string): void }[] = [];
    const app = new Hono();
    app.get(
      "/ws",
      upgradeWebSocket((_c) => ({
        onOpen(_evt, ws) {
          sockets.push(ws);
        },
        onMessage(evt, _ws) {
          if (typeof evt.data === "string") {
            frames.push(evt.data);
          }
        },
      })),
    );
    const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });

    const incarnations = [
      {
        address: "live@integration.interchange",
        generation: 3,
        state: "live" as const,
      },
      {
        address: "deploying@integration.interchange",
        generation: 1,
        state: "deploying" as const,
      },
      {
        address: "leaving@integration.interchange",
        generation: 2,
        state: "tearing-down" as const,
      },
    ];
    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-hello",
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      getIncarnations: () => incarnations,
    });
    const types = () =>
      frames.map((raw) => {
        const frame: { type: string } = JSON.parse(raw);
        return frame.type;
      });

    client.connect();
    try {
      // An event is best-effort and goes nowhere before the welcome, and so
      // does a stop report, which the sidecar sends again after it; the
      // register is owed and waits for it.
      client.sendEvent("live@integration.interchange", 3, "sess-queued", {
        type: "reactor.start",
        seq: 0,
        data: {},
      });
      const stopped = {
        agentAddress: "live@integration.interchange",
        generation: 3,
        error: "Its workflow child ended itself",
        refTips: { "refs/heads/main": "c".repeat(40) },
      };
      client.sendDeploymentStopped(stopped);
      client.sendSignalCorrelationRegister({
        correlationId: "corr-queued",
        runId: "run-1",
        anchorRunId: "anchor-1",
        agentAddress: "live@integration.interchange",
        generation: 3,
        kind: "approval",
        approvalSnapshot: {
          name: "tool",
          description: "a tool",
          inputSchema: {},
          arguments: {},
        },
      });

      await waitUntil(() => types().includes("hello"));
      expect(types()).toEqual(["hello"]);
      const hello: { sidecarId: string; incarnations: unknown } = JSON.parse(
        frames[0]!,
      );
      expect(hello.sidecarId).toBe("sc-hello");
      expect(hello.incarnations).toEqual(incarnations);

      sockets[0]!.send(JSON.stringify({ type: "welcome", routed: [] }));
      await waitUntil(() => types().includes("signal.correlation.register"));
      expect(types()).toEqual(["hello", "signal.correlation.register"]);

      client.sendDeploymentStopped(stopped);
      await waitUntil(() => types().includes("deployment.stopped"));
      expect(JSON.parse(frames.at(-1)!)).toEqual({
        type: "deployment.stopped",
        ...stopped,
      });
    } finally {
      client.close();
      await server.stop(true);
    }
  });

  test("a register waiting for the welcome is not retried into the queue", async () => {
    const frames: string[] = [];
    const sockets: { send(data: string): void }[] = [];
    const app = new Hono();
    app.get(
      "/ws",
      upgradeWebSocket((_c) => ({
        onOpen(_evt, ws) {
          sockets.push(ws);
        },
        onMessage(evt, _ws) {
          if (typeof evt.data === "string") {
            frames.push(evt.data);
          }
        },
      })),
    );
    const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });
    const armedRetries = new Set<() => void>();
    const fireArmedRetries = () => {
      for (const retry of [...armedRetries]) {
        armedRetries.delete(retry);
        retry();
      }
    };
    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-register-held",
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      registerAckMaxAttempts: 3,
      scheduleRegisterRetry: (retry) => {
        armedRetries.add(retry);
        return () => armedRetries.delete(retry);
      },
      getIncarnations: () => [],
    });
    const types = () =>
      frames.map((raw) => {
        const frame: { type: string } = JSON.parse(raw);
        return frame.type;
      });

    const register = (correlationId: string) => {
      client.sendSignalCorrelationRegister({
        correlationId,
        runId: "run-1",
        anchorRunId: "anchor-1",
        agentAddress: "live@integration.interchange",
        generation: 1,
        kind: "approval",
        approvalSnapshot: {
          name: "tool",
          description: "a tool",
          inputSchema: {},
          arguments: {},
        },
      });
    };
    const registersFor = (correlationId: string) =>
      frames.filter((raw) => {
        const frame: { type: string; correlationId?: string } = JSON.parse(raw);
        return (
          frame.type === "signal.correlation.register" &&
          frame.correlationId === correlationId
        );
      });

    client.connect();
    try {
      await waitUntil(() => types().includes("hello"));
      register("corr-held");
      // Every retry the held register arms comes due before the welcome; a
      // retry that resends would queue a copy behind the held one.
      expect(armedRetries.size).toBe(1);
      fireArmedRetries();
      expect(armedRetries.size).toBe(0);

      sockets[0]!.send(JSON.stringify({ type: "welcome", routed: [] }));
      await waitUntil(() => registersFor("corr-held").length === 1);
      // The socket delivers in send order, so any copy the welcome flushed
      // arrives before the marker sent after it.
      register("corr-marker");
      await waitUntil(() => registersFor("corr-marker").length === 1);
      expect(registersFor("corr-held")).toHaveLength(1);
      expect(types()[0]).toBe("hello");
    } finally {
      client.close();
      await server.stop(true);
    }
  });

  test("an outage that fills the queue drops audit copies before it refuses mail to route", async () => {
    const frames: string[] = [];
    const sockets: { send(data: string): void }[] = [];
    const app = new Hono();
    app.get(
      "/ws",
      upgradeWebSocket((_c) => ({
        onOpen(_evt, ws) {
          sockets.push(ws);
        },
        onMessage(evt, _ws) {
          if (typeof evt.data === "string") frames.push(evt.data);
        },
      })),
    );
    const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });
    const sender = "run_outage@integration.interchange";
    const transport = createInMemoryTransport();
    transport.register(sender, createEd25519Crypto(await generateKeyPair()));
    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-outage",
      token: "test-token",
      transport,
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      getIncarnations: () => [
        { address: sender, generation: 1, state: "live" },
      ],
    });
    const send = (index: number) =>
      transport.getTransportFor(sender).send({
        to: "remote@example.test",
        type: "conversation.message",
        content: `mail ${String(index)}`,
      });
    const mailFrames = () =>
      frames.flatMap((raw) => {
        const frame: { type: string; delivered?: boolean } = JSON.parse(raw);
        return frame.type === "mail.outbound" ? [frame] : [];
      });

    client.connect();
    try {
      // The Hub never welcomes, so every send queues a routing frame and an
      // audit copy.
      await waitUntil(() => frames.length > 0);
      for (let index = 0; index < 1024; index += 1) await send(index);
      await expect(send(1024)).rejects.toThrow(
        "The outbound queue is full of mail waiting for the Hub",
      );

      sockets[0]!.send(JSON.stringify({ type: "welcome", routed: [] }));
      await waitUntil(() => mailFrames().length === 1024);
      expect(mailFrames().every((frame) => frame.delivered !== true)).toBe(
        true,
      );
    } finally {
      client.close();
      await server.stop(true);
    }
  });

  test("an outage that fills the queue drops an arriving correlation registration before an audit copy", async () => {
    const frames: string[] = [];
    const sockets: { send(data: string): void }[] = [];
    const app = new Hono();
    app.get(
      "/ws",
      upgradeWebSocket((_c) => ({
        onOpen(_evt, ws) {
          sockets.push(ws);
        },
        onMessage(evt, _ws) {
          if (typeof evt.data === "string") frames.push(evt.data);
        },
      })),
    );
    const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });
    const sender = "run_outage_register@integration.interchange";
    const transport = createInMemoryTransport();
    transport.register(sender, createEd25519Crypto(await generateKeyPair()));
    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-outage-register",
      token: "test-token",
      transport,
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      getIncarnations: () => [
        { address: sender, generation: 1, state: "live" },
      ],
      // The acker would resend the dropped registration once welcomed, so only
      // the queue may decide whether it reaches the Hub.
      registerAckMaxAttempts: 1,
    });
    const sent = (type: string) =>
      frames.flatMap((raw) => {
        const frame: { type: string; delivered?: boolean } = JSON.parse(raw);
        return frame.type === type ? [frame] : [];
      });

    client.connect();
    try {
      // 512 sends queue 512 frames to route and 512 audit copies, which fills
      // the queue exactly while the Hub has not welcomed the connection.
      await waitUntil(() => frames.length > 0);
      for (let index = 0; index < 512; index += 1) {
        await transport.getTransportFor(sender).send({
          to: "remote@example.test",
          type: "conversation.message",
          content: `mail ${String(index)}`,
        });
      }
      client.sendSignalCorrelationRegister({
        correlationId: "corr-during-outage",
        runId: "run-1",
        anchorRunId: "anchor-1",
        agentAddress: sender,
        generation: 1,
        kind: "approval",
        approvalSnapshot: {
          name: "tool",
          description: "a tool",
          inputSchema: {},
          arguments: {},
        },
      });

      sockets[0]!.send(JSON.stringify({ type: "welcome", routed: [] }));
      await waitUntil(
        () =>
          sent("mail.outbound").length +
            sent("signal.correlation.register").length >=
          1024,
      );
      expect(sent("signal.correlation.register")).toHaveLength(0);
      expect(
        sent("mail.outbound").filter((frame) => frame.delivered === true),
      ).toHaveLength(512);
    } finally {
      client.close();
      await server.stop(true);
    }
  });

  test("refuses a send that names more than one workflow deployment", async () => {
    const app = new Hono();
    app.get(
      "/ws",
      upgradeWebSocket((_c) => ({})),
    );
    const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });
    const sender = "run_multi@integration.interchange";
    // Relay-only, as the sidecar's is, so the sender's own copy is relayed.
    const transport = createInMemoryTransport({ relayOnly: true });
    transport.register(sender, createEd25519Crypto(await generateKeyPair()));
    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-multi",
      token: "test-token",
      transport,
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      getIncarnations: () => [
        { address: sender, generation: 1, state: "live" },
      ],
    });
    const scoped = transport.getTransportFor(sender);
    try {
      await expect(
        scoped.send({
          to: "run_other@integration.interchange",
          cc: sender,
          type: "conversation.message",
          content: "reply that copies its sender",
        }),
      ).rejects.toThrow("a mail may name only one");
      await expect(
        scoped.send({
          to: "run_other@integration.interchange",
          cc: "person@example.test",
          type: "conversation.message",
          content: "one deployment",
        }),
      ).resolves.toMatchObject({ status: "queued" });
    } finally {
      client.close();
      await server.stop(true);
    }
  });

  test("sends a hello with no incarnations when none is held", async () => {
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

    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-hello-empty",
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      getIncarnations: () => [],
    });

    client.connect();
    try {
      await waitUntil(() => frames.length > 0);
      const parsed = frames.map((raw) => {
        const frame: { type: string; incarnations?: [] } = JSON.parse(raw);
        return frame;
      });
      expect(parsed.map((frame) => frame.type)).toEqual(["hello"]);
      expect(parsed[0]?.incarnations).toEqual([]);
    } finally {
      client.close();
      await server.stop(true);
    }
  });

  test("blocks every held address from the moment a connection opens until its welcome routes it", async () => {
    const frames: string[] = [];
    const sockets: { send(data: string): void }[] = [];
    const app = new Hono();
    app.get(
      "/ws",
      upgradeWebSocket((_c) => ({
        onOpen(_evt, ws) {
          sockets.push(ws);
        },
        onMessage(evt, _ws) {
          if (typeof evt.data === "string") {
            frames.push(evt.data);
          }
        },
      })),
    );
    const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });
    const events: string[] = [];

    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-blocked-at-open",
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      getIncarnations: () => [
        {
          address: "restored@integration.interchange",
          generation: 1,
          state: "live",
        },
      ],
      onWorkflowAddressesUnroutable: (addresses) => {
        events.push(`unroutable ${addresses.join(",")}`);
      },
      onWorkflowAddressesRoutable: (addresses) => {
        events.push(`routable ${addresses.join(",")}`);
      },
    });

    client.connect();
    try {
      await waitUntil(() => frames.length > 0);
      expect(events).toEqual(["unroutable restored@integration.interchange"]);

      sockets[0]!.send(
        JSON.stringify({
          type: "welcome",
          routed: [
            { address: "restored@integration.interchange", generation: 1 },
          ],
        }),
      );
      await waitUntil(() => events.length === 2);
      expect(events).toEqual([
        "unroutable restored@integration.interchange",
        "routable restored@integration.interchange",
      ]);
    } finally {
      client.close();
      await server.stop(true);
    }
  });

  test("reconnects when the Hub does not answer hello in time", async () => {
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

    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-unanswered",
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      welcomeTimeoutMs: 30,
      reconnectDelayMs: 10,
    });

    client.connect();
    try {
      client.sendSignalCorrelationRegister({
        correlationId: "corr-held",
        runId: "run-1",
        anchorRunId: "anchor-1",
        agentAddress: "live@integration.interchange",
        generation: 1,
        kind: "approval",
        approvalSnapshot: {
          name: "tool",
          description: "a tool",
          inputSchema: {},
          arguments: {},
        },
      });
      await waitUntil(
        () => frames.filter((s) => JSON.parse(s).type === "hello").length >= 2,
      );
      // The queued register never went out: no connection was welcomed.
      expect(frames.map((raw) => JSON.parse(raw).type)).toEqual(
        frames.map(() => "hello"),
      );
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
      getIncarnations: () => [],
    });

    try {
      expect(() =>
        client.sendSignalCorrelationRegister({
          correlationId: "corr-throw",
          runId: "run-1",
          anchorRunId: "dep-1",
          agentAddress: "run_dep@integration.interchange",
          generation: 1,
          kind: "approval",
        }),
      ).toThrow(/corr-throw/);
    } finally {
      client.close();
    }
  });

  test("sendSignalCorrelationRegister ships a register frame carrying the approval snapshot", async () => {
    const { server, frames } = startWelcomingHub();

    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();

    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-register-snapshot",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
      getIncarnations: () => [],
    });

    const snapshot = {
      name: "charge_card",
      description: "Charge the customer's card",
      inputSchema: { type: "object" },
      arguments: { amount: 100 },
    };

    client.connect();
    try {
      await waitUntil(() => frames.some((s) => JSON.parse(s).type === "hello"));
      client.sendSignalCorrelationRegister({
        correlationId: "corr-1",
        runId: "run-1",
        anchorRunId: "dep-1",
        agentAddress: "run_reg2@integration.interchange",
        generation: 4,
        kind: "approval",
        approvalSnapshot: snapshot,
      });
      await waitUntil(() =>
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
      expect(frame.generation).toBe(4);
      expect(frame.snapshot).toEqual(snapshot);
    } finally {
      client.close();
      await server.stop(true);
    }
  });

  test("an ack from the hub settles the register retry", async () => {
    const allFrames: string[] = [];
    const app = new Hono();
    // Acks "corr-ack" and deliberately ignores "corr-unacked". The unacked
    // correlation is the test's clock: its watchdog was armed AFTER the acked
    // one and carries the same interval, so its retry cannot reach the socket
    // before a retry of the acked one would have. Seeing it is proof the acked
    // correlation's window has passed, which a pause could only guess at.
    app.get(
      "/ws",
      upgradeWebSocket((_c) => ({
        onMessage(evt, ws) {
          if (typeof evt.data !== "string") return;
          allFrames.push(evt.data);
          const frame: { type: string; correlationId?: string } = JSON.parse(
            evt.data,
          );
          if (frame.type === "hello") {
            ws.send(JSON.stringify({ type: "welcome", routed: [] }));
          }
          if (
            frame.type === "signal.correlation.register" &&
            frame.correlationId === "corr-ack"
          ) {
            ws.send(
              JSON.stringify({
                type: "signal.correlation.register.ack",
                agentAddress: "run_reg_ack@integration.interchange",
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
      getIncarnations: () => [],
      // The watchdog here races a real round trip: the ack has to travel the
      // socket and be handled before the acked correlation's own watchdog
      // fires, or the retry this test forbids is the correct behaviour. At
      // thirty milliseconds a loaded machine loses that race -- observed
      // once in five runs of the unit pass at thirty-two workers -- so the
      // interval is several times the loopback trip it surrounds.
      registerAckTimeoutMs: 300,
      registerAckMaxAttempts: 10,
    });

    const registersFor = (correlationId: string): string[] =>
      allFrames.filter((s) => {
        const frame: { type: string; correlationId?: string } = JSON.parse(s);
        return (
          frame.type === "signal.correlation.register" &&
          frame.correlationId === correlationId
        );
      });

    const approvalSnapshot = {
      name: "charge_card",
      description: "Charge the customer's card",
      inputSchema: { type: "object" },
      arguments: { amount: 100 },
    };

    client.connect();
    try {
      await waitUntil(() =>
        allFrames.some((s) => JSON.parse(s).type === "hello"),
      );
      client.sendSignalCorrelationRegister({
        correlationId: "corr-ack",
        runId: "run-1",
        anchorRunId: "dep-1",
        agentAddress: "run_reg_ack@integration.interchange",
        generation: 1,
        kind: "approval",
        approvalSnapshot,
      });
      client.sendSignalCorrelationRegister({
        correlationId: "corr-unacked",
        runId: "run-2",
        anchorRunId: "dep-1",
        agentAddress: "run_reg_ack@integration.interchange",
        generation: 1,
        kind: "approval",
        approvalSnapshot,
      });

      // The unacked correlation's watchdog has fired and resent. Both frames
      // travel the one socket in send order, so a retry of the acked
      // correlation would already be recorded here -- and there is none.
      await waitUntil(() => registersFor("corr-unacked").length >= 2);
      expect(registersFor("corr-ack")).toHaveLength(1);
    } finally {
      client.close();
      await server.stop(true);
    }
  });

  test("a push before the welcome sends nothing and fails as a lost connection", async () => {
    const allFrames: string[] = [];
    const app = new Hono();
    app.get(
      "/ws",
      upgradeWebSocket((_c) => ({
        onMessage(evt, _ws) {
          if (typeof evt.data === "string") allFrames.push(evt.data);
        },
      })),
    );
    const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });
    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-push-unwelcomed",
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      getIncarnations: () =>
        liveIncarnation("run_early@integration.interchange"),
    });

    client.connect();
    try {
      await waitUntil(() =>
        allFrames.some((s) => JSON.parse(s).type === "hello"),
      );
      await expect(
        client.pushWorkflowRunPack({
          agentAddress: "run_early@integration.interchange",
          generation: 1,
          repoId: {
            kind: "workflow-run",
            id: "run_early-integration-interchange",
          },
          pack: new Uint8Array([1, 2, 3]),
          ref: "refs/heads/events",
          commitSha: "a".repeat(40),
        }),
      ).rejects.toThrow("Connection lost");
      expect(
        allFrames.filter((s) => JSON.parse(s).type.startsWith("repo.pack.")),
      ).toEqual([]);
    } finally {
      client.close();
      await server.stop(true);
    }
  });

  test("an undeploy of an older generation than the one held leaves it alone", async () => {
    const address = "run_lifecycle@integration.interchange";
    let held: HostedIncarnation | undefined;
    const calls: string[] = [];
    const hub = startWelcomingHub();
    const client = createHubLink({
      hubURL: `ws://localhost:${hub.server.port}/ws`,
      sidecarId: "sc-lifecycle",
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      deployRouter: {
        async deploy(frame) {
          calls.push(`deploy ${String(frame.generation)}`);
          held = {
            address: frame.agentAddress,
            generation: frame.generation,
            state: "live",
          };
          return { publicKey: "aa".repeat(32) };
        },
        async undeploy(frame) {
          calls.push(`undeploy ${String(frame.generation)}`);
          held = undefined;
        },
      },
      getIncarnations: () => (held === undefined ? [] : [held]),
    });
    const answerTo = async (requestId: string) => {
      const answers = () =>
        hub.frames
          .map((s): { requestId?: string } => JSON.parse(s))
          .filter((frame) => frame.requestId === requestId);
      await waitUntil(() => answers().length > 0);
      return answers()[0];
    };
    const deploy = (requestId: string, generation: number) => {
      hub.send({
        type: "agent.deploy",
        requestId,
        agentAddress: address,
        generation,
        agentId: TEST_CONFIG.agentId,
        config: TEST_CONFIG,
        hubPublicKey: "a".repeat(64),
        provisionStep: true,
      });
    };
    const undeploy = (requestId: string, generation: number) => {
      hub.send({
        type: "agent.undeploy",
        requestId,
        agentAddress: address,
        generation,
        reason: "test",
      });
    };

    client.connect();
    try {
      await waitUntil(() =>
        hub.frames.some((s) => JSON.parse(s).type === "hello"),
      );
      deploy("deploy-2", 2);
      expect(await answerTo("deploy-2")).toMatchObject({
        type: "agent.deploy.ack",
        generation: 2,
      });

      // Nothing of generation 1 is held here, so its undeploy does not reach
      // the router.
      undeploy("undeploy-1", 1);
      expect(await answerTo("undeploy-1")).toMatchObject({
        type: "agent.undeploy.ack",
        generation: 1,
      });
      expect(calls).toEqual(["deploy 2"]);

      undeploy("undeploy-2", 2);
      expect(await answerTo("undeploy-2")).toMatchObject({
        type: "agent.undeploy.ack",
        generation: 2,
      });
      expect(calls).toEqual(["deploy 2", "undeploy 2"]);
    } finally {
      client.close();
      await hub.server.stop(true);
    }
  });

  test("an undeploy whose connection closed before its turn still runs, and only its answers are dropped", async () => {
    const address = "run_orphan@integration.interchange";
    const calls: string[] = [];
    const deploying = Promise.withResolvers<boolean>();
    const finishDeploy = Promise.withResolvers<boolean>();
    const hub = startWelcomingHub();
    const client = createHubLink({
      hubURL: `ws://localhost:${hub.server.port}/ws`,
      sidecarId: "sc-orphan",
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      pingIntervalMs: 20,
      reconnectDelayMs: 10,
      deployRouter: {
        async deploy() {
          calls.push("deploy");
          deploying.resolve(true);
          await finishDeploy.promise;
          return { publicKey: "aa".repeat(32) };
        },
        async undeploy() {
          calls.push("undeploy");
        },
      },
    });
    const hellos = () =>
      hub.frames.filter((s) => JSON.parse(s).type === "hello");

    client.connect();
    try {
      await waitUntil(() => hellos().length === 1);
      hub.send({
        type: "agent.deploy",
        requestId: "deploy-1",
        agentAddress: address,
        generation: 1,
        agentId: TEST_CONFIG.agentId,
        config: TEST_CONFIG,
        hubPublicKey: "a".repeat(64),
        provisionStep: true,
      });
      await deploying.promise;
      // The undeploy waits on the address's lane behind the deploy, and the
      // connection that carried it drops before its turn comes.
      hub.send({
        type: "agent.undeploy",
        requestId: "undeploy-1",
        agentAddress: address,
        generation: 1,
        reason: "released",
      });
      hub.silence();
      await waitUntil(() => hellos().length === 2);

      finishDeploy.resolve(true);
      // A later undeploy of the address runs behind the orphaned one on its
      // lane, so its answer comes after any the orphaned one could send.
      hub.send({
        type: "agent.undeploy",
        requestId: "undeploy-2",
        agentAddress: address,
        generation: 1,
        reason: "released",
      });
      const answered = () =>
        hub.frames.flatMap((raw) => {
          const frame: { requestId?: string } = JSON.parse(raw);
          return frame.requestId === undefined ? [] : [frame.requestId];
        });
      await waitUntil(() => answered().includes("undeploy-2"));
      expect(calls).toEqual(["deploy", "undeploy", "undeploy"]);
      expect(answered()).toEqual(["undeploy-2"]);
    } finally {
      client.close();
      await hub.server.stop(true);
    }
  });

  test("an undeployed incarnation's queued reports never reach the Hub", async () => {
    const frames: string[] = [];
    const sockets: { send(data: string): void }[] = [];
    const app = new Hono();
    app.get(
      "/ws",
      upgradeWebSocket((_c) => ({
        onOpen(_evt, ws) {
          sockets.push(ws);
        },
        onMessage(evt, _ws) {
          if (typeof evt.data === "string") {
            frames.push(evt.data);
          }
        },
      })),
    );
    const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });
    const removed = "run_removed@integration.interchange";
    const neighbour = "run_neighbour@integration.interchange";
    const held = new Map<string, HostedIncarnation>([
      [removed, { address: removed, generation: 1, state: "live" }],
      [neighbour, { address: neighbour, generation: 1, state: "live" }],
    ]);
    const welcomed = Promise.withResolvers<undefined>();
    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-forget",
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      getIncarnations: () => [...held.values()],
      onWorkflowAddressesRoutable: () => {
        welcomed.resolve(undefined);
      },
      deployRouter: {
        async deploy() {
          throw new Error("unused");
        },
        async undeploy(frame) {
          held.delete(frame.agentAddress);
        },
      },
    });
    const register = (agentAddress: string, correlationId: string) => {
      client.sendSignalCorrelationRegister({
        correlationId,
        runId: "run-1",
        anchorRunId: "anchor-1",
        agentAddress,
        generation: 1,
        kind: "approval",
        approvalSnapshot: {
          name: "tool",
          description: "a tool",
          inputSchema: {},
          arguments: {},
        },
      });
    };
    const sent = () =>
      frames.map(
        (raw): { type: string; requestId?: string; correlationId?: string } =>
          JSON.parse(raw),
      );

    client.connect();
    try {
      await waitUntil(() => frames.length > 0);
      register(removed, "corr-removed");
      sockets[0]!.send(
        JSON.stringify({
          type: "agent.undeploy",
          requestId: "undeploy-removed",
          agentAddress: removed,
          generation: 1,
          reason: "released",
        }),
      );
      await waitUntil(() =>
        sent().some((frame) => frame.requestId === "undeploy-removed"),
      );

      sockets[0]!.send(JSON.stringify({ type: "welcome", routed: [] }));
      await welcomed.promise;
      // The welcome sends the queue before the neighbour registers, so a
      // queued register of the removed incarnation would reach the Hub first.
      register(neighbour, "corr-neighbour");
      await waitUntil(() =>
        sent().some((frame) => frame.correlationId === "corr-neighbour"),
      );
      expect(
        sent().filter((frame) => frame.correlationId === "corr-removed"),
      ).toEqual([]);
    } finally {
      client.close();
      await server.stop(true);
    }
  });

  test("an undeployed incarnation's register retries end with its undeploy", async () => {
    const frames: string[] = [];
    const sockets: { send(data: string): void }[] = [];
    const app = new Hono();
    app.get(
      "/ws",
      upgradeWebSocket((_c) => ({
        onOpen(_evt, ws) {
          sockets.push(ws);
        },
        onMessage(evt, _ws) {
          if (typeof evt.data === "string") {
            frames.push(evt.data);
          }
        },
      })),
    );
    const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });
    const removed = "run_removed@integration.interchange";
    const neighbour = "run_neighbour@integration.interchange";
    const held = new Map<string, HostedIncarnation>([
      [removed, { address: removed, generation: 1, state: "live" }],
      [neighbour, { address: neighbour, generation: 1, state: "live" }],
    ]);
    const welcomed = Promise.withResolvers<undefined>();
    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-forget-retries",
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      // Retries that never run out keep the removed register retrying until
      // its undeploy lands, however long that takes.
      registerAckTimeoutMs: 20,
      registerAckMaxAttempts: Number.MAX_SAFE_INTEGER,
      getIncarnations: () => [...held.values()],
      onWorkflowAddressesRoutable: () => {
        welcomed.resolve(undefined);
      },
      deployRouter: {
        async deploy() {
          throw new Error("unused");
        },
        async undeploy(frame) {
          held.delete(frame.agentAddress);
        },
      },
    });
    const register = (agentAddress: string, correlationId: string) => {
      client.sendSignalCorrelationRegister({
        correlationId,
        runId: "run-1",
        anchorRunId: "anchor-1",
        agentAddress,
        generation: 1,
        kind: "approval",
        approvalSnapshot: {
          name: "tool",
          description: "a tool",
          inputSchema: {},
          arguments: {},
        },
      });
    };
    const sent = () =>
      frames.map(
        (raw): { type: string; requestId?: string; correlationId?: string } =>
          JSON.parse(raw),
      );
    const registers = (correlationId: string) =>
      sent().filter((frame) => frame.correlationId === correlationId).length;

    client.connect();
    try {
      await waitUntil(() => frames.length > 0);
      sockets[0]!.send(JSON.stringify({ type: "welcome", routed: [] }));
      await welcomed.promise;
      register(removed, "corr-removed");
      sockets[0]!.send(
        JSON.stringify({
          type: "agent.undeploy",
          requestId: "undeploy-removed",
          agentAddress: removed,
          generation: 1,
          reason: "released",
        }),
      );
      await waitUntil(() =>
        sent().some((frame) => frame.requestId === "undeploy-removed"),
      );
      const beforeUndeploy = registers("corr-removed");
      expect(beforeUndeploy).toBeGreaterThan(0);

      // The neighbour's first retry is due a full retry interval after the
      // undeploy was answered, so a retry of the removed register that the
      // undeploy left running would reach the Hub before it.
      register(neighbour, "corr-neighbour");
      await waitUntil(() => registers("corr-neighbour") >= 2);
      expect(registers("corr-removed")).toBe(beforeUndeploy);
    } finally {
      client.close();
      await server.stop(true);
    }
  });

  test("a Hub frame for another generation than the one held is refused", async () => {
    const address = "run_refused@integration.interchange";
    const hub = startWelcomingHub();
    const drained: number[] = [];
    const client = createHubLink({
      hubURL: `ws://localhost:${hub.server.port}/ws`,
      sidecarId: "sc-refused",
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      getIncarnations: () => [{ address, generation: 2, state: "live" }],
      drainInboundRouter: {
        async tryRoute(frame) {
          drained.push(frame.generation);
          return true;
        },
      },
      sourcesInboundRouter: {
        async tryRoute() {
          return true;
        },
      },
    });

    client.connect();
    try {
      await waitUntil(() =>
        hub.frames.some((s) => JSON.parse(s).type === "hello"),
      );
      // Frames for one address are handled in arrival order, so once the
      // current generation's drain is routed, the two before it were handled.
      hub.send({
        type: "drain.deliver",
        agentAddress: address,
        generation: 1,
        deadlineMs: 10,
      });
      hub.send({
        type: "sources.update",
        requestId: "sources-stale",
        agentAddress: address,
        generation: 1,
        sources: TEST_CONFIG.sources,
        defaultSource: TEST_CONFIG.defaultSource,
      });
      hub.send({
        type: "drain.deliver",
        agentAddress: address,
        generation: 2,
        deadlineMs: 10,
      });
      await waitUntil(() => drained.length > 0);
      expect(drained).toEqual([2]);
      const answer = () =>
        hub.frames
          .map((s): { requestId?: string } => JSON.parse(s))
          .find((frame) => frame.requestId === "sources-stale");
      await waitUntil(() => answer() !== undefined);
      expect(answer()).toMatchObject({
        type: "session.error",
        error: `${address} generation 2 is hosted here, not generation 1`,
      });
    } finally {
      client.close();
      await hub.server.stop(true);
    }
  });

  test("a Hub frame for an address not held here or not live is refused", async () => {
    const stopped = "run_stopped@integration.interchange";
    const deploying = "run_deploying@integration.interchange";
    const unknown = "run_unknown@integration.interchange";
    const hub = startWelcomingHub();
    const drained: string[] = [];
    const client = createHubLink({
      hubURL: `ws://localhost:${hub.server.port}/ws`,
      sidecarId: "sc-refused-states",
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      getIncarnations: () => [
        { address: stopped, generation: 1, state: "stopped" },
        { address: deploying, generation: 1, state: "deploying" },
      ],
      drainInboundRouter: {
        async tryRoute(frame) {
          drained.push(frame.agentAddress);
          return true;
        },
      },
      sourcesInboundRouter: {
        async tryRoute() {
          return true;
        },
      },
    });

    client.connect();
    try {
      await waitUntil(() =>
        hub.frames.some((s) => JSON.parse(s).type === "hello"),
      );
      // Frames for one address are handled in arrival order, so the answer to
      // each address's sources update proves its drain was handled first.
      for (const address of [stopped, deploying, unknown]) {
        hub.send({
          type: "drain.deliver",
          agentAddress: address,
          generation: 1,
          deadlineMs: 10,
        });
        hub.send({
          type: "sources.update",
          requestId: `sources-${address}`,
          agentAddress: address,
          generation: 1,
          sources: TEST_CONFIG.sources,
          defaultSource: TEST_CONFIG.defaultSource,
        });
      }
      const answer = (address: string) =>
        hub.frames
          .map((s): { requestId?: string } => JSON.parse(s))
          .find((frame) => frame.requestId === `sources-${address}`);
      await waitUntil(() =>
        [stopped, deploying, unknown].every(
          (address) => answer(address) !== undefined,
        ),
      );
      expect(drained).toEqual([]);
      expect(answer(stopped)).toMatchObject({
        type: "session.error",
        error: `${stopped} generation 1 is stopped`,
      });
      expect(answer(deploying)).toMatchObject({
        type: "session.error",
        error: `${deploying} generation 1 is deploying`,
      });
      expect(answer(unknown)).toMatchObject({
        type: "session.error",
        error: `${unknown} is not hosted here`,
      });
    } finally {
      client.close();
      await hub.server.stop(true);
    }
  });

  test("refuses a Hub pack for an address it holds", async () => {
    const address = "run_held@integration.interchange";
    const repoId = {
      kind: "workflow-run",
      id: "run_held-integration-interchange",
    };
    const applied: string[] = [];
    const hub = startWelcomingHub();
    const client = createHubLink({
      hubURL: `ws://localhost:${hub.server.port}/ws`,
      sidecarId: "sc-held-pack",
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...withTestDeployBindings(),
      getIncarnations: () => [{ address, generation: 2, state: "live" }],
      applyWorkflowRunPack: async (args) => {
        applied.push(args.commitSha);
      },
    });
    const sendPackFor = (transferId: string, generation: number) => {
      hub.send({
        type: "repo.pack.push",
        agentAddress: address,
        generation,
        repoId,
        transferId,
        seq: 0,
        data: "BwgJ",
      });
      hub.send({
        type: "repo.pack.done",
        agentAddress: address,
        generation,
        repoId,
        transferId,
        ref: "refs/heads/events",
        commitSha: "c".repeat(40),
      });
    };
    const answerTo = async (transferId: string) => {
      const answers = () =>
        hub.frames
          .map((raw): { transferId?: string } => JSON.parse(raw))
          .filter((frame) => frame.transferId === transferId);
      await waitUntil(() => answers().length > 0);
      return answers()[0];
    };

    client.connect();
    try {
      await waitUntil(() =>
        hub.frames.some((s) => JSON.parse(s).type === "hello"),
      );
      for (const generation of [2, 1]) {
        sendPackFor(`seed-${String(generation)}`, generation);
        expect(await answerTo(`seed-${String(generation)}`)).toMatchObject({
          type: "repo.pack.reject",
          reason: "conflict",
        });
      }
      expect(applied).toEqual([]);
    } finally {
      client.close();
      await hub.server.stop(true);
    }
  });

  test("an unacked register is retried on the watchdog up to the cap", async () => {
    const allFrames: string[] = [];
    const app = new Hono();
    app.get(
      "/ws",
      upgradeWebSocket((_c) => ({
        // Deliberately never ack the register, so the client's watchdog fires.
        onMessage(evt, ws) {
          if (typeof evt.data !== "string") return;
          allFrames.push(evt.data);
          const frame: { type: string } = JSON.parse(evt.data);
          if (frame.type === "hello") {
            ws.send(JSON.stringify({ type: "welcome", routed: [] }));
          }
        },
      })),
    );
    const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });

    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();

    const welcomed = Promise.withResolvers<undefined>();
    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-register-retry",
      token: "test-token",
      transport,
      sessions,
      ...withTestDeployBindings(),
      getIncarnations: () => [],
      registerAckTimeoutMs: 40,
      registerAckMaxAttempts: 3,
      onWorkflowAddressesRoutable: () => {
        welcomed.resolve(undefined);
      },
    });

    const registersFor = (correlationId: string): string[] =>
      allFrames.filter((s) => {
        const frame: { type: string; correlationId?: string } = JSON.parse(s);
        return (
          frame.type === "signal.correlation.register" &&
          frame.correlationId === correlationId
        );
      });

    const approvalSnapshot = {
      name: "charge_card",
      description: "Charge the customer's card",
      inputSchema: { type: "object" },
      arguments: { amount: 100 },
    };

    client.connect();
    try {
      // A register sent before the welcome is only queued, and its watchdog
      // gives it up if it fires before the welcome lands.
      await welcomed.promise;
      client.sendSignalCorrelationRegister({
        correlationId: "corr-retry",
        runId: "run-1",
        anchorRunId: "dep-1",
        agentAddress: "run_reg_retry@integration.interchange",
        generation: 1,
        kind: "approval",
        approvalSnapshot,
      });
      // The subject's first retry is on the socket, so its watchdog has
      // fired once.
      await waitUntil(() => registersFor("corr-retry").length >= 2);

      // "corr-late" is the test's clock. Its watchdog is armed after that
      // fire and carries the same interval, so each of its timers falls due
      // after the subject's matching one, and a timer heap fires in due
      // order. Its third send therefore cannot reach the one shared socket
      // before a fourth send of the subject would have.
      client.sendSignalCorrelationRegister({
        correlationId: "corr-late",
        runId: "run-2",
        anchorRunId: "dep-1",
        agentAddress: "run_reg_retry@integration.interchange",
        generation: 1,
        kind: "approval",
        approvalSnapshot,
      });
      await waitUntil(() => registersFor("corr-late").length >= 3);

      // Three sends total (initial plus two retries), then the acker gives
      // up: the fire that would have carried a fourth is the one that found
      // the budget spent.
      expect(registersFor("corr-retry")).toHaveLength(3);
    } finally {
      client.close();
      await server.stop(true);
    }
  });
});

describe("answerMalformedRequestFrame", () => {
  type Answer =
    | SessionErrorFrame
    | AgentDeployErrorFrame
    | AgentUndeployErrorFrame
    | PackRejectFrame;

  test("answers a malformed sources.update with session.error carrying the requestId", () => {
    const sent: Answer[] = [];
    // A structurally-invalid sources list (empty source object) that failed
    // the top-level parse but kept its type + requestId.
    const answered = answerMalformedRequestFrame(
      {
        type: "sources.update",
        requestId: "req-1",
        agentAddress: "run_x@example.com",
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

  test("answers a malformed agent.deploy with agent.deploy.error naming its request and incarnation", () => {
    const sent: Answer[] = [];
    const answered = answerMalformedRequestFrame(
      {
        type: "agent.deploy",
        requestId: "req-deploy",
        agentAddress: "run_deploy@example.com",
        generation: 3,
        agentId: "x",
      },
      "config must be an object",
      (frame) => sent.push(frame),
    );
    expect(answered).toBe(true);
    expect(sent).toHaveLength(1);
    expect(sent[0]).toMatchObject({
      type: "agent.deploy.error",
      requestId: "req-deploy",
      agentAddress: "run_deploy@example.com",
      generation: 3,
      error: expect.stringMatching(/malformed agent.deploy frame/),
    });
  });

  test("does not answer a malformed agent.deploy whose generation is unrecoverable", () => {
    const sent: Answer[] = [];
    const answered = answerMalformedRequestFrame(
      {
        type: "agent.deploy",
        requestId: "req-deploy",
        agentAddress: "run_deploy@example.com",
        generation: "three",
      },
      "generation must be a number",
      (frame) => sent.push(frame),
    );
    expect(answered).toBe(false);
    expect(sent).toHaveLength(0);
  });

  test("drops an unhandled request-shaped frame instead of answering it", () => {
    // A request-shaped frame whose type is in none of the answerable sets
    // (session.error / lifecycle / pack reject) has no requester to
    // answer, so a malformed one is dropped rather than answered.
    for (const frameType of ["some.unhandled.request", "another.unknown"]) {
      const sent: Answer[] = [];
      const answered = answerMalformedRequestFrame(
        {
          type: frameType,
          requestId: "req-2",
          agentAddress: "run_x@example.com",
        },
        "payload must be valid",
        (frame) => sent.push(frame),
      );
      expect(answered).toBe(false);
      expect(sent).toHaveLength(0);
    }
  });

  test("answers a malformed agent.undeploy with agent.undeploy.error", () => {
    const sent: Answer[] = [];
    const answered = answerMalformedRequestFrame(
      {
        type: "agent.undeploy",
        requestId: "req-undeploy",
        agentAddress: "run_undeploy@example.com",
        generation: 2,
      },
      "reason must be a string",
      (frame) => sent.push(frame),
    );
    expect(answered).toBe(true);
    expect(sent[0]).toMatchObject({
      type: "agent.undeploy.error",
      requestId: "req-undeploy",
      agentAddress: "run_undeploy@example.com",
      generation: 2,
      error: expect.stringMatching(/malformed agent.undeploy frame/),
    });
  });

  test.each(["agent.deploy", "agent.undeploy"])(
    "cuts a malformed %s's long validation summary to a reply the Hub accepts",
    (frameType) => {
      const sent: Answer[] = [];
      const answered = answerMalformedRequestFrame(
        {
          type: frameType,
          requestId: "req-long",
          agentAddress: "run_long@example.com",
          generation: 1,
        },
        "x".repeat(MAX_DEPLOYMENT_ERROR_LENGTH * 2),
        (frame) => sent.push(frame),
      );
      expect(answered).toBe(true);
      expect(sent).toHaveLength(1);
      expect(SidecarFrame(sent[0]) instanceof type.errors).toBe(false);
    },
  );

  test("answers a malformed repo.pack frame with repo.pack.reject on its transferId", () => {
    for (const frameType of ["repo.pack.push", "repo.pack.done"]) {
      const sent: Answer[] = [];
      const answered = answerMalformedRequestFrame(
        {
          type: frameType,
          agentAddress: "run_pack@example.com",
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
    const sent: Answer[] = [];
    const answered = answerMalformedRequestFrame(
      {
        type: "repo.pack.push",
        agentAddress: "run_pack@example.com",
        repoId: { kind: "workflow-run", id: "dep-1" },
      },
      "bad",
      (frame) => sent.push(frame),
    );
    expect(answered).toBe(false);
    expect(sent).toHaveLength(0);
  });

  test("does not answer a repo.pack frame whose repoId is itself malformed", () => {
    const sent: Answer[] = [];
    const answered = answerMalformedRequestFrame(
      {
        type: "repo.pack.push",
        agentAddress: "run_pack@example.com",
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
    const sent: Answer[] = [];
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
    const sent: Answer[] = [];
    const answered = answerMalformedRequestFrame(
      { type: "sources.update", sources: [{}] },
      "bad",
      (frame) => sent.push(frame),
    );
    expect(answered).toBe(false);
    expect(sent).toHaveLength(0);
  });

  test("does not answer a fire-and-forget frame even with a requestId present", () => {
    const sent: Answer[] = [];
    const answered = answerMalformedRequestFrame(
      { type: "signal.deliver", agentAddress: "x", requestId: "r" },
      "bad",
      (frame) => sent.push(frame),
    );
    expect(answered).toBe(false);
    expect(sent).toHaveLength(0);
  });

  test("does not answer a frame with no recognizable type", () => {
    const sent: Answer[] = [];
    const answered = answerMalformedRequestFrame(
      { garbage: true },
      "bad",
      (frame) => sent.push(frame),
    );
    expect(answered).toBe(false);
    expect(sent).toHaveLength(0);
  });
});

describe("classifyAssetPackRejectReason", () => {
  test("classifies a symlink rejection as path_violation, not corrupt", () => {
    expect(
      classifyAssetPackRejectReason(
        "asset_materialization_failed: writeTreeToDisk: symlink at link is not supported",
      ),
    ).toBe("path_violation");
  });

  test("classifies a submodule rejection as path_violation", () => {
    expect(
      classifyAssetPackRejectReason(
        "asset_materialization_failed: writeTreeToDisk: submodule reference at sub is not supported",
      ),
    ).toBe("path_violation");
  });

  test("classifies an escaping mountPath as path_violation", () => {
    expect(
      classifyAssetPackRejectReason(
        'asset_materialization_failed: mountPath "x/../y" normalizes to a workspace-root or escaping path',
      ),
    ).toBe("path_violation");
  });

  test("classifies a missing-commit (bad bytes) rejection as corrupt", () => {
    expect(
      classifyAssetPackRejectReason(
        "asset_materialization_failed: indexPackIntoGitDir: expected commit abc not found in the pack",
      ),
    ).toBe("corrupt");
  });

  test("keeps sha_mismatch and signature reasons", () => {
    expect(classifyAssetPackRejectReason("sha_mismatch: ...")).toBe(
      "sha_mismatch",
    );
    expect(classifyAssetPackRejectReason("signature_unsigned: ...")).toBe(
      "signature_invalid",
    );
  });

  test("classifies a seed that would move existing history as conflict", () => {
    expect(
      classifyAssetPackRejectReason(
        "workflow_run_restore_conflict: refs/heads/main of run@example.com is already at abc here",
      ),
    ).toBe("conflict");
  });
});

describe("cleartextTransportWarning", () => {
  test("warns on cleartext ws:// to a non-loopback host", () => {
    expect(
      cleartextTransportWarning("ws://hub.example.com/api/sidecars/ws"),
    ).toContain("cleartext ws://");
    expect(
      cleartextTransportWarning("ws://10.0.0.5:3000/api/sidecars/ws"),
    ).toContain("cleartext ws://");
  });

  test("normalizes the host, so an uppercase remote still warns", () => {
    expect(
      cleartextTransportWarning("ws://HUB.EXAMPLE.COM/api/sidecars/ws"),
    ).toContain("cleartext ws://");
  });

  test("stays silent for cleartext ws:// to a loopback host", () => {
    for (const url of [
      "ws://localhost/api/sidecars/ws",
      "ws://localhost:3000/api/sidecars/ws",
      "ws://127.0.0.1:3000/api/sidecars/ws",
      "ws://[::1]:3000/api/sidecars/ws",
    ]) {
      expect(cleartextTransportWarning(url)).toBeNull();
    }
  });

  test("stays silent for wss:// regardless of host", () => {
    expect(
      cleartextTransportWarning("wss://hub.example.com/api/sidecars/ws"),
    ).toBeNull();
  });

  test("throws on a malformed hub URL rather than reporting no warning", () => {
    expect(() => cleartextTransportWarning("not a url")).toThrow();
  });
});
