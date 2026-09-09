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

import {
  describe,
  test,
  expect,
  afterAll,
  beforeAll,
  beforeEach,
} from "bun:test";
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
import {
  generateKeyPair,
  verifySSHSignature,
  createEd25519Crypto,
} from "@intx/crypto";
import { hexDecode, hexEncode } from "@intx/types";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  generateMessageId,
  type MessageHeaders,
} from "@intx/mime";
import { configureSync, getConfig, resetSync } from "@intx/log";

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
      expect(
        env.router.routeMail(
          deploymentAddress,
          encoded,
          "user@integration.interchange",
          null,
        ),
      ).toBe(true);

      // Second mail.inbound: the router accepts. With the fix in
      // place this frame still flows through; without the fix the
      // chain has been wedged by the prior rejection and the router
      // is never consulted.
      expect(
        env.router.routeMail(
          deploymentAddress,
          encoded,
          "user@integration.interchange",
          null,
        ),
      ).toBe(true);

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

type CapturedLog = {
  category: readonly string[];
  level: string;
  properties: Record<string, unknown>;
};

const capturedLogs: CapturedLog[] = [];
const savedLogConfig = getConfig();

beforeAll(() => {
  configureSync({
    reset: true,
    sinks: {
      capture: (record) => {
        capturedLogs.push({
          category: record.category,
          level: record.level,
          properties: record.properties,
        });
      },
    },
    loggers: [
      { category: [], lowestLevel: "debug", sinks: ["capture"] },
      {
        category: ["logtape", "meta"],
        lowestLevel: "warning",
        sinks: ["capture"],
      },
    ],
  });
});

afterAll(() => {
  if (savedLogConfig) {
    configureSync({ reset: true, ...savedLogConfig });
  } else {
    resetSync();
  }
});

function shadowVerdicts(): CapturedLog[] {
  return capturedLogs.filter(
    (r) =>
      r.category.length >= 4 &&
      r.category[2] === "ws" &&
      r.category[3] === "inbound-signature-shadow",
  );
}

function signedHeaders(from: string): MessageHeaders {
  return {
    from,
    to: ["agent-1@test.interchange"],
    cc: undefined,
    date: new Date("2026-04-17T12:00:00Z"),
    messageId: generateMessageId(from),
    subject: undefined,
    inReplyTo: undefined,
    references: undefined,
    mimeVersion: "1.0",
    interchangeType: "conversation.message",
    interchangeCorrelationId: undefined,
    interchangeTenantId: undefined,
    interchangeAgentId: undefined,
    interchangeSessionId: undefined,
    interchangeOfferingId: undefined,
    interchangeSchemaVersion: undefined,
    traceparent: undefined,
    tracestate: undefined,
  };
}

async function makeSignedMail(
  crypto: Awaited<ReturnType<typeof createEd25519Crypto>>,
  from: string,
): Promise<Uint8Array> {
  const content = assembleSignedContent({
    kind: "conversation",
    text: "signed body",
  });
  const sig = await createDetachedSignatureFromProvider(content, crypto);
  return assembleMessage(signedHeaders(from), content, sig);
}

// Drives a `mail.inbound` frame across the real hub-link WS surface and proves
// the INTR-512 shadow verify at the ingress seam LOGS a verdict and ADMITS the
// mail in every case (the router still receives it), never dropping.
describe("hub-link mail.inbound signature shadow", () => {
  beforeEach(() => {
    capturedLogs.length = 0;
  });

  async function withConnectedLink(
    label: string,
    body: (ctx: {
      deploymentAddress: string;
      routed: Uint8Array[];
    }) => Promise<void>,
  ): Promise<void> {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deploymentAddress = `run_${label}@integration.interchange`;
    sessions.addresses.push(deploymentAddress);

    const routed: Uint8Array[] = [];
    const mailInboundRouter = {
      tryRoute(_address: string, message: Uint8Array): Promise<void> | null {
        routed.push(message);
        return Promise.resolve();
      },
    };

    const bindings = withTestDeployBindings();
    await provisionDeploymentKey(bindings.keyStore, deploymentAddress);
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: `sc-shadow-${label}`,
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
      await body({ deploymentAddress, routed });
    } finally {
      client.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes(`sc-shadow-${label}`),
      );
    }
  }

  test("a validly-signed frame logs a valid/match verdict and is admitted", async () => {
    const sender = "external@remote.interchange";
    const crypto = createEd25519Crypto(await generateKeyPair());
    const raw = await makeSignedMail(crypto, sender);

    await withConnectedLink(
      "shadowvalid",
      async ({ deploymentAddress, routed }) => {
        expect(
          env.router.routeMail(
            deploymentAddress,
            base64Encode(raw),
            sender,
            hexEncode(crypto.getPublicKey()),
          ),
        ).toBe(true);

        await waitFor(() => shadowVerdicts().length > 0);
        const verdict = shadowVerdicts()[0];
        expect(verdict?.properties["signature"]).toBe("valid");
        expect(verdict?.properties["fromMatch"]).toBe("match");
        // Admitted: the frame still reached the mail router.
        await waitFor(() => routed.length > 0);
        expect(routed).toHaveLength(1);
      },
    );
  });

  test("a tampered signature logs invalid and is still admitted", async () => {
    const sender = "external@remote.interchange";
    const signer = createEd25519Crypto(await generateKeyPair());
    const other = createEd25519Crypto(await generateKeyPair());
    const raw = await makeSignedMail(signer, sender);

    await withConnectedLink(
      "shadowbad",
      async ({ deploymentAddress, routed }) => {
        // The hub stamps a key that does not match the signer, so the recipient
        // verdict is invalid -- but shadow admits it anyway.
        expect(
          env.router.routeMail(
            deploymentAddress,
            base64Encode(raw),
            sender,
            hexEncode(other.getPublicKey()),
          ),
        ).toBe(true);

        await waitFor(() => shadowVerdicts().length > 0);
        expect(shadowVerdicts()[0]?.properties["signature"]).toBe("invalid");
        await waitFor(() => routed.length > 0);
        expect(routed).toHaveLength(1);
      },
    );
  });

  test("a null sender key logs unknown and is still admitted", async () => {
    const sender = "external@remote.interchange";
    const crypto = createEd25519Crypto(await generateKeyPair());
    const raw = await makeSignedMail(crypto, sender);

    await withConnectedLink(
      "shadownull",
      async ({ deploymentAddress, routed }) => {
        expect(
          env.router.routeMail(
            deploymentAddress,
            base64Encode(raw),
            sender,
            null,
          ),
        ).toBe(true);

        await waitFor(() => shadowVerdicts().length > 0);
        expect(shadowVerdicts()[0]?.properties["signature"]).toBe("unknown");
        await waitFor(() => routed.length > 0);
        expect(routed).toHaveLength(1);
      },
    );
  });

  test("a valid signature under a forged From is flagged and still admitted", async () => {
    const stamp = "external@remote.interchange";
    const forgedFrom = "victim@remote.interchange";
    const crypto = createEd25519Crypto(await generateKeyPair());
    const raw = await makeSignedMail(crypto, forgedFrom);

    await withConnectedLink(
      "shadowforge",
      async ({ deploymentAddress, routed }) => {
        expect(
          env.router.routeMail(
            deploymentAddress,
            base64Encode(raw),
            stamp,
            hexEncode(crypto.getPublicKey()),
          ),
        ).toBe(true);

        await waitFor(() => shadowVerdicts().length > 0);
        const verdict = shadowVerdicts()[0];
        expect(verdict?.properties["signature"]).toBe("valid");
        expect(verdict?.properties["fromMatch"]).toBe("mismatch");
        await waitFor(() => routed.length > 0);
        expect(routed).toHaveLength(1);
      },
    );
  });
});
