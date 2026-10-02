// Pins the contract for the `mail.inbound` arm in `handleMessage`: a
// throwing `mailInboundRouter.tryRoute` must not stop the frames after it.
// The arm wraps the router call in try/catch (mirroring `signal.deliver`
// and `drain.deliver`), so later frames for the address -- and the
// heartbeat `pong` -- keep dispatching. The test drives the arm end to end
// through the real hub-link WS surface.

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
  type SidecarAuthIdentity,
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
import { hexDecode } from "@intx/types";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  generateMessageId,
  type MessageHeaders,
} from "@intx/mime";
import { configureSync, getConfig } from "@intx/log";
import { waitUntil } from "@intx/types/testing";

import { createHubLink, type DeployRouter } from "./hub-link";
import {
  resolveInboundMailPolicy,
  type ResolvedInboundMailPolicy,
} from "./inbound-signature";
import {
  createInboundMailPolicyRegistry,
  createInboundMailPolicyLookup,
} from "./inbound-mail-policy-registry";
import { createPublicKeyCrypto } from "../sender-crypto";
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
  resolveSenderCrypto: () => undefined;
  cacheSenderKey: () => Promise<void>;
  evictSenderKey: () => Promise<void>;
} {
  const keyStore = createTestKeyStore();
  return {
    keyStore,
    deployRouter: createTestDeployRouter(keyStore),
    resolveSenderCrypto: () => undefined,
    cacheSenderKey: async () => undefined,
    evictSenderKey: async () => undefined,
  };
}

// A resolved policy that admits every outcome. Used by the queue-liveness test,
// where the point is that a frame flows, not which outcome the policy relaxes.
const ADMIT_ALL_INBOUND_MAIL_POLICY: ResolvedInboundMailPolicy = {
  clean: "admit",
  error: "admit",
  untrustedFrom: "admit",
  invalid: "admit",
  missing: "admit",
  unknown: "admit",
};

type DeliveredMessage = { agentAddress: string; message: InboundMessage };

function createMockSessionManager(): SessionManager & {
  provisioned: HarnessConfig[];
  started: string[];
  destroyed: string[];
  aborted: { address: string; reason: string }[];
  delivered: DeliveredMessage[];
  provisionedAddresses: string[];
  shouldThrow: string | null;
} {
  const mock = {
    provisioned: [] as HarnessConfig[],
    started: [] as string[],
    destroyed: [] as string[],
    aborted: [] as { address: string; reason: string }[],
    delivered: [] as DeliveredMessage[],
    provisionedAddresses: [] as string[],
    shouldThrow: null as string | null,

    initRepo: (_address: string) => Promise.resolve(),
    applyDeployPack: () => Promise.resolve(),
    applyAssetPack: () => Promise.resolve(),
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
  Extract<SidecarAuthIdentity, { kind: "allocated" }>
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
function startTestServer(): TestEnv {
  const router = createSidecarRouter({
    withExecutableWorkflowRun: async (_target, send) => send(),
    authenticateSidecar: async ({ sidecarId }) => ({ sidecarId }),
    resolveSidecarBindings: async (sidecarId) => [ensureIdentity(sidecarId)],
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
              incarnations?: { address: string }[];
            };
            if (frame.type === "hello" && frame.sidecarId !== undefined) {
              router.fenceAllocation(`allocation-${frame.sidecarId}`, 1);
              const identity = ensureIdentity(frame.sidecarId);
              const reported = frame.incarnations?.[0];
              if (frame.incarnations?.length === 1 && reported !== undefined) {
                Object.assign(identity, {
                  workflowRunAddress: reported.address,
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
 * pinned key. The deployment address then routes once the hub's
 * `welcome` routes it on (re)connect.
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
    const policyRegistry = createInboundMailPolicyRegistry();
    policyRegistry.register(deploymentAddress, ADMIT_ALL_INBOUND_MAIL_POLICY);
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: "sc-mail-wedge",
      token: "test-token",
      transport,
      sessions,
      ...bindings,
      lookupInboundMailPolicy: createInboundMailPolicyLookup(policyRegistry),
      mailInboundRouter,
      getIncarnations: () => [
        { address: deploymentAddress, generation: 1, state: "live" },
      ],
    });

    client.connect();
    try {
      await waitUntil(() =>
        env.router.getRoutableAddresses().includes(deploymentAddress),
      );

      const encoded = base64Encode(VALID_MESSAGE);

      // First mail.inbound: the router throws. The link's switch arm
      // catches the throw and logs it, and the frames after it still run.
      expect(
        await env.router.routeMail(
          deploymentAddress,
          encoded,
          "user@integration.interchange",
        ),
      ).toBe(true);

      // Second mail.inbound: the router accepts. With the fix in
      // place this frame still flows through; without the fix the
      // chain has been wedged by the prior rejection and the router
      // is never consulted.
      expect(
        await env.router.routeMail(
          deploymentAddress,
          encoded,
          "user@integration.interchange",
        ),
      ).toBe(true);

      await waitUntil(() => routedAfterThrow.length > 0);
      expect(routedAfterThrow).toHaveLength(1);
      expect(calls).toBe(2);
    } finally {
      client.close();
      await waitUntil(
        () => !env.router.getConnectedSidecars().includes("sc-mail-wedge"),
      );
    }
  });
});

describe("hub-link mail.inbound acknowledgement across an undeploy", () => {
  test("an acknowledgement names the incarnation the mail was delivered to", async () => {
    const leaving = "run_ack_leaving@integration.interchange";
    const staying = "run_ack_staying@integration.interchange";
    const fromHub: string[] = [];
    let hubSocket: { send(data: string): void } | undefined;
    const app = new Hono();
    app.get(
      "/ws",
      upgradeWebSocket((_c) => ({
        onOpen(_evt, ws) {
          hubSocket = ws;
        },
        onMessage(evt) {
          if (typeof evt.data !== "string") return;
          fromHub.push(evt.data);
          const frame: { type: string } = JSON.parse(evt.data);
          if (frame.type === "hello") {
            hubSocket?.send(JSON.stringify({ type: "welcome", routed: [] }));
          }
        },
      })),
    );
    const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });
    const framesOfType = (
      type: string,
    ): { agentAddress?: string; generation?: number; messageId?: string }[] =>
      fromHub
        .map(
          (
            raw,
          ): {
            type: string;
            agentAddress?: string;
            generation?: number;
            messageId?: string;
          } => JSON.parse(raw),
        )
        .filter((frame) => frame.type === type);
    const deliver = (agentAddress: string, messageId: string): void => {
      hubSocket?.send(
        JSON.stringify({
          type: "mail.inbound",
          agentAddress,
          generation: 1,
          rawMessage: base64Encode(VALID_MESSAGE),
          authenticatedSender: "user@integration.interchange",
          messageId,
        }),
      );
    };

    const leavingInbox = Promise.withResolvers<boolean>();
    const mailInboundRouter = {
      tryRoute(address: string): Promise<void> | null {
        if (address !== leaving) return Promise.resolve();
        return leavingInbox.promise.then(() => undefined);
      },
    };
    const bindings = withTestDeployBindings();
    await provisionDeploymentKey(bindings.keyStore, leaving);
    await provisionDeploymentKey(bindings.keyStore, staying);
    const policyRegistry = createInboundMailPolicyRegistry();
    policyRegistry.register(leaving, ADMIT_ALL_INBOUND_MAIL_POLICY);
    policyRegistry.register(staying, ADMIT_ALL_INBOUND_MAIL_POLICY);
    const held = new Set([leaving, staying]);
    const client = createHubLink({
      hubURL: `ws://localhost:${server.port}/ws`,
      sidecarId: "sc-mail-ack-undeploy",
      token: "test-token",
      transport: createInMemoryTransport(),
      sessions: createMockSessionManager(),
      ...bindings,
      deployRouter: {
        ...bindings.deployRouter,
        async undeploy(frame) {
          held.delete(frame.agentAddress);
        },
      },
      lookupInboundMailPolicy: createInboundMailPolicyLookup(policyRegistry),
      mailInboundRouter,
      getIncarnations: () =>
        [...held].map((address) => ({
          address,
          generation: 1,
          state: "live" as const,
        })),
    });

    client.connect();
    try {
      await waitUntil(() => framesOfType("hello").length > 0);
      deliver(leaving, "mail-to-leaving");
      hubSocket?.send(
        JSON.stringify({
          type: "agent.undeploy",
          requestId: "undeploy-leaving",
          agentAddress: leaving,
          generation: 1,
          reason: "Generation 2 superseded it",
        }),
      );
      await waitUntil(() => framesOfType("agent.undeploy.ack").length > 0);
      leavingInbox.resolve(true);

      // Mail for an incarnation no longer held is neither delivered nor
      // acknowledged, so its redelivery reaches the one the Hub routes.
      deliver(leaving, "mail-after-undeploy");
      deliver(staying, "mail-to-staying");
      await waitUntil(() =>
        framesOfType("mail.inbound.ack").some(
          (frame) => frame.agentAddress === staying,
        ),
      );
      // The late ack still names the incarnation that received the mail;
      // the Hub credits it to no later one.
      expect(
        framesOfType("mail.inbound.ack").map(
          ({ agentAddress, generation, messageId }) => ({
            agentAddress,
            generation,
            messageId,
          }),
        ),
      ).toEqual([
        { agentAddress: leaving, generation: 1, messageId: "mail-to-leaving" },
        { agentAddress: staying, generation: 1, messageId: "mail-to-staying" },
      ]);
    } finally {
      client.close();
      await server.stop(true);
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
  // A null capture means this file loaded without `@intx/log` having
  // installed its default sink, which cannot happen -- importing the
  // package runs the install. Resetting here instead would leave the
  // worker with no logging configuration at all, and the install
  // cannot re-fire to repair it.
  if (!savedLogConfig) {
    throw new Error(
      "no logging configuration was captured before this suite replaced it",
    );
  }
  configureSync({ reset: true, ...savedLogConfig });
});

function verdicts(): CapturedLog[] {
  return capturedLogs.filter(
    (r) =>
      r.category.length >= 4 &&
      r.category[2] === "ws" &&
      r.category[3] === "inbound-signature",
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

// Drives `mail.inbound` frames across the real hub-link WS surface and proves
// the INTR-512 verify at the ingress seam now ENFORCES the recipient's resolved
// inbound-mail policy: an admitted outcome reaches the mail router, a rejected
// one is dropped before it ever gets there.
describe("hub-link mail.inbound signature enforcement", () => {
  beforeEach(() => {
    capturedLogs.length = 0;
  });

  async function withConnectedLink(
    label: string,
    body: (ctx: {
      deploymentAddress: string;
      routed: Uint8Array[];
    }) => Promise<void>,
    opts?: {
      resolveSenderCrypto?: Parameters<
        typeof createHubLink
      >[0]["resolveSenderCrypto"];
      // The resolved policy registered for the deployment address. Omit for the
      // neutral "author declared nothing" policy (clean admits, the four
      // author-controllable outcomes reject); pass `null` to leave the address
      // UNREGISTERED so the seam resolves it to the fully-closed default and
      // rejects every outcome.
      policy?: ResolvedInboundMailPolicy | null;
    },
  ): Promise<void> {
    const transport = createInMemoryTransport();
    const sessions = createMockSessionManager();
    const deploymentAddress = `run_${label}@integration.interchange`;

    const routed: Uint8Array[] = [];
    const mailInboundRouter = {
      tryRoute(_address: string, message: Uint8Array): Promise<void> | null {
        routed.push(message);
        return Promise.resolve();
      },
    };

    const policyRegistry = createInboundMailPolicyRegistry();
    const policy = opts?.policy;
    if (policy !== null) {
      policyRegistry.register(
        deploymentAddress,
        policy ?? resolveInboundMailPolicy(undefined),
      );
    }

    const bindings = withTestDeployBindings();
    await provisionDeploymentKey(bindings.keyStore, deploymentAddress);
    const client = createHubLink({
      hubURL: `ws://localhost:${env.server.port}/ws`,
      sidecarId: `sc-enforce-${label}`,
      token: "test-token",
      transport,
      sessions,
      ...bindings,
      ...(opts?.resolveSenderCrypto !== undefined
        ? { resolveSenderCrypto: opts.resolveSenderCrypto }
        : {}),
      lookupInboundMailPolicy: createInboundMailPolicyLookup(policyRegistry),
      mailInboundRouter,
      getIncarnations: () => [
        { address: deploymentAddress, generation: 1, state: "live" },
      ],
    });

    client.connect();
    try {
      await waitUntil(() =>
        env.router.getRoutableAddresses().includes(deploymentAddress),
      );
      await body({ deploymentAddress, routed });
    } finally {
      client.close();
      await waitUntil(
        () =>
          !env.router.getConnectedSidecars().includes(`sc-enforce-${label}`),
      );
    }
  }

  test("a clean valid/match signature under an admit policy is delivered", async () => {
    // The cache resolves the sender's real key, so the message verifies
    // valid/match -> clean, which the neutral policy admits. The frame reaches
    // the mail router.
    const sender = "external@remote.interchange";
    const crypto = createEd25519Crypto(await generateKeyPair());
    const raw = await makeSignedMail(crypto, sender);

    await withConnectedLink(
      "clean",
      async ({ deploymentAddress, routed }) => {
        expect(
          await env.router.routeMail(
            deploymentAddress,
            base64Encode(raw),
            sender,
          ),
        ).toBe(true);

        await waitUntil(() => verdicts().length > 0);
        const verdict = verdicts()[0];
        expect(verdict?.properties["signature"]).toBe("valid");
        expect(verdict?.properties["fromMatch"]).toBe("match");
        await waitUntil(() => routed.length > 0);
        expect(routed).toHaveLength(1);
      },
      {
        resolveSenderCrypto: (address) =>
          address === sender
            ? createPublicKeyCrypto(crypto.getPublicKey())
            : undefined,
      },
    );
  });

  test("a fault degrades to an error verdict and is rejected", async () => {
    const sender = "external@remote.interchange";
    const crypto = createEd25519Crypto(await generateKeyPair());
    const raw = await makeSignedMail(crypto, sender);

    await withConnectedLink(
      "error",
      async ({ deploymentAddress, routed }) => {
        expect(
          await env.router.routeMail(
            deploymentAddress,
            base64Encode(raw),
            sender,
          ),
        ).toBe(true);

        await waitUntil(() => verdicts().length > 0);
        expect(verdicts()[0]?.properties["signature"]).toBe("error");
        // error is pinned to reject in every resolved policy, so the mail is
        // dropped before the router is consulted.
        expect(routed).toHaveLength(0);
      },
      {
        // The resolver throws; the verify contains that as an `error` verdict
        // rather than letting it escape.
        resolveSenderCrypto: () => {
          throw new Error("resolver boom");
        },
      },
    );
  });

  test("an unregistered address resolves to the fully-closed policy and rejects a clean mail", async () => {
    const sender = "external@remote.interchange";
    const crypto = createEd25519Crypto(await generateKeyPair());
    const raw = await makeSignedMail(crypto, sender);

    await withConnectedLink(
      "unregistered",
      async ({ deploymentAddress, routed }) => {
        expect(
          await env.router.routeMail(
            deploymentAddress,
            base64Encode(raw),
            sender,
          ),
        ).toBe(true);

        await waitUntil(() => verdicts().length > 0);
        const verdict = verdicts()[0];
        // The verify still runs and the message is clean (valid/match), yet the
        // fully-closed policy of an unregistered address rejects even `clean`.
        expect(verdict?.properties["signature"]).toBe("valid");
        expect(verdict?.properties["fromMatch"]).toBe("match");
        expect(routed).toHaveLength(0);
      },
      {
        policy: null,
        resolveSenderCrypto: (address) =>
          address === sender
            ? createPublicKeyCrypto(crypto.getPublicKey())
            : undefined,
      },
    );
  });

  test("a valid signature under a forged From is untrustedFrom and rejected", async () => {
    const stamp = "external@remote.interchange";
    const forgedFrom = "victim@remote.interchange";
    const crypto = createEd25519Crypto(await generateKeyPair());
    const raw = await makeSignedMail(crypto, forgedFrom);

    await withConnectedLink(
      "forged",
      async ({ deploymentAddress, routed }) => {
        expect(
          await env.router.routeMail(
            deploymentAddress,
            base64Encode(raw),
            stamp,
          ),
        ).toBe(true);

        await waitUntil(() => verdicts().length > 0);
        const verdict = verdicts()[0];
        expect(verdict?.properties["signature"]).toBe("valid");
        expect(verdict?.properties["fromMatch"]).toBe("mismatch");
        // valid+mismatch -> untrustedFrom, which the neutral policy rejects.
        expect(routed).toHaveLength(0);
      },
      {
        resolveSenderCrypto: (address) =>
          address === stamp
            ? createPublicKeyCrypto(crypto.getPublicKey())
            : undefined,
      },
    );
  });

  test("a present but unparseable From is untrustedFrom and rejected", async () => {
    const sender = "external@remote.interchange";
    const crypto = createEd25519Crypto(await generateKeyPair());
    // A two-address From cannot reduce to one addr-spec, so the binding is
    // `unparseable` -- present but malformed, distinct from no From at all.
    const raw = await makeSignedMail(
      crypto,
      "alpha@remote.interchange, beta@remote.interchange",
    );

    await withConnectedLink(
      "unparseable",
      async ({ deploymentAddress, routed }) => {
        expect(
          await env.router.routeMail(
            deploymentAddress,
            base64Encode(raw),
            sender,
          ),
        ).toBe(true);

        // An unparseable From also emits a debug log in the same category
        // ahead of the verdict line, so select the verdict record by its
        // `signature` property rather than taking the first record.
        await waitUntil(() =>
          verdicts().some((r) => r.properties["signature"] !== undefined),
        );
        const verdict = verdicts().find(
          (r) => r.properties["signature"] !== undefined,
        );
        expect(verdict?.properties["fromMatch"]).toBe("unparseable");
        // unparseable -> untrustedFrom, which the neutral policy rejects.
        expect(routed).toHaveLength(0);
      },
      {
        resolveSenderCrypto: (address) =>
          address === sender
            ? createPublicKeyCrypto(crypto.getPublicKey())
            : undefined,
      },
    );
  });

  test("a missing signature under the neutral policy is rejected", async () => {
    // A plain, unsigned message (no multipart/signed body) verifies `missing`
    // once a cached key resolves for the sender, distinct from the `unknown` of
    // a cache miss. The neutral policy leaves `missing` at the default reject,
    // so the mail is dropped before the router is consulted.
    const sender = "external@remote.interchange";
    const crypto = createEd25519Crypto(await generateKeyPair());

    await withConnectedLink(
      "missing",
      async ({ deploymentAddress, routed }) => {
        expect(
          await env.router.routeMail(
            deploymentAddress,
            base64Encode(VALID_MESSAGE),
            sender,
          ),
        ).toBe(true);

        await waitUntil(() => verdicts().length > 0);
        const verdict = verdicts()[0];
        expect(verdict?.properties["signature"]).toBe("missing");
        // missing -> missing, which the neutral policy rejects.
        expect(routed).toHaveLength(0);
      },
      {
        resolveSenderCrypto: (address) =>
          address === sender
            ? createPublicKeyCrypto(crypto.getPublicKey())
            : undefined,
      },
    );
  });

  test("an author policy admitting unknown admits an unknown but still rejects an invalid", async () => {
    const unknownSender = "stranger@remote.interchange";
    const invalidSender = "imposter@remote.interchange";
    const signer = createEd25519Crypto(await generateKeyPair());
    const wrongKey = createEd25519Crypto(await generateKeyPair());
    const unknownRaw = await makeSignedMail(signer, unknownSender);
    const invalidRaw = await makeSignedMail(signer, invalidSender);

    await withConnectedLink(
      "unknownadmit",
      async ({ deploymentAddress, routed }) => {
        // unknownSender has no cached key -> unknown -> admitted by the policy.
        expect(
          await env.router.routeMail(
            deploymentAddress,
            base64Encode(unknownRaw),
            unknownSender,
          ),
        ).toBe(true);
        // invalidSender's cached key did not sign the message -> invalid, which
        // this policy leaves at the default reject.
        expect(
          await env.router.routeMail(
            deploymentAddress,
            base64Encode(invalidRaw),
            invalidSender,
          ),
        ).toBe(true);

        await waitUntil(() => verdicts().length >= 2);
        const signatures = verdicts().map((r) => r.properties["signature"]);
        expect(signatures).toContain("unknown");
        expect(signatures).toContain("invalid");
        await waitUntil(() => routed.length > 0);
        expect(routed).toHaveLength(1);
        expect(routed[0]).toEqual(unknownRaw);
      },
      {
        policy: resolveInboundMailPolicy({ unknown: "admit" }),
        resolveSenderCrypto: (address) =>
          address === invalidSender
            ? createPublicKeyCrypto(wrongKey.getPublicKey())
            : undefined,
      },
    );
  });

  test("a rejected frame does not wedge the queue for a following frame", async () => {
    const rejectSender = "reject@remote.interchange";
    const admitSender = "admit@remote.interchange";
    const crypto = createEd25519Crypto(await generateKeyPair());
    const rejectRaw = await makeSignedMail(crypto, rejectSender);
    const admitRaw = await makeSignedMail(crypto, admitSender);

    await withConnectedLink(
      "liveness",
      async ({ deploymentAddress, routed }) => {
        // First frame: the resolver throws -> error -> rejected inline. The
        // inline verify + reject must not stop the frames after it.
        expect(
          await env.router.routeMail(
            deploymentAddress,
            base64Encode(rejectRaw),
            rejectSender,
          ),
        ).toBe(true);
        // Second frame: a cache miss -> unknown -> admitted by the policy, so
        // it proves the chain still processes after the reject.
        expect(
          await env.router.routeMail(
            deploymentAddress,
            base64Encode(admitRaw),
            admitSender,
          ),
        ).toBe(true);

        await waitUntil(() => routed.length > 0);
        expect(routed).toHaveLength(1);
        expect(routed[0]).toEqual(admitRaw);
      },
      {
        policy: resolveInboundMailPolicy({ unknown: "admit" }),
        resolveSenderCrypto: (address) => {
          if (address === rejectSender) throw new Error("resolver boom");
          return undefined;
        },
      },
    );
  });
});
