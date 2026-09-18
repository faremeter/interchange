// Exercises both halves of the sender-key refresh feature's hub-link surface:
//
//   1. REPORT (on connect): the link announces the sidecar's cached rotatable
//      senders on its register/reconnect frame, read from the
//      `getCachedSenderAddresses` callback. Reported on BOTH frame types and
//      omitted when empty.
//   2. RECEIVE (`sender.key.refresh` arm in `handleMessage`): a hub-pushed
//      key-only refresh drives the source-opaque `cacheSenderKey` write peer
//      and touches nothing else. A cache-write fault (transient disk fault or
//      malformed key) is logged at ERROR and swallowed, never wedging the
//      per-connection message chain and never poisoning a run -- there is no
//      run to poison on this address-keyed path.
//
// The receive path is driven through the REAL edge composition the sidecar
// wires in `index.ts` (`(address, hex) => senderKeyCache.put(address,
// hexDecode(hex))`) against a real `SenderKeyCache`, so the hex-decode and
// 32-byte-length boundaries are exercised end to end rather than mocked.

import {
  describe,
  test,
  expect,
  afterAll,
  beforeAll,
  beforeEach,
  afterEach,
} from "bun:test";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { Hono } from "hono";
import { upgradeWebSocket, websocket } from "hono/bun";
import { type } from "arktype";
import { createInMemoryTransport } from "@intx/mail-memory";
import { hexDecode, hexEncode } from "@intx/types";
import { RegisterFrame, ReconnectFrame } from "@intx/types/sidecar";
import { configureSync, getConfig } from "@intx/log";
import { waitUntil } from "@intx/types/testing";

import { createHubLink, type DeployRouter } from "./hub-link";
import { resolveInboundMailPolicy } from "./inbound-signature";
import { createSenderKeyCache } from "../sender-key-cache";
import type { AgentKeyStore } from "../agent-key-store";
import type { SessionManager } from "../session-manager";

// ---------------------------------------------------------------------------
// Minimal link dependencies. The refresh arm touches only `cacheSenderKey`, so
// the deploy/session/key stubs exist only to satisfy the required config.
// ---------------------------------------------------------------------------

function createStubKeyStore(): AgentKeyStore {
  return {
    async loadOrGenerateKey(address: string) {
      throw new Error(`No key registered for ${address} in test store`);
    },
    recordHubKey: () => undefined,
    verifyDeployCommit: () => Promise.resolve(true),
    forgetAgent: () => undefined,
  };
}

function createStubDeployRouter(): DeployRouter {
  return {
    async deploy() {
      return { publicKey: "aa".repeat(32) };
    },
  };
}

function createStubSessionManager(): SessionManager {
  return {
    initRepo: () => Promise.resolve(),
    applyDeployPack: () => Promise.resolve(),
    applyAssetPack: () => Promise.resolve(),
    createStatePack: () =>
      Promise.resolve({
        pack: new Uint8Array([1, 2, 3]),
        commitSha: "abc123",
        ref: "refs/heads/main",
      }),
    deleteAgentDir: () => Promise.resolve(),
    getAddresses: () => [],
    getSessionId: () => undefined,
  };
}

async function writeFileDurable(
  filePath: string,
  contents: string,
): Promise<void> {
  await fs.writeFile(filePath, contents);
}

async function removeFileDurable(filePath: string): Promise<void> {
  await fs.rm(filePath, { force: true });
}

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fs.mkdtemp(
    path.join(os.tmpdir(), "sender-key-refresh-test-"),
  );
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.rm(d, { recursive: true, force: true })),
  );
});

// ---------------------------------------------------------------------------
// Test server: relays raw hub->sidecar frames to the connected link and hands
// a test the handshake frame its link sent along with a send handle for that
// same link, both keyed by the link's own sidecar id.
// ---------------------------------------------------------------------------

type ServerSend = (frame: unknown) => void;
type HandshakeFrame = RegisterFrame | ReconnectFrame;

// The sidecar sends a register or reconnect frame at handshake (plus pings and
// pack frames later). Validate each inbound frame against the real
// register/reconnect schemas; anything else is ignored.
function parseHandshake(raw: unknown): HandshakeFrame | null {
  const register = RegisterFrame(raw);
  if (!(register instanceof type.errors)) return register;
  const reconnect = ReconnectFrame(raw);
  if (!(reconnect instanceof type.errors)) return reconnect;
  return null;
}

type Connection = { frame: HandshakeFrame; send: ServerSend };

// Connections are addressed by the `sidecarId` their handshake carries, which
// every test here sets to a distinct value. The previous shape kept one
// mutable slot for whichever connection was current and treated "the slot is
// populated" as readiness, which cannot tell this test's link from the
// previous test's not-yet-closed one -- so a test's frames went out on the
// prior socket and its own wait expired. Registering at the handshake rather
// than at open is also the stronger signal: the handshake arriving proves
// that link's message chain is live, where an open socket only proves it
// exists.
function startTestServer(): {
  server: ReturnType<typeof Bun.serve>;
  awaitConnection: (sidecarId: string) => Promise<Connection>;
} {
  // A test may await its connection before or after the link handshakes, so
  // whichever happens first creates the slot and the other settles it.
  const slots = new Map<string, PromiseWithResolvers<Connection>>();

  function slotFor(sidecarId: string): PromiseWithResolvers<Connection> {
    const existing = slots.get(sidecarId);
    if (existing !== undefined) return existing;
    const created = Promise.withResolvers<Connection>();
    slots.set(sidecarId, created);
    return created;
  }

  const app = new Hono();
  app.get(
    "/ws",
    upgradeWebSocket(() => {
      let registered: string | undefined;
      return {
        onMessage(evt, ws) {
          if (typeof evt.data !== "string") return;
          const raw: unknown = JSON.parse(evt.data);
          const frame = parseHandshake(raw);
          if (frame === null) return;
          registered = frame.sidecarId;
          slotFor(frame.sidecarId).resolve({
            frame,
            send: (f) => {
              ws.send(JSON.stringify(f));
            },
          });
        },
        onClose() {
          // Drop the slot so a reconnect under the same id registers afresh
          // instead of handing back a closed socket.
          if (registered !== undefined) slots.delete(registered);
        },
      };
    }),
  );

  const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });

  async function awaitConnection(sidecarId: string): Promise<Connection> {
    return slotFor(sidecarId).promise;
  }

  return { server, awaitConnection };
}

const env = startTestServer();

afterAll(async () => {
  await env.server.stop(true);
});

type CapturedLog = {
  category: readonly string[];
  level: string;
  message: readonly unknown[];
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
          message: record.message,
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

// One clear per test rather than per describe. The error assertions read a
// module-level array, so a record left by a previous test -- or by a link that
// had not finished closing -- would otherwise be attributed to whichever test
// read next. Two of these clears used to sit inside test bodies and two in
// describe hooks, which made the empty-log assertion correct only by virtue of
// running first in its describe.
beforeEach(() => {
  capturedLogs.length = 0;
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

function refreshErrors(): string[] {
  return capturedLogs
    .filter(
      (r) =>
        r.level === "error" &&
        r.message.join("").includes("sender.key.refresh"),
    )
    .map((r) => r.message.join(""));
}

function evictErrors(): string[] {
  return capturedLogs
    .filter(
      (r) =>
        r.level === "error" && r.message.join("").includes("sender.key.evict"),
    )
    .map((r) => r.message.join(""));
}

function createTestLink(
  cacheSenderKey: (address: string, publicKey: string) => Promise<void>,
  sidecarId: string,
  report?: {
    getWorkflowAddresses?: () => string[];
    getCachedSenderAddresses?: () => string[];
  },
  evictSenderKey: (address: string) => Promise<void> = async () => undefined,
) {
  return createHubLink({
    hubURL: `ws://localhost:${env.server.port}/ws`,
    sidecarId,
    token: "test-token",
    transport: createInMemoryTransport(),
    sessions: createStubSessionManager(),
    keyStore: createStubKeyStore(),
    resolveSenderCrypto: () => undefined,
    lookupInboundMailPolicy: () => resolveInboundMailPolicy(undefined),
    cacheSenderKey,
    evictSenderKey,
    deployRouter: createStubDeployRouter(),
    ...(report?.getWorkflowAddresses !== undefined
      ? { getWorkflowAddresses: report.getWorkflowAddresses }
      : {}),
    ...(report?.getCachedSenderAddresses !== undefined
      ? { getCachedSenderAddresses: report.getCachedSenderAddresses }
      : {}),
  });
}

const noopCacheSenderKey = async () => undefined;

function makeKey(seed: number): Uint8Array {
  const key = new Uint8Array(32);
  for (let i = 0; i < 32; i++) key[i] = (seed + i) & 0xff;
  return key;
}

describe("hub-link sender.key.refresh", () => {
  test("updates the cache with the refreshed key through the real edge", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    const client = createTestLink(
      (address, publicKey) => cache.put(address, hexDecode(publicKey)),
      "sc-refresh-ok",
    );

    client.connect();
    try {
      const { send } = await env.awaitConnection("sc-refresh-ok");
      const address = "usr_alice@tenant.example";
      const key = makeKey(11);
      send({ type: "sender.key.refresh", address, publicKey: hexEncode(key) });

      await waitUntil(() => cache.get(address) !== undefined);
      expect(cache.get(address)).toEqual(key);
      expect(refreshErrors()).toHaveLength(0);
    } finally {
      client.close();
    }
  });

  test("a cache-write fault is logged and does not wedge later frames", async () => {
    let calls = 0;
    const recorded: string[] = [];
    const cacheSenderKey = async (address: string) => {
      calls += 1;
      // Transient fault on the first write; the second must still be processed,
      // proving the swallowed throw did not wedge the message chain.
      if (calls === 1) throw new Error("sender-key disk full");
      recorded.push(address);
    };
    const client = createTestLink(cacheSenderKey, "sc-refresh-fault");

    client.connect();
    try {
      const { send } = await env.awaitConnection("sc-refresh-fault");
      const key = hexEncode(makeKey(3));
      send({
        type: "sender.key.refresh",
        address: "usr_faulty@tenant.example",
        publicKey: key,
      });
      send({
        type: "sender.key.refresh",
        address: "usr_second@tenant.example",
        publicKey: key,
      });

      await waitUntil(() => recorded.length > 0);
      expect(recorded).toEqual(["usr_second@tenant.example"]);
      expect(calls).toBe(2);
      const errors = refreshErrors();
      expect(errors).toHaveLength(1);
      expect(errors[0]).toContain("usr_faulty@tenant.example");
    } finally {
      client.close();
    }
  });

  test("a wrong-length key is rejected without partially updating the cache", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    const client = createTestLink(
      (address, publicKey) => cache.put(address, hexDecode(publicKey)),
      "sc-refresh-badkey",
    );

    client.connect();
    try {
      const { send } = await env.awaitConnection("sc-refresh-badkey");
      const badAddress = "usr_short@tenant.example";
      // A 31-byte key: hexDecode succeeds but put's length guard throws.
      send({
        type: "sender.key.refresh",
        address: badAddress,
        publicKey: hexEncode(new Uint8Array(31)),
      });

      await waitUntil(() => refreshErrors().length > 0);
      expect(cache.get(badAddress)).toBeUndefined();

      // The link survived the malformed frame: a following valid frame caches.
      const goodAddress = "usr_good@tenant.example";
      const goodKey = makeKey(7);
      send({
        type: "sender.key.refresh",
        address: goodAddress,
        publicKey: hexEncode(goodKey),
      });
      await waitUntil(() => cache.get(goodAddress) !== undefined);
      expect(cache.get(goodAddress)).toEqual(goodKey);
    } finally {
      client.close();
    }
  });
});

describe("hub-link sender.key.evict", () => {
  test("removes the cached key through the real edge", async () => {
    const dataDir = await tempDir();
    const cache = await createSenderKeyCache({
      dataDir,
      writeFileDurable,
      removeFileDurable,
    });
    const address = "usr_deleted@tenant.example";
    await cache.put(address, makeKey(11));

    const client = createTestLink(
      noopCacheSenderKey,
      "sc-evict-ok",
      undefined,
      (addr) => cache.evict(addr),
    );

    client.connect();
    try {
      const { send } = await env.awaitConnection("sc-evict-ok");
      send({ type: "sender.key.evict", address });

      await waitUntil(() => cache.get(address) === undefined);
      expect(cache.get(address)).toBeUndefined();
      expect(evictErrors()).toHaveLength(0);
    } finally {
      client.close();
    }
  });

  test("an eviction fault is logged and does not wedge later frames", async () => {
    let calls = 0;
    const evicted: string[] = [];
    const evictSenderKey = async (address: string) => {
      calls += 1;
      // Transient fault on the first evict; the second must still be processed,
      // proving the swallowed throw did not wedge the message chain.
      if (calls === 1) throw new Error("unlink failed");
      evicted.push(address);
    };
    const client = createTestLink(
      noopCacheSenderKey,
      "sc-evict-fault",
      undefined,
      evictSenderKey,
    );

    client.connect();
    try {
      const { send } = await env.awaitConnection("sc-evict-fault");
      send({ type: "sender.key.evict", address: "usr_faulty@tenant.example" });
      send({ type: "sender.key.evict", address: "usr_second@tenant.example" });

      await waitUntil(() => evicted.length > 0);
      expect(evicted).toEqual(["usr_second@tenant.example"]);
      expect(calls).toBe(2);
      const errors = evictErrors();
      expect(errors).toHaveLength(1);
      expect(errors[0]).toContain("usr_faulty@tenant.example");
    } finally {
      client.close();
    }
  });
});

describe("hub-link cached-sender report on connect", () => {
  test("a register frame carries the reported cached senders", async () => {
    const reported = ["usr_alice@tenant.example", "usr_bob@tenant.example"];
    const client = createTestLink(noopCacheSenderKey, "sc-report-register", {
      // No workflow addresses -> the link sends a register frame.
      getWorkflowAddresses: () => [],
      getCachedSenderAddresses: () => reported,
    });

    client.connect();
    try {
      const { frame } = await env.awaitConnection("sc-report-register");
      expect(frame.type).toBe("register");
      expect(frame.cachedSenderAddresses).toEqual(reported);
    } finally {
      client.close();
    }
  });

  test("a reconnect frame carries the reported cached senders", async () => {
    const reported = ["usr_carol@tenant.example"];
    const client = createTestLink(noopCacheSenderKey, "sc-report-reconnect", {
      // A restored workflow address -> the link sends a reconnect frame; the
      // cached-sender report must ride it too, not only the register path.
      getWorkflowAddresses: () => ["run_deployment@tenant.example"],
      getCachedSenderAddresses: () => reported,
    });

    client.connect();
    try {
      const { frame } = await env.awaitConnection("sc-report-reconnect");
      expect(frame.type).toBe("reconnect");
      expect(frame.cachedSenderAddresses).toEqual(reported);
    } finally {
      client.close();
    }
  });

  test("an empty report omits the field from the frame", async () => {
    const client = createTestLink(noopCacheSenderKey, "sc-report-empty", {
      getWorkflowAddresses: () => [],
      getCachedSenderAddresses: () => [],
    });

    client.connect();
    try {
      const { frame } = await env.awaitConnection("sc-report-empty");
      expect(frame.type).toBe("register");
      expect(frame.cachedSenderAddresses).toBeUndefined();
    } finally {
      client.close();
    }
  });
});
