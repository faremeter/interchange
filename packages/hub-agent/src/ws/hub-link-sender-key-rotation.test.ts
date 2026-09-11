// End-to-end proof of INTR-520: a user-principal sender key that rotates while
// a sidecar is disconnected is reconciled into the sidecar's SenderKeyCache when
// it reconnects. This exercises the whole loop against real components -- a real
// HubLink, a real SenderKeyCache, and a real hub-side createSidecarRouter -- so
// the pieces the per-commit unit tests cover in isolation are proven to compose:
//
//   1. The sidecar reports its cached rotatable senders on the reconnect frame.
//   2. The hub re-resolves each reported sender's CURRENT key (a per-call DB
//      lookup, so it sees the rotation) and pushes a sender.key.refresh.
//   3. The sidecar applies the pushed key to its cache.
//
// The reconnecting sidecar loads its cache cold from the same data dir the first
// connection persisted to, modelling a real drop-and-reconnect where the cache
// survives on disk. The reconnect connect carries a restored workflow address so
// it emits a genuine `reconnect` frame (not `register`), pinning both hub
// handshake paths to this behavior.

import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { Hono } from "hono";
import { upgradeWebSocket, websocket } from "hono/bun";
import {
  createSidecarRouter,
  type SidecarAuthenticator,
  type WsHandle,
} from "@intx/hub-sessions";
import { createInMemoryTransport } from "@intx/mail-memory";
import { hexDecode, hexEncode } from "@intx/types";

import { createHubLink, type DeployRouter } from "./hub-link";
import { resolveInboundMailPolicy } from "./inbound-signature";
import { createSenderKeyCache } from "../sender-key-cache";
import type { AgentKeyStore } from "../agent-key-store";
import type { SessionManager } from "../session-manager";

// ---------------------------------------------------------------------------
// Minimal link dependencies. The rotation loop touches only the sender cache
// wiring, so the deploy/session/key stubs exist only to satisfy the config.
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
  const d = await fs.mkdtemp(path.join(os.tmpdir(), "sender-key-rotation-"));
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.rm(d, { recursive: true, force: true })),
  );
});

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

function makeKey(seed: number): Uint8Array {
  const key = new Uint8Array(32);
  for (let i = 0; i < 32; i++) key[i] = (seed + i) & 0xff;
  return key;
}

// ---------------------------------------------------------------------------
// Test server: a real createSidecarRouter behind a ws server. It fences the
// reporting sidecar's allocation and pins its workflow address from the
// handshake frame (mirroring the production allocation-authenticated flow), and
// records the handshake frame types so the test can assert a real reconnect.
// ---------------------------------------------------------------------------

type AllocatedIdentity = Extract<
  Awaited<ReturnType<SidecarAuthenticator>>,
  { kind: "allocated" }
>;

function startRotationServer(
  resolveSenderKeyStrict: (address: string) => Promise<string | null>,
) {
  const observedHandshakes: string[] = [];
  // Memoize one mutable identity per sidecarId so the handshake frame can pin
  // its workflow address before authentication reads it back (mirroring the
  // production allocation-authenticated reconnect flow).
  const identities = new Map<string, AllocatedIdentity>();
  function ensureIdentity(sidecarId: string): AllocatedIdentity {
    const existing = identities.get(sidecarId);
    if (existing !== undefined) return existing;
    const identity: AllocatedIdentity = {
      kind: "allocated",
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

  const router = createSidecarRouter({
    authenticateSidecar: async ({ sidecarId }) => ensureIdentity(sidecarId),
    validateSidecarIdentity: async () => true,
    hubPublicKey: "a".repeat(64),
    requestTimeoutMs: 5000,
    lookups: { resolveSenderKeyStrict },
  });

  function prepareHandshake(data: string): void {
    const raw: unknown = JSON.parse(data);
    if (
      typeof raw !== "object" ||
      raw === null ||
      !("type" in raw) ||
      (raw.type !== "register" && raw.type !== "reconnect") ||
      !("sidecarId" in raw) ||
      typeof raw.sidecarId !== "string"
    ) {
      return;
    }
    observedHandshakes.push(raw.type);
    router.fenceAllocation(`allocation-${raw.sidecarId}`, 1);
    const identity = ensureIdentity(raw.sidecarId);
    const addresses =
      "agentAddresses" in raw && Array.isArray(raw.agentAddresses)
        ? raw.agentAddresses
        : [];
    if (addresses.length === 1 && typeof addresses[0] === "string") {
      // The identity's workflowRunAddress is readonly on the type; the
      // production allocation flow pins it from the frame the same way.
      Object.assign(identity, { workflowRunAddress: addresses[0] });
    }
  }

  const app = new Hono();
  app.get(
    "/ws",
    upgradeWebSocket(() => {
      let handle: WsHandle;
      return {
        onOpen(_evt, ws) {
          handle = {
            send: (data: string) => ws.send(data),
            close: () => ws.close(),
          };
          router.handleOpen(handle);
        },
        onMessage(evt) {
          if (typeof evt.data !== "string") return;
          prepareHandshake(evt.data);
          router.handleMessage(handle, evt.data);
        },
        onClose() {
          router.handleClose(handle);
        },
      };
    }),
  );

  const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });
  return { server, router, observedHandshakes };
}

describe("hub-link sender-key rotation on reconnect", () => {
  test("a key rotated during the offline window lands in the cache on reconnect", async () => {
    const sender = "usr_sender@tenant.example";
    const oldKey = makeKey(1);
    const newKey = makeKey(2);
    let currentKey = hexEncode(oldKey);
    const { server, router, observedHandshakes } = startRotationServer(
      (address) => Promise.resolve(address === sender ? currentKey : null),
    );

    const dataDir = await tempDir();
    const sidecarId = "sc-rotation";
    const hubURL = `ws://localhost:${server.port}/ws`;

    try {
      // The sidecar already holds the sender's OLD key before it ever connects.
      const seedCache = await createSenderKeyCache({
        dataDir,
        writeFileDurable,
        removeFileDurable,
      });
      await seedCache.put(sender, oldKey);

      // First connection: a live session that will drop. It reports the cached
      // sender and the hub pushes the still-current OLD key (a no-op refresh).
      const firstCache = await createSenderKeyCache({
        dataDir,
        writeFileDurable,
        removeFileDurable,
      });
      const first = createHubLink({
        hubURL,
        sidecarId,
        token: "test-token",
        transport: createInMemoryTransport(),
        sessions: createStubSessionManager(),
        keyStore: createStubKeyStore(),
        resolveSenderCrypto: () => undefined,
        lookupInboundMailPolicy: () => resolveInboundMailPolicy(undefined),
        cacheSenderKey: (address, publicKey) =>
          firstCache.put(address, hexDecode(publicKey)),
        evictSenderKey: (address) => firstCache.evict(address),
        deployRouter: createStubDeployRouter(),
        getCachedSenderAddresses: () => firstCache.rotatableAddresses(),
      });
      first.connect();
      await waitFor(() => router.getConnectedSidecars().includes(sidecarId));
      first.close();
      await waitFor(() => !router.getConnectedSidecars().includes(sidecarId));

      // The key rotates hub-side while the sidecar is disconnected.
      currentKey = hexEncode(newKey);

      // The sidecar reconnects, cold-loading its cache from the same data dir.
      // A restored workflow address makes this a genuine `reconnect` frame.
      const reconnectCache = await createSenderKeyCache({
        dataDir,
        writeFileDurable,
        removeFileDurable,
      });
      expect(reconnectCache.get(sender)).toEqual(oldKey);
      const workflowAddress = "run_deploy@tenant.example";
      const second = createHubLink({
        hubURL,
        sidecarId,
        token: "test-token",
        transport: createInMemoryTransport(),
        sessions: createStubSessionManager(),
        keyStore: createStubKeyStore(),
        resolveSenderCrypto: () => undefined,
        lookupInboundMailPolicy: () => resolveInboundMailPolicy(undefined),
        cacheSenderKey: (address, publicKey) =>
          reconnectCache.put(address, hexDecode(publicKey)),
        evictSenderKey: (address) => reconnectCache.evict(address),
        deployRouter: createStubDeployRouter(),
        getWorkflowAddresses: () => [workflowAddress],
        getCachedSenderAddresses: () => reconnectCache.rotatableAddresses(),
      });
      second.connect();
      try {
        // The refresh push is fire-and-forget, so poll for the cache to update.
        await waitFor(() => {
          const held = reconnectCache.get(sender);
          return held !== undefined && held[0] === newKey[0];
        });
        expect(reconnectCache.get(sender)).toEqual(newKey);
        expect(reconnectCache.get(sender)).not.toEqual(oldKey);
        expect(observedHandshakes).toContain("reconnect");
      } finally {
        second.close();
      }
    } finally {
      await server.stop(true);
    }
  });

  test("a sender deleted during the offline window is evicted from the cache on reconnect", async () => {
    const sender = "usr_deleted@tenant.example";
    const key = makeKey(3);
    // The sender exists on the first connection and is hard-deleted before the
    // reconnect. The strict resolver returns the key while it exists and a
    // confirmed null once deleted -- the deleted-vs-fault distinction the evict
    // path turns on.
    let deleted = false;
    const { server, router, observedHandshakes } = startRotationServer(
      (address) =>
        Promise.resolve(address === sender && !deleted ? hexEncode(key) : null),
    );

    const dataDir = await tempDir();
    const sidecarId = "sc-evict-reconnect";
    const hubURL = `ws://localhost:${server.port}/ws`;

    try {
      // The sidecar already holds the sender's key before it ever connects.
      const seedCache = await createSenderKeyCache({
        dataDir,
        writeFileDurable,
        removeFileDurable,
      });
      await seedCache.put(sender, key);

      // First connection: the sender still exists, so the hub refreshes the
      // unchanged key -- a no-op that leaves the cache holding it.
      const firstCache = await createSenderKeyCache({
        dataDir,
        writeFileDurable,
        removeFileDurable,
      });
      const first = createHubLink({
        hubURL,
        sidecarId,
        token: "test-token",
        transport: createInMemoryTransport(),
        sessions: createStubSessionManager(),
        keyStore: createStubKeyStore(),
        resolveSenderCrypto: () => undefined,
        lookupInboundMailPolicy: () => resolveInboundMailPolicy(undefined),
        cacheSenderKey: (address, publicKey) =>
          firstCache.put(address, hexDecode(publicKey)),
        evictSenderKey: (address) => firstCache.evict(address),
        deployRouter: createStubDeployRouter(),
        getCachedSenderAddresses: () => firstCache.rotatableAddresses(),
      });
      first.connect();
      await waitFor(() => router.getConnectedSidecars().includes(sidecarId));
      first.close();
      await waitFor(() => !router.getConnectedSidecars().includes(sidecarId));

      // The sender's principal is hard-deleted hub-side while the sidecar is
      // disconnected: the strict resolver now returns a confirmed null for it.
      deleted = true;

      // The sidecar reconnects, cold-loading the cache (which still holds the
      // now-revoked key) from the same data dir. A restored workflow address
      // makes this a genuine `reconnect` frame.
      const reconnectCache = await createSenderKeyCache({
        dataDir,
        writeFileDurable,
        removeFileDurable,
      });
      // Precondition: the key IS present before reconnect, so the removal
      // assertion below is non-vacuous -- were the evict path dropped, the key
      // would remain and `get` would keep returning it (the reconnect refresh
      // never re-adds a deleted sender's key).
      expect(reconnectCache.get(sender)).toEqual(key);
      const workflowAddress = "run_deploy@tenant.example";
      const second = createHubLink({
        hubURL,
        sidecarId,
        token: "test-token",
        transport: createInMemoryTransport(),
        sessions: createStubSessionManager(),
        keyStore: createStubKeyStore(),
        resolveSenderCrypto: () => undefined,
        lookupInboundMailPolicy: () => resolveInboundMailPolicy(undefined),
        cacheSenderKey: (address, publicKey) =>
          reconnectCache.put(address, hexDecode(publicKey)),
        evictSenderKey: (address) => reconnectCache.evict(address),
        deployRouter: createStubDeployRouter(),
        getWorkflowAddresses: () => [workflowAddress],
        getCachedSenderAddresses: () => reconnectCache.rotatableAddresses(),
      });
      second.connect();
      try {
        // The evict push is fire-and-forget, so poll for the cache to drop it.
        await waitFor(() => reconnectCache.get(sender) === undefined);
        expect(reconnectCache.get(sender)).toBeUndefined();
        expect(observedHandshakes).toContain("reconnect");
      } finally {
        second.close();
      }
    } finally {
      await server.stop(true);
    }
  });
});
