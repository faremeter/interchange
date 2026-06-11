import { describe, test, expect, afterEach } from "bun:test";
import fsp from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { createInMemoryTransport } from "@intx/mail-memory";
import type { MailAuditStore } from "@intx/storage-isogit";
import type { Harness } from "@intx/harness";
import type {
  CryptoProvider,
  HarnessConfig,
  InferenceEvent,
  KeyPair,
} from "@intx/types/runtime";

import { createAgentKeyStore } from "./agent-key-store";
import { createAgentRepoStore } from "./agent-repo-store";
import { createSessionManager } from "./session-manager";
import type {
  BuildHarnessArgs,
  HarnessBuilder,
  HarnessBundle,
} from "./harness-builder";

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fsp.mkdtemp(path.join(os.tmpdir(), "session-manager-test-"));
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fsp.rm(d, { recursive: true, force: true })),
  );
});

function makeKeyPair(seed: number): KeyPair {
  const privateKey = new Uint8Array(32);
  const publicKey = new Uint8Array(32);
  for (let i = 0; i < 32; i++) {
    privateKey[i] = (seed + i) & 0xff;
    publicKey[i] = (seed * 2 + i) & 0xff;
  }
  return { privateKey, publicKey };
}

function makeConfig(address: string): HarnessConfig {
  return {
    agentId: "test-agent",
    agentAddress: address,
    sessionId: "sess-1",
    principalId: "principal-1",
    tenantId: "tenant-1",
    systemPrompt: "test",
    tools: [],
    grants: [],
    sources: [
      {
        id: "test:test-model",
        provider: "test",
        apiKey: "key",
        baseURL: "http://localhost",
        model: "test-model",
      },
    ],
    defaultSource: "test:test-model",
  };
}

function makeCrypto(kp: KeyPair): CryptoProvider {
  return {
    async sign() {
      return new Uint8Array(64);
    },
    async signSSH() {
      return "unused";
    },
    async verify() {
      return true;
    },
    getPublicKey: () => kp.publicKey,
  };
}

function makeMailStoreStub(): MailAuditStore & { commits: string[] } {
  const commits: string[] = [];
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- MailAuditStore from storage-isogit is a structural type whose unrelated methods are unused in these tests
  const store = {
    commits,
    async commitMail() {
      commits.push("commit");
      return null;
    },
  } as unknown as MailAuditStore & { commits: string[] };
  return store;
}

function makeHarnessStub(): Harness {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Harness is a library type whose unused members are not structurally satisfied by the stub
  return {
    start: () => {
      /* no-op: harness.start is invoked by SessionManager but the tests
         do not observe inference activity */
    },
    stop: () => {
      /* no-op */
    },
    deliver: () => {
      /* no-op */
    },
    setSource: () => {
      /* no-op */
    },
    get blobReader() {
      throw new Error("blobReader unused in these tests");
    },
  } as unknown as Harness;
}

type RecordingBuilder = HarnessBuilder & {
  buildCalls: BuildHarnessArgs[];
  grants: GrantRecord[];
};
type GrantRecord = { address: string; count: number };

function makeRecordingBuilder(opts: {
  rejectSource?: (s: { provider: string }) => boolean;
  throwOnBuild?: boolean;
}): RecordingBuilder {
  const buildCalls: BuildHarnessArgs[] = [];
  const grants: GrantRecord[] = [];

  return {
    buildCalls,
    grants,
    canBuildSource(source) {
      if (opts.rejectSource?.(source)) {
        throw new Error(`source rejected: ${source.provider}`);
      }
    },
    async build(args) {
      buildCalls.push(args);
      if (opts.throwOnBuild) {
        throw new Error("builder boom");
      }
      const harness = makeHarnessStub();
      const mailStore = makeMailStoreStub();
      const bundle: HarnessBundle = {
        harness,
        mailStore,
        updateGrants(g) {
          grants.push({ address: args.agentAddress, count: g.length });
        },
        disposers: [],
      };
      return bundle;
    },
  };
}

function makeManagerHarness(
  dataDir: string,
  opts: {
    rejectSource?: (s: { provider: string }) => boolean;
    throwOnBuild?: boolean;
  } = {},
): {
  repoStore: ReturnType<typeof createAgentRepoStore>;
  keyStore: ReturnType<typeof createAgentKeyStore>;
  builder: RecordingBuilder;
  transport: ReturnType<typeof createInMemoryTransport>;
  manager: ReturnType<typeof createSessionManager>;
  events: { addr: string; sid: string }[];
} {
  const repoStore = createAgentRepoStore({ dataDir });
  const keyStore = createAgentKeyStore({
    dataDir,
    generateKeyPair: async () => makeKeyPair(11),
    signEd25519: () => new Uint8Array(64),
    verifySSHSig: () => true,
  });
  const builder = makeRecordingBuilder(opts);
  const transport = createInMemoryTransport();
  const events: { addr: string; sid: string }[] = [];
  const manager = createSessionManager({
    transport,
    repoStore,
    keyStore,
    buildHarness: builder,
    createAgentCrypto: (kp) => makeCrypto(kp),
    onEvent: (addr, sid) => events.push({ addr, sid }),
    onConnectorStateChanged: () => {
      /* no-op: tests do not assert on connector state */
    },
  });
  return { repoStore, keyStore, builder, transport, manager, events };
}

describe("SessionManager.provisionAgent + startSession happy path", () => {
  test("invokes the builder with the resolved per-agent context", async () => {
    const dataDir = await tempDir();
    const { manager, builder } = makeManagerHarness(dataDir);
    const cfg = makeConfig("agent@local");

    await manager.provisionAgent(cfg);
    await manager.startSession("agent@local");

    expect(manager.hasSession("agent@local")).toBe(true);
    expect(builder.buildCalls).toHaveLength(1);
    const call = builder.buildCalls[0];
    if (call === undefined) throw new Error("unreachable");
    expect(call.agentAddress).toBe("agent@local");
    expect(call.agentConfig.agentId).toBe("test-agent");
    expect(call.source.id).toBe("test:test-model");
    expect(call.storeDir).toContain("agent_at_local");
  });
});

describe("SessionManager.startSession rollback when builder throws", () => {
  test("restores provisioned state and unregisters the transport", async () => {
    const dataDir = await tempDir();
    const { manager, transport } = makeManagerHarness(dataDir, {
      throwOnBuild: true,
    });
    const cfg = makeConfig("agent@local");

    await manager.provisionAgent(cfg);
    await expect(manager.startSession("agent@local")).rejects.toThrow(
      "builder boom",
    );

    expect(manager.hasSession("agent@local")).toBe(false);
    expect(manager.isProvisioned("agent@local")).toBe(true);
    // Transport's per-agent view should be gone; getTransportFor throws
    // for unregistered addresses.
    expect(() => transport.getTransportFor("agent@local")).toThrow();
  });
});

describe("SessionManager.restoreSessions mismatch handling", () => {
  test("config without a key pair is reported as failed", async () => {
    const dataDir = await tempDir();
    const { manager, repoStore } = makeManagerHarness(dataDir);

    // Agent with a config but no on-disk keypair → goes to `failed`.
    await fsp.mkdir(path.join(dataDir, "ghost_at_local"), { recursive: true });
    await repoStore.persistConfig("ghost@local", makeConfig("ghost@local"));

    const result = await manager.restoreSessions();

    expect(result.failed).toContain("ghost@local");
    expect(result.restored.map((r) => r.address)).not.toContain("ghost@local");
  });
});

describe("SessionManager.applyAssetPack materializes under the workspace dir", () => {
  test("writes pack contents under <agentDir>/workspace/<mountPath>/", async () => {
    const dataDir = await tempDir();
    const { manager, repoStore } = makeManagerHarness(dataDir);

    const address = "agent@local";
    await manager.provisionAgent(makeConfig(address));

    // Build a real pack from a tiny source repo so applyAssetPack
    // can index it. Reuses isomorphic-git directly to avoid pulling
    // in the asset-service surface for this test.
    const sourceDir = path.join(dataDir, "asset-source");
    await fsp.mkdir(sourceDir, { recursive: true });
    const git = await import("isomorphic-git");
    const fs = await import("node:fs");
    await git.default.init({ fs, dir: sourceDir, defaultBranch: "main" });
    await fsp.writeFile(path.join(sourceDir, "hello.txt"), "hello from asset");
    await git.default.add({ fs, dir: sourceDir, filepath: "hello.txt" });
    const commitSha = await git.default.commit({
      fs,
      dir: sourceDir,
      message: "asset",
      author: { name: "t", email: "t@t" },
    });
    const oids = new Set<string>([commitSha]);
    const { commit } = await git.default.readCommit({
      fs,
      dir: sourceDir,
      oid: commitSha,
    });
    oids.add(commit.tree);
    const { tree } = await git.default.readTree({
      fs,
      dir: sourceDir,
      oid: commit.tree,
    });
    for (const entry of tree) oids.add(entry.oid);
    const packResult = await git.default.packObjects({
      fs,
      dir: sourceDir,
      oids: [...oids],
      write: false,
    });
    if (packResult.packfile === undefined) {
      throw new Error("packObjects produced no pack");
    }

    await manager.applyAssetPack(
      address,
      "skills/greet/",
      packResult.packfile,
      "refs/heads/main",
      commitSha,
    );

    const expected = path.join(
      repoStore.getAgentDir(address),
      "workspace",
      "skills",
      "greet",
      "hello.txt",
    );
    const contents = await fsp.readFile(expected, "utf-8");
    expect(contents).toBe("hello from asset");
  });
});

describe("SessionManager.updateGrants routes through the bundle", () => {
  test("calls bundle.updateGrants with the new grants and persists config", async () => {
    const dataDir = await tempDir();
    const { manager, builder, repoStore } = makeManagerHarness(dataDir);
    const cfg = makeConfig("agent@local");

    await manager.provisionAgent(cfg);
    await manager.startSession("agent@local");

    await manager.updateGrants("agent@local", [
      {
        id: "g-1",
        resource: "*",
        action: "*",
        effect: "allow",
        origin: "system",
        conditions: null,
        expiresAt: null,
        roleId: null,
        principalId: null,
      },
    ]);

    expect(builder.grants).toEqual([{ address: "agent@local", count: 1 }]);

    // Persisted config should now carry the new grants array length.
    const configs = await repoStore.scanConfigs();
    const entry = configs.find((c) => c.address === "agent@local");
    expect(entry?.config.grants).toHaveLength(1);
  });
});

describe("SessionManager.onAgentEvent per-agent fan-out", () => {
  test("delivers events only to the subscribed agent's listeners and respects the disposer", async () => {
    const dataDir = await tempDir();
    const { manager, builder } = makeManagerHarness(dataDir);

    await manager.provisionAgent(makeConfig("alice@local"));
    await manager.startSession("alice@local");
    await manager.provisionAgent(makeConfig("bob@local"));
    await manager.startSession("bob@local");

    const aliceBuild = builder.buildCalls.find(
      (c) => c.agentAddress === "alice@local",
    );
    const bobBuild = builder.buildCalls.find(
      (c) => c.agentAddress === "bob@local",
    );
    if (aliceBuild === undefined || bobBuild === undefined) {
      throw new Error("unreachable: builder did not capture both addresses");
    }

    const aliceEvents: InferenceEvent[] = [];
    const bobEvents: InferenceEvent[] = [];
    const dispose = manager.onAgentEvent("alice@local", (e) =>
      aliceEvents.push(e),
    );
    manager.onAgentEvent("bob@local", (e) => bobEvents.push(e));

    const aliceTick: InferenceEvent = {
      type: "inference.start",
      seq: 0,
      data: { model: "alice-m" },
    };
    const bobTick: InferenceEvent = {
      type: "inference.start",
      seq: 0,
      data: { model: "bob-m" },
    };
    aliceBuild.onEvent(aliceTick);
    bobBuild.onEvent(bobTick);

    expect(aliceEvents).toEqual([aliceTick]);
    expect(bobEvents).toEqual([bobTick]);

    dispose();
    aliceBuild.onEvent(aliceTick);
    expect(aliceEvents).toEqual([aliceTick]);
    // bob continues to receive after alice's disposer runs.
    bobBuild.onEvent(bobTick);
    expect(bobEvents).toEqual([bobTick, bobTick]);
  });
});
