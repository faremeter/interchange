import { describe, test, expect, beforeEach } from "bun:test";
import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import * as tar from "tar";
import type { CryptoProvider, HarnessConfig } from "@intx/types/runtime";
import type { AgentRepoStore, DeployContent } from "./agent-repo";
import type { AgentAssetWithAsset, AssetService } from "./asset-service";
import type { Principal, RepoId, RepoStore } from "./repo-store";
import {
  createSessionService,
  SessionLaunchError,
  type UserMessageParams,
} from "./session-service";
import { skillKindHandler } from "./skill-kind";
import type { SendPackOptions, SidecarRouter } from "./ws/sidecar-handler";
import { createSidecarEmitter } from "./ws/sidecar-events";

type Call = { method: string; args: unknown[] };

function createMockRouter(): SidecarRouter & {
  calls: Call[];
  routeMailResult: boolean;
} {
  const calls: Call[] = [];
  const track =
    (method: string) =>
    (...args: unknown[]) => {
      calls.push({ method, args });
      return Promise.resolve();
    };

  // track() returns a generic variadic function; each SidecarRouter method has
  // a specific typed signature. The casts below are unavoidable given the
  // generic tracker design — each method's parameter types cannot be inferred.
  const mock: SidecarRouter & {
    calls: Call[];
    routeMailResult: boolean;
  } = {
    calls,
    routeMailResult: true,
    handleOpen: track("handleOpen") as SidecarRouter["handleOpen"],
    handleMessage: track("handleMessage") as SidecarRouter["handleMessage"],
    handleClose: track("handleClose") as SidecarRouter["handleClose"],
    routeMail(agentAddress: string, rawMessage: string): boolean {
      calls.push({ method: "routeMail", args: [agentAddress, rawMessage] });
      return mock.routeMailResult;
    },
    sendAgentDeploy: ((
      agentAddress: string,
      config: HarnessConfig,
      workflow?: Parameters<SidecarRouter["sendAgentDeploy"]>[2],
    ) => {
      calls.push({
        method: "sendAgentDeploy",
        args: [agentAddress, config, workflow],
      });
      return Promise.resolve({ publicKey: "mock-public-key" });
    }) as SidecarRouter["sendAgentDeploy"],
    sendAgentUndeploy: track(
      "sendAgentUndeploy",
    ) as SidecarRouter["sendAgentUndeploy"],
    sendSessionStart: track(
      "sendSessionStart",
    ) as SidecarRouter["sendSessionStart"],
    sendSessionAbort: track(
      "sendSessionAbort",
    ) as SidecarRouter["sendSessionAbort"],
    sendGrantsUpdate: track(
      "sendGrantsUpdate",
    ) as SidecarRouter["sendGrantsUpdate"],
    sendSourcesUpdate: track(
      "sendSourcesUpdate",
    ) as SidecarRouter["sendSourcesUpdate"],
    sendPack: ((
      agentAddress: string,
      pack: Uint8Array,
      ref: string,
      commitSha: string,
      options?: SendPackOptions,
    ) => {
      calls.push({
        method: "sendPack",
        args: [agentAddress, pack, ref, commitSha, options],
      });
      return Promise.resolve();
    }) as SidecarRouter["sendPack"],
    sendSyncRequest: track(
      "sendSyncRequest",
    ) as SidecarRouter["sendSyncRequest"],
    subscribeAgent: (() => () => undefined) as SidecarRouter["subscribeAgent"],
    dispatchAgentEvent: () => undefined,
    getConnectedSidecars: () => [],
    getRoutableAddresses: () => [],
    getConnectorState: () => null,
    events: createSidecarEmitter(),
  };
  return mock;
}

function createMockRepoStore(): AgentRepoStore & { calls: Call[] } {
  const calls: Call[] = [];
  return {
    calls,
    async writeDeployTree(agentId: string, content: DeployContent) {
      calls.push({ method: "writeDeployTree", args: [agentId, content] });
      return { commitSha: "abc123" + "0".repeat(34) };
    },
    async createDeployPack(agentId: string) {
      calls.push({ method: "createDeployPack", args: [agentId] });
      return {
        pack: new Uint8Array([1, 2, 3]),
        commitSha: "abc123" + "0".repeat(34),
        ref: "refs/heads/deploy",
      };
    },
    async receiveAgentStatePack(
      repoId: { kind: "agent-state"; id: string },
      _pack: Uint8Array,
      _ref: string,
      _commitSha: string,
    ) {
      calls.push({ method: "receiveAgentStatePack", args: [repoId.id] });
    },
    async receiveWorkflowRunPack(
      _repoId: { kind: "workflow-run"; id: string },
      _pack: Uint8Array,
      _ref: string,
      _commitSha: string,
    ) {
      throw new Error("mock: receiveWorkflowRunPack not implemented");
    },
    getSigningPublicKey() {
      return new Uint8Array(32);
    },
    getDeployRef: (_agentId: string) => Promise.resolve(null),
    repoStore: unusedRepoStore(),
  };
}

function unusedRepoStore(): RepoStore {
  // SessionService tests never exercise the substrate; the inner
  // store is only present because AgentRepoStore exposes it. A typed
  // throwing stub keeps the surface honest without dragging in a
  // tmpdir-backed real store.
  const unused = () =>
    Promise.reject(new Error("mock AgentRepoStore.repoStore is not wired"));
  return {
    initRepo: unused,
    writeTree: unused,
    writeTreePreservingPrefix: unused,
    receivePack: unused,
    createPack: unused,
    resolveRef: unused,
    listRefs: unused,
    resolveHead: unused,
    getRepoDir: () => {
      throw new Error("mock AgentRepoStore.repoStore is not wired");
    },
    subscribe: () => {
      throw new Error("mock AgentRepoStore.repoStore is not wired");
    },
  };
}

type FakeAssetPackEntry = {
  pack: Uint8Array;
  commitSha: string;
  ref: string;
};

function createFakeRepoStore(
  packsByAssetId: Map<string, FakeAssetPackEntry>,
): RepoStore & {
  resolveRefCalls: { repoId: RepoId; ref: string }[];
  createPackCalls: { repoId: RepoId; ref: string }[];
} {
  const resolveRefCalls: { repoId: RepoId; ref: string }[] = [];
  const createPackCalls: { repoId: RepoId; ref: string }[] = [];
  const unused = () =>
    Promise.reject(new Error("repoStore method not wired in fake"));
  return {
    initRepo: unused,
    writeTree: unused,
    writeTreePreservingPrefix: unused,
    receivePack: unused,
    listRefs: unused,
    resolveHead: unused,
    getRepoDir: () => {
      throw new Error("repoStore method not wired in fake");
    },
    subscribe: () => {
      throw new Error("repoStore method not wired in fake");
    },
    async resolveRef(_principal: Principal, repoId: RepoId, ref: string) {
      resolveRefCalls.push({ repoId, ref });
      const entry = packsByAssetId.get(repoId.id);
      if (entry === undefined) return null;
      return entry.commitSha;
    },
    async createPack(_principal: Principal, repoId: RepoId, ref: string) {
      createPackCalls.push({ repoId, ref });
      const entry = packsByAssetId.get(repoId.id);
      if (entry === undefined) {
        throw new Error(`no fake pack registered for ${repoId.id}`);
      }
      return {
        pack: entry.pack,
        commitSha: entry.commitSha,
        ref: entry.ref,
      };
    },
    resolveRefCalls,
    createPackCalls,
  };
}

function createFakeAssetService(
  attachments: AgentAssetWithAsset[],
): AssetService {
  return {
    createAsset: () => {
      throw new Error("not used");
    },
    populateAsset: () => {
      throw new Error("not used");
    },
    attachAsset: () => {
      throw new Error("not used");
    },
    listAgentAssets: async (_agentId: string) => attachments,
    readAssetBlob: () => {
      throw new Error("not used");
    },
    listAssetBlobs: () => {
      throw new Error("not used");
    },
  };
}

type CapturedSessionAssetRow = {
  instanceId: string;
  agentAssetId: string | null;
  mountPath: string;
  assetPackSha: string;
  sourceCommitSha: string;
  source: "direct" | "resolved";
};

function createFakeDb(captured: CapturedSessionAssetRow[]) {
  // The session-service calls `db.insert(sessionAssetTable).values(row)` on
  // the happy path and `db.delete(sessionAssetTable).where(...)` on the
  // rollback path when sendPack fails. Both are no-ops here aside from
  // recording the inserts; the delete just resolves so the catch handler
  // can rethrow without secondary errors.
  const builder = {
    values(row: CapturedSessionAssetRow) {
      captured.push(row);
      return Promise.resolve();
    },
  };
  return {
    insert(_table: unknown) {
      return builder;
    },
    delete(_table: unknown) {
      return {
        where(_predicate: unknown) {
          return Promise.resolve();
        },
      };
    },
  };
}

const AGENT_ADDRESS = "agent-1@test.local";
const AGENT_ID = "agent-1";
const INSTANCE_ID = "instance-1";

const MOCK_CONFIG: HarnessConfig = {
  sessionId: "ses-1",
  agentId: AGENT_ID,
  tenantId: "tenant-1",
  principalId: "prin-1",
  agentAddress: AGENT_ADDRESS,
  systemPrompt: "Test",
  tools: [],
  grants: [],
  sources: [],
  defaultSource: "",
};

const MOCK_CONTENT: DeployContent = {
  systemPrompt: "Test",
};

describe("SessionService", () => {
  let router: ReturnType<typeof createMockRouter>;
  let repoStore: ReturnType<typeof createMockRepoStore>;

  beforeEach(() => {
    router = createMockRouter();
    repoStore = createMockRepoStore();
  });

  test("launchSession calls steps in order", async () => {
    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    await service.launchSession({
      agentAddress: AGENT_ADDRESS,
      agentId: AGENT_ID,
      instanceId: INSTANCE_ID,
      config: MOCK_CONFIG,
      deployContent: MOCK_CONTENT,
    });

    const methods = [
      ...repoStore.calls.map((c) => c.method),
      ...router.calls.map((c) => c.method),
    ];

    expect(methods).toEqual([
      "writeDeployTree",
      "createDeployPack",
      "sendAgentDeploy",
      "sendPack",
      "sendSessionStart",
    ]);
  });

  test("launchSession cleans up on pack failure", async () => {
    router.sendPack = () => Promise.reject(new Error("pack failed"));

    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    const err = await service
      .launchSession({
        agentAddress: AGENT_ADDRESS,
        agentId: AGENT_ID,
        instanceId: INSTANCE_ID,
        config: MOCK_CONFIG,
        deployContent: MOCK_CONTENT,
      })
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(SessionLaunchError);
    if (!(err instanceof SessionLaunchError)) throw new Error("unreachable");
    expect(err.phase).toBe("pack");
    expect(err.leakedAgent).toBe(false);

    const routerMethods = router.calls.map((c) => c.method);
    expect(routerMethods).toContain("sendAgentUndeploy");
  });

  test("launchSession cleans up on session start failure", async () => {
    router.sendSessionStart = () => Promise.reject(new Error("start failed"));

    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    const err = await service
      .launchSession({
        agentAddress: AGENT_ADDRESS,
        agentId: AGENT_ID,
        instanceId: INSTANCE_ID,
        config: MOCK_CONFIG,
        deployContent: MOCK_CONTENT,
      })
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(SessionLaunchError);
    if (!(err instanceof SessionLaunchError)) throw new Error("unreachable");
    expect(err.phase).toBe("start");
    expect(err.leakedAgent).toBe(false);

    const routerMethods = router.calls.map((c) => c.method);
    expect(routerMethods).toContain("sendAgentUndeploy");
  });

  test("launchSession reports leaked agent when cleanup fails", async () => {
    router.sendPack = () => Promise.reject(new Error("pack failed"));
    router.sendAgentUndeploy = () =>
      Promise.reject(new Error("cleanup failed"));

    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    const err = await service
      .launchSession({
        agentAddress: AGENT_ADDRESS,
        agentId: AGENT_ID,
        instanceId: INSTANCE_ID,
        config: MOCK_CONFIG,
        deployContent: MOCK_CONTENT,
      })
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(SessionLaunchError);
    if (!(err instanceof SessionLaunchError)) throw new Error("unreachable");
    expect(err.leakedAgent).toBe(true);
    expect(err.phase).toBe("pack");

    // The original error (pack failure) must be preserved as the cause,
    // not the cleanup failure.
    expect(err.cause).toBeInstanceOf(Error);
    if (!(err.cause instanceof Error)) throw new Error("unreachable");
    expect(err.cause.message).toBe("pack failed");
  });

  test("launchSession does not provision on write failure", async () => {
    repoStore.writeDeployTree = () => Promise.reject(new Error("write failed"));

    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    const err = await service
      .launchSession({
        agentAddress: AGENT_ADDRESS,
        agentId: AGENT_ID,
        instanceId: INSTANCE_ID,
        config: MOCK_CONFIG,
        deployContent: MOCK_CONTENT,
      })
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(SessionLaunchError);
    if (!(err instanceof SessionLaunchError)) throw new Error("unreachable");
    expect(err.phase).toBe("write");
    expect(err.leakedAgent).toBe(false);
    expect(router.calls.length).toBe(0);
  });

  test("launchSession does not send pack on provision failure", async () => {
    router.sendAgentDeploy = () =>
      Promise.reject(new Error("provision failed"));

    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    const err = await service
      .launchSession({
        agentAddress: AGENT_ADDRESS,
        agentId: AGENT_ID,
        instanceId: INSTANCE_ID,
        config: MOCK_CONFIG,
        deployContent: MOCK_CONTENT,
      })
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(SessionLaunchError);
    if (!(err instanceof SessionLaunchError)) throw new Error("unreachable");
    expect(err.phase).toBe("provision");
    expect(err.leakedAgent).toBe(false);

    const routerMethods = router.calls.map((c) => c.method);
    expect(routerMethods).not.toContain("sendPack");
    expect(routerMethods).not.toContain("sendAgentUndeploy");
  });

  test("endSession awaits undeploy ack", async () => {
    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    await service.endSession(AGENT_ADDRESS, "test_end");

    expect(router.calls.length).toBe(1);
    const call = router.calls[0];
    if (call === undefined) throw new Error("unreachable");
    expect(call.method).toBe("sendAgentUndeploy");
    expect(call.args).toEqual([AGENT_ADDRESS, "test_end"]);
  });

  // --- sendUserMessage tests ---

  function mockCryptoProvider(): CryptoProvider {
    const fakeSig = new Uint8Array(64);
    fakeSig.fill(0xab);
    return {
      sign: async (_data: Uint8Array) => fakeSig,
      signSSH: async () => "unused-in-this-test",
      verify: async () => true,
      getPublicKey: () => new Uint8Array(32),
    };
  }

  function userMessageParams(
    overrides?: Partial<UserMessageParams>,
  ): UserMessageParams {
    return {
      agentAddress: AGENT_ADDRESS,
      from: "user@test.local",
      messageId: "<msg-1@test.local>",
      date: new Date("2026-01-15T12:00:00Z"),
      content: "Hello agent",
      sessionId: "ses-1",
      tenantId: "tenant-1",
      cryptoProvider: mockCryptoProvider(),
      ...overrides,
    };
  }

  test("sendUserMessage calls routeMail with base64 MIME", async () => {
    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    await service.sendUserMessage(userMessageParams());

    const mailCalls = router.calls.filter((c) => c.method === "routeMail");
    expect(mailCalls.length).toBe(1);
    const call = mailCalls[0];
    if (call === undefined) throw new Error("unreachable");
    expect(call.args[0]).toBe(AGENT_ADDRESS);

    const rawArg = call.args[1];
    if (typeof rawArg !== "string") throw new Error("expected string arg");
    const decoded = Buffer.from(rawArg, "base64").toString("utf-8");
    expect(decoded).toContain("From: user@test.local");
    expect(decoded).toContain(`To: ${AGENT_ADDRESS}`);
    expect(decoded).toContain("Message-ID: <msg-1@test.local>");
    expect(decoded).toContain("Interchange-Session-ID: ses-1");
    expect(decoded).toContain("Interchange-Tenant-ID: tenant-1");
    expect(decoded).toContain("Hello agent");
  });

  test("sendUserMessage includes threading headers", async () => {
    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    await service.sendUserMessage(
      userMessageParams({
        inReplyTo: "<prev@test.local>",
        references: ["<root@test.local>", "<prev@test.local>"],
      }),
    );

    const call = router.calls.find((c) => c.method === "routeMail");
    if (call === undefined) throw new Error("unreachable");

    const rawArg1 = call.args[1];
    if (typeof rawArg1 !== "string") throw new Error("expected string arg");
    const decoded = Buffer.from(rawArg1, "base64").toString("utf-8");
    expect(decoded).toContain("In-Reply-To: <prev@test.local>");
    expect(decoded).toContain(
      "References: <root@test.local> <prev@test.local>",
    );
  });

  test("sendUserMessage throws when agent is unreachable", async () => {
    router.routeMailResult = false;

    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    const err = await service
      .sendUserMessage(userMessageParams())
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(Error);
    if (!(err instanceof Error)) throw new Error("unreachable");
    expect(err.message).toContain("unreachable");
  });

  test("sendUserMessage propagates signing failure", async () => {
    const badProvider: CryptoProvider = {
      sign: async () => {
        throw new Error("signing failed");
      },
      signSSH: async () => {
        throw new Error("unreachable in this test");
      },
      verify: async () => true,
      getPublicKey: () => new Uint8Array(32),
    };

    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    const err = await service
      .sendUserMessage(userMessageParams({ cryptoProvider: badProvider }))
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(Error);
    if (!(err instanceof Error)) throw new Error("unreachable");
    expect(err.message).toBe("signing failed");
    expect(router.calls.filter((c) => c.method === "routeMail").length).toBe(0);
  });

  // ---------------------------------------------------------------------
  // Attachment fan-out
  // ---------------------------------------------------------------------

  function makeAttachment(overrides: {
    id: string;
    assetId: string;
    name: string;
    ref?: string;
  }): AgentAssetWithAsset {
    return {
      id: overrides.id,
      agentId: AGENT_ID,
      assetId: overrides.assetId,
      ref: overrides.ref ?? "refs/heads/main",
      accessMode: "read-only",
      createdAt: new Date(),
      asset: {
        id: overrides.assetId,
        tenantId: "tenant-1",
        kind: "skill",
        name: overrides.name,
        displayName: null,
      },
    };
  }

  test("launchSession fans out attachment packs and inserts manifest rows", async () => {
    const packsByAssetId = new Map<string, FakeAssetPackEntry>([
      [
        "ast_greet",
        {
          pack: new Uint8Array([10, 11, 12]),
          commitSha: "c".repeat(40),
          ref: "refs/heads/main",
        },
      ],
      [
        "ast_search",
        {
          pack: new Uint8Array([20, 21, 22, 23]),
          commitSha: "d".repeat(40),
          ref: "refs/heads/main",
        },
      ],
    ]);
    const fakeRepoStore = createFakeRepoStore(packsByAssetId);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- replace the empty unusedRepoStore with the resolving fake for this test
    (repoStore as unknown as { repoStore: RepoStore }).repoStore =
      fakeRepoStore;

    const attachments = [
      makeAttachment({ id: "aas_greet", assetId: "ast_greet", name: "greet" }),
      makeAttachment({
        id: "aas_search",
        assetId: "ast_search",
        name: "search",
      }),
    ];

    const captured: CapturedSessionAssetRow[] = [];
    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
      assetService: createFakeAssetService(attachments),
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- DB stub satisfies the narrow surface session-service actually calls (insert().values())
      db: createFakeDb(captured) as unknown as NonNullable<
        Parameters<typeof createSessionService>[0]["db"]
      >,
    });

    await service.launchSession({
      agentAddress: AGENT_ADDRESS,
      agentId: AGENT_ID,
      instanceId: INSTANCE_ID,
      config: MOCK_CONFIG,
      deployContent: MOCK_CONTENT,
    });

    expect(captured).toHaveLength(2);

    const greetRow = captured.find((r) => r.agentAssetId === "aas_greet");
    if (greetRow === undefined) throw new Error("greet row missing");
    expect(greetRow.mountPath).toBe("skills/greet/");
    expect(greetRow.sourceCommitSha).toBe("c".repeat(40));
    expect(greetRow.instanceId).toBe(INSTANCE_ID);
    expect(greetRow.assetPackSha).toBe(
      createHash("sha256")
        .update(new Uint8Array([10, 11, 12]))
        .digest("hex"),
    );

    const searchRow = captured.find((r) => r.agentAssetId === "aas_search");
    if (searchRow === undefined) throw new Error("search row missing");
    expect(searchRow.mountPath).toBe("skills/search/");
    expect(searchRow.sourceCommitSha).toBe("d".repeat(40));

    const packCalls = router.calls.filter((c) => c.method === "sendPack");
    // 1 deploy pack + 2 attachment packs
    expect(packCalls).toHaveLength(3);
    const attachmentPackCalls = packCalls.slice(1);
    const opts0 = attachmentPackCalls[0]?.args[4];
    const opts1 = attachmentPackCalls[1]?.args[4];
    expect(opts0).toEqual({
      mountPath: "skills/greet/",
      repoId: { kind: "skill", id: "ast_greet" },
    });
    expect(opts1).toEqual({
      mountPath: "skills/search/",
      repoId: { kind: "skill", id: "ast_search" },
    });
  });

  test("launchSession appends the available_skills stanza to deploy prompt before writeDeployTree", async () => {
    const assetGreet = "ast_skill_greet_" + Math.random().toString(36).slice(2);
    const assetSearch =
      "ast_skill_search_" + Math.random().toString(36).slice(2);

    // Seed the skill index for both assets by driving the kind
    // handler's push lifecycle directly. The substrate runs
    // validatePush then onRefUpdated in the same write; we mirror
    // that ordering here.
    async function seedSkillIndex(
      assetId: string,
      skills: { name: string; description: string }[],
    ): Promise<void> {
      const ref = "refs/heads/main";
      const repoId: RepoId = { kind: "skill", id: assetId };
      const files: Record<string, string> = {};
      for (const s of skills) {
        files[`${s.name}/SKILL.md`] =
          `---\nname: ${s.name}\ndescription: ${s.description}\n---\nbody\n`;
      }
      const readBlob = async (p: string): Promise<Uint8Array> => {
        const body = files[p];
        if (body === undefined) throw new Error(`missing ${p}`);
        return new TextEncoder().encode(body);
      };
      const listDir = async (dirPath: string): Promise<string[]> => {
        const prefix = dirPath === "" ? "" : `${dirPath}/`;
        const names = new Set<string>();
        for (const p of Object.keys(files)) {
          if (prefix !== "" && !p.startsWith(prefix)) continue;
          const rest = p.slice(prefix.length);
          if (rest.length === 0) continue;
          const slash = rest.indexOf("/");
          names.add(slash === -1 ? rest : rest.substring(0, slash));
        }
        return Array.from(names);
      };
      const result = await skillKindHandler.validatePush({
        repoId,
        ref,
        principal: { kind: "hub" },
        topLevelTreePaths: skills.map((s) => s.name),
        readBlob,
        listDir,
        priorReadBlob: async () => null,
        priorListDir: async () => [],
      });
      if (!result.ok) {
        throw new Error(`validatePush failed: ${result.reason}`);
      }
      await skillKindHandler.onRefUpdated({
        repoId,
        ref,
        oldSha: null,
        newSha: "a".repeat(40),
      });
    }

    await seedSkillIndex(assetGreet, [
      { name: "wave", description: "Waves at the user." },
      { name: "bow", description: "Bows formally with A & B." },
    ]);
    await seedSkillIndex(assetSearch, [
      { name: "wave", description: "Searches for waves." },
    ]);

    const packsByAssetId = new Map<string, FakeAssetPackEntry>([
      [
        assetGreet,
        {
          pack: new Uint8Array([10, 11, 12]),
          commitSha: "c".repeat(40),
          ref: "refs/heads/main",
        },
      ],
      [
        assetSearch,
        {
          pack: new Uint8Array([20, 21, 22]),
          commitSha: "d".repeat(40),
          ref: "refs/heads/main",
        },
      ],
    ]);
    const fakeRepoStore = createFakeRepoStore(packsByAssetId);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- replace the empty unusedRepoStore with the resolving fake for this test
    (repoStore as unknown as { repoStore: RepoStore }).repoStore =
      fakeRepoStore;

    const attachments = [
      makeAttachment({
        id: "aas_greet",
        assetId: assetGreet,
        name: "greeter",
      }),
      makeAttachment({
        id: "aas_search",
        assetId: assetSearch,
        name: "searcher",
      }),
    ];

    const captured: CapturedSessionAssetRow[] = [];
    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
      assetService: createFakeAssetService(attachments),
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- DB stub satisfies the narrow surface session-service actually calls (insert().values())
      db: createFakeDb(captured) as unknown as NonNullable<
        Parameters<typeof createSessionService>[0]["db"]
      >,
    });

    await service.launchSession({
      agentAddress: AGENT_ADDRESS,
      agentId: AGENT_ID,
      instanceId: INSTANCE_ID,
      config: MOCK_CONFIG,
      deployContent: { systemPrompt: "Base prompt" },
    });

    const writeCall = repoStore.calls.find(
      (c) => c.method === "writeDeployTree",
    );
    if (writeCall === undefined) throw new Error("writeDeployTree not called");
    const content = writeCall.args[1];
    if (
      content === null ||
      typeof content !== "object" ||
      !("systemPrompt" in content) ||
      typeof content.systemPrompt !== "string"
    ) {
      throw new Error("writeDeployTree content shape unexpected");
    }
    const prompt = content.systemPrompt;

    expect(prompt.startsWith("Base prompt")).toBe(true);
    expect(prompt).toContain("<available_skills>");
    expect(prompt).toContain("</available_skills>");

    // Skill order: assets in listAgentAssets order; within an asset,
    // skills in index order (which the handler sorts).
    const greetWaveIdx = prompt.indexOf("<name>greeter/wave</name>");
    const greetBowIdx = prompt.indexOf("<name>greeter/bow</name>");
    const searchWaveIdx = prompt.indexOf("<name>searcher/wave</name>");
    expect(greetWaveIdx).toBeGreaterThan(-1);
    expect(greetBowIdx).toBeGreaterThan(-1);
    expect(searchWaveIdx).toBeGreaterThan(-1);
    expect(greetBowIdx).toBeLessThan(greetWaveIdx);
    expect(greetWaveIdx).toBeLessThan(searchWaveIdx);

    expect(prompt).toContain(
      "<description>Bows formally with A &amp; B.</description>",
    );
    expect(prompt).toContain("<path>workspace/skills/greeter/wave/</path>");
    expect(prompt).toContain("<path>workspace/skills/searcher/wave/</path>");
  });

  test("launchSession omits the available_skills stanza when no skill assets are attached", async () => {
    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    await service.launchSession({
      agentAddress: AGENT_ADDRESS,
      agentId: AGENT_ID,
      instanceId: INSTANCE_ID,
      config: MOCK_CONFIG,
      deployContent: { systemPrompt: "Only the base prompt" },
    });

    const writeCall = repoStore.calls.find(
      (c) => c.method === "writeDeployTree",
    );
    if (writeCall === undefined) throw new Error("writeDeployTree not called");
    const content = writeCall.args[1];
    if (
      content === null ||
      typeof content !== "object" ||
      !("systemPrompt" in content) ||
      typeof content.systemPrompt !== "string"
    ) {
      throw new Error("writeDeployTree content shape unexpected");
    }
    expect(content.systemPrompt).toBe("Only the base prompt");
    expect(content.systemPrompt).not.toContain("<available_skills>");
  });

  test("launchSession without assetService leaves the deploy-only flow unchanged", async () => {
    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    await service.launchSession({
      agentAddress: AGENT_ADDRESS,
      agentId: AGENT_ID,
      instanceId: INSTANCE_ID,
      config: MOCK_CONFIG,
      deployContent: MOCK_CONTENT,
    });

    const methods = [
      ...repoStore.calls.map((c) => c.method),
      ...router.calls.map((c) => c.method),
    ];
    expect(methods).toEqual([
      "writeDeployTree",
      "createDeployPack",
      "sendAgentDeploy",
      "sendPack",
      "sendSessionStart",
    ]);

    // No mountPath options on the single sendPack call.
    const packCall = router.calls.find((c) => c.method === "sendPack");
    if (packCall === undefined) throw new Error("sendPack not called");
    expect(packCall.args[4]).toBeUndefined();
  });

  test("launchSession writes a resolved-source session_asset row for resolver-derived packs", async () => {
    // Build a single-tarball asset registry, fake the DB query path
    // the session service walks (`listAssetsForTenant` walks
    // `tenant.findFirst` + `asset.findMany`), and assert the fan-out
    // emits a session_asset row whose `source` is "resolved" and
    // whose `agentAssetId` is null — the contract the audit split
    // introduced.
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "ss-resolved-"));
    const stagingDir = path.join(dir, "tools-resolved-1.0.0");
    const pkgDir = path.join(stagingDir, "package");
    await fs.mkdir(pkgDir, { recursive: true });
    await fs.writeFile(
      path.join(pkgDir, "package.json"),
      JSON.stringify({ name: "tools-resolved", version: "1.0.0" }),
    );
    const tarballPath = path.join(stagingDir, "out.tgz");
    await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
      "package",
    ]);
    const tarballBytes = await fs.readFile(tarballPath);
    const byPath = new Map<string, Uint8Array>([
      ["tarballs/tools-resolved-1.0.0.tgz", tarballBytes],
    ]);

    const RESOLVED_ASSET_ID = "ast_workspace_builtins";
    const RESOLVED_ASSET_NAME = "workspace-builtins";
    const TENANT_ID = "tenant-1";

    const assetService: AssetService = {
      createAsset: () => {
        throw new Error("not used");
      },
      populateAsset: () => {
        throw new Error("not used");
      },
      attachAsset: () => {
        throw new Error("not used");
      },
      // No direct attachments — the session has only the resolver pin.
      listAgentAssets: async (_agentId: string) => [],
      readAssetBlob: async ({ assetId, path: p }) => {
        if (assetId !== RESOLVED_ASSET_ID) {
          throw new Error(`unexpected assetId: ${assetId}`);
        }
        const b = byPath.get(p);
        if (b === undefined) throw new Error(`no blob at ${p}`);
        return b;
      },
      listAssetBlobs: async ({ assetId, dir: d }) => {
        if (assetId !== RESOLVED_ASSET_ID) {
          throw new Error(`unexpected assetId: ${assetId}`);
        }
        if (d !== "tarballs") {
          throw new Error(`unexpected list dir: ${d}`);
        }
        return Array.from(byPath.keys()).map((p) =>
          p.slice("tarballs/".length),
        );
      },
    };

    const assetRow = {
      id: RESOLVED_ASSET_ID,
      tenantId: TENANT_ID,
      kind: "package-registry" as const,
      name: RESOLVED_ASSET_NAME,
      displayName: null,
      creatorPrincipalId: null,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    const captured: CapturedSessionAssetRow[] = [];
    const fakeDb = {
      query: {
        tenant: {
          findFirst: async (_args: unknown) =>
            ({ parentId: null }) as { parentId: string | null },
        },
        asset: {
          findMany: async (_args: unknown) => [assetRow],
        },
      },
      insert(_table: unknown) {
        return {
          values(row: CapturedSessionAssetRow) {
            captured.push(row);
            return Promise.resolve();
          },
        };
      },
      delete(_table: unknown) {
        return {
          where(_predicate: unknown) {
            return Promise.resolve();
          },
        };
      },
    };

    const packsByAssetId = new Map<string, FakeAssetPackEntry>([
      [
        RESOLVED_ASSET_ID,
        {
          pack: new Uint8Array([42, 43, 44]),
          commitSha: "e".repeat(40),
          ref: "refs/heads/main",
        },
      ],
    ]);
    const fakeRepoStore = createFakeRepoStore(packsByAssetId);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- replace the empty unusedRepoStore with the resolving fake for this test
    (repoStore as unknown as { repoStore: RepoStore }).repoStore =
      fakeRepoStore;

    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
      assetService,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- DB stub satisfies the narrow surface session-service actually calls (query.tenant.findFirst, query.asset.findMany, insert/delete)
      db: fakeDb as unknown as NonNullable<
        Parameters<typeof createSessionService>[0]["db"]
      >,
      toolPackageRegistries: {
        httpRegistries: new Map(),
        defaultRegistry: RESOLVED_ASSET_NAME,
      },
    });

    await service.launchSession({
      agentAddress: AGENT_ADDRESS,
      agentId: AGENT_ID,
      instanceId: INSTANCE_ID,
      config: MOCK_CONFIG,
      deployContent: MOCK_CONTENT,
      toolPackagePins: [{ name: "tools-resolved", version: "1.0.0" }],
    });

    expect(captured).toHaveLength(1);
    const row = captured[0];
    if (row === undefined) throw new Error("unreachable");
    expect(row.agentAssetId).toBeNull();
    expect(row.source).toBe("resolved");
    expect(row.mountPath).toBe(`package-registries/${RESOLVED_ASSET_NAME}/`);
    expect(row.sourceCommitSha).toBe("e".repeat(40));
    expect(row.instanceId).toBe(INSTANCE_ID);
    expect(row.assetPackSha).toBe(
      createHash("sha256")
        .update(new Uint8Array([42, 43, 44]))
        .digest("hex"),
    );
  });

  test("launchSession rolls back earlier-committed session_asset rows on a later fan-out failure", async () => {
    // Two skill attachments; sendPack succeeds on the first attachment
    // pack (instance 1 of `sendPack`, after the deploy pack) and fails
    // on the second. The first attachment's row must come off the
    // books — the sidecar undeploy tears down its materialized state
    // and the manifest must follow.
    const packsByAssetId = new Map<string, FakeAssetPackEntry>([
      [
        "ast_greet",
        {
          pack: new Uint8Array([10, 11, 12]),
          commitSha: "c".repeat(40),
          ref: "refs/heads/main",
        },
      ],
      [
        "ast_search",
        {
          pack: new Uint8Array([20, 21, 22, 23]),
          commitSha: "d".repeat(40),
          ref: "refs/heads/main",
        },
      ],
    ]);
    const fakeRepoStore = createFakeRepoStore(packsByAssetId);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- replace the empty unusedRepoStore with the resolving fake for this test
    (repoStore as unknown as { repoStore: RepoStore }).repoStore =
      fakeRepoStore;

    let attachmentPackCalls = 0;
    const originalSendPack = router.sendPack.bind(router);
    router.sendPack = ((
      agentAddress: string,
      pack: Uint8Array,
      ref: string,
      commitSha: string,
      options?: SendPackOptions,
    ) => {
      if (options !== undefined) {
        attachmentPackCalls += 1;
        if (attachmentPackCalls === 2) {
          return Promise.reject(new Error("induced fan-out failure"));
        }
      }
      return originalSendPack(agentAddress, pack, ref, commitSha, options);
    }) as SidecarRouter["sendPack"];

    const attachments = [
      makeAttachment({ id: "aas_greet", assetId: "ast_greet", name: "greet" }),
      makeAttachment({
        id: "aas_search",
        assetId: "ast_search",
        name: "search",
      }),
    ];

    const captured: CapturedSessionAssetRow[] = [];
    let deleteCalls = 0;
    const fakeDb = {
      insert(_table: unknown) {
        return {
          values(row: CapturedSessionAssetRow) {
            captured.push(row);
            return Promise.resolve();
          },
        };
      },
      delete(_table: unknown) {
        return {
          where(_predicate: unknown) {
            deleteCalls += 1;
            return Promise.resolve();
          },
        };
      },
    };

    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
      assetService: createFakeAssetService(attachments),
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- DB stub satisfies the narrow surface session-service actually calls
      db: fakeDb as unknown as NonNullable<
        Parameters<typeof createSessionService>[0]["db"]
      >,
    });

    let err: unknown;
    try {
      await service.launchSession({
        agentAddress: AGENT_ADDRESS,
        agentId: AGENT_ID,
        instanceId: INSTANCE_ID,
        config: MOCK_CONFIG,
        deployContent: MOCK_CONTENT,
      });
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(SessionLaunchError);

    // The first attachment committed; the second failed mid-send and
    // its own catch handler rolled back its row. The outer rollback
    // sweep must additionally remove the first attachment's row even
    // though its own send succeeded — two delete calls total
    // (sendAttachmentPack's own rollback for the failed entry +
    // rollbackCommittedAttachments for the earlier successful one).
    expect(captured).toHaveLength(2);
    expect(deleteCalls).toBeGreaterThanOrEqual(2);
  });

  test("launchSession refuses overlapping direct + resolved attachments by asset id", async () => {
    // A package-registry asset attached directly to the agent AND
    // picked from by the tool-package resolver. The direct attachment
    // can carry any ref the operator chose; the resolver path emits
    // assetMounts at DEFAULT_ASSET_REF. Letting the launch proceed
    // would materialize the direct attachment's bytes at the mount
    // while assetMounts pointed at the resolver's ref — the loader
    // would then look up tarballs that do not exist at the
    // materialized mount. Refuse the conflict at launch as a
    // manifest-shaped error.
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "ss-dedup-"));
    const stagingDir = path.join(dir, "tools-shared-1.0.0");
    const pkgDir = path.join(stagingDir, "package");
    await fs.mkdir(pkgDir, { recursive: true });
    await fs.writeFile(
      path.join(pkgDir, "package.json"),
      JSON.stringify({ name: "tools-shared", version: "1.0.0" }),
    );
    const tarballPath = path.join(stagingDir, "out.tgz");
    await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
      "package",
    ]);
    const tarballBytes = await fs.readFile(tarballPath);
    const byPath = new Map<string, Uint8Array>([
      ["tarballs/tools-shared-1.0.0.tgz", tarballBytes],
    ]);

    const SHARED_ASSET_ID = "ast_shared";
    const SHARED_ASSET_NAME = "shared-registry";
    const TENANT_ID = "tenant-1";

    const directAttachment: AgentAssetWithAsset = {
      id: "att_direct",
      agentId: AGENT_ID,
      assetId: SHARED_ASSET_ID,
      ref: "refs/heads/main",
      accessMode: "read-only",
      createdAt: new Date(),
      asset: {
        id: SHARED_ASSET_ID,
        tenantId: TENANT_ID,
        kind: "package-registry",
        name: SHARED_ASSET_NAME,
        displayName: null,
      },
    };

    const assetService: AssetService = {
      createAsset: () => {
        throw new Error("not used");
      },
      populateAsset: () => {
        throw new Error("not used");
      },
      attachAsset: () => {
        throw new Error("not used");
      },
      listAgentAssets: async (_agentId: string) => [directAttachment],
      readAssetBlob: async ({ assetId, path: p }) => {
        if (assetId !== SHARED_ASSET_ID) {
          throw new Error(`unexpected assetId: ${assetId}`);
        }
        const b = byPath.get(p);
        if (b === undefined) throw new Error(`no blob at ${p}`);
        return b;
      },
      listAssetBlobs: async ({ assetId, dir: d }) => {
        if (assetId !== SHARED_ASSET_ID) {
          throw new Error(`unexpected assetId: ${assetId}`);
        }
        if (d !== "tarballs") {
          throw new Error(`unexpected list dir: ${d}`);
        }
        return Array.from(byPath.keys()).map((p) =>
          p.slice("tarballs/".length),
        );
      },
    };

    const assetRow = {
      id: SHARED_ASSET_ID,
      tenantId: TENANT_ID,
      kind: "package-registry" as const,
      name: SHARED_ASSET_NAME,
      displayName: null,
      creatorPrincipalId: null,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    const captured: CapturedSessionAssetRow[] = [];
    const fakeDb = {
      query: {
        tenant: {
          findFirst: async (_args: unknown) =>
            ({ parentId: null }) as { parentId: string | null },
        },
        asset: {
          findMany: async (_args: unknown) => [assetRow],
        },
      },
      insert(_table: unknown) {
        return {
          values(row: CapturedSessionAssetRow) {
            captured.push(row);
            return Promise.resolve();
          },
        };
      },
      delete(_table: unknown) {
        return {
          where(_predicate: unknown) {
            return Promise.resolve();
          },
        };
      },
    };

    const packsByAssetId = new Map<string, FakeAssetPackEntry>([
      [
        SHARED_ASSET_ID,
        {
          pack: new Uint8Array([7, 8, 9]),
          commitSha: "f".repeat(40),
          ref: "refs/heads/main",
        },
      ],
    ]);
    const fakeRepoStore = createFakeRepoStore(packsByAssetId);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- replace the empty unusedRepoStore with the resolving fake for this test
    (repoStore as unknown as { repoStore: RepoStore }).repoStore =
      fakeRepoStore;

    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
      assetService,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- DB stub satisfies the narrow surface session-service actually calls
      db: fakeDb as unknown as NonNullable<
        Parameters<typeof createSessionService>[0]["db"]
      >,
      toolPackageRegistries: {
        httpRegistries: new Map(),
        defaultRegistry: SHARED_ASSET_NAME,
      },
    });

    let caught: unknown;
    try {
      await service.launchSession({
        agentAddress: AGENT_ADDRESS,
        agentId: AGENT_ID,
        instanceId: INSTANCE_ID,
        config: MOCK_CONFIG,
        deployContent: MOCK_CONTENT,
        toolPackagePins: [{ name: "tools-shared", version: "1.0.0" }],
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    expect(captured).toHaveLength(0);
    if (caught instanceof Error) {
      expect(caught.message).toMatch(
        /both directly attached to the agent and selected by the tool-package resolver/,
      );
    }
  });
});

describe("sendMultiStepDeployFrame", () => {
  test("wires the workflow projection onto sendAgentDeploy", async () => {
    const mockRouter = createMockRouter();
    const sentWorkflows: Parameters<SidecarRouter["sendAgentDeploy"]>[2][] = [];
    mockRouter.sendAgentDeploy = ((
      _agentAddress: string,
      _config: HarnessConfig,
      workflow?: Parameters<SidecarRouter["sendAgentDeploy"]>[2],
    ) => {
      sentWorkflows.push(workflow);
      return Promise.resolve({ publicKey: "ed25519-supervisor-pubkey" });
    }) as SidecarRouter["sendAgentDeploy"];

    const { sendMultiStepDeployFrame } = await import("./session-service");
    const { defineWorkflow, step } = await import("@intx/workflow/definition");
    const { defineAgent } = await import("@intx/agent");
    const stubAgent = defineAgent({
      id: "stub",
      systemPrompt: "you stub",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });
    const definition = defineWorkflow({
      id: "wf_multi",
      trigger: { type: "manual" },
      steps: {
        plan: step({ agent: stubAgent, after: [] }),
        execute: step({ agent: stubAgent, after: ["plan"] }),
      },
    });
    const sources = {
      plan: {
        id: "src-plan",
        provider: "anthropic",
        baseURL: "https://api.example/anthropic",
        apiKey: "secret-plan",
        model: "mock-model",
      },
      execute: {
        id: "src-execute",
        provider: "anthropic",
        baseURL: "https://api.example/anthropic",
        apiKey: "secret-execute",
        model: "mock-model",
      },
    };
    const config: HarnessConfig = {
      sessionId: "ses-multi",
      agentId: "ins_dep_abc",
      tenantId: "tenant-1",
      principalId: "prin-multi",
      agentAddress: "ins_dep_abc@workflow.interchange",
      systemPrompt: "deployment-level",
      tools: [],
      grants: [],
      sources: Object.values(sources),
      defaultSource: "src-plan",
    };

    const result = await sendMultiStepDeployFrame({
      sidecarRouter: mockRouter,
      agentAddress: "ins_dep_abc@workflow.interchange",
      config,
      definition,
      sources,
    });

    expect(result).toEqual({ publicKey: "ed25519-supervisor-pubkey" });
    expect(sentWorkflows).toHaveLength(1);
    const sent = sentWorkflows[0];
    if (sent === undefined) throw new Error("missing workflow projection");
    // The workflow field must structurally match the upstream
    // AgentDeployFrame projection: { definition: { id, stepOrder, steps }, sources }.
    expect(sent.definition.id).toBe("wf_multi");
    expect(sent.definition.stepOrder).toEqual(["plan", "execute"]);
    expect(Object.keys(sent.definition.steps).sort()).toEqual([
      "execute",
      "plan",
    ]);
    expect(sent.sources).toEqual(sources);
  });
});
