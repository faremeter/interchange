import { describe, test, expect, beforeEach } from "bun:test";
import { type } from "arktype";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import * as tar from "tar";
import type {
  CryptoProvider,
  HarnessConfig,
  MessageAttachment,
} from "@intx/types/runtime";
import { base64Decode, hexEncode } from "@intx/types";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import { projectLiveToInert } from "@intx/workflow";
import { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import type { ToolPackageManifest } from "@intx/types/tool-packages";
import { extractAttachments } from "@intx/mime";
import { sessionAsset as sessionAssetTable } from "@intx/db/schema";
import type { DB } from "@intx/db";
import { generateId } from "@intx/hub-common";
import { deriveRunAddress } from "@intx/workflow-deploy";
import type { AgentRepoStore, DeployContent } from "./agent-repo";
import type { AssetService } from "./asset-service";
import type { Principal, RepoId, RepoStore } from "./repo-store";
import {
  createSessionService,
  SessionLaunchError,
  type UserMessageParams,
} from "./session-service";
import type {
  SendPackOptions,
  SidecarAllocationRouter,
  SidecarRouter,
} from "./ws/sidecar-handler";
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
    sendRunGrants: ((
      agentAddress: string,
      runId: string,
      stepGrants: Parameters<SidecarRouter["sendRunGrants"]>[2],
    ): boolean => {
      calls.push({
        method: "sendRunGrants",
        args: [agentAddress, runId, stepGrants],
      });
      return mock.routeMailResult;
    }) as SidecarRouter["sendRunGrants"],
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
    sendSourcesUpdate: track(
      "sendSourcesUpdate",
    ) as SidecarRouter["sendSourcesUpdate"],
    sendCredentialsUpdate: track(
      "sendCredentialsUpdate",
    ) as SidecarRouter["sendCredentialsUpdate"],
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
    sendProvisionStep: track(
      "sendProvisionStep",
    ) as SidecarRouter["sendProvisionStep"],
    bindStepRoute(stepAddress: string) {
      calls.push({ method: "bindStepRoute", args: [stepAddress] });
    },
    unbindStepRoute(stepAddress: string) {
      calls.push({ method: "unbindStepRoute", args: [stepAddress] });
    },
    sendSyncRequest: track(
      "sendSyncRequest",
    ) as SidecarRouter["sendSyncRequest"],
    sendSignalDeliver: ((
      opts: Parameters<SidecarRouter["sendSignalDeliver"]>[0],
    ) => {
      calls.push({ method: "sendSignalDeliver", args: [opts] });
    }) as SidecarRouter["sendSignalDeliver"],
    sendDrain: ((opts: Parameters<SidecarRouter["sendDrain"]>[0]) => {
      calls.push({ method: "sendDrain", args: [opts] });
    }) as SidecarRouter["sendDrain"],
    subscribeAgent: (() => () => undefined) as SidecarRouter["subscribeAgent"],
    dispatchAgentEvent: () => undefined,
    getConnectedSidecars: () => [],
    getRoutableAddresses: () => [],
    getConnectorState: () => null,
    events: createSidecarEmitter(),
  };
  return mock;
}

function createMockAllocationRouter(): SidecarAllocationRouter & {
  calls: Call[];
} {
  const calls: Call[] = [];
  return {
    calls,
    fenceAllocation() {
      throw new Error("mock allocation fence is not used by session service");
    },
    waitForAllocatedSidecar: async () => undefined,
    isAllocatedSidecarReady: async () => true,
    isAllocatedWorkflowActive: async () => false,
    sendAgentDeployToAllocation: async (...args) => {
      calls.push({ method: "sendAgentDeployToAllocation", args });
      return { publicKey: "allocated-public-key" };
    },
    sendPackToAllocation: async (...args) => {
      calls.push({ method: "sendPackToAllocation", args });
    },
    sendWorkflowRunPackToAllocation: async (...args) => {
      calls.push({ method: "sendWorkflowRunPackToAllocation", args });
    },
    bindAllocatedStepRoute: async (...args) => {
      calls.push({ method: "bindAllocatedStepRoute", args });
    },
    unbindAllocatedStepRoute(...args) {
      calls.push({ method: "unbindAllocatedStepRoute", args });
    },
    sendProvisionStepToAllocation: async (...args) => {
      calls.push({ method: "sendProvisionStepToAllocation", args });
    },
    sendWorkflowRunDispatchToAllocation: async (...args) => {
      calls.push({ method: "sendWorkflowRunDispatchToAllocation", args });
    },
    sendSignalDeliverToAllocation: async (...args) => {
      calls.push({ method: "sendSignalDeliverToAllocation", args });
    },
  };
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
    writeTreeDelta: unused,
    receivePack: unused,
    createPack: unused,
    commitPackedTip: () => {
      throw new Error("mock AgentRepoStore.repoStore is not wired");
    },
    resolveRef: unused,
    listRefs: unused,
    resolveHead: unused,
    getRepoDir: () => {
      throw new Error("mock AgentRepoStore.repoStore is not wired");
    },
    openCommittedReads: unused,
    openCommittedReadsAtCommit: unused,
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
    writeTreeDelta: unused,
    receivePack: unused,
    commitPackedTip: () => {
      throw new Error("repoStore method not wired in fake");
    },
    listRefs: unused,
    resolveHead: unused,
    getRepoDir: () => {
      throw new Error("repoStore method not wired in fake");
    },
    openCommittedReads: unused,
    openCommittedReadsAtCommit: unused,
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

type CapturedSessionAssetRow = {
  runId: string;
  mountPath: string;
  assetPackSha: string;
  sourceCommitSha: string;
};

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

  test("stageWorkflowStep stages without a warm harness", async () => {
    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    await service.stageWorkflowStep({
      agentAddress: AGENT_ADDRESS,
      agentId: AGENT_ID,
      runId: INSTANCE_ID,
      config: MOCK_CONFIG,
      deployContent: MOCK_CONTENT,
    });

    const methods = [
      ...repoStore.calls.map((c) => c.method),
      ...router.calls.map((c) => c.method),
    ];

    // A stage-only per-step deploy binds a transient route, fires the
    // no-spawn provision frame, delivers the deploy pack, and unbinds the
    // route -- and NEVER provisions a warm harness (`sendAgentDeploy` with
    // no workflow frame).
    expect(methods).toEqual([
      "writeDeployTree",
      "createDeployPack",
      "bindStepRoute",
      "sendProvisionStep",
      "sendPack",
      "unbindStepRoute",
    ]);
    expect(methods).not.toContain("sendAgentDeploy");
  });

  test("stageWorkflowStep keeps every phase on its allocated worker", async () => {
    const allocationRouter = createMockAllocationRouter();
    const service = createSessionService({
      sidecarRouter: router,
      sidecarAllocationRouter: allocationRouter,
      agentRepoStore: repoStore,
    });
    const target = { allocationId: "alloc-1", generation: 2 };

    await service.stageWorkflowStep({
      agentAddress: AGENT_ADDRESS,
      agentId: AGENT_ID,
      runId: INSTANCE_ID,
      config: MOCK_CONFIG,
      deployContent: MOCK_CONTENT,
      allocationTarget: target,
    });

    expect(allocationRouter.calls.map((call) => call.method)).toEqual([
      "bindAllocatedStepRoute",
      "sendProvisionStepToAllocation",
      "sendPackToAllocation",
      "unbindAllocatedStepRoute",
    ]);
    expect(router.calls).toEqual([]);
  });

  test("stageWorkflowStep unbinds the route even when the pack fails", async () => {
    router.sendPack = () => Promise.reject(new Error("pack failed"));

    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    await service
      .stageWorkflowStep({
        agentAddress: AGENT_ADDRESS,
        agentId: AGENT_ID,
        runId: INSTANCE_ID,
        config: MOCK_CONFIG,
        deployContent: MOCK_CONTENT,
      })
      .catch((e: unknown) => e);

    const routerMethods = router.calls.map((c) => c.method);
    // The transient route is dropped in the `finally`, even on failure, so no
    // stale per-step route leaks. A stage-only step has no warm harness to
    // tear down, so it never undeploys.
    expect(routerMethods).toContain("unbindStepRoute");
    expect(routerMethods).not.toContain("sendAgentUndeploy");
  });

  test("launchSession does not provision on write failure", async () => {
    repoStore.writeDeployTree = () => Promise.reject(new Error("write failed"));

    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    const err = await service
      .stageWorkflowStep({
        agentAddress: AGENT_ADDRESS,
        agentId: AGENT_ID,
        runId: INSTANCE_ID,
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
    router.sendProvisionStep = () =>
      Promise.reject(new Error("provision failed"));

    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    const err = await service
      .stageWorkflowStep({
        agentAddress: AGENT_ADDRESS,
        agentId: AGENT_ID,
        runId: INSTANCE_ID,
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
    const decoded = new TextDecoder().decode(base64Decode(rawArg));
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
    const decoded = new TextDecoder().decode(base64Decode(rawArg1));
    expect(decoded).toContain("In-Reply-To: <prev@test.local>");
    expect(decoded).toContain(
      "References: <root@test.local> <prev@test.local>",
    );
  });

  test("sendUserMessage threads attachments into the signed envelope", async () => {
    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    const attachments: MessageAttachment[] = [
      {
        name: "shot.png",
        contentType: "image/png",
        data: new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
      },
    ];

    await service.sendUserMessage(userMessageParams({ attachments }));

    const call = router.calls.find((c) => c.method === "routeMail");
    if (call === undefined) throw new Error("unreachable");
    const rawArg = call.args[1];
    if (typeof rawArg !== "string") throw new Error("expected string arg");
    const raw = base64Decode(rawArg);

    const extracted = extractAttachments(raw);
    expect(extracted).toHaveLength(1);
    const got = extracted[0];
    const orig = attachments[0];
    if (got === undefined || orig === undefined) {
      throw new Error("unreachable");
    }
    expect(got.name).toBe("shot.png");
    expect(got.contentType).toBe("image/png");
    expect(Array.from(got.data)).toEqual(Array.from(orig.data));
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

  async function createAllocatedAssetFixture() {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "ss-allocated-asset-"));
    const packageDir = path.join(dir, "package");
    await fs.mkdir(packageDir, { recursive: true });
    await fs.writeFile(
      path.join(packageDir, "package.json"),
      JSON.stringify({ name: "tools-allocated", version: "1.0.0" }),
    );
    const tarballPath = path.join(dir, "tools-allocated-1.0.0.tgz");
    await tar.create({ cwd: dir, gzip: true, file: tarballPath }, ["package"]);
    const tarballBytes = await fs.readFile(tarballPath);

    const assetId = "ast_allocated_registry";
    const assetName = "allocated-registry";
    const assetService: AssetService = {
      createAsset: () => {
        throw new Error("not used");
      },
      populateAsset: () => {
        throw new Error("not used");
      },
      readAssetBlob: async ({ assetId: requestedAssetId, path: blobPath }) => {
        if (requestedAssetId !== assetId) {
          throw new Error(`unexpected assetId: ${requestedAssetId}`);
        }
        if (blobPath !== "tarballs/tools-allocated-1.0.0.tgz") {
          throw new Error(`unexpected blob path: ${blobPath}`);
        }
        return tarballBytes;
      },
      listAssetBlobs: async ({ assetId: requestedAssetId, dir: blobDir }) => {
        if (requestedAssetId !== assetId) {
          throw new Error(`unexpected assetId: ${requestedAssetId}`);
        }
        if (blobDir !== "tarballs") {
          throw new Error(`unexpected list dir: ${blobDir}`);
        }
        return ["tools-allocated-1.0.0.tgz"];
      },
    };
    const assetRow = {
      id: assetId,
      tenantId: MOCK_CONFIG.tenantId,
      kind: "package-registry" as const,
      name: assetName,
      displayName: null,
      creatorPrincipalId: null,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    const state: {
      row: CapturedSessionAssetRow | undefined;
      insertAttempts: number;
      deleteCalls: number;
    } = {
      row: undefined,
      insertAttempts: 0,
      deleteCalls: 0,
    };
    const fakeDb = {
      query: {
        tenant: {
          findFirst: async (_args: unknown) => ({ parentId: null }),
        },
        asset: {
          findMany: async (_args: unknown) => [assetRow],
        },
        sessionAsset: {
          findFirst: async (_args: unknown) => state.row,
        },
      },
      insert(table: unknown) {
        if (table !== sessionAssetTable) {
          throw new Error("unexpected insert table");
        }
        return {
          values(row: CapturedSessionAssetRow) {
            return {
              onConflictDoNothing() {
                return {
                  returning() {
                    state.insertAttempts += 1;
                    if (state.row !== undefined) return Promise.resolve([]);
                    state.row = row;
                    return Promise.resolve([{ runId: row.runId }]);
                  },
                };
              },
            };
          },
        };
      },
      delete(table: unknown) {
        if (table !== sessionAssetTable) {
          throw new Error("unexpected delete table");
        }
        return {
          where(_predicate: unknown) {
            state.deleteCalls += 1;
            state.row = undefined;
            return Promise.resolve();
          },
        };
      },
    };
    const packsByAssetId = new Map<string, FakeAssetPackEntry>([
      [
        assetId,
        {
          pack: new Uint8Array([42, 43, 44]),
          commitSha: "e".repeat(40),
          ref: "refs/heads/main",
        },
      ],
    ]);
    const fakeRepoStore = createFakeRepoStore(packsByAssetId);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- replace the empty unusedRepoStore with the resolving fake for this fixture
    (repoStore as unknown as { repoStore: RepoStore }).repoStore =
      fakeRepoStore;
    const allocationRouter = createMockAllocationRouter();
    const service = createSessionService({
      sidecarRouter: router,
      sidecarAllocationRouter: allocationRouter,
      agentRepoStore: repoStore,
      assetService,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- DB stub satisfies the narrow allocated-asset surface exercised here
      db: fakeDb as unknown as NonNullable<
        Parameters<typeof createSessionService>[0]["db"]
      >,
      toolPackageRegistries: {
        httpRegistries: new Map(),
        defaultRegistry: assetName,
      },
    });
    const allocationTarget = { allocationId: "alloc-asset", generation: 2 };
    const launch = () =>
      service.stageWorkflowStep({
        agentAddress: AGENT_ADDRESS,
        agentId: AGENT_ID,
        runId: INSTANCE_ID,
        config: MOCK_CONFIG,
        deployContent: MOCK_CONTENT,
        toolPackagePins: [{ name: "tools-allocated", version: "1.0.0" }],
        allocationTarget,
      });
    const assetPackCallCount = () =>
      allocationRouter.calls.filter(
        (call) =>
          call.method === "sendPackToAllocation" && call.args[5] !== undefined,
      ).length;
    const failAssetPacks = () => {
      const sendPack =
        allocationRouter.sendPackToAllocation.bind(allocationRouter);
      allocationRouter.sendPackToAllocation = async (
        target,
        agentAddress,
        pack,
        ref,
        commitSha,
        options,
      ) => {
        if (options !== undefined) throw new Error("replacement pack failed");
        await sendPack(target, agentAddress, pack, ref, commitSha, options);
      };
      return () => {
        allocationRouter.sendPackToAllocation = sendPack;
      };
    };

    return {
      allocationRouter,
      assetId,
      assetPackCallCount,
      failAssetPacks,
      launch,
      packsByAssetId,
      state,
    };
  }

  test("allocated asset restoration reuses its materialization record", async () => {
    const fixture = await createAllocatedAssetFixture();

    await fixture.launch();
    const originalRow = fixture.state.row;
    expect(originalRow).toBeDefined();

    await fixture.launch();
    expect(fixture.state.row).toEqual(originalRow);
    expect(fixture.state.insertAttempts).toBe(2);
    expect(fixture.state.deleteCalls).toBe(0);
    expect(fixture.assetPackCallCount()).toBe(2);

    fixture.failAssetPacks();

    const err = await fixture.launch().catch((error: unknown) => error);
    expect(err).toBeInstanceOf(SessionLaunchError);
    expect(fixture.state.row).toEqual(originalRow);
    expect(fixture.state.deleteCalls).toBe(0);
  });

  test("allocated asset restoration rejects a conflicting record", async () => {
    const fixture = await createAllocatedAssetFixture();
    await fixture.launch();
    const originalRow = fixture.state.row;
    expect(originalRow).toBeDefined();

    fixture.packsByAssetId.set(fixture.assetId, {
      pack: new Uint8Array([45, 46, 47]),
      commitSha: "f".repeat(40),
      ref: "refs/heads/main",
    });

    const err = await fixture.launch().catch((error: unknown) => error);
    expect(err).toBeInstanceOf(SessionLaunchError);
    if (!(err instanceof SessionLaunchError)) throw new Error("unreachable");
    expect(err.message).toContain("conflicts with the allocated workflow");
    expect(fixture.state.row).toEqual(originalRow);
    expect(fixture.state.deleteCalls).toBe(0);
    expect(fixture.assetPackCallCount()).toBe(1);
  });

  test("allocated asset failure preserves a newly created recovery record", async () => {
    const fixture = await createAllocatedAssetFixture();
    const restoreAssetPacks = fixture.failAssetPacks();

    const err = await fixture.launch().catch((error: unknown) => error);
    expect(err).toBeInstanceOf(SessionLaunchError);
    const recoveryRow = fixture.state.row;
    expect(recoveryRow).toBeDefined();
    expect(fixture.state.deleteCalls).toBe(0);

    restoreAssetPacks();
    await fixture.launch();
    expect(fixture.state.row).toEqual(recoveryRow);
    expect(fixture.state.insertAttempts).toBe(2);
    expect(fixture.state.deleteCalls).toBe(0);
  });

  test("launchSession writes a resolved-source session_asset row for resolver-derived packs", async () => {
    // Build a single-tarball asset registry, fake the DB query path
    // the session service walks (`listAssetsForTenant` walks
    // `tenant.findFirst` + `asset.findMany`), and assert the fan-out
    // materializes a session_asset row for the resolver-derived pack at
    // the expected mount path and source commit.
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

    await service.stageWorkflowStep({
      agentAddress: AGENT_ADDRESS,
      agentId: AGENT_ID,
      runId: INSTANCE_ID,
      config: MOCK_CONFIG,
      deployContent: MOCK_CONTENT,
      toolPackagePins: [{ name: "tools-resolved", version: "1.0.0" }],
    });

    expect(captured).toHaveLength(1);
    const row = captured[0];
    if (row === undefined) throw new Error("unreachable");
    expect(row.mountPath).toBe(`package-registries/${RESOLVED_ASSET_NAME}/`);
    expect(row.sourceCommitSha).toBe("e".repeat(40));
    expect(row.runId).toBe(INSTANCE_ID);
    expect(row.assetPackSha).toBe(
      hexEncode(
        new Uint8Array(
          await crypto.subtle.digest("SHA-256", new Uint8Array([42, 43, 44])),
        ),
      ),
    );
  });

  test("launchSession rolls back earlier-committed session_asset rows on a later fan-out failure", async () => {
    // Two resolver-derived package-registry attachments, one per
    // registry (routed by scope), so the fan-out has two items. The
    // first attachment pack sends cleanly; the second fails. The
    // second's own catch rolls back its row, and the outer sweep must
    // additionally remove the first attachment's already-committed row.
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "ss-rollback-"));
    const TENANT_ID = "tenant-1";

    // The asset registry keys packuments by each tarball's package.json
    // name, so the tarball filename is irrelevant — only the embedded
    // name/version matters.
    async function buildTarball(pkgName: string): Promise<Uint8Array> {
      const stagingDir = path.join(dir, pkgName.replace(/[@/]/g, "_"));
      const pkgDir = path.join(stagingDir, "package");
      await fs.mkdir(pkgDir, { recursive: true });
      await fs.writeFile(
        path.join(pkgDir, "package.json"),
        JSON.stringify({ name: pkgName, version: "1.0.0" }),
      );
      const tarballPath = path.join(stagingDir, "out.tgz");
      await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
        "package",
      ]);
      return fs.readFile(tarballPath);
    }

    const registries = [
      {
        assetId: "ast_reg_a",
        name: "reg-a",
        scope: "@rega",
        pkg: "@rega/tools",
        pack: new Uint8Array([1, 2, 3]),
      },
      {
        assetId: "ast_reg_b",
        name: "reg-b",
        scope: "@regb",
        pkg: "@regb/tools",
        pack: new Uint8Array([4, 5, 6]),
      },
    ];
    const tarballByAsset = new Map<string, Uint8Array>();
    for (const r of registries) {
      tarballByAsset.set(r.assetId, await buildTarball(r.pkg));
    }

    const assetService: AssetService = {
      createAsset: () => {
        throw new Error("not used");
      },
      populateAsset: () => {
        throw new Error("not used");
      },
      readAssetBlob: async ({ assetId, path: p }) => {
        const t = tarballByAsset.get(assetId);
        if (t === undefined) throw new Error(`unexpected assetId: ${assetId}`);
        if (!p.startsWith("tarballs/")) {
          throw new Error(`unexpected blob path: ${p}`);
        }
        return t;
      },
      listAssetBlobs: async ({ assetId, dir: d }) => {
        if (!tarballByAsset.has(assetId)) {
          throw new Error(`unexpected assetId: ${assetId}`);
        }
        if (d !== "tarballs") throw new Error(`unexpected list dir: ${d}`);
        return ["pkg-1.0.0.tgz"];
      },
    };

    const assetRows = registries.map((r) => ({
      id: r.assetId,
      tenantId: TENANT_ID,
      kind: "package-registry" as const,
      name: r.name,
      displayName: null,
      creatorPrincipalId: null,
      createdAt: new Date(),
      updatedAt: new Date(),
    }));

    const captured: CapturedSessionAssetRow[] = [];
    let deleteCalls = 0;
    const fakeDb = {
      query: {
        tenant: {
          findFirst: async (_args: unknown) =>
            ({ parentId: null }) as { parentId: string | null },
        },
        asset: {
          findMany: async (_args: unknown) => assetRows,
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
            deleteCalls += 1;
            return Promise.resolve();
          },
        };
      },
    };

    const packsByAssetId = new Map<string, FakeAssetPackEntry>(
      registries.map((r) => [
        r.assetId,
        { pack: r.pack, commitSha: "e".repeat(40), ref: "refs/heads/main" },
      ]),
    );
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
        defaultRegistry: "reg-a",
        scopeRouting: registries.map((r) => ({
          scope: r.scope,
          registry: r.name,
        })),
      },
    });

    let err: unknown;
    try {
      await service.stageWorkflowStep({
        agentAddress: AGENT_ADDRESS,
        agentId: AGENT_ID,
        runId: INSTANCE_ID,
        config: MOCK_CONFIG,
        deployContent: MOCK_CONTENT,
        toolPackagePins: registries.map((r) => ({
          name: r.pkg,
          version: "1.0.0",
        })),
      });
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(SessionLaunchError);
    // Both attachment rows are inserted before their pack sends; the
    // second send fails. Its own catch rolls back its row, and the
    // outer rollback sweep removes the first (already-committed) row —
    // at least two delete calls total.
    expect(captured.length).toBeGreaterThanOrEqual(2);
    expect(deleteCalls).toBeGreaterThanOrEqual(2);
  });
});

describe("sendMultiStepDeployFrame", () => {
  test("source-ref arm carries the gate-frozen hash and inert projection verbatim", async () => {
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
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });
    const definition = defineWorkflow({
      id: "wf_sourced",
      trigger: { type: "manual" },
      steps: { only: step({ agent: stubAgent, after: [] }) },
    });
    const sources = {
      only: [
        {
          id: "src-only",
          provider: "anthropic",
          baseURL: "https://api.example/anthropic",
          apiKey: "secret-only",
          model: "mock-model",
        },
      ],
    };
    const config: HarnessConfig = {
      sessionId: "ses-sourced",
      agentId: "ins_dep_src",
      tenantId: "tenant-1",
      principalId: "prin-src",
      agentAddress: "ins_dep_src@workflow.interchange",
      systemPrompt: "deployment-level",
      tools: [],
      grants: [],
      sources: Object.values(sources).flat(),
      defaultSource: "src-only",
    };

    const source = { kind: "registry", registry: "npm" } as const;
    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [],
      entries: [],
    };

    // The gate/freeze layer projects the live definition to its inert
    // needs-surface and hashes THAT; the source-ref arm ships the gate-frozen
    // hash and the pin, not an inline definition. Compute the gate-frozen hash
    // the same way the gate does.
    const inertProjection = projectLiveToInert(definition);
    const frozenWireHash = await computeWireDefinitionHash(inertProjection);

    await sendMultiStepDeployFrame({
      lineage: "source-ref",
      sidecarRouter: mockRouter,
      agentAddress: "ins_dep_src@workflow.interchange",
      config,
      sources,
      approvedWireHash: frozenWireHash,
      sourceRef: { source, closure },
    });

    const sent = sentWorkflows[0];
    if (sent === undefined) throw new Error("missing workflow projection");
    // The gate-frozen hash rides the frame VERBATIM -- the source-ref arm never
    // recomputes it, so the child's re-verify over the closure matches.
    expect(sent.approvedWireHash).toBe(frozenWireHash);
    // The frame carries no inline definition -- the wire type has no
    // `definition` field; the sidecar derives it from the closure the pin
    // materializes. The source-ref pin rides the frame as one co-required object.
    expect(sent.sourceRef).toEqual({ source, closure });
  });
});

describe("deployCodeSourcedWorkflow", () => {
  // Reproduce the approve output the composed entrypoint consumes: run the real
  // gate/freeze over an inert projection (as `installAndApproveWorkflowDefinition`
  // does after a probe) and pair its ok-arm with the frozen closure the pin
  // resolved to. The gate owns the frozen hash and the projection it hashed; the
  // closure is a passthrough. The entrypoint must build a source-ref frame that
  // binds to exactly these values with no recompute or re-resolution.
  async function makeApproveOutput(grants: string[]) {
    const { defineWorkflow, step } = await import("@intx/workflow/definition");
    const { defineAgent } = await import("@intx/agent");
    const { gateAndFreezeProbeResult } = await import("./workflow-probe-gate");
    const stubAgent = defineAgent({
      id: "stub",
      systemPrompt: "you stub",
      tools: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });
    const definition = defineWorkflow({
      id: "wf_composed",
      trigger: { type: "manual" },
      steps: { only: step({ agent: stubAgent, after: [] }) },
    });
    // The gate hashes the inert projection the sidecar ships back; mirror the
    // probe's serialize/validate round-trip so the approve output carries the
    // exact closed `WorkflowProjectionDefinition` shape production hands it.
    const roundTripped: unknown = JSON.parse(
      JSON.stringify(projectLiveToInert(definition)),
    );
    const projection = WorkflowProjectionDefinition(roundTripped);
    if (projection instanceof type.errors) {
      throw new Error(
        `inert projection failed WorkflowProjectionDefinition validation: ${projection.summary}`,
      );
    }
    const wireHash = await computeWireDefinitionHash(projection);
    const approval = await gateAndFreezeProbeResult({
      assetId: "asset-composed",
      probeResult: {
        projection,
        grants,
        grantWalkSnapshot: { perStep: [], grantRequirements: [] },
        wireHash,
      },
      approvals: new Set(grants),
      persist: async () => ({ definitionId: "def-composed" }),
    });
    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [],
      entries: [],
    };
    return { approval, projection, closure, wireHash };
  }

  const SOURCES = {
    only: [
      {
        id: "src-only",
        provider: "anthropic",
        baseURL: "https://api.example/anthropic",
        apiKey: "secret-only",
        model: "mock-model",
      },
    ],
  };
  const DEPLOYMENT_DOMAIN = "workflow.interchange";
  // The deployment's anchor run id and the run address it derives to. The
  // coherence guard in deployCodeSourcedWorkflow asserts they match, so the
  // successful-deploy test passes a consistent pair.
  const ANCHOR_RUN_ID = generateId("workflowRun");
  const DEPLOY_ADDRESS = deriveRunAddress({
    runId: ANCHOR_RUN_ID,
    domain: DEPLOYMENT_DOMAIN,
  });
  const CONFIG: HarnessConfig = {
    sessionId: "ses-composed",
    agentId: "ins_dep_composed",
    tenantId: "tenant-1",
    principalId: "prin-composed",
    agentAddress: DEPLOY_ADDRESS,
    systemPrompt: "deployment-level",
    tools: [],
    grants: [],
    sources: Object.values(SOURCES).flat(),
    defaultSource: "src-only",
  };
  const SOURCE = { kind: "registry", registry: "npm" } as const;
  const TENANT = "tnt_test";
  // These unit tests assert FRAME logic plus the SHAPE of the anchor
  // workflow_run row the composed entrypoint writes (status, self-ref, born
  // null-key) and its post-ack public-key stamp -- not its persistence (the
  // real anchor-in-a-live-DB proof, including the anchor-before-frame ordering,
  // is the tests/db regression test). A capturing db records the anchor insert
  // and the success-path public-key update, and answers the persisted-definition
  // guard's existence query with a matching row; the fail-path tests throw
  // before reaching the insert.
  let capturedAnchorRow: Record<string, unknown> | undefined;
  let capturedAnchorUpdate: Record<string, unknown> | undefined;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub: only the definition existence query, the anchor insert, and the success-path public-key update are exercised
  const CAPTURING_DB = {
    query: {
      workflowDefinition: {
        findFirst: () => Promise.resolve({ id: "def-composed" }),
      },
    },
    insert: () => ({
      values: (row: Record<string, unknown>) => {
        capturedAnchorRow = row;
        return Promise.resolve();
      },
    }),
    update: () => ({
      set: (vals: Record<string, unknown>) => {
        capturedAnchorUpdate = vals;
        return {
          where: () => ({
            returning: () => Promise.resolve([{ id: ANCHOR_RUN_ID }]),
          }),
        };
      },
    }),
  } as unknown as DB["db"];

  test("builds a self-consistent source-ref frame that binds to the gate's frozen hash and inert projection", async () => {
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

    const { deployCodeSourcedWorkflow } = await import("./session-service");
    const { approval, projection, closure, wireHash } = await makeApproveOutput(
      ["tool:read", "tool:write"],
    );

    // Approve surfaces the projection and closure the deploy frame needs.
    if (!approval.ok) throw new Error("expected approval");
    expect(approval.projection).toBe(projection);

    await deployCodeSourcedWorkflow({
      sidecarRouter: mockRouter,
      agentAddress: DEPLOY_ADDRESS,
      config: CONFIG,
      sources: SOURCES,
      approved: { approval, projection, closure },
      source: SOURCE,
      db: CAPTURING_DB,
      tenantId: TENANT,
      anchorRunId: ANCHOR_RUN_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });

    const sent = sentWorkflows[0];
    if (sent === undefined) throw new Error("missing workflow projection");
    // Frame hash == gate frozen hash: the composed entrypoint neither recomputes
    // the hash nor re-resolves the closure, so a downstream child re-verify over
    // the materialized closure would pass. The frame carries no inline
    // definition; the sidecar derives it from the closure the pin materializes.
    expect(sent.approvedWireHash).toBe(wireHash);
    // The composed entrypoint assembles the pin from its `source` arg and the
    // approve output's frozen closure into the frame's one co-required object.
    expect(sent.sourceRef).toEqual({ source: SOURCE, closure });

    // This projection carries no inline onTrigger body, so the frame carries no
    // referencedDefinitions: the field appears only when there is a body to pin.
    expect(sent.referencedDefinitions).toBeUndefined();

    // The anchor workflow_run row is born "deployed" (live but pre-trigger) and
    // self-referential (anchorRunId === id); its address is the run address
    // derived from the anchor run id. It is inserted with a NULL public key --
    // the row must exist before the frame, but the key is only known from the
    // ack -- and the key is stamped by a follow-up update once the ack returns.
    expect(capturedAnchorRow).toBeDefined();
    expect(capturedAnchorRow?.status).toBe("deployed");
    expect(capturedAnchorRow?.id).toBe(ANCHOR_RUN_ID);
    expect(capturedAnchorRow?.anchorRunId).toBe(ANCHOR_RUN_ID);
    expect(capturedAnchorRow?.address).toBe(DEPLOY_ADDRESS);
    expect(capturedAnchorRow?.publicKey).toBeNull();
    expect(capturedAnchorUpdate).toEqual({
      publicKey: "ed25519-supervisor-pubkey",
    });
  });

  test("refuses to deploy when the gate did not approve", async () => {
    const mockRouter = createMockRouter();
    let deployAttempted = false;
    mockRouter.sendAgentDeploy = (() => {
      deployAttempted = true;
      return Promise.resolve({ publicKey: "ed25519-supervisor-pubkey" });
    }) as SidecarRouter["sendAgentDeploy"];

    const { deployCodeSourcedWorkflow } = await import("./session-service");
    const { projection, closure } = await makeApproveOutput([
      "tool:read",
      "tool:write",
    ]);
    // A gate outcome that rejected the advisory grants: no frozen hash exists,
    // so the composed entrypoint must fail closed rather than ship a frame.
    const approval = {
      ok: false as const,
      reason: "grants_not_approved" as const,
      unapprovedGrants: ["tool:escalate"],
    };

    await expect(
      deployCodeSourcedWorkflow({
        sidecarRouter: mockRouter,
        agentAddress: DEPLOY_ADDRESS,
        config: CONFIG,
        sources: SOURCES,
        approved: { approval, projection, closure },
        source: SOURCE,
        db: CAPTURING_DB,
        tenantId: TENANT,
        anchorRunId: ANCHOR_RUN_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
      }),
    ).rejects.toThrow(/unapproved/);
    expect(deployAttempted).toBe(false);
  });

  test("fails closed when the definition carries credential bindings but no credentialCipher is supplied", async () => {
    const mockRouter = createMockRouter();
    let deployAttempted = false;
    mockRouter.sendAgentDeploy = (() => {
      deployAttempted = true;
      return Promise.resolve({ publicKey: "ed25519-supervisor-pubkey" });
    }) as SidecarRouter["sendAgentDeploy"];

    const { deployCodeSourcedWorkflow } = await import("./session-service");
    const { approval, projection, closure } = await makeApproveOutput([
      "tool:read",
    ]);
    if (!approval.ok) throw new Error("expected approval");
    // A projection carrying a credential binding: resolution must run, and with
    // db + tenant present but no credentialCipher it fails closed before any
    // frame is sent (never delivering an unresolvable-credential deployment).
    const withBinding = {
      ...projection,
      credentialBindings: [
        {
          package: "test/tool",
          handle: "api_key",
          provider: "openai",
          locator: "tenant" as const,
        },
      ],
    };

    await expect(
      deployCodeSourcedWorkflow({
        sidecarRouter: mockRouter,
        agentAddress: DEPLOY_ADDRESS,
        config: CONFIG,
        sources: SOURCES,
        approved: { approval, projection: withBinding, closure },
        source: SOURCE,
        db: CAPTURING_DB,
        tenantId: TENANT,
        anchorRunId: ANCHOR_RUN_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
      }),
    ).rejects.toThrow(/no credentialCipher was supplied/);
    expect(deployAttempted).toBe(false);
  });

  // An approve output whose definition carries a single inline onTrigger section
  // body with one tool-less agent step. Mirrors makeApproveOutput's gate/freeze
  // round-trip so the projection is the exact closed shape production hands the
  // deploy hand-off, with the body kept inline (as the inert projector emits).
  async function makeBodyApproveOutput(grants: string[]) {
    const { defineWorkflow, step, onTrigger } = await import(
      "@intx/workflow/definition"
    );
    const { defineAgent } = await import("@intx/agent");
    const { gateAndFreezeProbeResult } = await import("./workflow-probe-gate");
    const bodyAgent = defineAgent({
      id: "composed-body-agent",
      systemPrompt: "you are the composed body agent",
      tools: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });
    const definition = defineWorkflow({
      id: "wf_composed_body",
      trigger: { type: "mail", to: DEPLOY_ADDRESS },
      steps: {
        section: onTrigger({
          on: { type: "mail", to: DEPLOY_ADDRESS },
          body: defineWorkflow({
            id: "authored-body",
            trigger: { type: "manual" },
            steps: { work: step({ agent: bodyAgent }) },
          }),
        }),
      },
    });
    const roundTripped: unknown = JSON.parse(
      JSON.stringify(projectLiveToInert(definition)),
    );
    const projection = WorkflowProjectionDefinition(roundTripped);
    if (projection instanceof type.errors) {
      throw new Error(
        `inert projection failed WorkflowProjectionDefinition validation: ${projection.summary}`,
      );
    }
    const wireHash = await computeWireDefinitionHash(projection);
    const approval = await gateAndFreezeProbeResult({
      assetId: "asset-composed-body",
      probeResult: {
        projection,
        grants,
        grantWalkSnapshot: { perStep: [], grantRequirements: [] },
        wireHash,
      },
      approvals: new Set(grants),
      persist: async () => ({ definitionId: "def-composed-body" }),
    });
    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [],
      entries: [],
    };
    return { approval, projection, closure, wireHash };
  }

  // Like makeBodyApproveOutput, but the inline onTrigger body carries a `loop`
  // whose own body has an agent step. Exercises the per-body pin's recursion
  // into a loop nested inside a spawned body.
  async function makeLoopInBodyApproveOutput(grants: string[]) {
    const { defineWorkflow, step, onTrigger, loop } = await import(
      "@intx/workflow/definition"
    );
    const { defineAgent } = await import("@intx/agent");
    const { gateAndFreezeProbeResult } = await import("./workflow-probe-gate");
    const bodyAgent = defineAgent({
      id: "composed-loop-body-agent",
      systemPrompt: "you are the composed loop body agent",
      tools: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });
    const definition = defineWorkflow({
      id: "wf_composed_loop_body",
      trigger: { type: "mail", to: DEPLOY_ADDRESS },
      steps: {
        section: onTrigger({
          on: { type: "mail", to: DEPLOY_ADDRESS },
          body: defineWorkflow({
            id: "authored-loop-body",
            trigger: { type: "manual" },
            steps: {
              rework: loop({
                body: defineWorkflow({
                  id: "authored-inner-loop",
                  trigger: { type: "manual" },
                  steps: { turn: step({ agent: bodyAgent }) },
                }),
                while: "w",
                carry: "c",
                input: { literal: 0 },
                maxIterations: 3,
                onExhausted: "esc",
              }),
              esc: step({ agent: bodyAgent, after: ["rework"] }),
            },
          }),
        }),
      },
    });
    const roundTripped: unknown = JSON.parse(
      JSON.stringify(projectLiveToInert(definition)),
    );
    const projection = WorkflowProjectionDefinition(roundTripped);
    if (projection instanceof type.errors) {
      throw new Error(
        `inert projection failed WorkflowProjectionDefinition validation: ${projection.summary}`,
      );
    }
    const wireHash = await computeWireDefinitionHash(projection);
    const approval = await gateAndFreezeProbeResult({
      assetId: "asset-composed-loop-body",
      probeResult: {
        projection,
        grants,
        grantWalkSnapshot: { perStep: [], grantRequirements: [] },
        wireHash,
      },
      approvals: new Set(grants),
      persist: async () => ({ definitionId: "def-composed-loop-body" }),
    });
    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [],
      entries: [],
    };
    return { approval, projection, closure, wireHash };
  }

  // Like makeBodyApproveOutput, but the body agent declares NO inference source
  // (an empty `modelSources`). Such an agent still resolves a source at runtime,
  // so the per-body pin must run it through the approval gate, not the never-read
  // placeholder path a genuine non-agent step gets.
  async function makeEmptyAgentBodyApproveOutput(grants: string[]) {
    const { defineWorkflow, step, onTrigger } = await import(
      "@intx/workflow/definition"
    );
    const { defineAgent } = await import("@intx/agent");
    const { gateAndFreezeProbeResult } = await import("./workflow-probe-gate");
    const bodyAgent = defineAgent({
      id: "composed-empty-source-agent",
      systemPrompt: "you are the composed body agent with no declared source",
      tools: [],
      capabilities: [],
      inference: { sources: [] },
    });
    const definition = defineWorkflow({
      id: "wf_composed_empty_body",
      trigger: { type: "mail", to: DEPLOY_ADDRESS },
      steps: {
        section: onTrigger({
          on: { type: "mail", to: DEPLOY_ADDRESS },
          body: defineWorkflow({
            id: "authored-empty-body",
            trigger: { type: "manual" },
            steps: { work: step({ agent: bodyAgent }) },
          }),
        }),
      },
    });
    const roundTripped: unknown = JSON.parse(
      JSON.stringify(projectLiveToInert(definition)),
    );
    const projection = WorkflowProjectionDefinition(roundTripped);
    if (projection instanceof type.errors) {
      throw new Error(
        `inert projection failed WorkflowProjectionDefinition validation: ${projection.summary}`,
      );
    }
    const wireHash = await computeWireDefinitionHash(projection);
    const approval = await gateAndFreezeProbeResult({
      assetId: "asset-composed-empty-body",
      probeResult: {
        projection,
        grants,
        grantWalkSnapshot: { perStep: [], grantRequirements: [] },
        wireHash,
      },
      approvals: new Set(grants),
      persist: async () => ({ definitionId: "def-composed-empty-body" }),
    });
    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [],
      entries: [],
    };
    return { approval, projection, closure, wireHash };
  }

  test("pins and carries per-step inference sources for an inline onTrigger body", async () => {
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

    const { deployCodeSourcedWorkflow } = await import("./session-service");
    // Approve the body agent's declared inference source so the pin resolves it.
    const { approval, projection, closure } = await makeBodyApproveOutput([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${DEPLOY_ADDRESS}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);
    if (!approval.ok) throw new Error("expected approval");

    await deployCodeSourcedWorkflow({
      sidecarRouter: mockRouter,
      agentAddress: DEPLOY_ADDRESS,
      config: CONFIG,
      sources: SOURCES,
      approved: { approval, projection, closure },
      source: SOURCE,
      db: CAPTURING_DB,
      tenantId: TENANT,
      anchorRunId: ANCHOR_RUN_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });

    const sent = sentWorkflows[0];
    if (sent === undefined) throw new Error("missing workflow projection");
    const refs = sent.referencedDefinitions;
    if (refs === undefined) {
      throw new Error("frame carried no referencedDefinitions for the body");
    }
    expect(refs).toHaveLength(1);
    const body = refs[0];
    if (body === undefined) throw new Error("missing referenced body");
    // Staged under the SHARED body ref, so the sidecar path and the run child's
    // re-derived ref (inlineBodyRef(projection.id, stepId)) agree.
    expect(body.definition.id).toBe("wf_composed_body__section");
    // The wire narrow requires a source chain per body stepOrder entry.
    expect(Object.keys(body.sources).sort()).toEqual(
      [...body.definition.stepOrder].sort(),
    );
    // The body agent's declared (anthropic, mock-model) resolved to the
    // operator-approved config source, pinned as a single-element chain.
    expect(body.sources["work"]).toEqual(SOURCES.only);
    // Per-body hash is recomputed from the inert body verbatim, so a body
    // child's re-verify over the re-evaluated closure clears the same barrier.
    expect(body.approvedWireHash).toBe(
      await computeWireDefinitionHash(body.definition),
    );
  });

  test("fails closed when a body step's inference source is not operator-approved", async () => {
    const mockRouter = createMockRouter();
    let deployAttempted = false;
    mockRouter.sendAgentDeploy = (() => {
      deployAttempted = true;
      return Promise.resolve({ publicKey: "ed25519-supervisor-pubkey" });
    }) as SidecarRouter["sendAgentDeploy"];

    const { deployCodeSourcedWorkflow } = await import("./session-service");
    // Approve everything EXCEPT the body agent's inference source. The frozen
    // projection and closure are otherwise valid, but the per-body pin must
    // refuse to resolve a (provider, model) the operator never approved -- and
    // fail closed before any frame is sent, never staging an unapproved source.
    const { approval, projection, closure } = await makeBodyApproveOutput([
      "director:@intx/agent/default",
      `mail.address:${DEPLOY_ADDRESS}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);
    if (!approval.ok) throw new Error("expected approval");

    await expect(
      deployCodeSourcedWorkflow({
        sidecarRouter: mockRouter,
        agentAddress: DEPLOY_ADDRESS,
        config: CONFIG,
        sources: SOURCES,
        approved: { approval, projection, closure },
        source: SOURCE,
        db: CAPTURING_DB,
        tenantId: TENANT,
        anchorRunId: ANCHOR_RUN_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
      }),
    ).rejects.toThrow(/no approved inference source/);
    expect(deployAttempted).toBe(false);
  });

  test("pins a loop nested inside an onTrigger body, recursing into its body", async () => {
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

    const { deployCodeSourcedWorkflow } = await import("./session-service");
    const { approval, projection, closure } = await makeLoopInBodyApproveOutput(
      [
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${DEPLOY_ADDRESS}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
      ],
    );
    if (!approval.ok) throw new Error("expected approval");

    await deployCodeSourcedWorkflow({
      sidecarRouter: mockRouter,
      agentAddress: DEPLOY_ADDRESS,
      config: CONFIG,
      sources: SOURCES,
      approved: { approval, projection, closure },
      source: SOURCE,
      db: CAPTURING_DB,
      tenantId: TENANT,
      anchorRunId: ANCHOR_RUN_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });

    const sent = sentWorkflows[0];
    if (sent === undefined) throw new Error("missing workflow projection");
    const refs = sent.referencedDefinitions;
    if (refs === undefined) {
      throw new Error("frame carried no referencedDefinitions for the body");
    }
    expect(refs).toHaveLength(1);
    const body = refs[0];
    if (body === undefined) throw new Error("missing referenced body");
    // `turn` lives inside the loop body; its presence in the body's source map
    // proves the per-body pin recursed into the loop instead of rejecting it.
    expect(body.sources["turn"]).toEqual(SOURCES.only);
    // The dependent agent step is pinned too.
    expect(body.sources["esc"]).toEqual(SOURCES.only);
    // The loop container is a non-agent step: it gets the never-read placeholder
    // default.
    expect(body.sources["rework"]?.[0]?.id).toBe("src-only");
  });

  test("approval-gates a body agent that declares no source instead of placeholding it", async () => {
    const mockRouter = createMockRouter();
    let deployAttempted = false;
    mockRouter.sendAgentDeploy = (() => {
      deployAttempted = true;
      return Promise.resolve({ publicKey: "ed25519-supervisor-pubkey" });
    }) as SidecarRouter["sendAgentDeploy"];

    const { deployCodeSourcedWorkflow } = await import("./session-service");
    // Approve director + mail but NOT the deploy default's inference source. An
    // agent that declares no preference still resolves the default at runtime, so
    // the pin must approval-check it and fail closed -- it must NOT pin the
    // unapproved default as a never-read placeholder.
    const { approval, projection, closure } =
      await makeEmptyAgentBodyApproveOutput([
        "director:@intx/agent/default",
        `mail.address:${DEPLOY_ADDRESS}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
      ]);
    if (!approval.ok) throw new Error("expected approval");

    await expect(
      deployCodeSourcedWorkflow({
        sidecarRouter: mockRouter,
        agentAddress: DEPLOY_ADDRESS,
        config: CONFIG,
        sources: SOURCES,
        approved: { approval, projection, closure },
        source: SOURCE,
        db: CAPTURING_DB,
        tenantId: TENANT,
        anchorRunId: ANCHOR_RUN_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
      }),
    ).rejects.toThrow(/no approved inference source/);
    expect(deployAttempted).toBe(false);
  });
});
