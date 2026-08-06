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
import { projectLiveToInert } from "@intx/workflow-deploy";
import { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import type { ToolPackageManifest } from "@intx/types/tool-packages";
import { extractAttachments } from "@intx/mime";
import {
  asset as assetTable,
  grant as grantTable,
  sessionAsset as sessionAssetTable,
  workflowDefinition as workflowDefinitionTable,
  workflowDefinitionVersion as workflowDefinitionVersionTable,
  workflowRun as workflowRunTable,
} from "@intx/db/schema";
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
  instanceId: string;
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
      instanceId: INSTANCE_ID,
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
      instanceId: INSTANCE_ID,
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
        instanceId: INSTANCE_ID,
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
                    return Promise.resolve([{ instanceId: row.instanceId }]);
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
        instanceId: INSTANCE_ID,
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
      instanceId: INSTANCE_ID,
      config: MOCK_CONFIG,
      deployContent: MOCK_CONTENT,
      toolPackagePins: [{ name: "tools-resolved", version: "1.0.0" }],
    });

    expect(captured).toHaveLength(1);
    const row = captured[0];
    if (row === undefined) throw new Error("unreachable");
    expect(row.mountPath).toBe(`package-registries/${RESOLVED_ASSET_NAME}/`);
    expect(row.sourceCommitSha).toBe("e".repeat(40));
    expect(row.instanceId).toBe(INSTANCE_ID);
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
        instanceId: INSTANCE_ID,
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

describe("deployWorkflowDefinition", () => {
  type CapturedGrantRow = {
    principalId: string | null;
    resource: string;
    action: string;
    effect: string;
  };

  type CapturedRunRow = {
    id: string;
    tenantId: string;
    deploymentId: string;
    address: string;
    status: string;
    definitionId?: string | null;
    principalId?: string | null;
  };

  type CapturedDefinitionRow = {
    id: string;
    tenantId: string;
    assetId: string;
    name: string;
  };

  function createWorkflowDeployFixture() {
    const grantRows: CapturedGrantRow[] = [];
    const runRows: CapturedRunRow[] = [];
    const definitionRows: CapturedDefinitionRow[] = [];
    const definitionVersionRows: { definitionId: string; version: string }[] =
      [];
    const workflowRepoWrites: { repoId: RepoId; files: string[] }[] = [];

    // The workflow asset the definition is projected from. `ensureWorkflow-
    // DefinitionForAsset` selects it by id to shape the definition row.
    const assetRow = {
      tenantId: "tenant-1",
      creatorPrincipalId: "prin-creator",
      name: "wf-asset",
      displayName: "WF Asset",
    };

    const insert = (table: unknown) => {
      if (table === grantTable) {
        return {
          values(row: CapturedGrantRow) {
            grantRows.push(row);
            return Promise.resolve();
          },
        };
      }
      if (table === workflowRunTable) {
        return {
          values(row: CapturedRunRow) {
            runRows.push(row);
            return Promise.resolve();
          },
        };
      }
      if (table === workflowDefinitionTable) {
        return {
          values(row: CapturedDefinitionRow) {
            return {
              onConflictDoNothing() {
                return {
                  returning() {
                    definitionRows.push(row);
                    return Promise.resolve([{ id: row.id }]);
                  },
                };
              },
            };
          },
        };
      }
      if (table === workflowDefinitionVersionTable) {
        return {
          values(row: { definitionId: string; version: string }) {
            return {
              onConflictDoNothing() {
                definitionVersionRows.push(row);
                return Promise.resolve();
              },
            };
          },
        };
      }
      throw new Error("deployWorkflowDefinition fixture: unexpected insert");
    };

    // `ensureWorkflowDefinitionForAsset` selects the workflow asset by id; every
    // other select in the deploy path is served elsewhere, so only the asset
    // table is answered here.
    const select = (_projection: unknown) => ({
      from: (table: unknown) => ({
        where: () => ({
          limit: () => Promise.resolve(table === assetTable ? [assetRow] : []),
        }),
      }),
    });

    // The anchor run, its definition, and the run-read grant are written
    // inside a single `db.transaction`. The fixture passes a `tx` exposing the
    // same `insert`/`select` surface so every write is captured, mirroring
    // how the production code commits them atomically.
    const fakeDb = {
      insert,
      select,
      transaction(
        fn: (tx: {
          insert: typeof insert;
          select: typeof select;
        }) => Promise<void>,
      ) {
        return fn({ insert, select });
      },
    };

    const writingRepoStore: RepoStore = {
      ...unusedRepoStore(),
      async writeTree(
        _principal: Principal,
        repoId: RepoId,
        _ref: string,
        content: { files: Record<string, string | Uint8Array> },
      ) {
        workflowRepoWrites.push({
          repoId,
          files: Object.keys(content.files),
        });
        return {
          commitSha: "wfcommit" + "0".repeat(32),
          newlyTerminalRuns: [],
        };
      },
    };

    const repoStore = createMockRepoStore();
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- replace the throwing stub with a writeTree-recording substrate
    (repoStore as unknown as { repoStore: RepoStore }).repoStore =
      writingRepoStore;

    return {
      grantRows,
      runRows,
      definitionRows,
      definitionVersionRows,
      workflowRepoWrites,
      fakeDb,
      repoStore,
    };
  }

  test("deploys a multi-step workflow with an awaitSignal step on the multi-step branch and records the anchor run", async () => {
    const {
      grantRows,
      runRows,
      definitionRows,
      definitionVersionRows,
      workflowRepoWrites,
      fakeDb,
      repoStore,
    } = createWorkflowDeployFixture();
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

    const { defineWorkflow, step, awaitSignal } = await import(
      "@intx/workflow/definition"
    );
    const { defineAgent } = await import("@intx/agent");
    const stubAgent = defineAgent({
      id: "planner",
      systemPrompt: "plan it",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });
    const definition = defineWorkflow({
      id: "wf_deploy",
      trigger: { type: "manual" },
      steps: {
        plan: step({ agent: stubAgent, after: [] }),
        wait: awaitSignal({ name: "go", after: ["plan"] }),
      },
    });

    const sources = [
      {
        id: "src-plan",
        provider: "anthropic",
        baseURL: "https://api.example/anthropic",
        apiKey: "secret-plan",
        model: "mock-model",
      },
    ];
    const config: HarnessConfig = {
      sessionId: "ses-deploy",
      agentId: "ins_dep_xyz",
      tenantId: "tenant-1",
      principalId: "prin-deploy",
      agentAddress: "ins_dep_xyz@workflow.test",
      systemPrompt: "deployment-level",
      tools: [],
      grants: [],
      sources,
      defaultSource: "src-plan",
    };

    const service = createSessionService({
      sidecarRouter: mockRouter,
      agentRepoStore: repoStore,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- fake db only needs insert().values()
      db: fakeDb as unknown as NonNullable<
        Parameters<typeof createSessionService>[0]["db"]
      >,
    });

    const result = await service.deployWorkflowDefinition({
      tenantId: "tenant-1",
      deploymentId: "dep_xyz",
      deploymentDomain: "workflow.test",
      definition,
      definitionAssetId: "ast_workflow_1",
      config,
      deployContent: { systemPrompt: "deployment-level" },
    });

    // Multi-step branch: each step is provisioned via a per-step
    // sendAgentDeploy without a workflow field, then a single
    // deployment-level agent.deploy frame carries the workflow
    // projection.
    const workflowFrames = sentWorkflows.filter(
      (w): w is NonNullable<typeof w> => w !== undefined,
    );
    expect(workflowFrames).toHaveLength(1);
    const sent = workflowFrames[0];
    if (sent === undefined) throw new Error("missing workflow projection");
    expect(sent.definition.id).toBe("wf_deploy");
    expect(sent.definition.stepOrder).toEqual(["plan", "wait"]);
    // The awaitSignal step survives into the deployed definition, so the
    // run reaches an awaiting-signal state once the supervisor schedules it.
    expect(Object.keys(sent.definition.steps).sort()).toEqual(["plan", "wait"]);

    // The workflow repo tree is written before the deployment-level frame.
    expect(workflowRepoWrites).toHaveLength(1);
    const write = workflowRepoWrites[0];
    if (write === undefined) throw new Error("missing workflow repo write");
    expect(write.repoId).toEqual({ kind: "workflow", id: "wf_deploy" });
    expect(write.files.sort()).toEqual([
      ".gitignore",
      "capability-declarations.json",
      "workflow.json",
    ]);

    // The deploy projects a first-class definition (create-if-absent) and its
    // version "1" over the workflow asset, so the anchor run can carry it.
    expect(definitionRows).toHaveLength(1);
    const definitionRow = definitionRows[0];
    if (definitionRow === undefined) {
      throw new Error("missing workflow_definition");
    }
    expect(definitionRow.assetId).toBe("ast_workflow_1");
    expect(definitionVersionRows).toHaveLength(1);
    expect(definitionVersionRows[0]?.definitionId).toBe(definitionRow.id);
    expect(definitionVersionRows[0]?.version).toBe("1");

    // The deployment's anchor run is recorded in the same transaction: one
    // workflow_run 1:1 with the deployment, its id and routing address both
    // derived from the deployment, born running with no key yet (deploy-ack
    // fills it). It carries the just-projected definition so the run anchors on
    // a first-class definition; principalId is left unset.
    expect(runRows).toHaveLength(1);
    const runRow = runRows[0];
    if (runRow === undefined) throw new Error("missing anchor workflow_run");
    expect(runRow.id).toBe("dep_xyz");
    expect(runRow.tenantId).toBe("tenant-1");
    expect(runRow.deploymentId).toBe("dep_xyz");
    expect(runRow.address).toBe("ins_dep_xyz@workflow.test");
    expect(runRow.status).toBe("running");
    expect(runRow.definitionId).toBe(definitionRow.id);
    expect(runRow.principalId ?? null).toBeNull();

    // A read grant on the deployment's workflow-run resource is seeded
    // for the deploying principal so they can observe run events.
    expect(grantRows).toHaveLength(1);
    const grantRow = grantRows[0];
    if (grantRow === undefined) throw new Error("missing workflow-run grant");
    expect(grantRow.principalId).toBe("prin-deploy");
    expect(grantRow.resource).toBe("workflow-run:dep_xyz");
    expect(grantRow.action).toBe("read");
    expect(grantRow.effect).toBe("allow");

    expect(result).toEqual({
      deploymentId: "dep_xyz",
      deploymentAddress: "ins_dep_xyz@workflow.test",
      publicKey: "ed25519-supervisor-pubkey",
    });
  });
});

describe("deployPreparedWorkflowDefinition recovery", () => {
  async function createPreparedDeployFixture() {
    const allocationRouter = createMockAllocationRouter();
    const repoStore = createMockRepoStore();
    const restoreSha = "f".repeat(40);
    const substrate: RepoStore = {
      ...unusedRepoStore(),
      async resolveRef(_principal, repoId, ref) {
        return repoId.kind === "workflow-run" && ref === "refs/heads/main"
          ? restoreSha
          : null;
      },
      async createPack(_principal, repoId, ref) {
        if (repoId.kind !== "workflow-run" || ref !== "refs/heads/main") {
          throw new Error(`unexpected restore pack ${repoId.kind}/${ref}`);
        }
        return {
          pack: new Uint8Array([9, 8, 7]),
          commitSha: restoreSha,
          ref,
        };
      },
      async writeTree() {
        return {
          commitSha: "e".repeat(40),
          newlyTerminalRuns: [],
        };
      },
    };
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- replace the throwing substrate with the narrow restore/workflow-writer fixture
    (repoStore as unknown as { repoStore: RepoStore }).repoStore = substrate;

    const allocationState = {
      id: "alloc-restore",
      anchorRunId: "dep_restore_order",
      status: "allocated",
      generation: 3,
      ensureAcceptedGeneration: 3,
    };
    const fakeTx = {
      select() {
        return {
          from() {
            return {
              where() {
                return {
                  limit() {
                    return {
                      for: async () => [allocationState],
                    };
                  },
                };
              },
            };
          },
        };
      },
      update(table: unknown) {
        if (table !== workflowRunTable) {
          throw new Error("unexpected prepared-deploy update table");
        }
        return {
          set(_values: unknown) {
            return {
              where(_predicate: unknown) {
                return {
                  returning() {
                    return Promise.resolve([{ id: "dep_restore_order" }]);
                  },
                };
              },
            };
          },
        };
      },
    };
    const fakeDb = {
      ...fakeTx,
      transaction: async <T>(callback: (tx: typeof fakeTx) => Promise<T>) =>
        callback(fakeTx),
    };

    const { defineAgent } = await import("@intx/agent");
    const { defineWorkflow } = await import("@intx/workflow/definition");
    const source = {
      id: "source-restore",
      provider: "anthropic",
      baseURL: "https://api.example/anthropic",
      apiKey: "secret",
      model: "mock-model",
    };
    const definition = defineWorkflow({
      id: "wf_restore_order",
      trigger: { type: "manual" },
      agent: defineAgent({
        id: "restore-agent",
        systemPrompt: "continue the recovered run",
        tools: [],
        capabilities: [],
        inference: {
          sources: [{ provider: "anthropic", model: "mock-model" }],
        },
      }),
    });
    const params = {
      tenantId: "tenant-1",
      deploymentId: "dep_restore_order",
      deploymentDomain: "workflow.test",
      definition,
      config: {
        sessionId: "ses-restore-order",
        agentId: "ins_dep_restore_order",
        tenantId: "tenant-1",
        principalId: "principal-1",
        agentAddress: "ins_dep_restore_order@workflow.test",
        systemPrompt: "continue the recovered run",
        tools: [],
        grants: [],
        sources: [source],
        defaultSource: source.id,
      } satisfies HarnessConfig,
      deployContent: { systemPrompt: "continue the recovered run" },
      allocationTarget: { allocationId: "alloc-restore", generation: 3 },
    };
    const service = createSessionService({
      sidecarRouter: createMockRouter(),
      sidecarAllocationRouter: allocationRouter,
      agentRepoStore: repoStore,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- fake db implements the prepared anchor public-key update only
      db: fakeDb as unknown as NonNullable<
        Parameters<typeof createSessionService>[0]["db"]
      >,
    });

    return { allocationRouter, allocationState, params, repoStore, service };
  }

  test("acknowledges restored history before sending the frame that spawns the supervisor", async () => {
    const { allocationRouter, params, service } =
      await createPreparedDeployFixture();

    await service.deployPreparedWorkflowDefinition(params);

    const methods = allocationRouter.calls.map((call) => call.method);
    const restoreIndex = methods.indexOf("sendWorkflowRunPackToAllocation");
    const spawnIndex = methods.indexOf("sendAgentDeployToAllocation");
    expect(restoreIndex).toBeGreaterThanOrEqual(0);
    expect(spawnIndex).toBeGreaterThan(restoreIndex);
  });

  test("does not stage or spawn the workflow when history restoration fails", async () => {
    const { allocationRouter, params, repoStore, service } =
      await createPreparedDeployFixture();
    allocationRouter.sendWorkflowRunPackToAllocation = async (...args) => {
      allocationRouter.calls.push({
        method: "sendWorkflowRunPackToAllocation",
        args,
      });
      throw new Error("restore rejected");
    };

    await expect(
      service.deployPreparedWorkflowDefinition(params),
    ).rejects.toThrow("restore rejected");
    expect(allocationRouter.calls.map((call) => call.method)).toEqual([
      "sendWorkflowRunPackToAllocation",
    ]);
    expect(repoStore.calls).toEqual([]);
  });

  test("marks an allocated pack failure as a leaked supervisor", async () => {
    const { allocationRouter, params, service } =
      await createPreparedDeployFixture();
    allocationRouter.sendPackToAllocation = async (...args) => {
      allocationRouter.calls.push({ method: "sendPackToAllocation", args });
      throw new Error("deploy pack failed");
    };

    const error = await service
      .deployPreparedWorkflowDefinition(params)
      .catch((cause: unknown) => cause);

    expect(error).toBeInstanceOf(SessionLaunchError);
    if (!(error instanceof SessionLaunchError)) throw new Error("unreachable");
    expect(error.phase).toBe("pack");
    expect(error.leakedAgent).toBe(true);
  });

  test("rejects a stale generation before publishing its supervisor key", async () => {
    const { allocationRouter, allocationState, params, service } =
      await createPreparedDeployFixture();
    const sendDeploy = allocationRouter.sendAgentDeployToAllocation;
    allocationRouter.sendAgentDeployToAllocation = async (...args) => {
      const result = await sendDeploy(...args);
      allocationState.generation = 4;
      allocationState.ensureAcceptedGeneration = 4;
      return result;
    };

    const error = await service
      .deployPreparedWorkflowDefinition(params)
      .catch((cause: unknown) => cause);

    expect(error).toBeInstanceOf(SessionLaunchError);
    if (!(error instanceof SessionLaunchError)) throw new Error("unreachable");
    expect(error.phase).toBe("start");
    expect(error.leakedAgent).toBe(true);
    expect(error.message).toContain("lost allocation ownership");
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
    // Each step's value is its ordered inference-source failover chain.
    const sources = {
      plan: [
        {
          id: "src-plan",
          provider: "anthropic",
          baseURL: "https://api.example/anthropic",
          apiKey: "secret-plan",
          model: "mock-model",
        },
      ],
      execute: [
        {
          id: "src-execute",
          provider: "anthropic",
          baseURL: "https://api.example/anthropic",
          apiKey: "secret-execute",
          model: "mock-model",
        },
      ],
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
      sources: Object.values(sources).flat(),
      defaultSource: "src-plan",
    };

    const result = await sendMultiStepDeployFrame({
      lineage: "live",
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
    // The hub stamps the approved wire hash on every frame it builds: it is the
    // hash of the wire projection the frame carries, so the sidecar can feed it
    // to the child as DEFINITION_HASH without recomputing.
    expect(sent.approvedWireHash).toBe(
      await computeWireDefinitionHash(sent.definition),
    );
  });

  test("source-ref arm carries the gate-frozen hash and inert projection verbatim and materializes deploy grants from the frozen approved set", async () => {
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
    // The operator-approved (frozen) grant set. A candidate carrying an extra
    // grant the freeze never approved must be dropped, so the shipped set is a
    // strict subset of the frozen set.
    const frozenApprovedGrants = new Set(["tool:read", "tool:write"]);
    const deployGrantCandidates = ["tool:read", "tool:write", "tool:delete"];

    // The gate/freeze layer projects the live definition to its inert
    // needs-surface and hashes THAT; the sidecar probe serializes the inert
    // projection to the wire and the hub validates it back to a closed
    // `WorkflowProjectionDefinition`. Mirror that round-trip so the source-ref
    // arm receives exactly the probe-result shape it gets in production, and
    // compute the gate-frozen hash the same way the gate does.
    const inertProjection = projectLiveToInert(definition);
    const frozenWireHash = await computeWireDefinitionHash(inertProjection);
    const roundTripped: unknown = JSON.parse(JSON.stringify(inertProjection));
    const projection = WorkflowProjectionDefinition(roundTripped);
    if (projection instanceof type.errors) {
      throw new Error(
        `inert projection failed WorkflowProjectionDefinition validation: ${projection.summary}`,
      );
    }

    await sendMultiStepDeployFrame({
      lineage: "source-ref",
      sidecarRouter: mockRouter,
      agentAddress: "ins_dep_src@workflow.interchange",
      config,
      sources,
      projection,
      approvedWireHash: frozenWireHash,
      source,
      closure,
      frozenApprovedGrants,
      deployGrantCandidates,
    });

    const sent = sentWorkflows[0];
    if (sent === undefined) throw new Error("missing workflow projection");
    // The gate-frozen hash rides the frame VERBATIM -- the source-ref arm never
    // recomputes it, so the child's re-verify over the inert projection matches.
    expect(sent.approvedWireHash).toBe(frozenWireHash);
    // The inert projection itself is carried on the frame's `definition`
    // unchanged -- no live wire lineage leaks onto the frame.
    expect(sent.definition).toEqual(projection);
    expect(sent.source).toEqual(source);
    expect(sent.closure).toEqual(closure);
    // Deploy grants are the frozen subset: the unapproved `tool:delete` is
    // refused, so only the frozen-approved grants ride the frame.
    expect(sent.approvedDeployGrants?.slice().sort()).toEqual([
      "tool:read",
      "tool:write",
    ]);
  });

  test("a live definition and its inert projection hash differently so binding to the inert projection is load-bearing", async () => {
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
      id: "wf_divergent",
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
      sessionId: "ses-divergent",
      agentId: "ins_dep_div",
      tenantId: "tenant-1",
      principalId: "prin-div",
      agentAddress: "ins_dep_div@workflow.interchange",
      systemPrompt: "deployment-level",
      tools: [],
      grants: [],
      sources: Object.values(sources).flat(),
      defaultSource: "src-only",
    };

    // The live-authored arm stamps the hash of the LIVE wire lineage.
    await sendMultiStepDeployFrame({
      lineage: "live",
      sidecarRouter: mockRouter,
      agentAddress: "ins_dep_div@workflow.interchange",
      config,
      definition,
      sources,
    });
    const sent = sentWorkflows[0];
    if (sent === undefined) throw new Error("missing workflow projection");
    const liveWireHash = sent.approvedWireHash;
    if (liveWireHash === undefined) {
      throw new Error("live-authored arm did not stamp a wire hash");
    }

    // The gate freezes over the INERT projection. The live wire lineage and the
    // inert projection diverge (the live agent carries `inference`, the inert
    // projection carries `modelSources`), so a source-ref deploy MUST bind to
    // the inert hash -- binding to the live recompute would fail the child
    // re-verify closed.
    const inertHash = await computeWireDefinitionHash(
      projectLiveToInert(definition),
    );
    expect(liveWireHash).not.toBe(inertHash);
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
      probeResult: { projection, grants, wireHash },
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
  const CONFIG: HarnessConfig = {
    sessionId: "ses-composed",
    agentId: "ins_dep_composed",
    tenantId: "tenant-1",
    principalId: "prin-composed",
    agentAddress: "ins_dep_composed@workflow.interchange",
    systemPrompt: "deployment-level",
    tools: [],
    grants: [],
    sources: Object.values(SOURCES).flat(),
    defaultSource: "src-only",
  };
  const SOURCE = { kind: "registry", registry: "npm" } as const;

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
      agentAddress: "ins_dep_composed@workflow.interchange",
      config: CONFIG,
      sources: SOURCES,
      approved: { approval, projection, closure },
      source: SOURCE,
    });

    const sent = sentWorkflows[0];
    if (sent === undefined) throw new Error("missing workflow projection");
    // Frame hash == gate frozen hash == recompute over the frame's inert
    // definition: the composed entrypoint neither recomputes the hash nor
    // re-resolves the closure, so a downstream child re-verify would pass.
    expect(sent.approvedWireHash).toBe(wireHash);
    if (sent.definition === undefined) {
      throw new Error("frame carried no definition");
    }
    expect(await computeWireDefinitionHash(sent.definition)).toBe(wireHash);
    expect(sent.definition).toEqual(projection);
    expect(sent.source).toEqual(SOURCE);
    expect(sent.closure).toEqual(closure);
    // Deploy grants trace to the freeze: the full frozen approved set rides the
    // frame when no narrower candidate set is supplied.
    expect(sent.approvedDeployGrants?.slice().sort()).toEqual([
      "tool:read",
      "tool:write",
    ]);
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
        agentAddress: "ins_dep_composed@workflow.interchange",
        config: CONFIG,
        sources: SOURCES,
        approved: { approval, projection, closure },
        source: SOURCE,
      }),
    ).rejects.toThrow(/unapproved/);
    expect(deployAttempted).toBe(false);
  });
});

describe("deployInstanceAtHead inference-source pinning", () => {
  const MULTI_SOURCE_CONFIG: HarnessConfig = {
    ...MOCK_CONFIG,
    sources: [
      {
        id: "primary",
        provider: "anthropic",
        baseURL: "https://api.example/anthropic",
        apiKey: "sk-primary",
        model: "claude-mock",
      },
      {
        id: "failover",
        provider: "openai",
        baseURL: "https://api.example/openai",
        apiKey: "sk-failover",
        model: "gpt-mock",
      },
    ],
    defaultSource: "primary",
  };

  test("pins the instance's full ordered source chain to the sole step", async () => {
    const router = createMockRouter();
    // Capture the workflow frame off `sendAgentDeploy` with its typed
    // signature rather than casting the tracker's `unknown[]` args.
    const sentWorkflows: Parameters<SidecarRouter["sendAgentDeploy"]>[2][] = [];
    router.sendAgentDeploy = ((
      _agentAddress: string,
      _config: HarnessConfig,
      workflow?: Parameters<SidecarRouter["sendAgentDeploy"]>[2],
    ) => {
      sentWorkflows.push(workflow);
      return Promise.resolve({ publicKey: "mock-public-key" });
    }) as SidecarRouter["sendAgentDeploy"];
    const repoStore = createMockRepoStore();
    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    await service.deployInstanceAtHead({
      agentAddress: AGENT_ADDRESS,
      agentId: AGENT_ID,
      instanceId: INSTANCE_ID,
      config: MULTI_SOURCE_CONFIG,
      deployContent: MOCK_CONTENT,
    });

    const workflow = sentWorkflows[0];
    if (workflow === undefined) {
      throw new Error("sendAgentDeploy was not called with a workflow frame");
    }
    const chains = Object.values(workflow.sources);
    // A single-step-at-head deploy has exactly one step, so one chain.
    expect(chains).toHaveLength(1);
    // The reactor fails over forward across the whole chain, so the head
    // pins `config.sources` verbatim rather than only the default source.
    expect(chains[0]).toEqual(MULTI_SOURCE_CONFIG.sources);
  });

  test("rejects a config whose first source is not the default source", async () => {
    const router = createMockRouter();
    const repoStore = createMockRepoStore();
    const service = createSessionService({
      sidecarRouter: router,
      agentRepoStore: repoStore,
    });

    // The reactor resolves its initial source by `defaultSource` and fails
    // over forward with no wrap; a default that is not element 0 would leave
    // the head unreachable, so the deploy must fail loudly.
    const misordered: HarnessConfig = {
      ...MULTI_SOURCE_CONFIG,
      defaultSource: "failover",
    };
    await expect(
      service.deployInstanceAtHead({
        agentAddress: AGENT_ADDRESS,
        agentId: AGENT_ID,
        instanceId: INSTANCE_ID,
        config: misordered,
        deployContent: MOCK_CONTENT,
      }),
    ).rejects.toThrow(/must be the default source/);

    expect(router.calls.some((c) => c.method === "sendAgentDeploy")).toBe(
      false,
    );
  });
});
