import { describe, test, expect, beforeEach } from "bun:test";
import type { CryptoProvider, HarnessConfig } from "@intx/types/runtime";
import type { AgentRepoStore, DeployContent } from "./agent-repo";
import {
  createSessionService,
  SessionLaunchError,
  type UserMessageParams,
} from "./session-service";
import type { SidecarRouter } from "./ws/sidecar-handler";
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
  const mock: SidecarRouter & { calls: Call[]; routeMailResult: boolean } = {
    calls,
    routeMailResult: true,
    handleOpen: track("handleOpen") as SidecarRouter["handleOpen"],
    handleMessage: track("handleMessage") as SidecarRouter["handleMessage"],
    handleClose: track("handleClose") as SidecarRouter["handleClose"],
    routeMail(agentAddress: string, rawMessage: string): boolean {
      calls.push({ method: "routeMail", args: [agentAddress, rawMessage] });
      return mock.routeMailResult;
    },
    sendAgentDeploy: track(
      "sendAgentDeploy",
    ) as SidecarRouter["sendAgentDeploy"],
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
    sendPack: track("sendPack") as SidecarRouter["sendPack"],
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
    async writeDeployTree(agentId: string, _content: DeployContent) {
      calls.push({ method: "writeDeployTree", args: [agentId] });
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
    async receiveStatePack(
      repoId: { kind: "agent-state"; id: string },
      _pack: Uint8Array,
      _ref: string,
      _commitSha: string,
    ) {
      calls.push({ method: "receiveStatePack", args: [repoId.id] });
    },
    getSigningPublicKey() {
      return new Uint8Array(32);
    },
    getDeployRef: (_agentId: string) => Promise.resolve(null),
  };
}

const AGENT_ADDRESS = "agent-1@test.local";
const AGENT_ID = "agent-1";

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
});
