import { describe, test, expect, beforeEach } from "bun:test";
import type { HarnessConfig } from "@interchange/types/runtime";
import type { AgentRepoStore, DeployContent } from "./agent-repo";
import { createSessionService, SessionLaunchError } from "./session-service";
import type { SidecarRouter } from "./ws/sidecar-handler";

type Call = { method: string; args: unknown[] };

function createMockRouter(): SidecarRouter & { calls: Call[] } {
  const calls: Call[] = [];
  const track =
    (method: string) =>
    (...args: unknown[]) => {
      calls.push({ method, args });
      return Promise.resolve();
    };

  return {
    calls,
    handleOpen: track("handleOpen") as SidecarRouter["handleOpen"],
    handleMessage: track("handleMessage") as SidecarRouter["handleMessage"],
    handleClose: track("handleClose") as SidecarRouter["handleClose"],
    routeMail: (() => true) as SidecarRouter["routeMail"],
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
    sendMessage: track("sendMessage") as SidecarRouter["sendMessage"],
    sendGrantsUpdate: track(
      "sendGrantsUpdate",
    ) as SidecarRouter["sendGrantsUpdate"],
    sendPack: track("sendPack") as SidecarRouter["sendPack"],
    sendSyncRequest: track(
      "sendSyncRequest",
    ) as SidecarRouter["sendSyncRequest"],
    subscribeSession: (() => () =>
      undefined) as SidecarRouter["subscribeSession"],
    getConnectedSidecars: () => [],
    getRoutableAddresses: () => [],
  };
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
      agentId: string,
      _pack: Uint8Array,
      _ref: string,
      _commitSha: string,
    ) {
      calls.push({ method: "receiveStatePack", args: [agentId] });
    },
  };
}

const AGENT_ADDRESS = "agent-1@test.local";
const AGENT_ID = "agent-1";

const MOCK_CONFIG = {
  sessionId: "ses-1",
  agentId: AGENT_ID,
  tenantId: "tenant-1",
  principalId: "prin-1",
  agentAddress: AGENT_ADDRESS,
  systemPrompt: "Test",
  tools: [],
  grants: [],
  providers: [],
  defaultModel: "mock",
} as HarnessConfig;

const MOCK_CONTENT: DeployContent = {
  systemPrompt: "Test",
  skills: [],
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
    const launchErr = err as SessionLaunchError;
    expect(launchErr.phase).toBe("pack");
    expect(launchErr.leakedAgent).toBe(false);

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
    const launchErr = err as SessionLaunchError;
    expect(launchErr.phase).toBe("start");
    expect(launchErr.leakedAgent).toBe(false);

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
    const launchErr = err as SessionLaunchError;
    expect(launchErr.leakedAgent).toBe(true);
    expect(launchErr.phase).toBe("pack");

    // The original error (pack failure) must be preserved as the cause,
    // not the cleanup failure.
    expect(launchErr.cause).toBeInstanceOf(Error);
    expect((launchErr.cause as Error).message).toBe("pack failed");
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
    const launchErr = err as SessionLaunchError;
    expect(launchErr.phase).toBe("write");
    expect(launchErr.leakedAgent).toBe(false);
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
    const launchErr = err as SessionLaunchError;
    expect(launchErr.phase).toBe("provision");
    expect(launchErr.leakedAgent).toBe(false);

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
});
