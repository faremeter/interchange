import { describe, expect, test } from "bun:test";

import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import {
  createSidecarRouter,
  type SidecarAuthIdentity,
  type WsHandle,
} from "./sidecar-handler";

const identity: Extract<SidecarAuthIdentity, { kind: "allocated" }> = {
  kind: "allocated",
  sidecarId: "sc-allocated",
  allocationId: "alloc-1",
  tenantId: "tenant-1",
  anchorRunId: "run-anchor",
  workflowRunAddress: "workflow@exclusive",
  generation: 1,
};

const target = { allocationId: identity.allocationId, generation: 1 };
const config = {
  sessionId: "ses-exclusive",
  agentId: "workflow",
  tenantId: identity.tenantId,
  principalId: "principal-1",
  agentAddress: identity.workflowRunAddress,
  systemPrompt: "test",
  tools: [],
  grants: [],
  sources: [
    {
      id: "anthropic:test",
      provider: "anthropic",
      baseURL: "https://api.example.test",
      credentialId: "test-credential",
      model: "test",
    },
  ],
  defaultSource: "anthropic:test",
};

function createMockWs(): WsHandle & { sent: string[]; closed: boolean } {
  return {
    sent: [],
    closed: false,
    send(data: string) {
      this.sent.push(data);
    },
    close() {
      this.closed = true;
    },
  };
}

function lastFrame(
  ws: ReturnType<typeof createMockWs>,
): Record<string, unknown> {
  const raw = ws.sent.at(-1);
  if (raw === undefined) throw new Error("No frame was sent");
  return JSON.parse(raw);
}

async function tick(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

function createAllocatedRouter(
  overrides: Partial<Extract<SidecarAuthIdentity, { kind: "allocated" }>> = {},
) {
  const resolved = { ...identity, ...overrides };
  const router = createSidecarRouter({
    authenticateSidecar: async () => resolved,
    validateSidecarIdentity: async () => true,
    hubPublicKey: "a".repeat(64),
    requestTimeoutMs: 500,
    mailAckRetryIntervalMs: 10_000,
  });
  router.fenceAllocation(resolved.allocationId, resolved.generation);
  return router;
}

async function connect(
  router: ReturnType<typeof createSidecarRouter>,
  agentAddresses: string[] = [],
) {
  const ws = createMockWs();
  router.handleOpen(ws);
  router.handleMessage(
    ws,
    JSON.stringify({
      type: "register",
      sidecarId: identity.sidecarId,
      token: "token",
      agentAddresses,
    }),
  );
  await tick();
  return ws;
}

describe("SidecarRouter allocation routing", () => {
  test("rejects a worker whose allocation generation is not fenced", async () => {
    const router = createSidecarRouter({
      authenticateSidecar: async () => identity,
      validateSidecarIdentity: async () => true,
    });
    const ws = await connect(router);

    expect(ws.closed).toBe(true);
    expect(router.getConnectedSidecars()).toEqual([]);
  });

  test("registers only the exact allocation address", async () => {
    const router = createAllocatedRouter();
    const ws = await connect(router, [identity.workflowRunAddress]);

    expect(ws.closed).toBe(false);
    expect(router.getConnectedSidecars()).toEqual([identity.sidecarId]);
    expect(router.getRoutableAddresses()).toEqual([
      identity.workflowRunAddress,
    ]);

    const rogue = createAllocatedRouter();
    const rogueWs = await connect(rogue, ["other@tenant"]);
    expect(rogueWs.closed).toBe(true);
    expect(rogue.getRoutableAddresses()).toEqual([]);
  });

  test("reconciles credentials for newly routed run addresses", async () => {
    const resynced: string[] = [];
    const runAddress = "run_alloc1@exclusive";
    const router = createSidecarRouter({
      authenticateSidecar: async () => ({
        ...identity,
        workflowRunAddress: runAddress,
      }),
      validateSidecarIdentity: async () => true,
      hubPublicKey: "a".repeat(64),
      requestTimeoutMs: 500,
      lookups: {
        resyncCredentials: (addr) => resynced.push(addr),
      },
    });
    router.fenceAllocation(identity.allocationId, identity.generation);
    const ws = await connect(router, [runAddress]);

    expect(ws.closed).toBe(false);
    expect(resynced).toEqual([runAddress]);
  });

  test("deploys only through the exact allocation target", async () => {
    const router = createAllocatedRouter();
    const ws = await connect(router);
    const deployed = router.sendAgentDeployToAllocation(
      target,
      identity.workflowRunAddress,
      config,
    );
    await tick();
    expect(lastFrame(ws).type).toBe("agent.deploy");

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.deploy.ack",
        agentAddress: identity.workflowRunAddress,
        publicKey: "b".repeat(64),
      }),
    );

    await expect(deployed).resolves.toEqual({ publicKey: "b".repeat(64) });
    await expect(
      router.sendAgentDeployToAllocation(
        { ...target, generation: 2 },
        identity.workflowRunAddress,
        config,
      ),
    ).rejects.toThrow("is not current");
  });

  test("restores workflow history without making the address routable", async () => {
    const router = createAllocatedRouter();
    const ws = await connect(router);
    const restored = router.sendWorkflowRunPackToAllocation(
      target,
      identity.workflowRunAddress,
      new Uint8Array([1, 2, 3]),
      "refs/heads/events",
      "d".repeat(40),
    );
    await tick();

    const frame = lastFrame(ws);
    expect(frame).toMatchObject({
      type: "repo.pack.done",
      agentAddress: identity.workflowRunAddress,
      repoId: {
        kind: "workflow-run",
        id: deriveWorkflowRunRepoId(identity.workflowRunAddress),
      },
    });
    expect(router.getRoutableAddresses()).toEqual([]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "repo.pack.ack",
        agentAddress: identity.workflowRunAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
      }),
    );
    await expect(restored).resolves.toBeUndefined();
  });

  test("redelivers retained mail once when the generation reconnects", async () => {
    const router = createAllocatedRouter();
    const first = await connect(router, [identity.workflowRunAddress]);
    expect(
      router.routeMail(identity.workflowRunAddress, "aGVsbG8=", "message-1"),
    ).toBe(true);
    router.handleClose(first);

    const second = createMockWs();
    router.handleOpen(second);
    const reconnect = JSON.stringify({
      type: "reconnect",
      sidecarId: identity.sidecarId,
      token: "token",
      agentAddresses: [identity.workflowRunAddress],
    });
    router.handleMessage(second, reconnect);
    await tick();

    const count = () =>
      second.sent
        .map((raw) => JSON.parse(raw))
        .filter(
          (frame) =>
            frame.type === "mail.inbound" && frame.messageId === "message-1",
        ).length;
    expect(count()).toBe(1);
    router.handleMessage(second, reconnect);
    await tick();
    expect(count()).toBe(1);
  });

  test("delivers durable grants, mail, and signals to the exact generation", async () => {
    const router = createAllocatedRouter();
    const ws = await connect(router);
    const deployed = router.sendAgentDeployToAllocation(
      target,
      identity.workflowRunAddress,
      config,
    );
    await tick();
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.deploy.ack",
        agentAddress: identity.workflowRunAddress,
        publicKey: "b".repeat(64),
      }),
    );
    await deployed;

    await router.sendWorkflowRunDispatchToAllocation(
      target,
      identity.workflowRunAddress,
      identity.workflowRunAddress,
      [],
      "cmF3LW1haWw=",
      "message-1",
    );
    expect(ws.sent.slice(-2).map((raw) => JSON.parse(raw).type)).toEqual([
      "run.grants",
      "mail.inbound",
    ]);

    await router.sendSignalDeliverToAllocation(target, {
      agentAddress: identity.workflowRunAddress,
      runId: identity.workflowRunAddress,
      signalName: "continue",
      signalId: "signal-1",
      payload: { approved: true },
    });
    expect(lastFrame(ws)).toMatchObject({
      type: "signal.deliver",
      signalId: "signal-1",
    });
  });

  test("advancing the fence disconnects and rejects the old generation", async () => {
    const router = createAllocatedRouter();
    const ws = await connect(router);
    router.fenceAllocation(identity.allocationId, 2);

    expect(ws.closed).toBe(true);
    expect(await router.isAllocatedSidecarReady(target)).toBe(false);
  });
});
