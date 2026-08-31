import { describe, expect, test } from "bun:test";

import { chunkPack } from "@intx/pack-transport";
import type { RepoId } from "@intx/types/sidecar";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import {
  connectAllocated,
  createAllocatedRouter,
  createMockWs,
  TEST_CONFIG,
  TEST_IDENTITY,
  TEST_TARGET,
  tick,
} from "./sidecar-handler.test-helpers";
import {
  createSidecarRouter,
  isDeployFrameFailure,
  type SidecarAuthIdentity,
} from "./sidecar-handler";

function lastFrame(ws: { sent: string[] }): Record<string, unknown> {
  const raw = ws.sent.at(-1);
  if (raw === undefined) throw new Error("Expected a frame");
  const parsed: unknown = JSON.parse(raw);
  if (typeof parsed !== "object" || parsed === null) {
    throw new Error("Expected an object frame");
  }
  const frame: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(parsed)) {
    frame[key] = value;
  }
  return frame;
}

describe("SidecarRouter allocation deploy transport", () => {
  test("rejects deploys without a Hub signing key before mutating routing", async () => {
    const router = createSidecarRouter({
      authenticateSidecar: async () => TEST_IDENTITY,
      validateSidecarIdentity: async () => true,
    });
    router.fenceAllocation(TEST_TARGET.allocationId, TEST_TARGET.generation);
    await connectAllocated(router);

    const error = await router
      .sendAgentDeployToAllocation(
        TEST_TARGET,
        TEST_IDENTITY.workflowRunAddress,
        TEST_CONFIG,
      )
      .catch((cause: unknown) => cause);

    expect(isDeployFrameFailure(error)).toBe(true);
    if (!isDeployFrameFailure(error))
      throw new Error("Expected deploy failure");
    expect(error.frameSent).toBe(false);
    expect(router.getRoutableAddresses()).toEqual([]);
  });

  test("times out an unacknowledged deploy and removes its route", async () => {
    const router = createAllocatedRouter({ requestTimeoutMs: 20 });
    await connectAllocated(router);

    const error = await router
      .sendAgentDeployToAllocation(
        TEST_TARGET,
        TEST_IDENTITY.workflowRunAddress,
        TEST_CONFIG,
      )
      .catch((cause: unknown) => cause);

    expect(isDeployFrameFailure(error)).toBe(true);
    if (!isDeployFrameFailure(error))
      throw new Error("Expected deploy failure");
    expect(error.frameSent).toBe(true);
    expect(error.message).toContain("timed out");
    expect(router.getRoutableAddresses()).toEqual([]);
  });

  test("marks a synchronous socket failure as not sent", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router);
    ws.send = () => {
      throw new Error("socket closed");
    };

    const error = await router
      .sendAgentDeployToAllocation(
        TEST_TARGET,
        TEST_IDENTITY.workflowRunAddress,
        TEST_CONFIG,
      )
      .catch((cause: unknown) => cause);

    expect(isDeployFrameFailure(error)).toBe(true);
    if (!isDeployFrameFailure(error))
      throw new Error("Expected deploy failure");
    expect(error.frameSent).toBe(false);
    expect(error.message).toContain("failed to send");
  });

  test("rejects and rolls back routing on agent.error", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router);
    const deploy = router.sendAgentDeployToAllocation(
      TEST_TARGET,
      TEST_IDENTITY.workflowRunAddress,
      TEST_CONFIG,
    );
    await tick();

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.error",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        error: "worker failed",
      }),
    );

    await expect(deploy).rejects.toThrow("worker failed");
    expect(router.getRoutableAddresses()).toEqual([]);
  });

  test("rejects a deploy when its acknowledgement subscriber fails", async () => {
    const router = createAllocatedRouter();
    router.events.on("agent.deploy.ack", () => {
      throw new Error("database write failed");
    });
    const ws = await connectAllocated(router);
    const deploy = router.sendAgentDeployToAllocation(
      TEST_TARGET,
      TEST_IDENTITY.workflowRunAddress,
      TEST_CONFIG,
    );
    await tick();

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.deploy.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        publicKey: "b".repeat(64),
      }),
    );

    await expect(deploy).rejects.toThrow("Failed to store public key");
    expect(router.getRoutableAddresses()).toEqual([]);
  });

  test("disconnect rejects an in-flight deploy", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router);
    const deploy = router.sendAgentDeployToAllocation(
      TEST_TARGET,
      TEST_IDENTITY.workflowRunAddress,
      TEST_CONFIG,
    );
    await tick();

    router.handleClose(ws);

    await expect(deploy).rejects.toThrow("disconnected");
  });

  test("ignores a deploy acknowledgement from another allocation", async () => {
    const secondary: Extract<SidecarAuthIdentity, { kind: "allocated" }> = {
      ...TEST_IDENTITY,
      sidecarId: "sc-secondary",
      allocationId: "alloc-2",
      anchorRunId: "run-secondary",
      workflowRunAddress: "run-secondary@tenant.example",
    };
    const router = createSidecarRouter({
      authenticateSidecar: async ({ sidecarId }) =>
        sidecarId === secondary.sidecarId ? secondary : TEST_IDENTITY,
      validateSidecarIdentity: async () => true,
      hubPublicKey: "a".repeat(64),
      requestTimeoutMs: 500,
    });
    router.fenceAllocation(TEST_TARGET.allocationId, TEST_TARGET.generation);
    router.fenceAllocation(secondary.allocationId, secondary.generation);

    const primaryWs = createMockWs();
    router.handleOpen(primaryWs);
    router.handleMessage(
      primaryWs,
      JSON.stringify({
        type: "register",
        sidecarId: TEST_IDENTITY.sidecarId,
        token: "primary",
        agentAddresses: [],
      }),
    );
    const secondaryWs = createMockWs();
    router.handleOpen(secondaryWs);
    router.handleMessage(
      secondaryWs,
      JSON.stringify({
        type: "register",
        sidecarId: secondary.sidecarId,
        token: "secondary",
        agentAddresses: [],
      }),
    );
    await tick();

    const deploy = router.sendAgentDeployToAllocation(
      TEST_TARGET,
      TEST_IDENTITY.workflowRunAddress,
      TEST_CONFIG,
    );
    let settled = false;
    void deploy.finally(() => {
      settled = true;
    });
    router.handleMessage(
      secondaryWs,
      JSON.stringify({
        type: "agent.deploy.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        publicKey: "c".repeat(64),
      }),
    );
    await tick();
    expect(settled).toBe(false);

    router.handleMessage(
      primaryWs,
      JSON.stringify({
        type: "agent.deploy.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        publicKey: "b".repeat(64),
      }),
    );
    await expect(deploy).resolves.toEqual({ publicKey: "b".repeat(64) });
  });

  test("undeploy waits for its acknowledgement before removing routing", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    const undeploy = router.sendAgentUndeploy(
      TEST_IDENTITY.workflowRunAddress,
      "session-ended",
    );
    expect(lastFrame(ws)).toMatchObject({
      type: "agent.undeploy",
      agentAddress: TEST_IDENTITY.workflowRunAddress,
      reason: "session-ended",
    });
    expect(router.getRoutableAddresses()).toContain(
      TEST_IDENTITY.workflowRunAddress,
    );

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.undeploy.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        statePushed: true,
      }),
    );
    await undeploy;
    expect(router.getRoutableAddresses()).toEqual([]);
  });
});

describe("SidecarRouter allocation pack transport", () => {
  test("requires an outbound pack acknowledgement from its receiving socket", async () => {
    const router = createAllocatedRouter();
    const owner = await connectAllocated(router);
    const rogue = createMockWs();
    router.handleOpen(rogue);
    const sending = router.sendWorkflowRunPackToAllocation(
      TEST_TARGET,
      TEST_IDENTITY.workflowRunAddress,
      new Uint8Array([1, 2, 3]),
      "refs/heads/events",
      "a".repeat(40),
    );
    await tick();
    const done = lastFrame(owner);

    router.handleMessage(
      rogue,
      JSON.stringify({
        type: "repo.pack.ack",
        agentAddress: done["agentAddress"],
        repoId: done["repoId"],
        transferId: done["transferId"],
      }),
    );
    let settled = false;
    void sending.finally(() => {
      settled = true;
    });
    await tick();
    expect(settled).toBe(false);

    router.handleMessage(
      owner,
      JSON.stringify({
        type: "repo.pack.ack",
        agentAddress: done["agentAddress"],
        repoId: done["repoId"],
        transferId: done["transferId"],
      }),
    );
    await expect(sending).resolves.toBeUndefined();
  });

  test("accepts an authenticated workflow-run pack and acknowledges it", async () => {
    const received: unknown[] = [];
    const router = createAllocatedRouter({
      lookups: {
        async receiveWorkflowRunPack(repoId, pack, ref, commitSha, source) {
          received.push({ repoId, pack: [...pack], ref, commitSha, source });
          return { accepted: true };
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const repoId: RepoId = {
      kind: "workflow-run",
      id: deriveWorkflowRunRepoId(TEST_IDENTITY.workflowRunAddress),
    };
    const pack = new Uint8Array([4, 5, 6]);

    for (const chunk of chunkPack(pack)) {
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "repo.pack.push",
          agentAddress: TEST_IDENTITY.workflowRunAddress,
          repoId,
          transferId: "transfer-1",
          seq: chunk.seq,
          data: chunk.data,
        }),
      );
    }
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "repo.pack.done",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        repoId,
        transferId: "transfer-1",
        ref: "refs/heads/events",
        commitSha: "d".repeat(40),
      }),
    );
    await tick();

    expect(received).toEqual([
      {
        repoId,
        pack: [4, 5, 6],
        ref: "refs/heads/events",
        commitSha: "d".repeat(40),
        source: {
          kind: "allocated",
          agentAddress: TEST_IDENTITY.workflowRunAddress,
          allocationId: TEST_TARGET.allocationId,
          anchorRunId: TEST_IDENTITY.anchorRunId,
          generation: TEST_TARGET.generation,
        },
      },
    ]);
    expect(lastFrame(ws)).toMatchObject({
      type: "repo.pack.ack",
      transferId: "transfer-1",
      repoId,
    });
  });

  test("forwards a workflow-run pack rejection to the sidecar", async () => {
    const router = createAllocatedRouter({
      lookups: {
        async receiveWorkflowRunPack() {
          return { accepted: false, reason: "path_violation" };
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const repoId: RepoId = {
      kind: "workflow-run",
      id: deriveWorkflowRunRepoId(TEST_IDENTITY.workflowRunAddress),
    };
    const pack = new Uint8Array([7]);
    for (const chunk of chunkPack(pack)) {
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "repo.pack.push",
          agentAddress: TEST_IDENTITY.workflowRunAddress,
          repoId,
          transferId: "transfer-reject",
          seq: chunk.seq,
          data: chunk.data,
        }),
      );
    }
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "repo.pack.done",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        repoId,
        transferId: "transfer-reject",
        ref: "refs/heads/events",
        commitSha: "e".repeat(40),
      }),
    );
    await tick();

    expect(lastFrame(ws)).toMatchObject({
      type: "repo.pack.reject",
      transferId: "transfer-reject",
      reason: "path_violation",
    });
  });

  test("rejects a workflow-run pack outside the allocation repository", async () => {
    let received = false;
    const router = createAllocatedRouter({
      lookups: {
        async receiveWorkflowRunPack() {
          received = true;
          return { accepted: true };
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const repoId: RepoId = { kind: "workflow-run", id: "another-workflow" };
    const pack = new Uint8Array([8]);
    for (const chunk of chunkPack(pack)) {
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "repo.pack.push",
          agentAddress: TEST_IDENTITY.workflowRunAddress,
          repoId,
          transferId: "transfer-rogue",
          seq: chunk.seq,
          data: chunk.data,
        }),
      );
    }
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "repo.pack.done",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        repoId,
        transferId: "transfer-rogue",
        ref: "refs/heads/events",
        commitSha: "f".repeat(40),
      }),
    );
    await tick();

    expect(received).toBe(false);
    expect(lastFrame(ws)).toMatchObject({
      type: "repo.pack.reject",
      transferId: "transfer-rogue",
      reason: "path_violation",
    });
  });
});
