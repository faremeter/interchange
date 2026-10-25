import { describe, expect, test } from "bun:test";

import { chunkPack } from "@intx/pack-transport";
import type { RepoId } from "@intx/types/repo";
import {
  WORKFLOW_CONTROL_INITIALIZING_ERROR,
  WorkflowControlFrame,
  type WorkflowRunRefTips,
} from "@intx/types/sidecar";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import {
  SidecarIdentityValidationError,
  SidecarNotConnectedError,
  WorkflowControlHistoryPendingError,
  WorkflowControlInitializingError,
  WorkflowControlNotConnectedError,
  WorkflowControlRejectedError,
  type AllocatedSidecarTarget,
  type SidecarRouterConfig,
  WorkflowControlTimeoutError,
  WorkflowControlUnconfirmedError,
  WorkflowControlUnreachableError,
} from "./sidecar-handler";
import {
  connectAllocated,
  createAllocatedRouter,
  createManualRetries,
  deployReply,
  parsedFrames,
  TEST_CONFIG,
  TEST_IDENTITY,
  TEST_REF_TIPS,
  TEST_TARGET,
  tick,
} from "./sidecar-handler.test-helpers";

function framesOfType(ws: { sent: string[] }, type: string) {
  return parsedFrames(ws).filter(
    (frame): frame is Record<string, unknown> =>
      typeof frame === "object" &&
      frame !== null &&
      "type" in frame &&
      frame.type === type,
  );
}

// Longer than the runner's budget, so an acknowledgement, not the clock,
// settles each request.
const CONTROL_TIMEOUT_MS = 60_000;

const workflowRunRepoId: RepoId = {
  kind: "workflow-run",
  id: deriveWorkflowRunRepoId(TEST_IDENTITY.workflowRunAddress),
};

function pushPack(
  router: ReturnType<typeof createAllocatedRouter>,
  ws: Parameters<ReturnType<typeof createAllocatedRouter>["handleMessage"]>[0],
  transferId: string,
) {
  for (const chunk of chunkPack(new Uint8Array([1]))) {
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "repo.pack.push",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        generation: TEST_IDENTITY.generation,
        repoId: workflowRunRepoId,
        transferId,
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
      generation: TEST_IDENTITY.generation,
      repoId: workflowRunRepoId,
      transferId,
      ref: "refs/heads/main",
      commitSha: "a".repeat(40),
    }),
  );
}

function stopCommand() {
  return {
    agentAddress: TEST_IDENTITY.workflowRunAddress,
    runId: TEST_IDENTITY.anchorRunId,
    action: "stop",
    reason: "Lifetime expired",
  } as const;
}

async function acknowledgeStop(
  router: ReturnType<typeof createAllocatedRouter>,
  ws: Awaited<ReturnType<typeof connectAllocated>>,
  refTips: WorkflowRunRefTips | undefined,
) {
  const pending = router.sendWorkflowControl(
    TEST_IDENTITY,
    stopCommand(),
    CONTROL_TIMEOUT_MS,
  );
  await tick();
  const frame = WorkflowControlFrame.assert(
    framesOfType(ws, "workflow.control").at(-1),
  );
  router.handleMessage(
    ws,
    JSON.stringify({
      type: "workflow.control.ack",
      requestId: frame.requestId,
      ...(refTips !== undefined ? { refTips } : {}),
    }),
  );
  return pending;
}

const approvalSnapshot = {
  name: "charge_card",
  description: "Charge the customer",
  inputSchema: { type: "object" },
  arguments: { amount: 100 },
};

describe("SidecarRouter allocation control protocols", () => {
  test("workflow control accepts an acknowledgement only from its allocation connection", async () => {
    const router = createAllocatedRouter({
      withExecutableWorkflowRun: async () => {
        throw new Error("Workflow cannot accept work");
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const pending = router.sendWorkflowControl(
      TEST_IDENTITY,
      {
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        runId: TEST_IDENTITY.anchorRunId,
        action: "stop",
        reason: "Lifetime expired",
      },
      CONTROL_TIMEOUT_MS,
    );
    let settled = false;
    void pending.then(() => {
      settled = true;
    });
    await tick();
    const command = WorkflowControlFrame.assert(
      framesOfType(ws, "workflow.control")[0],
    );
    const acknowledgement = JSON.stringify({
      type: "workflow.control.ack",
      requestId: command.requestId,
      refTips: TEST_REF_TIPS,
    });
    router.handleMessage(
      { send: () => undefined, close: () => undefined },
      acknowledgement,
    );
    await tick();
    expect(settled).toBe(false);
    router.handleMessage(ws, acknowledgement);
    await pending;
    expect(settled).toBe(true);
  });

  test("workflow control cannot target a different run", async () => {
    const router = createAllocatedRouter();
    await connectAllocated(router, [TEST_IDENTITY.workflowRunAddress]);
    await expect(
      router.sendWorkflowControl(
        TEST_IDENTITY,
        {
          agentAddress: TEST_IDENTITY.workflowRunAddress,
          runId: "run_other",
          action: "cancel",
          reason: "Wrong run",
        },
        CONTROL_TIMEOUT_MS,
      ),
    ).rejects.toThrow("anchor run");
  });

  test("workflow control reports a generation this Hub does not hold as unreachable", async () => {
    const router = createAllocatedRouter();
    await expect(
      router.sendWorkflowControl(
        TEST_IDENTITY,
        {
          agentAddress: TEST_IDENTITY.workflowRunAddress,
          runId: TEST_IDENTITY.anchorRunId,
          action: "stop",
          reason: "Lifetime expired",
        },
        CONTROL_TIMEOUT_MS,
      ),
    ).rejects.toBeInstanceOf(WorkflowControlUnreachableError);
  });

  test("workflow control reports a disconnect before the acknowledgement as unreachable", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const pending = router.sendWorkflowControl(
      TEST_IDENTITY,
      {
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        runId: TEST_IDENTITY.anchorRunId,
        action: "stop",
        reason: "Lifetime expired",
      },
      CONTROL_TIMEOUT_MS,
    );
    await tick();
    router.handleClose(ws);
    await expect(pending).rejects.toBeInstanceOf(
      WorkflowControlUnreachableError,
    );
  });

  test("workflow control distinguishes a silent worker from a refusing one", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const command = {
      agentAddress: TEST_IDENTITY.workflowRunAddress,
      runId: TEST_IDENTITY.anchorRunId,
      action: "stop",
      reason: "Lifetime expired",
    } as const;
    await expect(
      router.sendWorkflowControl(TEST_IDENTITY, command, 1),
    ).rejects.toBeInstanceOf(WorkflowControlTimeoutError);

    const refused = router.sendWorkflowControl(
      TEST_IDENTITY,
      command,
      CONTROL_TIMEOUT_MS,
    );
    await tick();
    const frame = WorkflowControlFrame.assert(
      framesOfType(ws, "workflow.control").at(-1),
    );
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "workflow.control.ack",
        requestId: frame.requestId,
        error: "Shutdown failed",
      }),
    );
    await expect(refused).rejects.toBeInstanceOf(WorkflowControlRejectedError);

    const initializing = router.sendWorkflowControl(
      TEST_IDENTITY,
      command,
      CONTROL_TIMEOUT_MS,
    );
    await tick();
    const retryFrame = WorkflowControlFrame.assert(
      framesOfType(ws, "workflow.control").at(-1),
    );
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "workflow.control.ack",
        requestId: retryFrame.requestId,
        error: WORKFLOW_CONTROL_INITIALIZING_ERROR,
      }),
    );
    await expect(initializing).rejects.toBeInstanceOf(
      WorkflowControlInitializingError,
    );
  });

  test("a silent worker still deploying the run defers control instead of timing out", async () => {
    const router = createAllocatedRouter({ requestTimeoutMs: 5 });
    const ws = await connectAllocated(router);
    // The Hub stops waiting for the deploy before the worker answers it.
    await expect(
      router.sendAgentDeployToAllocation(
        TEST_TARGET,
        TEST_IDENTITY.workflowRunAddress,
        TEST_CONFIG,
      ),
    ).rejects.toThrow("timed out");

    await expect(
      router.sendWorkflowControl(TEST_IDENTITY, stopCommand(), 1),
    ).rejects.toBeInstanceOf(WorkflowControlInitializingError);

    router.handleMessage(ws, deployReply(ws, { publicKey: "b".repeat(64) }));
    await tick();
    await expect(
      router.sendWorkflowControl(TEST_IDENTITY, stopCommand(), 1),
    ).rejects.toBeInstanceOf(WorkflowControlTimeoutError);
  });

  test("a step the worker is still provisioning does not defer the deployment's control", async () => {
    const router = createAllocatedRouter({ requestTimeoutMs: 5 });
    await connectAllocated(router);
    const stepAddress = "run_anchor-step-1@tenant.example";
    await router.bindAllocatedStepRoute(TEST_TARGET, stepAddress);
    // The Hub stops waiting for the step before the worker answers it, but the
    // worker handles the step's frames apart from the deployment's.
    await expect(
      router.sendProvisionStepToAllocation(
        TEST_TARGET,
        stepAddress,
        TEST_CONFIG,
      ),
    ).rejects.toThrow("timed out");

    await expect(
      router.sendWorkflowControl(TEST_IDENTITY, stopCommand(), 1),
    ).rejects.toBeInstanceOf(WorkflowControlTimeoutError);
  });

  test("a deploy the worker refused no longer defers control", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router);
    const deployed = router.sendAgentDeployToAllocation(
      TEST_TARGET,
      TEST_IDENTITY.workflowRunAddress,
      TEST_CONFIG,
    );
    await tick();
    router.handleMessage(ws, deployReply(ws, { error: "Deploy failed" }));
    await expect(deployed).rejects.toThrow("Deploy failed");

    await expect(
      router.sendWorkflowControl(TEST_IDENTITY, stopCommand(), 1),
    ).rejects.toBeInstanceOf(WorkflowControlTimeoutError);
  });

  test("a stop acknowledgement resolves only after the packs sent before it", async () => {
    const packEntered = Promise.withResolvers<undefined>();
    const releasePack = Promise.withResolvers<undefined>();
    const received: string[] = [];
    const router = createAllocatedRouter({
      lookups: {
        async receiveWorkflowRunPack(_repoId, _pack, ref) {
          packEntered.resolve(undefined);
          await releasePack.promise;
          received.push(ref);
          return { accepted: true };
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const pending = router.sendWorkflowControl(
      TEST_IDENTITY,
      stopCommand(),
      CONTROL_TIMEOUT_MS,
    );
    let settled = false;
    void pending.then(() => {
      settled = true;
    });
    await tick();
    const frame = WorkflowControlFrame.assert(
      framesOfType(ws, "workflow.control")[0],
    );

    pushPack(router, ws, "transfer-before-ack");
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "workflow.control.ack",
        requestId: frame.requestId,
        refTips: TEST_REF_TIPS,
      }),
    );
    await packEntered.promise;
    await tick();
    expect(settled).toBe(false);

    releasePack.resolve(undefined);
    await pending;
    expect(received).toEqual(["refs/heads/main"]);
    expect(framesOfType(ws, "repo.pack.ack")).toHaveLength(1);
  });

  test.each(["processed", "stalled"] as const)(
    "a stop acknowledgement queued behind a slow pack is not a silent worker (%s)",
    async (outcome) => {
      const timers: { ms: number; handler: () => void; cancelled: boolean }[] =
        [];
      const packEntered = Promise.withResolvers<undefined>();
      const releasePack = Promise.withResolvers<undefined>();
      const router = createAllocatedRouter({
        scheduleTimeout: (handler, ms) => {
          const timer = { ms, handler, cancelled: false };
          timers.push(timer);
          return () => {
            timer.cancelled = true;
          };
        },
        lookups: {
          async receiveWorkflowRunPack() {
            packEntered.resolve(undefined);
            await releasePack.promise;
            return { accepted: true };
          },
        },
      });
      const ws = await connectAllocated(router, [
        TEST_IDENTITY.workflowRunAddress,
      ]);
      const pending = router.sendWorkflowControl(
        TEST_IDENTITY,
        stopCommand(),
        7_000,
      );
      await tick();
      const frame = WorkflowControlFrame.assert(
        framesOfType(ws, "workflow.control")[0],
      );
      pushPack(router, ws, "transfer-slow");
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "workflow.control.ack",
          requestId: frame.requestId,
          refTips: TEST_REF_TIPS,
        }),
      );
      await packEntered.promise;

      const [response, processing] = timers.filter(
        (timer) => timer.ms === 7_000,
      );
      if (response === undefined || processing === undefined)
        throw new Error("Expected response and processing timers");
      expect(response.cancelled).toBe(true);
      expect(processing.cancelled).toBe(false);

      if (outcome === "processed") {
        releasePack.resolve(undefined);
        await pending;
        expect(processing.cancelled).toBe(true);
        return;
      }
      processing.handler();
      await expect(pending).rejects.toBeInstanceOf(
        WorkflowControlUnconfirmedError,
      );
      releasePack.resolve(undefined);
    },
  );

  test("refuses workflow-run packs after a stop acknowledgement, including after a reconnect", async () => {
    let received = 0;
    const router = createAllocatedRouter({
      lookups: {
        async receiveWorkflowRunPack() {
          received += 1;
          return { accepted: true };
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const pending = router.sendWorkflowControl(
      TEST_IDENTITY,
      stopCommand(),
      CONTROL_TIMEOUT_MS,
    );
    await tick();
    const frame = WorkflowControlFrame.assert(
      framesOfType(ws, "workflow.control")[0],
    );
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "workflow.control.ack",
        requestId: frame.requestId,
        refTips: TEST_REF_TIPS,
      }),
    );
    await pending;

    pushPack(router, ws, "transfer-after-ack");
    await tick();
    expect(framesOfType(ws, "repo.pack.reject")).toEqual([
      expect.objectContaining({
        transferId: "transfer-after-ack",
        reason: "path_violation",
      }),
    ]);

    router.handleClose(ws);
    const reconnected = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    pushPack(router, reconnected, "transfer-after-reconnect");
    await tick();
    expect(framesOfType(reconnected, "repo.pack.reject")).not.toHaveLength(0);
    expect(received).toBe(0);
  });

  test("a failed history finality read keeps its cause", async () => {
    const lookupFailure = new Error("Database unavailable");
    const router = createAllocatedRouter({
      lookups: {
        isWorkflowRunHistoryFinal: async () => {
          throw lookupFailure;
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    try {
      const pending = router
        .retainAllocation(TEST_TARGET, CONTROL_TIMEOUT_MS)
        .catch((cause: unknown) => cause);
      await ws.awaitSent(() => framesOfType(ws, "workflow.control").length > 0);
      const frame = WorkflowControlFrame.assert(
        framesOfType(ws, "workflow.control").at(-1),
      );
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "workflow.control.ack",
          requestId: frame.requestId,
          retention: "kept",
          refTips: TEST_REF_TIPS,
        }),
      );
      const failure = await pending;
      expect(failure).toBeInstanceOf(WorkflowControlHistoryPendingError);
      expect(failure).toMatchObject({ cause: lookupFailure });
    } finally {
      router.handleClose(ws);
    }
  });

  test("retention waits for earlier history receives before closing further uploads", async () => {
    let receiving = true;
    let received = 0;
    const router = createAllocatedRouter({
      lookups: {
        isWorkflowRunHistoryFinal: async () => !receiving,
        receiveWorkflowRunPack: async () => {
          received++;
          return { accepted: true };
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    async function acknowledgeRetain() {
      const sentBefore = framesOfType(ws, "workflow.control").length;
      const pending = router
        .retainAllocation(TEST_TARGET, CONTROL_TIMEOUT_MS)
        .catch((cause: unknown) => cause);
      await ws.awaitSent(
        () => framesOfType(ws, "workflow.control").length > sentBefore,
      );
      const frame = WorkflowControlFrame.assert(
        framesOfType(ws, "workflow.control").at(-1),
      );
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "workflow.control.ack",
          requestId: frame.requestId,
          retention: "kept",
          refTips: TEST_REF_TIPS,
        }),
      );
      return pending;
    }
    expect(await acknowledgeRetain()).toBeInstanceOf(
      WorkflowControlHistoryPendingError,
    );
    expect(
      router.getSidecarInventory(TEST_IDENTITY.sidecarId)?.retainedIncarnations,
    ).toEqual([TEST_TARGET]);
    pushPack(router, ws, "retained-while-history-pending");
    await ws.awaitSent(() => framesOfType(ws, "repo.pack.ack").length === 1);
    expect(received).toBe(1);
    receiving = false;
    expect(await acknowledgeRetain()).toBe("kept");
    pushPack(router, ws, "retained-after-confirmation");
    await ws.awaitSent(() => framesOfType(ws, "repo.pack.reject").length === 1);
    expect(received).toBe(1);
    expect(framesOfType(ws, "repo.pack.reject")).toHaveLength(1);
    router.handleClose(ws);
  });

  test.each([false, true])(
    "an expired copy's reconnect stop uses terminal history finality: %s",
    async (final) => {
      let received = 0;
      let finalityReads = 0;
      let tipReads = 0;
      const router = createAllocatedRouter({
        requestTimeoutMs: CONTROL_TIMEOUT_MS,
        validateSidecarIdentity: async (_identity, use) =>
          use !== "reclaim" && use !== "retention",
        lookups: {
          isWorkflowRunHistoryFinal: async () => {
            finalityReads++;
            return final;
          },
          readWorkflowRunRefTips: async () => {
            tipReads++;
            return TEST_REF_TIPS;
          },
          receiveWorkflowRunPack: async () => {
            received++;
            return { accepted: true };
          },
        },
      });
      const ws = await connectAllocated(router, [
        TEST_IDENTITY.workflowRunAddress,
      ]);
      try {
        await ws.awaitSent(
          () => framesOfType(ws, "workflow.control").length > 0,
        );
        const frame = WorkflowControlFrame.assert(
          framesOfType(ws, "workflow.control")[0],
        );
        expect(frame.action).toBe("stop");
        router.handleMessage(
          ws,
          JSON.stringify({
            type: "workflow.control.ack",
            requestId: frame.requestId,
            // Matching tips cannot skip pending accepted history; divergent
            // post-terminal tips cannot prevent confirmation once it settles.
            refTips: final
              ? { ...TEST_REF_TIPS, "refs/heads/main": "b".repeat(40) }
              : TEST_REF_TIPS,
          }),
        );
        await router.syncSidecar(TEST_IDENTITY.sidecarId);
        pushPack(router, ws, "after-terminal-stop");
        await router.syncSidecar(TEST_IDENTITY.sidecarId);
        expect(received).toBe(final ? 0 : 1);
        expect(finalityReads).toBe(1);
        expect(tipReads).toBe(0);
        expect(
          framesOfType(ws, final ? "repo.pack.reject" : "repo.pack.ack"),
        ).not.toHaveLength(0);
        expect(
          router.getSidecarInventory(TEST_IDENTITY.sidecarId)
            ?.retainedIncarnations,
        ).toEqual([]);
      } finally {
        router.handleClose(ws);
      }
    },
  );

  test("confirms a stop only once the Hub holds the history the worker reported", async () => {
    let hubRefTips: WorkflowRunRefTips = {
      ...TEST_REF_TIPS,
      "refs/heads/main": "b".repeat(40),
    };
    let received = 0;
    let finalityReads = 0;
    const router = createAllocatedRouter({
      lookups: {
        readWorkflowRunRefTips: async () => hubRefTips,
        isWorkflowRunHistoryFinal: async () => {
          finalityReads++;
          return true;
        },
        async receiveWorkflowRunPack() {
          received += 1;
          return { accepted: true };
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    await expect(acknowledgeStop(router, ws, TEST_REF_TIPS)).rejects.toThrow(
      WorkflowControlHistoryPendingError,
    );
    // Unconfirmed, the stop fences nothing, so the rest of the history lands.
    pushPack(router, ws, "transfer-after-unconfirmed-stop");
    await tick();
    expect(received).toBe(1);
    expect(framesOfType(ws, "repo.pack.reject")).toHaveLength(0);
    expect(router.getRoutableAddresses()).toContain(
      TEST_IDENTITY.workflowRunAddress,
    );

    hubRefTips = TEST_REF_TIPS;
    await acknowledgeStop(router, ws, TEST_REF_TIPS);
    expect(router.getRoutableAddresses()).not.toContain(
      TEST_IDENTITY.workflowRunAddress,
    );
    pushPack(router, ws, "transfer-after-confirmed-stop");
    await tick();
    expect(received).toBe(1);
    expect(finalityReads).toBe(0);
  });

  test("a stopped worker that reconnects without its address still delivers its history", async () => {
    let hubRefTips: WorkflowRunRefTips = {
      ...TEST_REF_TIPS,
      "refs/heads/main": "b".repeat(40),
    };
    let received = 0;
    const router = createAllocatedRouter({
      lookups: {
        readWorkflowRunRefTips: async () => hubRefTips,
        async receiveWorkflowRunPack() {
          received += 1;
          hubRefTips = TEST_REF_TIPS;
          return { accepted: true };
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    await expect(acknowledgeStop(router, ws, TEST_REF_TIPS)).rejects.toThrow(
      WorkflowControlHistoryPendingError,
    );

    // A stopped worker no longer hosts the deployment, so its reconnect
    // announces no address.
    router.handleClose(ws);
    const reconnected = await connectAllocated(router, []);
    pushPack(router, reconnected, "transfer-after-reconnect");
    await tick();
    expect(received).toBe(1);
    expect(framesOfType(reconnected, "repo.pack.ack")).toEqual([
      expect.objectContaining({ transferId: "transfer-after-reconnect" }),
    ]);

    await acknowledgeStop(router, reconnected, TEST_REF_TIPS);
    pushPack(router, reconnected, "transfer-after-confirmed-stop");
    await tick();
    expect(received).toBe(1);
    expect(framesOfType(reconnected, "repo.pack.reject")).not.toHaveLength(0);
  });

  test("a disconnect cancels a transfer its worker began without a route", async () => {
    let received = 0;
    const router = createAllocatedRouter({
      lookups: {
        async receiveWorkflowRunPack() {
          received += 1;
          return { accepted: true };
        },
      },
    });
    const interrupted = await connectAllocated(router, []);
    const [chunk] = chunkPack(new Uint8Array([1]));
    if (chunk === undefined) throw new Error("Expected one pack chunk");
    router.handleMessage(
      interrupted,
      JSON.stringify({
        type: "repo.pack.push",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        generation: TEST_IDENTITY.generation,
        repoId: workflowRunRepoId,
        transferId: "transfer-interrupted",
        seq: chunk.seq,
        data: chunk.data,
      }),
    );
    await tick();
    router.handleClose(interrupted);

    const reconnected = await connectAllocated(router, []);
    pushPack(router, reconnected, "transfer-after-reconnect");
    await tick();
    expect(framesOfType(reconnected, "repo.pack.reject")).toHaveLength(0);
    expect(received).toBe(1);
  });

  test("does not confirm a stop whose acknowledgement reports no ref tips", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    await expect(acknowledgeStop(router, ws, undefined)).rejects.toThrow(
      WorkflowControlHistoryPendingError,
    );
    expect(router.getRoutableAddresses()).toContain(
      TEST_IDENTITY.workflowRunAddress,
    );
  });

  test("does not confirm a stop while the Hub cannot read its history", async () => {
    const router = createAllocatedRouter({
      lookups: {
        readWorkflowRunRefTips: async () => {
          throw new Error("repository unavailable");
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const stop = acknowledgeStop(router, ws, TEST_REF_TIPS);
    await expect(stop).rejects.toThrow(WorkflowControlHistoryPendingError);
    await expect(stop).rejects.toThrow("repository unavailable");
  });

  test("a repeated stop stays confirmed after the Hub fenced the worker's later history", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    await acknowledgeStop(router, ws, TEST_REF_TIPS);
    await acknowledgeStop(router, ws, {
      ...TEST_REF_TIPS,
      "refs/heads/main": "d".repeat(40),
    });
  });

  test("an acknowledgement that cannot be validated leaves the stop unknown", async () => {
    let validationFails = false;
    const router = createAllocatedRouter({
      validateSidecarIdentity: async () => {
        if (validationFails) throw new Error("database unavailable");
        return true;
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const pending = router.sendWorkflowControl(
      TEST_IDENTITY,
      {
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        runId: TEST_IDENTITY.anchorRunId,
        action: "stop",
        reason: "Lifetime expired",
      },
      CONTROL_TIMEOUT_MS,
    );
    await tick();
    const frame = WorkflowControlFrame.assert(
      framesOfType(ws, "workflow.control")[0],
    );
    validationFails = true;
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "workflow.control.ack",
        requestId: frame.requestId,
      }),
    );
    await expect(pending).rejects.toBeInstanceOf(
      SidecarIdentityValidationError,
    );
  });

  function reportStop(
    router: ReturnType<typeof createAllocatedRouter>,
    ws: Awaited<ReturnType<typeof connectAllocated>>,
    refTips: WorkflowRunRefTips | undefined,
    generation = TEST_IDENTITY.generation,
  ) {
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "deployment.stopped",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        generation,
        error: "Its workflow child ended itself",
        ...(refTips !== undefined ? { refTips } : {}),
      }),
    );
  }

  test("holds a reported stop until the Hub has the history the stopped copy reported", async () => {
    let hubRefTips: WorkflowRunRefTips = {
      ...TEST_REF_TIPS,
      "refs/heads/main": "b".repeat(40),
    };
    const router = createAllocatedRouter({
      lookups: { readWorkflowRunRefTips: async () => hubRefTips },
    });
    const reported: unknown[] = [];
    router.events.on("deployment.stopped", (event) => {
      reported.push(event);
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    reportStop(router, ws, TEST_REF_TIPS);
    await tick();

    expect(reported).toEqual([TEST_TARGET]);
    expect(router.reportedDeploymentFailure(TEST_TARGET)).toBe(
      "Its workflow child ended itself",
    );
    expect(router.getRoutableAddresses()).not.toContain(
      TEST_IDENTITY.workflowRunAddress,
    );
    const pending = await router.stoppedDeploymentHistory(TEST_TARGET);
    expect(pending?.unreceived).toContain("refs/heads/main");

    hubRefTips = TEST_REF_TIPS;
    reportStop(router, ws, TEST_REF_TIPS);
    await tick();
    const received = await router.stoppedDeploymentHistory(TEST_TARGET);
    expect(received?.unreceived).toBeNull();
    // The wait counts from the first report.
    expect(received?.reportedAt).toEqual(pending?.reportedAt);
  });

  test("keeps the tips an earlier report gave when a later one could not read them", async () => {
    const router = createAllocatedRouter({
      lookups: { readWorkflowRunRefTips: async () => TEST_REF_TIPS },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    reportStop(router, ws, undefined);
    await tick();
    const unknown = await router.stoppedDeploymentHistory(TEST_TARGET);
    expect(unknown?.unreceived).toBe("the worker did not report its ref tips");

    reportStop(router, ws, TEST_REF_TIPS);
    reportStop(router, ws, undefined);
    await tick();
    const known = await router.stoppedDeploymentHistory(TEST_TARGET);
    expect(known?.unreceived).toBeNull();
  });

  test("treats the history of a stop the Hub confirmed as final", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    await acknowledgeStop(router, ws, TEST_REF_TIPS);

    reportStop(router, ws, {
      ...TEST_REF_TIPS,
      "refs/heads/main": "d".repeat(40),
    });
    await tick();

    const history = await router.stoppedDeploymentHistory(TEST_TARGET);
    expect(history?.unreceived).toBeNull();
  });

  test("undeploys a reported stop of an incarnation it does not keep", async () => {
    const router = createAllocatedRouter();
    const reported: unknown[] = [];
    router.events.on("deployment.stopped", (event) => {
      reported.push(event);
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    reportStop(router, ws, TEST_REF_TIPS, TEST_IDENTITY.generation + 1);
    await tick();

    expect(reported).toEqual([]);
    expect(router.reportedDeploymentFailure(TEST_TARGET)).toBeUndefined();
    expect(framesOfType(ws, "agent.undeploy")).toMatchObject([
      {
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        generation: TEST_IDENTITY.generation + 1,
      },
    ]);
  });

  test("gives up mail pending for a deployment that stops while its retry waits for admission", async () => {
    const retries = createManualRetries(20);
    let holdAdmission = false;
    const entered = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 20,
      mailHoldTTLMs: 20,
      scheduleTimeout: retries.scheduleTimeout,
      withExecutableWorkflowRun: async (_target, send) => {
        if (holdAdmission) {
          entered.resolve(undefined);
          await release.promise;
        }
        return send();
      },
    });
    const undelivered = Promise.withResolvers<unknown>();
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.resolve(event);
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    expect(
      await router.routeMail(
        TEST_IDENTITY.workflowRunAddress,
        "c3RvcHBlZA==",
        "sender@example.test",
        "mid-stopped",
      ),
    ).toBe(true);

    holdAdmission = true;
    retries.fireNext();
    await entered.promise;
    reportStop(router, ws, TEST_REF_TIPS);
    await tick();
    release.resolve(undefined);

    expect(await undelivered.promise).toMatchObject({
      rawMessage: "c3RvcHBlZA==",
    });
  });

  test("acknowledges a signal correlation only after its durable co-write", async () => {
    const registered: string[] = [];
    const router = createAllocatedRouter({
      lookups: {
        async registerSignalCorrelation(args) {
          registered.push(args.correlationId);
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "signal.correlation.register",
        correlationId: "corr-1",
        runId: "run-1",
        anchorRunId: TEST_IDENTITY.anchorRunId,
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        generation: TEST_TARGET.generation,
        kind: "approval",
        snapshot: approvalSnapshot,
      }),
    );
    await tick();

    expect(registered).toEqual(["corr-1"]);
    expect(framesOfType(ws, "signal.correlation.register.ack")).toEqual([
      {
        type: "signal.correlation.register.ack",
        correlationId: "corr-1",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
      },
    ]);
  });

  test("withholds the correlation acknowledgement when persistence fails", async () => {
    const router = createAllocatedRouter({
      lookups: {
        async registerSignalCorrelation() {
          throw new Error("database unavailable");
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "signal.correlation.register",
        correlationId: "corr-failed",
        runId: "run-1",
        anchorRunId: TEST_IDENTITY.anchorRunId,
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        generation: TEST_TARGET.generation,
        kind: "approval",
        snapshot: approvalSnapshot,
      }),
    );
    await tick();

    expect(framesOfType(ws, "signal.correlation.register.ack")).toEqual([]);
  });

  test("does not persist a correlation for an unowned address", async () => {
    let called = false;
    const router = createAllocatedRouter({
      lookups: {
        async registerSignalCorrelation() {
          called = true;
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "signal.correlation.register",
        correlationId: "corr-rogue",
        runId: "run-1",
        anchorRunId: TEST_IDENTITY.anchorRunId,
        agentAddress: "other@tenant.example",
        generation: TEST_TARGET.generation,
        kind: "approval",
        snapshot: approvalSnapshot,
      }),
    );
    await tick();

    expect(called).toBe(false);
  });

  test("routes source updates through the owned address and waits for its ack", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const updating = router.sendSourcesUpdate(
      TEST_IDENTITY.workflowRunAddress,
      [
        {
          id: "replacement",
          provider: "test",
          baseURL: "https://api.example.test",
          credentialId: "new-credential",
          model: "new-model",
        },
      ],
      "replacement",
    );
    await tick();
    const frame = framesOfType(ws, "sources.update")[0];
    expect(frame).toBeDefined();
    const requestId = frame?.["requestId"];
    if (typeof requestId !== "string") {
      throw new Error("Expected sources.update requestId");
    }

    router.handleMessage(
      ws,
      JSON.stringify({ type: "session.ack", requestId }),
    );

    await expect(updating).resolves.toBeUndefined();
  });

  test("rejects source updates when the address is disconnected", async () => {
    const router = createAllocatedRouter();

    await expect(
      router.sendSourcesUpdate("missing@tenant.example", [], "missing"),
    ).rejects.toThrow("No sidecar connected");
  });

  test("sends drain control only to the sidecar owning the deployment", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    router.sendDrain({
      agentAddress: TEST_IDENTITY.workflowRunAddress,
      deadlineMs: 5_000,
    });

    expect(framesOfType(ws, "drain.deliver")).toEqual([
      {
        type: "drain.deliver",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        generation: TEST_TARGET.generation,
        deadlineMs: 5_000,
      },
    ]);
    expect(() =>
      router.sendDrain({
        agentAddress: "missing@tenant.example",
        deadlineMs: 5_000,
      }),
    ).toThrow("No sidecar connected");
  });
});

describe("SidecarRouter allocated outbound mail", () => {
  test("surfaces mail that cannot be routed locally", async () => {
    const undelivered: unknown[] = [];
    const router = createAllocatedRouter();
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push(event);
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        generation: TEST_TARGET.generation,
        rawMessage: "bWFpbA==",
        recipients: ["external@example.test"],
      }),
    );
    await tick();

    expect(undelivered).toEqual([
      {
        rawMessage: "bWFpbA==",
        recipients: ["external@example.test"],
      },
    ]);
  });

  test("emits one mail.persisted event per persisted row", async () => {
    const persisted: unknown[] = [];
    const createdAt = new Date("2026-08-31T12:00:00.000Z");
    const router = createAllocatedRouter({
      lookups: {
        async persistMail({ senderAddress, recipients }) {
          return [
            {
              id: "mail-out",
              createdAt,
              direction: "outbound",
              runId: TEST_IDENTITY.anchorRunId,
              address: senderAddress,
            },
            {
              id: "mail-in",
              createdAt,
              direction: "inbound",
              runId: null,
              address: recipients[0] ?? "missing",
            },
          ];
        },
      },
    });
    router.events.on("mail.persisted", (event) => {
      persisted.push(event);
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        delivered: true,
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        generation: TEST_TARGET.generation,
        rawMessage: "cGVyc2lzdGVk",
        recipients: ["user@example.test"],
      }),
    );
    await tick();

    expect(persisted).toHaveLength(2);
    expect(persisted).toEqual([
      expect.objectContaining({ id: "mail-out", raw: expect.any(Uint8Array) }),
      expect.objectContaining({ id: "mail-in", raw: expect.any(Uint8Array) }),
    ]);
  });

  test("drops routed mail from an address the sidecar does not own", async () => {
    const undelivered: unknown[] = [];
    const router = createAllocatedRouter();
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push(event);
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: "other@tenant.example",
        generation: TEST_TARGET.generation,
        rawMessage: "bWFpbA==",
        recipients: ["external@example.test"],
      }),
    );
    await tick();

    expect(undelivered).toEqual([]);
  });

  test("drops mail its routed address sends as another generation", async () => {
    const undelivered: unknown[] = [];
    const router = createAllocatedRouter();
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push(event);
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        generation: TEST_TARGET.generation + 1,
        rawMessage: "bWFpbA==",
        recipients: ["external@example.test"],
      }),
    );
    await tick();

    expect(undelivered).toEqual([]);
  });

  test("does not persist delivered mail for an unowned sender", async () => {
    let persisted = false;
    const router = createAllocatedRouter({
      lookups: {
        async persistMail() {
          persisted = true;
          return [];
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        delivered: true,
        senderAddress: "other@tenant.example",
        generation: TEST_TARGET.generation,
        rawMessage: "cGVyc2lzdGVk",
        recipients: ["user@example.test"],
      }),
    );
    await tick();

    expect(persisted).toBe(false);
  });
});

describe("SidecarRouter not-connected allocations", () => {
  const createdAt = new Date("2026-01-01T00:00:00.000Z");
  const later = (ms: number) => new Date(createdAt.getTime() + ms);
  const stepAddress = "run_anchor-step-1@tenant.example";

  function clockedRouter(config: Partial<SidecarRouterConfig> = {}) {
    const clock = { current: createdAt };
    const router = createAllocatedRouter({
      now: () => clock.current,
      ...config,
    });
    return { clock, router };
  }

  async function rejectedControl(
    router: ReturnType<typeof createAllocatedRouter>,
    target: AllocatedSidecarTarget = TEST_TARGET,
  ): Promise<unknown> {
    return router.sendWorkflowControl(target, stopCommand(), 1).then(
      () => {
        throw new Error("Workflow control did not fail");
      },
      (error: unknown) => error,
    );
  }

  async function notConnectedSince(
    router: ReturnType<typeof createAllocatedRouter>,
    target: AllocatedSidecarTarget = TEST_TARGET,
  ) {
    const error = await rejectedControl(router, target);
    if (!(error instanceof WorkflowControlNotConnectedError)) throw error;
    return error.notConnectedSince;
  }

  test("a generation with no connection has not been connected since the router was created", async () => {
    const { clock, router } = clockedRouter();
    clock.current = later(20_000);

    const raw = await router
      .bindAllocatedStepRoute(TEST_TARGET, stepAddress)
      .then(
        () => {
          throw new Error("The step route was bound");
        },
        (error: unknown) => error,
      );
    if (!(raw instanceof SidecarNotConnectedError)) throw raw;
    expect(raw.message).toContain("Allocated sidecar is not connected");
    expect(raw.notConnectedSince).toEqual(createdAt);

    const control = await rejectedControl(router);
    if (!(control instanceof WorkflowControlNotConnectedError)) throw control;
    expect(control).toBeInstanceOf(WorkflowControlUnreachableError);
    expect(control.message).toBe(raw.message);
    expect(control.cause).toBeInstanceOf(SidecarNotConnectedError);
    expect(control.notConnectedSince).toEqual(createdAt);
  });

  test("a closed connection has not been connected since it closed", async () => {
    const { clock, router } = clockedRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    clock.current = later(5_000);
    router.handleClose(ws);
    clock.current = later(20_000);
    expect(await notConnectedSince(router)).toEqual(later(5_000));

    const again = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    clock.current = later(30_000);
    router.handleClose(again);
    clock.current = later(40_000);
    expect(await notConnectedSince(router)).toEqual(later(30_000));
  });

  test("a detached binding has not been connected since it was detached", async () => {
    let identityCurrent = true;
    const { clock, router } = clockedRouter({
      validateSidecarIdentity: async (_identity, use) =>
        use !== "routing" || identityCurrent,
    });
    await connectAllocated(router, [TEST_IDENTITY.workflowRunAddress]);
    clock.current = later(5_000);
    identityCurrent = false;
    // An identity that is no longer current detaches its binding. That
    // failure is not a missing connection.
    const stale = await rejectedControl(router);
    expect(stale).toBeInstanceOf(WorkflowControlUnreachableError);
    expect(stale).not.toBeInstanceOf(WorkflowControlNotConnectedError);
    expect(stale).toHaveProperty(
      "message",
      expect.stringContaining("identity is no longer current"),
    );

    clock.current = later(20_000);
    expect(await notConnectedSince(router)).toEqual(later(5_000));
  });

  test("a lost binding of an older generation does not count for the current one", async () => {
    const { clock, router } = clockedRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    clock.current = later(5_000);
    router.handleClose(ws);
    router.fenceAllocation(TEST_TARGET.allocationId, 2);
    expect(
      await notConnectedSince(router, {
        allocationId: TEST_TARGET.allocationId,
        generation: 2,
      }),
    ).toEqual(createdAt);
  });

  test("a retired allocation forgets when its binding was lost", async () => {
    const { clock, router } = clockedRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    clock.current = later(5_000);
    router.handleClose(ws);
    router.retireAllocation(TEST_TARGET);
    router.fenceAllocation(TEST_TARGET.allocationId, TEST_TARGET.generation);
    expect(await notConnectedSince(router)).toEqual(createdAt);
  });

  test("a generation that is not current is not a missing connection", async () => {
    const { router } = clockedRouter();
    const error = await rejectedControl(router, {
      allocationId: TEST_TARGET.allocationId,
      generation: 2,
    });
    expect(error).toBeInstanceOf(WorkflowControlUnreachableError);
    expect(error).not.toBeInstanceOf(WorkflowControlNotConnectedError);
    expect(error).toHaveProperty(
      "message",
      expect.stringContaining("is not current"),
    );
  });

  test("a connection that changes during validation is not a missing connection", async () => {
    const validating = Promise.withResolvers<undefined>();
    const validated = Promise.withResolvers<boolean>();
    const { router } = clockedRouter({
      validateSidecarIdentity: async (_identity, use) => {
        if (use !== "routing") return true;
        validating.resolve(undefined);
        return validated.promise;
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const pending = rejectedControl(router);
    await validating.promise;
    router.handleClose(ws);
    validated.resolve(true);
    const error = await pending;
    expect(error).toBeInstanceOf(WorkflowControlUnreachableError);
    expect(error).not.toBeInstanceOf(WorkflowControlNotConnectedError);
    expect(error).toHaveProperty(
      "message",
      expect.stringContaining("connection changed"),
    );
  });
});
