import { describe, expect, test } from "bun:test";

import { chunkPack } from "@intx/pack-transport";
import {
  type RepoId,
  WORKFLOW_CONTROL_INITIALIZING_ERROR,
  WorkflowControlFrame,
  type WorkflowRunRefTips,
} from "@intx/types/sidecar";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import {
  SidecarIdentityValidationError,
  WorkflowControlHistoryPendingError,
  WorkflowControlInitializingError,
  WorkflowControlRejectedError,
  WorkflowControlTimeoutError,
  WorkflowControlUnconfirmedError,
  WorkflowControlUnreachableError,
} from "./sidecar-handler";
import {
  connectAllocated,
  createAllocatedRouter,
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

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.deploy.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        publicKey: "b".repeat(64),
      }),
    );
    await tick();
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
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.error",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        error: "Deploy failed",
      }),
    );
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
      expect.objectContaining({
        transferId: "transfer-after-ack",
        reason: "path_violation",
      }),
    ]);

    router.handleClose(ws);
    const reconnected = await connectAllocated(
      router,
      [TEST_IDENTITY.workflowRunAddress],
      "reconnect",
    );
    pushPack(router, reconnected, "transfer-after-reconnect");
    await tick();
    expect(framesOfType(reconnected, "repo.pack.reject")).not.toHaveLength(0);
    expect(received).toBe(0);
  });

  test("confirms a stop only once the Hub holds the history the worker reported", async () => {
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
        rawMessage: "cGVyc2lzdGVk",
        recipients: ["user@example.test"],
      }),
    );
    await tick();

    expect(persisted).toBe(false);
  });
});
