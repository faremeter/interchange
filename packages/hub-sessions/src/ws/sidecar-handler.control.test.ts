import { describe, expect, test } from "bun:test";
import { WorkflowControlFrame } from "@intx/types/sidecar";

import {
  SidecarIdentityValidationError,
  WorkflowControlRejectedError,
  WorkflowControlTimeoutError,
  WorkflowControlUnreachableError,
} from "./sidecar-handler";
import {
  connectAllocated,
  createAllocatedRouter,
  parsedFrames,
  TEST_IDENTITY,
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

const approvalSnapshot = {
  name: "charge_card",
  description: "Charge the customer",
  inputSchema: { type: "object" },
  arguments: { amount: 100 },
};

describe("SidecarRouter allocation control protocols", () => {
  test("workflow control accepts an acknowledgement only from its allocation connection", async () => {
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
