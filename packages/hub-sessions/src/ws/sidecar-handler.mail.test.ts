import { describe, expect, test } from "bun:test";

import {
  connectAllocated,
  createAllocatedRouter,
  createMockWs,
  parsedFrames,
  TEST_IDENTITY,
  TEST_TARGET,
  tick,
} from "./sidecar-handler.test-helpers";
import {
  createSidecarRouter,
  type SidecarAuthIdentity,
} from "./sidecar-handler";

const TEST_SENDER = "sender@example.test";

function framesOfType(ws: { sent: string[] }, type: string) {
  return parsedFrames(ws).filter(
    (frame): frame is Record<string, unknown> =>
      typeof frame === "object" &&
      frame !== null &&
      "type" in frame &&
      frame.type === type,
  );
}

function inboundCount(ws: { sent: string[] }, messageId: string): number {
  return framesOfType(ws, "mail.inbound").filter(
    (frame) => frame["messageId"] === messageId,
  ).length;
}

describe("SidecarRouter allocation mail durability", () => {
  test("routeMail reports routable and unknown allocation addresses", async () => {
    const router = createAllocatedRouter();
    await connectAllocated(router, [TEST_IDENTITY.workflowRunAddress]);

    expect(
      router.routeMail(
        TEST_IDENTITY.workflowRunAddress,
        "aGVsbG8=",
        TEST_SENDER,
        null,
      ),
    ).toBe(true);
    expect(
      router.routeMail("unknown@example.test", "aGVsbG8=", TEST_SENDER, null),
    ).toBe(false);
  });

  test("redelivers identical bytes until the allocated sidecar acks", async () => {
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 20,
      mailAckMaxRetries: 5,
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    expect(
      router.routeMail(
        TEST_IDENTITY.workflowRunAddress,
        "aGVsbG8=",
        TEST_SENDER,
        null,
        "mid-retry",
      ),
    ).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 50));

    const deliveries = framesOfType(ws, "mail.inbound").filter(
      (frame) => frame["messageId"] === "mid-retry",
    );
    expect(deliveries.length).toBeGreaterThanOrEqual(2);
    expect(
      deliveries.every((frame) => frame["rawMessage"] === "aGVsbG8="),
    ).toBe(true);
  });

  test("an acknowledgement stops connected-window redelivery", async () => {
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 20,
      mailAckMaxRetries: 5,
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "aGk=",
      TEST_SENDER,
      null,
      "mid-acked",
    );

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.inbound.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        messageId: "mid-acked",
      }),
    );
    await tick();
    await new Promise((resolve) => setTimeout(resolve, 60));

    expect(inboundCount(ws, "mid-acked")).toBe(1);
  });

  test("an acknowledgement from another allocation does not clear pending mail", async () => {
    const secondary: Extract<SidecarAuthIdentity, { kind: "allocated" }> = {
      ...TEST_IDENTITY,
      sidecarId: "sc-mail-secondary",
      allocationId: "alloc-mail-secondary",
      anchorRunId: "run_secondary",
      workflowRunAddress: "run_secondary@tenant.example",
    };
    const router = createSidecarRouter({
      authenticateSidecar: async ({ sidecarId }) =>
        sidecarId === secondary.sidecarId ? secondary : TEST_IDENTITY,
      validateSidecarIdentity: async () => true,
      mailAckRetryIntervalMs: 10,
      mailAckMaxRetries: 5,
    });
    router.fenceAllocation(TEST_TARGET.allocationId, TEST_TARGET.generation);
    router.fenceAllocation(secondary.allocationId, secondary.generation);

    const owner = createMockWs();
    router.handleOpen(owner);
    router.handleMessage(
      owner,
      JSON.stringify({
        type: "register",
        sidecarId: TEST_IDENTITY.sidecarId,
        token: "owner",
        agentAddresses: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    const rogue = createMockWs();
    router.handleOpen(rogue);
    router.handleMessage(
      rogue,
      JSON.stringify({
        type: "register",
        sidecarId: secondary.sidecarId,
        token: "rogue",
        agentAddresses: [secondary.workflowRunAddress],
      }),
    );
    await tick();

    router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "b3duZWQ=",
      TEST_SENDER,
      null,
      "mid-owned",
    );
    router.handleMessage(
      rogue,
      JSON.stringify({
        type: "mail.inbound.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        messageId: "mid-owned",
      }),
    );
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(inboundCount(owner, "mid-owned")).toBeGreaterThanOrEqual(2);

    router.handleMessage(
      owner,
      JSON.stringify({
        type: "mail.inbound.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        messageId: "mid-owned",
      }),
    );
  });

  test("retry exhaustion surfaces the mail as undelivered", async () => {
    const undelivered: { rawMessage: string; recipients: string[] }[] = [];
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10,
      mailAckMaxRetries: 2,
    });
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push(event);
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "ZHJvcA==",
      TEST_SENDER,
      null,
      "mid-drop",
    );
    await new Promise((resolve) => setTimeout(resolve, 80));

    expect(inboundCount(ws, "mid-drop")).toBe(3);
    expect(undelivered).toEqual([
      {
        rawMessage: "ZHJvcA==",
        recipients: [TEST_IDENTITY.workflowRunAddress],
      },
    ]);
  });

  test("mail without a message id is not tracked for redelivery", async () => {
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10,
      mailAckMaxRetries: 3,
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "eXk=",
      TEST_SENDER,
      null,
    );
    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(framesOfType(ws, "mail.inbound")).toHaveLength(1);
  });

  test("retains unacknowledged mail across an allocation reconnect", async () => {
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10_000,
      disconnectQueueTTLMs: 60_000,
    });
    const first = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "cmV0YWluZWQ=",
      TEST_SENDER,
      null,
      "mid-retained",
    );
    router.handleClose(first);

    const second = await connectAllocated(
      router,
      [TEST_IDENTITY.workflowRunAddress],
      "reconnect",
    );

    expect(inboundCount(second, "mid-retained")).toBe(1);
    expect(framesOfType(second, "mail.inbound")[0]?.["rawMessage"]).toBe(
      "cmV0YWluZWQ=",
    );
  });

  test("drops retained mail after the disconnect retention TTL", async () => {
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10_000,
      disconnectQueueTTLMs: 20,
    });
    const first = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "ZXhwaXJlZA==",
      TEST_SENDER,
      null,
      "mid-expired",
    );
    router.handleClose(first);
    await new Promise((resolve) => setTimeout(resolve, 50));

    const second = await connectAllocated(
      router,
      [TEST_IDENTITY.workflowRunAddress],
      "reconnect",
    );
    expect(inboundCount(second, "mid-expired")).toBe(0);
  });

  test("replays run grants before retained trigger mail", async () => {
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10_000,
      disconnectQueueTTLMs: 60_000,
    });
    const first = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    await router.sendWorkflowRunDispatchToAllocation(
      TEST_TARGET,
      TEST_IDENTITY.workflowRunAddress,
      TEST_IDENTITY.anchorRunId,
      [],
      "dHJpZ2dlcg==",
      TEST_SENDER,
      "mid-grants",
    );
    router.handleClose(first);

    const second = await connectAllocated(
      router,
      [TEST_IDENTITY.workflowRunAddress],
      "reconnect",
    );
    const relevant = parsedFrames(second).filter(
      (frame): frame is Record<string, unknown> =>
        typeof frame === "object" &&
        frame !== null &&
        "type" in frame &&
        (frame.type === "run.grants" || frame.type === "mail.inbound"),
    );

    expect(relevant.map((frame) => frame["type"])).toEqual([
      "run.grants",
      "mail.inbound",
    ]);
  });

  test("dispatched inbound mail carries the hub-resolved sender key", async () => {
    const hexKey = "bb".repeat(32);
    const resolvedFor: string[] = [];
    const router = createAllocatedRouter({
      lookups: {
        async resolveSenderKey(address) {
          resolvedFor.push(address);
          return hexKey;
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    await router.sendWorkflowRunDispatchToAllocation(
      TEST_TARGET,
      TEST_IDENTITY.workflowRunAddress,
      TEST_IDENTITY.anchorRunId,
      [],
      "dHJpZ2dlcg==",
      TEST_SENDER,
      "mid-key",
    );

    // The dispatch path resolves the key from the persisted sender, never the
    // MIME From, and stamps it on the redelivered frame.
    expect(resolvedFor).toEqual([TEST_SENDER]);
    const inbound = framesOfType(ws, "mail.inbound").filter(
      (frame) => frame["messageId"] === "mid-key",
    );
    expect(inbound).toHaveLength(1);
    expect(inbound[0]?.["authenticatedSenderPublicKey"]).toBe(hexKey);
  });
});

describe("SidecarRouter workflow-trigger mail gating", () => {
  const rawMessage = btoa(
    "From: sender@example.test\r\nTo: run_anchor@tenant.example\r\nMessage-ID: <mail-1@example.test>\r\n\r\nbody",
  );

  test("materializes and sends run grants before workflow mail", async () => {
    const materialized: unknown[] = [];
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants(args) {
          materialized.push(args);
          return { outcome: "materialized", stepGrants: [] };
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
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();

    expect(materialized).toEqual([
      {
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        runId: TEST_IDENTITY.anchorRunId,
      },
    ]);
    expect(
      parsedFrames(ws)
        .filter(
          (frame): frame is Record<string, unknown> =>
            typeof frame === "object" &&
            frame !== null &&
            "type" in frame &&
            (frame.type === "run.grants" || frame.type === "mail.inbound"),
        )
        .map((frame) => frame["type"]),
    ).toEqual(["run.grants", "mail.inbound"]);
  });

  test("fails a rejected workflow recipient closed", async () => {
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants() {
          return {
            outcome: "rejected",
            status: 403,
            code: "grant_requirement_unsatisfied",
            message: "creator lacks authority",
          };
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const sentBefore = ws.sent.length;

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();

    expect(ws.sent).toHaveLength(sentBefore);
  });

  test("forwards workflow mail without grants when materialization skips", async () => {
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants() {
          return { outcome: "skip" };
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
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();

    expect(
      parsedFrames(ws)
        .filter(
          (frame): frame is Record<string, unknown> =>
            typeof frame === "object" &&
            frame !== null &&
            "type" in frame &&
            (frame.type === "run.grants" || frame.type === "mail.inbound"),
        )
        .map((frame) => frame["type"]),
    ).toEqual(["mail.inbound"]);
  });

  test("relayed inbound mail carries the gate-verified sender", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    // The connection is gated on `senderAddress` by connOwnsAddress before the
    // relay runs; the delivered frame must carry that hub-verified address as
    // authenticatedSender, independent of whatever MIME From the opaque
    // rawMessage bytes contain.
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();

    const inbound = framesOfType(ws, "mail.inbound");
    expect(inbound).toHaveLength(1);
    expect(inbound[0]?.["authenticatedSender"]).toBe(
      TEST_IDENTITY.workflowRunAddress,
    );
    // And explicitly NOT the spoofable MIME From carried in rawMessage
    // (`From: sender@example.test`) -- value-level independence, not just
    // equality to the gate value.
    expect(inbound[0]?.["authenticatedSender"]).not.toBe("sender@example.test");
  });

  test("relayed inbound mail carries the hub-resolved sender key", async () => {
    const hexKey = "aa".repeat(32);
    const resolvedFor: string[] = [];
    const router = createAllocatedRouter({
      lookups: {
        async resolveSenderKey(address) {
          resolvedFor.push(address);
          return hexKey;
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
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();

    // The key is resolved from the hub-verified sender, never the MIME From.
    expect(resolvedFor).toEqual([TEST_IDENTITY.workflowRunAddress]);
    const inbound = framesOfType(ws, "mail.inbound");
    expect(inbound).toHaveLength(1);
    expect(inbound[0]?.["authenticatedSenderPublicKey"]).toBe(hexKey);
  });

  test("inbound mail carries a null sender key when unresolvable", async () => {
    const router = createAllocatedRouter({
      lookups: {
        async resolveSenderKey() {
          return null;
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
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();

    const inbound = framesOfType(ws, "mail.inbound");
    expect(inbound).toHaveLength(1);
    expect(inbound[0]?.["authenticatedSenderPublicKey"]).toBeNull();
  });

  test("drops mail.outbound whose sender the connection does not own", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    // The ownership gate landed by outcome #1 must still hold: a sidecar
    // cannot relay mail as an address it does not own, so nothing is routed.
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: "not-owned@tenant.example",
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();

    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);
  });

  test("durable-dispatch inbound mail carries the enqueue-time sender", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    await router.sendWorkflowRunDispatchToAllocation(
      TEST_TARGET,
      TEST_IDENTITY.workflowRunAddress,
      TEST_IDENTITY.anchorRunId,
      [],
      "dHJpZ2dlcg==",
      TEST_SENDER,
      "durable-mid",
    );

    const inbound = framesOfType(ws, "mail.inbound");
    expect(inbound).toHaveLength(1);
    expect(inbound[0]?.["authenticatedSender"]).toBe(TEST_SENDER);
  });
});
