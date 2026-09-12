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
import { MAX_MAIL_OUTBOUND_BODY_BYTES } from "@intx/types/sidecar";

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
      ),
    ).toBe(true);
    expect(
      router.routeMail("unknown@example.test", "aGVsbG8=", TEST_SENDER),
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

    router.routeMail(TEST_IDENTITY.workflowRunAddress, "eXk=", TEST_SENDER);
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

  test("co-delivers the resolved sender key on the dispatched run.grants frame", async () => {
    const hexKey = "cc".repeat(32);
    const router = createAllocatedRouter({
      lookups: {
        async resolveSenderKey() {
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
      "mid-codeliver",
    );

    const grants = framesOfType(ws, "run.grants");
    expect(grants).toHaveLength(1);
    expect(grants[0]?.["senderIdentities"]).toEqual([
      { address: TEST_SENDER, publicKey: hexKey },
    ]);
  });

  test("replays the co-delivered sender key on reconnect", async () => {
    // A sidecar that missed the original dispatch learns the grant only from
    // the reconnect replay; the co-delivered key must ride that replay too, or
    // the sidecar would hold a grant for a sender whose key it never cached.
    const hexKey = "dd".repeat(32);
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10_000,
      disconnectQueueTTLMs: 60_000,
      lookups: {
        async resolveSenderKey() {
          return hexKey;
        },
      },
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
      "mid-replay-key",
    );
    router.handleClose(first);

    const second = await connectAllocated(
      router,
      [TEST_IDENTITY.workflowRunAddress],
      "reconnect",
    );
    const grants = framesOfType(second, "run.grants");
    expect(grants).toHaveLength(1);
    expect(grants[0]?.["senderIdentities"]).toEqual([
      { address: TEST_SENDER, publicKey: hexKey },
    ]);
  });

  test("re-resolves and co-delivers a keyless run sender's key on reconnect replay", async () => {
    // A trigger tracked with NO captured senderIdentities and a RUN-address
    // sender is the gap: replaying it as-is would leave the recipient with no
    // key and see the sender as unknown. A run's deployment key is immutable
    // once acked, so re-resolving at replay is safe and co-delivers the key
    // ahead of the mail.
    const runSender = "run_peer@tenant.example";
    const hexKey = "ee".repeat(32);
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10_000,
      disconnectQueueTTLMs: 60_000,
      lookups: {
        async resolveSenderKey() {
          return hexKey;
        },
      },
    });
    const first = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "a2V5bGVzcw==",
      runSender,
      "mid-keyless-run",
      { runId: TEST_IDENTITY.anchorRunId, stepGrants: [] },
    );
    router.handleClose(first);

    const second = await connectAllocated(
      router,
      [TEST_IDENTITY.workflowRunAddress],
      "reconnect",
    );

    const grants = framesOfType(second, "run.grants");
    expect(grants).toHaveLength(1);
    expect(grants[0]?.["senderIdentities"]).toEqual([
      { address: runSender, publicKey: hexKey },
    ]);
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

  test("leaves a keyless USER sender's replay keyless (never re-resolves)", async () => {
    // A user (non-run) sender's key may have rotated since it signed; re-resolving
    // would check the fixed signed bytes against a newer key and turn a valid
    // message into a false invalid. A keyless user-sender entry stays keyless.
    const hexKey = "ff".repeat(32);
    let resolveCalls = 0;
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10_000,
      disconnectQueueTTLMs: 60_000,
      lookups: {
        async resolveSenderKey() {
          resolveCalls += 1;
          return hexKey;
        },
      },
    });
    const first = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "a2V5bGVzcw==",
      TEST_SENDER,
      "mid-keyless-user",
      { runId: TEST_IDENTITY.anchorRunId, stepGrants: [] },
    );
    router.handleClose(first);

    const second = await connectAllocated(
      router,
      [TEST_IDENTITY.workflowRunAddress],
      "reconnect",
    );

    const grants = framesOfType(second, "run.grants");
    expect(grants).toHaveLength(1);
    expect(grants[0]?.["senderIdentities"]).toBeUndefined();
    expect(resolveCalls).toBe(0);
  });

  test("replays a captured sender key rather than the current re-resolved one", async () => {
    // An entry that captured senderIdentities at track time carries the
    // SIGNING-TIME key. Re-resolving would fetch the CURRENT key, which for a
    // rotated sender differs and would turn a valid message into a false
    // invalid. The captured snapshot must replay verbatim.
    const capturedKey = "11".repeat(32);
    const rotatedKey = "22".repeat(32);
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10_000,
      disconnectQueueTTLMs: 60_000,
      lookups: {
        async resolveSenderKey() {
          return rotatedKey;
        },
      },
    });
    const first = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "Y2FwdHVyZWQ=",
      TEST_SENDER,
      "mid-captured",
      {
        runId: TEST_IDENTITY.anchorRunId,
        stepGrants: [],
        senderIdentities: [{ address: TEST_SENDER, publicKey: capturedKey }],
      },
    );
    router.handleClose(first);

    const second = await connectAllocated(
      router,
      [TEST_IDENTITY.workflowRunAddress],
      "reconnect",
    );

    const grants = framesOfType(second, "run.grants");
    expect(grants).toHaveLength(1);
    expect(grants[0]?.["senderIdentities"]).toEqual([
      { address: TEST_SENDER, publicKey: capturedKey },
    ]);
  });

  test("pushes a sender.key.refresh ahead of a grant-less run sender's replay", async () => {
    // A tracked mail with NO run grants still needs its run sender's key
    // co-delivered on replay. There is no run.grants frame to carry it, so a
    // bare sender.key.refresh precedes the mail on the FIFO socket.
    const runSender = "run_peer@tenant.example";
    const hexKey = "33".repeat(32);
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10_000,
      disconnectQueueTTLMs: 60_000,
      lookups: {
        async resolveSenderKey() {
          return hexKey;
        },
      },
    });
    const first = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "Z3JhbnRsZXNz",
      runSender,
      "mid-grantless-run",
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
        (frame.type === "sender.key.refresh" || frame.type === "mail.inbound"),
    );
    expect(relevant.map((frame) => frame["type"])).toEqual([
      "sender.key.refresh",
      "mail.inbound",
    ]);
    expect(relevant[0]).toEqual({
      type: "sender.key.refresh",
      address: runSender,
      publicKey: hexKey,
    });
    expect(framesOfType(second, "run.grants")).toHaveLength(0);
  });

  test("a run-sender replay acked mid-resolve is not redelivered or re-armed", async () => {
    // The connected-window retry runs as an independent setTimeout macrotask, so
    // while its keyless run-sender replay awaits resolveSenderKey a queued
    // mail.inbound.ack can advance on the owning ws and run resolvePendingMail
    // (delete + clearTimeout). The post-await guard in replaySendPendingMail
    // must observe the entry is gone and skip the send, so the acked mail is
    // neither redelivered nor re-armed onto a detached entry. Drive that race
    // deterministically by parking resolveSenderKey on a deferred, acking while
    // it is parked, then releasing it.
    const runSender = "run_peer@tenant.example";
    const hexKey = "44".repeat(32);
    let releaseKey!: (key: string) => void;
    let signalKeyRequested!: () => void;
    const keyRequested = new Promise<void>((resolve) => {
      signalKeyRequested = resolve;
    });
    let resolveCalls = 0;
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 20,
      mailAckMaxRetries: 5,
      lookups: {
        async resolveSenderKey() {
          resolveCalls += 1;
          signalKeyRequested();
          return new Promise<string>((resolve) => {
            releaseKey = resolve;
          });
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "cmVzb2x2ZS1yYWNl",
      runSender,
      "mid-ack-race",
    );
    // The initial delivery is synchronous and does not resolve the sender key.
    expect(inboundCount(ws, "mid-ack-race")).toBe(1);
    expect(resolveCalls).toBe(0);

    // Let the retry macrotask fire and park on the deferred resolveSenderKey.
    await keyRequested;
    expect(resolveCalls).toBe(1);

    // Ack while the replay is parked: resolvePendingMail deletes the entry and
    // clears its timer. Drain the ack's message-chain microtask before release.
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.inbound.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        messageId: "mid-ack-race",
      }),
    );
    await tick();

    // Release the resolve; the guard must see the entry is gone and skip.
    releaseKey(hexKey);
    await tick();
    await new Promise((resolve) => setTimeout(resolve, 50));

    // No redelivery: still exactly the one initial mail.inbound, no
    // sender.key.refresh pushed for the aborted replay, and no re-armed retry
    // (resolveSenderKey was called only for the single parked replay).
    expect(inboundCount(ws, "mid-ack-race")).toBe(1);
    expect(framesOfType(ws, "sender.key.refresh")).toHaveLength(0);
    expect(resolveCalls).toBe(1);
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

  test("co-delivers the sender key on the live run.grants frame", async () => {
    const hexKey = "ee".repeat(32);
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants() {
          return { outcome: "materialized", stepGrants: [] };
        },
        async resolveSenderKey() {
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

    const grants = framesOfType(ws, "run.grants");
    expect(grants).toHaveLength(1);
    expect(grants[0]?.["senderIdentities"]).toEqual([
      { address: TEST_IDENTITY.workflowRunAddress, publicKey: hexKey },
    ]);
  });

  test("replays the mail-trigger sender key on reconnect", async () => {
    // The mail-relay trigger commits a run through the messageId handshake, so
    // its co-delivered key rides the pending-mail entry, not just the live
    // send. A sidecar that drops before the ack must still learn the key from
    // the reconnect replay.
    const hexKey = "ff".repeat(32);
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10_000,
      disconnectQueueTTLMs: 60_000,
      lookups: {
        async materializeMailTriggeredRunGrants() {
          return { outcome: "materialized", stepGrants: [] };
        },
        async resolveSenderKey() {
          return hexKey;
        },
      },
    });
    const first = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    router.handleMessage(
      first,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();
    router.handleClose(first);

    const second = await connectAllocated(
      router,
      [TEST_IDENTITY.workflowRunAddress],
      "reconnect",
    );
    const grants = framesOfType(second, "run.grants");
    expect(grants).toHaveLength(1);
    expect(grants[0]?.["senderIdentities"]).toEqual([
      { address: TEST_IDENTITY.workflowRunAddress, publicKey: hexKey },
    ]);
  });

  test("holds a run sender's mail while its key is unresolvable", async () => {
    // A run-address sender whose key does not resolve is treated as pre-ack: it
    // minted its keypair locally and may have sent before the hub recorded its
    // public key. The mail is HELD (not delivered keyless) until the key lands,
    // so no run.grants and no mail.inbound reach the recipient here. A later
    // deploy settle wakes it, or the TTL surfaces it as undelivered.
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants() {
          return { outcome: "materialized", stepGrants: [] };
        },
        async resolveSenderKey() {
          return null;
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    // The mail is held only while a key-record settle is guaranteed to arrive.
    // Mark the sender's deploy in flight so an unresolvable key parks rather than
    // delivering keyless.
    router.noteSenderDeployStarted(TEST_IDENTITY.workflowRunAddress);
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

    expect(framesOfType(ws, "run.grants")).toHaveLength(0);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);
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

describe("SidecarRouter mail.outbound body cap", () => {
  const smallRawMessage = btoa(
    "From: run_anchor@tenant.example\r\nTo: run_anchor@tenant.example\r\nMessage-ID: <cap-1@example.test>\r\n\r\nbody",
  );

  function createCapRouter() {
    return createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants() {
          return { outcome: "materialized", stepGrants: [] };
        },
        async resolveSenderKey() {
          return "ab".repeat(32);
        },
      },
    });
  }

  test("delivers a within-cap mail.outbound", async () => {
    const router = createCapRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        rawMessage: smallRawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();

    // A legit-sized frame is delivered: the cap does not false-reject.
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(1);
  });

  test("drops an over-cap mail.outbound before either delivery path", async () => {
    const router = createCapRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    // The array-length ceiling admits this frame (one recipient) and the
    // schema puts no length bound on rawMessage, so it passes the union parse
    // and reaches dispatch -- where the app-layer byte cap drops it. The
    // materializer/mail-inbound fan-out never runs, so no frame is emitted.
    const oversized = "A".repeat(MAX_MAIL_OUTBOUND_BODY_BYTES + 4);
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        rawMessage: oversized,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();

    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);
    expect(framesOfType(ws, "run.grants")).toHaveLength(0);
  });
});
