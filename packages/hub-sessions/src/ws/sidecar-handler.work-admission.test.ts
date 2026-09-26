import { expect, test } from "bun:test";
import { canExecuteWorkflowRun, WorkflowRunNotExecutableError } from "@intx/db";

import {
  connectAllocated,
  createAllocatedRouter,
  createManualRetries,
  createMockWs,
  parsedFrames,
  TEST_IDENTITY,
  TEST_TARGET,
  tick,
} from "./sidecar-handler.test-helpers";

const address = TEST_IDENTITY.workflowRunAddress;
const signal = {
  agentAddress: address,
  runId: TEST_IDENTITY.anchorRunId,
  signalName: "continue",
  signalId: "signal-1",
  payload: {},
};

test("reconnect publishes readiness while replay admission is blocked", async () => {
  const replayEntered = Promise.withResolvers<undefined>();
  const releaseReplay = Promise.withResolvers<undefined>();
  let blockReplay = false;
  const router = createAllocatedRouter({
    mailAckRetryIntervalMs: 60_000,
    withExecutableWorkflowRun: async (_target, send) => {
      if (blockReplay) {
        replayEntered.resolve(undefined);
        await releaseReplay.promise;
      }
      return send();
    },
  });
  const first = await connectAllocated(router, [address]);
  await router.routeMail(address, "bWFpbA==", "sender@example.test", "mail-1");
  router.handleClose(first);

  let readinessConfirmed = false;
  let connectionsReported = 0;
  router.events.on("sidecar.allocated.connected", () => {
    connectionsReported += 1;
  });
  const waiting = router
    .waitForAllocatedSidecar(TEST_TARGET, 10_000)
    .then(() => {
      readinessConfirmed = true;
    });
  await tick();
  blockReplay = true;
  const second = createMockWs();
  router.handleOpen(second);
  router.handleMessage(
    second,
    JSON.stringify({
      type: "reconnect",
      sidecarId: TEST_IDENTITY.sidecarId,
      token: "token",
      agentAddresses: [address],
    }),
  );
  try {
    await replayEntered.promise;
    expect(await router.isAllocatedSidecarReady(TEST_TARGET)).toBe(true);
    expect(readinessConfirmed).toBe(true);
    expect(connectionsReported).toBe(1);
    expect(second.sent).toEqual([]);
  } finally {
    releaseReplay.resolve(undefined);
    // This acknowledgement queues behind reconnect, so it also drains replay.
    router.handleMessage(
      second,
      JSON.stringify({
        type: "mail.inbound.ack",
        agentAddress: address,
        messageId: "mail-1",
      }),
    );
    await waiting;
    await tick();
    router.handleClose(second);
  }
});

test("grants and their mail stay adjacent when admission waits overlap", async () => {
  const release = Promise.withResolvers<undefined>();
  let admissions = 0;
  const router = createAllocatedRouter({
    withExecutableWorkflowRun: async (_target, send) => {
      admissions += 1;
      if (admissions === 1) await release.promise;
      return send();
    },
  });
  const ws = await connectAllocated(router, [address]);
  ws.sent.length = 0;
  const first = router.routeMail(
    address,
    "first",
    "sender@example.test",
    undefined,
    {
      runId: TEST_IDENTITY.anchorRunId,
      stepGrants: [],
      senderIdentities: [
        { address: "sender@example.test", publicKey: "a".repeat(64) },
      ],
    },
  );
  try {
    await router.routeMail(
      address,
      "second",
      "sender@example.test",
      undefined,
      {
        runId: TEST_IDENTITY.anchorRunId,
        stepGrants: [],
        senderIdentities: [
          { address: "sender@example.test", publicKey: "b".repeat(64) },
        ],
      },
    );
  } finally {
    release.resolve(undefined);
    await first;
  }
  expect(parsedFrames(ws)).toMatchObject([
    { type: "run.grants", senderIdentities: [{ publicKey: "b".repeat(64) }] },
    { type: "mail.inbound", rawMessage: "second" },
    { type: "run.grants", senderIdentities: [{ publicKey: "a".repeat(64) }] },
    { type: "mail.inbound", rawMessage: "first" },
  ]);
  router.handleClose(ws);
});

test.each(["expiry", "cancellation"] as const)(
  "rejects mail when %s occurs during sender-key lookup",
  async (change) => {
    const entered = Promise.withResolvers<undefined>();
    const key = Promise.withResolvers<string>();
    const run: {
      status: string;
      expiresAt: Date | null;
      cancellationRequestedAt: Date | null;
    } = {
      status: "running",
      expiresAt: new Date("2026-01-01T12:00:00Z"),
      cancellationRequestedAt: null,
    };
    let now = new Date("2026-01-01T11:59:59Z");
    const router = createAllocatedRouter({
      lookups: {
        resolveSenderKey: () => {
          entered.resolve(undefined);
          return key.promise;
        },
      },
      withExecutableWorkflowRun: async (_target, send) => {
        if (!canExecuteWorkflowRun(run, now))
          throw new WorkflowRunNotExecutableError(
            TEST_IDENTITY.anchorRunId,
            "stopping",
          );
        return send();
      },
    });
    const ws = await connectAllocated(router, [address]);
    const before = [...ws.sent];
    const delivery = router
      .sendWorkflowRunDispatchToAllocation(
        TEST_TARGET,
        address,
        TEST_IDENTITY.anchorRunId,
        [],
        "bWFpbA==",
        "sender@example.test",
        "mail-1",
      )
      .catch((cause: unknown) => cause);
    await entered.promise;
    if (change === "expiry") now = new Date("2026-01-01T12:00:00Z");
    else run.cancellationRequestedAt = now;
    key.resolve("a".repeat(64));
    expect(await delivery).toBeInstanceOf(WorkflowRunNotExecutableError);
    expect(ws.sent).toEqual(before);
    router.handleClose(ws);
  },
);

test.each(["mail", "signal", "direct mail", "direct signal"] as const)(
  "guards %s delivery",
  async (operation) => {
    const router = createAllocatedRouter({
      withExecutableWorkflowRun: async () => {
        throw new WorkflowRunNotExecutableError(
          TEST_IDENTITY.anchorRunId,
          "stopping",
        );
      },
    });
    const ws = await connectAllocated(router, [address]);
    const before = [...ws.sent];
    const sending =
      operation === "mail"
        ? router.sendWorkflowRunDispatchToAllocation(
            TEST_TARGET,
            address,
            TEST_IDENTITY.anchorRunId,
            [],
            "bWFpbA==",
            "sender@example.test",
            "mail-1",
          )
        : operation === "signal"
          ? router.sendSignalDeliverToAllocation(TEST_TARGET, signal)
          : operation === "direct signal"
            ? router.sendSignalDeliver(signal)
            : router.routeMail(
                address,
                "bWFpbA==",
                "sender@example.test",
                "mail-1",
              );
    await expect(sending).rejects.toBeInstanceOf(WorkflowRunNotExecutableError);
    expect(ws.sent).toEqual(before);
    router.handleClose(ws);
  },
);

test("reports untracked relay mail when admission fails transiently", async () => {
  const undelivered = Promise.withResolvers<{
    rawMessage: string;
    recipients: string[];
  }>();
  const router = createAllocatedRouter({
    withExecutableWorkflowRun: async () => {
      throw new Error("Database unavailable");
    },
  });
  router.events.on("mail.outbound.undelivered", (event) => {
    undelivered.resolve(event);
  });
  const ws = await connectAllocated(router, [address]);
  router.handleMessage(
    ws,
    JSON.stringify({
      type: "mail.outbound",
      senderAddress: address,
      rawMessage: "bWFpbA==",
      recipients: [address],
    }),
  );
  expect(await undelivered.promise).toEqual({
    rawMessage: "bWFpbA==",
    recipients: [address],
  });
  router.handleClose(ws);
});

test.each(["retry", "reconnect"] as const)(
  "drops pending mail denied at %s admission",
  async (mode) => {
    const retries = createManualRetries(20);
    let allowed = true;
    const denied = Promise.withResolvers<undefined>();
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 20,
      scheduleTimeout: retries.scheduleTimeout,
      withExecutableWorkflowRun: async (_target, send) => {
        if (!allowed) {
          denied.resolve(undefined);
          throw new WorkflowRunNotExecutableError(
            TEST_IDENTITY.anchorRunId,
            "stopping",
          );
        }
        return send();
      },
    });
    const ws = await connectAllocated(router, [address]);
    await router.routeMail(
      address,
      "bWFpbA==",
      "sender@example.test",
      "mail-1",
    );
    allowed = false;
    let current = ws;
    if (mode === "reconnect") {
      router.handleClose(ws);
      current = await connectAllocated(router, [address], "reconnect");
    } else {
      retries.fireNext();
    }
    await denied.promise;
    await tick();
    const incoming = parsedFrames(current).filter(
      (frame) =>
        typeof frame === "object" &&
        frame !== null &&
        "type" in frame &&
        frame.type === "mail.inbound",
    );
    expect(incoming).toHaveLength(mode === "retry" ? 1 : 0);
    expect(retries.armedCount()).toBe(0);
    router.handleClose(current);
  },
);

test("a transient admission failure during reconnect remains retryable", async () => {
  const retries = createManualRetries(20);
  let attempts = 0;
  const retried = Promise.withResolvers<undefined>();
  const router = createAllocatedRouter({
    mailAckRetryIntervalMs: 20,
    scheduleTimeout: retries.scheduleTimeout,
    withExecutableWorkflowRun: async (_target, send) => {
      attempts += 1;
      if (attempts === 2) throw new Error("Database unavailable");
      const sent = send();
      if (attempts === 3) retried.resolve(undefined);
      return sent;
    },
  });
  const ws = await connectAllocated(router, [address]);
  await router.routeMail(address, "bWFpbA==", "sender@example.test", "mail-1");
  router.handleClose(ws);
  const reconnected = await connectAllocated(router, [address], "reconnect");
  expect(attempts).toBe(2);
  expect(retries.armedCount()).toBe(1);
  retries.fireNext();
  await retried.promise;
  router.handleMessage(
    reconnected,
    JSON.stringify({
      type: "mail.inbound.ack",
      agentAddress: address,
      messageId: "mail-1",
    }),
  );
  await tick();
  expect(
    parsedFrames(reconnected).filter(
      (frame) =>
        typeof frame === "object" &&
        frame !== null &&
        "type" in frame &&
        frame.type === "mail.inbound",
    ),
  ).toHaveLength(1);
  router.handleClose(reconnected);
});

test("persistent admission failures exhaust the pending-mail retry budget", async () => {
  const retries = createManualRetries(10);
  let admissions = 0;
  let nextArm: ReturnType<typeof Promise.withResolvers<undefined>> | undefined;
  const undelivered = Promise.withResolvers<{
    rawMessage: string;
    recipients: string[];
  }>();
  const router = createAllocatedRouter({
    mailAckRetryIntervalMs: 10,
    mailAckMaxRetries: 2,
    scheduleTimeout: (handler, ms) => {
      const cancel = retries.scheduleTimeout(handler, ms);
      if (ms === 10) nextArm?.resolve(undefined);
      return cancel;
    },
    withExecutableWorkflowRun: async (_target, send) => {
      admissions += 1;
      if (admissions > 1) throw new Error("Database unavailable");
      return send();
    },
  });
  router.events.on("mail.outbound.undelivered", (event) => {
    undelivered.resolve(event);
  });
  const ws = await connectAllocated(router, [address]);
  await router.routeMail(address, "bWFpbA==", "sender@example.test", "mail-1");

  for (let attempt = 0; attempt < 2; attempt += 1) {
    const armed = Promise.withResolvers<undefined>();
    nextArm = armed;
    retries.fireNext();
    await armed.promise;
  }
  retries.fireNext();

  expect(await undelivered.promise).toEqual({
    rawMessage: "bWFpbA==",
    recipients: [address],
  });
  expect(admissions).toBe(3);
  expect(retries.armedCount()).toBe(0);
  router.handleClose(ws);
});

test("an acknowledgement during admission transaction completion is not lost", async () => {
  const queued = Promise.withResolvers<undefined>();
  const commit = Promise.withResolvers<undefined>();
  const router = createAllocatedRouter({
    withExecutableWorkflowRun: async (_target, send) => {
      const sent = send();
      queued.resolve(undefined);
      await commit.promise;
      return sent;
    },
  });
  const ws = await connectAllocated(router, [address]);
  const delivery = router.routeMail(
    address,
    "bWFpbA==",
    "sender@example.test",
    "mail-1",
  );
  await queued.promise;
  router.handleMessage(
    ws,
    JSON.stringify({
      type: "mail.inbound.ack",
      agentAddress: address,
      messageId: "mail-1",
    }),
  );
  await tick();
  commit.resolve(undefined);
  await delivery;
  router.handleClose(ws);
  const reconnected = await connectAllocated(router, [address], "reconnect");
  expect(
    parsedFrames(reconnected).filter(
      (frame) =>
        typeof frame === "object" &&
        frame !== null &&
        "type" in frame &&
        frame.type === "mail.inbound",
    ),
  ).toHaveLength(0);
  router.handleClose(reconnected);
});
