import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { type } from "arktype";

import { configureSync, getConfig } from "@intx/log";

import {
  connectAllocated,
  createAllocatedRouter,
  createManualRetries,
  createMockWs,
  helloFrame,
  liveIncarnations,
  parsedFrames,
  sidecarAuth,
  TEST_IDENTITY,
  TEST_TARGET,
  tick,
} from "./sidecar-handler.test-helpers";
import {
  createSidecarRouter,
  type SidecarAuthIdentity,
} from "./sidecar-handler";
import type { SidecarLookups } from "./sidecar-events";
import { deriveDeliveryKey } from "@intx/types/message-id";
import {
  MailInboundAckFrame,
  MAX_MAIL_OUTBOUND_BODY_BYTES,
} from "@intx/types/sidecar";
import { waitUntil } from "@intx/types/testing";

const TEST_SENDER = "sender@example.test";

type AllocatedRouter = ReturnType<typeof createAllocatedRouter>;

// Settle the test sender's deploy as an allocated deploy does: open an attempt
// for its address, then settle that attempt. Each call opens its own attempt,
// because a settle closes the attempt it names.
let senderAttempts = 0;
function settleSenderDeploy(
  router: AllocatedRouter,
  outcome: Parameters<AllocatedRouter["noteSenderDeploySettled"]>[1],
): void {
  senderAttempts += 1;
  const attempt = {
    ...TEST_TARGET,
    leaseId: `sender-settle-${String(senderAttempts)}`,
  };
  router.noteSenderDeployStarted(TEST_IDENTITY.workflowRunAddress, attempt);
  router.noteSenderDeploySettled(attempt, outcome);
}

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
      await router.routeMail(
        TEST_IDENTITY.workflowRunAddress,
        "aGVsbG8=",
        TEST_SENDER,
      ),
    ).toBe(true);
    expect(
      await router.routeMail("unknown@example.test", "aGVsbG8=", TEST_SENDER),
    ).toBe(false);
  });

  test("redelivers identical bytes until the allocated sidecar acks", async () => {
    const retries = createManualRetries(20);
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 20,
      mailAckMaxRetries: 5,
      scheduleTimeout: retries.scheduleTimeout,
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    expect(
      await router.routeMail(
        TEST_IDENTITY.workflowRunAddress,
        "aGVsbG8=",
        TEST_SENDER,
        "mid-retry",
      ),
    ).toBe(true);
    // One redelivery, fired rather than waited for, then awaited on the
    // socket: the retry hands off to a fire-and-forget continuation, so the
    // send is the event. The initial delivery plus this retry is exactly two,
    // where the pause this replaces asserted "at least two" because it could
    // not know how many had fit.
    retries.fireNext();
    await ws.awaitSent(
      (sent) =>
        sent.filter((line) => line.includes('"messageId":"mid-retry"'))
          .length >= 2,
    );

    const deliveries = framesOfType(ws, "mail.inbound").filter(
      (frame) => frame["messageId"] === "mid-retry",
    );
    expect(deliveries).toHaveLength(2);
    expect(
      deliveries.every((frame) => frame["rawMessage"] === "aGVsbG8="),
    ).toBe(true);
  });

  test("an acknowledgement stops connected-window redelivery", async () => {
    const retries = createManualRetries(20);
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 20,
      mailAckMaxRetries: 5,
      scheduleTimeout: retries.scheduleTimeout,
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    await router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "aGk=",
      TEST_SENDER,
      "mid-acked",
    );
    // The delivery armed a retry; the ack has to take it away again.
    expect(retries.armedCount()).toBe(1);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.inbound.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        generation: 1,
        messageId: "mid-acked",
        authenticatedSender: TEST_SENDER,
      }),
    );
    // Connected-window redelivery only ever runs off an armed retry, so the
    // retry being gone is what "stops redelivery" means -- a stronger
    // statement than the pause this replaces, which could only observe that
    // no redelivery had arrived yet.
    await waitUntil(() => retries.armedCount() === 0);

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
      withExecutableWorkflowRun: async (_target, send) => send(),
      ...sidecarAuth((sidecarId) => [
        sidecarId === secondary.sidecarId ? secondary : TEST_IDENTITY,
      ]),
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
      helloFrame(
        TEST_IDENTITY.sidecarId,
        liveIncarnations([TEST_IDENTITY.workflowRunAddress]),
        "owner",
      ),
    );
    const rogue = createMockWs();
    router.handleOpen(rogue);
    router.handleMessage(
      rogue,
      helloFrame(
        secondary.sidecarId,
        liveIncarnations([secondary.workflowRunAddress]),
        "rogue",
      ),
    );
    await tick();

    await router.routeMail(
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
        generation: 1,
        messageId: "mid-owned",
        authenticatedSender: TEST_SENDER,
      }),
    );
    await waitUntil(() => inboundCount(owner, "mid-owned") >= 2);
    expect(inboundCount(owner, "mid-owned")).toBeGreaterThanOrEqual(2);

    router.handleMessage(
      owner,
      JSON.stringify({
        type: "mail.inbound.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        generation: 1,
        messageId: "mid-owned",
        authenticatedSender: TEST_SENDER,
      }),
    );
  });

  test("an acknowledgement naming another generation does not clear pending mail", async () => {
    const retries = createManualRetries(10);
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10,
      mailAckMaxRetries: 5,
      scheduleTimeout: retries.scheduleTimeout,
    });
    const acknowledged: unknown[] = [];
    router.events.on("mail.inbound.acknowledged", (event) => {
      acknowledged.push(event);
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    await router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "c3RhbGU=",
      TEST_SENDER,
      "mid-generation",
    );

    // The mail went to the routed generation; an ack naming any other one is
    // for an inbox the routed deployment does not have.
    const ack = (generation: number) =>
      JSON.stringify({
        type: "mail.inbound.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        generation,
        messageId: "mid-generation",
        authenticatedSender: TEST_SENDER,
      });
    router.handleMessage(ws, ack(TEST_TARGET.generation + 1));
    await tick();
    retries.fireNext();
    await ws.awaitSent(() => inboundCount(ws, "mid-generation") === 2);
    expect(retries.armedCount()).toBe(1);

    router.handleMessage(ws, ack(TEST_TARGET.generation));
    await waitUntil(() => retries.armedCount() === 0);
    expect(acknowledged).toHaveLength(1);
  });

  test("retry exhaustion surfaces the mail as undelivered", async () => {
    const undelivered: { rawMessage: string; recipients: string[] }[] = [];
    const retries = createManualRetries(10);
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10,
      mailAckMaxRetries: 2,
      scheduleTimeout: retries.scheduleTimeout,
    });
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push(event);
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    await router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "ZHJvcA==",
      TEST_SENDER,
      "mid-drop",
    );
    // Two fires fit inside the 2-retry budget: each redelivers and re-arms,
    // so waiting for the re-arm is what says the redelivery completed. The
    // third fire finds the budget spent and surfaces the mail instead. Firing
    // the retries makes the attempt count exact, where the pause this
    // replaces made it a function of how many intervals the machine got
    // through in 80ms.
    for (let redelivery = 0; redelivery < 2; redelivery += 1) {
      retries.fireNext();
      await waitUntil(() => retries.armedCount() === 1);
    }
    retries.fireNext();
    await waitUntil(() => undelivered.length >= 1);

    expect(inboundCount(ws, "mid-drop")).toBe(3);
    expect(undelivered).toEqual([
      {
        rawMessage: "ZHJvcA==",
        recipients: [TEST_IDENTITY.workflowRunAddress],
      },
    ]);
  });

  test("giving up on dispatched mail leaves it to its dispatch row instead of reporting it undelivered", async () => {
    const undelivered: { rawMessage: string; recipients: string[] }[] = [];
    const retries = createManualRetries(10);
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10,
      mailAckMaxRetries: 0,
      scheduleTimeout: retries.scheduleTimeout,
    });
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push(event);
    });
    await connectAllocated(router, [TEST_IDENTITY.workflowRunAddress]);

    await router.sendWorkflowRunDispatchToAllocation(
      TEST_TARGET,
      TEST_IDENTITY.workflowRunAddress,
      TEST_IDENTITY.anchorRunId,
      [],
      "ZGlzcGF0Y2hlZA==",
      TEST_SENDER,
      "mid-dispatched",
    );
    await router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "cGxhaW4=",
      TEST_SENDER,
      "mid-plain",
    );
    retries.fireNext();
    retries.fireNext();
    await tick();

    expect(retries.armedCount()).toBe(0);
    expect(undelivered).toEqual([
      {
        rawMessage: "cGxhaW4=",
        recipients: [TEST_IDENTITY.workflowRunAddress],
      },
    ]);
  });

  test("retained dispatched mail that expires is left to its dispatch row", async () => {
    const undelivered: { rawMessage: string; recipients: string[] }[] = [];
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10_000,
      mailHoldTTLMs: 20,
    });
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push(event);
    });
    const first = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    await router.sendWorkflowRunDispatchToAllocation(
      TEST_TARGET,
      TEST_IDENTITY.workflowRunAddress,
      TEST_IDENTITY.anchorRunId,
      [],
      "ZGlzcGF0Y2hlZA==",
      TEST_SENDER,
      "mid-dispatched",
    );
    await router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "cGxhaW4=",
      TEST_SENDER,
      "mid-plain",
    );
    router.handleClose(first);

    // One timer expires both entries, so the plain one's report says the
    // dispatched one has been given up too.
    await waitUntil(() => undelivered.length >= 1);
    expect(undelivered).toEqual([
      {
        rawMessage: "cGxhaW4=",
        recipients: [TEST_IDENTITY.workflowRunAddress],
      },
    ]);
  });

  test("mail without a message id is not tracked for redelivery", async () => {
    const retries = createManualRetries(10);
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10,
      mailAckMaxRetries: 3,
      scheduleTimeout: retries.scheduleTimeout,
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    await router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "eXk=",
      TEST_SENDER,
    );

    // Tracking a mail is what arms its redelivery retry, so no armed retry is
    // the untracked state itself -- there is nothing left that could redeliver
    // later. The pause this replaces could only report that none had yet.
    expect(retries.armedCount()).toBe(0);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(1);
  });

  test("retains unacknowledged mail across an allocation reconnect", async () => {
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10_000,
      mailHoldTTLMs: 60_000,
    });
    const first = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    await router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "cmV0YWluZWQ=",
      TEST_SENDER,
      "mid-retained",
    );
    router.handleClose(first);

    const second = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    expect(inboundCount(second, "mid-retained")).toBe(1);
    expect(framesOfType(second, "mail.inbound")[0]?.["rawMessage"]).toBe(
      "cmV0YWluZWQ=",
    );
  });

  test("drops retained mail after the disconnect retention TTL", async () => {
    // The retention timer surfaces every entry it drops as
    // `mail.outbound.undelivered`, and it drops the whole pending set before
    // emitting. That event is therefore the expiry's own report: awaiting it
    // says the drop has happened, where the pause it replaces only said 50ms
    // of a 20ms TTL had elapsed on an unloaded machine.
    let reportExpired!: () => void;
    const expired = new Promise<void>((resolve) => {
      reportExpired = resolve;
    });
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10_000,
      mailHoldTTLMs: 20,
    });
    router.events.on("mail.outbound.undelivered", () => {
      reportExpired();
    });
    const first = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    await router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "ZXhwaXJlZA==",
      TEST_SENDER,
      "mid-expired",
    );
    router.handleClose(first);
    await expired;

    const second = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    expect(inboundCount(second, "mid-expired")).toBe(0);
  });

  describe("when a replacement generation takes the address", () => {
    const address = TEST_IDENTITY.workflowRunAddress;

    // A sidecar connected at generation 1 that reconnects at generation 2.
    function replacedRouter() {
      let current: SidecarAuthIdentity = TEST_IDENTITY;
      const router = createSidecarRouter({
        withExecutableWorkflowRun: async (_target, send) => send(),
        ...sidecarAuth(() => [current]),
        validateSidecarIdentity: async () => true,
        requestTimeoutMs: 500,
        mailAckRetryIntervalMs: 10_000,
        mailHoldTTLMs: 60_000,
      });
      router.fenceAllocation(TEST_IDENTITY.allocationId, 1);
      const connectAt = async (generation: number) => {
        const ws = createMockWs();
        router.handleOpen(ws);
        router.handleMessage(
          ws,
          helloFrame(
            TEST_IDENTITY.sidecarId,
            liveIncarnations([address], generation),
          ),
        );
        await tick();
        return ws;
      };
      const replace = async (first: ReturnType<typeof createMockWs>) => {
        router.handleClose(first);
        current = { ...TEST_IDENTITY, generation: 2 };
        router.fenceAllocation(TEST_IDENTITY.allocationId, 2);
        return connectAt(2);
      };
      return { router, connectAt, replace };
    }

    test("routed mail follows it, restamped, with its run grants ahead of it", async () => {
      const { router, connectAt, replace } = replacedRouter();
      const first = await connectAt(1);
      expect(
        await router.routeMail(address, "Zm9sbG93", TEST_SENDER, "mid-follow", {
          runId: "run-follow",
          stepGrants: [],
        }),
      ).toBe(true);

      const second = await replace(first);

      const relevant = parsedFrames(second).filter(
        (frame): frame is Record<string, unknown> =>
          typeof frame === "object" &&
          frame !== null &&
          "type" in frame &&
          (frame.type === "run.grants" || frame.type === "mail.inbound"),
      );
      expect(relevant).toEqual([
        expect.objectContaining({
          type: "run.grants",
          runId: "run-follow",
          generation: 2,
        }),
        expect.objectContaining({
          type: "mail.inbound",
          messageId: "mid-follow",
          generation: 2,
        }),
      ]);

      // Only the generation it now follows may acknowledge it.
      router.handleMessage(
        second,
        JSON.stringify({
          type: "mail.inbound.ack",
          agentAddress: address,
          generation: 2,
          messageId: "mid-follow",
          authenticatedSender: TEST_SENDER,
        }),
      );
      await tick();
      const third = await replace(second);
      expect(inboundCount(third, "mid-follow")).toBe(0);
    });

    test("dispatched mail is left to its dispatch row", async () => {
      const { router, connectAt, replace } = replacedRouter();
      const first = await connectAt(1);
      await router.sendWorkflowRunDispatchToAllocation(
        TEST_TARGET,
        address,
        TEST_IDENTITY.anchorRunId,
        [],
        "ZGlzcGF0Y2g=",
        TEST_SENDER,
        "mid-dispatch",
      );

      const second = await replace(first);

      expect(inboundCount(second, "mid-dispatch")).toBe(0);
    });
  });

  test("replays run grants before retained trigger mail", async () => {
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10_000,
      mailHoldTTLMs: 60_000,
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

    const second = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
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

  describe("hub credentials on the run.grants barrier", () => {
    const delivery = {
      bindings: [
        {
          handle: "interchange.hub",
          credentialId: "hub:action:a",
          consumer: "action:a",
        },
      ],
      materials: [
        {
          credentialId: "hub:action:a",
          providerKey: "http",
          origin: "https://hub.test",
          secret: "itx_run_secret",
        },
      ],
    };

    function routerWithCredentials(loadedFor: string[]) {
      return createAllocatedRouter({
        mailAckRetryIntervalMs: 10_000,
        mailHoldTTLMs: 60_000,
        lookups: {
          async loadRunCredentials(runId) {
            loadedFor.push(runId);
            return delivery;
          },
        },
      });
    }

    test("the dispatched barrier and its reconnect replay both carry them", async () => {
      const loadedFor: string[] = [];
      const router = routerWithCredentials(loadedFor);
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
        "mid-credentials",
      );
      expect(framesOfType(first, "run.grants")[0]?.["credentials"]).toEqual(
        delivery,
      );

      router.handleClose(first);
      const second = await connectAllocated(router, [
        TEST_IDENTITY.workflowRunAddress,
      ]);
      expect(framesOfType(second, "run.grants")[0]?.["credentials"]).toEqual(
        delivery,
      );
      expect(loadedFor).toEqual([
        TEST_IDENTITY.anchorRunId,
        TEST_IDENTITY.anchorRunId,
      ]);
    });

    test("a load failure on reconnect still redelivers the barrier and its mail", async () => {
      let failing = false;
      const router = createAllocatedRouter({
        mailAckRetryIntervalMs: 10_000,
        mailHoldTTLMs: 60_000,
        lookups: {
          async loadRunCredentials() {
            if (failing) throw new Error("decrypt failed");
            return delivery;
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
        "mid-load-failure",
      );
      router.handleClose(first);
      failing = true;

      const second = await connectAllocated(router, [
        TEST_IDENTITY.workflowRunAddress,
      ]);
      const grants = framesOfType(second, "run.grants");
      expect(grants).toHaveLength(1);
      expect(grants[0]?.["credentials"]).toBeUndefined();
      expect(framesOfType(second, "mail.inbound")).toHaveLength(1);
    });
  });

  test("replays the co-delivered sender key on reconnect", async () => {
    // A sidecar that missed the original dispatch learns the grant only from
    // the reconnect replay; the co-delivered key must ride that replay too, or
    // the sidecar would hold a grant for a sender whose key it never cached.
    const hexKey = "dd".repeat(32);
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10_000,
      mailHoldTTLMs: 60_000,
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

    const second = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const grants = framesOfType(second, "run.grants");
    expect(grants).toHaveLength(1);
    expect(grants[0]?.["senderIdentities"]).toEqual([
      { address: TEST_SENDER, publicKey: hexKey },
    ]);
  });

  test("re-resolves and co-delivers a keyless run sender's key on reconnect replay", async () => {
    // A trigger tracked with NO captured senderIdentities and a RUN-address
    // sender is the gap: replaying it as-is would leave the recipient with no
    // key and see the sender as unknown. A run's key changes only when its
    // sidecar is replaced, so re-resolving at replay is safe and co-delivers
    // the key ahead of the mail.
    const runSender = "run_peer@tenant.example";
    const hexKey = "ee".repeat(32);
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 10_000,
      mailHoldTTLMs: 60_000,
      lookups: {
        async resolveSenderKey() {
          return hexKey;
        },
      },
    });
    const first = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    await router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "a2V5bGVzcw==",
      runSender,
      "mid-keyless-run",
      { runId: TEST_IDENTITY.anchorRunId, stepGrants: [] },
    );
    router.handleClose(first);

    const second = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

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
      mailHoldTTLMs: 60_000,
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
    await router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "a2V5bGVzcw==",
      TEST_SENDER,
      "mid-keyless-user",
      { runId: TEST_IDENTITY.anchorRunId, stepGrants: [] },
    );
    router.handleClose(first);

    const second = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

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
      mailHoldTTLMs: 60_000,
      lookups: {
        async resolveSenderKey() {
          return rotatedKey;
        },
      },
    });
    const first = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    await router.routeMail(
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

    const second = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

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
      mailHoldTTLMs: 60_000,
      lookups: {
        async resolveSenderKey() {
          return hexKey;
        },
      },
    });
    const first = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    await router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "Z3JhbnRsZXNz",
      runSender,
      "mid-grantless-run",
    );
    router.handleClose(first);

    const second = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

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
    const retries = createManualRetries(20);
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 20,
      mailAckMaxRetries: 5,
      scheduleTimeout: retries.scheduleTimeout,
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
    await router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "cmVzb2x2ZS1yYWNl",
      runSender,
      "mid-ack-race",
    );
    // The initial delivery is synchronous and does not resolve the sender key.
    expect(inboundCount(ws, "mid-ack-race")).toBe(1);
    expect(resolveCalls).toBe(0);

    // Fire the retry so its replay parks on the deferred resolveSenderKey.
    retries.fireNext();
    await keyRequested;
    expect(resolveCalls).toBe(1);

    // Ack while the replay is parked: resolvePendingMail deletes the entry and
    // clears its timer. Drain the ack's message-chain microtask before release.
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.inbound.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        generation: 1,
        messageId: "mid-ack-race",
        authenticatedSender: runSender,
      }),
    );
    await tick();

    // Release the resolve; the guard must see the entry is gone and skip.
    // Everything between the release and the guard is microtask work -- the
    // parked promise, the async frames around it -- so the macrotask `tick`
    // resumes only once the guard has run.
    releaseKey(hexKey);
    await tick();

    // No redelivery: still exactly the one initial mail.inbound, no
    // sender.key.refresh pushed for the aborted replay, and no re-armed retry
    // (re-arming is the last statement of the redelivery path, so the fired
    // retry staying the only one says the path exited at the guard).
    expect(retries.armedCount()).toBe(0);
    expect(inboundCount(ws, "mid-ack-race")).toBe(1);
    expect(framesOfType(ws, "sender.key.refresh")).toHaveLength(0);
    expect(resolveCalls).toBe(1);
  });
});

describe("SidecarRouter workflow-trigger mail gating", () => {
  const rawMessage = btoa(
    "From: sender@example.test\r\nTo: run_anchor@tenant.example\r\nMessage-ID: <mail-1@example.test>\r\n\r\nbody",
  );

  test("refuses relayed mail whose Message-ID starts with the delivery-key prefix", async () => {
    const materialized: unknown[] = [];
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants(args) {
          materialized.push(args);
          return { outcome: "materialized", stepGrants: [] };
        },
        async resolveSenderPrincipal() {
          return {
            principalId: "prn_sender",
            tenantId: "tenant-x",
            coordinates: [],
          };
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
        generation: 1,
        rawMessage: btoa(
          "From: sender@example.test\r\nTo: run_anchor@tenant.example\r\nMessage-ID: mail-4cf62b5d6c6d0b19b118d1d6aa409e76f3fa347627bf67ea9d1db415d6260060\r\n\r\nbody",
        ),
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();

    expect(materialized).toEqual([]);
    expect(framesOfType(ws, "mail.inbound")).toEqual([]);
  });

  test("stores no recipient record for the audit copy of such mail", async () => {
    const persisted: string[][] = [];
    const router = createAllocatedRouter({
      lookups: {
        async persistMail({ recipients }) {
          persisted.push(recipients);
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
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        generation: 1,
        rawMessage: btoa(
          "From: sender@example.test\r\nTo: peer@tenant.example\r\nMessage-ID: mail-4cf62b5d6c6d0b19b118d1d6aa409e76f3fa347627bf67ea9d1db415d6260060\r\n\r\nbody",
        ),
        recipients: ["peer@tenant.example"],
        delivered: true,
      }),
    );
    await waitUntil(() => persisted.length === 1);

    expect(persisted).toEqual([[]]);
  });

  test("materializes and sends run grants before workflow mail, threading the resolved sender through", async () => {
    const materialized: unknown[] = [];
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants(args) {
          materialized.push(args);
          return { outcome: "materialized", stepGrants: [] };
        },
        // The strict, coordinate-carrying resolution the seam runs ONCE for the
        // fan-out. Its principal/tenant thread into the materializer as the
        // sender it gates.
        async resolveSenderPrincipal() {
          return {
            principalId: "prn_sender",
            tenantId: "tenant-x",
            coordinates: [],
          };
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
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();

    // The materializer is invoked with the recipient's run id AND the once-
    // resolved sender's admission coordinates threaded through the seam.
    expect(materialized).toEqual([
      {
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        runId: TEST_IDENTITY.anchorRunId,
        senderCoordinates: [],
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

  test("mints correspondent grants for every recipient before delivering to any", async () => {
    const calls: unknown[] = [];
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants(args) {
          calls.push({ materialize: args.agentAddress });
          return { outcome: "materialized", stepGrants: [] };
        },
        async resolveSenderPrincipal() {
          return null;
        },
        async mintCorrespondentGrant(args) {
          calls.push({ mint: args });
        },
      },
    });
    // The external recipient is unrouted, so this event ends the fan-out.
    const fanOutDone = Promise.withResolvers<undefined>();
    router.events.on("mail.outbound.undelivered", () => {
      fanOutDone.resolve(undefined);
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress, "someone@example.test"],
      }),
    );
    await fanOutDone.promise;

    // One mint for the whole fan-out, ahead of the first recipient's
    // materialization, so a reply cannot outrun the grant that admits it.
    expect(calls).toEqual([
      {
        mint: {
          senderAddress: TEST_IDENTITY.workflowRunAddress,
          recipientAddresses: [
            TEST_IDENTITY.workflowRunAddress,
            "someone@example.test",
          ],
        },
      },
      { materialize: TEST_IDENTITY.workflowRunAddress },
    ]);
  });

  test("keeps the correspondent grant of a recipient whose delivery fails closed", async () => {
    const calls: unknown[] = [];
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants(args) {
          calls.push({ materialize: args.agentAddress });
          return {
            outcome: "rejected",
            status: 403,
            code: "mail_admission_denied",
            message: "refused",
          };
        },
        async resolveSenderPrincipal() {
          return null;
        },
        async mintCorrespondentGrant(args) {
          calls.push({ mint: args });
        },
      },
    });
    // The external co-recipient is unrouted, so this event ends the fan-out,
    // after the run recipient's delivery has failed closed.
    const fanOutDone = Promise.withResolvers<undefined>();
    router.events.on("mail.outbound.undelivered", () => {
      fanOutDone.resolve(undefined);
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress, "someone@example.test"],
      }),
    );
    await fanOutDone.promise;

    // The recipient is minted, then its delivery fails closed. The minter is
    // the only grant write on this path, so the grant stays: the run's
    // lifetime limits it, not the delivery outcome.
    expect(calls).toEqual([
      {
        mint: {
          senderAddress: TEST_IDENTITY.workflowRunAddress,
          recipientAddresses: [
            TEST_IDENTITY.workflowRunAddress,
            "someone@example.test",
          ],
        },
      },
      { materialize: TEST_IDENTITY.workflowRunAddress },
    ]);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);
  });

  test("threads null sender coordinates when the sender does not resolve", async () => {
    // An unresolvable sender yields no principal; the materializer is still
    // invoked, with null coordinates, so its admission gate refuses the sender.
    const materialized: unknown[] = [];
    const materializerCalled = Promise.withResolvers<undefined>();
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants(args) {
          materialized.push(args);
          materializerCalled.resolve(undefined);
          return { outcome: "materialized", stepGrants: [] };
        },
        async resolveSenderPrincipal() {
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
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await materializerCalled.promise;

    expect(materialized).toEqual([
      {
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        runId: TEST_IDENTITY.anchorRunId,
        senderCoordinates: null,
      },
    ]);
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
        generation: 1,
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
      mailHoldTTLMs: 60_000,
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
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();
    router.handleClose(first);

    const second = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const grants = framesOfType(second, "run.grants");
    expect(grants).toHaveLength(1);
    expect(grants[0]?.["senderIdentities"]).toEqual([
      { address: TEST_IDENTITY.workflowRunAddress, publicKey: hexKey },
    ]);
  });

  test.each(["before", "after"] as const)(
    "retries a committed trigger mail when reconnect is %s failed admission",
    async (reconnectOrder) => {
      const admissionEntered = Promise.withResolvers<undefined>();
      const releaseAdmission = Promise.withResolvers<undefined>();
      const retryArmed = Promise.withResolvers<undefined>();
      const retries = createManualRetries(20);
      let materializations = 0;
      let admissions = 0;
      const undelivered: { rawMessage: string; recipients: string[] }[] = [];
      const router = createAllocatedRouter({
        mailAckRetryIntervalMs: 20,
        scheduleTimeout: (handler, ms) => {
          const cancel = retries.scheduleTimeout(handler, ms);
          if (ms === 20) retryArmed.resolve(undefined);
          return cancel;
        },
        lookups: {
          async materializeMailTriggeredRunGrants() {
            materializations += 1;
            return { outcome: "materialized", stepGrants: [] };
          },
        },
        withExecutableWorkflowRun: async (_target, send) => {
          admissions += 1;
          if (admissions === 1) {
            admissionEntered.resolve(undefined);
            await releaseAdmission.promise;
          }
          return send();
        },
      });
      router.events.on("mail.outbound.undelivered", (event) => {
        undelivered.push(event);
      });
      const first = await connectAllocated(router, [
        TEST_IDENTITY.workflowRunAddress,
      ]);
      router.handleMessage(
        first,
        JSON.stringify({
          type: "mail.outbound",
          senderAddress: TEST_IDENTITY.workflowRunAddress,
          generation: TEST_IDENTITY.generation,
          rawMessage,
          recipients: [TEST_IDENTITY.workflowRunAddress],
        }),
      );
      await admissionEntered.promise;
      expect(materializations).toBe(1);

      router.handleClose(first);
      const second =
        reconnectOrder === "before"
          ? await connectAllocated(router, [TEST_IDENTITY.workflowRunAddress])
          : undefined;
      releaseAdmission.resolve(undefined);
      await retryArmed.promise;
      if (second !== undefined) {
        expect(framesOfType(second, "mail.inbound")).toHaveLength(0);
        retries.fireNext();
      }
      const recipient =
        second ??
        (await connectAllocated(router, [TEST_IDENTITY.workflowRunAddress]));
      await recipient.awaitSent((sent) =>
        sent.some((line) => line.includes('"type":"mail.inbound"')),
      );
      expect(
        parsedFrames(recipient)
          .filter(
            (frame): frame is Record<string, unknown> =>
              typeof frame === "object" &&
              frame !== null &&
              "type" in frame &&
              (frame.type === "run.grants" || frame.type === "mail.inbound"),
          )
          .map((frame) => frame["type"]),
      ).toEqual(["run.grants", "mail.inbound"]);
      expect(materializations).toBe(1);
      expect(undelivered).toEqual([]);

      const messageId = framesOfType(recipient, "mail.inbound")[0]?.[
        "messageId"
      ];
      if (typeof messageId !== "string") throw new Error("Missing message ID");
      router.handleMessage(
        recipient,
        JSON.stringify({
          type: "mail.inbound.ack",
          agentAddress: TEST_IDENTITY.workflowRunAddress,
          generation: TEST_IDENTITY.generation,
          messageId,
          authenticatedSender: TEST_IDENTITY.workflowRunAddress,
        }),
      );
      await tick();
      expect(retries.armedCount()).toBe(0);
      router.handleClose(recipient);
    },
  );

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
    router.noteSenderDeployStarted(TEST_IDENTITY.workflowRunAddress, {
      ...TEST_TARGET,
      leaseId: "sender-deploy",
    });
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();

    expect(framesOfType(ws, "run.grants")).toHaveLength(0);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);
  });

  test("retiring an allocation fails its in-flight sender attempt", async () => {
    const undelivered: { rawMessage: string; recipients: string[] }[] = [];
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
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push(event);
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    router.noteSenderDeployStarted(TEST_IDENTITY.workflowRunAddress, {
      ...TEST_TARGET,
      leaseId: "sender-deploy",
    });
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        generation: 1,
        rawMessage: "aGVsbG8=",
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();

    router.retireAllocation(TEST_TARGET);

    expect(undelivered).toEqual([
      {
        rawMessage: "aGVsbG8=",
        recipients: [TEST_IDENTITY.workflowRunAddress],
      },
    ]);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);
    expect(() =>
      router.noteSenderDeployStarted(TEST_IDENTITY.workflowRunAddress, {
        ...TEST_TARGET,
        leaseId: "sender-deploy-retry",
      }),
    ).not.toThrow();
  });

  test("holds mail to a deployment whose credential resolution is unrecorded", async () => {
    const undelivered: { rawMessage: string; recipients: string[] }[] = [];
    let ready = false;
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants() {
          if (!ready) return { outcome: "notReady" };
          return { outcome: "materialized", stepGrants: [] };
        },
      },
    });
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
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();

    expect(framesOfType(ws, "run.grants")).toHaveLength(0);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);
    expect(undelivered).toEqual([]);

    ready = true;
    settleSenderDeploy(router, {
      recorded: "recipient-key",
    });
    await tick();

    expect(undelivered).toEqual([]);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(1);
    expect(JSON.stringify(framesOfType(ws, "run.grants"))).not.toContain(
      "recipient-key",
    );
  });

  test("a resolution redrive keeps the sender key bound when the mail was accepted", async () => {
    const senderKey = "ab".repeat(32);
    const replacedKey = "cd".repeat(32);
    let resolves = 0;
    let ready = false;
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants() {
          if (!ready) return { outcome: "notReady" };
          return { outcome: "materialized", stepGrants: [] };
        },
        async resolveSenderKey() {
          resolves += 1;
          return resolves === 1 ? senderKey : replacedKey;
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
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();
    expect(resolves).toBe(1);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);

    ready = true;
    settleSenderDeploy(router, {
      recorded: "recipient-key",
    });
    await tick();

    expect(resolves).toBe(1);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(1);
    expect(framesOfType(ws, "run.grants")[0]?.["senderIdentities"]).toEqual([
      {
        address: TEST_IDENTITY.workflowRunAddress,
        publicKey: senderKey,
      },
    ]);
    const rendered = JSON.stringify(framesOfType(ws, "run.grants"));
    expect(rendered).not.toContain(replacedKey);
    expect(rendered).not.toContain("recipient-key");
  });

  test("a resolution redrive keeps an absent sender key absent", async () => {
    const replacedKey = "cd".repeat(32);
    let resolves = 0;
    let ready = false;
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants() {
          if (!ready) return { outcome: "notReady" };
          return { outcome: "materialized", stepGrants: [] };
        },
        async resolveSenderKey() {
          resolves += 1;
          return resolves === 1 ? null : replacedKey;
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
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();
    expect(resolves).toBe(1);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);

    ready = true;
    settleSenderDeploy(router, {
      recorded: "recipient-key",
    });
    await tick();

    expect(resolves).toBe(1);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(1);
    expect(
      framesOfType(ws, "run.grants")[0]?.["senderIdentities"],
    ).toBeUndefined();
    expect(JSON.stringify(framesOfType(ws, "run.grants"))).not.toContain(
      replacedKey,
    );
  });

  test("a failed settle leaves resolution mail parked for a later record", async () => {
    const undelivered: { rawMessage: string; recipients: string[] }[] = [];
    let ready = false;
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants() {
          if (!ready) return { outcome: "notReady" };
          return { outcome: "materialized", stepGrants: [] };
        },
      },
    });
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
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();
    settleSenderDeploy(router, {
      failed: "init failed",
    });
    await tick();
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);
    expect(undelivered).toEqual([]);

    ready = true;
    settleSenderDeploy(router, {
      recorded: "recipient-key",
    });
    await tick();
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(1);
    expect(undelivered).toEqual([]);
    expect(JSON.stringify(framesOfType(ws, "run.grants"))).not.toContain(
      "recipient-key",
    );
  });

  test("a settle during an in-flight not-ready materialize delivers once", async () => {
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let calls = 0;
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants() {
          calls += 1;
          if (calls === 1) {
            await gate;
            return { outcome: "notReady" };
          }
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
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();
    expect(calls).toBe(1);
    settleSenderDeploy(router, {
      recorded: "recipient-key",
    });
    await tick();
    release?.();
    await tick();
    await tick();
    expect(calls).toBe(2);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(1);
    expect(JSON.stringify(framesOfType(ws, "run.grants"))).not.toContain(
      "recipient-key",
    );
  });

  test("a failed settle during an in-flight not-ready materialize leaves the hold", async () => {
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let ready = false;
    let calls = 0;
    const undelivered: { recipients: string[] }[] = [];
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants() {
          calls += 1;
          if (calls === 1) {
            await gate;
            return { outcome: "notReady" };
          }
          if (!ready) return { outcome: "notReady" };
          return { outcome: "materialized", stepGrants: [] };
        },
      },
    });
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push({ recipients: event.recipients });
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();
    expect(calls).toBe(1);
    settleSenderDeploy(router, {
      failed: "init failed",
    });
    await tick();
    release?.();
    await tick();
    await tick();
    expect(calls).toBe(1);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);
    expect(undelivered).toEqual([]);

    ready = true;
    settleSenderDeploy(router, {
      recorded: "recipient-key",
    });
    await tick();
    expect(calls).toBe(2);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(1);
    expect(undelivered).toEqual([]);
    expect(JSON.stringify(framesOfType(ws, "run.grants"))).not.toContain(
      "recipient-key",
    );
  });

  test("a recorded wake still delivers when a failed settle follows it", async () => {
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let calls = 0;
    const undelivered: { recipients: string[] }[] = [];
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants() {
          calls += 1;
          if (calls === 1) {
            await gate;
            return { outcome: "notReady" };
          }
          return { outcome: "materialized", stepGrants: [] };
        },
      },
    });
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push({ recipients: event.recipients });
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();
    expect(calls).toBe(1);
    settleSenderDeploy(router, {
      recorded: "recipient-key",
    });
    settleSenderDeploy(router, {
      failed: "init failed",
    });
    await tick();
    release?.();
    await tick();
    await tick();
    expect(calls).toBe(2);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(1);
    expect(undelivered).toEqual([]);
    expect(JSON.stringify(framesOfType(ws, "run.grants"))).not.toContain(
      "recipient-key",
    );
  });

  // Distinct from the ping deadline and the mail-ack retry so a manual
  // scheduler can fire this hold without closing the socket or redelivering.
  const resolutionTtlMs = 45_000;

  test("a recorded settle during materialize is not dropped by the resolution TTL", async () => {
    const ttl = createManualRetries(resolutionTtlMs);
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let calls = 0;
    const undelivered: { recipients: string[] }[] = [];
    const router = createAllocatedRouter({
      mailHoldTTLMs: resolutionTtlMs,
      scheduleTimeout: ttl.scheduleTimeout,
      lookups: {
        async materializeMailTriggeredRunGrants() {
          calls += 1;
          if (calls === 1) {
            await gate;
            return { outcome: "notReady" };
          }
          return { outcome: "materialized", stepGrants: [] };
        },
      },
    });
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push({ recipients: event.recipients });
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();
    expect(calls).toBe(1);
    expect(ttl.armedCount()).toBe(0);
    settleSenderDeploy(router, {
      recorded: "recipient-key",
    });
    await tick();
    expect(ttl.armedCount()).toBe(0);
    release?.();
    await tick();
    await tick();
    expect(calls).toBe(2);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(1);
    expect(undelivered).toEqual([]);
    expect(JSON.stringify(framesOfType(ws, "run.grants"))).not.toContain(
      "recipient-key",
    );
  });

  test("a materialized return is not dropped by the resolution TTL", async () => {
    const ttl = createManualRetries(resolutionTtlMs);
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let calls = 0;
    const undelivered: { recipients: string[] }[] = [];
    const router = createAllocatedRouter({
      mailHoldTTLMs: resolutionTtlMs,
      scheduleTimeout: ttl.scheduleTimeout,
      lookups: {
        async materializeMailTriggeredRunGrants() {
          calls += 1;
          if (calls === 1) await gate;
          return { outcome: "materialized", stepGrants: [] };
        },
      },
    });
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push({ recipients: event.recipients });
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();
    expect(calls).toBe(1);
    expect(ttl.armedCount()).toBe(0);
    release?.();
    await tick();
    await tick();
    expect(calls).toBe(1);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(1);
    expect(undelivered).toEqual([]);
  });

  test("an idle resolution hold undelivers when its TTL fires", async () => {
    const ttl = createManualRetries(resolutionTtlMs);
    const undelivered: { recipients: string[] }[] = [];
    const router = createAllocatedRouter({
      mailHoldTTLMs: resolutionTtlMs,
      scheduleTimeout: ttl.scheduleTimeout,
      lookups: {
        async materializeMailTriggeredRunGrants() {
          return { outcome: "notReady" };
        },
      },
    });
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push({ recipients: event.recipients });
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);
    expect(ttl.armedCount()).toBe(1);
    ttl.fireNext();
    expect(undelivered).toEqual([
      { recipients: [TEST_IDENTITY.workflowRunAddress] },
    ]);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);
    expect(ttl.armedCount()).toBe(0);
  });

  test("a settle during an in-flight ready materialize delivers the mail once", async () => {
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let calls = 0;
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants() {
          calls += 1;
          if (calls === 1) await gate;
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
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();
    settleSenderDeploy(router, {
      recorded: "recipient-key",
    });
    await tick();
    release?.();
    await tick();
    await tick();
    expect(calls).toBe(1);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(1);
    expect(framesOfType(ws, "run.grants")).toHaveLength(1);
    expect(JSON.stringify(framesOfType(ws, "run.grants"))).not.toContain(
      "recipient-key",
    );
  });

  test("a thrown materializer does not undeliver the held recipient after the TTL", async () => {
    const undelivered: { recipients: string[] }[] = [];
    const router = createAllocatedRouter({
      mailHoldTTLMs: 30,
      lookups: {
        async materializeMailTriggeredRunGrants() {
          throw new Error("definition has no approved grant snapshot");
        },
      },
    });
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push({ recipients: event.recipients });
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        generation: 1,
        rawMessage,
        recipients: [TEST_IDENTITY.workflowRunAddress, "other@example.test"],
      }),
    );
    await tick();
    const immediate = undelivered.map((event) => event.recipients);
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);
    expect(immediate).toEqual([["other@example.test"]]);
    expect(undelivered.map((event) => event.recipients)).toEqual([
      ["other@example.test"],
    ]);
  });

  // An address with no route is reported on the attempt that parks the workflow
  // address, not when the hold later settles.
  const later = "later@example.test";
  const mixedRecipients = [TEST_IDENTITY.workflowRunAddress, later];

  function inboundAddresses(ws: { sent: string[] }, address: string): number {
    return framesOfType(ws, "mail.inbound").filter(
      (frame) => frame["agentAddress"] === address,
    ).length;
  }

  async function sendMixedResolutionMail(ready: () => boolean) {
    const undelivered: { recipients: string[] }[] = [];
    const router = createAllocatedRouter({
      lookups: {
        async materializeMailTriggeredRunGrants() {
          if (!ready()) return { outcome: "notReady" };
          return { outcome: "materialized", stepGrants: [] };
        },
      },
    });
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push({ recipients: event.recipients });
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: TEST_IDENTITY.workflowRunAddress,
        generation: 1,
        rawMessage,
        recipients: mixedRecipients,
      }),
    );
    await tick();
    return { router, ws, undelivered };
  }

  test("a not-ready workflow recipient reports an unrouted co-recipient on this attempt", async () => {
    let ready = false;
    const { router, ws, undelivered } = await sendMixedResolutionMail(
      () => ready,
    );

    expect(inboundAddresses(ws, TEST_IDENTITY.workflowRunAddress)).toBe(0);
    expect(undelivered.map((event) => event.recipients)).toEqual([[later]]);

    ready = true;
    settleSenderDeploy(router, {
      recorded: "recipient-key",
    });
    await tick();

    expect(inboundAddresses(ws, TEST_IDENTITY.workflowRunAddress)).toBe(1);
    expect(undelivered.map((event) => event.recipients)).toEqual([[later]]);
    expect(JSON.stringify(framesOfType(ws, "run.grants"))).not.toContain(
      "recipient-key",
    );
  });

  test("a failed resolution settle leaves the parked workflow recipient", async () => {
    let ready = false;
    const { router, ws, undelivered } = await sendMixedResolutionMail(
      () => ready,
    );

    expect(inboundAddresses(ws, TEST_IDENTITY.workflowRunAddress)).toBe(0);
    expect(undelivered.map((event) => event.recipients)).toEqual([[later]]);

    settleSenderDeploy(router, {
      failed: "init failed",
    });
    await tick();

    expect(inboundAddresses(ws, TEST_IDENTITY.workflowRunAddress)).toBe(0);
    expect(undelivered.map((event) => event.recipients)).toEqual([[later]]);

    ready = true;
    settleSenderDeploy(router, {
      recorded: "recipient-key",
    });
    await tick();

    expect(inboundAddresses(ws, TEST_IDENTITY.workflowRunAddress)).toBe(1);
    expect(undelivered.map((event) => event.recipients)).toEqual([[later]]);
    expect(JSON.stringify(framesOfType(ws, "run.grants"))).not.toContain(
      "recipient-key",
    );
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
        generation: 1,
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
        generation: 1,
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
        generation: 1,
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
        generation: 1,
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
        generation: 1,
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
        generation: 1,
        rawMessage: oversized,
        recipients: [TEST_IDENTITY.workflowRunAddress],
      }),
    );
    await tick();

    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);
    expect(framesOfType(ws, "run.grants")).toHaveLength(0);
  });
});

describe("SidecarRouter mail send check", () => {
  const rawMessage = btoa(
    "From: sender@example.test\r\nTo: someone@example.test\r\nMessage-ID: <send-check@example.test>\r\n\r\nbody",
  );
  const SENDER = TEST_IDENTITY.workflowRunAddress;
  const ALLOWED = "allowed@example.test";
  const REFUSED = "refused@example.test";

  // The relay logs why it refused a recipient; a test checks the reason here.
  const logLines: string[] = [];
  let savedLogConfig: ReturnType<typeof getConfig>;

  beforeAll(() => {
    savedLogConfig = getConfig();
    configureSync({
      reset: true,
      sinks: {
        capture: (record) => {
          logLines.push(record.message.join(""));
        },
      },
      loggers: [
        { category: [], lowestLevel: "debug", sinks: ["capture"] },
        {
          category: ["logtape", "meta"],
          lowestLevel: "warning",
          sinks: ["capture"],
        },
      ],
    });
  });

  afterAll(() => {
    if (!savedLogConfig) {
      throw new Error(
        "no logging configuration was captured before this suite replaced it",
      );
    }
    configureSync({ reset: true, ...savedLogConfig });
  });

  beforeEach(() => {
    logLines.length = 0;
  });

  function decide(
    allowed: (recipient: string) => boolean,
  ): NonNullable<SidecarLookups["authorizeMailSend"]> {
    return async ({ recipients }) => ({
      ok: true,
      decisions: recipients.map((recipient) =>
        allowed(recipient)
          ? {
              recipient,
              outcome: "evaluated",
              allowed: true,
              effect: "allow",
              grant: null,
            }
          : {
              recipient,
              outcome: "evaluated",
              allowed: false,
              effect: "deny",
              grant: {
                id: "grt_deny_refused",
                resource: `mail.send:domain:example.test`,
                action: "send",
                effect: "deny",
                origin: "role",
                specificity: 0,
              },
            },
      ),
    });
  }

  function send(
    router: AllocatedRouter,
    ws: Parameters<AllocatedRouter["handleMessage"]>[0],
    recipients: string[],
  ): void {
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: SENDER,
        generation: 1,
        rawMessage,
        recipients,
      }),
    );
  }

  // The router handles one socket's `mail.outbound` frames in order, so an
  // audit copy sent after a mail is persisted only once the relay has finished
  // with that mail. A test awaits the persist before it asserts what the relay
  // did not do.
  function persistBarrier(): {
    persistMail: NonNullable<SidecarLookups["persistMail"]>;
    persisted: Promise<undefined>;
  } {
    const persisted = Promise.withResolvers<undefined>();
    return {
      persisted: persisted.promise,
      persistMail: async () => {
        persisted.resolve(undefined);
        return [];
      },
    };
  }

  function sendAuditCopy(
    router: AllocatedRouter,
    ws: Parameters<AllocatedRouter["handleMessage"]>[0],
  ): void {
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: SENDER,
        generation: 1,
        rawMessage,
        recipients: [ALLOWED],
        delivered: true,
      }),
    );
  }

  test("routes, mints and materializes only the recipients it allows", async () => {
    const calls: unknown[] = [];
    const undelivered: string[][] = [];
    const router = createAllocatedRouter({
      lookups: {
        authorizeMailSend: decide((recipient) => recipient === ALLOWED),
        async materializeMailTriggeredRunGrants(args) {
          calls.push({ materialize: args.agentAddress });
          return { outcome: "materialized", stepGrants: [] };
        },
        async mintCorrespondentGrant(args) {
          calls.push({ mint: args.recipientAddresses });
        },
      },
    });
    const done = Promise.withResolvers<undefined>();
    router.events.on("mail.outbound.undelivered", ({ recipients }) => {
      undelivered.push(recipients);
      done.resolve(undefined);
    });
    const ws = await connectAllocated(router, [SENDER]);

    send(router, ws, [SENDER, ALLOWED, REFUSED]);
    await done.promise;

    expect(undelivered).toEqual([[ALLOWED]]);
    expect(calls).toEqual([{ mint: [ALLOWED] }]);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);
    expect(
      logLines.filter((line) => line.startsWith("Refused mail from")),
    ).toEqual([
      `Refused mail from ${SENDER} to ${SENDER}: grant grt_deny_refused (deny) decides it`,
      `Refused mail from ${SENDER} to ${REFUSED}: grant grt_deny_refused (deny) decides it`,
    ]);
  });

  test("delivers to an allowed run recipient", async () => {
    const router = createAllocatedRouter({
      lookups: {
        authorizeMailSend: decide(() => true),
        async materializeMailTriggeredRunGrants() {
          return { outcome: "materialized", stepGrants: [] };
        },
      },
    });
    const ws = await connectAllocated(router, [SENDER]);

    send(router, ws, [SENDER]);
    await ws.awaitSent((sent) =>
      sent.some((raw) => raw.includes('"mail.inbound"')),
    );

    expect(framesOfType(ws, "mail.inbound")).toHaveLength(1);
  });

  test("refuses a recipient no grant allows", async () => {
    const barrier = persistBarrier();
    const calls: unknown[] = [];
    const router = createAllocatedRouter({
      lookups: {
        async authorizeMailSend({ recipients }) {
          return {
            ok: true,
            decisions: recipients.map((recipient) => ({
              recipient,
              outcome: "evaluated",
              allowed: false,
              effect: null,
              grant: null,
            })),
          };
        },
        persistMail: barrier.persistMail,
        async materializeMailTriggeredRunGrants(args) {
          calls.push({ materialize: args.agentAddress });
          return { outcome: "materialized", stepGrants: [] };
        },
        async mintCorrespondentGrant(args) {
          calls.push({ mint: args.recipientAddresses });
        },
      },
    });
    const undelivered: unknown[] = [];
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push(event);
    });
    const ws = await connectAllocated(router, [SENDER]);

    send(router, ws, [SENDER, REFUSED]);
    sendAuditCopy(router, ws);
    await barrier.persisted;

    expect(undelivered).toEqual([]);
    expect(calls).toEqual([]);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);
    expect(logLines).toContain(
      `Refused mail from ${SENDER} to ${REFUSED}: no send grant allows it`,
    );
  });

  test.each([
    {
      name: "records only the recipients it allows in the audit copy",
      allowed: (recipient: string) => recipient === ALLOWED,
      stored: [ALLOWED],
    },
    {
      name: "keeps only the sender's record of an audit copy it refuses in full",
      allowed: () => false,
      stored: [],
    },
  ])("$name", async ({ allowed, stored }) => {
    const persisted = Promise.withResolvers<string[]>();
    const router = createAllocatedRouter({
      lookups: {
        authorizeMailSend: decide(allowed),
        async persistMail({ recipients }) {
          persisted.resolve(recipients);
          return [];
        },
      },
    });
    const ws = await connectAllocated(router, [SENDER]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: SENDER,
        generation: 1,
        rawMessage,
        recipients: [ALLOWED, REFUSED],
        delivered: true,
      }),
    );

    expect(await persisted.promise).toEqual([...stored]);
    expect(logLines).toContain(
      `Refused mail from ${SENDER} to ${REFUSED}: grant grt_deny_refused (deny) decides it`,
    );
  });

  test("refuses every recipient without an authorizer", async () => {
    const barrier = persistBarrier();
    const router = createSidecarRouter({
      withExecutableWorkflowRun: async (_target, deliver) => deliver(),
      ...sidecarAuth(() => [TEST_IDENTITY]),
      validateSidecarIdentity: async () => true,
      requestTimeoutMs: 500,
      lookups: { persistMail: barrier.persistMail },
    });
    router.fenceAllocation(TEST_TARGET.allocationId, TEST_TARGET.generation);
    const undelivered: unknown[] = [];
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push(event);
    });
    const ws = await connectAllocated(router, [SENDER]);

    send(router, ws, [ALLOWED]);
    sendAuditCopy(router, ws);
    await barrier.persisted;

    expect(undelivered).toEqual([]);
    expect(
      logLines.some((line) =>
        line.includes("No mail send authorizer is wired"),
      ),
    ).toBe(true);
  });

  test("refuses only a recipient whose coordinates could not be resolved", async () => {
    const persisted = Promise.withResolvers<string[]>();
    const router = createAllocatedRouter({
      lookups: {
        async authorizeMailSend({ recipients }) {
          return {
            ok: true,
            decisions: recipients.map((recipient) =>
              recipient === REFUSED
                ? { recipient, outcome: "unresolved", allowed: false }
                : {
                    recipient,
                    outcome: "evaluated",
                    allowed: true,
                    effect: "allow",
                    grant: null,
                  },
            ),
          };
        },
        async persistMail({ recipients }) {
          persisted.resolve(recipients);
          return [];
        },
      },
    });
    const ws = await connectAllocated(router, [SENDER]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: SENDER,
        generation: 1,
        rawMessage,
        recipients: [ALLOWED, REFUSED],
        delivered: true,
      }),
    );

    expect(await persisted.promise).toEqual([ALLOWED]);
    expect(logLines).toContain(
      `Refused mail from ${SENDER} to ${REFUSED}: its coordinates could not be resolved`,
    );
  });

  test("refuses every recipient of an unresolved sender or a fault", async () => {
    const outcomes: (() => ReturnType<
      NonNullable<SidecarLookups["authorizeMailSend"]>
    >)[] = [
      async () => ({ ok: false, reason: "sender_unresolved" }),
      async () => {
        throw new Error("grant store down");
      },
    ];
    let call = 0;
    const barrier = persistBarrier();
    const router = createAllocatedRouter({
      lookups: {
        async authorizeMailSend() {
          const outcome = outcomes[call];
          call += 1;
          if (outcome === undefined) throw new Error("unexpected call");
          return outcome();
        },
        persistMail: barrier.persistMail,
      },
    });
    const undelivered: unknown[] = [];
    router.events.on("mail.outbound.undelivered", (event) => {
      undelivered.push(event);
    });
    const ws = await connectAllocated(router, [SENDER]);

    send(router, ws, [ALLOWED]);
    send(router, ws, [ALLOWED]);
    sendAuditCopy(router, ws);
    await barrier.persisted;

    expect(undelivered).toEqual([]);
    for (const reason of ["it names no deployment", "grant store down"]) {
      expect(logLines.some((line) => line.includes(reason))).toBe(true);
    }
  });

  test("a parked mail whose sender fails to deploy is reported without its refused recipients", async () => {
    const parked = Promise.withResolvers<undefined>();
    const router = createAllocatedRouter({
      lookups: {
        authorizeMailSend: decide((recipient) => recipient !== REFUSED),
        async materializeMailTriggeredRunGrants() {
          return { outcome: "materialized", stepGrants: [] };
        },
        // The relay parks the mail before it resolves the key, so this call
        // means the mail is parked.
        async resolveSenderKey() {
          parked.resolve(undefined);
          return null;
        },
      },
    });
    const undelivered = Promise.withResolvers<string[]>();
    router.events.on("mail.outbound.undelivered", ({ recipients }) => {
      undelivered.resolve(recipients);
    });
    const ws = await connectAllocated(router, [SENDER]);
    const attempt = { ...TEST_TARGET, leaseId: "send-check-park" };
    router.noteSenderDeployStarted(SENDER, attempt);

    // The run recipient makes the mail park while the sender's key is
    // unresolved; the refused external recipient is dropped before that.
    send(router, ws, [SENDER, ALLOWED, REFUSED]);
    await parked.promise;
    router.noteSenderDeploySettled(attempt, { failed: "deploy timed out" });

    expect(await undelivered.promise).toEqual([SENDER, ALLOWED]);
  });

  test("a grant revoked while the mail is parked refuses it on the re-drive", async () => {
    let revoked = false;
    const parked = Promise.withResolvers<undefined>();
    const materialized: string[] = [];
    const router = createAllocatedRouter({
      lookups: {
        // The run recipient parks the mail and loses its grant while parked.
        // The external recipient stays allowed, so its undelivered report
        // marks the end of the re-drive.
        authorizeMailSend: decide(
          (recipient) => recipient === ALLOWED || !revoked,
        ),
        async materializeMailTriggeredRunGrants(args) {
          materialized.push(args.agentAddress);
          return { outcome: "materialized", stepGrants: [] };
        },
        async resolveSenderKey() {
          parked.resolve(undefined);
          return null;
        },
      },
    });
    const undelivered = Promise.withResolvers<string[]>();
    router.events.on("mail.outbound.undelivered", ({ recipients }) => {
      undelivered.resolve(recipients);
    });
    const ws = await connectAllocated(router, [SENDER]);
    const attempt = { ...TEST_TARGET, leaseId: "send-check-revoke" };
    router.noteSenderDeployStarted(SENDER, attempt);

    send(router, ws, [SENDER, ALLOWED]);
    await parked.promise;
    revoked = true;
    router.noteSenderDeploySettled(attempt, { recorded: "ab".repeat(32) });

    expect(await undelivered.promise).toEqual([ALLOWED]);
    expect(materialized).toEqual([]);
    expect(framesOfType(ws, "mail.inbound")).toHaveLength(0);
  });

  test("a refused run recipient does not hold an allowed external recipient", async () => {
    const router = createAllocatedRouter({
      lookups: {
        authorizeMailSend: decide((recipient) => recipient === ALLOWED),
        async materializeMailTriggeredRunGrants() {
          return { outcome: "materialized", stepGrants: [] };
        },
        async resolveSenderKey() {
          return null;
        },
      },
    });
    const undelivered = Promise.withResolvers<string[]>();
    router.events.on("mail.outbound.undelivered", ({ recipients }) => {
      undelivered.resolve(recipients);
    });
    const ws = await connectAllocated(router, [SENDER]);
    router.noteSenderDeployStarted(SENDER, {
      ...TEST_TARGET,
      leaseId: "send-check-no-park",
    });

    send(router, ws, [SENDER, ALLOWED]);

    expect(await undelivered.promise).toEqual([ALLOWED]);
  });
});

describe("SidecarRouter pending mail from two senders that share a Message-ID", () => {
  const OTHER_SENDER = "other@example.test";
  const SHARED_ID = "<shared@example.test>";

  function ackFrame(fields: {
    generation?: number;
    authenticatedSender: string;
  }): string {
    return JSON.stringify({
      type: "mail.inbound.ack",
      agentAddress: TEST_IDENTITY.workflowRunAddress,
      generation: fields.generation ?? TEST_TARGET.generation,
      messageId: SHARED_ID,
      authenticatedSender: fields.authenticatedSender,
    });
  }

  async function routeFromBoth() {
    const retries = createManualRetries(20);
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 20,
      mailAckMaxRetries: 5,
      mailHoldTTLMs: 60_000,
      scheduleTimeout: retries.scheduleTimeout,
    });
    const acknowledged: { messageId: string; deliveryKey: string }[] = [];
    router.events.on("mail.inbound.acknowledged", (event) => {
      acknowledged.push(event);
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    await router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "YQ==",
      TEST_SENDER,
      SHARED_ID,
    );
    await router.routeMail(
      TEST_IDENTITY.workflowRunAddress,
      "Yg==",
      OTHER_SENDER,
      SHARED_ID,
    );
    return { retries, router, acknowledged, ws };
  }

  // The senders whose mail is still pending, read from what a reconnect
  // redelivers.
  async function pendingSendersAfterReconnect(
    router: AllocatedRouter,
    ws: Parameters<AllocatedRouter["handleClose"]>[0],
  ): Promise<unknown[]> {
    router.handleClose(ws);
    const second = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    return framesOfType(second, "mail.inbound")
      .filter((frame) => frame["messageId"] === SHARED_ID)
      .map((frame) => frame["authenticatedSender"]);
  }

  test("tracks each sender's mail as its own pending entry", async () => {
    const { retries } = await routeFromBoth();
    expect(retries.armedCount()).toBe(2);
  });

  test("an ack that names a sender resolves only that sender's mail", async () => {
    const { retries, router, acknowledged, ws } = await routeFromBoth();
    router.handleMessage(ws, ackFrame({ authenticatedSender: TEST_SENDER }));
    await waitUntil(() => retries.armedCount() === 1);
    expect(acknowledged).toEqual([
      expect.objectContaining({
        messageId: SHARED_ID,
        deliveryKey: await deriveDeliveryKey(TEST_SENDER, SHARED_ID),
      }),
    ]);
    expect(await pendingSendersAfterReconnect(router, ws)).toEqual([
      OTHER_SENDER,
    ]);
  });

  test("an ack that names a sender with no pending mail leaves the others pending", async () => {
    const { retries, router, acknowledged, ws } = await routeFromBoth();
    router.handleMessage(
      ws,
      ackFrame({ authenticatedSender: "third@example.test" }),
    );
    await waitUntil(() => acknowledged.length === 1);
    expect(retries.armedCount()).toBe(2);
    expect(acknowledged[0]?.deliveryKey).toBe(
      await deriveDeliveryKey("third@example.test", SHARED_ID),
    );
  });

  test("an ack for a generation this sidecar does not route resolves nothing", async () => {
    const { retries, router, ws } = await routeFromBoth();
    router.handleMessage(
      ws,
      ackFrame({
        generation: TEST_TARGET.generation + 1,
        authenticatedSender: TEST_SENDER,
      }),
    );
    await tick();
    expect(retries.armedCount()).toBe(2);
  });

  test("an ack that names no sender is refused at the frame boundary", async () => {
    const { retries, router, acknowledged, ws } = await routeFromBoth();
    const senderless = JSON.stringify({
      type: "mail.inbound.ack",
      agentAddress: TEST_IDENTITY.workflowRunAddress,
      generation: TEST_TARGET.generation,
      messageId: SHARED_ID,
    });
    expect(
      MailInboundAckFrame(JSON.parse(senderless)) instanceof type.errors,
    ).toBe(true);
    router.handleMessage(ws, senderless);
    await tick();
    expect(acknowledged).toEqual([]);
    expect(retries.armedCount()).toBe(2);
  });

  test("an ack whose sender holds NUL is refused at the frame boundary", () => {
    const parsed = MailInboundAckFrame(
      JSON.parse(ackFrame({ authenticatedSender: "a\u0000b" })),
    );
    expect(parsed instanceof type.errors).toBe(true);
  });

  test("tracks dispatched mail per sender and resolves it by the ack's sender", async () => {
    const retries = createManualRetries(20);
    const router = createAllocatedRouter({
      mailAckRetryIntervalMs: 20,
      mailAckMaxRetries: 5,
      scheduleTimeout: retries.scheduleTimeout,
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    for (const [sender, raw] of [
      [TEST_SENDER, "YQ=="],
      [OTHER_SENDER, "Yg=="],
    ] as const) {
      await router.sendWorkflowRunDispatchToAllocation(
        TEST_TARGET,
        TEST_IDENTITY.workflowRunAddress,
        TEST_IDENTITY.anchorRunId,
        [],
        raw,
        sender,
        SHARED_ID,
      );
    }
    expect(retries.armedCount()).toBe(2);
    router.handleMessage(ws, ackFrame({ authenticatedSender: OTHER_SENDER }));
    await waitUntil(() => retries.armedCount() === 1);
    expect(await pendingSendersAfterReconnect(router, ws)).toEqual([
      TEST_SENDER,
    ]);
  });

  test("a reconnect redelivers both senders' mail", async () => {
    const { router, ws } = await routeFromBoth();
    router.handleClose(ws);
    const second = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    expect(
      framesOfType(second, "mail.inbound")
        .filter((frame) => frame["messageId"] === SHARED_ID)
        .map((frame) => frame["authenticatedSender"])
        .sort(),
    ).toEqual([OTHER_SENDER, TEST_SENDER].sort());
  });
});
