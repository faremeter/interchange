// When the router removes the mailbox a deploy provisioned for an address.
//
// The hub owns both ends of a mailbox's life because it owns the address: it
// creates the mailbox inside the deploy, so it has to remove it on the undeploy
// or the mailbox outlives every reader it could ever have. A deployment has one
// run and is never redeployed to the same address, so nothing reclaims a
// mailbox left behind -- it costs disk for the life of the server.
//
// The ordering is the part worth pinning down, and it is not "whenever the
// undeploy is requested":
//
//   - Not before the ack. Until the sidecar acks, it still holds IMAP sessions
//     on that mailbox, and removing it under a live session breaks the teardown
//     the sidecar is in the middle of.
//   - Not at all when the undeploy failed. A timeout or an error may have left
//     the deployment running, and taking the mailbox away from a deployment
//     still reading it is worse than leaving one behind.
//
// These run against the router with a mock socket, so the ack, the timeout and
// the error are each driven exactly rather than raced for.

import { describe, expect, test } from "bun:test";

import {
  connectAllocated,
  createAllocatedRouter,
  TEST_IDENTITY,
  tick,
} from "./sidecar-handler.test-helpers";

const ADDRESS = TEST_IDENTITY.workflowRunAddress;

/** Records the addresses the router asked to have deprovisioned. */
function recordingDeprovisioner() {
  const removed: string[] = [];
  return {
    removed,
    deprovisionMailbox: async (address: string) => {
      removed.push(address);
    },
  };
}

describe("mailbox removal on undeploy", () => {
  test("the mailbox is not removed before the sidecar acks", async () => {
    const { removed, deprovisionMailbox } = recordingDeprovisioner();
    const router = createAllocatedRouter({ lookups: { deprovisionMailbox } });
    const ws = await connectAllocated(router, [ADDRESS]);

    const undeploy = router.sendAgentUndeploy(ADDRESS, "session-ended");
    await tick();

    // The frame is out and the sidecar is still tearing down. Its IMAP sessions
    // on this mailbox are live until it acks.
    expect(removed).toEqual([]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.undeploy.ack",
        agentAddress: ADDRESS,
        statePushed: true,
      }),
    );
    await undeploy;

    expect(removed).toEqual([ADDRESS]);
  });

  test("an undeploy that times out leaves the mailbox in place", async () => {
    const { removed, deprovisionMailbox } = recordingDeprovisioner();
    const router = createAllocatedRouter({ lookups: { deprovisionMailbox } });
    await connectAllocated(router, [ADDRESS]);

    // No ack. The helper's router times out at 500ms.
    await expect(
      router.sendAgentUndeploy(ADDRESS, "session-ended"),
    ).rejects.toThrow(/timed out/);

    expect(removed).toEqual([]);
  });

  test("an undeploy the sidecar refuses leaves the mailbox in place", async () => {
    const { removed, deprovisionMailbox } = recordingDeprovisioner();
    const router = createAllocatedRouter({ lookups: { deprovisionMailbox } });
    const ws = await connectAllocated(router, [ADDRESS]);

    const undeploy = router.sendAgentUndeploy(ADDRESS, "session-ended");
    await tick();
    // `agent.error` is how a sidecar refuses an undeploy: there is no
    // undeploy-specific error frame, so one frame rejects whichever of the
    // deploy and undeploy round-trips is pending for the address.
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.error",
        agentAddress: ADDRESS,
        error: "state push failed",
      }),
    );

    await expect(undeploy).rejects.toThrow(/state push failed/);
    expect(removed).toEqual([]);
  });

  test("a hub with no mail infrastructure undeploys unchanged", async () => {
    // The hook is absent on every hub that provisions no mailboxes, which is
    // every hub in the tree today. Its absence must not be a failure path.
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router, [ADDRESS]);

    const undeploy = router.sendAgentUndeploy(ADDRESS, "session-ended");
    await tick();
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.undeploy.ack",
        agentAddress: ADDRESS,
        statePushed: true,
      }),
    );

    await undeploy;
    expect(router.getRoutableAddresses()).toEqual([]);
  });

  test("a removal that throws does not fail the undeploy that succeeded", async () => {
    // The contract says the hook does not throw. If an implementation breaks
    // that, the undeploy has still happened -- the sidecar acked -- so
    // reporting a failure to the caller would describe the wrong thing. The
    // throw is reported through the log instead of being swallowed silently.
    const router = createAllocatedRouter({
      lookups: {
        deprovisionMailbox: () => {
          throw new Error("docker exec: no such container");
        },
      },
    });
    const ws = await connectAllocated(router, [ADDRESS]);

    const undeploy = router.sendAgentUndeploy(ADDRESS, "session-ended");
    await tick();
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.undeploy.ack",
        agentAddress: ADDRESS,
        statePushed: true,
      }),
    );

    await undeploy;
    // Routing is torn down regardless: the mailbox is a resource, not a route.
    expect(router.getRoutableAddresses()).toEqual([]);
  });
});
