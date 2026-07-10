import { describe, test, expect } from "bun:test";

import type { InferenceSource } from "@intx/types/runtime";
import type { RepoId, RepoStore } from "@intx/hub-sessions";

import {
  createDeploymentAddressRegistry,
  createMultistepDrainRouter,
  createMultistepMailRouter,
  createMultistepSignalRouter,
  createMultistepSourcesRouter,
  createWorkflowRunPackClient,
  createWorkflowRunPackPushingRepoStore,
} from "./workflow-run-pack-client";

function createRecordingUnderlyingRepoStore(): {
  store: RepoStore;
  preserveCalls: {
    principal: { kind: string };
    repoId: RepoId;
    ref: string;
  }[];
  packs: { principal: { kind: string }; repoId: RepoId; ref: string }[];
  packedTipCommits: { repoId: RepoId; ref: string; commitSha: string }[];
} {
  const preserveCalls: {
    principal: { kind: string };
    repoId: RepoId;
    ref: string;
  }[] = [];
  const packs: {
    principal: { kind: string };
    repoId: RepoId;
    ref: string;
  }[] = [];
  const packedTipCommits: {
    repoId: RepoId;
    ref: string;
    commitSha: string;
  }[] = [];
  const stub: Partial<RepoStore> = {
    getRepoDir(_repoId: RepoId): string {
      return "/tmp/unused";
    },
    async writeTreePreservingPrefix(principal, repoId, ref, args) {
      preserveCalls.push({ principal, repoId, ref });
      await args.merge(new Map());
      return {
        commitSha: `sha-${String(preserveCalls.length)}`,
        newlyTerminalRuns: [],
      };
    },
    async resolveRef(_principal, _repoId, _ref) {
      // The client's empty-delta guard compares the current ref tip against
      // the last commit it acked. Return a fixed tip distinct from the
      // `createPack` sha so the guard never short-circuits these tests: the
      // client acks `stub-pack-sha`, so a tip of `stub-tip-sha` always has
      // un-shipped work.
      return "stub-tip-sha";
    },
    async createPack(principal, repoId, ref) {
      packs.push({ principal, repoId, ref });
      return {
        pack: new Uint8Array([0xab, 0xcd]),
        commitSha: "stub-pack-sha",
        ref,
      };
    },
    commitPackedTip(repoId, ref, commitSha) {
      packedTipCommits.push({ repoId, ref, commitSha });
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- in-test stub; the unused RepoStore methods are guarded by the Proxy below
  const store = new Proxy(stub as RepoStore, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (value !== undefined) return value;
      return () => {
        throw new Error(
          `stub RepoStore: ${String(prop)} not implemented for this test`,
        );
      };
    },
  });
  return { store, preserveCalls, packs, packedTipCommits };
}

describe("createWorkflowRunPackClient", () => {
  test("push builds a pack under the supervisor principal, forwards it to the hub link, and commits the packed tip on the ack", async () => {
    const { store, packs, packedTipCommits } =
      createRecordingUnderlyingRepoStore();
    const sent: {
      agentAddress: string;
      repoId: RepoId;
      pack: Uint8Array;
      ref: string;
      commitSha: string;
    }[] = [];
    const client = createWorkflowRunPackClient({
      substrate: store,
      hubLink: {
        async pushWorkflowRunPack(opts) {
          sent.push(opts);
        },
      },
    });

    await client.push({
      agentAddress: "agent@example.com",
      repoId: { kind: "workflow-run", id: "agent-example-com" },
      ref: "refs/heads/main",
    });

    expect(packs).toHaveLength(1);
    expect(packs[0]?.principal.kind).toBe("supervisor");
    expect(packs[0]?.repoId.kind).toBe("workflow-run");
    expect(sent).toHaveLength(1);
    expect(sent[0]?.agentAddress).toBe("agent@example.com");
    expect(sent[0]?.commitSha).toBe("stub-pack-sha");
    expect(sent[0]?.ref).toBe("refs/heads/main");
    // The ack (pushWorkflowRunPack resolving) commits the packed tip
    // for the shipped commit, so the next createPack ships incrementally.
    expect(packedTipCommits).toHaveLength(1);
    expect(packedTipCommits[0]?.repoId.id).toBe("agent-example-com");
    expect(packedTipCommits[0]?.ref).toBe("refs/heads/main");
    expect(packedTipCommits[0]?.commitSha).toBe("stub-pack-sha");
  });

  test("push does not commit the packed tip when the hub link rejects the transfer", async () => {
    const { store, packedTipCommits } = createRecordingUnderlyingRepoStore();
    const client = createWorkflowRunPackClient({
      substrate: store,
      hubLink: {
        pushWorkflowRunPack: () =>
          Promise.reject(new Error("transfer cancelled: Connection lost")),
      },
    });

    await expect(
      client.push({
        agentAddress: "agent@example.com",
        repoId: { kind: "workflow-run", id: "agent-example-com" },
        ref: "refs/heads/main",
      }),
    ).rejects.toThrow(/Connection lost/);

    // A cancelled/rejected transfer never acks, so the packed tip must
    // not advance: the retry rebuild re-includes the un-acked commits.
    expect(packedTipCommits).toHaveLength(0);
  });

  test("push rejects when given a non-workflow-run repoId", async () => {
    const { store } = createRecordingUnderlyingRepoStore();
    const client = createWorkflowRunPackClient({
      substrate: store,
      hubLink: {
        pushWorkflowRunPack: () => Promise.resolve(),
      },
    });
    await expect(
      client.push({
        agentAddress: "a@example.com",
        repoId: { kind: "agent-state", id: "a@example.com" },
        ref: "refs/heads/deploy",
      }),
    ).rejects.toThrow(/workflow-run/);
  });
});

describe("createWorkflowRunPackPushingRepoStore", () => {
  test("writeTreePreservingPrefix against a workflow-run repo fires the push hook", async () => {
    const { store, preserveCalls } = createRecordingUnderlyingRepoStore();
    const registry = createDeploymentAddressRegistry();
    registry.record("dep-1", "agent-1@example.com");
    const pushed: { agentAddress: string; repoId: RepoId; ref: string }[] = [];
    const facade = createWorkflowRunPackPushingRepoStore({
      underlying: store,
      packClient: {
        async push(opts) {
          pushed.push(opts);
        },
      },
      registry,
    });

    const repoId: RepoId = { kind: "workflow-run", id: "dep-1" };
    const result = await facade.writeTreePreservingPrefix(
      { kind: "supervisor" },
      repoId,
      "refs/heads/main",
      {
        preservePrefix: "runs/r-1/events/",
        merge: async () => ({ "runs/r-1/events/0.json": "{}" }),
        message: "append RunStarted",
      },
    );
    await facade.flushWorkflowRunPushes(repoId, "refs/heads/main");

    expect(result.commitSha).toBe("sha-1");
    expect(preserveCalls).toHaveLength(1);
    expect(pushed).toHaveLength(1);
    expect(pushed[0]?.agentAddress).toBe("agent-1@example.com");
    expect(pushed[0]?.repoId.id).toBe("dep-1");
    expect(pushed[0]?.ref).toBe("refs/heads/main");
  });

  test("writeTreePreservingPrefix returns before the pack push finishes", async () => {
    // The facade is required to return from writeTreePreservingPrefix
    // as soon as the local commit lands; the pack push runs
    // asynchronously. This is the throughput-critical behaviour the
    // fifo-mail load test depends on -- without it, every
    // supervisor write pays a full hub-ack round-trip in series and
    // the dispatch loop's per-mail wall-clock balloons to ~15-25s/mail
    // under sustained pressure.
    const { store } = createRecordingUnderlyingRepoStore();
    const registry = createDeploymentAddressRegistry();
    registry.record("dep-pipeline", "agent-pipeline@example.com");
    let resolvePush: () => void = () => {
      throw new Error("test: gate resolver was not captured before use");
    };
    const gate = new Promise<void>((resolve) => {
      resolvePush = resolve;
    });
    const pushOrder: string[] = [];
    const facade = createWorkflowRunPackPushingRepoStore({
      underlying: store,
      packClient: {
        async push() {
          pushOrder.push("enter");
          await gate;
          pushOrder.push("exit");
        },
      },
      registry,
    });

    const repoId: RepoId = { kind: "workflow-run", id: "dep-pipeline" };
    await facade.writeTreePreservingPrefix(
      { kind: "supervisor" },
      repoId,
      "refs/heads/main",
      {
        preservePrefix: "runs/r-1/events/",
        merge: async () => ({ "runs/r-1/events/0.json": "{}" }),
        message: "first",
      },
    );
    // Yield to the microtask queue so the push's `enter` log lands.
    // With a serialised wrap the write would not resolve until
    // `exit`; pipelining is the property under test, so we assert
    // the write returned while the push is still parked inside
    // packClient.push.
    await new Promise((r) => setTimeout(r, 0));
    expect(pushOrder).toEqual(["enter"]);
    resolvePush();
    await facade.flushWorkflowRunPushes(repoId, "refs/heads/main");
    expect(pushOrder).toEqual(["enter", "exit"]);
  });

  test("a burst of writes against the same (repoId, ref) coalesces into at most 2 pushes", async () => {
    // The coalescing invariant: while a push is in flight, follow-on
    // writes mark the slot as dirty rather than enqueueing a new
    // push. After the in-flight push exits, the loop runs one more
    // push that captures whichever commits arrived during the
    // window. This collapses N hub-ack round-trips into 2 for a
    // burst of N back-to-back writes, which is the load-bearing
    // throughput win for the fifo-mail load test.
    const { store } = createRecordingUnderlyingRepoStore();
    const registry = createDeploymentAddressRegistry();
    registry.record("dep-burst", "agent-burst@example.com");
    let resolveFirst: () => void = () => {
      throw new Error("test: first gate not captured");
    };
    const firstGate = new Promise<void>((resolve) => {
      resolveFirst = resolve;
    });
    let pushCount = 0;
    const facade = createWorkflowRunPackPushingRepoStore({
      underlying: store,
      packClient: {
        async push() {
          const idx = pushCount;
          pushCount += 1;
          if (idx === 0) await firstGate;
        },
      },
      registry,
    });

    const repoId: RepoId = { kind: "workflow-run", id: "dep-burst" };
    // Five back-to-back writes. The first triggers a push; the next
    // four land while the first push is in flight and all flip the
    // slot's `dirty` flag, but only one coalesced follow-up push
    // runs after the first exits.
    for (let i = 0; i < 5; i += 1) {
      await facade.writeTreePreservingPrefix(
        { kind: "supervisor" },
        repoId,
        "refs/heads/main",
        {
          preservePrefix: "runs/r-1/events/",
          merge: async () => ({
            [`runs/r-1/events/${String(i)}.json`]: "{}",
          }),
          message: `write-${String(i)}`,
        },
      );
    }
    expect(pushCount).toBe(1);
    resolveFirst();
    await facade.flushWorkflowRunPushes(repoId, "refs/heads/main");
    // First push covered write 0 (the only commit landed when it
    // started); the four follow-up writes coalesced into ONE
    // additional push regardless of count.
    expect(pushCount).toBe(2);
  });

  test("a failed pipelined push surfaces on the next writeTreePreservingPrefix call", async () => {
    // The facade swallows the failed push at fire time but latches
    // the error on the per-(repoId, ref) chain; the next
    // writeTreePreservingPrefix on the same (repoId, ref) re-throws
    // it. The defensive-coding rule says errors must surface; this
    // is how they surface from a pipelined writer.
    const { store } = createRecordingUnderlyingRepoStore();
    const registry = createDeploymentAddressRegistry();
    registry.record("dep-fail", "agent-fail@example.com");
    let pushCount = 0;
    const facade = createWorkflowRunPackPushingRepoStore({
      underlying: store,
      packClient: {
        async push() {
          pushCount += 1;
          if (pushCount === 1) {
            throw new Error("hub_rejected: non_fast_forward");
          }
        },
      },
      registry,
    });
    const repoId: RepoId = { kind: "workflow-run", id: "dep-fail" };
    await facade.writeTreePreservingPrefix(
      { kind: "supervisor" },
      repoId,
      "refs/heads/main",
      {
        preservePrefix: "runs/r/events/",
        merge: async () => ({ "runs/r/events/0.json": "{}" }),
        message: "first",
      },
    );
    // Wait for the failed push to settle on the chain without
    // consuming the latched error; flush would also surface the
    // error, but the contract being pinned here is that the NEXT
    // writeTreePreservingPrefix surfaces it -- ordinary supervisor
    // code does not call flush between writes.
    await new Promise((resolve) => setTimeout(resolve, 10));
    await expect(
      facade.writeTreePreservingPrefix(
        { kind: "supervisor" },
        repoId,
        "refs/heads/main",
        {
          preservePrefix: "runs/r/events/",
          merge: async () => ({ "runs/r/events/1.json": "{}" }),
          message: "second",
        },
      ),
    ).rejects.toThrow(/non_fast_forward/);
  });

  test("flushWorkflowRunPushes resolves immediately when no pushes are pending", async () => {
    const { store } = createRecordingUnderlyingRepoStore();
    const registry = createDeploymentAddressRegistry();
    const facade = createWorkflowRunPackPushingRepoStore({
      underlying: store,
      packClient: { push: () => Promise.resolve() },
      registry,
    });
    await facade.flushWorkflowRunPushes(
      { kind: "workflow-run", id: "never-touched" },
      "refs/heads/main",
    );
  });

  test("writeTreePreservingPrefix against a non-workflow-run repo bypasses the push hook", async () => {
    const { store } = createRecordingUnderlyingRepoStore();
    const registry = createDeploymentAddressRegistry();
    const pushed: { agentAddress: string; repoId: RepoId; ref: string }[] = [];
    const facade = createWorkflowRunPackPushingRepoStore({
      underlying: store,
      packClient: {
        async push(opts) {
          pushed.push(opts);
        },
      },
      registry,
    });

    await facade.writeTreePreservingPrefix(
      { kind: "hub" },
      { kind: "agent-state", id: "a-1" },
      "refs/heads/deploy",
      {
        preservePrefix: "deploy/",
        merge: async () => ({ "deploy/prompt.md": "hi" }),
        message: "deploy",
      },
    );

    expect(pushed).toEqual([]);
  });

  test("workflow-run write surfaces a structured error when no agent address is registered", async () => {
    const { store } = createRecordingUnderlyingRepoStore();
    const registry = createDeploymentAddressRegistry();
    const facade = createWorkflowRunPackPushingRepoStore({
      underlying: store,
      packClient: {
        push: () => Promise.resolve(),
      },
      registry,
    });

    await expect(
      facade.writeTreePreservingPrefix(
        { kind: "supervisor" },
        { kind: "workflow-run", id: "missing-dep" },
        "refs/heads/main",
        {
          preservePrefix: "runs/r/events/",
          merge: async () => ({}),
          message: "append",
        },
      ),
    ).rejects.toThrow(/no agent address registered/);
  });

  test("markAddressUnroutable holds a push until notifyAddressRoutable resumes it", async () => {
    // The reconnect ordering contract. A WS disconnect blocks the address:
    // a write that lands while blocked schedules no wire push, because a
    // push shipped on the fresh, not-yet-challenged connection is dropped by
    // the hub as "unrouted". The reconnect challenge lifts the block and the
    // held push ships then -- after the hub has re-routed the address.
    const { store } = createRecordingUnderlyingRepoStore();
    const registry = createDeploymentAddressRegistry();
    registry.record("dep-blocked", "agent-blocked@example.com");
    let pushCount = 0;
    const facade = createWorkflowRunPackPushingRepoStore({
      underlying: store,
      packClient: {
        async push() {
          pushCount += 1;
        },
      },
      registry,
    });
    const repoId: RepoId = { kind: "workflow-run", id: "dep-blocked" };

    facade.markAddressUnroutable("agent-blocked@example.com");
    await facade.writeTreePreservingPrefix(
      { kind: "supervisor" },
      repoId,
      "refs/heads/main",
      {
        preservePrefix: "runs/r/events/",
        merge: async () => ({ "runs/r/events/0.json": "{}" }),
        message: "append while blocked",
      },
    );
    // Yield: a wire push would have run by now if the block were not held.
    await new Promise((r) => setTimeout(r, 0));
    expect(pushCount).toBe(0);

    facade.notifyAddressRoutable("agent-blocked@example.com");
    await facade.flushWorkflowRunPushes(repoId, "refs/heads/main");
    expect(pushCount).toBe(1);
  });

  test("notifyAddressRoutable re-drives a push a disconnect cancelled with no fresh write", async () => {
    // The liveness contract a synchronous single-step run depends on. The
    // first push rejects "Connection lost" (the disconnect cancelled the
    // in-flight transfer) and latches its error. There is no later local
    // write to re-arm the coalescing loop, so without the routable-again
    // re-drive the run would strand forever. notifyAddressRoutable re-ships
    // the un-acked commits once the challenge re-routes the address.
    const { store } = createRecordingUnderlyingRepoStore();
    const registry = createDeploymentAddressRegistry();
    registry.record("dep-cancelled", "agent-cancelled@example.com");
    let pushCount = 0;
    const facade = createWorkflowRunPackPushingRepoStore({
      underlying: store,
      packClient: {
        async push() {
          pushCount += 1;
          if (pushCount === 1) {
            throw new Error("transfer cancelled: Connection lost");
          }
        },
      },
      registry,
    });
    const repoId: RepoId = { kind: "workflow-run", id: "dep-cancelled" };

    await facade.writeTreePreservingPrefix(
      { kind: "supervisor" },
      repoId,
      "refs/heads/main",
      {
        preservePrefix: "runs/r/events/",
        merge: async () => ({ "runs/r/events/0.json": "{}" }),
        message: "single batch",
      },
    );
    // Let the first push settle and latch "Connection lost" without a
    // second write to re-arm the loop.
    await new Promise((r) => setTimeout(r, 10));
    expect(pushCount).toBe(1);

    facade.notifyAddressRoutable("agent-cancelled@example.com");
    await facade.flushWorkflowRunPushes(repoId, "refs/heads/main");
    // The re-drive re-shipped the un-acked commits; the second attempt
    // succeeds, so the latched error clears and flush resolves cleanly.
    expect(pushCount).toBe(2);
  });

  test("notifyAddressRoutable is a no-op for a slot with nothing un-shipped", async () => {
    // A clean, already-acked slot (no dirty work, no latched error) has
    // nothing to re-ship. This pins the facade's re-drive gating: a
    // routable-again notification for such a slot does not re-drive, so it
    // does not call push again. Only slots with pending work or a latched
    // failure re-drive.
    const { store } = createRecordingUnderlyingRepoStore();
    const registry = createDeploymentAddressRegistry();
    registry.record("dep-clean", "agent-clean@example.com");
    let pushCount = 0;
    const facade = createWorkflowRunPackPushingRepoStore({
      underlying: store,
      packClient: {
        async push() {
          pushCount += 1;
        },
      },
      registry,
    });
    const repoId: RepoId = { kind: "workflow-run", id: "dep-clean" };

    await facade.writeTreePreservingPrefix(
      { kind: "supervisor" },
      repoId,
      "refs/heads/main",
      {
        preservePrefix: "runs/r/events/",
        merge: async () => ({ "runs/r/events/0.json": "{}" }),
        message: "append",
      },
    );
    await facade.flushWorkflowRunPushes(repoId, "refs/heads/main");
    expect(pushCount).toBe(1);

    facade.notifyAddressRoutable("agent-clean@example.com");
    await facade.flushWorkflowRunPushes(repoId, "refs/heads/main");
    expect(pushCount).toBe(1);
  });
});

describe("createMultistepMailRouter", () => {
  test("tryRoute returns false when no handler is registered", () => {
    const router = createMultistepMailRouter();
    expect(
      router.tryRoute("dep@integration.interchange", new Uint8Array([1])),
    ).toBe(false);
  });

  test("a registered handler receives the inbound message and tryRoute returns true", () => {
    const router = createMultistepMailRouter();
    const received: Uint8Array[] = [];
    router.register("dep@integration.interchange", (msg) => {
      received.push(msg);
    });
    const message = new Uint8Array([1, 2, 3, 4]);
    const claimed = router.tryRoute("dep@integration.interchange", message);
    expect(claimed).toBe(true);
    expect(received).toHaveLength(1);
    expect(received[0]).toEqual(message);
  });

  test("registration is per-address; an unrelated address falls through", () => {
    const router = createMultistepMailRouter();
    const received: Uint8Array[] = [];
    router.register("dep-a@integration.interchange", (msg) => {
      received.push(msg);
    });
    expect(
      router.tryRoute("dep-b@integration.interchange", new Uint8Array([9])),
    ).toBe(false);
    expect(received).toHaveLength(0);
  });

  test("unregister removes the handler", () => {
    const router = createMultistepMailRouter();
    const received: Uint8Array[] = [];
    router.register("dep@integration.interchange", (msg) => {
      received.push(msg);
    });
    router.unregister("dep@integration.interchange");
    expect(
      router.tryRoute("dep@integration.interchange", new Uint8Array([1])),
    ).toBe(false);
    expect(received).toHaveLength(0);
  });

  test("re-registering an address replaces the prior handler", () => {
    const router = createMultistepMailRouter();
    const first: Uint8Array[] = [];
    const second: Uint8Array[] = [];
    router.register("dep@integration.interchange", (msg) => {
      first.push(msg);
    });
    router.register("dep@integration.interchange", (msg) => {
      second.push(msg);
    });
    router.tryRoute("dep@integration.interchange", new Uint8Array([7]));
    expect(first).toHaveLength(0);
    expect(second).toHaveLength(1);
  });
});

describe("createMultistepSourcesRouter", () => {
  const source: InferenceSource = {
    id: "primary",
    provider: "anthropic",
    baseURL: "https://api.anthropic.com",
    apiKey: "sk-x",
    model: "claude-test",
  };
  const frame = {
    type: "sources.update" as const,
    agentAddress: "dep@integration.interchange",
    sources: [source],
    defaultSource: "primary",
  };

  test("tryRoute returns false when no handler is registered", async () => {
    const router = createMultistepSourcesRouter();
    expect(await router.tryRoute(frame)).toBe(false);
  });

  test("a registered handler receives the rotation and tryRoute returns true", async () => {
    const router = createMultistepSourcesRouter();
    const received: { sources: InferenceSource[]; defaultSource: string }[] =
      [];
    router.register("dep@integration.interchange", async (args) => {
      received.push(args);
    });
    expect(await router.tryRoute(frame)).toBe(true);
    expect(received).toHaveLength(1);
    expect(received[0]?.sources).toEqual([source]);
    expect(received[0]?.defaultSource).toBe("primary");
  });

  test("registration is per-address; an unrelated address falls through", async () => {
    const router = createMultistepSourcesRouter();
    router.register("dep-a@integration.interchange", async () => undefined);
    expect(
      await router.tryRoute({
        ...frame,
        agentAddress: "dep-b@integration.interchange",
      }),
    ).toBe(false);
  });

  test("unregister removes the handler", async () => {
    const router = createMultistepSourcesRouter();
    router.register("dep@integration.interchange", async () => undefined);
    router.unregister("dep@integration.interchange");
    expect(await router.tryRoute(frame)).toBe(false);
  });

  test("rejects a rotation with duplicate ids for a registered address without dispatching", async () => {
    const router = createMultistepSourcesRouter();
    let called = false;
    router.register("dep@integration.interchange", async () => {
      called = true;
    });
    // Duplicate ids would crash the child's control-channel receiver on
    // its narrow, so the router rejects before dispatch.
    await expect(
      router.tryRoute({ ...frame, sources: [source, { ...source }] }),
    ).rejects.toThrow(/unique ids/);
    expect(called).toBe(false);
  });

  test("rejects a rotation whose default is not the head source", async () => {
    const router = createMultistepSourcesRouter();
    let called = false;
    router.register("dep@integration.interchange", async () => {
      called = true;
    });
    const second = { ...source, id: "secondary" };
    await expect(
      router.tryRoute({
        ...frame,
        sources: [source, second],
        defaultSource: "secondary",
      }),
    ).rejects.toThrow(/first element is the default/);
    expect(called).toBe(false);
  });

  test("an invalid rotation for an unregistered address is unrouted, not rejected", async () => {
    const router = createMultistepSourcesRouter();
    // Registration is checked before validation: an unregistered address
    // reports `false` and its (here invalid) payload is never inspected.
    expect(
      await router.tryRoute({
        ...frame,
        agentAddress: "unregistered@integration.interchange",
        sources: [source, { ...source }],
      }),
    ).toBe(false);
  });
});

describe("createMultistepDrainRouter", () => {
  test("tryRoute resolves to false when no handler is registered", async () => {
    const router = createMultistepDrainRouter();
    const claimed = await router.tryRoute({
      type: "drain.deliver",
      agentAddress: "dep@integration.interchange",
      deadlineMs: 1_000,
    });
    expect(claimed).toBe(false);
  });

  test("a registered handler receives the deadline and tryRoute resolves to true", async () => {
    const router = createMultistepDrainRouter();
    const received: { deadlineMs: number }[] = [];
    router.register("dep@integration.interchange", async (args) => {
      received.push({ deadlineMs: args.deadlineMs });
    });
    const claimed = await router.tryRoute({
      type: "drain.deliver",
      agentAddress: "dep@integration.interchange",
      deadlineMs: 3_500,
    });
    expect(claimed).toBe(true);
    expect(received).toEqual([{ deadlineMs: 3_500 }]);
  });

  test("registration is per-address; an unrelated address falls through", async () => {
    const router = createMultistepDrainRouter();
    const received: number[] = [];
    router.register("dep-a@integration.interchange", async (args) => {
      received.push(args.deadlineMs);
    });
    const claimed = await router.tryRoute({
      type: "drain.deliver",
      agentAddress: "dep-b@integration.interchange",
      deadlineMs: 9_000,
    });
    expect(claimed).toBe(false);
    expect(received).toHaveLength(0);
  });

  test("unregister removes the handler", async () => {
    const router = createMultistepDrainRouter();
    const received: number[] = [];
    router.register("dep@integration.interchange", async (args) => {
      received.push(args.deadlineMs);
    });
    router.unregister("dep@integration.interchange");
    const claimed = await router.tryRoute({
      type: "drain.deliver",
      agentAddress: "dep@integration.interchange",
      deadlineMs: 1_000,
    });
    expect(claimed).toBe(false);
    expect(received).toHaveLength(0);
  });

  test("re-registering an address replaces the prior handler", async () => {
    const router = createMultistepDrainRouter();
    const first: number[] = [];
    const second: number[] = [];
    router.register("dep@integration.interchange", async (args) => {
      first.push(args.deadlineMs);
    });
    router.register("dep@integration.interchange", async (args) => {
      second.push(args.deadlineMs);
    });
    await router.tryRoute({
      type: "drain.deliver",
      agentAddress: "dep@integration.interchange",
      deadlineMs: 4_200,
    });
    expect(first).toHaveLength(0);
    expect(second).toEqual([4_200]);
  });

  test("handler rejection propagates through tryRoute", async () => {
    const router = createMultistepDrainRouter();
    router.register("dep@integration.interchange", async () => {
      throw new Error("supervisor.drain failed");
    });
    await expect(
      router.tryRoute({
        type: "drain.deliver",
        agentAddress: "dep@integration.interchange",
        deadlineMs: 1_000,
      }),
    ).rejects.toThrow(/supervisor\.drain failed/);
  });

  test("drain.deliver for a never-registered deployment id drops cleanly without throwing", async () => {
    // Pins the defensive contract for an inbound drain.deliver frame
    // that names a deploymentId the sidecar's supervisor never spawned
    // (e.g. an in-flight frame outracing the deploy ack, or a hub-side
    // stale-state retry). The router must not throw; the hub-link's
    // handleDrainDeliver then logs and drops, leaving sibling
    // deployments unaffected.
    const router = createMultistepDrainRouter();
    router.register("dep-known@integration.interchange", async () => {
      throw new Error("known handler must not be invoked");
    });
    const claimed = await router.tryRoute({
      type: "drain.deliver",
      agentAddress: "dep-unknown@integration.interchange",
      deadlineMs: 1_000,
    });
    expect(claimed).toBe(false);
  });
});

describe("createMultistepSignalRouter", () => {
  test("tryRoute resolves to false when no handler is registered", async () => {
    const router = createMultistepSignalRouter();
    const claimed = await router.tryRoute({
      type: "signal.deliver",
      agentAddress: "dep@integration.interchange",
      runId: "run-1",
      signalName: "approve",
      signalId: "sig-1",
      payload: { ok: true },
    });
    expect(claimed).toBe(false);
  });

  test("a registered handler receives the signal and tryRoute resolves to true", async () => {
    const router = createMultistepSignalRouter();
    const received: {
      runId: string;
      signalName: string;
      signalId: string;
      payload: unknown;
    }[] = [];
    router.register("dep@integration.interchange", async (args) => {
      received.push(args);
    });
    const claimed = await router.tryRoute({
      type: "signal.deliver",
      agentAddress: "dep@integration.interchange",
      runId: "run-42",
      signalName: "approve",
      signalId: "sig-42",
      payload: { ok: true },
    });
    expect(claimed).toBe(true);
    expect(received).toEqual([
      {
        runId: "run-42",
        signalName: "approve",
        signalId: "sig-42",
        payload: { ok: true },
      },
    ]);
  });

  test("registration is per-address; an unrelated address falls through", async () => {
    const router = createMultistepSignalRouter();
    const received: string[] = [];
    router.register("dep-a@integration.interchange", async (args) => {
      received.push(args.signalId);
    });
    const claimed = await router.tryRoute({
      type: "signal.deliver",
      agentAddress: "dep-b@integration.interchange",
      runId: "run-1",
      signalName: "approve",
      signalId: "sig-1",
      payload: null,
    });
    expect(claimed).toBe(false);
    expect(received).toHaveLength(0);
  });

  test("unregister removes the handler", async () => {
    const router = createMultistepSignalRouter();
    const received: string[] = [];
    router.register("dep@integration.interchange", async (args) => {
      received.push(args.signalId);
    });
    router.unregister("dep@integration.interchange");
    const claimed = await router.tryRoute({
      type: "signal.deliver",
      agentAddress: "dep@integration.interchange",
      runId: "run-1",
      signalName: "approve",
      signalId: "sig-1",
      payload: null,
    });
    expect(claimed).toBe(false);
    expect(received).toHaveLength(0);
  });

  test("re-registering an address replaces the prior handler", async () => {
    // Pins the contract that drives the "stale-cohort signal" edge
    // case. A signal frame in flight while the deploy router re-binds
    // the deployment address (the only legitimate path that swaps the
    // handler today) must route to the most-recently-registered
    // handler; the prior cohort's handler is unreachable once
    // replaced. The router does not carry a cohortId on the wire, so
    // "live registration wins" is the contract that captures the
    // intent.
    const router = createMultistepSignalRouter();
    const first: string[] = [];
    const second: string[] = [];
    router.register("dep@integration.interchange", async (args) => {
      first.push(args.signalId);
    });
    router.register("dep@integration.interchange", async (args) => {
      second.push(args.signalId);
    });
    await router.tryRoute({
      type: "signal.deliver",
      agentAddress: "dep@integration.interchange",
      runId: "run-1",
      signalName: "approve",
      signalId: "sig-late",
      payload: null,
    });
    expect(first).toHaveLength(0);
    expect(second).toEqual(["sig-late"]);
  });

  test("handler rejection propagates through tryRoute", async () => {
    const router = createMultistepSignalRouter();
    router.register("dep@integration.interchange", async () => {
      throw new Error("supervisor.deliverSignal failed");
    });
    await expect(
      router.tryRoute({
        type: "signal.deliver",
        agentAddress: "dep@integration.interchange",
        runId: "run-1",
        signalName: "approve",
        signalId: "sig-1",
        payload: null,
      }),
    ).rejects.toThrow(/supervisor\.deliverSignal failed/);
  });

  test("signal.deliver for a never-registered deployment id drops cleanly without throwing", async () => {
    // Pins the defensive contract for an inbound signal.deliver frame
    // that names a deploymentId the sidecar's supervisor never spawned
    // (e.g. an in-flight frame outracing the deploy ack, or a hub-side
    // stale-state retry). The router must not throw; the hub-link's
    // handleSignalDeliver then logs and drops, leaving sibling
    // deployments unaffected.
    const router = createMultistepSignalRouter();
    router.register("dep-known@integration.interchange", async () => {
      throw new Error("known handler must not be invoked");
    });
    const claimed = await router.tryRoute({
      type: "signal.deliver",
      agentAddress: "dep-unknown@integration.interchange",
      runId: "run-1",
      signalName: "approve",
      signalId: "sig-1",
      payload: null,
    });
    expect(claimed).toBe(false);
  });
});
