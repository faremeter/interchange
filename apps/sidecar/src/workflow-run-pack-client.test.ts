import { describe, test, expect } from "bun:test";

import type { RepoId, RepoStore } from "@intx/hub-sessions";

import {
  createDeploymentAddressRegistry,
  createMultistepMailRouter,
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
  const stub: Partial<RepoStore> = {
    getRepoDir(_repoId: RepoId): string {
      return "/tmp/unused";
    },
    async writeTreePreservingPrefix(principal, repoId, ref, args) {
      preserveCalls.push({ principal, repoId, ref });
      await args.merge(new Map());
      return { commitSha: `sha-${String(preserveCalls.length)}` };
    },
    async createPack(principal, repoId, ref) {
      packs.push({ principal, repoId, ref });
      return {
        pack: new Uint8Array([0xab, 0xcd]),
        commitSha: "stub-pack-sha",
        ref,
      };
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
  return { store, preserveCalls, packs };
}

describe("createWorkflowRunPackClient", () => {
  test("push builds a pack under the supervisor principal and forwards it to the hub link", async () => {
    const { store, packs } = createRecordingUnderlyingRepoStore();
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

    const result = await facade.writeTreePreservingPrefix(
      { kind: "supervisor" },
      { kind: "workflow-run", id: "dep-1" },
      "refs/heads/main",
      {
        preservePrefix: "runs/r-1/events/",
        merge: async () => ({ "runs/r-1/events/0.json": "{}" }),
        message: "append RunStarted",
      },
    );

    expect(result.commitSha).toBe("sha-1");
    expect(preserveCalls).toHaveLength(1);
    expect(pushed).toHaveLength(1);
    expect(pushed[0]?.agentAddress).toBe("agent-1@example.com");
    expect(pushed[0]?.repoId.id).toBe("dep-1");
    expect(pushed[0]?.ref).toBe("refs/heads/main");
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
