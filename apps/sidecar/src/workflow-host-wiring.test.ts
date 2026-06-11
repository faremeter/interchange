import { describe, test, expect } from "bun:test";

import { generateKeyPair } from "@intx/crypto-node";
import { createInMemoryTransport } from "@intx/mail-memory";
import type { RepoId, RepoStore } from "@intx/hub-sessions";
import type {
  CommitRunEventResult,
  SubprocessSpawner,
  SupervisorRunEvent,
} from "@intx/workflow-host";
import type { InferenceEvent } from "@intx/types/runtime";

import {
  createSidecarDeployRouter,
  createSidecarWorkflowSupervisor,
  driveTrivialRunChain,
  type TrivialRunCell,
} from "./workflow-host-wiring";

function createMinimalStubRepoStore(): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(_repoId: RepoId): string {
      return "/tmp/unused";
    },
    async writeTreePreservingPrefix(_p, _id, _ref, args) {
      // The wiring test exercises signature attribution by driving a
      // requestCancel; the merge callback runs once with an empty
      // pre-image.
      await args.merge(new Map());
      return { commitSha: "stub-sha" };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; the wiring test exercises only getRepoDir + writeTreePreservingPrefix
  return new Proxy(stub as RepoStore, {
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
}

describe("createSidecarWorkflowSupervisor", () => {
  test("constructs the supervisor with the sidecar's bindings and signs CancelRequested via the host's signing key", async () => {
    const transport = createInMemoryTransport();
    const keyPair = await generateKeyPair();
    const repoStore = createMinimalStubRepoStore();

    const spawner: SubprocessSpawner = () => {
      throw new Error("spawner not invoked in this test");
    };

    const wired = createSidecarWorkflowSupervisor({
      transport,
      repoStore,
      signingKeySeed: keyPair.privateKey,
      workflowRunRepoId: { kind: "workflow-run", id: "wire-test" },
      workflowRunRef: "refs/heads/main",
      deploymentId: "wire-test",
      deploymentMailAddress: "wire-test@example.com",
      deriveStepAddress: ({ deploymentId, stepId }) =>
        `${deploymentId}-${stepId}@example.com`,
      substrateEnv: { DATA_DIR: "/tmp/wire" },
      subprocessSpawner: spawner,
      trivialLaunch: () => Promise.resolve(),
    });

    expect(typeof wired.supervisor.spawn).toBe("function");
    expect(wired.getCredentialsSnapshot()).toBeNull();

    const result = await wired.supervisor.requestCancel({
      runId: "r-wire-1",
      origin: "supervisor-operator",
      reason: "wiring test",
      at: "2026-01-01T00:00:00.000Z",
    });
    expect(result.commitSha).toBe("stub-sha");
    expect(result.seq).toBe(0);
  });

  test("routeInbound forwards delivered messages to the supervisor's mail subscription", () => {
    const transport = createInMemoryTransport();
    // generateKeyPair is async; this test only exercises the
    // mail-routing path so we synthesize a 32-byte seed without
    // calling crypto.
    const fakeSeed = new Uint8Array(32);
    const repoStore = createMinimalStubRepoStore();
    const wired = createSidecarWorkflowSupervisor({
      transport,
      repoStore,
      signingKeySeed: fakeSeed,
      workflowRunRepoId: { kind: "workflow-run", id: "inbound" },
      workflowRunRef: "refs/heads/main",
      deploymentId: "inbound",
      deploymentMailAddress: "inbound@example.com",
      deriveStepAddress: ({ deploymentId, stepId }) =>
        `${deploymentId}-${stepId}@example.com`,
      substrateEnv: {},
      subprocessSpawner: () => {
        throw new Error("spawner not invoked in this test");
      },
      trivialLaunch: () => Promise.resolve(),
    });
    // Without a subscriber, routeInbound is a no-op rather than a
    // throw -- the wiring's mail bus map is a per-address Set that
    // returns early when no handler is registered.
    expect(() =>
      wired.routeInbound(new TextEncoder().encode("hello")),
    ).not.toThrow();
  });
});

describe("driveTrivialRunChain projects reactor events onto the workflow-run chain", () => {
  function makeRecorder(): {
    calls: SupervisorRunEvent[];
    record: (e: SupervisorRunEvent) => Promise<CommitRunEventResult>;
  } {
    const calls: SupervisorRunEvent[] = [];
    return {
      calls,
      record: async (e) => {
        calls.push(e);
        return {
          commitSha: "stub",
          seq: calls.length - 1,
          signature: { sig: new Uint8Array(64), principalKind: "supervisor" },
        };
      },
    };
  }

  test("completed run drives RunStarted, StepStarted, StepCompleted, RunCompleted", async () => {
    const cell: TrivialRunCell = { runId: null, stepStarted: false };
    const { calls, record } = makeRecorder();

    const started: InferenceEvent = {
      type: "message.run.started",
      seq: 0,
      data: { messageId: "m-1", messageRunId: "r-1", receivedAt: 1 },
    };
    const inferStart: InferenceEvent = {
      type: "inference.start",
      seq: 1,
      data: { model: "test" },
    };
    const ended: InferenceEvent = {
      type: "message.run.ended",
      seq: 2,
      data: { messageRunId: "r-1", messageId: "m-1", status: "completed" },
    };

    await driveTrivialRunChain(started, record, cell);
    await driveTrivialRunChain(inferStart, record, cell);
    // A second inference.start within the same bracket does NOT mint a
    // duplicate StepStarted.
    await driveTrivialRunChain(inferStart, record, cell);
    await driveTrivialRunChain(ended, record, cell);

    const kinds = calls.map((c) => c.kind);
    expect(kinds).toEqual([
      "RunStarted",
      "StepStarted",
      "StepCompleted",
      "RunCompleted",
    ]);
    for (const call of calls) {
      expect(call.runId).toBe("r-1");
    }
    const runStarted = calls[0];
    if (runStarted?.kind !== "RunStarted") {
      throw new Error("unreachable");
    }
    expect(runStarted.consumedMessageId).toBe("m-1");
    expect(cell.runId).toBeNull();
    expect(cell.stepStarted).toBe(false);
  });

  test("failed run emits StepCompleted but not RunCompleted", async () => {
    const cell: TrivialRunCell = { runId: null, stepStarted: false };
    const { calls, record } = makeRecorder();

    await driveTrivialRunChain(
      {
        type: "message.run.started",
        seq: 0,
        data: { messageId: "m-2", messageRunId: "r-2", receivedAt: 1 },
      },
      record,
      cell,
    );
    await driveTrivialRunChain(
      { type: "inference.start", seq: 1, data: { model: "test" } },
      record,
      cell,
    );
    await driveTrivialRunChain(
      {
        type: "message.run.ended",
        seq: 2,
        data: {
          messageRunId: "r-2",
          messageId: "m-2",
          status: "failed",
          error: { message: "boom" },
        },
      },
      record,
      cell,
    );

    expect(calls.map((c) => c.kind)).toEqual([
      "RunStarted",
      "StepStarted",
      "StepCompleted",
    ]);
  });

  test("inference.start without a live bracket is ignored", async () => {
    const cell: TrivialRunCell = { runId: null, stepStarted: false };
    const { calls, record } = makeRecorder();

    await driveTrivialRunChain(
      { type: "inference.start", seq: 0, data: { model: "test" } },
      record,
      cell,
    );

    expect(calls).toEqual([]);
    expect(cell.runId).toBeNull();
  });
});

describe("createSidecarDeployRouter wires the InferenceEvent subscription to recordRunEvent", () => {
  test("the trivial-launch closure brackets a real mail trigger via onAgentEvent", async () => {
    const transport = createInMemoryTransport();
    const keyPair = await generateKeyPair();

    // Capture every `merge` payload the supervisor commits through the
    // substrate. Each successful `recordRunEvent` call drives one
    // writeTreePreservingPrefix invocation that runs `merge` against
    // an empty existing-tree map; we read the files the merge wrote
    // back out and snapshot their `type` discriminator.
    const writtenEvents: string[] = [];
    const repoStore: RepoStore = ((): RepoStore => {
      const stub: Partial<RepoStore> = {
        getRepoDir(_repoId: RepoId): string {
          return "/tmp/unused";
        },
        async writeTreePreservingPrefix(_p, _id, _ref, args) {
          const files = await args.merge(new Map());
          for (const value of Object.values(files)) {
            const text =
              value instanceof Uint8Array
                ? new TextDecoder().decode(value)
                : value;
            const parsed: unknown = JSON.parse(text);
            if (
              typeof parsed === "object" &&
              parsed !== null &&
              "type" in parsed &&
              typeof parsed.type === "string"
            ) {
              writtenEvents.push(parsed.type);
            }
          }
          return { commitSha: `c-${String(writtenEvents.length)}` };
        },
      };
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- in-test stub; the unused RepoStore methods are guarded by the Proxy below
      return new Proxy(stub as RepoStore, {
        get(target, prop, receiver) {
          const value = Reflect.get(target, prop, receiver);
          if (value !== undefined) return value;
          return () => {
            throw new Error(`stub RepoStore: ${String(prop)} not implemented`);
          };
        },
      });
    })();

    // Capture the per-agent InferenceEvent listener the trivial-launch
    // closure registers so the test can fire events at it directly.
    type CapturedListener = {
      address: string;
      listener: (e: InferenceEvent) => void;
    };
    const captured: CapturedListener[] = [];
    const onAgentEvent = (
      address: string,
      listener: (e: InferenceEvent) => void,
    ): (() => void) => {
      captured.push({ address, listener });
      return () => {
        const idx = captured.findIndex((c) => c.listener === listener);
        if (idx >= 0) captured.splice(idx, 1);
      };
    };

    const router = createSidecarDeployRouter({
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the deploy-router test exercises only provisionAgent + persistHubPublicKey
      sessions: {
        provisionAgent: async (_config: unknown) => ({
          publicKey: "pk-trivial",
          keyPair: {
            publicKey: new Uint8Array(32),
            privateKey: new Uint8Array(32),
          },
        }),
        persistHubPublicKey: async (_a: string, _h: string) => {
          /* no-op */
        },
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["sessions"],
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the deploy-router test exercises only recordHubKey
      keyStore: {
        recordHubKey: (_a: string, _h: string) => {
          /* no-op */
        },
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["keyStore"],
      onAgentEvent,
      transport,
      repoStore,
      signingKeySeed: keyPair.privateKey,
    });

    const result = await router.deploy({
      type: "agent.deploy",
      agentAddress: "trivial@example.com",
      agentId: "trivial-agent",
      hubPublicKey: "hub-pk",
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the trivial-launch closure passes config to the SessionManager mock above without inspecting it
      config: {} as unknown as Parameters<
        ReturnType<typeof createSidecarDeployRouter>["deploy"]
      >[0]["config"],
    });
    expect(result.publicKey).toBe("pk-trivial");

    if (captured.length !== 1) {
      throw new Error(
        `expected exactly one onAgentEvent registration, got ${String(captured.length)}`,
      );
    }
    const entry = captured[0];
    if (entry === undefined) throw new Error("unreachable");
    // The slug derivation strips the `@example.com` suffix; the
    // listener is registered against the original frame address.
    expect(entry.address).toBe("trivial@example.com");

    entry.listener({
      type: "message.run.started",
      seq: 0,
      data: { messageId: "m-1", messageRunId: "r-1", receivedAt: 1 },
    });
    entry.listener({
      type: "inference.start",
      seq: 1,
      data: { model: "test" },
    });
    entry.listener({
      type: "message.run.ended",
      seq: 2,
      data: { messageRunId: "r-1", messageId: "m-1", status: "completed" },
    });

    // recordRunEvent fires are sequenced through Promises; await a
    // microtask drain before snapshot.
    await new Promise<void>((r) => setTimeout(r, 0));

    expect(writtenEvents).toEqual([
      "RunStarted",
      "StepStarted",
      "StepCompleted",
      "RunCompleted",
    ]);
  });
});
