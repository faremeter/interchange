import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto-node";
import { createInMemoryTransport } from "@intx/mail-memory";
import type { RepoId, RepoStore } from "@intx/hub-sessions";
import {
  createControlChannelSender,
  type CommitRunEventResult,
  type EventPayload,
  type FrameReader,
  type NdjsonReader,
  type NdjsonWriter,
  type SubprocessHandle,
  type SubprocessSpawner,
  type SupervisorRunEvent,
} from "@intx/workflow-host";
import type { InferenceEvent } from "@intx/types/runtime";
import type { AgentDeployFrame } from "@intx/types/sidecar";

import {
  computeWireDefinitionHash,
  createSidecarDeployRouter,
  createSidecarWorkflowSupervisor,
  driveTrivialRunChain,
  STEP_INFERENCE_SOURCES_ENV_KEY,
  validateWorkflowProjection,
  type TrivialRunCell,
} from "./workflow-host-wiring";
import {
  createMultistepMailRouter,
  type MultistepMailRouter,
} from "./workflow-run-pack-client";

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
      registerDeployment: () => {
        /* the in-test repoStore is a stub; the pack-push facade is exercised separately */
      },
      unregisterDeployment: () => {
        /* no-op for parity with registerDeployment in this stub */
      },
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

// --------------------------------------------------------------------
// Multi-step branch tests
// --------------------------------------------------------------------

function createMemoryNdjsonStream() {
  const buffer: string[] = [];
  let waiter: (() => void) | null = null;
  let done = false;
  function wake() {
    const w = waiter;
    waiter = null;
    if (w) w();
  }
  const reader: NdjsonReader = {
    read(): AsyncIterableIterator<string> {
      return (async function* () {
        while (true) {
          if (buffer.length > 0) {
            const next = buffer.shift();
            if (next === undefined) {
              throw new Error("buffer shift returned undefined");
            }
            yield next;
            continue;
          }
          if (done) return;
          await new Promise<void>((resolve) => {
            waiter = resolve;
          });
        }
      })();
    },
  };
  const writer: NdjsonWriter = {
    write(line: string) {
      buffer.push(line.replace(/\n$/, ""));
      wake();
      return Promise.resolve();
    },
  };
  return {
    writer,
    reader,
    inject(line: string) {
      buffer.push(line.replace(/\n$/, ""));
      wake();
    },
    flushed(): readonly string[] {
      return buffer.slice();
    },
    close() {
      done = true;
      wake();
    },
  };
}

function createMemoryFrameStream() {
  const buffer: Uint8Array[] = [];
  let waiter: (() => void) | null = null;
  let done = false;
  function wake() {
    const w = waiter;
    waiter = null;
    if (w) w();
  }
  const reader: FrameReader = {
    read(): AsyncIterableIterator<Uint8Array> {
      return (async function* () {
        while (true) {
          if (buffer.length > 0) {
            const next = buffer.shift();
            if (next === undefined) {
              throw new Error("frame buffer shift returned undefined");
            }
            yield next;
            continue;
          }
          if (done) return;
          await new Promise<void>((resolve) => {
            waiter = resolve;
          });
        }
      })();
    },
  };
  return {
    reader,
    inject(bytes: Uint8Array) {
      buffer.push(bytes);
      wake();
    },
    close() {
      done = true;
      wake();
    },
  };
}

function createTempBaseDir(prefix: string): Promise<string> {
  return fs.mkdtemp(path.join(os.tmpdir(), prefix));
}

/**
 * Build a stub RepoStore whose `getRepoDir` resolves under the supplied
 * tempBase. The multi-step branch's `assembleCredentialsSnapshot`
 * reads `state/grants.json` from disk -- missing files are treated as
 * empty grants, so a freshly-created tempBase produces an empty
 * credentials snapshot which is what the wiring test wants.
 */
function createSpawnTestRepoStore(tempBase: string): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(tempBase, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix(_p, _id, _ref, args) {
      await args.merge(new Map());
      return { commitSha: "stub-sha" };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; only getRepoDir + writeTreePreservingPrefix exercised
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

type WorkflowProjection = NonNullable<AgentDeployFrame["workflow"]>;
type InferenceSourceFixture = WorkflowProjection["sources"][string];

type MultistepDeployArgs = {
  sources: Record<string, InferenceSourceFixture>;
  definition: {
    id: string;
    triggers: unknown[];
    stepOrder: string[];
    steps: Record<string, unknown>;
  };
};

function makeInferenceSource(id: string): InferenceSourceFixture {
  return {
    id,
    provider: "anthropic",
    baseURL: "https://api.anthropic.com",
    apiKey: `sk-${id}`,
    model: "claude-3-5",
  };
}

function makeMultistepFrame(args: MultistepDeployArgs): AgentDeployFrame {
  return {
    type: "agent.deploy",
    agentAddress: "multi@example.com",
    agentId: "multi-agent",
    hubPublicKey: "hub-pk",
    // The wire-side HarnessConfig has many required fields. The router
    // never inspects `config` on the multi-step branch (only the
    // trivial branch hands it to provisionAgent), so an opaque
    // placeholder satisfies the surface contract for these tests.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the multi-step branch does not read config
    config: {} as AgentDeployFrame["config"],
    workflow: {
      definition: args.definition,
      sources: args.sources,
    },
  };
}

function defaultMultistepSources(): Record<string, InferenceSourceFixture> {
  return {
    "step-1": makeInferenceSource("step-1"),
    "step-2": makeInferenceSource("step-2"),
  };
}

describe("validateWorkflowProjection", () => {
  test("rejects an empty stepOrder", () => {
    expect(() =>
      validateWorkflowProjection({
        definition: { id: "w-1", stepOrder: [], steps: {} },
        sources: {},
      }),
    ).toThrow(/stepOrder must be a non-empty array/);
  });

  test("rejects a stepId that violates STEP_ID_PATTERN", () => {
    expect(() =>
      validateWorkflowProjection({
        definition: {
          id: "w-1",
          stepOrder: ["bad.step"],
          steps: { "bad.step": {} },
        },
        sources: { "bad.step": {} },
      }),
    ).toThrow(/must match \^/);
  });

  test("rejects a missing sources entry for a stepOrder id", () => {
    expect(() =>
      validateWorkflowProjection({
        definition: {
          id: "w-1",
          stepOrder: ["step-1"],
          steps: { "step-1": {} },
        },
        sources: {},
      }),
    ).toThrow(/sources is missing entry/);
  });

  test("accepts a well-formed projection", () => {
    expect(() =>
      validateWorkflowProjection({
        definition: {
          id: "w-1",
          stepOrder: ["step-1", "step-2"],
          steps: { "step-1": {}, "step-2": {} },
        },
        sources: { "step-1": {}, "step-2": {} },
      }),
    ).not.toThrow();
  });
});

describe("computeWireDefinitionHash", () => {
  test("is stable across key-ordering differences", () => {
    const a = { id: "w-1", stepOrder: ["s1"], steps: { s1: { kind: "step" } } };
    const b = { steps: { s1: { kind: "step" } }, stepOrder: ["s1"], id: "w-1" };
    expect(computeWireDefinitionHash(a)).toBe(computeWireDefinitionHash(b));
  });

  test("differs across different definitions", () => {
    const a = { id: "w-1", stepOrder: ["s1"], steps: { s1: {} } };
    const b = { id: "w-2", stepOrder: ["s1"], steps: { s1: {} } };
    expect(computeWireDefinitionHash(a)).not.toBe(computeWireDefinitionHash(b));
  });
});

describe("createSidecarDeployRouter trivial-frame regression", () => {
  test("a frame without `workflow` still drives the trivial provisioning path", async () => {
    const transport = createInMemoryTransport();
    const keyPair = await generateKeyPair();
    const repoStore = createMinimalStubRepoStore();

    let provisionAgentCalled = false;
    let spawnerInvoked = false;

    const router = createSidecarDeployRouter({
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; trivial branch exercises only provisionAgent + persistHubPublicKey
      sessions: {
        provisionAgent: async (_config: unknown) => {
          provisionAgentCalled = true;
          return {
            publicKey: "pk-trivial-regression",
            keyPair: {
              publicKey: new Uint8Array(32),
              privateKey: new Uint8Array(32),
            },
          };
        },
        persistHubPublicKey: async (_a: string, _h: string) => {
          /* no-op */
        },
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["sessions"],
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub
      keyStore: {
        recordHubKey: (_a: string, _h: string) => {
          /* no-op */
        },
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["keyStore"],
      onAgentEvent: () => () => {
        /* unused in this test */
      },
      transport,
      repoStore,
      signingKeySeed: keyPair.privateKey,
      registerDeployment: () => {
        /* no-op */
      },
      unregisterDeployment: () => {
        /* no-op */
      },
      multistepSubprocessSpawner: () => {
        spawnerInvoked = true;
        throw new Error("the trivial branch must not invoke the spawner");
      },
    });

    const result = await router.deploy({
      type: "agent.deploy",
      agentAddress: "trivial-regression@example.com",
      agentId: "trivial-agent",
      hubPublicKey: "hub-pk",
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the trivial-launch closure passes config to the SessionManager mock above without inspecting it
      config: {} as unknown as Parameters<
        ReturnType<typeof createSidecarDeployRouter>["deploy"]
      >[0]["config"],
    });

    expect(provisionAgentCalled).toBe(true);
    expect(spawnerInvoked).toBe(false);
    expect(result.publicKey).toBe("pk-trivial-regression");
  });

  test("two distinct agent addresses whose deriveTrivialDeploymentId slugs collide are rejected at the second deploy", async () => {
    // `deriveTrivialDeploymentId` substitutes every disallowed
    // character with `-`, so two agent addresses that differ only in
    // disallowed characters collapse to the same slug. The slug IS
    // the workflow-run repoId, so a silent collision would let the
    // second deploy overwrite the first deploy's repo state. The
    // slug-claims map rejects the second deploy at the router edge.
    const transport = createInMemoryTransport();
    const keyPair = await generateKeyPair();
    const repoStore = createMinimalStubRepoStore();
    const router = createSidecarDeployRouter({
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; trivial branch exercises only provisionAgent + persistHubPublicKey
      sessions: {
        provisionAgent: async (_config: unknown) => ({
          publicKey: "pk-slug-collision",
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
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub
      keyStore: {
        recordHubKey: (_a: string, _h: string) => {
          /* no-op */
        },
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["keyStore"],
      onAgentEvent: () => () => {
        /* unused in this test */
      },
      transport,
      repoStore,
      signingKeySeed: keyPair.privateKey,
      registerDeployment: () => {
        /* no-op */
      },
      unregisterDeployment: () => {
        /* no-op */
      },
      multistepSubprocessSpawner: () => {
        throw new Error("the trivial branch must not invoke the spawner");
      },
    });

    // `agent@a.b.com` and `agent!a!b!com` both project to
    // `agent-a-b-com` under the slug derivation.
    const first = await router.deploy({
      type: "agent.deploy",
      agentAddress: "agent@a.b.com",
      agentId: "agent-id-1",
      hubPublicKey: "hub-pk",
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the trivial-launch closure forwards config opaquely
      config: {} as unknown as Parameters<
        ReturnType<typeof createSidecarDeployRouter>["deploy"]
      >[0]["config"],
    });
    expect(first.publicKey).toBe("pk-slug-collision");

    let caught: unknown;
    try {
      await router.deploy({
        type: "agent.deploy",
        agentAddress: "agent!a!b!com",
        agentId: "agent-id-2",
        hubPublicKey: "hub-pk",
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- forwarded opaquely
        config: {} as unknown as Parameters<
          ReturnType<typeof createSidecarDeployRouter>["deploy"]
        >[0]["config"],
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    expect(caught instanceof Error && caught.message).toMatch(
      /deriveTrivialDeploymentId collision/,
    );
    expect(caught instanceof Error && caught.message).toMatch(/agent-a-b-com/);
  });

  test("re-deploying the same address is a no-op claim and succeeds", async () => {
    // The slug-claims map records the FIRST claimer's agent
    // address; a second claim from the SAME address must not
    // throw. Otherwise idempotent re-deploys would be rejected as
    // self-collisions.
    const transport = createInMemoryTransport();
    const keyPair = await generateKeyPair();
    const repoStore = createMinimalStubRepoStore();
    let provisionCount = 0;
    const router = createSidecarDeployRouter({
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub
      sessions: {
        provisionAgent: async (_config: unknown) => {
          provisionCount += 1;
          return {
            publicKey: `pk-redeploy-${String(provisionCount)}`,
            keyPair: {
              publicKey: new Uint8Array(32),
              privateKey: new Uint8Array(32),
            },
          };
        },
        persistHubPublicKey: async (_a: string, _h: string) => undefined,
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["sessions"],
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub
      keyStore: {
        recordHubKey: (_a: string, _h: string) => undefined,
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["keyStore"],
      onAgentEvent: () => () => undefined,
      transport,
      repoStore,
      signingKeySeed: keyPair.privateKey,
      registerDeployment: () => undefined,
      unregisterDeployment: () => undefined,
      multistepSubprocessSpawner: () => {
        throw new Error("trivial branch must not invoke the spawner");
      },
    });

    const first = await router.deploy({
      type: "agent.deploy",
      agentAddress: "redeploy@example.com",
      agentId: "redeploy-1",
      hubPublicKey: "hub-pk",
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- forwarded opaquely
      config: {} as unknown as Parameters<
        ReturnType<typeof createSidecarDeployRouter>["deploy"]
      >[0]["config"],
    });
    const second = await router.deploy({
      type: "agent.deploy",
      agentAddress: "redeploy@example.com",
      agentId: "redeploy-2",
      hubPublicKey: "hub-pk",
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- forwarded opaquely
      config: {} as unknown as Parameters<
        ReturnType<typeof createSidecarDeployRouter>["deploy"]
      >[0]["config"],
    });
    expect(first.publicKey).toBe("pk-redeploy-1");
    expect(second.publicKey).toBe("pk-redeploy-2");
  });

  test("a failed deploy releases the slug so a subsequent deploy on the same address succeeds", async () => {
    // Without the release-on-failure guard, the first deploy's
    // `claimSlug` would leak after `provisionAgent` throws, and
    // every subsequent retry on the same address would be rejected
    // as a phantom collision.
    const transport = createInMemoryTransport();
    const keyPair = await generateKeyPair();
    const repoStore = createMinimalStubRepoStore();
    let provisionCount = 0;
    const router = createSidecarDeployRouter({
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub
      sessions: {
        provisionAgent: async (_config: unknown) => {
          provisionCount += 1;
          if (provisionCount === 1) {
            throw new Error("provision failed (synthetic)");
          }
          return {
            publicKey: "pk-after-retry",
            keyPair: {
              publicKey: new Uint8Array(32),
              privateKey: new Uint8Array(32),
            },
          };
        },
        persistHubPublicKey: async (_a: string, _h: string) => undefined,
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["sessions"],
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub
      keyStore: {
        recordHubKey: (_a: string, _h: string) => undefined,
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["keyStore"],
      onAgentEvent: () => () => undefined,
      transport,
      repoStore,
      signingKeySeed: keyPair.privateKey,
      registerDeployment: () => undefined,
      unregisterDeployment: () => undefined,
      multistepSubprocessSpawner: () => {
        throw new Error("trivial branch must not invoke the spawner");
      },
    });

    let firstCaught: unknown;
    try {
      await router.deploy({
        type: "agent.deploy",
        agentAddress: "retry@example.com",
        agentId: "retry-1",
        hubPublicKey: "hub-pk",
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- forwarded opaquely
        config: {} as unknown as Parameters<
          ReturnType<typeof createSidecarDeployRouter>["deploy"]
        >[0]["config"],
      });
    } catch (err) {
      firstCaught = err;
    }
    expect(firstCaught).toBeInstanceOf(Error);
    expect(firstCaught instanceof Error && firstCaught.message).toMatch(
      /provision failed \(synthetic\)/,
    );

    // Retry on the SAME address must succeed -- if the slug were
    // leaked, this would throw `deriveTrivialDeploymentId collision`.
    const retry = await router.deploy({
      type: "agent.deploy",
      agentAddress: "retry@example.com",
      agentId: "retry-2",
      hubPublicKey: "hub-pk",
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- forwarded opaquely
      config: {} as unknown as Parameters<
        ReturnType<typeof createSidecarDeployRouter>["deploy"]
      >[0]["config"],
    });
    expect(retry.publicKey).toBe("pk-after-retry");
  });

  test("undeploy releases the slug so a different-address deploy on the same slug succeeds", async () => {
    // After deploy -> undeploy on `release@a.b.com` (slug
    // `release-a-b-com`), a fresh deploy on `release!a!b!com`
    // (same slug) must be accepted. Without `releaseSlug` running
    // on undeploy, the slug would stay claimed and the second
    // deploy would surface a phantom collision.
    const transport = createInMemoryTransport();
    const keyPair = await generateKeyPair();
    const repoStore = createMinimalStubRepoStore();
    let provisionCount = 0;
    const router = createSidecarDeployRouter({
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub
      sessions: {
        provisionAgent: async (_config: unknown) => {
          provisionCount += 1;
          return {
            publicKey: `pk-release-${String(provisionCount)}`,
            keyPair: {
              publicKey: new Uint8Array(32),
              privateKey: new Uint8Array(32),
            },
          };
        },
        persistHubPublicKey: async (_a: string, _h: string) => undefined,
        destroySession: async (_a: string, _r: string) => undefined,
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["sessions"],
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub
      keyStore: {
        recordHubKey: (_a: string, _h: string) => undefined,
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["keyStore"],
      onAgentEvent: () => () => undefined,
      transport,
      repoStore,
      signingKeySeed: keyPair.privateKey,
      registerDeployment: () => undefined,
      unregisterDeployment: () => undefined,
      multistepSubprocessSpawner: () => {
        throw new Error("trivial branch must not invoke the spawner");
      },
    });

    await router.deploy({
      type: "agent.deploy",
      agentAddress: "release@a.b.com",
      agentId: "release-1",
      hubPublicKey: "hub-pk",
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- forwarded opaquely
      config: {} as unknown as Parameters<
        ReturnType<typeof createSidecarDeployRouter>["deploy"]
      >[0]["config"],
    });
    if (router.undeploy === undefined) {
      throw new Error("router.undeploy is required for this test");
    }
    await router.undeploy({
      type: "agent.undeploy",
      agentAddress: "release@a.b.com",
      reason: "test",
    });
    const reclaimed = await router.deploy({
      type: "agent.deploy",
      agentAddress: "release!a!b!com",
      agentId: "release-2",
      hubPublicKey: "hub-pk",
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- forwarded opaquely
      config: {} as unknown as Parameters<
        ReturnType<typeof createSidecarDeployRouter>["deploy"]
      >[0]["config"],
    });
    expect(reclaimed.publicKey).toBe("pk-release-2");
  });
});

describe("createSidecarDeployRouter multi-step branch", () => {
  async function buildMultistepFixture(opts: {
    spawner: SubprocessSpawner;
    publishWorkflowInferenceEvent?: (
      address: string,
      event: EventPayload,
    ) => void;
    multistepBinaryPath?: string;
    multistepSubstrateEnv?: Record<string, string>;
    multistepMailRouter?: MultistepMailRouter;
  }) {
    const transport = createInMemoryTransport();
    const keyPair = await generateKeyPair();
    const tempBase = await createTempBaseDir("sidecar-multistep-");
    const repoStore = createSpawnTestRepoStore(tempBase);
    // The deploy router's multi-step branch materializes
    // `workflow.json` under `${SIDECAR_DATA_DIR}/assets/workflow/<id>/`
    // before invoking the spawner. The test fixture defaults the data
    // dir to a per-test mkdtemp so the wiring tests do not have to
    // touch a real /tmp path; callers can override
    // `SIDECAR_DATA_DIR` (and any other key) by passing
    // `multistepSubstrateEnv`.
    const defaultSubstrateEnv: Record<string, string> = {
      SIDECAR_DATA_DIR: await createTempBaseDir("sidecar-multistep-data-"),
    };
    const mergedSubstrateEnv: Record<string, string> = {
      ...defaultSubstrateEnv,
      ...(opts.multistepSubstrateEnv ?? {}),
    };
    const router = createSidecarDeployRouter({
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the multi-step branch never invokes provisionAgent; the stub throws if it does
      sessions: {
        provisionAgent: async () => {
          throw new Error("multi-step branch must not invoke provisionAgent");
        },
        persistHubPublicKey: async () => {
          throw new Error(
            "multi-step branch must not invoke persistHubPublicKey",
          );
        },
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["sessions"],
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub
      keyStore: {
        recordHubKey: () => {
          throw new Error("multi-step branch must not invoke recordHubKey");
        },
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["keyStore"],
      onAgentEvent: () => () => {
        /* unused in multi-step branch */
      },
      transport,
      repoStore,
      signingKeySeed: keyPair.privateKey,
      registerDeployment: () => {
        /* no-op */
      },
      unregisterDeployment: () => {
        /* no-op */
      },
      multistepSubprocessSpawner: opts.spawner,
      ...(opts.multistepBinaryPath !== undefined
        ? { multistepBinaryPath: opts.multistepBinaryPath }
        : {}),
      multistepSubstrateEnv: mergedSubstrateEnv,
      ...(opts.publishWorkflowInferenceEvent !== undefined
        ? {
            publishWorkflowInferenceEvent: opts.publishWorkflowInferenceEvent,
          }
        : {}),
      ...(opts.multistepMailRouter !== undefined
        ? { multistepMailRouter: opts.multistepMailRouter }
        : {}),
    });
    return { router, tempBase, keyPair, substrateEnv: mergedSubstrateEnv };
  }

  test("validates the projection, constructs SpawnOpts from the frame, drives spawn, and surfaces the supervisor's principal pubkey", async () => {
    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    let observedBinary: string | undefined;
    let observedEnv: Record<string, string> | undefined;
    const spawner: SubprocessSpawner = ({ binaryPath, env }) => {
      observedBinary = binaryPath;
      observedEnv = env;
      const handle: SubprocessHandle = {
        pid: 7321,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
      return handle;
    };

    const multiDataDir = await createTempBaseDir("sidecar-multi-data-");
    const { router, keyPair } = await buildMultistepFixture({
      spawner,
      multistepBinaryPath: "/fake/bin/multistep-workflow-child",
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: multiDataDir,
      },
    });

    // Hijack the supervisor's ipc keypair factory by routing through
    // the test-construction surface: the router constructs the
    // supervisor via createSidecarWorkflowSupervisor which does not
    // expose ipcKeyPairFactory. The supervisor's default keypair is
    // generated with generateKeyPair; the test signs the `ready` frame
    // with whatever channelId the spawn-time env carries plus the
    // child's keypair, and the supervisor accepts a bootstrap
    // signature from any childPublicKey carried in the `ready` payload.

    const sources = defaultMultistepSources();
    const definition = {
      id: "wf-router-test",
      triggers: [{ type: "manual" }],
      stepOrder: ["step-1", "step-2"],
      steps: { "step-1": { kind: "step" }, "step-2": { kind: "step" } },
    };
    const frame = makeMultistepFrame({ definition, sources });

    const deployPromise = router.deploy(frame);

    // Wait until the spawner has been invoked.
    while (observedEnv === undefined) {
      await new Promise((r) => setTimeout(r, 1));
    }

    const env = observedEnv;
    expect(observedBinary).toBe("/fake/bin/multistep-workflow-child");
    expect(env).toMatchObject({
      SIDECAR_DATA_DIR: multiDataDir,
      DEPLOYMENT_ID: "multi-example-com",
      MAILBOX_ADDRESS: "multi@example.com",
    });
    expect(env.DEFINITION_HASH).toBe(computeWireDefinitionHash(definition));
    expect(env[STEP_INFERENCE_SOURCES_ENV_KEY]).toBe(JSON.stringify(sources));
    expect(env.IPC_CHANNEL_ID).toMatch(/^[0-9a-f]{32}$/);

    // Drive the `ready` handshake.
    const channelId = env.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeyPair.privateKey,
      channelId,
      writer: {
        write(line: string) {
          childToSupervisor.inject(line);
          return Promise.resolve();
        },
      },
    });
    await childSender.send({
      type: "ready",
      data: {
        childPid: 7321,
        childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString("hex"),
      },
    });

    const result = await deployPromise;
    // The publicKey is the sidecar's principal public key (hex). The
    // router derives it from the signing seed; the resulting hex is a
    // 64-character lowercase string.
    expect(result.publicKey).toMatch(/^[0-9a-f]{64}$/);

    // Sanity check: re-deriving the public key from the keypair lines
    // up with the router's returned value.
    expect(result.publicKey).toBe(
      Buffer.from(keyPair.publicKey).toString("hex"),
    );

    // Teardown: kill the child so the spawn-time pumps unwind.
    // Use unused supervisorToChild to silence the linter.
    void supervisorToChild;
    void supervisorIpcKeyPair;
  });

  test("registers a multistepMailRouter handler against the deployment address once spawn succeeds", async () => {
    // Drives the spawn handshake the same way the first multi-step
    // test does, but injects a `multistepMailRouter` and asserts the
    // deploy router registered a handler against the deployment's
    // mail address by the time `deploy(frame)` resolves. The handler
    // is what the sidecar hub-link's `mail.inbound` path dispatches
    // through; without this registration, an inbound mail aimed at
    // the deployment address falls into the legacy session path,
    // which has no transport entry and no `sessions` row for the
    // deployment address.
    const childIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    let observedEnv: Record<string, string> | undefined;
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      const handle: SubprocessHandle = {
        pid: 9123,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
      return handle;
    };

    const mailRouter = createMultistepMailRouter();
    const { router } = await buildMultistepFixture({
      spawner,
      multistepMailRouter: mailRouter,
    });

    const sources = defaultMultistepSources();
    const definition = {
      id: "wf-mail-router-test",
      triggers: [{ type: "manual" }],
      stepOrder: ["step-1", "step-2"],
      steps: { "step-1": { kind: "step" }, "step-2": { kind: "step" } },
    };
    const frame = makeMultistepFrame({ definition, sources });

    const deployPromise = router.deploy(frame);

    while (observedEnv === undefined) {
      await new Promise((r) => setTimeout(r, 1));
    }

    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeyPair.privateKey,
      channelId,
      writer: {
        write(line: string) {
          childToSupervisor.inject(line);
          return Promise.resolve();
        },
      },
    });
    await childSender.send({
      type: "ready",
      data: {
        childPid: 9123,
        childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString("hex"),
      },
    });

    await deployPromise;

    // The handler must be installed against the deployment's mail
    // address (`frame.agentAddress`), and tryRoute must claim it.
    const claimed = mailRouter.tryRoute(
      frame.agentAddress,
      new Uint8Array([1, 2, 3]),
    );
    expect(claimed).toBe(true);

    // Teardown.
    void supervisorToChild;
  });

  test("does not register a multistepMailRouter handler if spawn rejects", async () => {
    const mailRouter = createMultistepMailRouter();
    const crashSpawner: SubprocessSpawner = () => {
      throw new Error("ENOENT: binary missing");
    };
    const { router } = await buildMultistepFixture({
      spawner: crashSpawner,
      multistepMailRouter: mailRouter,
    });

    const frame = makeMultistepFrame({
      definition: {
        id: "wf-crash-noreg",
        triggers: [{ type: "manual" }],
        stepOrder: ["step-1"],
        steps: { "step-1": { kind: "step" } },
      },
      sources: { "step-1": makeInferenceSource("step-1") },
    });

    await expect(router.deploy(frame)).rejects.toThrow(
      /ENOENT: binary missing/,
    );

    expect(mailRouter.tryRoute(frame.agentAddress, new Uint8Array([1]))).toBe(
      false,
    );
  });

  test("a spawner that throws synchronously surfaces a structured rejection rather than hanging in starting", async () => {
    // Simulates `Bun.spawn` failing to launch (binary missing,
    // permissions error). The router must surface the rejection
    // through `deploy(frame)` without leaving the supervisor wedged.
    const crashSpawner: SubprocessSpawner = () => {
      throw new Error("ENOENT: binary missing");
    };

    const { router } = await buildMultistepFixture({ spawner: crashSpawner });

    const frame = makeMultistepFrame({
      definition: {
        id: "wf-crash",
        triggers: [{ type: "manual" }],
        stepOrder: ["step-1"],
        steps: { "step-1": { kind: "step" } },
      },
      sources: {
        "step-1": makeInferenceSource("step-1"),
      },
    });

    await expect(router.deploy(frame)).rejects.toThrow(
      /ENOENT: binary missing/,
    );
  });

  test("rejects a malformed workflow projection at the router boundary before spawn fires", async () => {
    let spawnerInvoked = false;
    const spawner: SubprocessSpawner = () => {
      spawnerInvoked = true;
      throw new Error("spawner must not run for an invalid projection");
    };

    const { router } = await buildMultistepFixture({ spawner });

    const frame = makeMultistepFrame({
      definition: {
        id: "wf-bad",
        triggers: [{ type: "manual" }],
        // stepOrder mentions a step that has no steps[] entry
        stepOrder: ["step-1", "step-missing"],
        steps: { "step-1": { kind: "step" } },
      },
      sources: {
        "step-1": makeInferenceSource("step-1"),
      },
    });

    await expect(router.deploy(frame)).rejects.toThrow(
      /workflow\.definition\.steps is missing entry/,
    );
    expect(spawnerInvoked).toBe(false);
  });

  test("does not drop the first upstream control frame the child sends after ready", async () => {
    // The supervisor's `pumpUpstreamControl` consumes the same
    // control-receive iterator `waitForReady` initialised. A buggy
    // `waitForReady` that finalised the iterator on `ready` would
    // silently drop the next upstream frame; a correct handoff
    // surfaces a `recycle.request` as a real supervisor.recycle()
    // call, which the supervisor implements by spawning a new child
    // via the injected subprocessSpawner. Counting spawner
    // invocations is the cleanest observable: 1 means the upstream
    // frame was dropped; >=2 means the pump consumed it.
    //
    // The mock spawner serves a fresh control/event pair per call so
    // the recycle path's own ready handshake completes; the test's
    // child sender signs `ready` once per spawn.
    type SpawnFixture = {
      supervisorToChild: ReturnType<typeof createMemoryNdjsonStream>;
      childToSupervisor: ReturnType<typeof createMemoryNdjsonStream>;
      eventChildToSupervisor: ReturnType<typeof createMemoryFrameStream>;
      env: Record<string, string>;
      childIpcKeyPair: { privateKey: Uint8Array; publicKey: Uint8Array };
    };
    const spawns: SpawnFixture[] = [];
    let resolveSpawnAdded: (() => void) | null = null;
    const spawnAdded = (): Promise<void> =>
      new Promise((resolve) => {
        resolveSpawnAdded = resolve;
      });
    const spawner: SubprocessSpawner = ({ env }) => {
      const supervisorToChild = createMemoryNdjsonStream();
      const childToSupervisor = createMemoryNdjsonStream();
      const eventChildToSupervisor = createMemoryFrameStream();
      let resolveExit: ((code: number) => void) | undefined;
      const exited = new Promise<number>((resolve) => {
        resolveExit = resolve;
      });
      const handle: SubprocessHandle = {
        pid: 4400 + spawns.length,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
      // Capture the per-spawn streams synchronously so the test can
      // drive the child side once the supervisor has wired the
      // receiver.
      const fixture: SpawnFixture = {
        supervisorToChild,
        childToSupervisor,
        eventChildToSupervisor,
        env,
        // Mint a fresh child keypair per spawn; the supervisor's
        // receiveControlChannel opens in bootstrap mode and pins on
        // the per-spawn ready frame's `childPublicKey`.
        childIpcKeyPair: {
          publicKey: new Uint8Array(),
          privateKey: new Uint8Array(),
        },
      };
      spawns.push(fixture);
      const r = resolveSpawnAdded;
      resolveSpawnAdded = null;
      if (r) r();
      return handle;
    };

    const { router } = await buildMultistepFixture({ spawner });

    const sources = defaultMultistepSources();
    const definition = {
      id: "wf-handoff",
      triggers: [{ type: "manual" }],
      stepOrder: ["step-1", "step-2"],
      steps: { "step-1": { kind: "step" }, "step-2": { kind: "step" } },
    };
    const frame = makeMultistepFrame({ definition, sources });

    // Helper to drive the child side of one spawn fixture's ready
    // handshake, optionally chaining an upstream `recycle.request`.
    async function driveReady(
      fixture: SpawnFixture,
      opts: { sendRecycleRequest: boolean },
    ): Promise<void> {
      const channelId = fixture.env.IPC_CHANNEL_ID;
      if (channelId === undefined) {
        throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
      }
      const childIpcKeyPair = await generateKeyPair();
      fixture.childIpcKeyPair = childIpcKeyPair;
      const childSender = createControlChannelSender({
        privateKeySeed: childIpcKeyPair.privateKey,
        channelId,
        writer: {
          write(line: string) {
            fixture.childToSupervisor.inject(line);
            return Promise.resolve();
          },
        },
      });
      await childSender.send({
        type: "ready",
        data: {
          childPid: 4400 + spawns.length,
          childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString(
            "hex",
          ),
        },
      });
      if (opts.sendRecycleRequest) {
        await childSender.send({
          type: "recycle.request",
          data: { reason: "iterator-handoff-test" },
        });
      }
    }

    const deployPromise = router.deploy(frame);

    // Wait for the first spawn to land.
    while (spawns.length === 0) {
      await spawnAdded();
    }
    const first = spawns[0];
    if (first === undefined) throw new Error("unreachable");
    // Drive ready + immediate recycle.request on the first spawn.
    await driveReady(first, { sendRecycleRequest: true });

    // The initial deploy's spawn() resolves once `ready` lands. The
    // supervisor's pump consumes the recycle.request and kicks off a
    // recycle, which calls the spawner a second time.
    await deployPromise;

    // Wait for the recycle's respawn.
    while (spawns.length < 2) {
      await spawnAdded();
    }
    const second = spawns[1];
    if (second === undefined) throw new Error("unreachable");
    // Drive ready on the second (recycle's) spawn so the recycle path
    // unwinds cleanly. We do not assert on this spawn's effects; the
    // assertion below covers the iterator-handoff invariant.
    await driveReady(second, { sendRecycleRequest: false });
    // Allow the recycle path to settle its post-ready work.
    await new Promise((r) => setTimeout(r, 25));

    expect(spawns.length).toBeGreaterThanOrEqual(2);
  });

  test("multistepSubstrateEnv carries HUB_WS_URL, SIDECAR_ID, SIDECAR_TOKEN through to the spawn-time env", async () => {
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    let observedEnv: Record<string, string> | undefined;
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      const handle: SubprocessHandle = {
        pid: 7600,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
      return handle;
    };
    const bootEdgeDataDir = await createTempBaseDir("sidecar-boot-edge-data-");
    const { router } = await buildMultistepFixture({
      spawner,
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: bootEdgeDataDir,
        HUB_WS_URL: "ws://hub.example/sidecar-boot",
        SIDECAR_ID: "sidecar-boot-1",
        SIDECAR_TOKEN: "boot-token-abc",
      },
    });
    const sources = defaultMultistepSources();
    const definition = {
      id: "wf-boot-edge",
      triggers: [{ type: "manual" }],
      stepOrder: ["step-1", "step-2"],
      steps: { "step-1": { kind: "step" }, "step-2": { kind: "step" } },
    };
    const frame = makeMultistepFrame({ definition, sources });
    const deployPromise = router.deploy(frame);
    while (observedEnv === undefined) {
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(observedEnv.HUB_WS_URL).toBe("ws://hub.example/sidecar-boot");
    expect(observedEnv.SIDECAR_ID).toBe("sidecar-boot-1");
    expect(observedEnv.SIDECAR_TOKEN).toBe("boot-token-abc");
    expect(observedEnv.SIDECAR_DATA_DIR).toBe(bootEdgeDataDir);
    // Round out the spawn so the test exits cleanly.
    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID missing from spawn env");
    }
    const childIpcKeyPair = await generateKeyPair();
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeyPair.privateKey,
      channelId,
      writer: {
        write(line: string) {
          childToSupervisor.inject(line);
          return Promise.resolve();
        },
      },
    });
    await childSender.send({
      type: "ready",
      data: {
        childPid: 7600,
        childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString("hex"),
      },
    });
    await deployPromise;
  });
});
