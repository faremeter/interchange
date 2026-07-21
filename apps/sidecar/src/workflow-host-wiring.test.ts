import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";

import { createEd25519Crypto, generateKeyPair } from "@intx/crypto";
import { hexEncode } from "@intx/types";
import { createInMemoryTransport } from "@intx/mail-memory";
import type { Principal, RepoId, RepoStore } from "@intx/hub-sessions";
import {
  createControlChannelSender,
  type EventPayload,
  type FrameReader,
  type NdjsonReader,
  type NdjsonWriter,
  type SubprocessHandle,
  type SubprocessSpawner,
} from "@intx/workflow-host";
import type { AgentDeployFrame } from "@intx/types/sidecar";

import {
  assembleRunCredentialsSnapshot,
  computeWireDefinitionHash,
  createSidecarDeployRouter,
  createSidecarWorkflowSupervisor,
  deriveDeploymentId,
  STEP_INFERENCE_SOURCES_ENV_KEY,
  validateWorkflowProjection,
} from "./workflow-host-wiring";
import {
  createMultistepGrantsRouter,
  createMultistepMailRouter,
  createMultistepSourcesRouter,
  type MultistepGrantsRouter,
  type MultistepMailRouter,
  type MultistepSourcesRouter,
} from "./workflow-run-pack-client";
import {
  scanWorkflowDeploymentRecords,
  writeWorkflowDeploymentRecord,
  type WorkflowDeploymentRecord,
} from "./workflow-deployment-record";

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
      return { commitSha: "stub-sha", newlyTerminalRuns: [] };
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
      stepCount: 1,
      stepOrder: ["step1"],
      deploymentMailAddress: "wire-test@example.com",
      deriveStepAddress: ({ deploymentId, stepId }) =>
        `${deploymentId}-${stepId}@example.com`,
      substrateEnv: { DATA_DIR: "/tmp/wire" },
      dynamicSpawnEnv: () => ({}),
      subprocessSpawner: spawner,
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
      stepCount: 1,
      stepOrder: ["step1"],
      deploymentMailAddress: "inbound@example.com",
      deriveStepAddress: ({ deploymentId, stepId }) =>
        `${deploymentId}-${stepId}@example.com`,
      substrateEnv: {},
      dynamicSpawnEnv: () => ({}),
      subprocessSpawner: () => {
        throw new Error("spawner not invoked in this test");
      },
    });
    // Without a subscriber, routeInbound is a no-op rather than a
    // throw -- the wiring's mail bus map is a per-address Set that
    // returns early when no handler is registered.
    expect(() =>
      wired.routeInbound(new TextEncoder().encode("hello")),
    ).not.toThrow();
  });

  test("onRunStart fails a poisoned run and passes an unpoisoned one", async () => {
    // A poisoned run (its `run.grants` write failed) must be rejected at the
    // barrier rather than started under the deploy-time grant set. An
    // unpoisoned run with a per-run grants file on disk resolves normally.
    const tempBase = await createTempBaseDir("sidecar-poison-barrier-");
    const deploymentId = "dep-poison";
    const cleanRunId = "run-clean";
    const grantsDir = path.join(
      tempBase,
      "workflow-run",
      deploymentId,
      "runs",
      cleanRunId,
    );
    await fs.mkdir(grantsDir, { recursive: true });
    const runGrants = [
      { id: "g1", resource: "tool:send-mail", effect: "allow" },
    ];
    await fs.writeFile(
      path.join(grantsDir, "grants.json"),
      JSON.stringify({ grants: runGrants }),
    );

    const readStub: Partial<RepoStore> = {
      getRepoDir(repoId: RepoId): string {
        return path.join(tempBase, repoId.kind, repoId.id);
      },
    };
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; onRunStart reads only via getRepoDir working-tree reads
    const repoStore = new Proxy(readStub as RepoStore, {
      get(target, prop, receiver) {
        const value = Reflect.get(target, prop, receiver);
        if (value !== undefined) return value;
        return () => {
          throw new Error(`stub RepoStore: ${String(prop)} not implemented`);
        };
      },
    });

    const wired = createSidecarWorkflowSupervisor({
      transport: createInMemoryTransport(),
      repoStore,
      signingKeySeed: new Uint8Array(32),
      workflowRunRepoId: { kind: "workflow-run", id: deploymentId },
      workflowRunRef: "refs/heads/main",
      deploymentId,
      stepCount: 1,
      stepOrder: ["step-1"],
      deploymentMailAddress: `${deploymentId}@example.com`,
      deriveStepAddress: ({ deploymentId: dep, stepId }) =>
        `${dep}-${stepId}@example.com`,
      substrateEnv: {},
      dynamicSpawnEnv: () => ({}),
      subprocessSpawner: () => {
        throw new Error("spawner not invoked in this test");
      },
      isRunPoisoned: (runId) => runId === "run-poisoned",
    });

    await expect(
      wired.onRunStart({ runId: "run-poisoned", deploymentId }),
    ).rejects.toThrow(/grants write failed/);

    const snapshot = await wired.onRunStart({
      runId: cleanRunId,
      deploymentId,
    });
    expect(snapshot.steps).toHaveLength(1);
    expect(snapshot.steps[0]?.grants).toEqual(runGrants);
  });
});

describe("createSidecarDeployRouter provision-step (no-spawn) mode", () => {
  test("a provisionStep frame inits the repo and records the hub key without spawning", async () => {
    const transport = createInMemoryTransport();
    const keyPair = await generateKeyPair();

    const initRepoCalls: string[] = [];
    const recordHubKeyCalls: { address: string; hubKey: string }[] = [];

    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- provisionStep touches no RepoStore method; the Proxy throws if it ever does
    const repoStore = new Proxy({} as RepoStore, {
      get(_target, prop) {
        return () => {
          throw new Error(`stub RepoStore: ${String(prop)} not implemented`);
        };
      },
    });

    const router = createSidecarDeployRouter({
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- provisionStep exercises only initRepo
      sessions: {
        initRepo: async (a: string) => {
          initRepoCalls.push(a);
        },
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["sessions"],
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- provisionStep exercises only recordHubKey
      keyStore: {
        recordHubKey: (a: string, h: string) => {
          recordHubKeyCalls.push({ address: a, hubKey: h });
        },
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["keyStore"],
      transport,
      repoStore,
      signingKeySeed: keyPair.privateKey,
      createAgentCrypto: createEd25519Crypto,
      assertSourceBuildable: () => undefined,
      registerDeployment: () => undefined,
      unregisterDeployment: () => undefined,
    });

    const STEP_ADDR = "ins_dep_abc-step1@example.com";
    const HUB_KEY = "aa".repeat(32);
    const result = await router.deploy({
      type: "agent.deploy",
      agentAddress: STEP_ADDR,
      agentId: "ins_dep_abc-step1",
      hubPublicKey: HUB_KEY,
      provisionStep: true,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- provisionStep never inspects config
      config: {} as unknown as Parameters<
        ReturnType<typeof createSidecarDeployRouter>["deploy"]
      >[0]["config"],
    });

    // The step's agent-state repo is initialized and the hub key recorded,
    // so the follow-up full-closure deploy pack applies into a repo and
    // verifies against the recorded key.
    expect(initRepoCalls).toEqual([STEP_ADDR]);
    expect(recordHubKeyCalls).toEqual([
      { address: STEP_ADDR, hubKey: HUB_KEY },
    ]);

    // The ack carries the sidecar principal key (the hub discards it for a
    // workflow-derived per-step address).
    expect(result.publicKey).toMatch(/^[0-9a-f]{64}$/);

    // Nothing spawned: no supervisor, so no active address.
    expect(router.activeAddresses()).toEqual([]);
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

// The supervisor's `parked-correlations.request` control-frame discriminator.
// The re-emit-parked-correlations tests assert this reaches the downstream
// (supervisor -> child) stream when Trigger B fires.
const PARKED_REQUEST_TYPE = "parked-correlations.request";

// One downstream control line is the JSON serialization of a signed envelope:
// `{ envelope: { seq, channelId, payload }, sig }`. The re-emit tests inspect
// `envelope.payload.type` to detect a `parked-correlations.request` without
// verifying the signature (the supervisor's IPC public key is not exposed to
// the test). Structurally validate the envelope-carrying shape and return the
// payload discriminator, or `undefined` when the line is not a typed frame.
const DownstreamFrameShape = type({
  envelope: {
    payload: {
      "type?": "string",
      "+": "ignore",
    },
    "+": "ignore",
  },
  "+": "ignore",
});

function downstreamPayloadType(line: string): string | undefined {
  const parsed: unknown = JSON.parse(line);
  const validated = DownstreamFrameShape(parsed);
  if (validated instanceof type.errors) return undefined;
  return validated.envelope.payload.type;
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
      return { commitSha: "stub-sha", newlyTerminalRuns: [] };
    },
    // The deploy router's grants bridge writes `state/grants.json` to
    // each step's agent-state repo before `spawn()`. Mirror the
    // `getRepoDir` layout so the write lands where the subsequent
    // `assembleCredentialsSnapshot` working-tree read looks for it.
    async writeTree(_p, repoId, _ref, content) {
      const dir = path.join(tempBase, repoId.kind, repoId.id);
      for (const [relPath, contents] of Object.entries(content.files)) {
        const full = path.join(dir, relPath);
        await fs.mkdir(path.dirname(full), { recursive: true });
        await fs.writeFile(full, contents);
      }
      return { commitSha: "stub-sha", newlyTerminalRuns: [] };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; only getRepoDir + writeTreePreservingPrefix + writeTree exercised
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
// A single source: each step's `sources` value is an ordered failover chain,
// so the fixture element type is the chain's member.
type InferenceSourceFixture = WorkflowProjection["sources"][string][number];

type MultistepDeployArgs = {
  sources: WorkflowProjection["sources"];
  definition: {
    id: string;
    triggers: unknown[];
    stepOrder: string[];
    steps: Record<string, unknown>;
  };
  /**
   * Override the deploy frame's `agentAddress`. Single-step projections
   * are the agent-launch identity path: the deploy router derives the
   * sole step's agent-state repo from `parseAgentId(agentAddress)`, which
   * requires the canonical `ins_<id>@<domain>` shape. Tests that drive a
   * single-step projection supply a valid instance address here; the
   * default keeps the historical multi-step address for the multi-step
   * tests (whose derived per-step repos do not parse the frame address).
   */
  agentAddress?: string;
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
    agentAddress: args.agentAddress ?? "multi@example.com",
    agentId: "multi-agent",
    hubPublicKey: "hub-pk",
    // The wire-side HarnessConfig has many required fields. On the
    // workflow deploy path the router reads only `config.sessionId`
    // and `config.grants`, both of which tolerate the empty
    // placeholder (they resolve to `undefined`), so an opaque `{}`
    // satisfies the surface contract for these tests.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the workflow path reads only config.sessionId/config.grants, which tolerate undefined
    config: {} as AgentDeployFrame["config"],
    workflow: {
      definition: args.definition,
      sources: args.sources,
    },
  };
}

function defaultMultistepSources(): WorkflowProjection["sources"] {
  return {
    "step-1": [makeInferenceSource("step-1")],
    "step-2": [makeInferenceSource("step-2")],
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

  test("rejects an empty sources chain for a stepOrder id", () => {
    expect(() =>
      validateWorkflowProjection({
        definition: {
          id: "w-1",
          stepOrder: ["step-1"],
          steps: { "step-1": {} },
        },
        sources: { "step-1": [] },
      }),
    ).toThrow(/must be a non-empty array/);
  });

  test("rejects a non-array sources entry for a stepOrder id", () => {
    expect(() =>
      validateWorkflowProjection({
        definition: {
          id: "w-1",
          stepOrder: ["step-1"],
          steps: { "step-1": {} },
        },
        sources: { "step-1": {} },
      }),
    ).toThrow(/must be a non-empty array/);
  });

  test("accepts a well-formed projection", () => {
    expect(() =>
      validateWorkflowProjection({
        definition: {
          id: "w-1",
          stepOrder: ["step-1", "step-2"],
          steps: { "step-1": {}, "step-2": {} },
        },
        sources: { "step-1": [{}], "step-2": [{}] },
      }),
    ).not.toThrow();
  });
});

describe("computeWireDefinitionHash", () => {
  test("is stable across key-ordering differences", async () => {
    const a = { id: "w-1", stepOrder: ["s1"], steps: { s1: { kind: "step" } } };
    const b = { steps: { s1: { kind: "step" } }, stepOrder: ["s1"], id: "w-1" };
    expect(await computeWireDefinitionHash(a)).toBe(
      await computeWireDefinitionHash(b),
    );
  });

  test("differs across different definitions", async () => {
    const a = { id: "w-1", stepOrder: ["s1"], steps: { s1: {} } };
    const b = { id: "w-2", stepOrder: ["s1"], steps: { s1: {} } };
    expect(await computeWireDefinitionHash(a)).not.toBe(
      await computeWireDefinitionHash(b),
    );
  });
});

describe("createSidecarDeployRouter multi-step branch", () => {
  async function buildMultistepFixture(opts: {
    spawner: SubprocessSpawner;
    publishWorkflowInferenceEvent?: (
      address: string,
      event: EventPayload,
      sessionId: string | undefined,
    ) => void;
    multistepBinaryPath?: string;
    multistepSubstrateEnv?: Record<string, string>;
    multistepMailRouter?: MultistepMailRouter;
    multistepGrantsRouter?: MultistepGrantsRouter;
    multistepSourcesRouter?: MultistepSourcesRouter;
    registerDeployment?: (args: {
      deploymentId: string;
      agentAddress: string;
    }) => void;
    assertSourceBuildable?: Parameters<
      typeof createSidecarDeployRouter
    >[0]["assertSourceBuildable"];
    /**
     * Reuse an existing transport instead of a fresh one. The restore
     * tests deploy through one fixture, then build a SECOND fixture over
     * the same on-disk data dir with a FRESH transport to model a sidecar
     * process restart (the in-memory transport is process-local, so a
     * restart starts with an empty registration table).
     */
    transport?: ReturnType<typeof createInMemoryTransport>;
    /**
     * Spawn ready-handshake timeout (ms) threaded to every supervisor the
     * router constructs. The ready-timeout test uses a small value with a
     * spawner that never drives `ready`, asserting the deploy rejects with
     * the threaded value echoed in the message.
     */
    readyTimeoutMs?: number;
    /**
     * Fixed keypair the keyStore's `loadOrGenerateKey` returns for the head.
     * The B-key test pins the single-step deploy ack to this agent key; when
     * omitted a fresh keypair is minted per call as before.
     */
    headKeyPair?: Awaited<ReturnType<typeof generateKeyPair>>;
    /**
     * Injectable deployment-record writer. The rotation-interleave tests
     * pass a blockable/failing stub so a recycle can be driven into the
     * rotation's persist window; omitted, the router uses the real writer.
     */
    writeWorkflowDeploymentRecord?: typeof writeWorkflowDeploymentRecord;
  }) {
    const transport = opts.transport ?? createInMemoryTransport();
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
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the workflow path never invokes provisionAgent/persistHubPublicKey (single-step uses the narrow initRepo; the child mints its own key); the stubs throw if it does. initRepo is a no-op for the single-step head repo.
      sessions: {
        provisionAgent: async () => {
          throw new Error("workflow branch must not invoke provisionAgent");
        },
        persistHubPublicKey: async () => {
          throw new Error(
            "workflow branch must not invoke persistHubPublicKey",
          );
        },
        initRepo: async () => undefined,
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["sessions"],
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; the single-step head deploy records the hub key for pack verification
      keyStore: {
        recordHubKey: () => undefined,
        loadOrGenerateKey: async () => ({
          keyPair: opts.headKeyPair ?? (await generateKeyPair()),
          isNew: false,
        }),
        // A single-step spawn failure unwinds the recorded hub key via
        // forgetAgent; the fixture exercises that unwind, so the stub must
        // honor the call.
        forgetAgent: () => undefined,
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["keyStore"],
      transport,
      repoStore,
      signingKeySeed: keyPair.privateKey,
      createAgentCrypto: createEd25519Crypto,
      assertSourceBuildable: opts.assertSourceBuildable ?? (() => undefined),
      registerDeployment: opts.registerDeployment ?? (() => undefined),
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
      ...(opts.multistepGrantsRouter !== undefined
        ? { multistepGrantsRouter: opts.multistepGrantsRouter }
        : {}),
      ...(opts.multistepSourcesRouter !== undefined
        ? { multistepSourcesRouter: opts.multistepSourcesRouter }
        : {}),
      ...(opts.readyTimeoutMs !== undefined
        ? { readyTimeoutMs: opts.readyTimeoutMs }
        : {}),
      ...(opts.writeWorkflowDeploymentRecord !== undefined
        ? {
            writeWorkflowDeploymentRecord: opts.writeWorkflowDeploymentRecord,
          }
        : {}),
    });
    return {
      router,
      tempBase,
      keyPair,
      substrateEnv: mergedSubstrateEnv,
      transport,
    };
  }

  test("validates the projection, constructs SpawnOpts from the frame, drives spawn, and acks the deployment address's public key", async () => {
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
    // The deployment address's own key -- what `loadOrGenerateKey` mints and
    // `signChallenge` signs reconnect challenges with. Pin it so the ack
    // assertion below is deterministic.
    const deploymentKeyPair = await generateKeyPair();
    const { router } = await buildMultistepFixture({
      spawner,
      headKeyPair: deploymentKeyPair,
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
    expect(env.DEFINITION_HASH).toBe(
      await computeWireDefinitionHash(definition),
    );
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
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });

    const result = await deployPromise;
    // Every deployment -- single- or multi-step -- acks the deployment
    // address's own public key, the one `signChallenge` signs reconnect
    // challenges with, so the hub can verify ownership on reconnect. A
    // multi-step deployment previously acked the supervisor principal key,
    // which the hub discarded. The hex is a 64-character lowercase string.
    expect(result.publicKey).toMatch(/^[0-9a-f]{64}$/);
    expect(result.publicKey).toBe(hexEncode(deploymentKeyPair.publicKey));

    // Teardown: kill the child so the spawn-time pumps unwind.
    // Use unused supervisorToChild to silence the linter.
    void supervisorToChild;
    void supervisorIpcKeyPair;
  });

  test("a second same-address deploy is rejected mid-spawn and never deletes the live deployment record", async () => {
    // Pins the synchronous single-flight reservation guard. The first
    // deploy runs its durable writes and then suspends inside
    // supervisor.spawn awaiting the child's `ready` handshake -- the window
    // in which its reservation is held but `activeSupervisors` is not yet
    // populated. A second same-address frame arriving in that window must be
    // rejected at the reservation guard (its own message, distinct from the
    // spawn-core backstop) before it touches durable state, so it cannot
    // delete the first deploy's live record via the soft-fail catch.
    const childIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    let spawnCount = 0;
    let observedEnv: Record<string, string> | undefined;
    const spawner: SubprocessSpawner = ({ env }) => {
      spawnCount += 1;
      observedEnv = env;
      const handle: SubprocessHandle = {
        pid: 4242,
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

    const multiDataDir = await createTempBaseDir("sidecar-concurrent-deploy-");
    const registered: string[] = [];
    const { router } = await buildMultistepFixture({
      spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: multiDataDir },
      registerDeployment: ({ agentAddress }) => {
        registered.push(agentAddress);
      },
    });

    const sources = defaultMultistepSources();
    const definition = {
      id: "wf-concurrent",
      triggers: [{ type: "manual" }],
      stepOrder: ["step-1", "step-2"],
      steps: { "step-1": { kind: "step" }, "step-2": { kind: "step" } },
    };
    const frame = makeMultistepFrame({ definition, sources });
    const deploymentId = deriveDeploymentId(frame.agentAddress);
    const recordFile = path.join(
      multiDataDir,
      "workflow-runs",
      deploymentId,
      "deployment.json",
    );

    const firstDeploy = router.deploy(frame);
    // Wait until the first deploy has spawned: its record is on disk and it
    // is now suspended in the ready handshake with the reservation held.
    while (observedEnv === undefined) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const recordBefore = await fs.readFile(recordFile, "utf8");
    expect(recordBefore.length).toBeGreaterThan(0);

    // The loser is rejected at the reservation guard, not the spawn-core
    // backstop -- the guard's message is the one asserted here.
    await expect(router.deploy(frame)).rejects.toThrow(
      /is already deployed; undeploy it before redeploying/,
    );
    // It never reached the spawner and never deleted the live record.
    expect(spawnCount).toBe(1);
    const recordAfter = await fs.readFile(recordFile, "utf8");
    expect(recordAfter).toBe(recordBefore);

    // Drive the first deploy's ready handshake so it completes, then confirm
    // the winner is the live, registered deployment.
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
        childPid: 4242,
        childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString("hex"),
      },
    });

    const result = await firstDeploy;
    expect(result.publicKey).toMatch(/^[0-9a-f]{64}$/);
    expect(registered).toEqual([frame.agentAddress]);
    expect(router.activeAddresses()).toEqual([frame.agentAddress]);

    void supervisorToChild;
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
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
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

  test("a run.grants frame writes the run's grants to runs/<runId>/grants.json in the workflow-run repo", async () => {
    // Drives the same spawn handshake as the mail-router test, then routes
    // a `run.grants` frame through the injected `multistepGrantsRouter` and
    // asserts the handler the deploy router installed wrote the run's
    // grants to `runs/<runId>/grants.json` inside the deployment's
    // `workflow-run` repo -- sibling to the run's `runs/<runId>/events/`
    // subtree. Nothing reads the grants back yet; the assertion is on the
    // on-disk write (right repo, right path, right content).
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
        pid: 9124,
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

    const grantsRouter = createMultistepGrantsRouter();
    const { router, tempBase } = await buildMultistepFixture({
      spawner,
      multistepGrantsRouter: grantsRouter,
    });

    const sources = defaultMultistepSources();
    const definition = {
      id: "wf-grants-router-test",
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
        childPid: 9124,
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });

    await deployPromise;

    const runId = "run-abc";
    const stepGrants = [
      {
        id: "grant-1",
        resource: "tool:send-mail",
        action: "invoke",
        effect: "allow",
        origin: "creator",
        conditions: null,
        expiresAt: null,
        roleId: null,
        principalId: "prn_deployment",
      },
    ];
    const routed = await grantsRouter.tryRoute({
      type: "run.grants",
      agentAddress: frame.agentAddress,
      runId,
      stepGrants,
    });
    expect(routed).toBe(true);

    // The write lands in the deployment's workflow-run repo at
    // `runs/<runId>/grants.json`, the run-owned sibling of `events/`.
    const deploymentId = deriveDeploymentId(frame.agentAddress);
    const grantsFile = path.join(
      tempBase,
      "workflow-run",
      deploymentId,
      "runs",
      runId,
      "grants.json",
    );
    const onDisk: unknown = JSON.parse(await fs.readFile(grantsFile, "utf8"));
    expect(onDisk).toEqual({ grants: stepGrants });

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
      agentAddress: "ins_crash-noreg@example.com",
      definition: {
        id: "wf-crash-noreg",
        triggers: [{ type: "manual" }],
        stepOrder: ["step-1"],
        steps: { "step-1": { kind: "step" } },
      },
      sources: { "step-1": [makeInferenceSource("step-1")] },
    });

    await expect(router.deploy(frame)).rejects.toThrow(
      /ENOENT: binary missing/,
    );

    expect(mailRouter.tryRoute(frame.agentAddress, new Uint8Array([1]))).toBe(
      false,
    );
  });

  test("a soft-failed deploy (spawn rejects) leaves no restore record", async () => {
    const crashSpawner: SubprocessSpawner = () => {
      throw new Error("ENOENT: binary missing");
    };
    const { router, substrateEnv } = await buildMultistepFixture({
      spawner: crashSpawner,
    });
    const agentAddress = "ins_softfail@example.com";
    const frame = makeMultistepFrame({
      agentAddress,
      definition: {
        id: "wf-softfail",
        triggers: [{ type: "manual" }],
        stepOrder: ["step-1"],
        steps: { "step-1": { kind: "step" } },
      },
      sources: { "step-1": [makeInferenceSource("step-1")] },
    });

    await expect(router.deploy(frame)).rejects.toThrow(/ENOENT/);

    // The record is written before the spawn, so the soft-failure catch must
    // delete it -- a boot-time restore must not re-spawn a deploy that never
    // completed. (A hard crash mid-spawn, by contrast, deliberately leaves
    // the record for the restore to re-drive.)
    const dataDir = substrateEnv.SIDECAR_DATA_DIR;
    if (dataDir === undefined)
      throw new Error("fixture SIDECAR_DATA_DIR unset");
    const recordFile = path.join(
      dataDir,
      "workflow-runs",
      deriveDeploymentId(agentAddress),
      "deployment.json",
    );
    expect(
      await fs.access(recordFile).then(
        () => true,
        () => false,
      ),
    ).toBe(false);
  });

  test("rejects a deploy whose step pins an unbuildable provider before spawning", async () => {
    // The source-admission gate runs before any state is claimed or the
    // child is spawned. A step whose pinned source names a provider the
    // sidecar cannot build must reject the whole deploy synchronously --
    // the admission control property -- rather than spawning a child that
    // fails when the step's inference first resolves.
    let spawnCount = 0;
    const trackingSpawner: SubprocessSpawner = () => {
      spawnCount++;
      throw new Error("spawn must not be reached for an inadmissible source");
    };
    const { router } = await buildMultistepFixture({
      spawner: trackingSpawner,
      assertSourceBuildable: (source) => {
        if (source.provider === "ghost-provider") {
          throw new Error(
            `Source provider "${source.provider}" is not registered`,
          );
        }
      },
    });

    const frame = makeMultistepFrame({
      agentAddress: "ins_unbuildable@example.com",
      definition: {
        id: "wf-unbuildable",
        triggers: [{ type: "manual" }],
        stepOrder: ["step-1"],
        steps: { "step-1": { kind: "step" } },
      },
      sources: {
        "step-1": [
          {
            ...makeInferenceSource("step-1"),
            provider: "ghost-provider",
          },
        ],
      },
    });

    await expect(router.deploy(frame)).rejects.toThrow(
      /ghost-provider.*not registered/,
    );
    expect(spawnCount).toBe(0);
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
      agentAddress: "ins_crash@example.com",
      definition: {
        id: "wf-crash",
        triggers: [{ type: "manual" }],
        stepOrder: ["step-1"],
        steps: { "step-1": { kind: "step" } },
      },
      sources: {
        "step-1": [makeInferenceSource("step-1")],
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
        "step-1": [makeInferenceSource("step-1")],
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
          childPublicKey: hexEncode(childIpcKeyPair.publicKey),
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
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
    await deployPromise;
  });

  test("a registerDeployment failure before spawn unwinds the slug and leaves the address claimable", async () => {
    // The multi-step partial-state unwind for the address-registry step.
    // `registerDeployment` runs BEFORE `supervisor.spawn` -- the replay the
    // spawn kicks off writes through the pack-pushing facade, which must
    // resolve the deployment-address mapping, so the mapping has to exist
    // before the spawn. A `registerDeployment` failure therefore throws
    // before any child is spawned; the unwind must release the slug (and
    // reverse nothing else, since nothing after it ran). The observable
    // evidence is that (a) NO child was spawned for the failed deploy and
    // (b) a subsequent deploy on the SAME address succeeds, which is only
    // possible if the slug was released.
    const childIpcKeyPair = await generateKeyPair();
    const spawnedHandles: {
      pid: number;
      killed: boolean;
      supervisorToChild: ReturnType<typeof createMemoryNdjsonStream>;
      childToSupervisor: ReturnType<typeof createMemoryNdjsonStream>;
      eventChildToSupervisor: ReturnType<typeof createMemoryFrameStream>;
    }[] = [];
    const observedEnvs: Record<string, string>[] = [];
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnvs.push(env);
      const supervisorToChild = createMemoryNdjsonStream();
      const childToSupervisor = createMemoryNdjsonStream();
      const eventChildToSupervisor = createMemoryFrameStream();
      let resolveExit: ((code: number) => void) | undefined;
      const exited = new Promise<number>((resolve) => {
        resolveExit = resolve;
      });
      const record = {
        pid: 9000 + spawnedHandles.length,
        killed: false,
        supervisorToChild,
        childToSupervisor,
        eventChildToSupervisor,
      };
      spawnedHandles.push(record);
      const handle: SubprocessHandle = {
        pid: record.pid,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          record.killed = true;
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
      return handle;
    };

    let registerCallCount = 0;
    const multiDataDir = await createTempBaseDir("sidecar-multi-unwind-");
    const { router } = await buildMultistepFixture({
      spawner,
      multistepBinaryPath: "/fake/bin/multistep-workflow-child",
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: multiDataDir },
      registerDeployment: () => {
        registerCallCount += 1;
        if (registerCallCount === 1) {
          throw new Error("registerDeployment failure (synthetic)");
        }
      },
    });

    async function driveReadyFor(
      handleIndex: number,
      childPid: number,
    ): Promise<void> {
      while (spawnedHandles.length <= handleIndex) {
        await new Promise((r) => setTimeout(r, 1));
      }
      const env = observedEnvs[handleIndex];
      const channelId = env?.IPC_CHANNEL_ID;
      if (channelId === undefined) {
        throw new Error("IPC_CHANNEL_ID missing in observed env");
      }
      const record = spawnedHandles[handleIndex];
      if (record === undefined) {
        throw new Error(`spawnedHandles[${String(handleIndex)}] missing`);
      }
      const childSender = createControlChannelSender({
        privateKeySeed: childIpcKeyPair.privateKey,
        channelId,
        writer: {
          write(line: string) {
            record.childToSupervisor.inject(line);
            return Promise.resolve();
          },
        },
      });
      await childSender.send({
        type: "ready",
        data: {
          childPid,
          childPublicKey: hexEncode(childIpcKeyPair.publicKey),
        },
      });
    }

    const sources = defaultMultistepSources();
    const definition = {
      id: "wf-unwind-test",
      triggers: [{ type: "manual" }],
      stepOrder: ["step-1", "step-2"],
      steps: { "step-1": { kind: "step" }, "step-2": { kind: "step" } },
    };
    const frame = makeMultistepFrame({ definition, sources });

    // The first deploy throws at `registerDeployment`, which now runs before
    // `supervisor.spawn`, so it rejects WITHOUT spawning a child -- no ready
    // handshake to drive.
    let firstCaught: unknown;
    try {
      await router.deploy(frame);
    } catch (err) {
      firstCaught = err;
    }
    expect(firstCaught).toBeInstanceOf(Error);
    expect(firstCaught instanceof Error && firstCaught.message).toMatch(
      /registerDeployment failure \(synthetic\)/,
    );

    // No child was spawned for the failed deploy: the throw preceded spawn.
    expect(spawnedHandles).toHaveLength(0);

    // Re-deploy on the SAME address must succeed. If the unwind missed the
    // slug release, the second deploy would surface a phantom collision. The
    // router's public contract is that a failed deploy leaves the address
    // claimable again. This is the first deploy that actually spawns, so its
    // ready handshake is at index 0.
    const secondDeploy = router.deploy(frame);
    await driveReadyFor(0, 9000);
    const secondResult = await secondDeploy;
    expect(secondResult.publicKey).toMatch(/^[0-9a-f]{64}$/);
    expect(registerCallCount).toBe(2);
  });

  // ------------------------------------------------------------------
  // Boot-time restore of persisted workflow deployments
  // ------------------------------------------------------------------

  // A mock spawner that serves a fresh control/event channel per spawn and
  // lets the test complete each child's `ready` handshake. Both `deploy` and
  // `restoreWorkflowDeployments` block on `supervisor.spawn` until `ready`
  // lands, so every spawned child needs its handshake driven.
  function makeReadyDrivingSpawner(pidBase: number) {
    type Spawn = {
      env: Record<string, string>;
      childToSupervisor: ReturnType<typeof createMemoryNdjsonStream>;
      eventChildToSupervisor: ReturnType<typeof createMemoryFrameStream>;
      childSender?: ReturnType<typeof createControlChannelSender>;
    };
    const spawns: Spawn[] = [];
    const spawner: SubprocessSpawner = ({ env }) => {
      const supervisorToChild = createMemoryNdjsonStream();
      const childToSupervisor = createMemoryNdjsonStream();
      const eventChildToSupervisor = createMemoryFrameStream();
      let resolveExit: ((code: number) => void) | undefined;
      const exited = new Promise<number>((resolve) => {
        resolveExit = resolve;
      });
      spawns.push({ env, childToSupervisor, eventChildToSupervisor });
      const handle: SubprocessHandle = {
        pid: pidBase + spawns.length,
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
    async function driveReadyFor(
      index: number,
      opts?: { sendRecycleRequest?: boolean },
    ): Promise<void> {
      while (spawns.length <= index) {
        await new Promise((r) => setTimeout(r, 1));
      }
      const spawn = spawns[index];
      if (spawn === undefined) {
        throw new Error(`spawn ${String(index)} missing`);
      }
      const channelId = spawn.env.IPC_CHANNEL_ID;
      if (channelId === undefined) {
        throw new Error("IPC_CHANNEL_ID missing in spawn env");
      }
      const childIpcKeyPair = await generateKeyPair();
      const childSender = createControlChannelSender({
        privateKeySeed: childIpcKeyPair.privateKey,
        channelId,
        writer: {
          write(line: string) {
            spawn.childToSupervisor.inject(line);
            return Promise.resolve();
          },
        },
      });
      // Retain the sender so a later recycle.request (recycleRequestFor)
      // signs with the SAME keypair the supervisor pinned from this ready.
      spawn.childSender = childSender;
      await childSender.send({
        type: "ready",
        data: {
          childPid: pidBase + index,
          childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString(
            "hex",
          ),
        },
      });
      if (opts?.sendRecycleRequest === true) {
        // Model the child asking to recycle; the supervisor's upstream pump
        // consumes it and respawns.
        await childSender.send({
          type: "recycle.request",
          data: { reason: "sources-rotation-recycle" },
        });
      }
    }
    return {
      spawner,
      driveReadyFor,
      spawnCount: () => spawns.length,
      envFor: (index: number): Record<string, string> | undefined =>
        spawns[index]?.env,
      async recycleRequestFor(index: number): Promise<void> {
        const sender = spawns[index]?.childSender;
        if (sender === undefined) {
          throw new Error(
            `spawn ${String(index)} has no sender; drive its ready first`,
          );
        }
        await sender.send({
          type: "recycle.request",
          data: { reason: "sources-rotation-recycle" },
        });
      },
    };
  }

  function isRegistered(
    transport: ReturnType<typeof createInMemoryTransport>,
    address: string,
  ): boolean {
    try {
      transport.getTransportFor(address);
      return true;
    } catch {
      return false;
    }
  }

  function recordExists(
    dataDir: string,
    deploymentId: string,
  ): Promise<boolean> {
    return fs
      .access(
        path.join(dataDir, "workflow-runs", deploymentId, "deployment.json"),
      )
      .then(
        () => true,
        () => false,
      );
  }

  function singleStepFrame(
    agentAddress: string,
    definitionId: string,
  ): AgentDeployFrame {
    return makeMultistepFrame({
      agentAddress,
      definition: {
        id: definitionId,
        triggers: [{ type: "manual" }],
        stepOrder: ["step-1"],
        steps: { "step-1": { kind: "step" } },
      },
      sources: { "step-1": [makeInferenceSource("step-1")] },
    });
  }

  test("restore re-spawns a persisted single-step deployment and re-registers its head on a fresh transport", async () => {
    const dataDir = await createTempBaseDir("sidecar-restore-restart-data-");
    const head = "ins_restart@example.com";

    // First process: deploy a single-step workflow. The deploy persists a
    // restore record under `dataDir` and materializes its `workflow.json`.
    const first = makeReadyDrivingSpawner(9100);
    const { router: routerA } = await buildMultistepFixture({
      spawner: first.spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });
    const deployPromise = routerA.deploy(singleStepFrame(head, "wf-restart"));
    await first.driveReadyFor(0);
    await deployPromise;

    // Second process (simulated restart): a FRESH transport (empty
    // registration table) and fresh in-memory router state over the SAME
    // on-disk data dir.
    const second = makeReadyDrivingSpawner(9200);
    const freshTransport = createInMemoryTransport();
    const { router: routerB } = await buildMultistepFixture({
      spawner: second.spawner,
      transport: freshTransport,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    // Nothing is registered before restore -- the restart started clean.
    expect(isRegistered(freshTransport, head)).toBe(false);

    const restorePromise = routerB.restoreWorkflowDeployments();
    await second.driveReadyFor(0);
    await restorePromise;

    // The deployment was re-spawned exactly once and its head is live again.
    expect(second.spawnCount()).toBe(1);
    expect(isRegistered(freshTransport, head)).toBe(true);
  });

  test("restore soft-fails a record whose workflow.json is missing and restores the rest", async () => {
    const dataDir = await createTempBaseDir("sidecar-restore-softfail-data-");
    const goodHead = "ins_good@example.com";
    const badHead = "ins_bad@example.com";

    const first = makeReadyDrivingSpawner(9300);
    const { router: routerA } = await buildMultistepFixture({
      spawner: first.spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });
    const deployGood = routerA.deploy(singleStepFrame(goodHead, "wf-good"));
    await first.driveReadyFor(0);
    await deployGood;
    const deployBad = routerA.deploy(singleStepFrame(badHead, "wf-bad"));
    await first.driveReadyFor(1);
    await deployBad;

    // Remove the bad deployment's definition so its restore read faults.
    await fs.rm(
      path.join(dataDir, "assets", "workflow", "wf-bad", "workflow.json"),
    );

    const second = makeReadyDrivingSpawner(9400);
    const freshTransport = createInMemoryTransport();
    const { router: routerB } = await buildMultistepFixture({
      spawner: second.spawner,
      transport: freshTransport,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    // The good deployment re-spawns (exactly one handshake to drive);
    // scan order is filesystem-dependent, but only the good record spawns.
    const restorePromise = routerB.restoreWorkflowDeployments();
    await second.driveReadyFor(0);
    await restorePromise;

    expect(second.spawnCount()).toBe(1);
    expect(isRegistered(freshTransport, goodHead)).toBe(true);
    expect(isRegistered(freshTransport, badHead)).toBe(false);
    // The failed record is KEPT on disk -- never deleted, unlike a
    // soft-failed deploy -- so a later boot can retry it.
    expect(await recordExists(dataDir, deriveDeploymentId(badHead))).toBe(true);
  });

  test("restore applies validateWorkflowProjection: a stepOrder entry with no matching steps is skipped", async () => {
    const dataDir = await createTempBaseDir("sidecar-restore-validator-data-");
    const head = "ins_validator@example.com";
    const deploymentId = deriveDeploymentId(head);

    // Hand-write a record plus a workflow.json whose `stepOrder` names a step
    // `steps` does not define. This clears the wire arktype
    // (`AgentDeployWorkflow` only checks that `sources` cover `stepOrder`) but
    // MUST be rejected by `validateWorkflowProjection`, the second gate the
    // deploy path applies. If restore ran only the arktype it would spawn a
    // child for a structurally invalid definition.
    const record: WorkflowDeploymentRecord = {
      version: 1,
      agentAddress: head,
      definitionId: "wf-missing-step",
      sources: { "step-1": [makeInferenceSource("step-1")] },
      hubPublicKey: "hub-pk",
    };
    await writeWorkflowDeploymentRecord(dataDir, deploymentId, record);
    const workflowJsonPath = path.join(
      dataDir,
      "assets",
      "workflow",
      "wf-missing-step",
      "workflow.json",
    );
    await fs.mkdir(path.dirname(workflowJsonPath), { recursive: true });
    await fs.writeFile(
      workflowJsonPath,
      JSON.stringify({
        id: "wf-missing-step",
        triggers: [{ type: "manual" }],
        stepOrder: ["step-1"],
        steps: {},
      }),
      "utf8",
    );

    const spawner = makeReadyDrivingSpawner(9500);
    const freshTransport = createInMemoryTransport();
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      transport: freshTransport,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    await router.restoreWorkflowDeployments();

    expect(spawner.spawnCount()).toBe(0);
    expect(isRegistered(freshTransport, head)).toBe(false);
  });

  test("restore is a no-op for a deployment already live in this process", async () => {
    const dataDir = await createTempBaseDir("sidecar-restore-guard-data-");
    const head = "ins_guard@example.com";

    const spawner = makeReadyDrivingSpawner(9600);
    const { router, transport } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    const deployPromise = router.deploy(singleStepFrame(head, "wf-guard"));
    await spawner.driveReadyFor(0);
    await deployPromise;
    expect(spawner.spawnCount()).toBe(1);

    // The record is on disk and the address is live in this same process. A
    // restore pass must NOT spawn a second child for an address the core's
    // double-spawn guard already owns (the transition guard the B-reroute
    // follow-up leans on).
    await router.restoreWorkflowDeployments();

    expect(spawner.spawnCount()).toBe(1);
    expect(isRegistered(transport, head)).toBe(true);
  });

  test("a second deploy for a live address is rejected without orphaning its restore record", async () => {
    const dataDir = await createTempBaseDir("sidecar-restore-dup-data-");
    const head = "ins_dup@example.com";
    const deploymentId = deriveDeploymentId(head);

    const spawner = makeReadyDrivingSpawner(9700);
    const { router, transport } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    const deployPromise = router.deploy(singleStepFrame(head, "wf-dup"));
    await spawner.driveReadyFor(0);
    await deployPromise;
    expect(await recordExists(dataDir, deploymentId)).toBe(true);

    // A second deploy for the already-live address must be rejected WITHOUT
    // touching the running deployment's durable state. The reject fires
    // before any overwrite; without it, deployMultiStep's catch would delete
    // the live deployment's record and release its slug, silently breaking
    // the next restart for a still-running agent.
    await expect(
      router.deploy(singleStepFrame(head, "wf-dup")),
    ).rejects.toThrow(/already deployed/);
    expect(spawner.spawnCount()).toBe(1);
    expect(await recordExists(dataDir, deploymentId)).toBe(true);
    expect(isRegistered(transport, head)).toBe(true);
  });

  test("restore skips a record whose address does not derive its directory name", async () => {
    const dataDir = await createTempBaseDir("sidecar-restore-mismatch-data-");
    const head = "ins_mismatch@example.com";
    // A record filed under a directory that is NOT its own derived slug --
    // a corrupt or misplaced record that must not be restored under the
    // wrong slug.
    const wrongDir = "not-the-right-slug";
    const record: WorkflowDeploymentRecord = {
      version: 1,
      agentAddress: head,
      definitionId: "wf-mismatch",
      sources: { "step-1": [makeInferenceSource("step-1")] },
      hubPublicKey: "hub-pk",
    };
    await writeWorkflowDeploymentRecord(dataDir, wrongDir, record);

    const spawner = makeReadyDrivingSpawner(9800);
    const freshTransport = createInMemoryTransport();
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      transport: freshTransport,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    await router.restoreWorkflowDeployments();

    expect(spawner.spawnCount()).toBe(0);
    expect(isRegistered(freshTransport, head)).toBe(false);
    // The record is kept on a skip, not deleted.
    expect(await recordExists(dataDir, wrongDir)).toBe(true);
  });

  test("restore soft-fails and keeps the record when the pinned source is no longer buildable", async () => {
    const dataDir = await createTempBaseDir(
      "sidecar-restore-unbuildable-data-",
    );
    const head = "ins_unbuildable_restore@example.com";
    const deploymentId = deriveDeploymentId(head);

    // First process: a permissive gate lets the deploy through, persisting
    // the record and its workflow.json.
    const first = makeReadyDrivingSpawner(9900);
    const { router: routerA } = await buildMultistepFixture({
      spawner: first.spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });
    const deployPromise = routerA.deploy(
      singleStepFrame(head, "wf-unbuildable-restore"),
    );
    await first.driveReadyFor(0);
    await deployPromise;

    // Restart with a gate that now rejects the pinned provider.
    const second = makeReadyDrivingSpawner(10000);
    const freshTransport = createInMemoryTransport();
    const { router: routerB } = await buildMultistepFixture({
      spawner: second.spawner,
      transport: freshTransport,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
      assertSourceBuildable: (source) => {
        throw new Error(
          `Source provider "${source.provider}" is not registered`,
        );
      },
    });

    await routerB.restoreWorkflowDeployments();

    expect(second.spawnCount()).toBe(0);
    expect(isRegistered(freshTransport, head)).toBe(false);
    // The record survives so a later boot, once the provider is buildable
    // again, can retry it.
    expect(await recordExists(dataDir, deploymentId)).toBe(true);
  });

  test("restore isolates an unbuildable-provider record: it keeps the record and surfaces the failure while the healthy deployment still restores", async () => {
    const dataDir = await createTempBaseDir(
      "sidecar-restore-unbuildable-isolate-data-",
    );
    const healthyHead = "ins_healthy_isolate@example.com";
    const unbuildableHead = "ins_unbuildable_isolate@example.com";
    const healthyId = deriveDeploymentId(healthyHead);
    const unbuildableId = deriveDeploymentId(unbuildableHead);

    // The unbuildable deployment pins a source whose provider the restart's
    // gate will reject; the healthy deployment keeps the default `anthropic`
    // source the gate admits. Distinguishing on `provider` lets one
    // `assertSourceBuildable` reject exactly one of the two restored records.
    const unbuildableProvider = "phantom-provider";
    function unbuildableSingleStepFrame(): AgentDeployFrame {
      return makeMultistepFrame({
        agentAddress: unbuildableHead,
        definition: {
          id: "wf-unbuildable-isolate",
          triggers: [{ type: "manual" }],
          stepOrder: ["step-1"],
          steps: { "step-1": { kind: "step" } },
        },
        sources: {
          "step-1": [
            { ...makeInferenceSource("step-1"), provider: unbuildableProvider },
          ],
        },
      });
    }

    // First process: a permissive gate lets BOTH deploys through, persisting
    // each record and its workflow.json.
    const first = makeReadyDrivingSpawner(11400);
    const { router: routerA } = await buildMultistepFixture({
      spawner: first.spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });
    const deployHealthy = routerA.deploy(
      singleStepFrame(healthyHead, "wf-healthy-isolate"),
    );
    await first.driveReadyFor(0);
    await deployHealthy;
    const deployUnbuildable = routerA.deploy(unbuildableSingleStepFrame());
    await first.driveReadyFor(1);
    await deployUnbuildable;
    expect(await recordExists(dataDir, healthyId)).toBe(true);
    expect(await recordExists(dataDir, unbuildableId)).toBe(true);

    // Restart: a fresh transport plus a gate that rejects ONLY the phantom
    // provider. Capture the module's warn output through the default console
    // sink (threshold "warning" routes `logger.warn` to `console.warn`) so we
    // can assert the failure is surfaced loudly rather than silently dropped.
    const warnCaptured: string[] = [];
    // eslint-disable-next-line no-console -- intentionally spy on the default sink's warn target to prove the restore failure is surfaced
    const originalWarn = console.warn;
    // eslint-disable-next-line no-console
    console.warn = (...parts: unknown[]) => {
      warnCaptured.push(parts.map((part) => String(part)).join(" "));
    };
    try {
      const second = makeReadyDrivingSpawner(11500);
      const freshTransport = createInMemoryTransport();
      const { router: routerB } = await buildMultistepFixture({
        spawner: second.spawner,
        transport: freshTransport,
        multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
        assertSourceBuildable: (source) => {
          if (source.provider === unbuildableProvider) {
            throw new Error(
              `Source provider "${source.provider}" is not registered`,
            );
          }
        },
      });

      // Only the healthy deployment spawns, so its handshake is the sole one
      // to drive; the unbuildable record faults before its spawner is ever
      // reached. Restore is serial and per-record isolated, so scan order does
      // not change the outcome.
      const restorePromise = routerB.restoreWorkflowDeployments();
      await second.driveReadyFor(0);
      await restorePromise;

      // The unbuildable record did NOT strand the healthy one: it re-spawned
      // exactly once and its head is routable again on the fresh transport.
      expect(second.spawnCount()).toBe(1);
      expect(isRegistered(freshTransport, healthyHead)).toBe(true);

      // The unbuildable deployment was not stood up: no spawn, no route.
      expect(isRegistered(freshTransport, unbuildableHead)).toBe(false);

      // Its record survives -- restore keeps an unrestorable record so a later
      // boot with the provider restored can retry it.
      expect(await recordExists(dataDir, unbuildableId)).toBe(true);

      // The failure is surfaced loudly: a warning naming the failed deployment
      // and the provider rejection reason, not a silent drop.
      const failureWarn = warnCaptured.find(
        (line) =>
          line.includes(unbuildableId) &&
          line.includes(unbuildableProvider) &&
          line.includes("is not registered"),
      );
      expect(failureWarn).toBeDefined();
    } finally {
      // eslint-disable-next-line no-console
      console.warn = originalWarn;
    }
  });

  test("a deploy whose child never signals ready times out and rejects", async () => {
    const dataDir = await createTempBaseDir("sidecar-ready-timeout-data-");
    const head = "ins_readytimeout@example.com";
    const deploymentId = deriveDeploymentId(head);

    // A spawner whose child is created but never driven through the `ready`
    // handshake. With a small threaded readyTimeoutMs the supervisor times
    // out, kills the child, and rejects the spawn. The message echoes the
    // threaded value, so this also proves readyTimeoutMs reaches the
    // supervisor across the router's forwarding.
    const spawner = makeReadyDrivingSpawner(10100);
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      readyTimeoutMs: 40,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    await expect(
      router.deploy(singleStepFrame(head, "wf-readytimeout")),
    ).rejects.toThrow(/did not emit ready within 40ms/);

    // The deploy soft-failed, so its restore record was cleaned up -- a
    // wedged deploy leaves nothing for a later boot to re-spawn.
    expect(await recordExists(dataDir, deploymentId)).toBe(false);
  });

  test("a single-step deploy acks the agent key, not the supervisor key", async () => {
    // The single-step head IS an agent identity: it signs its own reconnect
    // challenges with the agent key, and the hub records the ack's key into
    // agent_instance.publicKey and verifies the challenge against it. So the
    // ack must surface the AGENT key, not the supervisor principal key --
    // otherwise a rerouted instance's reconnect signature never matches.
    const headKeyPair = await generateKeyPair();
    const spawner = makeReadyDrivingSpawner(10300);
    const { router, keyPair: fixtureKeyPair } = await buildMultistepFixture({
      spawner: spawner.spawner,
      headKeyPair,
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: await createTempBaseDir("sidecar-bkey-data-"),
      },
    });

    const deployPromise = router.deploy(
      singleStepFrame("ins_bkey@example.com", "wf-bkey"),
    );
    await spawner.driveReadyFor(0);
    const result = await deployPromise;

    // The supervisor principal key is derived from the fixture's signing seed
    // (fixtureKeyPair); the head's agent key is the distinct headKeyPair.
    expect(result.publicKey).toBe(
      Buffer.from(headKeyPair.publicKey).toString("hex"),
    );
    expect(result.publicKey).not.toBe(
      Buffer.from(fixtureKeyPair.publicKey).toString("hex"),
    );
  });

  test("registers a sources-rotation handler for a single-step deployment", async () => {
    const sourcesRouter = createMultistepSourcesRouter();
    const spawner = makeReadyDrivingSpawner(10600);
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSourcesRouter: sourcesRouter,
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: await createTempBaseDir("sidecar-sources-single-"),
      },
    });

    const deployPromise = router.deploy(
      singleStepFrame("ins_srcsingle@example.com", "wf-srcsingle"),
    );
    await spawner.driveReadyFor(0);
    await deployPromise;

    // The single-step deploy registered a rotation handler, so an inbound
    // sources.update for its address routes.
    expect(
      await sourcesRouter.tryRoute({
        type: "sources.update",
        agentAddress: "ins_srcsingle@example.com",
        sources: [makeInferenceSource("primary")],
        defaultSource: "primary",
      }),
    ).toBe(true);
  });

  test("does not register a sources-rotation handler for a multi-step deployment", async () => {
    const sourcesRouter = createMultistepSourcesRouter();
    const spawner = makeReadyDrivingSpawner(10700);
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSourcesRouter: sourcesRouter,
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: await createTempBaseDir("sidecar-sources-multi-"),
      },
    });

    const frame = makeMultistepFrame({
      definition: {
        id: "wf-srcmulti",
        triggers: [{ type: "manual" }],
        stepOrder: ["step-1", "step-2"],
        steps: { "step-1": { kind: "step" }, "step-2": { kind: "step" } },
      },
      sources: {
        "step-1": [makeInferenceSource("step-1")],
        "step-2": [makeInferenceSource("step-2")],
      },
    });
    const deployPromise = router.deploy(frame);
    await spawner.driveReadyFor(0);
    await deployPromise;

    // A multi-step deployment has no single warm agent to rotate, so no
    // handler is registered and the inbound rotation is unrouted.
    expect(
      await sourcesRouter.tryRoute({
        type: "sources.update",
        agentAddress: frame.agentAddress,
        sources: [makeInferenceSource("primary")],
        defaultSource: "primary",
      }),
    ).toBe(false);
  });

  test("a source rotation survives a recycle respawn", async () => {
    // End-to-end guard for the rotation-survives-recycle fix: the single-step
    // rotation handler mutates `currentSources`, `dynamicSpawnEnv`
    // re-serializes it, and the recycle respawn's STEP_INFERENCE_SOURCES
    // carries the ROTATED table -- not the frozen deploy-time one.
    const sourcesRouter = createMultistepSourcesRouter();
    const spawner = makeReadyDrivingSpawner(10800);
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSourcesRouter: sourcesRouter,
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: await createTempBaseDir("sidecar-rot-survive-"),
      },
    });

    const addr = "ins_rotsurvive@example.com";
    const deployPromise = router.deploy(singleStepFrame(addr, "wf-rotsurvive"));
    await spawner.driveReadyFor(0);
    await deployPromise;

    // The initial spawn carries the deploy-time source table.
    const initialEnv = spawner.envFor(0);
    if (initialEnv === undefined) throw new Error("initial spawn env missing");
    expect(
      JSON.parse(initialEnv[STEP_INFERENCE_SOURCES_ENV_KEY] ?? "null"),
    ).toEqual({ "step-1": [makeInferenceSource("step-1")] });

    // Rotate the single-step deployment's sources in place.
    const rotated = makeInferenceSource("rotated");
    expect(
      await sourcesRouter.tryRoute({
        type: "sources.update",
        agentAddress: addr,
        sources: [rotated],
        defaultSource: "rotated",
      }),
    ).toBe(true);

    // The child asks to recycle; the supervisor respawns.
    await spawner.recycleRequestFor(0);
    while (spawner.spawnCount() < 2) {
      await new Promise((r) => setTimeout(r, 1));
    }
    await spawner.driveReadyFor(1);

    // The recycle respawn's sources are the ROTATED table, proving the
    // rotation survived the recycle (before the fix it reverted to the
    // deploy-time list frozen in substrateEnv).
    const respawnEnv = spawner.envFor(1);
    if (respawnEnv === undefined) throw new Error("respawn env missing");
    expect(
      JSON.parse(respawnEnv[STEP_INFERENCE_SOURCES_ENV_KEY] ?? "null"),
    ).toEqual({ "step-1": [rotated] });
  });

  test("a source rotation survives a full sidecar restart", async () => {
    // Restart-durability: a rotation is persisted into the deployment record,
    // so a fresh sidecar process (a restore over the same data dir) respawns
    // the deployment on the ROTATED sources, not the deploy-time ones.
    const dataDir = await createTempBaseDir("sidecar-rot-restart-");
    const addr = "ins_rotrestart@example.com";

    // First process: deploy a single-step deployment and rotate its sources.
    const sourcesRouter = createMultistepSourcesRouter();
    const first = makeReadyDrivingSpawner(10900);
    const { router: routerA } = await buildMultistepFixture({
      spawner: first.spawner,
      multistepSourcesRouter: sourcesRouter,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });
    const deployPromise = routerA.deploy(
      singleStepFrame(addr, "wf-rotrestart"),
    );
    await first.driveReadyFor(0);
    await deployPromise;

    const rotated = makeInferenceSource("rotated");
    expect(
      await sourcesRouter.tryRoute({
        type: "sources.update",
        agentAddress: addr,
        sources: [rotated],
        defaultSource: "rotated",
      }),
    ).toBe(true);

    // Second process (simulated restart): a fresh router over the SAME data
    // dir restores the deployment from its durable record.
    const second = makeReadyDrivingSpawner(11000);
    const { router: routerB } = await buildMultistepFixture({
      spawner: second.spawner,
      transport: createInMemoryTransport(),
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });
    const restorePromise = routerB.restoreWorkflowDeployments();
    await second.driveReadyFor(0);
    await restorePromise;

    // The restored spawn carries the ROTATED sources, read back from the
    // durable record -- not the deploy-time list.
    const restoredEnv = second.envFor(0);
    if (restoredEnv === undefined) {
      throw new Error("restored spawn env missing");
    }
    expect(
      JSON.parse(restoredEnv[STEP_INFERENCE_SOURCES_ENV_KEY] ?? "null"),
    ).toEqual({ "step-1": [rotated] });
  });

  test("a rotation whose persist fails leaves no partial effect", async () => {
    // Atomicity: if the durable write rejects, the rotation takes NO net
    // effect. The handler swaps currentSources synchronously, then rolls it
    // back on a failed persist, so once the handler throws currentSources is
    // on the deploy-time table (a recycle respawn AFTER the failure is
    // unrotated) and deliverSources is never reached. Rolling the in-memory
    // hint back keeps currentSources and the record in agreement in this
    // no-interleaved-recycle failure case.
    const dataDir = await createTempBaseDir("sidecar-rot-failatomic-");
    const addr = "ins_rotfailatomic@example.com";
    const sourcesRouter = createMultistepSourcesRouter();
    const spawner = makeReadyDrivingSpawner(11100);
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSourcesRouter: sourcesRouter,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    const deployPromise = router.deploy(singleStepFrame(addr, "wf-rotfail"));
    await spawner.driveReadyFor(0);
    await deployPromise;

    // Replace the deployment record file with a directory so the rotation's
    // writeWorkflowDeploymentRecord rejects (EISDIR) -- a deterministic,
    // root-immune write fault (a chmod guard would be bypassed under root).
    const recordFile = path.join(
      dataDir,
      "workflow-runs",
      deriveDeploymentId(addr),
      "deployment.json",
    );
    await fs.rm(recordFile);
    await fs.mkdir(recordFile);

    // The persist rejects, so the route rejects and nothing after the write
    // runs.
    await expect(
      sourcesRouter.tryRoute({
        type: "sources.update",
        agentAddress: addr,
        sources: [makeInferenceSource("rotated")],
        defaultSource: "rotated",
      }),
    ).rejects.toThrow();

    // currentSources was rolled back: a recycle respawn AFTER the failed
    // rotation carries the DEPLOY-TIME sources, proving the failed rotation
    // left no net in-memory effect.
    await spawner.recycleRequestFor(0);
    while (spawner.spawnCount() < 2) {
      await new Promise((r) => setTimeout(r, 1));
    }
    await spawner.driveReadyFor(1);
    const respawnEnv = spawner.envFor(1);
    if (respawnEnv === undefined) throw new Error("respawn env missing");
    expect(
      JSON.parse(respawnEnv[STEP_INFERENCE_SOURCES_ENV_KEY] ?? "null"),
    ).toEqual({ "step-1": [makeInferenceSource("step-1")] });
  });

  test("a recycle interleaving the rotation persist respawns on the rotated sources", async () => {
    // Interleave guard: the rotation swaps currentSources synchronously
    // BEFORE the durable persist, so a recycle that lands inside the persist
    // window respawns the child on the ROTATED sources -- consistent with
    // what is being persisted -- rather than the stale deploy-time table.
    const dataDir = await createTempBaseDir("sidecar-rot-interleave-ok-");
    const addr = "ins_rotinterok@example.com";
    const sourcesRouter = createMultistepSourcesRouter();
    const spawner = makeReadyDrivingSpawner(11200);

    // A persist that blocks on a test-controlled gate once armed, so a recycle
    // can be driven into the rotation's persist window. The deploy's own
    // persist (before the gate is armed) passes straight through to the real
    // writer.
    let gate: Promise<void> | null = null;
    let release: () => void = () => undefined;
    const persist: typeof writeWorkflowDeploymentRecord = async (
      d,
      id,
      rec,
    ) => {
      if (gate !== null) await gate;
      await writeWorkflowDeploymentRecord(d, id, rec);
    };

    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSourcesRouter: sourcesRouter,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
      writeWorkflowDeploymentRecord: persist,
    });

    const deployPromise = router.deploy(singleStepFrame(addr, "wf-rotinterok"));
    await spawner.driveReadyFor(0);
    await deployPromise;

    // Arm the gate so the rotation's persist blocks mid-window.
    gate = new Promise<void>((resolve) => {
      release = resolve;
    });

    // Start the rotation but do not await it: the handler swaps currentSources
    // synchronously, then parks on the blocked persist before deliverSources.
    const rotated = makeInferenceSource("rotated");
    const rotatePromise = sourcesRouter.tryRoute({
      type: "sources.update",
      agentAddress: addr,
      sources: [rotated],
      defaultSource: "rotated",
    });

    // Drive a recycle while the persist is blocked. The respawn env must carry
    // the ROTATED sources, because the synchronous swap already ran.
    await spawner.recycleRequestFor(0);
    while (spawner.spawnCount() < 2) {
      await new Promise((r) => setTimeout(r, 1));
    }
    await spawner.driveReadyFor(1);
    const respawnEnv = spawner.envFor(1);
    if (respawnEnv === undefined) throw new Error("respawn env missing");
    expect(
      JSON.parse(respawnEnv[STEP_INFERENCE_SOURCES_ENV_KEY] ?? "null"),
    ).toEqual({ "step-1": [rotated] });

    // Release the persist; the rotation completes without throwing and the
    // durable record converges on the rotated sources.
    release();
    await rotatePromise;
    const scanned = await scanWorkflowDeploymentRecords(dataDir);
    const record = scanned.find(
      (s) => s.deploymentId === deriveDeploymentId(addr),
    );
    expect(record?.record.sources).toEqual({ "step-1": [rotated] });
  });

  test("a recycle interleaving a failed rotation persist respawns on new sources then rolls back", async () => {
    // The benign residual, pinned: a persist that fails WHILE a recycle
    // interleaves leaves the just-respawned child transiently ahead on the
    // rotated sources (the intended, self-healing direction), while
    // currentSources rolls back to the deploy-time table so the NEXT recycle
    // reverts the child to durable truth.
    const dataDir = await createTempBaseDir("sidecar-rot-interleave-fail-");
    const addr = "ins_rotinterfail@example.com";
    const sourcesRouter = createMultistepSourcesRouter();
    const spawner = makeReadyDrivingSpawner(11300);

    let gate: Promise<void> | null = null;
    let release: () => void = () => undefined;
    const persist: typeof writeWorkflowDeploymentRecord = async (
      d,
      id,
      rec,
    ) => {
      if (gate !== null) {
        await gate;
        throw new Error("rotation persist boom");
      }
      await writeWorkflowDeploymentRecord(d, id, rec);
    };

    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSourcesRouter: sourcesRouter,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
      writeWorkflowDeploymentRecord: persist,
    });

    const deployPromise = router.deploy(
      singleStepFrame(addr, "wf-rotinterfail"),
    );
    await spawner.driveReadyFor(0);
    await deployPromise;

    gate = new Promise<void>((resolve) => {
      release = resolve;
    });

    const rotated = makeInferenceSource("rotated");
    const rotatePromise = sourcesRouter.tryRoute({
      type: "sources.update",
      agentAddress: addr,
      sources: [rotated],
      defaultSource: "rotated",
    });

    // A recycle interleaves the about-to-fail persist: the respawn still
    // carries the ROTATED sources, because the swap already ran.
    await spawner.recycleRequestFor(0);
    while (spawner.spawnCount() < 2) {
      await new Promise((r) => setTimeout(r, 1));
    }
    await spawner.driveReadyFor(1);
    const respawnEnv = spawner.envFor(1);
    if (respawnEnv === undefined) throw new Error("respawn env missing");
    expect(
      JSON.parse(respawnEnv[STEP_INFERENCE_SOURCES_ENV_KEY] ?? "null"),
    ).toEqual({ "step-1": [rotated] });

    // Release the persist so it rejects; the rotation rolls currentSources
    // back and rethrows.
    release();
    await expect(rotatePromise).rejects.toThrow(/rotation persist boom/);

    // A SECOND recycle now respawns on the ROLLED-BACK deploy-time sources,
    // healing the transient down to durable truth.
    await spawner.recycleRequestFor(1);
    while (spawner.spawnCount() < 3) {
      await new Promise((r) => setTimeout(r, 1));
    }
    await spawner.driveReadyFor(2);
    const secondRespawnEnv = spawner.envFor(2);
    if (secondRespawnEnv === undefined) {
      throw new Error("second respawn env missing");
    }
    expect(
      JSON.parse(secondRespawnEnv[STEP_INFERENCE_SOURCES_ENV_KEY] ?? "null"),
    ).toEqual({ "step-1": [makeInferenceSource("step-1")] });
  });

  test("two addresses whose deriveDeploymentId slugs collide are rejected at the second deploy", async () => {
    // deriveDeploymentId substitutes every disallowed character with
    // `-`, so two distinct addresses can collapse to the same slug. The slug
    // IS the workflow-run repoId, so a silent collision would let the second
    // deploy overwrite the first deploy's repo state. claimSlug rejects the
    // second deploy at the router edge, before any spawn or repo write.
    const spawner = makeReadyDrivingSpawner(10400);
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: await createTempBaseDir("sidecar-collision-data-"),
      },
    });

    // `ins_col.a@example.com` and `ins_col-a@example.com` both project to
    // `ins_col-a-example-com` under the slug derivation.
    const deployPromise = router.deploy(
      singleStepFrame("ins_col.a@example.com", "wf-collide"),
    );
    await spawner.driveReadyFor(0);
    await deployPromise;

    await expect(
      router.deploy(singleStepFrame("ins_col-a@example.com", "wf-collide")),
    ).rejects.toThrow(/deriveDeploymentId collision/);
    expect(spawner.spawnCount()).toBe(1);
  });

  test("reEmitParkedCorrelations reaches the live supervisor and sends a parked-correlations.request downstream", async () => {
    // Trigger B: the hub-reconnect fan-out. After a deploy populates
    // `activeSupervisors` for the deployment address, the router's
    // address-dispatch wrapper must reach the live supervisor's own no-arg
    // `reEmitParkedCorrelations`, which the supervisor implements by sending a
    // `parked-correlations.request` control frame down to the child. The mock
    // child never answers, so the supervisor's watchdog eventually fires; the
    // router's fire-and-forget contract means the request frame lands on the
    // downstream stream regardless, which is the observable evidence here.
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
        pid: 10500,
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

    const dataDir = await createTempBaseDir("sidecar-reemit-data-");
    const { router } = await buildMultistepFixture({
      spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    const sources = defaultMultistepSources();
    const definition = {
      id: "wf-reemit",
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
        childPid: 10500,
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
    await deployPromise;

    // `activeSupervisors` is keyed by the frame's agent address; the router
    // routes the re-emit through that same key.
    expect(router.activeAddresses()).toEqual([frame.agentAddress]);

    // Each downstream line is a signed envelope `{ envelope: { seq, channelId,
    // payload }, sig }`; read `envelope.payload.type` without verifying the
    // signature (the supervisor's IPC public key is not exposed to the test).
    // The supervisor emits its own `parked-correlations.request` on spawn (a
    // fresh child becoming addressable), so the trigger is observed as an
    // increase in the downstream request count, not its first appearance.
    function parkedRequestCount(): number {
      return supervisorToChild
        .flushed()
        .filter((line) => downstreamPayloadType(line) === PARKED_REQUEST_TYPE)
        .length;
    }

    // Let the spawn-time Trigger A re-emit settle so the baseline captures it;
    // the assertion below then proves the Trigger B call adds a request on top.
    await new Promise((r) => setTimeout(r, 50));
    const baseline = parkedRequestCount();

    router.reEmitParkedCorrelations(frame.agentAddress);

    // Fire-and-forget: poll the downstream stream until the router's request
    // frame lands on top of the spawn-time baseline, bounded so a wiring break
    // fails the test instead of hanging.
    const deadline = Date.now() + 2000;
    while (parkedRequestCount() <= baseline) {
      if (Date.now() > deadline) {
        throw new Error(
          "reEmitParkedCorrelations did not add a parked-correlations.request to the downstream control stream",
        );
      }
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(parkedRequestCount()).toBeGreaterThan(baseline);
  });

  test("reEmitParkedCorrelations for an address with no active supervisor is a no-op", async () => {
    // The edge boundary: the hub reports an address routable, but no live
    // supervisor owns it (a torn-down or not-yet-respawned deployment). The
    // router must skip it -- neither throw nor drive any downstream frame.
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
        pid: 10600,
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

    const dataDir = await createTempBaseDir("sidecar-reemit-miss-data-");
    const { router } = await buildMultistepFixture({
      spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    const sources = defaultMultistepSources();
    const definition = {
      id: "wf-reemit-miss",
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
        childPid: 10600,
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
    await deployPromise;

    const missAddress = "ins_dep_nonexistent@wf.example";
    expect(router.activeAddresses()).not.toContain(missAddress);

    // The deployed supervisor emits its own `parked-correlations.request` on
    // spawn; the miss-address re-emit must add nothing on top of that
    // baseline. Count downstream requests before and after the no-op call.
    function parkedRequestCount(): number {
      return supervisorToChild
        .flushed()
        .filter((line) => downstreamPayloadType(line) === PARKED_REQUEST_TYPE)
        .length;
    }
    // Settle the spawn-time Trigger A re-emit so it is folded into the baseline
    // rather than racing the post-call window below.
    await new Promise((r) => setTimeout(r, 50));
    const baseline = parkedRequestCount();

    // The no-op skip must not throw for an address with no live supervisor.
    expect(() => router.reEmitParkedCorrelations(missAddress)).not.toThrow();

    // Give any errant fire-and-forget a chance to land, then confirm the miss
    // added no downstream request for the missing address.
    await new Promise((r) => setTimeout(r, 50));
    expect(parkedRequestCount()).toBe(baseline);
  });
});

describe("assembleRunCredentialsSnapshot", () => {
  // getRepoDir mirrors the production `<base>/<kind>/<id>` layout that
  // `createSpawnTestRepoStore` uses, so a file written at
  // `<base>/workflow-run/<deploymentId>/runs/<runId>/grants.json` lands where
  // the sink's working-tree read looks, and a step's deploy-time grants land
  // at `<base>/agent-state/<repoId>/state/grants.json`.
  function createReadStubRepoStore(tempBase: string): RepoStore {
    const stub: Partial<RepoStore> = {
      getRepoDir(repoId: RepoId): string {
        return path.join(tempBase, repoId.kind, repoId.id);
      },
    };
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; the sink reads only via getRepoDir working-tree reads
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

  const principal: Principal = { kind: "hub" };
  const deploymentId = "dep-run-grants";
  const runId = "run-xyz";
  const stepOrder = ["step-1", "step-2"];
  const deriveStepAddress = ({
    deploymentId: dep,
    stepId,
  }: {
    deploymentId: string;
    stepId: string;
  }) => `${dep}-${stepId}@example.com`;

  async function writeRunGrantsFile(
    tempBase: string,
    contents: string,
  ): Promise<void> {
    const dir = path.join(
      tempBase,
      "workflow-run",
      deploymentId,
      "runs",
      runId,
    );
    await fs.mkdir(dir, { recursive: true });
    await fs.writeFile(path.join(dir, "grants.json"), contents);
  }

  async function writeDeployTimeStepGrants(
    tempBase: string,
    stepId: string,
    contents: string,
  ): Promise<void> {
    const dir = path.join(
      tempBase,
      "agent-state",
      `${deploymentId}-${stepId}`,
      "state",
    );
    await fs.mkdir(dir, { recursive: true });
    await fs.writeFile(path.join(dir, "grants.json"), contents);
  }

  test("a run with a per-run grants file reads those grants over deploy-time", async () => {
    const tempBase = await createTempBaseDir("sidecar-run-grants-");
    const repoStore = createReadStubRepoStore(tempBase);

    const runGrants = [
      { id: "run-grant", resource: "tool:send-mail", effect: "allow" },
    ];
    await writeRunGrantsFile(tempBase, JSON.stringify({ grants: runGrants }));
    // Deploy-time grants differ; the per-run read must win, so these are
    // never surfaced.
    await writeDeployTimeStepGrants(
      tempBase,
      "step-1",
      JSON.stringify({ grants: [{ id: "deploy-grant" }] }),
    );

    const snapshot = await assembleRunCredentialsSnapshot({
      repoStore,
      principal,
      deploymentId,
      runId,
      stepOrder,
      deriveStepAddress,
    });

    // Every step carries the single flat per-run grant set, keyed on the
    // deploy-time per-step address, with a shared content hash.
    expect(snapshot.steps).toHaveLength(2);
    for (const step of snapshot.steps) {
      expect(step.grants).toEqual(runGrants);
      expect(step.address).toBe(`${deploymentId}-${step.stepId}@example.com`);
    }
    expect(snapshot.steps[0]?.stepId).toBe("step-1");
    expect(snapshot.steps[1]?.stepId).toBe("step-2");
    expect(snapshot.steps[0]?.contentHash).toBe(
      snapshot.steps[1]?.contentHash ?? "",
    );
  });

  test("a run without a per-run grants file falls back to deploy-time grants", async () => {
    const tempBase = await createTempBaseDir("sidecar-run-grants-");
    const repoStore = createReadStubRepoStore(tempBase);

    // No per-run file: the internal-run inherit path. Each step's grants come
    // from its own agent-state repo instead.
    const step1Grants = [{ id: "deploy-grant-1" }];
    await writeDeployTimeStepGrants(
      tempBase,
      "step-1",
      JSON.stringify({ grants: step1Grants }),
    );
    // step-2 carries no deploy-time file either, which resolves to an empty
    // grant set rather than throwing.

    const snapshot = await assembleRunCredentialsSnapshot({
      repoStore,
      principal,
      deploymentId,
      runId,
      stepOrder,
      deriveStepAddress,
    });

    expect(snapshot.steps).toHaveLength(2);
    expect(snapshot.steps[0]?.stepId).toBe("step-1");
    expect(snapshot.steps[0]?.grants).toEqual(step1Grants);
    expect(snapshot.steps[1]?.stepId).toBe("step-2");
    expect(snapshot.steps[1]?.grants).toEqual([]);
  });

  test("a run with a malformed per-run grants file throws", async () => {
    const tempBase = await createTempBaseDir("sidecar-run-grants-");
    const repoStore = createReadStubRepoStore(tempBase);

    // The file exists but is not valid JSON. A deploy-time file is present so
    // a swallowed error would silently fall back rather than surface.
    await writeRunGrantsFile(tempBase, "{ not valid json");
    await writeDeployTimeStepGrants(
      tempBase,
      "step-1",
      JSON.stringify({ grants: [{ id: "deploy-grant" }] }),
    );

    await expect(
      assembleRunCredentialsSnapshot({
        repoStore,
        principal,
        deploymentId,
        runId,
        stepOrder,
        deriveStepAddress,
      }),
    ).rejects.toThrow(/is not valid JSON/);

    // A file that is valid JSON but violates the `{ grants: [] }` envelope
    // also throws, rather than falling back -- the presence of the file
    // implies a grants frame was delivered.
    await writeRunGrantsFile(tempBase, JSON.stringify({ grants: "not-array" }));
    await expect(
      assembleRunCredentialsSnapshot({
        repoStore,
        principal,
        deploymentId,
        runId,
        stepOrder,
        deriveStepAddress,
      }),
    ).rejects.toThrow(/failed validation/);
  });

  test("a re-dispatched run re-reads the durable per-run grants, not the deploy-time fallback", async () => {
    // On a child respawn the supervisor's `replayProcessingToInbox` moves an
    // already-consumed run's orphaned `processing/` entry back to `inbox/`,
    // so the fresh dispatch loop re-dequeues it and the `onRunStart` grants
    // barrier fires AGAIN for the same runId. The per-run grants file is a
    // durable commit -- `readRunGrants` never deletes it -- so this second
    // resolution must still read `runs/<runId>/grants.json` and win over the
    // deploy-time fallback, rather than inheriting the deployment's grants as
    // if the run had never carried its own.
    const tempBase = await createTempBaseDir("sidecar-run-grants-");
    const repoStore = createReadStubRepoStore(tempBase);

    const runGrants = [
      { id: "run-grant", resource: "tool:send-mail", effect: "allow" },
    ];
    await writeRunGrantsFile(tempBase, JSON.stringify({ grants: runGrants }));
    // A deploy-time file is present so a spurious fallback (a deleted or
    // missed per-run file on the second read) would surface as the wrong
    // grants rather than an empty set.
    await writeDeployTimeStepGrants(
      tempBase,
      "step-1",
      JSON.stringify({ grants: [{ id: "deploy-grant" }] }),
    );

    const first = await assembleRunCredentialsSnapshot({
      repoStore,
      principal,
      deploymentId,
      runId,
      stepOrder,
      deriveStepAddress,
    });

    // The re-dispatch: resolve the same run a second time with no rewrite of
    // the grants file in between.
    const second = await assembleRunCredentialsSnapshot({
      repoStore,
      principal,
      deploymentId,
      runId,
      stepOrder,
      deriveStepAddress,
    });

    for (const snapshot of [first, second]) {
      expect(snapshot.steps).toHaveLength(2);
      for (const step of snapshot.steps) {
        expect(step.grants).toEqual(runGrants);
      }
    }
    // The second resolution is byte-identical to the first: same flat grant
    // set, same content hash across every step.
    expect(second.steps.map((s) => s.contentHash)).toEqual(
      first.steps.map((s) => s.contentHash),
    );
  });
});
