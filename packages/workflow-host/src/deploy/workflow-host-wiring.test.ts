import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";

import {
  createEd25519Crypto,
  createNoopCredentialCipher,
  generateKeyPair,
} from "@intx/crypto";
import { hexEncode, parseRunAddress } from "@intx/types";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import { createInMemoryTransport } from "@intx/mail-memory";
import type { RepoId, RepoStore } from "@intx/hub-sessions";
import {
  createControlChannelSender,
  type EventPayload,
  type SubprocessHandle,
  type SubprocessSpawner,
} from "@intx/workflow-host";
import type { WorkflowDefinition } from "@intx/workflow";
import {
  WORKFLOW_CONTROL_INITIALIZING_ERROR,
  type AgentDeployFrame,
} from "@intx/types/sidecar";
import { waitUntil } from "@intx/types/testing";
import {
  createMemoryFrameStream,
  createMemoryNdjsonStream,
  createSpawnObserver,
  createChangeNotifier,
} from "@intx/workflow-host/testing";
import {
  assembleRunCredentialsSnapshot,
  createSidecarDeployRouter,
  createSidecarWorkflowSupervisor,
  STEP_INFERENCE_SOURCES_ENV_KEY,
  validateWorkflowProjection,
} from "./workflow-host-wiring";
import {
  createDeploymentAddressRegistry,
  createMultistepGrantsRouter,
  createMultistepMailRouter,
  createMultistepSourcesRouter,
  type MultistepGrantsRouter,
  type MultistepMailRouter,
  type MultistepSourcesRouter,
} from "./workflow-run-pack-client";
import {
  scanWorkflowRunRecords,
  writeWorkflowRunRecord,
  type WorkflowRunRecord,
} from "./workflow-run-record";

function deriveWorkflowRunRepoId(agentAddress: string): string {
  return agentAddress.replaceAll(/[^a-zA-Z0-9_-]/g, "-");
}

function parseAgentId(agentAddress: string): string {
  const parsed = parseRunAddress(agentAddress);
  if (parsed === null) {
    throw new Error(`Invalid run address: "${agentAddress}"`);
  }
  return parsed.runId;
}

const resolvedInboundMailPolicy = {
  clean: "admit",
  error: "reject",
  untrustedFrom: "reject",
  mismatchedFrom: "reject",
  absentFrom: "reject",
  invalid: "reject",
  missing: "reject",
  unknown: "reject",
} as const;

function deployHostBindings() {
  return {
    deriveWorkflowRunRepoId,
    parseAgentId,
    inertFlatNamespaceStepIds: (args: {
      definition: { readonly stepOrder: readonly string[] };
    }) => args.definition.stepOrder,
    workflowSourceAssetMountPath: (assetId: string) =>
      `source-assets/${assetId}/`,
    sourceAssetGitDir: (gitDirRoot: string, assetId: string) =>
      path.join(gitDirRoot, assetId),
    readRegistries: () => new Map(),
    resolveHostPlatform: () => ({ os: "linux", cpu: "arm64" }),
    resolveInboundMailPolicy: () => resolvedInboundMailPolicy,
    materializeWorkflowAssets: () => Promise.resolve(undefined),
    maxInlineAssetPayloadBytes: 32 * 1024 * 1024,
    multistepBinaryPath: "/fake/bin/workflow-child",
    multistepSubprocessSpawner: (): never => {
      throw new Error("workflow child spawner was not provided");
    },
    applyFrozenWorkflowClosure: (): never => {
      throw new Error("frozen closure apply was not provided");
    },
  };
}

function createMinimalStubRepoStore(): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(_repoId: RepoId): string {
      return "/tmp/unused";
    },
    async writeTreePreservingPrefix(_p, _id, _ref, args) {
      // The wiring test drives signature attribution via requestCancel; the
      // merge callback runs once with an empty pre-image.
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
      runId: "wire-test",
      stepCount: 1,
      stepOrder: ["step1"],
      deploymentMailAddress: "wire-test@example.com",
      deriveStepAddress: ({ runId, stepId }) =>
        `${runId}-${stepId}@example.com`,
      substrateEnv: { DATA_DIR: "/tmp/wire" },
      dynamicSpawnEnv: () => ({}),
      binaryPath: "/fake/bin/workflow-child",
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
    expect(result.seq).toBe(1);
  });

  test("routeInbound rejects when no subscriber is registered so undelivered mail is withheld", async () => {
    const transport = createInMemoryTransport();
    // Avoid the async generateKeyPair; this test only needs a seed.
    const fakeSeed = new Uint8Array(32);
    const repoStore = createMinimalStubRepoStore();
    const wired = createSidecarWorkflowSupervisor({
      transport,
      repoStore,
      signingKeySeed: fakeSeed,
      workflowRunRepoId: { kind: "workflow-run", id: "inbound" },
      workflowRunRef: "refs/heads/main",
      runId: "inbound",
      stepCount: 1,
      stepOrder: ["step1"],
      deploymentMailAddress: "inbound@example.com",
      deriveStepAddress: ({ runId, stepId }) =>
        `${runId}-${stepId}@example.com`,
      substrateEnv: {},
      dynamicSpawnEnv: () => ({}),
      binaryPath: "/fake/bin/workflow-child",
      subprocessSpawner: () => {
        throw new Error("spawner not invoked in this test");
      },
    });
    // Without a subscriber the delivery is not durably accepted, so
    // routeInbound rejects: the hub-link withholds the ack and the hub
    // redelivers rather than dropping under an ack model.
    await expect(
      wired.routeInbound(new TextEncoder().encode("hello")),
    ).rejects.toThrow(/no active mail subscriber/);
  });

  test("onRunStart fails a poisoned run and passes an unpoisoned one", async () => {
    // A poisoned run (its `run.grants` write failed) is rejected at the
    // barrier rather than started under the deploy-time grant set.
    const tempBase = await createTempBaseDir("sidecar-poison-barrier-");
    const anchorRunId = "dep-poison";
    const cleanRunId = "run-clean";
    const grantsDir = path.join(
      tempBase,
      "workflow-run",
      anchorRunId,
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
      workflowRunRepoId: { kind: "workflow-run", id: anchorRunId },
      workflowRunRef: "refs/heads/main",
      runId: anchorRunId,
      stepCount: 1,
      stepOrder: ["step-1"],
      deploymentMailAddress: `${anchorRunId}@example.com`,
      deriveStepAddress: ({ runId: dep, stepId }) =>
        `${dep}-${stepId}@example.com`,
      substrateEnv: {},
      dynamicSpawnEnv: () => ({}),
      binaryPath: "/fake/bin/workflow-child",
      subprocessSpawner: () => {
        throw new Error("spawner not invoked in this test");
      },
      isRunPoisoned: (runId) => runId === "run-poisoned",
    });

    await expect(
      wired.onRunStart({ runId: "run-poisoned", anchorRunId }),
    ).rejects.toThrow(/grants write failed/);

    const snapshot = await wired.onRunStart({
      runId: cleanRunId,
      anchorRunId,
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
      ...deployHostBindings(),
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
      senderKeyCache: {
        put: async () => undefined,
      },
      transport,
      repoStore,
      signingKeySeed: keyPair.privateKey,
      credentialCipher: createNoopCredentialCipher(),
      createAgentCrypto: createEd25519Crypto,
      assertSourceBuildable: () => undefined,
      registerDeployment: () => undefined,
      unregisterDeployment: () => undefined,
      reportDeploymentRefTips: async () => ({}),
    });

    const STEP_ADDR = "run_abc-step1@example.com";
    const HUB_KEY = "aa".repeat(32);
    const result = await router.deploy({
      type: "agent.deploy",
      agentAddress: STEP_ADDR,
      agentId: "run_abc-step1",
      hubPublicKey: HUB_KEY,
      provisionStep: true,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- provisionStep never inspects config
      config: {} as unknown as Parameters<
        ReturnType<typeof createSidecarDeployRouter>["deploy"]
      >[0]["config"],
    });

    // The repo was initialized and the hub key recorded, so the follow-up
    // deploy pack can apply and verify against the recorded key.
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

// The child-process stream trio every deploy test wires, registered here so
// the streams are always reachable for teardown: a deploy starts pumps that
// read them as long as they stay open, and a test failing before its own
// teardown would leave those pumps running in a shared worker. Tests use only
// the handles they drive; the reaper below owns the rest.
//
// The exit resolver is deliberately NOT registered here: the reaper must not
// settle an exit (see below), and a test that settles its own takes the
// resolver off `createChildStreams()`'s return.
const liveChildStreams: {
  supervisorToChild: ReturnType<typeof createMemoryNdjsonStream>;
  childToSupervisor: ReturnType<typeof createMemoryNdjsonStream>;
  eventChildToSupervisor: ReturnType<typeof createMemoryFrameStream>;
}[] = [];

function createChildStreams() {
  const supervisorToChild = createMemoryNdjsonStream();
  const childToSupervisor = createMemoryNdjsonStream();
  const eventChildToSupervisor = createMemoryFrameStream();
  let resolveExit: ((code: number) => void) | undefined;
  const exited = new Promise<number>((resolve) => {
    resolveExit = resolve;
  });
  // The executor above runs synchronously, so the resolver is assigned by the
  // time anything can call this.
  const settleExit = (code: number): void => {
    if (resolveExit === undefined) {
      throw new Error("exit resolver was not captured");
    }
    resolveExit(code);
  };
  liveChildStreams.push({
    supervisorToChild,
    childToSupervisor,
    eventChildToSupervisor,
  });
  return {
    supervisorToChild,
    childToSupervisor,
    eventChildToSupervisor,
    exited,
    resolveExit: settleExit,
  };
}

afterEach(() => {
  // Close both directions so the deploy's pumps unwind. Closing is idempotent,
  // so tests that already tear themselves down are unaffected.
  //
  // The exit is deliberately left unsettled: settling it tells a supervisor
  // whose deploy SUCCEEDED that its child died unexpectedly, which it answers
  // by respawning -- and the spawner hands every spawn the same streams, so
  // the replacement child reads a closed stream and dies on its handshake.
  // Only the test that fails mid-handshake settles its own exit; there is no
  // live supervisor to respawn.
  for (const streams of liveChildStreams.splice(0)) {
    streams.childToSupervisor.close();
    streams.eventChildToSupervisor.close();
    streams.supervisorToChild.close();
  }
});

function createTempBaseDir(prefix: string): Promise<string> {
  return fs.mkdtemp(path.join(os.tmpdir(), prefix));
}

// The `parked-correlations.request` control-frame discriminator the re-emit
// tests assert reaches the downstream (supervisor -> child) stream.
const PARKED_REQUEST_TYPE = "parked-correlations.request";

// Downstream control lines are signed envelopes `{ envelope, sig }`; read
// `envelope.payload.type` to detect a `parked-correlations.request` without
// verifying the signature (the supervisor's IPC public key is not exposed).
// Return the payload discriminator, or `undefined` for a non-typed frame.
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
 * Stub RepoStore whose `getRepoDir` resolves under the supplied tempBase.
 * Missing `state/grants.json` reads as empty grants, so a fresh tempBase
 * yields the empty credentials snapshot these tests expect.
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
    // Mirror the `getRepoDir` layout so the grants write lands where the
    // subsequent `assembleCredentialsSnapshot` working-tree read looks.
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
// Each step's `sources` is an ordered failover chain; the fixture element is
// the chain's member type.
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
   * Override the deploy frame's `agentAddress`. Single-step tests supply a
   * canonical `run_<id>@<domain>` address (the deploy derives the sole step's
   * agent-state repo from `parseAgentId`); the default keeps the multi-step
   * address.
   */
  agentAddress?: string;
  /**
   * Hub-approved wire hash stamped on the deploy frame's workflow (the
   * child's `DEFINITION_HASH`). Defaults to a placeholder; a re-verify test
   * passes a real computed hash.
   */
  approvedWireHash?: string;
  /**
   * Build a frame with NO `approvedWireHash` to exercise the deploy path's
   * fail-loud guard against recomputing a hub-authority hash.
   */
  omitApprovedWireHash?: boolean;
};

function makeInferenceSource(id: string): InferenceSourceFixture {
  return {
    id,
    provider: "anthropic",
    baseURL: "https://api.anthropic.com",
    credentialId: `sk-${id}`,
    model: "claude-3-5",
  };
}

/**
 * Live definitions the source-ref deploy/restore path reconstructs, keyed by
 * derived deployment id. Module-level so a restore fixture over the same data
 * dir (a simulated restart) reads the same entry the deploy fixture
 * registered.
 */
const deployDefinitionRegistry = new Map<string, WorkflowDefinition>();

/**
 * Upgrade an inert definition to a valid LIVE definition (every step needs a
 * real agent to survive `projectLiveToInert`). `stepOrder` is preserved
 * verbatim.
 */
function toLiveClosureDefinition(
  definition: MultistepDeployArgs["definition"],
): WorkflowDefinition {
  const steps: Record<string, unknown> = {};
  for (const stepId of Object.keys(definition.steps)) {
    steps[stepId] = {
      kind: "step",
      id: stepId,
      agent: {
        id: `agent-${stepId}`,
        systemPrompt: "sys",
        capabilities: [],
        toolFactories: [],
        inference: { sources: [] },
      },
    };
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- a hand-built live definition cannot satisfy the full `WorkflowDefinition` nominal type; it stands in for a real closure evaluation, exactly as the restore test's `closureDefinition` does
  return {
    id: definition.id,
    triggers: definition.triggers,
    stepOrder: definition.stepOrder,
    steps,
  } as unknown as WorkflowDefinition;
}

/**
 * Build a source-ref deploy frame: no inline `definition`, but pinned
 * inference sources, the hub-approved wire hash, and a placeholder source-ref
 * pin. The runnable definition is decoupled from the frame exactly as in
 * production -- re-materialized via the injected `applyFrozenWorkflowClosure`.
 * Registers the caller's definition (upgraded to live) under the frame's
 * derived deployment id so the default closure stub finds it.
 */
function makeMultistepFrame(args: MultistepDeployArgs): AgentDeployFrame {
  const agentAddress = args.agentAddress ?? "multi@example.com";
  deployDefinitionRegistry.set(
    deriveWorkflowRunRepoId(agentAddress),
    toLiveClosureDefinition(args.definition),
  );
  return {
    type: "agent.deploy",
    agentAddress,
    agentId: "multi-agent",
    hubPublicKey: "hub-pk",
    // The router reads only `config.sessionId` and `config.grants`, which
    // tolerate an empty placeholder, so an opaque `{}` satisfies the contract.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the workflow path reads only config.sessionId/config.grants, which tolerate undefined
    config: {} as AgentDeployFrame["config"],
    workflow: {
      sources: args.sources,
      // Placeholder source-ref pin: the deploy/restore path re-materializes
      // the definition through the closure stub keyed by deployment id, so
      // the pin only has to be well-formed.
      sourceRef: {
        source: { kind: "registry", registry: "test-registry" },
        closure: { schemaVersion: "1", topLevel: [], entries: [] },
      },
      // Default the hub-approved hash to a placeholder so deploy tests need
      // not compute one; omit it only to exercise the fail-loud guard.
      ...(args.omitApprovedWireHash
        ? {}
        : { approvedWireHash: args.approvedWireHash ?? "a".repeat(64) }),
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
    /**
     * Injectable sender-key cache. Co-delivery tests pass a spy or throwing
     * stub to observe the grants handler's cache write; omitted, a no-op cache
     * satisfies the dependency.
     */
    senderKeyCache?: Parameters<
      typeof createSidecarDeployRouter
    >[0]["senderKeyCache"];
    multistepSourcesRouter?: MultistepSourcesRouter;
    registerDeployment?: (args: {
      runId: string;
      agentAddress: string;
    }) => void;
    /**
     * Injectable deployment-address unregister hook, defaults to no-op. The
     * self-termination retention test wires a real `deploymentAddressRegistry`
     * to assert the reclaim does NOT remove the mapping.
     */
    unregisterDeployment?: (args: {
      runId: string;
      agentAddress: string;
    }) => void;
    reportDeploymentRefTips?: Parameters<
      typeof createSidecarDeployRouter
    >[0]["reportDeploymentRefTips"];
    assertSourceBuildable?: Parameters<
      typeof createSidecarDeployRouter
    >[0]["assertSourceBuildable"];
    /**
     * Reuse an existing transport instead of a fresh one. Restore tests deploy
     * through one fixture, then build a SECOND over the same data dir with a
     * FRESH transport to model a process restart (the in-memory transport is
     * process-local).
     */
    transport?: ReturnType<typeof createInMemoryTransport>;
    /**
     * Spawn ready-handshake timeout (ms) threaded to every supervisor. The
     * ready-timeout test uses a small value with a spawner that never drives
     * `ready`.
     */
    readyTimeoutMs?: number;
    /**
     * Fixed keypair `loadOrGenerateKey` returns for the head. Pins the
     * single-step deploy ack to this agent key; omitted, a fresh keypair is
     * minted per call.
     */
    headKeyPair?: Awaited<ReturnType<typeof generateKeyPair>>;
    /**
     * Injectable deployment-record writer. Rotation tests pass a
     * blockable/failing stub to drive a recycle into the rotation's persist
     * window; omitted, the real writer is used.
     */
    writeWorkflowRunRecord?: typeof writeWorkflowRunRecord;
    /**
     * Injectable closure materializer. Source-ref deploy/restore tests pass a
     * stub so the path runs without a live registry; omitted, the real
     * `applyFrozenWorkflowClosure` is used.
     */
    applyFrozenWorkflowClosure?: Parameters<
      typeof createSidecarDeployRouter
    >[0]["applyFrozenWorkflowClosure"];
  }) {
    const transport = opts.transport ?? createInMemoryTransport();
    const keyPair = await generateKeyPair();
    const tempBase = await createTempBaseDir("sidecar-multistep-");
    const repoStore = createSpawnTestRepoStore(tempBase);
    // Default the data dir to a per-test mkdtemp so deploy tests avoid /tmp;
    // callers can override `SIDECAR_DATA_DIR` via `multistepSubstrateEnv`.
    const defaultSubstrateEnv: Record<string, string> = {
      SIDECAR_DATA_DIR: await createTempBaseDir("sidecar-multistep-data-"),
      // The closure materializer requires both substrate byte caps; default
      // them so a deploy test need not thread them.
      SIDECAR_CACHE_MAX_BYTES: "1000000",
      SIDECAR_REGISTRY_MAX_TARBALL_BYTES: "1000000",
    };
    const mergedSubstrateEnv: Record<string, string> = {
      ...defaultSubstrateEnv,
      ...(opts.multistepSubstrateEnv ?? {}),
    };
    // Default stub returns the live definition `makeMultistepFrame` registered
    // under the deployment id, so the deploy/restore evaluates the topology
    // the frame described. Tests that hand-write a record or want a bespoke
    // closure result pass their own `applyFrozenWorkflowClosure`.
    const defaultApplyFrozenWorkflowClosure: NonNullable<
      Parameters<
        typeof createSidecarDeployRouter
      >[0]["applyFrozenWorkflowClosure"]
    > = (applyArgs) => {
      const deploymentId = path.basename(applyArgs.instanceDir);
      const definition = deployDefinitionRegistry.get(deploymentId);
      if (definition === undefined) {
        throw new Error(
          `test default applyFrozenWorkflowClosure: no registered definition for deploymentId ${deploymentId} (instanceDir ${applyArgs.instanceDir}). Build the deploy frame via makeMultistepFrame/singleStepFrame, or pass an explicit applyFrozenWorkflowClosure when hand-writing a record.`,
        );
      }
      return Promise.resolve({
        definition,
        packageDir: path.join(tempBase, "closure-package", deploymentId),
        deployDir: path.join(tempBase, "closure-deploy", deploymentId),
      });
    };
    const router = createSidecarDeployRouter({
      ...deployHostBindings(),
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
        // A spawn failure unwinds the recorded hub key via forgetAgent; the
        // fixture exercises that unwind, so honor the call.
        forgetAgent: () => undefined,
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["keyStore"],
      senderKeyCache: opts.senderKeyCache ?? {
        put: async () => undefined,
      },
      transport,
      repoStore,
      signingKeySeed: keyPair.privateKey,
      credentialCipher: createNoopCredentialCipher(),
      createAgentCrypto: createEd25519Crypto,
      assertSourceBuildable: opts.assertSourceBuildable ?? (() => undefined),
      registerDeployment: opts.registerDeployment ?? (() => undefined),
      unregisterDeployment:
        opts.unregisterDeployment ??
        (() => {
          /* no-op */
        }),
      reportDeploymentRefTips:
        opts.reportDeploymentRefTips ?? (async () => ({})),
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
      ...(opts.writeWorkflowRunRecord !== undefined
        ? {
            writeWorkflowRunRecord: opts.writeWorkflowRunRecord,
          }
        : {}),
      applyFrozenWorkflowClosure:
        opts.applyFrozenWorkflowClosure ?? defaultApplyFrozenWorkflowClosure,
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
    const {
      supervisorToChild,
      childToSupervisor,
      eventChildToSupervisor,
      exited,
      resolveExit,
    } = createChildStreams();
    let observedBinary: string | undefined;
    let observedEnv: Record<string, string> | undefined;
    // Scoped to this fixture: `first()` must resolve with THIS spawn, not
    // the file's earliest.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ binaryPath, env }) => {
      observedBinary = binaryPath;
      observedEnv = env;
      spawnObserver.record(env);
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
    // Pin the deployment address's own key so the ack assertion is
    // deterministic.
    const deploymentKeyPair = await generateKeyPair();
    const { router } = await buildMultistepFixture({
      spawner,
      headKeyPair: deploymentKeyPair,
      multistepBinaryPath: "/fake/bin/multistep-workflow-child",
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: multiDataDir,
      },
    });

    // createSidecarWorkflowSupervisor does not expose ipcKeyPairFactory, so
    // the fixture keys the `ready` frame with the child's keypair; the
    // supervisor accepts a bootstrap signature from any childPublicKey in the
    // `ready` payload.

    const sources = defaultMultistepSources();
    const definition = {
      id: "wf-router-test",
      triggers: [{ type: "manual" }],
      stepOrder: ["step-1", "step-2"],
      steps: { "step-1": { kind: "step" }, "step-2": { kind: "step" } },
    };
    // The child's DEFINITION_HASH is the frame's hub-approved hash verbatim
    // (never a recompute); stamp the real wire hash and assert it arrives.
    const approvedWireHash = await computeWireDefinitionHash(definition);
    const frame = makeMultistepFrame({ definition, sources, approvedWireHash });

    const deployPromise = router.deploy(frame);

    // Wait until the spawner has been invoked.
    observedEnv = await spawnObserver.first();

    const env = observedEnv;
    expect(observedBinary).toBe("/fake/bin/multistep-workflow-child");
    expect(env).toMatchObject({
      SIDECAR_DATA_DIR: multiDataDir,
      DEPLOYMENT_ID: "multi-example-com",
      MAILBOX_ADDRESS: "multi@example.com",
    });
    expect(env.DEFINITION_HASH).toBe(approvedWireHash);
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
    // Every deployment acks the deployment address's own public key so the
    // hub can verify ownership on reconnect (multi-step previously acked the
    // supervisor principal key, which the hub discarded).
    expect(result.publicKey).toMatch(/^[0-9a-f]{64}$/);
    expect(result.publicKey).toBe(hexEncode(deploymentKeyPair.publicKey));
    void supervisorIpcKeyPair;
  });

  test("sources the child's DEFINITION_HASH from the frame's hub-approved wire hash, not a sidecar recompute", async () => {
    const {
      supervisorToChild,
      childToSupervisor,
      eventChildToSupervisor,
      exited,
      resolveExit,
    } = createChildStreams();
    let observedEnv: Record<string, string> | undefined;
    // Scoped to this fixture: `first()` must resolve with THIS spawn, not
    // the file's earliest.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      spawnObserver.record(env);
      return {
        pid: 9001,
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
    };

    const multiDataDir = await createTempBaseDir("sidecar-approved-hash-");
    const { router } = await buildMultistepFixture({
      spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: multiDataDir },
    });

    const sources = defaultMultistepSources();
    const definition = {
      id: "wf-approved-hash",
      triggers: [{ type: "manual" }],
      stepOrder: ["step-1", "step-2"],
      steps: { "step-1": { kind: "step" }, "step-2": { kind: "step" } },
    };
    // Deliberately NOT the wire hash of `definition`, so the child receiving
    // it proves the value came from the frame, not a sidecar recompute.
    const HUB_APPROVED_HASH = "hub-approved-sentinel-hash";
    const recomputed = await computeWireDefinitionHash(definition);
    expect(HUB_APPROVED_HASH).not.toBe(recomputed);

    const frame = makeMultistepFrame({
      definition,
      sources,
      approvedWireHash: HUB_APPROVED_HASH,
    });
    const deployPromise = router.deploy(frame);

    observedEnv = await spawnObserver.first();
    const env = observedEnv;
    // The child's DEFINITION_HASH is the frame's hub-approved hash, verbatim.
    expect(env.DEFINITION_HASH).toBe(HUB_APPROVED_HASH);

    // Drive the child to exit so the spawn pumps unwind; the deploy settles
    // (rejecting mid-handshake is fine -- the env assertion is the subject).
    const channelId = env.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    childToSupervisor.close();
    eventChildToSupervisor.close();
    resolveExit?.(0);
    await deployPromise.catch(() => undefined);
  });

  test("a second same-address deploy is rejected mid-spawn and never deletes the live run record", async () => {
    // Pins the synchronous single-flight guard: a second same-address frame
    // arriving while the first deploy suspends in the `ready` handshake (its
    // reservation held, `activeSupervisors` not yet populated) must be
    // rejected before it touches durable state or deletes the live record.
    const childIpcKeyPair = await generateKeyPair();
    const {
      supervisorToChild,
      childToSupervisor,
      eventChildToSupervisor,
      exited,
      resolveExit,
    } = createChildStreams();
    let spawnCount = 0;
    let observedEnv: Record<string, string> | undefined;
    // Scoped to this fixture: `first()` must resolve with THIS spawn, not
    // the file's earliest.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      spawnCount += 1;
      observedEnv = env;
      spawnObserver.record(env);
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
    const frame = makeMultistepFrame({
      definition,
      sources,
      agentAddress: "run_concurrent@example.com",
    });
    const anchorRunId = deriveWorkflowRunRepoId(frame.agentAddress);
    const recordFile = path.join(
      multiDataDir,
      "workflow-runs",
      anchorRunId,
      "deployment.json",
    );

    const firstDeploy = router.deploy(frame);
    // Wait until the first deploy suspends in the ready handshake with its
    // reservation held.
    observedEnv = await spawnObserver.first();
    const recordBefore = await fs.readFile(recordFile, "utf8");
    expect(recordBefore.length).toBeGreaterThan(0);

    if (router.control === undefined)
      throw new Error("router.control is undefined");
    await expect(
      router.control({
        type: "workflow.control",
        requestId: "stop-during-deploy",
        agentAddress: frame.agentAddress,
        runId: "run_concurrent",
        action: "stop",
        reason: "Lifetime expired",
      }),
    ).rejects.toThrow(WORKFLOW_CONTROL_INITIALIZING_ERROR);

    // Rejected at the reservation guard, not the spawn-core backstop -- its
    // message is the one asserted.
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
  });

  test("a stop reports the deployment's ref tips and a cancellation reports none", async () => {
    const agentAddress = "run_stoptips@example.com";
    const reported: string[] = [];
    const { router } = await buildMultistepFixture({
      spawner: () => {
        throw new Error("control must not spawn");
      },
      reportDeploymentRefTips: async (address) => {
        reported.push(address);
        return { "refs/heads/main": "main-tip", "refs/heads/events": null };
      },
    });
    if (router.control === undefined)
      throw new Error("router.control is undefined");
    const command = {
      type: "workflow.control",
      requestId: "stop-tips",
      agentAddress,
      runId: "run_stoptips",
      action: "stop",
      reason: "Lifetime expired",
    } as const;

    expect(await router.control(command)).toEqual({
      refTips: { "refs/heads/main": "main-tip", "refs/heads/events": null },
    });
    expect(
      await router.control({
        ...command,
        requestId: "cancel-tips",
        action: "cancel",
      }),
    ).toEqual({});
    expect(reported).toEqual([agentAddress]);
  });

  test("registers a multistepMailRouter handler against the deployment address once spawn succeeds", async () => {
    // Same spawn handshake as the first multi-step test, but with a
    // `multistepMailRouter`: asserts the deploy registered a handler against
    // the deployment's mail address by the time `deploy(frame)` resolves.
    // Without it, inbound mail falls into the legacy session path.
    const childIpcKeyPair = await generateKeyPair();
    const {
      supervisorToChild,
      childToSupervisor,
      eventChildToSupervisor,
      exited,
      resolveExit,
    } = createChildStreams();
    let observedEnv: Record<string, string> | undefined;
    // Scoped to this fixture: `first()` must resolve with THIS spawn, not
    // the file's earliest.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      spawnObserver.record(env);
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

    observedEnv = await spawnObserver.first();

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

    // The handler must be installed at `frame.agentAddress` and tryRoute must
    // claim it.
    const claimed = mailRouter.tryRoute(
      frame.agentAddress,
      new Uint8Array([1, 2, 3]),
    );
    expect(claimed).not.toBeNull();
    // Settle the durable promise to avoid an unhandled rejection; this test
    // only asserts the address is claimed.
    await claimed?.catch(() => undefined);
  });

  test("a run.grants frame writes the run's grants to runs/<runId>/grants.json in the workflow-run repo", async () => {
    // Same spawn handshake as the mail-router test, then routes a `run.grants`
    // frame through `multistepGrantsRouter` and asserts the handler wrote
    // `runs/<runId>/grants.json` in the deployment's `workflow-run` repo.
    const childIpcKeyPair = await generateKeyPair();
    const {
      supervisorToChild,
      childToSupervisor,
      eventChildToSupervisor,
      exited,
      resolveExit,
    } = createChildStreams();
    let observedEnv: Record<string, string> | undefined;
    // Scoped to this fixture: `first()` must resolve with THIS spawn, not
    // the file's earliest.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      spawnObserver.record(env);
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

    observedEnv = await spawnObserver.first();

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

    // The write lands at `runs/<runId>/grants.json`, the run-owned sibling of
    // `events/`.
    const anchorRunId = deriveWorkflowRunRepoId(frame.agentAddress);
    const grantsFile = path.join(
      tempBase,
      "workflow-run",
      anchorRunId,
      "runs",
      runId,
      "grants.json",
    );
    const onDisk: unknown = JSON.parse(await fs.readFile(grantsFile, "utf8"));
    expect(onDisk).toEqual({ grants: stepGrants });
  });

  // Deploy a multi-step deployment through the full spawn/ready handshake so
  // the grants handler is installed, then hand back what a co-delivery test
  // needs to route a `run.grants` frame and inspect the write.
  async function deployMultistepForGrants(
    definitionId: string,
    senderKeyCache: Parameters<
      typeof createSidecarDeployRouter
    >[0]["senderKeyCache"],
  ): Promise<{
    grantsRouter: MultistepGrantsRouter;
    agentAddress: string;
    anchorRunId: string;
    tempBase: string;
  }> {
    const childIpcKeyPair = await generateKeyPair();
    const {
      supervisorToChild,
      childToSupervisor,
      eventChildToSupervisor,
      exited,
      resolveExit,
    } = createChildStreams();
    let observedEnv: Record<string, string> | undefined;
    // Scoped to this fixture: `first()` must resolve with THIS spawn, not
    // the file's earliest.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      spawnObserver.record(env);
      return {
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
    };

    const grantsRouter = createMultistepGrantsRouter();
    const { router, tempBase } = await buildMultistepFixture({
      spawner,
      multistepGrantsRouter: grantsRouter,
      senderKeyCache,
    });

    const definition = {
      id: definitionId,
      triggers: [{ type: "manual" }],
      stepOrder: ["step-1", "step-2"],
      steps: { "step-1": { kind: "step" }, "step-2": { kind: "step" } },
    };
    const frame = makeMultistepFrame({
      definition,
      sources: defaultMultistepSources(),
    });
    const deployPromise = router.deploy(frame);

    observedEnv = await spawnObserver.first();
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

    return {
      grantsRouter,
      agentAddress: frame.agentAddress,
      anchorRunId: deriveWorkflowRunRepoId(frame.agentAddress),
      tempBase,
    };
  }

  test("caches each co-delivered sender key before the run's grants land", async () => {
    // The sender key must be cached BEFORE grants.json is written, so a
    // durable grant never misses the key its recipient verifies against.
    // `grantsFileRef`'s path fills in once the deploy resolves the run's
    // on-disk location; the spy reads it at `put` time.
    const grantsFileRef: { path: string | undefined } = { path: undefined };
    const puts: { address: string; grantsExisted: boolean }[] = [];
    const senderKeyCache = {
      get: () => undefined,
      put: async (address: string) => {
        const grantsExisted =
          grantsFileRef.path !== undefined &&
          (await fs
            .stat(grantsFileRef.path)
            .then(() => true)
            .catch(() => false));
        puts.push({ address, grantsExisted });
      },
      evict: async () => undefined,
      addresses: () => [],
      rotatableAddresses: () => [],
    };

    const { grantsRouter, agentAddress, anchorRunId, tempBase } =
      await deployMultistepForGrants("wf-sender-key-order", senderKeyCache);

    const runId = "run-codeliver";
    const grantsFilePath = path.join(
      tempBase,
      "workflow-run",
      anchorRunId,
      "runs",
      runId,
      "grants.json",
    );
    grantsFileRef.path = grantsFilePath;
    const routed = await grantsRouter.tryRoute({
      type: "run.grants",
      agentAddress,
      runId,
      stepGrants: [],
      senderIdentities: [
        {
          address: "sender@peer.example",
          publicKey: hexEncode(new Uint8Array(32)),
        },
      ],
    });

    expect(routed).toBe(true);
    expect(puts).toEqual([
      { address: "sender@peer.example", grantsExisted: false },
    ]);
    const grantsLanded = await fs
      .stat(grantsFilePath)
      .then(() => true)
      .catch(() => false);
    expect(grantsLanded).toBe(true);
  });

  test("a sender-key cache-write failure fails the run's grants", async () => {
    // The cache write gates the grants write: a throw propagates and
    // grants.json never lands, so the run cannot start under an unverifiable
    // grant.
    const senderKeyCache = {
      get: () => undefined,
      put: async () => {
        throw new Error("sender-key disk full");
      },
      evict: async () => undefined,
      addresses: () => [],
      rotatableAddresses: () => [],
    };

    const { grantsRouter, agentAddress, anchorRunId, tempBase } =
      await deployMultistepForGrants("wf-sender-key-fault", senderKeyCache);

    const runId = "run-cache-fault";
    await expect(
      grantsRouter.tryRoute({
        type: "run.grants",
        agentAddress,
        runId,
        stepGrants: [],
        senderIdentities: [
          {
            address: "sender@peer.example",
            publicKey: hexEncode(new Uint8Array(32)),
          },
        ],
      }),
    ).rejects.toThrow("sender-key disk full");

    const grantsFilePath = path.join(
      tempBase,
      "workflow-run",
      anchorRunId,
      "runs",
      runId,
      "grants.json",
    );
    const grantsLanded = await fs
      .stat(grantsFilePath)
      .then(() => true)
      .catch(() => false);
    expect(grantsLanded).toBe(false);
  });

  test("skips a malformed co-delivered key without failing the run's grants", async () => {
    // A malformed key is a hub-side defect, not a disk fault: it must NOT
    // poison the run (a persistently bad key would wedge every replay). The
    // valid siblings still cache and the grants still land.
    const puts: string[] = [];
    const senderKeyCache = {
      get: () => undefined,
      put: async (address: string) => {
        puts.push(address);
      },
      evict: async () => undefined,
      addresses: () => [],
      rotatableAddresses: () => [],
    };

    const { grantsRouter, agentAddress, anchorRunId, tempBase } =
      await deployMultistepForGrants("wf-sender-key-malformed", senderKeyCache);

    const runId = "run-malformed";
    const routed = await grantsRouter.tryRoute({
      type: "run.grants",
      agentAddress,
      runId,
      stepGrants: [],
      senderIdentities: [
        // Wrong length: valid hex, but 16 bytes rather than 32.
        {
          address: "bad@peer.example",
          publicKey: hexEncode(new Uint8Array(16)),
        },
        {
          address: "good@peer.example",
          publicKey: hexEncode(new Uint8Array(32)),
        },
      ],
    });

    expect(routed).toBe(true);
    expect(puts).toEqual(["good@peer.example"]);
    const grantsFilePath = path.join(
      tempBase,
      "workflow-run",
      anchorRunId,
      "runs",
      runId,
      "grants.json",
    );
    const grantsLanded = await fs
      .stat(grantsFilePath)
      .then(() => true)
      .catch(() => false);
    expect(grantsLanded).toBe(true);
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
      agentAddress: "run_crash-noreg@example.com",
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

    expect(
      mailRouter.tryRoute(frame.agentAddress, new Uint8Array([1])),
    ).toBeNull();
  });

  test("a soft-failed deploy (spawn rejects) leaves no restore record", async () => {
    const crashSpawner: SubprocessSpawner = () => {
      throw new Error("ENOENT: binary missing");
    };
    const { router, substrateEnv } = await buildMultistepFixture({
      spawner: crashSpawner,
    });
    const agentAddress = "run_softfail@example.com";
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

    // The record is written before the spawn, so a soft failure must delete
    // it: a boot-time restore must not re-spawn a deploy that never completed.
    // (A hard crash mid-spawn deliberately leaves the record to re-drive.)
    const dataDir = substrateEnv.SIDECAR_DATA_DIR;
    if (dataDir === undefined)
      throw new Error("fixture SIDECAR_DATA_DIR unset");
    const recordFile = path.join(
      dataDir,
      "workflow-runs",
      deriveWorkflowRunRepoId(agentAddress),
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
    // The admission gate runs before any state is claimed or child spawned:
    // a step pinning an unbuildable provider rejects the deploy synchronously
    // rather than spawning a child that fails at first inference.
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
      agentAddress: "run_unbuildable@example.com",
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
    // Simulates `Bun.spawn` failing to launch; the router must surface the
    // rejection through `deploy(frame)` without wedging the supervisor.
    const crashSpawner: SubprocessSpawner = () => {
      throw new Error("ENOENT: binary missing");
    };

    const { router } = await buildMultistepFixture({ spawner: crashSpawner });

    const frame = makeMultistepFrame({
      agentAddress: "run_crash@example.com",
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

    // `stepOrder` names `step-missing`, which `steps` does not define. The
    // live->inert projector rejects the dangling entry at the router boundary,
    // before any spawn fires.
    await expect(router.deploy(frame)).rejects.toThrow(
      /stepOrder names "step-missing" .* the steps record has no such entry/,
    );
    expect(spawnerInvoked).toBe(false);
  });

  test("refuses to deploy a workflow frame carrying no approvedWireHash rather than recomputing it", async () => {
    // The child re-verifies its recompute against the HUB-approved wire hash.
    // A frame carrying none is a wiring bug: the sidecar must fail loud rather
    // than substitute its own recompute (a self-check). Only a malformed frame
    // reaches this guard.
    let spawnerInvoked = false;
    const spawner: SubprocessSpawner = () => {
      spawnerInvoked = true;
      throw new Error("spawner must not run for a hash-less frame");
    };

    const { router } = await buildMultistepFixture({ spawner });

    const frame = makeMultistepFrame({
      // Single-step parses the frame address as a run address; use the
      // canonical `run_<id>@<domain>` shape to reach the hash guard.
      agentAddress: "run_nohash@example.com",
      definition: {
        id: "wf-no-hash",
        triggers: [{ type: "manual" }],
        stepOrder: ["step-1"],
        steps: { "step-1": { kind: "step" } },
      },
      sources: { "step-1": [makeInferenceSource("step-1")] },
      omitApprovedWireHash: true,
    });

    await expect(router.deploy(frame)).rejects.toThrow(
      /must carry approvedWireHash/,
    );
    expect(spawnerInvoked).toBe(false);
  });

  test("does not drop the first upstream control frame the child sends after ready", async () => {
    // `pumpUpstreamControl` consumes the same control-receive iterator
    // `waitForReady` initialised; a buggy handoff that finalised it on `ready`
    // would drop the next upstream frame. Counting spawner invocations is the
    // observable: 1 means the frame was dropped, >=2 means the pump consumed
    // it. The mock spawner serves a fresh stream pair per call so the recycle
    // path's own handshake completes.
    type SpawnFixture = {
      supervisorToChild: ReturnType<typeof createMemoryNdjsonStream>;
      childToSupervisor: ReturnType<typeof createMemoryNdjsonStream>;
      eventChildToSupervisor: ReturnType<typeof createMemoryFrameStream>;
      env: Record<string, string>;
      childIpcKeyPair: { privateKey: Uint8Array; publicKey: Uint8Array };
    };
    const spawnsChanges = createChangeNotifier();
    const spawns: SpawnFixture[] = [];
    const spawner: SubprocessSpawner = ({ env }) => {
      const {
        supervisorToChild,
        childToSupervisor,
        eventChildToSupervisor,
        exited,
        resolveExit,
      } = createChildStreams();
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
      // Capture the per-spawn streams synchronously so the test can drive the
      // child side once the supervisor wires the receiver.
      const fixture: SpawnFixture = {
        supervisorToChild,
        childToSupervisor,
        eventChildToSupervisor,
        env,
        // Fresh child keypair per spawn; the bootstrap-mode control channel
        // pins on the ready frame's `childPublicKey`.
        childIpcKeyPair: {
          publicKey: new Uint8Array(),
          privateKey: new Uint8Array(),
        },
      };
      spawns.push(fixture);
      spawnsChanges.notify();
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

    // Drive the child side of one spawn's ready handshake, optionally chaining
    // an upstream `recycle.request`.
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
    await spawnsChanges.until(() => spawns.length > 0);
    const first = spawns[0];
    if (first === undefined) throw new Error("unreachable");
    // Drive ready + immediate recycle.request on the first spawn.
    await driveReady(first, { sendRecycleRequest: true });

    // The pump consumes the recycle.request and kicks off a recycle, calling
    // the spawner a second time.
    await deployPromise;

    // Wait for the recycle's respawn.
    await spawnsChanges.until(() => spawns.length >= 2);
    const second = spawns[1];
    if (second === undefined) throw new Error("unreachable");
    // Drive ready on the recycle's spawn so the path unwinds cleanly; the
    // assertion below covers the iterator-handoff invariant.
    await driveReady(second, { sendRecycleRequest: false });

    expect(spawns.length).toBeGreaterThanOrEqual(2);
  });

  test("multistepSubstrateEnv carries HUB_WS_URL, SIDECAR_ID, SIDECAR_TOKEN through to the spawn-time env", async () => {
    const {
      supervisorToChild,
      childToSupervisor,
      eventChildToSupervisor,
      exited,
      resolveExit,
    } = createChildStreams();
    let observedEnv: Record<string, string> | undefined;
    // Scoped to this fixture: `first()` must resolve with THIS spawn, not
    // the file's earliest.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      spawnObserver.record(env);
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
    observedEnv = await spawnObserver.first();
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
    // `registerDeployment` runs BEFORE `supervisor.spawn` (the spawn's replay
    // writes through the pack-pushing facade, which must resolve the address
    // mapping), so a failure throws before any child is spawned. The unwind
    // must release the slug: the observable is that no child spawned, and a
    // re-deploy on the SAME address succeeds.
    const childIpcKeyPair = await generateKeyPair();
    const spawnedHandlesChanges = createChangeNotifier();
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
      const {
        supervisorToChild,
        childToSupervisor,
        eventChildToSupervisor,
        exited,
        resolveExit,
      } = createChildStreams();
      const record = {
        pid: 9000 + spawnedHandles.length,
        killed: false,
        supervisorToChild,
        childToSupervisor,
        eventChildToSupervisor,
      };
      spawnedHandles.push(record);
      spawnedHandlesChanges.notify();
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
      await spawnedHandlesChanges.until(
        () => spawnedHandles.length > handleIndex,
      );
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

    // The first deploy throws at `registerDeployment`, before spawn -- no
    // ready handshake to drive.
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

    // Re-deploy on the SAME address must succeed: a missed slug release would
    // surface a phantom collision. A failed deploy leaves the address
    // claimable again.
    const secondDeploy = router.deploy(frame);
    await driveReadyFor(0, 9000);
    const secondResult = await secondDeploy;
    expect(secondResult.publicKey).toMatch(/^[0-9a-f]{64}$/);
    expect(registerCallCount).toBe(2);
  });

  // ------------------------------------------------------------------
  // Boot-time restore of persisted workflow deployments
  // ------------------------------------------------------------------

  // Mock spawner serving a fresh control/event channel per spawn, with each
  // child's `ready` handshake completed by the test. Both `deploy` and
  // `restoreWorkflowRuns` block on spawn until `ready` lands.
  function makeReadyDrivingSpawner(pidBase: number) {
    type Spawn = {
      env: Record<string, string>;
      childToSupervisor: ReturnType<typeof createMemoryNdjsonStream>;
      eventChildToSupervisor: ReturnType<typeof createMemoryFrameStream>;
      childSender?: ReturnType<typeof createControlChannelSender>;
    };
    const spawnsChanges = createChangeNotifier();
    const spawns: Spawn[] = [];
    // One-shot spawn failure: when armed, the next spawner invocation throws
    // then disarms. Fails a recycle respawn so the supervisor self-terminates
    // without a ready-handshake timeout.
    let failNext = false;
    const spawner: SubprocessSpawner = ({ env }) => {
      if (failNext) {
        failNext = false;
        throw new Error("makeReadyDrivingSpawner: armed spawn failure");
      }
      const {
        supervisorToChild,
        childToSupervisor,
        eventChildToSupervisor,
        exited,
        resolveExit,
      } = createChildStreams();
      spawns.push({ env, childToSupervisor, eventChildToSupervisor });
      spawnsChanges.notify();
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
      await spawnsChanges.until(() => spawns.length > index);
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
      // Retain the sender so a later recycle.request signs with the SAME
      // keypair the supervisor pinned from this ready.
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
      awaitSpawnCount: (count: number) =>
        spawnsChanges.until(() => spawns.length >= count),
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
      // Arm a one-shot spawn failure for the next spawner invocation.
      failNextSpawn(): void {
        failNext = true;
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
    anchorRunId: string,
  ): Promise<boolean> {
    return fs
      .access(
        path.join(dataDir, "workflow-runs", anchorRunId, "deployment.json"),
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
    const head = "run_restart@example.com";

    // First process: deploy a single-step workflow, persisting its restore
    // record and `workflow.json` under `dataDir`.
    const first = makeReadyDrivingSpawner(9100);
    const { router: routerA } = await buildMultistepFixture({
      spawner: first.spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });
    const deployPromise = routerA.deploy(singleStepFrame(head, "wf-restart"));
    await first.driveReadyFor(0);
    await deployPromise;

    // Second process (simulated restart): a FRESH transport and in-memory
    // router state over the SAME on-disk data dir.
    const second = makeReadyDrivingSpawner(9200);
    const freshTransport = createInMemoryTransport();
    const { router: routerB } = await buildMultistepFixture({
      spawner: second.spawner,
      transport: freshTransport,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    // Nothing is registered before restore -- the restart started clean.
    expect(isRegistered(freshTransport, head)).toBe(false);

    const restorePromise = routerB.restoreWorkflowRuns();
    await second.driveReadyFor(0);
    await restorePromise;

    // The deployment was re-spawned exactly once and its head is live again.
    expect(second.spawnCount()).toBe(1);
    expect(isRegistered(freshTransport, head)).toBe(true);
  });

  test("restore soft-fails a record whose closure will not materialize and restores the rest", async () => {
    const dataDir = await createTempBaseDir("sidecar-restore-softfail-data-");
    const goodHead = "run_good@example.com";
    const badHead = "run_bad@example.com";

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

    // Make the bad deployment's closure materialization fault (its pinned
    // code no longer resolves) so its restore soft-fails; the good one
    // materializes and re-spawns.
    const badDeploymentId = deriveWorkflowRunRepoId(badHead);
    const second = makeReadyDrivingSpawner(9400);
    const freshTransport = createInMemoryTransport();
    const { router: routerB } = await buildMultistepFixture({
      spawner: second.spawner,
      transport: freshTransport,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
      applyFrozenWorkflowClosure: (applyArgs) => {
        const deploymentId = path.basename(applyArgs.instanceDir);
        if (deploymentId === badDeploymentId) {
          throw new Error(
            `closure materialization faulted for ${deploymentId}: pinned code did not resolve`,
          );
        }
        const definition = deployDefinitionRegistry.get(deploymentId);
        if (definition === undefined) {
          throw new Error(
            `test applyFrozenWorkflowClosure: no registered definition for ${deploymentId}`,
          );
        }
        return Promise.resolve({
          definition,
          packageDir: path.join(dataDir, "closure-package", deploymentId),
          deployDir: path.join(dataDir, "closure-deploy", deploymentId),
        });
      },
    });

    // Only the good record spawns (one handshake to drive); scan order is
    // filesystem-dependent.
    const restorePromise = routerB.restoreWorkflowRuns();
    await second.driveReadyFor(0);
    await restorePromise;

    expect(second.spawnCount()).toBe(1);
    expect(isRegistered(freshTransport, goodHead)).toBe(true);
    expect(isRegistered(freshTransport, badHead)).toBe(false);
    // The failed record is KEPT on disk (unlike a soft-failed deploy) so a
    // later boot can retry it.
    expect(await recordExists(dataDir, deriveWorkflowRunRepoId(badHead))).toBe(
      true,
    );
  });

  test("restore rejects a closure-derived definition whose stepOrder names a step with no matching entry", async () => {
    const dataDir = await createTempBaseDir("sidecar-restore-validator-data-");
    const head = "run_validator@example.com";
    const anchorRunId = deriveWorkflowRunRepoId(head);

    // A well-formed source-ref record clears the scan boundary, but its
    // closure evaluates to a structurally invalid definition (`stepOrder`
    // names a step `steps` does not define). The restore arm projects the
    // closure-derived definition before spawning, so it rejects the dangling
    // entry and never spawns for a broken definition.
    const record: WorkflowRunRecord = {
      version: 1,
      agentAddress: head,
      definitionId: "wf-missing-step",
      sources: { "step-1": [makeInferenceSource("step-1")] },
      hubPublicKey: "hub-pk",
      approvedWireHash: "a".repeat(64),
      lineage: "source-ref",
      sourceRef: {
        source: { kind: "registry", registry: "test-registry" },
        closure: { schemaVersion: "1", topLevel: [], entries: [] },
      },
    };
    await writeWorkflowRunRecord(
      dataDir,
      anchorRunId,
      record,
      createNoopCredentialCipher(),
    );

    const spawner = makeReadyDrivingSpawner(9500);
    const freshTransport = createInMemoryTransport();
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      transport: freshTransport,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
      applyFrozenWorkflowClosure: (applyArgs) =>
        Promise.resolve({
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- a hand-built live definition cannot satisfy the full WorkflowDefinition nominal type; this stands in for a closure that evaluates to a structurally invalid definition
          definition: {
            id: "wf-missing-step",
            triggers: [{ type: "manual" }],
            stepOrder: ["step-1", "step-missing"],
            steps: {
              "step-1": {
                kind: "step",
                id: "step-1",
                agent: {
                  id: "agent-step-1",
                  systemPrompt: "sys",
                  capabilities: [],
                  toolFactories: [],
                  inference: { sources: [] },
                },
              },
            },
          } as unknown as WorkflowDefinition,
          packageDir: path.join(applyArgs.instanceDir, "package"),
          deployDir: path.join(applyArgs.instanceDir, "deploy"),
        }),
    });

    await router.restoreWorkflowRuns();

    expect(spawner.spawnCount()).toBe(0);
    expect(isRegistered(freshTransport, head)).toBe(false);
  });

  test("restore soft-skips a malformed source-ref record (missing sourceRef) at the scan boundary", async () => {
    const dataDir = await createTempBaseDir("sidecar-restore-srcref-bad-");
    const head = "run_srcref@example.com";
    const deploymentId = deriveWorkflowRunRepoId(head);

    // Write a raw source-ref record with none of the schema-required fields
    // (sourceRef pin, approvedWireHash) straight to the record path, bypassing
    // the typed writer. The scan soft-skips it as corruption, so restore never
    // reaches a spawn.
    const recordPath = path.join(
      dataDir,
      "workflow-runs",
      deploymentId,
      "deployment.json",
    );
    await fs.mkdir(path.dirname(recordPath), { recursive: true });
    await fs.writeFile(
      recordPath,
      JSON.stringify({
        version: 1,
        agentAddress: head,
        definitionId: "wf-srcref",
        sources: { "step-1": [makeInferenceSource("step-1")] },
        hubPublicKey: "hub-pk",
        lineage: "source-ref",
      }),
      "utf8",
    );

    const spawner = makeReadyDrivingSpawner(9550);
    const freshTransport = createInMemoryTransport();
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      transport: freshTransport,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    await router.restoreWorkflowRuns();

    // Rejected at the scan boundary: no child spawned, nothing registered.
    expect(spawner.spawnCount()).toBe(0);
    expect(isRegistered(freshTransport, head)).toBe(false);
  });

  test("restore re-materializes a source-ref deployment's closure and re-spawns it as source-ref", async () => {
    const dataDir = await createTempBaseDir("sidecar-restore-srcref-ok-");
    const head = "run_srcref_ok@example.com";
    const deploymentId = deriveWorkflowRunRepoId(head);

    // A well-formed source-ref record: sourceRef pin + approvedWireHash,
    // exactly what the deploy path persists.
    const record: WorkflowRunRecord = {
      version: 1,
      agentAddress: head,
      definitionId: "wf-srcref",
      sources: { "step-1": [makeInferenceSource("step-1")] },
      hubPublicKey: "hub-pk",
      approvedWireHash: "a".repeat(64),
      lineage: "source-ref",
      sourceRef: {
        source: { kind: "registry", registry: "test-registry" },
        closure: { schemaVersion: "1", topLevel: [], entries: [] },
      },
    };
    await writeWorkflowRunRecord(
      dataDir,
      deploymentId,
      record,
      createNoopCredentialCipher(),
    );

    // The restore arm reconstructs the definition from the re-materialized
    // closure, NOT the on-disk inert workflow.json. Write a DELIBERATELY-CORRUPT
    // workflow.json: a successful restore proves the closure is the source of
    // truth.
    const workflowJsonPath = path.join(
      dataDir,
      "assets",
      "workflow",
      "wf-srcref",
      "workflow.json",
    );
    await fs.mkdir(path.dirname(workflowJsonPath), { recursive: true });
    await fs.writeFile(workflowJsonPath, "}{ not valid json", "utf8");

    // Stub the closure materializer: return a fake package dir plus the
    // evaluated live definition the restore arm projects to the inert wire
    // shape -- the SAME `projectLiveToInert` computation the deploy path
    // applies. The projection must cover the record's sources (`step-1`).
    const fakePackageDir = path.join(dataDir, "fake-closure-package");
    const closureDefinition = {
      id: "wf-srcref",
      triggers: [{ type: "manual" }],
      stepOrder: ["step-1"],
      steps: {
        "step-1": {
          kind: "step",
          id: "step-1",
          agent: {
            id: "agent-1",
            systemPrompt: "sys",
            capabilities: [],
            toolFactories: [],
            inference: { sources: [] },
          },
        },
      },
    };
    const applyCalls: { registry: string; entryCount: number }[] = [];
    const applyStub: NonNullable<
      Parameters<typeof buildMultistepFixture>[0]["applyFrozenWorkflowClosure"]
    > = (args) => {
      if (args.source.kind !== "registry") {
        throw new Error(
          `applyStub expected a registry source, got ${args.source.kind}`,
        );
      }
      applyCalls.push({
        registry: args.source.registry,
        entryCount: args.closure.entries.length,
      });
      return Promise.resolve({
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the stub stands in for a real closure evaluation; a minimal hand-built live definition cannot satisfy the full `WorkflowDefinition` nominal type
        definition: closureDefinition as unknown as WorkflowDefinition,
        packageDir: fakePackageDir,
        deployDir: path.join(dataDir, "fake-deploy-dir"),
      });
    };

    const spawner = makeReadyDrivingSpawner(9560);
    const freshTransport = createInMemoryTransport();
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      transport: freshTransport,
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: dataDir,
        SIDECAR_CACHE_MAX_BYTES: "1000000",
        SIDECAR_REGISTRY_MAX_TARBALL_BYTES: "1000000",
      },
      applyFrozenWorkflowClosure: applyStub,
    });

    const restorePromise = router.restoreWorkflowRuns();
    await spawner.driveReadyFor(0);
    await restorePromise;

    // Re-materialized (the frozen closure fed to the materializer) and spawned.
    expect(applyCalls).toEqual([{ registry: "test-registry", entryCount: 0 }]);
    expect(spawner.spawnCount()).toBe(1);
    expect(isRegistered(freshTransport, head)).toBe(true);
    // The spawn env carries the re-materialized closure package dir, so the
    // child evaluates the pinned code rather than reading a definition off
    // disk.
    const spawnEnv = spawner.envFor(0);
    expect(spawnEnv?.CLOSURE_PACKAGE_DIR).toBe(fakePackageDir);
  });

  test("restore is a no-op for a deployment already live in this process", async () => {
    const dataDir = await createTempBaseDir("sidecar-restore-guard-data-");
    const head = "run_guard@example.com";

    const spawner = makeReadyDrivingSpawner(9600);
    const { router, transport } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    const deployPromise = router.deploy(singleStepFrame(head, "wf-guard"));
    await spawner.driveReadyFor(0);
    await deployPromise;
    expect(spawner.spawnCount()).toBe(1);

    // The address is already live in this process; a restore pass must NOT
    // spawn a second child for an address the core's double-spawn guard owns.
    await router.restoreWorkflowRuns();

    expect(spawner.spawnCount()).toBe(1);
    expect(isRegistered(transport, head)).toBe(true);
  });

  test("a second deploy for a live address is rejected without orphaning its restore record", async () => {
    const dataDir = await createTempBaseDir("sidecar-restore-dup-data-");
    const head = "run_dup@example.com";
    const anchorRunId = deriveWorkflowRunRepoId(head);

    const spawner = makeReadyDrivingSpawner(9700);
    const { router, transport } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    const deployPromise = router.deploy(singleStepFrame(head, "wf-dup"));
    await spawner.driveReadyFor(0);
    await deployPromise;
    expect(await recordExists(dataDir, anchorRunId)).toBe(true);

    // A second deploy for an already-live address must reject WITHOUT touching
    // the running deployment's durable state: without the guard, the catch
    // would delete its record and release its slug.
    await expect(
      router.deploy(singleStepFrame(head, "wf-dup")),
    ).rejects.toThrow(/already deployed/);
    expect(spawner.spawnCount()).toBe(1);
    expect(await recordExists(dataDir, anchorRunId)).toBe(true);
    expect(isRegistered(transport, head)).toBe(true);
  });

  test("a self-terminated deployment is reclaimed so its address redeploys without a manual undeploy", async () => {
    const dataDir = await createTempBaseDir("sidecar-self-term-redeploy-data-");
    const head = "run_selfterm@example.com";
    const anchorRunId = deriveWorkflowRunRepoId(head);

    const spawner = makeReadyDrivingSpawner(9750);
    const { router, transport } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    // Deploy and bring the supervisor to `running`.
    const deployPromise = router.deploy(singleStepFrame(head, "wf-selfterm"));
    await spawner.driveReadyFor(0);
    await deployPromise;
    expect(router.activeAddresses()).toEqual([head]);
    expect(isRegistered(transport, head)).toBe(true);

    // Drive a self-termination: a recycle whose respawn fails tears the
    // supervisor down to `stopped` through the recycle-failure path, firing
    // `onSelfTerminate` and driving the reclaim.
    spawner.failNextSpawn();
    await spawner.recycleRequestFor(0);

    // The reclaim drops the address from the active map and releases its
    // transport registration. No completion signal exists, so this stays a
    // deadline-free poll ("Synchronizing on State, Not Time" in
    // CONVENTIONS.md); a reclaim that never lands is caught by the lane
    // timeout.
    await waitUntil(() => !router.activeAddresses().includes(head));
    expect(router.activeAddresses()).toEqual([]);
    expect(isRegistered(transport, head)).toBe(false);

    // Redeploy with no prior undeploy succeeds: the map slot is free and the
    // transport is no longer registered (a stale registration would throw
    // "already registered"). The redeploy overwrites the durable record.
    expect(await recordExists(dataDir, anchorRunId)).toBe(true);
    const redeployPromise = router.deploy(singleStepFrame(head, "wf-selfterm"));
    await spawner.driveReadyFor(1);
    await redeployPromise;
    expect(router.activeAddresses()).toEqual([head]);
    expect(isRegistered(transport, head)).toBe(true);
    expect(spawner.spawnCount()).toBe(2);
  });

  test("cancelling a self-terminated deployment prevents boot restore", async () => {
    const dataDir = await createTempBaseDir("sidecar-self-term-cancel-data-");
    const head = "run_selfterm_cancel@example.com";
    const deploymentId = deriveWorkflowRunRepoId(head);
    const spawner = makeReadyDrivingSpawner(9780);
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    const deploying = router.deploy(singleStepFrame(head, "wf-st-cancel"));
    await spawner.driveReadyFor(0);
    await deploying;
    spawner.failNextSpawn();
    await spawner.recycleRequestFor(0);
    // No completion signal; use the deadline-free wait for the active-address transition.
    await waitUntil(() => !router.activeAddresses().includes(head));
    expect(await recordExists(dataDir, deploymentId)).toBe(true);

    const scratch = path.join(
      dataDir,
      "workflow-step-state",
      deploymentId,
      "inspection.txt",
    );
    await fs.mkdir(path.dirname(scratch), { recursive: true });
    await fs.writeFile(scratch, "retained");
    const control = router.control;
    if (control === undefined) throw new Error("router.control is undefined");
    const command = {
      type: "workflow.control",
      requestId: "cancel-request",
      action: "cancel",
      runId: "run_selfterm_cancel",
      agentAddress: head,
      reason: "Operator cancellation after failed recycle",
    } as const;
    await Promise.all([
      control(command),
      control({ ...command, requestId: "concurrent-cancel" }),
    ]);
    await control({ ...command, requestId: "cancel-retry" });
    expect(await recordExists(dataDir, deploymentId)).toBe(false);
    expect(await fs.readFile(scratch, "utf8")).toBe("retained");

    const restartedSpawner = makeReadyDrivingSpawner(9790);
    const { router: restarted } = await buildMultistepFixture({
      spawner: restartedSpawner.spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });
    await restarted.restoreWorkflowRuns();
    expect(restartedSpawner.spawnCount()).toBe(0);
    expect(restarted.activeAddresses()).toEqual([]);
  });

  test("a reclaimed self-terminated address survives a following operator undeploy", async () => {
    const dataDir = await createTempBaseDir("sidecar-self-term-undeploy-data-");
    const head = "run_selfterm_undeploy@example.com";

    const spawner = makeReadyDrivingSpawner(9760);
    const { router, transport } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    const deployPromise = router.deploy(
      singleStepFrame(head, "wf-st-undeploy"),
    );
    await spawner.driveReadyFor(0);
    await deployPromise;

    spawner.failNextSpawn();
    await spawner.recycleRequestFor(0);
    // No completion signal; deadline-free poll per CONVENTIONS.md.
    await waitUntil(() => !router.activeAddresses().includes(head));
    expect(router.activeAddresses()).toEqual([]);

    // An operator undeploy after the reclaim is a clean no-op: undeploy's
    // idempotent unregisters neither throw nor double-remove. This is the
    // observable form of the reclaim/undeploy race resolution.
    const undeploy = router.undeploy;
    if (undeploy === undefined) {
      throw new Error("router.undeploy is undefined");
    }
    await expect(
      undeploy({
        type: "agent.undeploy",
        agentAddress: head,
        reason: "operator undeploy after self-termination",
      }),
    ).resolves.toBeUndefined();
    expect(router.activeAddresses()).toEqual([]);
    expect(isRegistered(transport, head)).toBe(false);
  });

  test("the reclaim retains the deployment-address mapping so the terminal RunFailed commit can still resolve its run address", async () => {
    // Regression guard for the reclaim/terminal-commit ordering hazard: the
    // crash-loop latch commits its `RunFailed` tombstone AFTER teardown fires
    // `onSelfTerminate`, and the commit resolves the deployment-address
    // mapping for the outbound pack push. A reclaim that dropped the mapping
    // stranded the commit with "no run address registered". Wires a REAL
    // `deploymentAddressRegistry` through the hooks and asserts the mapping
    // survives the reclaim.
    const dataDir = await createTempBaseDir("sidecar-self-term-mapping-data-");
    const head = "run_selfterm_mapping@example.com";
    const runId = deriveWorkflowRunRepoId(head);

    const registry = createDeploymentAddressRegistry();

    const spawner = makeReadyDrivingSpawner(9770);
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
      registerDeployment: ({ runId: id, agentAddress }) => {
        registry.record(id, agentAddress);
      },
      unregisterDeployment: ({ runId: id }) => {
        registry.unregister(id);
      },
    });

    const deployPromise = router.deploy(singleStepFrame(head, "wf-st-mapping"));
    await spawner.driveReadyFor(0);
    await deployPromise;
    // The deploy recorded the mapping before spawn, so it resolves while the
    // supervisor is live.
    expect(registry.resolve(runId)).toBe(head);

    // Drive the supervisor to a self-termination via the recycle-failure path.
    spawner.failNextSpawn();
    await spawner.recycleRequestFor(0);
    // No completion signal; deadline-free poll per CONVENTIONS.md.
    await waitUntil(() => !router.activeAddresses().includes(head));
    expect(router.activeAddresses()).toEqual([]);

    // The reclaim dropped the redeploy gate but RETAINED the address mapping:
    // the supervisor's terminal `RunFailed` commit must still resolve it.
    expect(registry.resolve(runId)).toBe(head);
  });

  test("restore skips a record whose address does not derive its directory name", async () => {
    const dataDir = await createTempBaseDir("sidecar-restore-mismatch-data-");
    const head = "run_mismatch@example.com";
    // A record under a directory that is NOT its own derived slug -- a corrupt
    // or misplaced record that must not restore under the wrong slug.
    const wrongDir = "not-the-right-slug";
    // Otherwise-valid source-ref record so the scan admits it and restore
    // reaches the address-vs-directory mismatch, not the schema.
    const record: WorkflowRunRecord = {
      version: 1,
      agentAddress: head,
      definitionId: "wf-mismatch",
      sources: { "step-1": [makeInferenceSource("step-1")] },
      hubPublicKey: "hub-pk",
      approvedWireHash: "a".repeat(64),
      lineage: "source-ref",
      sourceRef: {
        source: { kind: "registry", registry: "test-registry" },
        closure: { schemaVersion: "1", topLevel: [], entries: [] },
      },
    };
    await writeWorkflowRunRecord(
      dataDir,
      wrongDir,
      record,
      createNoopCredentialCipher(),
    );

    const spawner = makeReadyDrivingSpawner(9800);
    const freshTransport = createInMemoryTransport();
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      transport: freshTransport,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    await router.restoreWorkflowRuns();

    expect(spawner.spawnCount()).toBe(0);
    expect(isRegistered(freshTransport, head)).toBe(false);
    // The record is kept on a skip, not deleted.
    expect(await recordExists(dataDir, wrongDir)).toBe(true);
  });

  test("restore soft-fails and keeps the record when the pinned source is no longer buildable", async () => {
    const dataDir = await createTempBaseDir(
      "sidecar-restore-unbuildable-data-",
    );
    const head = "run_unbuildable_restore@example.com";
    const anchorRunId = deriveWorkflowRunRepoId(head);

    // First process: a permissive gate lets the deploy through, persisting
    // its record and workflow.json.
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

    await routerB.restoreWorkflowRuns();

    expect(second.spawnCount()).toBe(0);
    expect(isRegistered(freshTransport, head)).toBe(false);
    // The record survives so a later boot can retry once the provider is
    // buildable again.
    expect(await recordExists(dataDir, anchorRunId)).toBe(true);
  });

  test("restore isolates an unbuildable-provider record: it keeps the record and surfaces the failure while the healthy deployment still restores", async () => {
    const dataDir = await createTempBaseDir(
      "sidecar-restore-unbuildable-isolate-data-",
    );
    const healthyHead = "run_healthy_isolate@example.com";
    const unbuildableHead = "run_unbuildable_isolate@example.com";
    const healthyId = deriveWorkflowRunRepoId(healthyHead);
    const unbuildableId = deriveWorkflowRunRepoId(unbuildableHead);

    // The unbuildable deployment pins a provider the restart's gate rejects;
    // the healthy one keeps the default `anthropic` source. One gate rejects
    // exactly one of the two records.
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
    // each record.
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

    // Restart: a fresh transport plus a gate rejecting ONLY the phantom
    // provider. Capture `logger.warn` output (threshold "warning" routes to
    // `console.warn`) to assert the failure is surfaced, not silently dropped.
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

      // Only the healthy deployment spawns (the unbuildable record faults
      // before its spawner is reached); restore is serial and per-record
      // isolated, so scan order does not change the outcome.
      const restorePromise = routerB.restoreWorkflowRuns();
      await second.driveReadyFor(0);
      await restorePromise;

      // The unbuildable record did NOT strand the healthy one: it re-spawned
      // and its head is routable on the fresh transport.
      expect(second.spawnCount()).toBe(1);
      expect(isRegistered(freshTransport, healthyHead)).toBe(true);

      // The unbuildable deployment was not stood up: no spawn, no route.
      expect(isRegistered(freshTransport, unbuildableHead)).toBe(false);

      // Its record survives so a later boot can retry once the provider is
      // buildable again.
      expect(await recordExists(dataDir, unbuildableId)).toBe(true);

      // The failure is surfaced: a warning naming the deployment and the
      // rejection reason, not a silent drop.
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
    const head = "run_readytimeout@example.com";
    const anchorRunId = deriveWorkflowRunRepoId(head);

    // A spawner whose child is never driven through `ready`. With a small
    // threaded readyTimeoutMs the supervisor times out, kills the child, and
    // rejects the spawn; the message echoing the threaded value also proves
    // the timeout reaches the supervisor.
    const spawner = makeReadyDrivingSpawner(10100);
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      readyTimeoutMs: 40,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });

    await expect(
      router.deploy(singleStepFrame(head, "wf-readytimeout")),
    ).rejects.toThrow(/did not emit ready within 40ms/);

    // The deploy soft-failed, so its restore record was cleaned up: a wedged
    // deploy leaves nothing for a later boot to re-spawn.
    expect(await recordExists(dataDir, anchorRunId)).toBe(false);
  });

  test("a single-step deploy acks the agent key, not the supervisor key", async () => {
    // The single-step head is the deployed agent identity, so the ack carries
    // the agent key, not the supervisor principal key.
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
      singleStepFrame("run_bkey@example.com", "wf-bkey"),
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
      singleStepFrame("run_srcsingle@example.com", "wf-srcsingle"),
    );
    await spawner.driveReadyFor(0);
    await deployPromise;

    // The single-step deploy registered a rotation handler, so an inbound
    // sources.update for its address routes.
    expect(
      await sourcesRouter.tryRoute({
        type: "sources.update",
        agentAddress: "run_srcsingle@example.com",
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
    // handler is registered and the rotation is unrouted.
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
    // End-to-end guard for rotation-survives-recycle: the recycle respawn's
    // STEP_INFERENCE_SOURCES carries the ROTATED table, not the frozen
    // deploy-time one.
    const sourcesRouter = createMultistepSourcesRouter();
    const spawner = makeReadyDrivingSpawner(10800);
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSourcesRouter: sourcesRouter,
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: await createTempBaseDir("sidecar-rot-survive-"),
      },
    });

    const addr = "run_rotsurvive@example.com";
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
    await spawner.awaitSpawnCount(2);
    await spawner.driveReadyFor(1);

    // The recycle respawn's sources are the ROTATED table, proving the
    // rotation survived the recycle.
    const respawnEnv = spawner.envFor(1);
    if (respawnEnv === undefined) throw new Error("respawn env missing");
    expect(
      JSON.parse(respawnEnv[STEP_INFERENCE_SOURCES_ENV_KEY] ?? "null"),
    ).toEqual({ "step-1": [rotated] });
  });

  test("a source rotation survives a full sidecar restart", async () => {
    // Restart-durability: the rotation is persisted into the run record, so
    // a restore over the same data dir respawns on the ROTATED sources.
    const dataDir = await createTempBaseDir("sidecar-rot-restart-");
    const addr = "run_rotrestart@example.com";

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

    // Second process (simulated restart): a fresh router restores the
    // deployment from the durable record over the SAME data dir.
    const second = makeReadyDrivingSpawner(11000);
    const { router: routerB } = await buildMultistepFixture({
      spawner: second.spawner,
      transport: createInMemoryTransport(),
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
    });
    const restorePromise = routerB.restoreWorkflowRuns();
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
    // Atomicity: on a failed persist the handler rolls currentSources back to
    // the deploy-time table and rethrows before deliverSources, so the
    // rotation takes no net effect (a recycle respawn after the failure is
    // unrotated).
    const dataDir = await createTempBaseDir("sidecar-rot-failatomic-");
    const addr = "run_rotfailatomic@example.com";
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

    // Replace the run record file with a directory so the rotation's persist
    // rejects (EISDIR) -- a deterministic, root-immune write fault.
    const recordFile = path.join(
      dataDir,
      "workflow-runs",
      deriveWorkflowRunRepoId(addr),
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
    // rotation carries the DEPLOY-TIME sources, proving no net effect.
    await spawner.recycleRequestFor(0);
    await spawner.awaitSpawnCount(2);
    await spawner.driveReadyFor(1);
    const respawnEnv = spawner.envFor(1);
    if (respawnEnv === undefined) throw new Error("respawn env missing");
    expect(
      JSON.parse(respawnEnv[STEP_INFERENCE_SOURCES_ENV_KEY] ?? "null"),
    ).toEqual({ "step-1": [makeInferenceSource("step-1")] });
  });

  test("a recycle interleaving the rotation persist respawns on the rotated sources", async () => {
    // Interleave guard: currentSources swaps synchronously BEFORE the persist,
    // so a recycle landing inside the persist window respawns on the ROTATED
    // sources -- consistent with what is being persisted.
    const dataDir = await createTempBaseDir("sidecar-rot-interleave-ok-");
    const addr = "run_rotinterok@example.com";
    const sourcesRouter = createMultistepSourcesRouter();
    const spawner = makeReadyDrivingSpawner(11200);

    // A persist that blocks on a test-controlled gate once armed, so a recycle
    // can be driven into the persist window. Before the gate is armed the
    // deploy's own persist passes through to the real writer.
    let gate: Promise<void> | null = null;
    let release: () => void = () => undefined;
    const persist: typeof writeWorkflowRunRecord = async (
      d,
      id,
      rec,
      cipher,
    ) => {
      if (gate !== null) await gate;
      await writeWorkflowRunRecord(d, id, rec, cipher);
    };

    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSourcesRouter: sourcesRouter,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
      writeWorkflowRunRecord: persist,
    });

    const deployPromise = router.deploy(singleStepFrame(addr, "wf-rotinterok"));
    await spawner.driveReadyFor(0);
    await deployPromise;

    // Arm the gate so the rotation's persist blocks mid-window.
    gate = new Promise<void>((resolve) => {
      release = resolve;
    });

    // Start the rotation without awaiting: the handler swaps currentSources
    // synchronously, then parks on the blocked persist.
    const rotated = makeInferenceSource("rotated");
    const rotatePromise = sourcesRouter.tryRoute({
      type: "sources.update",
      agentAddress: addr,
      sources: [rotated],
      defaultSource: "rotated",
    });

    // Drive a recycle while the persist is blocked: the respawn env must carry
    // the ROTATED sources, since the synchronous swap already ran.
    await spawner.recycleRequestFor(0);
    await spawner.awaitSpawnCount(2);
    await spawner.driveReadyFor(1);
    const respawnEnv = spawner.envFor(1);
    if (respawnEnv === undefined) throw new Error("respawn env missing");
    expect(
      JSON.parse(respawnEnv[STEP_INFERENCE_SOURCES_ENV_KEY] ?? "null"),
    ).toEqual({ "step-1": [rotated] });

    // Release the persist; the rotation completes and the durable record
    // converges on the rotated sources.
    release();
    await rotatePromise;
    const scanned = await scanWorkflowRunRecords(
      dataDir,
      createNoopCredentialCipher(),
    );
    const record = scanned.find(
      (s) => s.runId === deriveWorkflowRunRepoId(addr),
    );
    expect(record?.record.sources).toEqual({ "step-1": [rotated] });
  });

  test("a recycle interleaving a failed rotation persist respawns on new sources then rolls back", async () => {
    // The benign residual, pinned: a persist failing WHILE a recycle
    // interleaves leaves the just-respawned child transiently ahead on the
    // rotated sources (the self-healing direction); currentSources rolls back
    // so the NEXT recycle reverts the child to durable truth.
    const dataDir = await createTempBaseDir("sidecar-rot-interleave-fail-");
    const addr = "run_rotinterfail@example.com";
    const sourcesRouter = createMultistepSourcesRouter();
    const spawner = makeReadyDrivingSpawner(11300);

    let gate: Promise<void> | null = null;
    let release: () => void = () => undefined;
    const persist: typeof writeWorkflowRunRecord = async (
      d,
      id,
      rec,
      cipher,
    ) => {
      if (gate !== null) {
        await gate;
        throw new Error("rotation persist boom");
      }
      await writeWorkflowRunRecord(d, id, rec, cipher);
    };

    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSourcesRouter: sourcesRouter,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
      writeWorkflowRunRecord: persist,
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
    // carries the ROTATED sources since the swap already ran.
    await spawner.recycleRequestFor(0);
    await spawner.awaitSpawnCount(2);
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
    // healing the transient.
    await spawner.recycleRequestFor(1);
    await spawner.awaitSpawnCount(3);
    await spawner.driveReadyFor(2);
    const secondRespawnEnv = spawner.envFor(2);
    if (secondRespawnEnv === undefined) {
      throw new Error("second respawn env missing");
    }
    expect(
      JSON.parse(secondRespawnEnv[STEP_INFERENCE_SOURCES_ENV_KEY] ?? "null"),
    ).toEqual({ "step-1": [makeInferenceSource("step-1")] });
  });

  test("two addresses that project to the same workflow-run repo id are rejected at the second deploy", async () => {
    // deriveWorkflowRunRepoId substitutes every disallowed character with `-`,
    // so two distinct addresses can collapse to the same slug -- which IS the
    // workflow-run repoId. claimSlug rejects the collision at the router edge,
    // before any spawn or repo write.
    const spawner = makeReadyDrivingSpawner(10400);
    const { router } = await buildMultistepFixture({
      spawner: spawner.spawner,
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: await createTempBaseDir("sidecar-collision-data-"),
      },
    });

    // `run_col.a@example.com` and `run_col-a@example.com` both project to
    // `run_col-a-example-com` under the slug derivation.
    const deployPromise = router.deploy(
      singleStepFrame("run_col.a@example.com", "wf-collide"),
    );
    await spawner.driveReadyFor(0);
    await deployPromise;

    await expect(
      router.deploy(singleStepFrame("run_col-a@example.com", "wf-collide")),
    ).rejects.toThrow(/workflow-run repo id collision/);
    expect(spawner.spawnCount()).toBe(1);
  });

  test("reEmitParkedCorrelations reaches the live supervisor and sends a parked-correlations.request downstream", async () => {
    // Trigger B: the hub-reconnect fan-out. The router's address-dispatch
    // wrapper must reach the live supervisor's no-arg `reEmitParkedCorrelations`,
    // which sends a `parked-correlations.request` control frame down to the
    // child. The mock child never answers, so the request frame landing on the
    // downstream stream is the observable evidence.
    const childIpcKeyPair = await generateKeyPair();
    const {
      supervisorToChild,
      childToSupervisor,
      eventChildToSupervisor,
      exited,
      resolveExit,
    } = createChildStreams();
    let observedEnv: Record<string, string> | undefined;
    // Scoped to this fixture: `first()` must resolve with THIS spawn, not
    // the file's earliest.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      spawnObserver.record(env);
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
    observedEnv = await spawnObserver.first();
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

    // `activeSupervisors` is keyed by the frame's run address; the re-emit
    // routes through that same key.
    expect(router.activeAddresses()).toEqual([frame.agentAddress]);

    // Read `envelope.payload.type` without verifying the signature (the
    // supervisor's IPC public key is not exposed to the test). The supervisor
    // emits its own request on spawn, so the trigger is an increase in the
    // request count, not its first appearance.
    function parkedRequestCount(): number {
      return supervisorToChild
        .flushed()
        .filter((line) => downstreamPayloadType(line) === PARKED_REQUEST_TYPE)
        .length;
    }

    // Wait for the spawn-time request so the baseline includes it; a fixed
    // wait could bank zero on a slow machine and pass on the spawn frame
    // alone.
    while (parkedRequestCount() < 1) {
      await supervisorToChild.nextWrite();
    }
    const baseline = parkedRequestCount();

    router.reEmitParkedCorrelations(frame.agentAddress);

    // The request frame reaching the downstream stream is the event; the
    // bounded poll this replaces made a wiring break indistinguishable from
    // slowness.
    while (parkedRequestCount() <= baseline) {
      await supervisorToChild.nextWrite();
    }
    expect(parkedRequestCount()).toBeGreaterThan(baseline);
  });

  test("reEmitParkedCorrelations for an address with no active supervisor is a no-op", async () => {
    // Edge boundary: the hub reports an address routable but no live
    // supervisor owns it (torn down or not yet respawned). The router must
    // skip it -- neither throw nor drive a downstream frame.
    const childIpcKeyPair = await generateKeyPair();
    const {
      supervisorToChild,
      childToSupervisor,
      eventChildToSupervisor,
      exited,
      resolveExit,
    } = createChildStreams();
    let observedEnv: Record<string, string> | undefined;
    // Scoped to this fixture: `first()` must resolve with THIS spawn, not
    // the file's earliest.
    const spawnObserver = createSpawnObserver();
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      spawnObserver.record(env);
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
    observedEnv = await spawnObserver.first();
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

    const missAddress = "run_nonexistent@wf.example";
    expect(router.activeAddresses()).not.toContain(missAddress);

    // The supervisor emits its own request on spawn; the miss-address re-emit
    // must add nothing. Count downstream requests before and after the no-op.
    function parkedRequestCount(): number {
      return supervisorToChild
        .flushed()
        .filter((line) => downstreamPayloadType(line) === PARKED_REQUEST_TYPE)
        .length;
    }
    // Wait for the spawn-time request so the baseline includes it; a fixed
    // wait could bank zero on a slow machine and pass on the spawn frame
    // alone.
    while (parkedRequestCount() < 1) {
      await supervisorToChild.nextWrite();
    }
    const baseline = parkedRequestCount();

    // The no-op skip must not throw for an address with no live supervisor.
    expect(() => router.reEmitParkedCorrelations(missAddress)).not.toThrow();

    // No pause: the skip path starts no work for an interval to catch, and a
    // pause would only give an unrelated frame time to arrive.
    expect(parkedRequestCount()).toBe(baseline);
  });
});

describe("assembleRunCredentialsSnapshot", () => {
  // Mirror the production `<base>/<kind>/<id>` layout so a grants file written
  // at `<base>/workflow-run/<anchorRunId>/runs/<runId>/grants.json` lands
  // where the sink's working-tree read looks.
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

  const anchorRunId = "dep-run-grants";
  const runId = "run-xyz";
  const stepOrder = ["step-1", "step-2"];
  const deriveStepAddress = ({
    runId: dep,
    stepId,
  }: {
    runId: string;
    stepId: string;
  }) => `${dep}-${stepId}@example.com`;

  async function writeRunGrantsFile(
    tempBase: string,
    contents: string,
  ): Promise<void> {
    const dir = path.join(tempBase, "workflow-run", anchorRunId, "runs", runId);
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
      `${anchorRunId}-${stepId}`,
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
      anchorRunId,
      runId,
      stepOrder,
      deriveStepAddress,
    });

    // Every step carries the single flat per-run grant set, keyed on the
    // deploy-time per-step address, with a shared content hash.
    expect(snapshot.steps).toHaveLength(2);
    for (const step of snapshot.steps) {
      expect(step.grants).toEqual(runGrants);
      expect(step.address).toBe(`${anchorRunId}-${step.stepId}@example.com`);
    }
    expect(snapshot.steps[0]?.stepId).toBe("step-1");
    expect(snapshot.steps[1]?.stepId).toBe("step-2");
    expect(snapshot.steps[0]?.contentHash).toBe(
      snapshot.steps[1]?.contentHash ?? "",
    );
  });

  test("a run without a per-run grants file fails closed", async () => {
    const tempBase = await createTempBaseDir("sidecar-run-grants-");
    const repoStore = createReadStubRepoStore(tempBase);

    // No per-run file; a deploy-time file is present so a silent fallback
    // would surface deploy-time grants. The run must fail closed instead.
    await writeDeployTimeStepGrants(
      tempBase,
      "step-1",
      JSON.stringify({ grants: [{ id: "deploy-grant-1" }] }),
    );

    await expect(
      assembleRunCredentialsSnapshot({
        repoStore,
        anchorRunId,
        runId,
        stepOrder,
        deriveStepAddress,
      }),
    ).rejects.toThrow(/has no grants file/);
  });

  test("a run with a malformed per-run grants file throws", async () => {
    const tempBase = await createTempBaseDir("sidecar-run-grants-");
    const repoStore = createReadStubRepoStore(tempBase);

    // The file exists but is not valid JSON; a deploy-time file is present so
    // a swallowed error would silently fall back.
    await writeRunGrantsFile(tempBase, "{ not valid json");
    await writeDeployTimeStepGrants(
      tempBase,
      "step-1",
      JSON.stringify({ grants: [{ id: "deploy-grant" }] }),
    );

    await expect(
      assembleRunCredentialsSnapshot({
        repoStore,
        anchorRunId,
        runId,
        stepOrder,
        deriveStepAddress,
      }),
    ).rejects.toThrow(/is not valid JSON/);

    // Valid JSON that violates the `{ grants: [] }` envelope also throws
    // rather than falling back -- the file's presence implies a grants frame
    // was delivered.
    await writeRunGrantsFile(tempBase, JSON.stringify({ grants: "not-array" }));
    await expect(
      assembleRunCredentialsSnapshot({
        repoStore,
        anchorRunId,
        runId,
        stepOrder,
        deriveStepAddress,
      }),
    ).rejects.toThrow(/failed validation/);
  });

  test("a re-dispatched run re-reads the durable per-run grants, not the deploy-time fallback", async () => {
    // On a child respawn the supervisor re-dequeues an already-consumed run,
    // so the grants barrier fires AGAIN for the same runId. The per-run grants
    // file is durable (`readRunGrants` never deletes it), so this second
    // resolution must still read `runs/<runId>/grants.json` and win over the
    // deploy-time fallback.
    const tempBase = await createTempBaseDir("sidecar-run-grants-");
    const repoStore = createReadStubRepoStore(tempBase);

    const runGrants = [
      { id: "run-grant", resource: "tool:send-mail", effect: "allow" },
    ];
    await writeRunGrantsFile(tempBase, JSON.stringify({ grants: runGrants }));
    // A deploy-time file is present so a spurious fallback on the second read
    // would surface the wrong grants rather than an empty set.
    await writeDeployTimeStepGrants(
      tempBase,
      "step-1",
      JSON.stringify({ grants: [{ id: "deploy-grant" }] }),
    );

    const first = await assembleRunCredentialsSnapshot({
      repoStore,
      anchorRunId,
      runId,
      stepOrder,
      deriveStepAddress,
    });

    // The re-dispatch: resolve the same run again with no rewrite in between.
    const second = await assembleRunCredentialsSnapshot({
      repoStore,
      anchorRunId,
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
    // The second resolution is byte-identical to the first: same grant set,
    // same content hash across every step.
    expect(second.steps.map((s) => s.contentHash)).toEqual(
      first.steps.map((s) => s.contentHash),
    );
  });
});
