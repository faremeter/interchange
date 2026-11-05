// Pins H-A1: a deploy-router rejection must not leave the
// `DeploymentAddressRegistry` populated. The multi-step branch defers
// `registerDeployment` until every step that can throw (asset
// materialization, `supervisor.spawn`) has succeeded. The link's
// `handleAgentDeploy` catches a rejection and sends `agent.deploy.error`
// without invoking `deployRouter.undeploy(frame)`, so a premature
// registration would retain a `(anchorRunId -> agentAddress)` mapping
// for a deployment that does not exist. The multi-step test drives that
// failure through a subprocess spawner that throws synchronously.

import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join as pathJoin } from "node:path";

import { describe, test, expect } from "bun:test";
import { createInMemoryTransport } from "@intx/mail-memory";
import {
  createEd25519Crypto,
  createNoopCredentialCipher,
  generateKeyPair,
} from "@intx/crypto";
import type { RepoId, RepoStore } from "@intx/hub-sessions";
import { hexEncode } from "@intx/types";
import {
  WorkflowProjectionDefinition,
  type AgentDeployFrame,
} from "@intx/types/sidecar";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type { WorkflowDefinition } from "@intx/workflow";
import { projectLiveToInert } from "@intx/workflow/projection";
import {
  createControlChannelSender,
  type SubprocessSpawner,
} from "@intx/workflow-host";
import {
  createMemoryFrameStream,
  createMemoryNdjsonStream,
  sendDefinition,
} from "@intx/workflow-host/testing";

import { DEFAULT_SIDECAR_CAPACITY } from "@intx/types/sidecar";
import {
  createDeploymentAddressRegistry,
  createMultistepDrainRouter,
  createMultistepMailRouter,
  createMultistepSignalRouter,
} from "./workflow-run-pack-client";
import { createSidecarDeployRouter } from "./workflow-host-wiring";

function deriveWorkflowRunRepoId(agentAddress: string): string {
  return agentAddress.replaceAll(/[^a-zA-Z0-9_-]/g, "-");
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
    inertFlatNamespaceStepIds: (args: {
      definition: { readonly stepOrder: readonly string[] };
    }) => args.definition.stepOrder,
    sourceAssetGitDir: (gitDirRoot: string, assetId: string) =>
      pathJoin(gitDirRoot, assetId),
    readRegistries: () => new Map(),
    resolveHostPlatform: () => ({ os: "linux", cpu: "arm64" }),
    resolveInboundMailPolicy: () => resolvedInboundMailPolicy,
    materializeWorkflowAssets: () => Promise.resolve(undefined),
    maxInlineAssetPayloadBytes: 32 * 1024 * 1024,
    closureCacheMaxBytes: 1_000_000,
    registryMaxTarballBytes: 1_000_000,
    multistepBinaryPath: "/fake/bin/workflow-child",
    multistepSubprocessSpawner: (): never => {
      throw new Error("workflow child spawner was not provided");
    },
    createWorkflowCache: () => ({}),
    applyFrozenWorkflowClosure: (): never => {
      throw new Error("frozen closure apply was not provided");
    },
  };
}

function stubKeyStore(): Parameters<
  typeof createSidecarDeployRouter
>[0]["keyStore"] {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub
  return {
    async loadOrGenerateKey() {
      // The single-step multi-step branch registers the agent's signing
      // key on the host transport before `spawn()` (OUTBOUND half of
      // mailbox ownership). Return a real keypair so that registration
      // succeeds and the SPAWNER failure remains the failure this test
      // exercises.
      return { keyPair: await generateKeyPair(), isNew: false };
    },
  } as unknown as Parameters<typeof createSidecarDeployRouter>[0]["keyStore"];
}

// Source-ref is the only deploy lineage: the router derives the runnable
// definition by materializing the pin's closure through this dependency. These
// spawn-failure tests need the closure to SUCCEED so the deploy reaches the
// spawn it exercises, so the stub returns a valid single-step live definition
// (its one step carries an agent so it survives `projectLiveToInert`), keyed to
// the frame's single source id `s1`.
function stubApplyFrozenWorkflowClosure(): NonNullable<
  Parameters<typeof createSidecarDeployRouter>[0]["applyFrozenWorkflowClosure"]
> {
  return (applyArgs) =>
    Promise.resolve({
      packageDir: pathJoin(applyArgs.instanceDir, "package"),
    });
}

// The single-step `s1` definition a child reports, and its approved hash.
const SINGLE_STEP_PROJECTION = WorkflowProjectionDefinition.assert(
  projectLiveToInert(
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- a hand-built live definition cannot satisfy the full WorkflowDefinition nominal type; it stands in for a real closure evaluation
    {
      id: "wf-single",
      triggers: [{ type: "manual" }],
      stepOrder: ["s1"],
      steps: {
        s1: {
          kind: "step",
          id: "s1",
          agent: {
            id: "agent-s1",
            systemPrompt: "sys",
            capabilities: [],
            toolFactories: [],
            inference: { sources: [] },
          },
        },
      },
    } as unknown as WorkflowDefinition,
  ),
);
const SINGLE_STEP_HASH = await computeWireDefinitionHash(
  SINGLE_STEP_PROJECTION,
);

// A child that reports its definition, then exits before it readies.
function reportThenExitSpawner(
  projection: WorkflowProjectionDefinition,
): SubprocessSpawner {
  return ({ env }) => {
    const controlIn = createMemoryNdjsonStream();
    const controlOut = createMemoryNdjsonStream();
    const events = createMemoryFrameStream();
    let resolveExit: (code: number) => void = () => undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    const exit = (code: number) => {
      controlIn.close();
      events.close();
      resolveExit(code);
    };
    const report = async () => {
      const keyPair = await generateKeyPair();
      await sendDefinition(
        createControlChannelSender({
          privateKeySeed: keyPair.privateKey,
          channelId: env.IPC_CHANNEL_ID ?? "",
          writer: {
            write(line: string) {
              controlIn.inject(line);
            },
          },
        }),
        {
          childPid: 4242,
          childPublicKey: hexEncode(keyPair.publicKey),
          projection,
        },
      );
    };
    // A failed report still ends the child, so the deploy fails fast and the
    // test's ready-step assertion shows the report never landed.
    void report().then(
      () => exit(1),
      () => exit(1),
    );
    return {
      pid: 4242,
      controlWriter: controlOut.writer,
      controlReader: controlIn.reader,
      eventReader: events.reader,
      kill: () => exit(0),
      exited,
    };
  };
}

function makeRouterDeps() {
  const registry = createDeploymentAddressRegistry();
  const mailRouter = createMultistepMailRouter();
  const signalRouter = createMultistepSignalRouter();
  const drainRouter = createMultistepDrainRouter();
  const transport = createInMemoryTransport();
  return { registry, mailRouter, signalRouter, drainRouter, transport };
}

describe("deploy-failure registry leak", () => {
  test("multi-step deploy: spawn-time failure leaves registry clean", async () => {
    const { registry, mailRouter, signalRouter, drainRouter, transport } =
      makeRouterDeps();
    const sessions = { deleteAgentDir: async () => undefined };
    const keyStore = stubKeyStore();

    const failingSpawner: SubprocessSpawner = () => {
      throw new Error("spawner forced failure");
    };

    const tmpDir = mkdtempSync(pathJoin(tmpdir(), "h-a1-deploy-failure-"));

    // A minimal RepoStore that honors `getRepoDir` + `writeTree`, so the
    // SPAWNER failure is the one this test exercises.
    const repoStoreStub: Partial<RepoStore> = {
      getRepoDir(repoId: RepoId): string {
        return pathJoin(tmpDir, repoId.kind, repoId.id);
      },
      writeTree(_p, repoId, _ref, content) {
        const dir = pathJoin(tmpDir, repoId.kind, repoId.id);
        for (const [relPath, contents] of Object.entries(content.files)) {
          const full = pathJoin(dir, relPath);
          mkdirSync(dirname(full), { recursive: true });
          writeFileSync(full, contents);
        }
        return Promise.resolve({
          commitSha: "stub-sha",
          newlyTerminalRuns: [],
        });
      },
    };

    const router = createSidecarDeployRouter({
      ...DEFAULT_SIDECAR_CAPACITY,
      ...deployHostBindings(),
      sessions,
      keyStore,
      transport,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub: only getRepoDir + writeTree are exercised before the spawn-time failure
      repoStore: repoStoreStub as RepoStore,
      signingKeySeed: new Uint8Array(32),
      credentialCipher: createNoopCredentialCipher(),
      createAgentCrypto: createEd25519Crypto,
      assertSourceBuildable: () => undefined,
      registerDeployment: ({ runId, agentAddress, generation }) => {
        registry.record(runId, { agentAddress, generation });
      },
      removeRunRepository: async () => undefined,
      unregisterDeployment: ({ runId }) => {
        registry.unregister(runId);
      },
      reportDeploymentRefTips: async () => ({}),
      multistepMailRouter: mailRouter,
      multistepSignalRouter: signalRouter,
      multistepDrainRouter: drainRouter,
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: tmpDir,
        SIDECAR_SIGNING_PUBLIC_KEY: "00".repeat(32),
        SIDECAR_SIGNING_PRIVATE_KEY: "00".repeat(32),
        PATH: "/usr/bin",
      },
      multistepSubprocessSpawner: failingSpawner,
      applyFrozenWorkflowClosure: stubApplyFrozenWorkflowClosure(),
    });

    const frame: AgentDeployFrame = {
      type: "agent.deploy",
      requestId: "deploy-test",
      // Single-step projection: the address carries the canonical
      // `run_<id>@<domain>` shape.
      agentAddress: "run_mstep@x.example",
      generation: 1,
      config: {
        sessionId: "s",
      },
      workflow: {
        // Production always stamps the hub-approved wire hash; a placeholder
        // satisfies the deploy path's fail-loud guard so this test reaches the
        // spawn-failure behavior it exercises.
        approvedWireHash: "a".repeat(64),
        // Source-ref is the only deploy lineage: the frame carries no inline
        // definition, only the pin the sidecar re-materializes for the child.
        sourceRef: {
          source: { kind: "registry", registry: "test-registry" },
          closure: { schemaVersion: "1", topLevel: [], entries: [] },
        },
        sources: {
          s1: [
            {
              id: "primary",
              provider: "anthropic",
              baseURL: "https://api.anthropic.com",
              credentialId: "sk-x",
              model: "claude-3-5",
            },
          ],
        },
      },
    };

    let threw = false;
    try {
      await router.deploy(frame);
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);

    const slug = "run_mstep-x-example";
    expect(registry.resolve(slug)).toBeNull();
  });

  test("single-step spawn failure preserves the head directory", async () => {
    const { registry, mailRouter, signalRouter, drainRouter, transport } =
      makeRouterDeps();

    const removedRepos: string[] = [];

    // A sessions stub whose `deleteAgentDir` is spied: the unwind must NOT call
    // it, because the head directory holds the durable identity keypair.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub: deleteAgentDir is spied to prove it is NOT called
    const sessions = {
      deleteAgentDir: async (address: string) => {
        removedRepos.push(address);
      },
    } as unknown as Parameters<typeof createSidecarDeployRouter>[0]["sessions"];

    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub
    const keyStore = {
      async loadOrGenerateKey() {
        return { keyPair: await generateKeyPair(), isNew: false };
      },
    } as unknown as Parameters<typeof createSidecarDeployRouter>[0]["keyStore"];

    const tmpDir = mkdtempSync(pathJoin(tmpdir(), "single-step-unwind-"));

    const repoStoreStub: Partial<RepoStore> = {
      getRepoDir(repoId: RepoId): string {
        return pathJoin(tmpDir, repoId.kind, repoId.id);
      },
      writeTree(_p, repoId, _ref, content) {
        const dir = pathJoin(tmpDir, repoId.kind, repoId.id);
        for (const [relPath, contents] of Object.entries(content.files)) {
          const full = pathJoin(dir, relPath);
          mkdirSync(dirname(full), { recursive: true });
          writeFileSync(full, contents);
        }
        return Promise.resolve({
          commitSha: "stub-sha",
          newlyTerminalRuns: [],
        });
      },
    };

    const router = createSidecarDeployRouter({
      ...DEFAULT_SIDECAR_CAPACITY,
      ...deployHostBindings(),
      sessions,
      keyStore,
      transport,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub: only getRepoDir + writeTree are exercised before the spawn-time failure
      repoStore: repoStoreStub as RepoStore,
      signingKeySeed: new Uint8Array(32),
      credentialCipher: createNoopCredentialCipher(),
      createAgentCrypto: createEd25519Crypto,
      assertSourceBuildable: () => undefined,
      registerDeployment: ({ runId, agentAddress, generation }) => {
        registry.record(runId, { agentAddress, generation });
      },
      removeRunRepository: async () => undefined,
      unregisterDeployment: ({ runId }) => {
        registry.unregister(runId);
      },
      reportDeploymentRefTips: async () => ({}),
      multistepMailRouter: mailRouter,
      multistepSignalRouter: signalRouter,
      multistepDrainRouter: drainRouter,
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: tmpDir,
        SIDECAR_SIGNING_PUBLIC_KEY: "00".repeat(32),
        SIDECAR_SIGNING_PRIVATE_KEY: "00".repeat(32),
        PATH: "/usr/bin",
      },
      multistepSubprocessSpawner: reportThenExitSpawner(SINGLE_STEP_PROJECTION),
      applyFrozenWorkflowClosure: stubApplyFrozenWorkflowClosure(),
    });

    const frame: AgentDeployFrame = {
      type: "agent.deploy",
      requestId: "deploy-test",
      agentAddress: "run_single@x.example",
      generation: 1,
      config: {
        sessionId: "s",
      },
      workflow: {
        approvedWireHash: SINGLE_STEP_HASH,
        // Source-ref is the only deploy lineage: the frame carries no inline
        // definition, only the pin the sidecar re-materializes for the child,
        // which reports the single-step `s1` definition.
        sourceRef: {
          source: { kind: "registry", registry: "test-registry" },
          closure: { schemaVersion: "1", topLevel: [], entries: [] },
        },
        sources: {
          s1: [
            {
              id: "primary",
              provider: "anthropic",
              baseURL: "https://api.anthropic.com",
              credentialId: "sk-x",
              model: "claude-3-5",
            },
          ],
        },
      },
    };

    await expect(router.deploy(frame)).rejects.toThrow(
      /control channel ended before child emitted ready/,
    );

    // A failed spawn keeps the single-step head's directory.
    expect(removedRepos).toEqual([]);
    // Registry untouched.
    expect(registry.resolve("run_single-x-example")).toBeNull();
  });

  test("an undeploy whose teardown fails still gives up the deployment's address", async () => {
    const { registry, mailRouter, signalRouter, drainRouter, transport } =
      makeRouterDeps();
    // A data directory that is a file: removing the run record under it throws.
    const dataDir = pathJoin(
      mkdtempSync(pathJoin(tmpdir(), "undeploy-teardown-failure-")),
      "data",
    );
    writeFileSync(dataDir, "");
    const router = createSidecarDeployRouter({
      ...DEFAULT_SIDECAR_CAPACITY,
      ...deployHostBindings(),
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- undeploy only deletes the agent directory
      sessions: {
        deleteAgentDir: async () => undefined,
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["sessions"],
      keyStore: stubKeyStore(),
      transport,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- undeploy never touches the repo store
      repoStore: {} as Parameters<
        typeof createSidecarDeployRouter
      >[0]["repoStore"],
      signingKeySeed: new Uint8Array(32),
      credentialCipher: createNoopCredentialCipher(),
      createAgentCrypto: createEd25519Crypto,
      assertSourceBuildable: () => undefined,
      registerDeployment: ({ runId, agentAddress, generation }) => {
        registry.record(runId, { agentAddress, generation });
      },
      removeRunRepository: async () => undefined,
      unregisterDeployment: ({ runId }) => {
        registry.unregister(runId);
      },
      reportDeploymentRefTips: async () => ({}),
      multistepMailRouter: mailRouter,
      multistepSignalRouter: signalRouter,
      multistepDrainRouter: drainRouter,
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: dataDir,
      },
      multistepSubprocessSpawner: () => {
        throw new Error("undeploy never spawns");
      },
    });
    const agentAddress = "run_teardown@x.example";
    const runId = deriveWorkflowRunRepoId(agentAddress);
    registry.record(runId, { agentAddress: agentAddress, generation: 1 });
    const undeploy = router.undeploy;
    if (undeploy === undefined) throw new Error("router.undeploy is undefined");

    await expect(
      undeploy({
        type: "agent.undeploy",
        requestId: "undeploy-test",
        agentAddress,
        generation: 1,
        reason: "Generation 2 superseded it",
      }),
    ).rejects.toThrow();

    expect(registry.resolve(runId)).toBeNull();
  });
});
