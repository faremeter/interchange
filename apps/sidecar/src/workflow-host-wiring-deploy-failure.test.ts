// Pins H-A1: a deploy-router rejection must not leave the
// `DeploymentAddressRegistry` populated. The multi-step branch defers
// `registerDeployment` until every step that can throw (asset
// materialization, `supervisor.spawn`) has succeeded. The link's
// `handleAgentDeploy` catches a rejection and sends `agent.error`
// without invoking `deployRouter.undeploy(frame)`, so a premature
// registration would retain a `(anchorRunId -> agentAddress)` mapping
// for a deployment that does not exist. The multi-step test drives that
// failure through a subprocess spawner that throws synchronously.
//
// The first test pins the router's frame-shape guard: a frame carrying
// neither `provisionStep` nor a workflow definition is rejected before
// any deploy work, so a malformed frame cannot leak registry state.

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
import type { AgentDeployFrame } from "@intx/types/sidecar";
import type { WorkflowDefinition } from "@intx/workflow";
import type { SubprocessSpawner } from "@intx/workflow-host";

import {
  createDeploymentAddressRegistry,
  createMultistepDrainRouter,
  createMultistepMailRouter,
  createMultistepSignalRouter,
} from "./workflow-run-pack-client";
import { createSidecarDeployRouter } from "./workflow-host-wiring";

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
    recordHubKey() {
      /* no-op */
    },
    verifyDeployCommit() {
      return true;
    },
    forgetAgent() {
      /* no-op */
    },
  } as unknown as Parameters<typeof createSidecarDeployRouter>[0]["keyStore"];
}

function stubFailingSessions(): Parameters<
  typeof createSidecarDeployRouter
>[0]["sessions"] {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub
  return {
    async provisionAgent() {
      throw new Error("provisionAgent forced failure");
    },
  } as unknown as Parameters<typeof createSidecarDeployRouter>[0]["sessions"];
}

// Source-ref is the only deploy lineage: the router derives the runnable
// definition by materializing the pin's closure through this dependency. These
// spawn-failure tests need the closure to SUCCEED so the deploy reaches the
// spawn it exercises, so the stub returns a valid single-step live definition
// (its one step carries an agent so it survives `projectLiveToInert`), keyed to
// the frame's single source id `s1`.
function stubApplyFrozenWorkflowClosure(
  definitionId: string,
): NonNullable<
  Parameters<typeof createSidecarDeployRouter>[0]["applyFrozenWorkflowClosure"]
> {
  return (applyArgs) =>
    Promise.resolve({
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- a hand-built live definition cannot satisfy the full WorkflowDefinition nominal type; it stands in for a real closure evaluation
      definition: {
        id: definitionId,
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
      packageDir: pathJoin(applyArgs.instanceDir, "package"),
      deployDir: pathJoin(applyArgs.instanceDir, "deploy"),
    });
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
  test("a frame carrying neither provisionStep nor a workflow is rejected before any deploy work", async () => {
    const { registry, mailRouter, signalRouter, drainRouter, transport } =
      makeRouterDeps();

    let spawnerInvoked = false;
    const router = createSidecarDeployRouter({
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- an unsupported frame throws before sessions is touched
      sessions: {} as Parameters<
        typeof createSidecarDeployRouter
      >[0]["sessions"],
      keyStore: stubKeyStore(),
      transport,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- an unsupported frame throws before any repoStore usage
      repoStore: {} as Parameters<
        typeof createSidecarDeployRouter
      >[0]["repoStore"],
      signingKeySeed: new Uint8Array(32),
      credentialCipher: createNoopCredentialCipher(),
      createAgentCrypto: createEd25519Crypto,
      assertSourceBuildable: () => undefined,
      registerDeployment: ({ runId, agentAddress }) => {
        registry.record(runId, agentAddress);
      },
      unregisterDeployment: ({ runId }) => {
        registry.unregister(runId);
      },
      multistepMailRouter: mailRouter,
      multistepSignalRouter: signalRouter,
      multistepDrainRouter: drainRouter,
      multistepSubprocessSpawner: () => {
        spawnerInvoked = true;
        throw new Error("spawner must not run for an unsupported frame");
      },
    });

    // Neither `provisionStep` nor a workflow definition: the router has
    // no path to stage this frame through the substrate, so it rejects on
    // shape before reaching any deploy work.
    const frame: AgentDeployFrame = {
      type: "agent.deploy",
      agentAddress: "agent-unsupported@x.example",
      agentId: "agent-unsupported",
      hubPublicKey: "00".repeat(32),
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- config is irrelevant; the frame is rejected on shape before config is read
      config: {} as unknown as AgentDeployFrame["config"],
    };

    await expect(router.deploy(frame)).rejects.toThrow(
      /unsupported deploy frame/,
    );
    // The throw fires before any deploy work: no spawn, and the registry
    // is never touched.
    expect(spawnerInvoked).toBe(false);
    const slug = "agent-unsupported-x-example";
    expect(registry.resolve(slug)).toBeNull();
  });

  test("multi-step deploy: spawn-time failure leaves registry clean", async () => {
    const { registry, mailRouter, signalRouter, drainRouter, transport } =
      makeRouterDeps();
    const sessions = stubFailingSessions();
    const keyStore = stubKeyStore();

    const failingSpawner: SubprocessSpawner = () => {
      throw new Error("spawner forced failure");
    };

    const tmpDir = mkdtempSync(pathJoin(tmpdir(), "h-a1-deploy-failure-"));

    // The grants bridge writes `state/grants.json` to each step's
    // agent-state repo before `spawn()`; supply a minimal RepoStore that
    // honors `getRepoDir` + `writeTree` so the bridge succeeds and the
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
      sessions,
      keyStore,
      transport,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub: only getRepoDir + writeTree are exercised before the spawn-time failure
      repoStore: repoStoreStub as RepoStore,
      signingKeySeed: new Uint8Array(32),
      credentialCipher: createNoopCredentialCipher(),
      createAgentCrypto: createEd25519Crypto,
      assertSourceBuildable: () => undefined,
      registerDeployment: ({ runId, agentAddress }) => {
        registry.record(runId, agentAddress);
      },
      unregisterDeployment: ({ runId }) => {
        registry.unregister(runId);
      },
      multistepMailRouter: mailRouter,
      multistepSignalRouter: signalRouter,
      multistepDrainRouter: drainRouter,
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: tmpDir,
        SIDECAR_SIGNING_PUBLIC_KEY: "00".repeat(32),
        SIDECAR_SIGNING_PRIVATE_KEY: "00".repeat(32),
        HUB_WS_URL: "ws://test",
        SIDECAR_ID: "sc",
        SIDECAR_TOKEN: "tok",
        PATH: "/usr/bin",
        // Source-ref materialization reads both byte caps from the substrate env.
        SIDECAR_CACHE_MAX_BYTES: "1000000",
        SIDECAR_REGISTRY_MAX_TARBALL_BYTES: "1000000",
      },
      multistepSubprocessSpawner: failingSpawner,
      applyFrozenWorkflowClosure: stubApplyFrozenWorkflowClosure("wf-1"),
    });

    const frame: AgentDeployFrame = {
      type: "agent.deploy",
      // Single-step projection: the deploy router parses the frame
      // address into the legacy agent-state repo id, so it must carry the
      // canonical `run_<id>@<domain>` shape.
      agentAddress: "run_mstep@x.example",
      agentId: "mstep",
      hubPublicKey: "00".repeat(32),
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- multi-step branch does not consult config before failing
      config: {
        agentAddress: "run_mstep@x.example",
        agentId: "mstep",
        sessionId: "s",
        sources: [],
        defaultSource: "primary",
        grants: [],
      } as unknown as AgentDeployFrame["config"],
      workflow: {
        // Production always stamps the hub-approved wire hash; a placeholder
        // satisfies the deploy path's fail-loud guard so this test reaches the
        // spawn-failure behavior it exercises.
        approvedWireHash: "a".repeat(64),
        // Source-ref is the only deploy lineage: the frame carries no inline
        // definition, only the pin the sidecar re-materializes (the injected
        // closure stub above evaluates it to a single-step `s1` definition).
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

  test("single-step spawn failure reverses the recorded hub key but preserves the head repo", async () => {
    const { registry, mailRouter, signalRouter, drainRouter, transport } =
      makeRouterDeps();

    const initedRepos: string[] = [];
    const removedRepos: string[] = [];
    const recordedHubKeys: string[] = [];
    const forgottenAgents: string[] = [];

    // A sessions stub whose `initRepo` succeeds (so the single-step head
    // proceeds to record the hub key and then fail at spawn) and whose
    // `deleteAgentDir` is spied: the unwind must NOT call it, because the
    // head repo directory also holds the durable identity keypair.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub: only initRepo runs on the happy path; deleteAgentDir is spied to prove it is NOT called
    const sessions = {
      initRepo: async (address: string) => {
        initedRepos.push(address);
      },
      deleteAgentDir: async (address: string) => {
        removedRepos.push(address);
      },
    } as unknown as Parameters<typeof createSidecarDeployRouter>[0]["sessions"];

    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub
    const keyStore = {
      async loadOrGenerateKey() {
        return { keyPair: await generateKeyPair(), isNew: false };
      },
      recordHubKey(address: string) {
        recordedHubKeys.push(address);
      },
      verifyDeployCommit() {
        return true;
      },
      forgetAgent(address: string) {
        forgottenAgents.push(address);
      },
    } as unknown as Parameters<typeof createSidecarDeployRouter>[0]["keyStore"];

    const failingSpawner: SubprocessSpawner = () => {
      throw new Error("spawner forced failure");
    };

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
      sessions,
      keyStore,
      transport,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub: only getRepoDir + writeTree are exercised before the spawn-time failure
      repoStore: repoStoreStub as RepoStore,
      signingKeySeed: new Uint8Array(32),
      credentialCipher: createNoopCredentialCipher(),
      createAgentCrypto: createEd25519Crypto,
      assertSourceBuildable: () => undefined,
      registerDeployment: ({ runId, agentAddress }) => {
        registry.record(runId, agentAddress);
      },
      unregisterDeployment: ({ runId }) => {
        registry.unregister(runId);
      },
      multistepMailRouter: mailRouter,
      multistepSignalRouter: signalRouter,
      multistepDrainRouter: drainRouter,
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: tmpDir,
        SIDECAR_SIGNING_PUBLIC_KEY: "00".repeat(32),
        SIDECAR_SIGNING_PRIVATE_KEY: "00".repeat(32),
        HUB_WS_URL: "ws://test",
        SIDECAR_ID: "sc",
        SIDECAR_TOKEN: "tok",
        PATH: "/usr/bin",
        // Source-ref materialization reads both byte caps from the substrate env.
        SIDECAR_CACHE_MAX_BYTES: "1000000",
        SIDECAR_REGISTRY_MAX_TARBALL_BYTES: "1000000",
      },
      multistepSubprocessSpawner: failingSpawner,
      applyFrozenWorkflowClosure: stubApplyFrozenWorkflowClosure("wf-single"),
    });

    const frame: AgentDeployFrame = {
      type: "agent.deploy",
      agentAddress: "run_single@x.example",
      agentId: "single",
      hubPublicKey: "00".repeat(32),
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- single-step branch does not consult config before failing at spawn
      config: {
        agentAddress: "run_single@x.example",
        agentId: "single",
        sessionId: "s",
        sources: [],
        defaultSource: "primary",
        grants: [],
      } as unknown as AgentDeployFrame["config"],
      workflow: {
        // Placeholder hub-approved wire hash so the deploy path's fail-loud
        // guard passes; production always stamps it.
        approvedWireHash: "a".repeat(64),
        // Source-ref is the only deploy lineage: the frame carries no inline
        // definition, only the pin the sidecar re-materializes (the injected
        // closure stub above evaluates it to a single-step `s1` definition).
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

    await expect(router.deploy(frame)).rejects.toThrow();

    // The single-step head initialized its repo and recorded the hub key
    // before the spawn failed.
    expect(initedRepos).toEqual(["run_single@x.example"]);
    expect(recordedHubKeys).toEqual(["run_single@x.example"]);
    // The unwind reversed the in-memory hub key...
    expect(forgottenAgents).toEqual(["run_single@x.example"]);
    // ...but did NOT remove the head repo directory (it holds the durable
    // identity keypair a rerouted head must keep across a failed redeploy).
    expect(removedRepos).toEqual([]);
    // Registry untouched.
    expect(registry.resolve("run_single-x-example")).toBeNull();
  });
});
