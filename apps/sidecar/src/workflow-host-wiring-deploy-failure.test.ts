// Pins H-A1: a deploy-router rejection must not leave the
// `DeploymentAddressRegistry` populated. The deploy router used to
// call `registerDeployment` BEFORE every step that can throw (the
// trivial branch's `provisionAgent`, the multi-step branch's asset
// materialization and `supervisor.spawn`). The link's
// `handleAgentDeploy` catches the rejection and sends `agent.error`
// without invoking `deployRouter.undeploy(frame)`, so the registry
// retained the `(deploymentId -> agentAddress)` mapping for a
// deployment that does not exist. The fix defers registration until
// every throwy step has succeeded; both branches must leave the
// registry clean on deploy failure.
//
// Both branches drive their failure path through this test:
//   - trivial: `sessions.provisionAgent` throws.
//   - multi-step: the subprocess spawner throws synchronously so
//     `supervisor.spawn` rejects.

import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join as pathJoin } from "node:path";

import { describe, test, expect } from "bun:test";
import { createInMemoryTransport } from "@intx/mail-memory";
import { createEd25519Crypto, generateKeyPair } from "@intx/crypto";
import type { RepoId, RepoStore } from "@intx/hub-sessions";
import type { AgentDeployFrame } from "@intx/types/sidecar";
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
    async scanKeys() {
      return [];
    },
    signChallenge() {
      return null;
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

function makeRouterDeps() {
  const registry = createDeploymentAddressRegistry();
  const mailRouter = createMultistepMailRouter();
  const signalRouter = createMultistepSignalRouter();
  const drainRouter = createMultistepDrainRouter();
  const transport = createInMemoryTransport();
  return { registry, mailRouter, signalRouter, drainRouter, transport };
}

describe("deploy-failure registry leak", () => {
  test("trivial deploy: provisionAgent throws and registry stays clean", async () => {
    const { registry, mailRouter, signalRouter, drainRouter, transport } =
      makeRouterDeps();

    const sessions = stubFailingSessions();
    const keyStore = stubKeyStore();

    const router = createSidecarDeployRouter({
      sessions,
      keyStore,
      onAgentEvent: () => () => undefined,
      transport,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- trivial branch reaches provisionAgent before any repoStore usage
      repoStore: {} as Parameters<
        typeof createSidecarDeployRouter
      >[0]["repoStore"],
      signingKeySeed: new Uint8Array(32),
      createAgentCrypto: createEd25519Crypto,
      assertSourceBuildable: () => undefined,
      registerDeployment: ({ deploymentId, agentAddress }) => {
        registry.record(deploymentId, agentAddress);
      },
      unregisterDeployment: ({ deploymentId }) => {
        registry.unregister(deploymentId);
      },
      multistepMailRouter: mailRouter,
      multistepSignalRouter: signalRouter,
      multistepDrainRouter: drainRouter,
    });

    const frame: AgentDeployFrame = {
      type: "agent.deploy",
      agentAddress: "agent-fail@x.example",
      agentId: "agent-fail",
      hubPublicKey: "00".repeat(32),
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- trivialLaunch surfaces config to the failing provisionAgent stub
      config: {
        agentAddress: "agent-fail@x.example",
        agentId: "agent-fail",
        sessionId: "session-fail",
        sources: [],
        defaultSource: "primary",
        grants: [],
      } as unknown as AgentDeployFrame["config"],
    };

    let threw = false;
    try {
      await router.deploy(frame);
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);

    // `deriveTrivialDeploymentId` replaces every char outside
    // `[a-zA-Z0-9_-]` with `-`, so `@` and `.` both become `-`.
    const slug = "agent-fail-x-example";
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
      onAgentEvent: () => () => undefined,
      transport,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub: only getRepoDir + writeTree are exercised before the spawn-time failure
      repoStore: repoStoreStub as RepoStore,
      signingKeySeed: new Uint8Array(32),
      createAgentCrypto: createEd25519Crypto,
      assertSourceBuildable: () => undefined,
      registerDeployment: ({ deploymentId, agentAddress }) => {
        registry.record(deploymentId, agentAddress);
      },
      unregisterDeployment: ({ deploymentId }) => {
        registry.unregister(deploymentId);
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
      },
      multistepSubprocessSpawner: failingSpawner,
    });

    const frame: AgentDeployFrame = {
      type: "agent.deploy",
      // Single-step projection: the deploy router parses the frame
      // address into the legacy agent-state repo id, so it must carry the
      // canonical `ins_<id>@<domain>` shape.
      agentAddress: "ins_mstep@x.example",
      agentId: "mstep",
      hubPublicKey: "00".repeat(32),
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- multi-step branch does not consult config before failing
      config: {
        agentAddress: "ins_mstep@x.example",
        agentId: "mstep",
        sessionId: "s",
        sources: [],
        defaultSource: "primary",
        grants: [],
      } as unknown as AgentDeployFrame["config"],
      workflow: {
        definition: {
          id: "wf-1",
          triggers: [{ type: "manual" }],
          stepOrder: ["s1"],
          steps: { s1: { kind: "step" } },
        },
        sources: {
          s1: {
            id: "primary",
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-x",
            model: "claude-3-5",
          },
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

    const slug = "ins_mstep-x-example";
    expect(registry.resolve(slug)).toBeNull();
  });
});
