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

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join as pathJoin } from "node:path";

import { describe, test, expect } from "bun:test";
import { createInMemoryTransport } from "@intx/mail-memory";
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
      throw new Error("not used in this test");
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

    const router = createSidecarDeployRouter({
      sessions,
      keyStore,
      onAgentEvent: () => () => undefined,
      transport,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- repoStore is consulted lazily by the supervisor; the spawn-time failure short-circuits the test
      repoStore: {} as Parameters<
        typeof createSidecarDeployRouter
      >[0]["repoStore"],
      signingKeySeed: new Uint8Array(32),
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
      agentAddress: "mstep@x.example",
      agentId: "mstep",
      hubPublicKey: "00".repeat(32),
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- multi-step branch does not consult config before failing
      config: {
        agentAddress: "mstep@x.example",
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

    const slug = "mstep-x-example";
    expect(registry.resolve(slug)).toBeNull();
  });
});
