// Unit-style smoke tests for the Phase I helpers landed alongside the
// integration-test fixture. These tests stand up only the hub
// substrate (no sidecar subprocess, no mock inference) so the helper
// surfaces that operate purely on the substrate (workflow-run repo
// reads, signal injection, processing-crash simulation) can be
// exercised in isolation. The end-to-end integration tests in the
// Phase I commit set exercise the helpers against the full env.

import { describe, test, expect, afterAll, beforeAll } from "bun:test";

import {
  WORKFLOW_RUN_TERMINAL_TYPES,
  injectSignal,
  readWorkflowRunEvents,
  simulateProcessingCrash,
  startHub,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type DeploymentHandle,
  type HubEnv,
} from "./deploy-flow-env";

import fs from "node:fs";

const DEPLOYMENT_ID = "ins_smoke-test";
const MAIL_ADDRESS = "ins_smoke-test@integration.interchange";

// `startHub` is the slice of `startDeployFlowEnv` that owns just the
// hub-substrate + WS server. The helpers we smoke-test here operate
// entirely on the substrate; standing up the sidecar subprocess would
// add minutes of startup without exercising any path under test.
async function startSmokeEnv(): Promise<{
  env: DeployFlowEnv;
  hub: HubEnv;
  tempDirs: string[];
}> {
  const tempDirs: string[] = [];
  const registerTempDir = (dir: string): void => {
    tempDirs.push(dir);
  };
  const hub = await startHub(registerTempDir);
  const deployments = new Map<string, DeploymentHandle>();
  const registerDeployment = (handle: DeploymentHandle): void => {
    if (deployments.has(handle.deploymentId)) {
      throw new Error(
        `smoke env: deployment ${handle.deploymentId} already registered`,
      );
    }
    deployments.set(handle.deploymentId, handle);
  };
  const env: DeployFlowEnv = {
    hub,
    // The fields below are unused by the helpers exercised here.
    // Construct narrow stand-ins so the env-shape type is satisfied
    // without spinning up the corresponding subsystems.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- inference is not consulted by the substrate-only helpers under test
    inference: {
      server: { stop: () => undefined },
      requests: [],
    } as unknown as DeployFlowEnv["inference"],
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- sidecar is not consulted by the substrate-only helpers under test
    sidecar: {
      proc: { kill: () => undefined },
      dataDir: "",
      stderr: [],
    } as unknown as DeployFlowEnv["sidecar"],
    sidecarDiagnostics: () => "",
    deployments,
    registerDeployment,
    teardown: async () => {
      await hub.server.stop(true);
      for (const d of tempDirs.splice(0)) {
        await fs.promises.rm(d, { recursive: true, force: true }).catch(() => {
          /* best effort cleanup */
        });
      }
    },
  };
  return { env, hub, tempDirs };
}

let env: DeployFlowEnv;

beforeAll(async () => {
  ({ env } = await startSmokeEnv());
  env.registerDeployment({
    deploymentId: DEPLOYMENT_ID,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the smoke tests do not exercise the workflow-definition shape; the helpers only consult `workflowRunRepoId`/`workflowRunRef`
    workflowDefinition: {
      id: "wf_smoke",
      stepOrder: [],
    } as unknown as DeploymentHandle["workflowDefinition"],
    workflowRunRepoId: { kind: "workflow-run", id: DEPLOYMENT_ID },
    workflowRunRef: "refs/heads/main",
    mailAddress: MAIL_ADDRESS,
  });
});

afterAll(async () => {
  await env.teardown();
});

describe("deploy-flow-env helpers smoke tests", () => {
  test("WORKFLOW_RUN_TERMINAL_TYPES matches the kind handler's vocabulary", () => {
    expect(WORKFLOW_RUN_TERMINAL_TYPES.has("RunCompleted")).toBe(true);
    expect(WORKFLOW_RUN_TERMINAL_TYPES.has("RunFailed")).toBe(true);
    expect(WORKFLOW_RUN_TERMINAL_TYPES.has("RunCancelled")).toBe(true);
    expect(WORKFLOW_RUN_TERMINAL_TYPES.has("RunStarted")).toBe(false);
  });

  test("readWorkflowRunEvents returns an empty array before any commit lands", async () => {
    const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, "run-1");
    expect(events).toEqual([]);
  });

  test("injectSignal routes the wire frame through the hub router to the deployment sidecar", async () => {
    // The helper now drives the production hub -> sidecar ->
    // supervisor -> workflow-process pipeline rather than writing a
    // SignalReceived blob directly to the hub substrate. Routing the
    // signal through the child preserves the workflow-run repo's
    // single-writer invariant on the sidecar side -- without it, a
    // host-side substrate write would race against the next pack push
    // from the child and surface `non_fast_forward` on the hub. The
    // smoke env has no sidecar registered against the deployment
    // address, so the helper surfaces the routing error verbatim.
    await expect(
      injectSignal(env, DEPLOYMENT_ID, "run-2", "operator.ack", { ok: true }),
    ).rejects.toThrow(/No sidecar connected/);

    const after = await readWorkflowRunEvents(env, DEPLOYMENT_ID, "run-2");
    expect(after).toEqual([]);
  });

  test("waitForWorkflowRunComplete throws on timeout when no terminal event lands", async () => {
    await expect(
      waitForWorkflowRunComplete(env, DEPLOYMENT_ID, "run-3", {
        timeoutMs: 100,
      }),
    ).rejects.toThrow(/timed out/);
  });

  test("simulateProcessingCrash composes enqueueInbox + dequeueToProcessing", async () => {
    const address = "ins_smoke-test@integration.interchange";
    const messageId = "<smoke-crash-1@integration.interchange>";
    const receivedAt = 1_700_000_000_000;
    await simulateProcessingCrash(
      env,
      DEPLOYMENT_ID,
      address,
      messageId,
      receivedAt,
    );

    // Surface the resulting tree via the substrate's getRepoDir +
    // isomorphic-git so the test is decoupled from the kind handler's
    // private path-construction helpers.
    const repoDir = env.hub.agentRepoStore.repoStore.getRepoDir({
      kind: "workflow-run",
      id: DEPLOYMENT_ID,
    });
    const git = await import("isomorphic-git");
    // The claim-check primitives target the workflow-run kind
    // handler's canonical claim-check ref (`refs/heads/events`); the
    // smoke test peeks at the resulting tree on that ref to assert
    // the processing entry landed.
    const oid = await git.default.resolveRef({
      fs,
      dir: repoDir,
      ref: "refs/heads/events",
    });
    const tree = await git.default.readTree({
      fs,
      dir: repoDir,
      oid,
      filepath: `addresses/${encodeURIComponent(address)}/processing`,
    });
    const filenames = tree.tree.map((e) => e.path);
    expect(filenames).toContain(`${String(receivedAt)}-${messageId}.json`);
  });
});
