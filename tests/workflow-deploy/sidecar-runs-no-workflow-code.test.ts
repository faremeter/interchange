// The sidecar process never evaluates a deployed workflow's code.
//
// Code the long-lived sidecar process imports stays in it until the process
// exits, so a sidecar that evaluated every deployment's closure grew with every
// deployment it ever hosted, and a workflow whose code failed while loading
// took the sidecar's other deployments with it. Only the workflow child, which
// exits when its deployment stops, evaluates the code.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, and real workflow-process children evaluating the deployed
// source.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import type { HarnessConfig } from "@intx/types/runtime";
import { deriveRunAddress } from "@intx/workflow-deploy";
import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  tenantSlugFromId,
} from "@intx/test-harness/seed";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  startDeployFlowEnv,
  terminateSidecarSubprocess,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type SidecarHandle,
} from "../hub-agent/lib/deploy-flow-env";
import { crashSidecar, restartSidecar } from "./sleep-restart-helpers";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const TENANT_ID = "tnt_sidecar_runs_no_code";
const CALLER_PRINCIPAL_ID = "prn_sidecar_runs_no_code";

let env: DeployFlowEnv;
let h: TestDb;
let markerDir: string;
// Run in reverse, so a restarted sidecar stops before its env tears down.
const cleanups: (() => Promise<void>)[] = [];

// A workflow of two actions whose module appends the evaluating process's pid
// to `marker`. With `throwInWorkflowChild` it throws while loading in a
// workflow child, which is the only process whose env names the closure dir,
// so the install-time probe still approves it.
function markingWorkflowEntry(params: {
  address: string;
  workflowId: string;
  marker: string;
  throwInWorkflowChild?: boolean;
}): string {
  return `
import { appendFileSync } from "node:fs";
import { defineWorkflow, action } from "@intx/workflow/definition";

appendFileSync(${JSON.stringify(params.marker)}, String(process.pid) + "\\n");
${
  params.throwInWorkflowChild === true
    ? `if (process.env.CLOSURE_PACKAGE_DIR !== undefined) {
  throw new Error("fixture: this workflow refuses to load in its child");
}`
    : ""
}
export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    gather: action({ handler: "gather" }),
    persist: action({ handler: "persist", after: ["gather"] }),
  },
});

export async function gather(input, _ctx, _signal) {
  return { ok: true, input };
}

export async function persist(input, _ctx, _signal) {
  return { persisted: true, input };
}
`;
}

async function deployMarkingWorkflow(
  env: DeployFlowEnv,
  params: {
    runId: string;
    marker: string;
    throwInWorkflowChild?: boolean;
  },
): Promise<string> {
  const address = deriveRunAddress({
    runId: params.runId,
    domain: DEPLOYMENT_DOMAIN,
  });
  const definitionAssetId = `ast_${params.runId}_wf`;
  await seedAsset(h.db, {
    id: definitionAssetId,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: definitionAssetId,
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });
  const config: HarnessConfig = {
    sessionId: SESSION_ID,
    agentId: params.runId,
    tenantId: "tenant-1",
    principalId: "prin_sidecar-runs-no-code",
    agentAddress: address,
    systemPrompt: "unused (no agent step)",
    tools: [],
    grants: [],
    sources: [
      {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        credentialId: "sk-mock",
        model: "mock-model",
      },
    ],
    defaultSource: "anthropic:mock-model",
  };
  await deployWorkflowSourceForTest(env, {
    entryModule: markingWorkflowEntry({
      address,
      workflowId: `wf_${params.runId}`,
      marker: params.marker,
      ...(params.throwInWorkflowChild === true
        ? { throwInWorkflowChild: true }
        : {}),
    }),
    actions: "./workflow.mjs",
    db: h.db,
    tenantId: TENANT_ID,
    definitionAssetId,
    anchorRunId: params.runId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: address,
    approvals: "approve-probed",
    config,
  });
  return address;
}

async function readPids(marker: string): Promise<number[]> {
  const text = await fs.readFile(marker, "utf8");
  return text
    .split("\n")
    .filter((line) => line.length > 0)
    .map((line) => Number(line));
}

function sidecarPid(sidecar: SidecarHandle): number {
  const pid = sidecar.proc.pid;
  if (pid === undefined) throw new Error("the sidecar subprocess has no pid");
  return pid;
}

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  h = await createTestDb();
  await h.db.insert(tenantTable).values({
    id: TENANT_ID,
    name: TENANT_ID,
    slug: tenantSlugFromId(TENANT_ID),
    domain: DEPLOYMENT_DOMAIN,
    parentId: null,
  });
  await seedPrincipal(h.db, {
    id: CALLER_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
  });
  markerDir = await fs.mkdtemp(path.join(os.tmpdir(), "sidecar-runs-no-code-"));
  env = await startDeployFlowEnv();
  cleanups.push(() => env.teardown());
});

afterAll(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  if (h !== undefined) await h.close();
  if (markerDir !== undefined) {
    await fs.rm(markerDir, { recursive: true, force: true });
  }
});

describe.skipIf(!harnessDbEnvAvailable())(
  "the sidecar process runs no workflow code",
  () => {
    test("repeated deploys and undeploys evaluate the code only outside the sidecar process", async () => {
      const marker = path.join(markerDir, "repeated.pids");
      const deploys = 3;
      for (let i = 0; i < deploys; i++) {
        const address = await deployMarkingWorkflow(env, {
          runId: `run_c0de1501a7ed${String(i).padStart(4, "0")}`,
          marker,
        });
        await waitFor(
          () => env.hub.router.getRoutableAddresses().includes(address),
          { diagnostics: env.sidecarDiagnostics },
        );
        await env.hub.router.sendAgentUndeploy(address, "test: next deploy");
        await waitFor(
          () => !env.hub.router.getRoutableAddresses().includes(address),
          { diagnostics: env.sidecarDiagnostics },
        );
      }

      // Every deploy's probe child and workflow child evaluated the code; the
      // sidecar process never did.
      const pids = await readPids(marker);
      expect(pids.length).toBeGreaterThanOrEqual(deploys);
      expect(pids).not.toContain(sidecarPid(env.sidecar));
    }, 180_000);

    test("a workflow whose code throws while loading fails only its own deploy", async () => {
      const marker = path.join(markerDir, "throwing.pids");
      await expect(
        deployMarkingWorkflow(env, {
          runId: "run_c0de1501a7edbad0",
          marker,
          throwInWorkflowChild: true,
        }),
      ).rejects.toThrow(/refuses to load in its child/);
      expect(await readPids(marker)).not.toContain(sidecarPid(env.sidecar));

      // The sidecar survived and still deploys and runs another workflow.
      expect(env.sidecar.proc.exitCode).toBeNull();
      const healthyMarker = path.join(markerDir, "healthy.pids");
      const address = await deployMarkingWorkflow(env, {
        runId: "run_c0de1501a7ed900d",
        marker: healthyMarker,
      });
      await waitFor(
        () => env.hub.router.getRoutableAddresses().includes(address),
        { diagnostics: env.sidecarDiagnostics },
      );
      await fireMailTrigger(env, address, {
        messageId: "<sidecar-runs-no-code-1@integration.interchange>",
        content: "go",
      });
      const deployment = env.deployments.get("run_c0de1501a7ed900d");
      if (deployment === undefined) {
        throw new Error("the healthy deployment was not registered");
      }
      const runId = await waitForFirstRunId(env, deployment.workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
      });
      const terminal = await waitForWorkflowRunComplete(
        env,
        "run_c0de1501a7ed900d",
        runId,
        { diagnostics: env.sidecarDiagnostics },
      );
      expect(terminal.type).toBe("RunCompleted");
      // Running the actions did not load the workflow's module here either.
      expect(await readPids(healthyMarker)).not.toContain(
        sidecarPid(env.sidecar),
      );
    }, 180_000);

    test("a restarted sidecar restores a deployment without evaluating its code", async () => {
      // Its own env, because the crash kills the env's sidecar.
      const restoreEnv = await startDeployFlowEnv();
      cleanups.push(() => restoreEnv.teardown());
      const marker = path.join(markerDir, "restore.pids");
      const address = await deployMarkingWorkflow(restoreEnv, {
        runId: "run_c0de1501a7edf00d",
        marker,
      });
      await waitFor(
        () => restoreEnv.hub.router.getRoutableAddresses().includes(address),
        { diagnostics: restoreEnv.sidecarDiagnostics },
      );
      const deployedPids = await readPids(marker);

      const dataDir = await crashSidecar(restoreEnv, address);
      const restarted = await restartSidecar(restoreEnv, {
        address,
        dataDir,
        registerTempDir: (dir) => {
          cleanups.push(() => fs.rm(dir, { recursive: true, force: true }));
        },
      });
      cleanups.push(() => terminateSidecarSubprocess(restarted));

      // The restored deployment's new workflow child evaluated the code before
      // its address became routable again; the restarted sidecar never did.
      const pids = await readPids(marker);
      expect(pids.length).toBeGreaterThan(deployedPids.length);
      expect(pids).not.toContain(sidecarPid(restarted));
    }, 180_000);
  },
);
