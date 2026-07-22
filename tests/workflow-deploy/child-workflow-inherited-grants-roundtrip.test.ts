// Parent -> child grants-file inheritance through the real subprocess.
//
// When a mail-triggered parent run spawns a child, the child runs under
// the parent's authority: the spawn adapter reads the parent's
// `runs/<parentRunId>/grants.json` and writes the same flat grant set to
// the child's own `runs/<childRunId>/grants.json`. This test drives that
// composition end to end -- a real deployed parent workflow whose spawn
// step fires a child through the real sidecar subprocess -- and asserts
// BOTH files land on the sidecar's on-disk workflow-run repo, carrying the
// grants the trigger delivered.
//
// What this ADDS over the unit coverage. The spawn adapter's behavior in
// isolation -- inheritance, grandchild multi-hop, and fail-closed-at-spawn
// when the parent grants file is absent -- is proven directly against a
// real on-disk substrate in
// `apps/sidecar/src/workflow-substrate-factory-child-grants.test.ts`, which
// calls `createSidecarRunChild` with hand-seeded grants. This test proves
// the WIRING composes: that a real mail trigger's delivered grants reach
// `runs/<parentRunId>/grants.json` through the supervisor, and that the
// real child-spawn adapter is actually invoked with those grants during an
// honest parent->child spawn, writing the child's inherited file. The
// fail-closed negative is not reproducible here -- every mail-triggered run
// materializes a grants file, so an absent parent file cannot arise through
// the trigger path -- and stays covered at the unit level.
//
// SCOPE. The child's step body does not execute: `childWorkflow` per-step
// execution is not implemented (INTR-310), so the child fails its step
// after the spawn. That is expected and irrelevant here -- the grants-file
// inheritance write happens at spawn time, BEFORE the child step runs, so
// grant CONSUMPTION by the child is out of reach and not asserted. Only the
// spawn-time inheritance write is under test.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { type } from "arktype";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import type { WireGrantRule } from "@intx/types/grant-wire";
import {
  childWorkflow,
  defineWorkflow,
  step,
  type WorkflowDefinition,
} from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveDeploymentAddress,
  type ApprovalSet,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import type { RepoId, WorkflowRunHubPrincipal } from "@intx/hub-sessions";
import { DEFAULT_ASSET_REF } from "@intx/hub-sessions";

import {
  SESSION_ID,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const PARENT_DEPLOYMENT_ID = "child-inherited-grants-parent-1";
const CHILD_DEPLOYMENT_ID = "child-inherited-grants-child-1";
const PARENT_WORKFLOW_ID = `wf_${PARENT_DEPLOYMENT_ID}`;
const CHILD_WORKFLOW_ID = `wf_${CHILD_DEPLOYMENT_ID}`;

// The parent run's grant, delivered per run via the `run.grants` frame.
// The child must inherit exactly this set into its own grants file.
const PARENT_GRANT: WireGrantRule = {
  id: "grant-parent-inherited",
  resource: "effect:fs:write",
  action: "invoke",
  effect: "allow",
  origin: "creator",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: null,
};

// The envelope `grants.json` carries: `{ grants: [...] }`.
const GrantsFile = type({ grants: "unknown[]" }).onUndeclaredKey("ignore");

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

// Read a run's grants.json off the sidecar's on-disk workflow-run repo.
// Returns null when the file is absent. The sidecar roots each workflow-run
// repo at `<dataDir>/workflow-runs/<repoId>` (the workflow-run kind
// handler's directoryPrefix), and each run's grants live at
// `runs/<runId>/grants.json` inside it.
function readRunGrantsFile(repoId: string, runId: string): unknown[] | null {
  const filePath = path.join(
    env.sidecar.dataDir,
    "workflow-runs",
    repoId,
    "runs",
    runId,
    "grants.json",
  );
  if (!fs.existsSync(filePath)) return null;
  const parsed: unknown = JSON.parse(fs.readFileSync(filePath, "utf-8"));
  return GrantsFile.assert(parsed).grants;
}

describe("parent -> child inherited grants round-trip", () => {
  test("the child inherits the parent's delivered grants file at spawn", async () => {
    const childAgent = defineAgent({
      id: "agent-inherited-child-step",
      systemPrompt: "You are the child workflow's step agent.",
      tools: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });
    const parentStepAgent = defineAgent({
      id: "agent-inherited-parent-step",
      systemPrompt: "You are the parent workflow's first step agent.",
      tools: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });

    const parentMailAddress = deriveDeploymentAddress({
      deploymentId: PARENT_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });
    const childMailAddress = deriveDeploymentAddress({
      deploymentId: CHILD_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });

    const childWorkflowDefinition: WorkflowDefinition = defineWorkflow({
      id: CHILD_WORKFLOW_ID,
      trigger: { type: "mail", to: childMailAddress },
      steps: { childStep: step({ agent: childAgent }) },
    });
    const parentWorkflowDefinition: WorkflowDefinition = defineWorkflow({
      id: PARENT_WORKFLOW_ID,
      trigger: { type: "mail", to: parentMailAddress },
      steps: {
        step1: step({ agent: parentStepAgent }),
        spawn: childWorkflow({
          definitionRef: CHILD_WORKFLOW_ID,
          after: ["step1"],
        }),
      },
    });

    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${parentMailAddress}`,
      `mail.address:${childMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

    const baseConfig = (address: string, agentId: string): HarnessConfig => ({
      sessionId: SESSION_ID,
      agentId,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: address,
      systemPrompt: "Fallback prompt (overridden per step).",
      tools: [],
      grants: [],
      sources: [
        {
          id: "anthropic:mock-model",
          provider: "anthropic",
          baseURL: `http://localhost:${String(env.inference.server.port)}`,
          apiKey: "sk-mock",
          model: "mock-model",
        },
      ],
      defaultSource: "anthropic:mock-model",
    });

    const launchSession: LaunchSessionFn = async (p) => {
      await env.hub.sessionService.stageWorkflowStep({
        agentAddress: p.agentAddress,
        agentId: p.agentId,
        instanceId: p.instanceId,
        config: p.config,
        deployContent: toLaunchDeployContent(p.deployContent),
        ...(p.toolPackagePins !== undefined
          ? { toolPackagePins: p.toolPackagePins }
          : {}),
      });
    };
    const sendMultiStepDeploy: SendMultiStepDeployFn = async (params) =>
      env.hub.router.sendAgentDeploy(params.agentAddress, params.config, {
        definition: {
          id: params.definition.id,
          triggers: [...params.definition.triggers],
          stepOrder: [...params.definition.stepOrder],
          steps: params.definition.steps as Record<string, unknown>,
          ...(params.definition.state !== undefined
            ? { state: params.definition.state }
            : {}),
        },
        sources: params.sources,
      });
    const workflowRepo: WorkflowRepoWriter = {
      async writeWorkflowRepo(args) {
        const repoId: RepoId = { kind: "workflow", id: args.workflowRepoId };
        const principal: WorkflowRunHubPrincipal = { kind: "hub" };
        const files: Record<string, string> = {};
        for (const [k, v] of args.files) files[k] = v;
        await env.hub.agentRepoStore.repoStore.writeTree(
          principal,
          repoId,
          DEFAULT_ASSET_REF,
          { files, message: `inherited-grants test: ${args.workflowRepoId}` },
        );
      },
    };

    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry: createDefaultDirectorRegistry(),
      workflowRepo,
      launchSession,
      sendMultiStepDeploy,
      deploySingleStepAtHead: (params) =>
        env.hub.sessionService.deploySingleStepAtHead(params),
    });

    // Deploy the child (as its own asset) then the parent.
    const childResult = await orchestrator.deployWorkflow({
      workflow: childWorkflowDefinition,
      config: baseConfig(childMailAddress, `ins_${CHILD_DEPLOYMENT_ID}`),
      deployContent: { systemPrompt: "Fallback prompt (overridden per step)." },
      operatorApprovals,
      deploymentId: CHILD_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      hubPublicKey: "00".repeat(32),
    });
    expect(childResult.publicKey).toBeTruthy();

    const parentResult = await orchestrator.deployWorkflow({
      workflow: parentWorkflowDefinition,
      config: baseConfig(parentMailAddress, `ins_${PARENT_DEPLOYMENT_ID}`),
      deployContent: { systemPrompt: "Fallback prompt (overridden per step)." },
      operatorApprovals,
      deploymentId: PARENT_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      hubPublicKey: "00".repeat(32),
    });
    expect(parentResult.publicKey).toBeTruthy();

    const parentRepoId = deriveDeploymentId(parentMailAddress);
    const parentWorkflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: parentRepoId,
    };
    env.registerDeployment({
      deploymentId: PARENT_DEPLOYMENT_ID,
      workflowDefinition: parentWorkflowDefinition,
      workflowRunRepoId: parentWorkflowRunRepoId,
      workflowRunRef: "refs/heads/main",
      mailAddress: parentMailAddress,
    });

    expect(env.hub.router.getRoutableAddresses()).toContain(parentMailAddress);

    // Fire the parent trigger carrying the parent grant per run.
    await fireMailTrigger(env, parentMailAddress, {
      messageId: "<child-inherited-grants-1@integration.interchange>",
      grants: [PARENT_GRANT],
    });

    const parentRunId = await waitForFirstRunId(env, parentWorkflowRunRepoId, {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 20_000,
    });

    // Wait for the spawn to fire; capture the child's runId.
    await waitFor(
      async () => {
        const events = await readWorkflowRunEvents(
          env,
          PARENT_DEPLOYMENT_ID,
          parentRunId,
        );
        return events.some((e) => e.type === "ChildSpawned");
      },
      { diagnostics: env.sidecarDiagnostics, timeoutMs: 20_000 },
    );

    const parentEvents = await readWorkflowRunEvents(
      env,
      PARENT_DEPLOYMENT_ID,
      parentRunId,
    );
    const spawned = parentEvents.find((e) => e.type === "ChildSpawned");
    if (spawned === undefined) throw new Error("unreachable: no ChildSpawned");
    const childRunId = spawned.body["childRunId"];
    if (typeof childRunId !== "string") {
      throw new Error(
        `ChildSpawned missing string childRunId; got ${typeof childRunId}`,
      );
    }

    // The parent's delivered grants landed in its own grants file.
    const parentGrantsOnDisk = readRunGrantsFile(parentRepoId, parentRunId);
    if (parentGrantsOnDisk === null) {
      throw new Error(
        `parent grants.json absent for run ${parentRunId}\n${env.sidecarDiagnostics()}`,
      );
    }
    expect(parentGrantsOnDisk).toEqual([PARENT_GRANT]);

    // The child's inherited grants file was written at spawn, carrying the
    // parent's grant set verbatim. The spawn writes it before the child's
    // (INTR-310-failing) step runs; poll to let the write land.
    await waitFor(() => readRunGrantsFile(parentRepoId, childRunId) !== null, {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 20_000,
    });
    const childGrantsOnDisk = readRunGrantsFile(parentRepoId, childRunId);
    expect(childGrantsOnDisk).toEqual([PARENT_GRANT]);
  });
});
