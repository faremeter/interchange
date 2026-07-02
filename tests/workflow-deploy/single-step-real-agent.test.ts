// Single-step real-agent round-trip integration test.
//
// The proof that the spawned workflow-process child runs a REAL agent
// for a step, not the placeholder stub. Deploys a one-step workflow
// through the workflow-deploy orchestrator's multi-step branch (which
// spawns the workflow-process subprocess) against the real hub + real
// sidecar subprocess + mock inference fixture, fires the deployment's
// mail trigger, and asserts the step's committed output carries the
// agent's deterministic inference reply produced by `agent.send` --
// NOT the old stub value `req.agent.id`.
//
// The mock inference server returns a canned assistant reply built from
// the tool names it was handed (`I see these tools: <names>`); with an
// empty tool set the reply is the stable prefix `I see these tools: `.
// That deterministic reply is the test-provider seam this phase drives
// the real agent against, since real inference in CI is impractical.
//
// The test additionally asserts the per-step agent storage/workspace
// materialized under the sidecar data dir, rooted per run/step in a
// `workflow-step-state/` subtree that is a sibling of the workflow-run
// repo's git directory (where the run-event log lives), so the per-step
// store cannot clobber the run-event tree.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveDeploymentAddress,
  type ApprovalSet,
  type DeploySingleStepFn,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { deriveTrivialDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import { reconstructDurableConversation } from "@intx/sidecar-app/src/conversation-state";
import type { RepoId, WorkflowRunHubPrincipal } from "@intx/hub-sessions";
import { DEFAULT_ASSET_REF } from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "single-step-real-agent-1";
const WORKFLOW_RUN_REF = "refs/heads/main";
const STEP_ID = "step1";

// The mock inference server's reply for an empty tool set. The server
// builds `I see these tools: <names>` from the tool names it was given;
// with no tools the names list is empty and the agent surfaces the
// reply with trailing whitespace trimmed.
const EXPECTED_REPLY = "I see these tools:";

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

describe("single-step real-agent round-trip", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("spawned child runs a real agent and commits its reply as the step output", async () => {
    const agent = defineAgent({
      id: "agent-step1",
      systemPrompt: "You are the single-step agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });

    const deploymentMailAddress = deriveDeploymentAddress({
      deploymentId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });

    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${DEPLOYMENT_ID}`,
      trigger: { type: "mail", to: deploymentMailAddress },
      steps: {
        [STEP_ID]: step({ agent }),
      },
    });

    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `ins_${DEPLOYMENT_ID}`,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: deploymentMailAddress,
      systemPrompt: "Fallback prompt (overridden per step by the orchestrator)",
      tools: [],
      grants: [],
      sources: [
        {
          id: "anthropic:mock-model",
          provider: "anthropic",
          baseURL: `http://localhost:${env.inference.server.port}`,
          apiKey: "sk-mock",
          model: "mock-model",
        },
      ],
      defaultSource: "anthropic:mock-model",
    };

    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${deploymentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

    const launchSession: LaunchSessionFn = async (orchestratorParams) => {
      const deployContent = orchestratorParams.deployContent;
      await env.hub.sessionService.launchSession({
        agentAddress: orchestratorParams.agentAddress,
        agentId: orchestratorParams.agentId,
        instanceId: orchestratorParams.instanceId,
        config: orchestratorParams.config,
        deployContent: toLaunchDeployContent(deployContent),
        ...(orchestratorParams.toolPackagePins !== undefined
          ? { toolPackagePins: orchestratorParams.toolPackagePins }
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

    const deploySingleStepAtHead: DeploySingleStepFn = (params) =>
      env.hub.sessionService.deploySingleStepAtHead(params);

    const workflowRepo: WorkflowRepoWriter = {
      async writeWorkflowRepo(args) {
        const repoId: RepoId = { kind: "workflow", id: args.workflowRepoId };
        const principal: WorkflowRunHubPrincipal = { kind: "hub" };
        const files: Record<string, string> = {};
        for (const [k, v] of args.files) {
          files[k] = v;
        }
        await env.hub.agentRepoStore.repoStore.writeTree(
          principal,
          repoId,
          DEFAULT_ASSET_REF,
          {
            files,
            message: `single-step-real-agent test: write workflow repo ${args.workflowRepoId}`,
          },
        );
      },
    };

    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry: createDefaultDirectorRegistry(),
      workflowRepo,
      launchSession,
      sendMultiStepDeploy,
      deploySingleStepAtHead,
    });

    let result: Awaited<ReturnType<typeof orchestrator.deployWorkflow>>;
    try {
      result = await orchestrator.deployWorkflow({
        workflow,
        config,
        deployContent: { systemPrompt: config.systemPrompt },
        operatorApprovals,
        deploymentId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        hubPublicKey: "00".repeat(32),
      });
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      const diag = env.sidecarDiagnostics();
      throw new Error(
        `deployWorkflow failed: ${message}\n${diag.length > 0 ? diag : "<no sidecar diagnostics>"}`,
        { cause },
      );
    }
    expect(result.kind).toBe("multi-step");

    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveTrivialDeploymentId(deploymentMailAddress),
    };
    env.registerDeployment({
      deploymentId: DEPLOYMENT_ID,
      workflowDefinition: workflow,
      workflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      mailAddress: deploymentMailAddress,
    });

    expect(env.hub.router.getRoutableAddresses()).toContain(
      deploymentMailAddress,
    );

    const { messageId } = await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<single-step-real-agent-1@integration.interchange>",
    });

    // Wait for the run to reach a terminal phase, then read its full
    // event log. The supervisor mints the runId from the inbound mail
    // bytes; the test discovers it by listing `runs/`.
    const runId = await waitForFirstRunId(env, workflowRunRepoId, {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 20_000,
    });

    const terminal = await waitForWorkflowRunComplete(
      env,
      DEPLOYMENT_ID,
      runId,
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
    );
    expect(terminal.type).toBe("RunCompleted");

    const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
    const runStartedBody = events.find((e) => e.type === "RunStarted")?.body;
    if (runStartedBody === undefined) throw new Error("missing RunStarted");
    expect(runStartedBody["consumedMessageId"]).toBe(messageId);

    const stepCompleted = events.find(
      (e) => e.type === "StepCompleted" && e.body["stepId"] === STEP_ID,
    );
    if (stepCompleted === undefined) {
      throw new Error("missing StepCompleted for the single step");
    }

    // The proof: the step output is the REAL agent reply from
    // `agent.send` (the mock provider's deterministic output), not the
    // old stub value `req.agent.id` (which would have been the agent's
    // definition id, "agent-step1").
    const reply = readStepReply(stepCompleted.body);
    expect(reply).toBe(EXPECTED_REPLY);
    expect(reply).not.toBe(agent.id);

    // The agent's inference call actually reached the mock provider, so
    // the reply is real model output rather than a synthesized constant.
    expect(env.inference.requests.length).toBeGreaterThan(0);

    // The warm single-step agent's workspace/tools are rooted at a STABLE
    // per-agent path under `workflow-step-state/<repoId>/warm/<stepId>/`
    // (keyed by the step identity like the durable conversation store, NOT
    // the per-message runId) so the cached agent reuses one workspace
    // across messages and the scratch is bounded to one dir per agent
    // rather than leaking a fresh per-run subtree. The subtree is a sibling
    // of the workflow-run repo's git directory; the run-event log lives
    // inside the workflow-run repo's own tree, so the warm root cannot
    // overlap it.
    const stepStoreDir = path.join(
      env.sidecar.dataDir,
      "workflow-step-state",
      workflowRunRepoId.id,
      "warm",
      encodeURIComponent(STEP_ID),
    );
    expect(fs.existsSync(stepStoreDir)).toBe(true);
    expect(fs.existsSync(path.join(stepStoreDir, "workspace"))).toBe(true);

    // The warm single-step agent's conversation ContextStore is durable
    // (Phase 4.5): its `.git` lives at the stable per-agent durable store
    // root, NOT under the per-run `attempt-1` dir, so the conversation
    // survives across runs and child respawn. The per-run dir therefore
    // carries the workspace + tool state but no conversation `.git`.
    const durableConversationDir = path.join(
      env.sidecar.dataDir,
      "agent-conversation-state",
      workflowRunRepoId.id,
      encodeURIComponent(STEP_ID),
    );
    expect(fs.existsSync(path.join(durableConversationDir, ".git"))).toBe(true);

    // The conversation is mirrored through the real proxy -> supervisor
    // single-writer path to the workflow-run substrate at the per-agent
    // `agent-state/<stepId>/` path (Phase D1: a bucket-sharded WAL plus a
    // periodic checkpoint, no longer a single `conversation.json`), sibling
    // to the per-run event log under `runs/<runId>/...`. The supervisor's
    // substrate is the sidecar's on-disk workflow-run repo; reconstruct the
    // durable conversation from it (deterministic, no hub pack-push timing
    // dependency) and assert the agent's turn is durably committed.
    const sidecarWorkflowRunRepoDir = path.join(
      env.sidecar.dataDir,
      "workflow-runs",
      workflowRunRepoId.id,
    );
    const durableSubstrateAgentStateDir = path.join(
      sidecarWorkflowRunRepoDir,
      "agent-state",
      encodeURIComponent(STEP_ID),
    );
    const durableConversation = await reconstructDurableConversation(
      durableSubstrateAgentStateDir,
      STEP_ID,
    );
    if (durableConversation === null) {
      throw new Error("expected a durable conversation in the substrate");
    }
    expect(durableConversation.turns.length).toBeGreaterThan(0);

    // Neither the per-step root nor the durable conversation store lives
    // inside the workflow-run repo's working tree (where
    // `runs/<runId>/events/...` is committed).
    const workflowRunRepoDir =
      env.hub.agentRepoStore.repoStore.getRepoDir(workflowRunRepoId);
    expect(stepStoreDir.startsWith(workflowRunRepoDir)).toBe(false);
    expect(durableConversationDir.startsWith(workflowRunRepoDir)).toBe(false);
  });
});

/**
 * Extract the agent's reply string from a `StepCompleted` event body.
 * The runtime records the step output through the blob substrate; a
 * small `{ reply, turn }` output inlines as `inline:<json>`, so the
 * reply is recovered by parsing the JSON after the `inline:` prefix.
 */
function readStepReply(body: Record<string, unknown>): string {
  const output = body["output"];
  if (typeof output !== "object" || output === null || !("ref" in output)) {
    throw new Error(
      `StepCompleted output is not a { ref } record: ${JSON.stringify(output)}`,
    );
  }
  const ref: unknown = output.ref;
  if (typeof ref !== "string") {
    throw new Error(`StepCompleted output ref is not a string: ${String(ref)}`);
  }
  const INLINE_PREFIX = "inline:";
  if (!ref.startsWith(INLINE_PREFIX)) {
    throw new Error(
      `expected an inline output ref for the small step output, got ${ref}`,
    );
  }
  const parsed: unknown = JSON.parse(ref.slice(INLINE_PREFIX.length));
  if (typeof parsed !== "object" || parsed === null || !("reply" in parsed)) {
    throw new Error(
      `step output does not carry a reply field: ${JSON.stringify(parsed)}`,
    );
  }
  const reply: unknown = parsed.reply;
  if (typeof reply !== "string") {
    throw new Error(
      `step output reply is not a string: ${JSON.stringify(parsed)}`,
    );
  }
  return reply;
}
