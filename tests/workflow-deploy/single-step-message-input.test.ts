// Single-step message-input round-trip integration test (Phase 4.2).
//
// The proof that the inbound mail's BODY reaches the warm agent's
// `agent.send` as the step input -- not an empty or placeholder input.
// Deploys a one-step workflow through the workflow-deploy orchestrator's
// multi-step branch (which spawns the workflow-process subprocess)
// against the real hub + real sidecar subprocess + an echo inference
// fixture, fires a mail with a KNOWN body, and asserts the step's
// committed reply echoes that body. A SECOND mail with a different body
// produces a SECOND run whose reply echoes the second body.
//
// The mock inference server runs in echo mode: it reflects the last
// user message's text back as `echo:<text>`. The agent delivers the
// inbound conversation content as the user turn, so the echoed reply is
// the load-bearing proof that the mail body traversed
// trigger.payload -> step input -> synthesizeInputContent -> agent.send.
//
// Against the pre-4.2 behaviour (trigger.fire carries no bytes; the
// child threads no triggerPayload, so the first step's
// `{ from: "trigger.payload" }` input resolves to null/empty), the
// echoed reply would be `echo:` with no body -- this test fails there.

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
import type { RepoId, WorkflowRunHubPrincipal } from "@intx/hub-sessions";
import { DEFAULT_ASSET_REF } from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  fireMailTrigger,
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "single-step-message-input-1";
const STEP_ID = "step1";

const FIRST_BODY = "First inbound body alpha-7391.";
const SECOND_BODY = "Second inbound body bravo-5520.";

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv({ inferenceEchoUserMessage: true });
});

afterAll(async () => {
  await env.teardown();
});

describe("single-step message-input round-trip", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("the inbound mail body reaches agent.send as the step input", async () => {
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
            message: `single-step-message-input test: write workflow repo ${args.workflowRepoId}`,
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
      workflowRunRef: "refs/heads/main",
      mailAddress: deploymentMailAddress,
    });

    expect(env.hub.router.getRoutableAddresses()).toContain(
      deploymentMailAddress,
    );

    // First message: a KNOWN body. The runId the supervisor mints from
    // the inbound mail is the messageId; discover it from `runs/`.
    const first = await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<single-step-message-input-1@integration.interchange>",
      content: FIRST_BODY,
    });

    const firstRunId = await waitForFirstRunId(env, workflowRunRepoId, {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 20_000,
    });

    const firstTerminal = await waitForWorkflowRunComplete(
      env,
      DEPLOYMENT_ID,
      firstRunId,
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
    );
    expect(firstTerminal.type).toBe("RunCompleted");

    const firstEvents = await readWorkflowRunEvents(
      env,
      DEPLOYMENT_ID,
      firstRunId,
    );
    const firstStartedBody = firstEvents.find(
      (e) => e.type === "RunStarted",
    )?.body;
    if (firstStartedBody === undefined) throw new Error("missing RunStarted");
    expect(firstStartedBody["consumedMessageId"]).toBe(first.messageId);

    const firstReply = readStepReply(stepCompletedBody(firstEvents));

    // The load-bearing assertion: the agent's `agent.send` received the
    // mail body, so the echoed user turn (and thus the reply) carries
    // it. The agent's reactor frames the inbound conversation as
    // `[From: <sender>]\n\n<body>` before inference, so the echo is
    // `echo:[From: ...]\n\n<body>` -- the body substring is the proof.
    // Pre-4.2 the trigger payload was absent and the step input
    // resolved to null, so the echo would carry no body at all.
    expect(firstReply.startsWith("echo:")).toBe(true);
    expect(firstReply).toContain(FIRST_BODY);
    expect(firstReply).not.toBe("echo:");
    expect(firstReply).not.toBe("echo:null");

    // Second message: a DIFFERENT body produces a SECOND run whose
    // agent.send got the second body.
    const second = await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<single-step-message-input-2@integration.interchange>",
      content: SECOND_BODY,
    });
    expect(second.messageId).not.toBe(first.messageId);

    const secondRunId = await waitForSecondRunId(
      env,
      workflowRunRepoId,
      firstRunId,
      { timeoutMs: 20_000 },
    );

    const secondTerminal = await waitForWorkflowRunComplete(
      env,
      DEPLOYMENT_ID,
      secondRunId,
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
    );
    expect(secondTerminal.type).toBe("RunCompleted");

    const secondEvents = await readWorkflowRunEvents(
      env,
      DEPLOYMENT_ID,
      secondRunId,
    );
    const secondStartedBody = secondEvents.find(
      (e) => e.type === "RunStarted",
    )?.body;
    if (secondStartedBody === undefined) {
      throw new Error("missing second RunStarted");
    }
    expect(secondStartedBody["consumedMessageId"]).toBe(second.messageId);

    const secondReply = readStepReply(stepCompletedBody(secondEvents));
    expect(secondReply.startsWith("echo:")).toBe(true);
    expect(secondReply).toContain(SECOND_BODY);
    expect(secondReply).not.toContain(FIRST_BODY);
    expect(secondReply).not.toBe(firstReply);
  });
});

/**
 * Poll until a run id distinct from `firstRunId` appears under `runs/`,
 * returning it. The supervisor processes one run per inbound message,
 * so the second mail produces a second run id.
 */
async function waitForSecondRunId(
  deployEnv: DeployFlowEnv,
  workflowRunRepoId: RepoId,
  firstRunId: string,
  opts: { timeoutMs: number },
): Promise<string> {
  const start = Date.now();
  for (;;) {
    const ids = await listRunIds(deployEnv, workflowRunRepoId);
    const other = ids.find((id) => id !== firstRunId);
    if (other !== undefined) return other;
    if (Date.now() - start > opts.timeoutMs) {
      throw new Error(
        `waitForSecondRunId timed out after ${String(opts.timeoutMs)}ms; saw runIds ${JSON.stringify(ids)}`,
      );
    }
    await new Promise((r) => setTimeout(r, 50));
  }
}

/** Find the single step's `StepCompleted` event body. */
function stepCompletedBody(
  events: { type: string; body: Record<string, unknown> }[],
): Record<string, unknown> {
  const stepCompleted = events.find(
    (e) => e.type === "StepCompleted" && e.body["stepId"] === STEP_ID,
  );
  if (stepCompleted === undefined) {
    throw new Error("missing StepCompleted for the single step");
  }
  return stepCompleted.body;
}

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
