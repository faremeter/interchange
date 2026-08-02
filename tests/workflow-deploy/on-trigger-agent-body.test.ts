// onTrigger body real-agent reachability gate (INTR-310).
//
// The proof that a deployed onTrigger section's body runs a REAL agent step,
// not the ChildStepNotImplementedError stub. An onTrigger body is a child run
// spawned per section event; body agent-step execution was stubbed until the
// cold, tool-less, body-only invoker landed. This drives that path end-to-end
// on the real deploy stack: a section subscribed to the deployment mail address
// whose body is a single tool-less agent step. Firing the trigger spawns the
// body child, whose agent step runs through the body invoker against the mock
// inference fixture and commits the model's deterministic reply as its output.
//
// The mock inference server returns `I see these tools: <names>` from the tool
// names it was handed; a tool-less agent yields the stable prefix `I see these
// tools:`. That deterministic reply is the seam the real agent is driven
// against (real inference in CI is impractical). The reachability assertion is
// three-fold: the body child completes, its agent step's committed output is
// the real reply (NOT the agent id -- the old stub value -- and NOT a
// ChildStepNotImplementedError failure), and the mock provider actually
// received an inference request, so the reply is real model output.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow-process child driving `runOnTrigger` with the production
// suspendable-child seam, and -- new here -- a real agent inside the body via
// the body-only invoker. Body agents are tool-less by the deploy guard, so no
// tool trees are staged; this exercises exactly the reachable body-agent path.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import {
  defineWorkflow,
  onTrigger,
  step,
  type WorkflowDefinition,
} from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveDeploymentAddress,
  type ApprovalSet,
  type DeploySingleStepFn,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import type { RepoId, WorkflowRunHubPrincipal } from "@intx/hub-sessions";
import { DEFAULT_ASSET_REF } from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  fireMailTrigger,
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "on-trigger-agent-body-1";
const WORKFLOW_RUN_REF = "refs/heads/main";
const SECTION_ID = "section";
const BODY_STEP_ID = "work";
const BODY_CHILD_RUN_ID = `${SECTION_ID}__0`;

// The mock inference server's reply for a tool-less agent (no tool names).
const EXPECTED_REPLY = "I see these tools:";

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

/**
 * The container run is the single run under the deployment's workflow-run repo
 * that is NOT a body child (body children are `${SECTION_ID}__<n>`).
 */
async function findContainerRunId(
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  const ids = await listRunIds(env, workflowRunRepoId);
  return ids.find((id) => !id.startsWith(`${SECTION_ID}__`));
}

const hasChildCompleted = (
  events: { type: string; body: Record<string, unknown> }[],
  childRunId: string,
): boolean =>
  events.some(
    (e) => e.type === "ChildCompleted" && e.body["childRunId"] === childRunId,
  );

describe("onTrigger body runs a real agent step through the body invoker", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("a tool-less agent body runs for real and commits the model reply as its step output", async () => {
    // A tool-less body agent: the deploy guard requires body agents carry no
    // tools, and the mock provider echoes the (empty) tool set.
    const bodyAgent = defineAgent({
      id: "agent-body-work",
      systemPrompt: "You are the onTrigger body agent.",
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

    const body: WorkflowDefinition = defineWorkflow({
      id: "authored-agent-body",
      trigger: { type: "manual" },
      steps: { [BODY_STEP_ID]: step({ agent: bodyAgent }) },
    });

    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${DEPLOYMENT_ID}`,
      trigger: { type: "mail", to: deploymentMailAddress },
      steps: {
        [SECTION_ID]: onTrigger({
          on: { type: "mail", to: deploymentMailAddress },
          body,
        }),
      },
    });

    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `ins_${DEPLOYMENT_ID}`,
      tenantId: "tenant-1",
      principalId: `prin_${DEPLOYMENT_ID}`,
      agentAddress: deploymentMailAddress,
      systemPrompt: "Fallback prompt (overridden per step by the orchestrator)",
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
    };

    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${deploymentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

    const launchSession: LaunchSessionFn = async (orchestratorParams) => {
      await env.hub.sessionService.stageWorkflowStep({
        agentAddress: orchestratorParams.agentAddress,
        agentId: orchestratorParams.agentId,
        instanceId: orchestratorParams.instanceId,
        config: orchestratorParams.config,
        deployContent: toLaunchDeployContent(orchestratorParams.deployContent),
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
        ...(params.referencedDefinitions !== undefined
          ? {
              referencedDefinitions: params.referencedDefinitions.map((b) => ({
                definition: {
                  id: b.definition.id,
                  triggers: [...b.definition.triggers],
                  stepOrder: [...b.definition.stepOrder],
                  steps: b.definition.steps as Record<string, unknown>,
                  ...(b.definition.state !== undefined
                    ? { state: b.definition.state }
                    : {}),
                },
                sources: b.sources,
              })),
            }
          : {}),
      });

    const deploySingleStepAtHead: DeploySingleStepFn = (params) =>
      env.hub.sessionService.deploySingleStepAtHead(params);

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
          {
            files,
            message: `on-trigger agent-body test: write workflow repo ${args.workflowRepoId}`,
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
    expect(result.publicKey).toBeTruthy();

    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveDeploymentId(deploymentMailAddress),
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

    // Fire the event; the section spawns the body child, whose agent step runs
    // through the body invoker and completes -- no signal, no gate, just the
    // agent. Wait for the body child to complete on the container's log.
    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: `<${DEPLOYMENT_ID}@integration.interchange>`,
      content: "trigger the agent body",
    });

    const containerRunId = await (async () => {
      await waitFor(
        async () => (await findContainerRunId(workflowRunRepoId)) !== undefined,
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
      );
      const id = await findContainerRunId(workflowRunRepoId);
      if (id === undefined) throw new Error("no container run");
      return id;
    })();

    await waitFor(
      async () => {
        const events = await readWorkflowRunEvents(
          env,
          DEPLOYMENT_ID,
          containerRunId,
        );
        return hasChildCompleted(events, BODY_CHILD_RUN_ID);
      },
      { diagnostics: env.sidecarDiagnostics, timeoutMs: 60_000 },
    );

    // ---- The reachability proof: read the BODY child's own run log ----
    const bodyEvents = await readWorkflowRunEvents(
      env,
      DEPLOYMENT_ID,
      BODY_CHILD_RUN_ID,
    );
    const bodyTypes = bodyEvents.map((e) => e.type);

    // The body agent step ran and committed output -- it was neither the stub
    // failure nor an unrun step.
    expect(bodyTypes).not.toContain("StepFailed");
    const stepCompleted = bodyEvents.find(
      (e) => e.type === "StepCompleted" && e.body["stepId"] === BODY_STEP_ID,
    );
    if (stepCompleted === undefined) {
      throw new Error(
        `missing StepCompleted for the body agent step; body events: ${bodyTypes.join(", ")}`,
      );
    }

    // The output is the REAL agent reply from `agent.send` (the mock provider's
    // deterministic output), NOT the old stub value `req.agent.id`.
    const reply = readStepReply(stepCompleted.body);
    expect(reply).toBe(EXPECTED_REPLY);
    expect(reply).not.toBe(bodyAgent.id);

    // The body agent's inference call actually reached the mock provider, so the
    // reply is real model output rather than a synthesized constant.
    expect(env.inference.requests.length).toBeGreaterThan(0);

    // The section re-arms on its input park for the next event and never
    // self-completes -- the body running a real agent didn't disturb the
    // long-lived container.
    const containerEvents = await readWorkflowRunEvents(
      env,
      DEPLOYMENT_ID,
      containerRunId,
    );
    const containerTypes = containerEvents.map((e) => e.type);
    expect(hasChildCompleted(containerEvents, BODY_CHILD_RUN_ID)).toBe(true);
    expect(containerTypes.filter((t) => t === "RunStarted").length).toBe(1);
    expect(containerTypes).not.toContain("RunCompleted");
    expect(containerTypes).not.toContain("RunFailed");
    expect(containerTypes).not.toContain("RunCancelled");
  }, 120_000);
});

/**
 * Extract the agent's reply string from a `StepCompleted` event body. A small
 * `{ reply, turn }` output inlines as `inline:<json>`, so the reply is
 * recovered by parsing the JSON after the `inline:` prefix.
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
