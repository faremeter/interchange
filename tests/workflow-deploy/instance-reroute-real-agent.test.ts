// Instance-reroute real-agent round-trip integration test.
//
// The proof that a single-agent INSTANCE deploy, routed through
// `SessionService.deployInstanceAtHead` (which wraps the harness as a
// one-step workflow via `wrapHarnessAsSingleStepWorkflow` and deploys it at the
// head), runs a REAL agent in the spawned workflow-process child -- against
// the real hub + real sidecar subprocess + mock inference fixture. This
// closes the gap that unit tests (mock spawner) leave open: that the
// walk-only single-step wrap agent instantiates and runs in a real child.
//
// The child never invokes the wrap's walk-only tool factories: it
// materializes tools from the deploy tree and calls the real `createAgent`
// reading only id/systemPrompt/capabilities. A tool-less instance gets an
// empty materialization slot and never hits the bare-createAgent fallback,
// so the wrapped agent runs to a real inference reply.
//
// The mock inference server returns `I see these tools: <names>`; with an
// empty tool set the reply is the stable prefix `I see these tools:`.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig } from "@intx/types/runtime";
import { wrapHarnessAsSingleStepWorkflow } from "@intx/workflow-deploy";
import { defineWorkflow, type WorkflowDefinition } from "@intx/workflow";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import type { RepoId } from "@intx/hub-sessions";

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

const DEPLOYMENT_DOMAIN = "integration.interchange";
// A real instance identity: `ins_` + 32 hex, NOT a `dep_`-prefixed deployment
// id. The head address IS the instance address; the reroute keeps that
// identity rather than deriving a synthetic deployment agent id.
const INSTANCE_ID = `ins_${"b".repeat(32)}`;
const AGENT_ID = "agent-instance-reroute";
const WORKFLOW_RUN_REF = "refs/heads/main";

// The mock inference server's reply for an empty tool set.
const EXPECTED_REPLY = "I see these tools:";

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

describe("instance-reroute real-agent round-trip", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("deployInstanceAtHead runs the wrapped single agent in a real child", async () => {
    const agentAddress = `${INSTANCE_ID}@${DEPLOYMENT_DOMAIN}`;

    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: AGENT_ID,
      tenantId: "tenant-1",
      principalId: "prin_integration-reroute-1",
      agentAddress,
      systemPrompt: "You are the rerouted single-agent instance.",
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
    const deployContent = { systemPrompt: config.systemPrompt };

    // Reconstruct the definition `deployInstanceAtHead` builds internally, for
    // the run-observation handle. Same deterministic inputs -> same wrap.
    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${AGENT_ID}`,
      agent: wrapHarnessAsSingleStepWorkflow({ config, deployContent }),
      trigger: { type: "mail", to: agentAddress },
    });

    // Deploy the instance through the reroute entry point under test.
    const { publicKey } = await env.hub.sessionService.deployInstanceAtHead({
      agentAddress,
      agentId: AGENT_ID,
      instanceId: INSTANCE_ID,
      config,
      deployContent,
    });
    expect(publicKey).toMatch(/^[0-9a-f]{64}$/);

    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveDeploymentId(agentAddress),
    };
    env.registerDeployment({
      deploymentId: INSTANCE_ID,
      workflowDefinition: workflow,
      workflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      mailAddress: agentAddress,
    });

    // The instance's head address is routable from the hub after the deploy.
    expect(env.hub.router.getRoutableAddresses()).toContain(agentAddress);

    const { messageId } = await fireMailTrigger(env, agentAddress, {
      messageId: "<instance-reroute-real-agent-1@integration.interchange>",
    });

    const runId = await waitForFirstRunId(env, workflowRunRepoId, {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 20_000,
    });

    const terminal = await waitForWorkflowRunComplete(env, INSTANCE_ID, runId, {
      timeoutMs: 20_000,
      diagnostics: env.sidecarDiagnostics,
    });
    expect(terminal.type).toBe("RunCompleted");

    const events = await readWorkflowRunEvents(env, INSTANCE_ID, runId);
    const runStartedBody = events.find((e) => e.type === "RunStarted")?.body;
    if (runStartedBody === undefined) throw new Error("missing RunStarted");
    expect(runStartedBody["consumedMessageId"]).toBe(messageId);

    const stepCompleted = events.find((e) => e.type === "StepCompleted");
    if (stepCompleted === undefined) {
      throw new Error("missing StepCompleted for the wrapped single step");
    }

    // The proof: the step output is the REAL agent reply from `agent.send`
    // (the mock provider's deterministic output) -- the wrapped single-step agent
    // instantiated and ran in the child rather than throwing on its walk-only
    // tool factories.
    const reply = readStepReply(stepCompleted.body);
    expect(reply).toBe(EXPECTED_REPLY);

    // The agent's inference call actually reached the mock provider.
    expect(env.inference.requests.length).toBeGreaterThan(0);
  });
});

/**
 * Extract the agent's reply string from a `StepCompleted` event body. A small
 * `{ reply, turn }` output inlines as `inline:<json>`.
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
    throw new Error(`expected an inline output ref, got ${ref}`);
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
