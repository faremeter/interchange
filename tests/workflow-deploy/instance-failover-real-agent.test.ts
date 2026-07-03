// Inference-source failover real-agent round-trip integration test.
//
// The proof that the per-step inference-source failover chain is HONORED at
// runtime by a REAL spawned workflow-process child -- not merely threaded
// through the wire. A single-agent instance is deployed with a two-element
// source chain whose head is a dead provider (HTTP 500) and whose tail is the
// healthy mock inference server. The child's reactor exhausts its mechanical
// retries against the head, fails over forward to the tail, and returns the
// tail's live reply. This closes the gap the unit tests leave open: they prove
// the child builds `env.sources` from the whole chain, but only a real child
// against a real provider pair proves the reactor actually advances across it.
//
// A dead head maps to `retryable` (HTTP 5xx), so the harness retries it a few
// times before the reactor fails over -- exercising the full
// harness-retry-then-reactor-failover path.
//
// The discriminating proof is that the step reply EQUALS the healthy tail's
// output. Run completion alone proves nothing: a total inference failure with
// no failover target ALSO completes -- the agent synthesizes a graceful
// provider-error reply and the run lands `RunCompleted`. The second test here
// pins that contract as a negative control, so the first test's reliance on
// reply-equality (rather than run completion) is grounded rather than assumed.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig } from "@intx/types/runtime";
import { wrapHarnessAsTrivialAgent } from "@intx/workflow-deploy";
import { defineWorkflow, type WorkflowDefinition } from "@intx/workflow";
import { deriveTrivialDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
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
// A real instance identity, distinct from the reroute test's `b`-filled id so
// the two integration tests never collide on a substrate slug.
const INSTANCE_ID = `ins_${"c".repeat(32)}`;
const AGENT_ID = "agent-instance-failover";
const WORKFLOW_RUN_REF = "refs/heads/main";

// The mock inference server's reply for an empty tool set.
const EXPECTED_REPLY = "I see these tools:";

let env: DeployFlowEnv;

// The dead head source. Always returns HTTP 500 (a `retryable` category), so
// the child's reactor fails over off it. `startDeployFlowEnv().teardown()` does
// not own this server, so it is started and stopped symmetrically here.
let headRequests = 0;
let deadHead: ReturnType<typeof Bun.serve>;

beforeAll(async () => {
  env = await startDeployFlowEnv();
  deadHead = Bun.serve({
    port: 0,
    fetch() {
      headRequests += 1;
      return new Response("upstream boom", { status: 500 });
    },
  });
});

afterAll(async () => {
  await env.teardown();
  await deadHead.stop(true);
});

describe("instance inference-source failover real-agent round-trip", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("the child fails over from a dead head source to the healthy tail", async () => {
    const agentAddress = `${INSTANCE_ID}@${DEPLOYMENT_DOMAIN}`;

    // A two-element failover chain: element 0 is the dead head (default), the
    // tail is the healthy mock inference server. The head must be element 0 and
    // equal `defaultSource` -- the reactor resolves its initial source by id and
    // fails over forward, so the deploy asserts that invariant.
    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: AGENT_ID,
      tenantId: "tenant-1",
      principalId: "prin_integration-failover-1",
      agentAddress,
      systemPrompt: "You are the failover single-agent instance.",
      tools: [],
      grants: [],
      sources: [
        {
          id: "anthropic:dead-head",
          provider: "anthropic",
          baseURL: `http://localhost:${deadHead.port}`,
          apiKey: "sk-dead",
          model: "mock-model",
        },
        {
          id: "anthropic:mock-model",
          provider: "anthropic",
          baseURL: `http://localhost:${env.inference.server.port}`,
          apiKey: "sk-mock",
          model: "mock-model",
        },
      ],
      defaultSource: "anthropic:dead-head",
    };
    const deployContent = { systemPrompt: config.systemPrompt };

    // Reconstruct the definition `deployInstanceAtHead` builds internally, for
    // the run-observation handle. Same deterministic inputs -> same wrap.
    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${AGENT_ID}`,
      agent: wrapHarnessAsTrivialAgent({ config, deployContent }),
      trigger: { type: "mail", to: agentAddress },
    });

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
      id: deriveTrivialDeploymentId(agentAddress),
    };
    env.registerDeployment({
      deploymentId: INSTANCE_ID,
      workflowDefinition: workflow,
      workflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      mailAddress: agentAddress,
    });

    expect(env.hub.router.getRoutableAddresses()).toContain(agentAddress);

    await fireMailTrigger(env, agentAddress, {
      messageId: "<instance-failover-real-agent-1@integration.interchange>",
    });

    const runId = await waitForFirstRunId(env, workflowRunRepoId, {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 20_000,
    });

    const terminal = await waitForWorkflowRunComplete(env, INSTANCE_ID, runId, {
      timeoutMs: 20_000,
      diagnostics: env.sidecarDiagnostics,
    });
    // A sanity check, not the proof: the run reaches a normal terminal. This
    // does NOT discriminate working from broken failover on its own -- the
    // negative-control test below shows a total inference failure also lands
    // `RunCompleted`. The reply-equality assertion is what proves failover.
    expect(terminal.type).toBe("RunCompleted");

    const events = await readWorkflowRunEvents(env, INSTANCE_ID, runId);
    const stepCompleted = events.find((e) => e.type === "StepCompleted");
    if (stepCompleted === undefined) {
      throw new Error("missing StepCompleted for the wrapped single step");
    }

    // The proof of failover: the reply EQUALS the healthy tail's output. The
    // dead head returns nothing but 500s, so it cannot produce this string; a
    // broken failover would instead surface the agent's synthesized
    // provider-error reply (the negative control below).
    const reply = readStepReply(stepCompleted.body);
    expect(reply).toBe(EXPECTED_REPLY);

    // The head was tried first (the default pins to element 0 every cycle), then
    // the healthy tail actually served the reply. Both counts are `>= 1` rather
    // than exact: the precise retry count is the harness retry policy's contract,
    // not this cross-process test's.
    expect(headRequests).toBeGreaterThanOrEqual(1);
    expect(env.inference.requests.length).toBeGreaterThanOrEqual(1);
  });

  test("a chain with no healthy source completes with a synthesized error reply", async () => {
    // Negative control grounding the test above: with the dead head as the SOLE
    // source there is nothing to fail over to, yet the run still lands
    // `RunCompleted` -- the agent synthesizes a graceful provider-error reply
    // rather than failing the run. This is why the failover test cannot lean on
    // the terminal type and instead proves failover through reply-equality.
    const soleDeadInstanceId = `ins_${"d".repeat(32)}`;
    const soleDeadAgentId = "agent-instance-failover-negctl";
    const agentAddress = `${soleDeadInstanceId}@${DEPLOYMENT_DOMAIN}`;

    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: soleDeadAgentId,
      tenantId: "tenant-1",
      principalId: "prin_integration-failover-negctl-1",
      agentAddress,
      systemPrompt: "You are the failover negative-control instance.",
      tools: [],
      grants: [],
      sources: [
        {
          id: "anthropic:dead-head",
          provider: "anthropic",
          baseURL: `http://localhost:${deadHead.port}`,
          apiKey: "sk-dead",
          model: "mock-model",
        },
      ],
      defaultSource: "anthropic:dead-head",
    };
    const deployContent = { systemPrompt: config.systemPrompt };

    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${soleDeadAgentId}`,
      agent: wrapHarnessAsTrivialAgent({ config, deployContent }),
      trigger: { type: "mail", to: agentAddress },
    });

    await env.hub.sessionService.deployInstanceAtHead({
      agentAddress,
      agentId: soleDeadAgentId,
      instanceId: soleDeadInstanceId,
      config,
      deployContent,
    });

    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveTrivialDeploymentId(agentAddress),
    };
    env.registerDeployment({
      deploymentId: soleDeadInstanceId,
      workflowDefinition: workflow,
      workflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      mailAddress: agentAddress,
    });

    await fireMailTrigger(env, agentAddress, {
      messageId: "<instance-failover-negctl-1@integration.interchange>",
    });

    const runId = await waitForFirstRunId(env, workflowRunRepoId, {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 20_000,
    });

    const terminal = await waitForWorkflowRunComplete(
      env,
      soleDeadInstanceId,
      runId,
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
    );
    // Total inference failure still completes the run.
    expect(terminal.type).toBe("RunCompleted");

    const events = await readWorkflowRunEvents(env, soleDeadInstanceId, runId);
    const stepCompleted = events.find((e) => e.type === "StepCompleted");
    if (stepCompleted === undefined) {
      throw new Error("missing StepCompleted for the sole-dead-source step");
    }

    // The reply is the synthesized error, NOT the healthy tail's output --
    // exactly the outcome the failover test's reply-equality assertion rules out.
    const reply = readStepReply(stepCompleted.body);
    expect(reply).not.toBe(EXPECTED_REPLY);
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
