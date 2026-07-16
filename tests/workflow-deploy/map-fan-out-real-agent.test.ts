// Map fan-out real-agent round-trip integration test.
//
// The proof that a top-level `map` fan-out runs a REAL per-item agent
// through the spawned workflow-process child, not the placeholder shape.
// A map iteration runs under a scoped step id `<mapId>[<index>]`, but
// deploy stages the map step's inference source, tool tree, and grants
// once, under the base map id; three deploy-asset lookups resolve the
// scoped id back to the base so every iteration shares those assets.
//
// The only other map test (`per-level-pipeline-real-agents.test.ts`) runs
// the runtime in-memory with a test `buildEnv`, so it never drives the
// sidecar's scoped-id lookup sites -- which is why the scoped-id bug was
// invisible to CI. This test deploys a top-level map through the real hub
// + real sidecar subprocess + mock inference fixture and asserts each
// iteration's committed output is the agent's deterministic reply.
//
// The workflow is deliberately multi-step (a leading `seed` step plus the
// `fanout` map) so the deploy takes the orchestrator's multi-step branch,
// the only path that stages per-step sources into the `STEP_INFERENCE_SOURCES`
// table keyed by base step id -- the table the scoped-id resolver reads.
// A regressed lookup throws on the scoped id, so the run would terminate
// `RunFailed` rather than `RunCompleted`; the RunCompleted assertion is the
// primary regression tripwire.
//
// With `tools: []` / `grants: []` agents, the site this integration test
// drives end to end is the inference-source resolver; the tool-deploy-tree
// and grant scoped-id lookups have their own unit coverage.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import {
  defineWorkflow,
  map,
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
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "map-fan-out-real-agent-1";
const WORKFLOW_RUN_REF = "refs/heads/main";
const SEED_STEP_ID = "seed";
const MAP_STEP_ID = "fanout";
const ITEM_COUNT = 2;

// The mock inference server's reply for an empty tool set: it builds
// `I see these tools: <names>` from the tool names it was handed, so with
// no tools the reply is the stable prefix (trailing whitespace trimmed).
const EXPECTED_REPLY = "I see these tools:";

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

/**
 * Parse a `StepCompleted` event's committed output. The runtime records
 * small step outputs through the blob substrate as an `inline:<json>` ref;
 * recover the raw value by parsing the JSON after the prefix.
 */
function parseInlineOutput(body: Record<string, unknown>): unknown {
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
  return JSON.parse(ref.slice(INLINE_PREFIX.length));
}

/** Recover the agent reply string from a `{ reply, turn }` step output. */
function replyOf(value: unknown): string {
  if (typeof value !== "object" || value === null || !("reply" in value)) {
    throw new Error(
      `step output does not carry a reply field: ${JSON.stringify(value)}`,
    );
  }
  const reply: unknown = value.reply;
  if (typeof reply !== "string") {
    throw new Error(
      `step output reply is not a string: ${JSON.stringify(value)}`,
    );
  }
  return reply;
}

function stepCompletedFor(
  events: readonly WorkflowRunEvent[],
  stepId: string,
): WorkflowRunEvent {
  const matches = events.filter(
    (e) => e.type === "StepCompleted" && e.body["stepId"] === stepId,
  );
  if (matches.length !== 1) {
    throw new Error(
      `expected exactly one StepCompleted for stepId ${stepId}, got ${String(matches.length)}`,
    );
  }
  const only = matches[0];
  if (only === undefined) throw new Error("unreachable");
  return only;
}

describe("map fan-out real-agent round-trip", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("a top-level map runs a real agent per item and commits real output", async () => {
    const seedAgent = defineAgent({
      id: "agent-seed",
      systemPrompt: "You are the seed step agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });
    const mapAgent = defineAgent({
      id: "agent-fanout-item",
      systemPrompt: "You are the map fan-out per-item agent.",
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
        [SEED_STEP_ID]: step({ agent: seedAgent }),
        [MAP_STEP_ID]: map({
          over: { literal: [{ id: "a" }, { id: "b" }] },
          step: step({ agent: mapAgent }),
          after: [SEED_STEP_ID],
        }),
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
            message: `map-fan-out-real-agent test: write workflow repo ${args.workflowRepoId}`,
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

    try {
      await orchestrator.deployWorkflow({
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

    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<map-fan-out-real-agent-1@integration.interchange>",
    });

    const runId = await waitForFirstRunId(env, workflowRunRepoId, {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 20_000,
    });

    // Primary regression tripwire: a regressed scoped-id lookup throws, so
    // the run would terminate RunFailed. Reaching RunCompleted means every
    // map iteration resolved its base step's staged assets and ran.
    const terminal = await waitForWorkflowRunComplete(
      env,
      DEPLOYMENT_ID,
      runId,
      {
        diagnostics: env.sidecarDiagnostics,
        timeoutMs: 30_000,
      },
    );
    expect(terminal.type).toBe("RunCompleted");

    const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);

    // The leading step runs a real agent and completes before the map.
    const seedReply = replyOf(
      parseInlineOutput(stepCompletedFor(events, SEED_STEP_ID).body),
    );
    expect(seedReply).toBe(EXPECTED_REPLY);

    // Each iteration runs under a distinct scoped step id and commits the
    // real agent reply -- not the old placeholder `req.agent.id`.
    for (let i = 0; i < ITEM_COUNT; i += 1) {
      const scopedId = `${MAP_STEP_ID}[${String(i)}]`;
      const reply = replyOf(
        parseInlineOutput(stepCompletedFor(events, scopedId).body),
      );
      expect(reply).toBe(EXPECTED_REPLY);
      expect(reply).not.toBe(mapAgent.id);
    }

    // The map assembles its per-item outputs into one array on the base
    // step's completion; every element is real agent output.
    const mapOutput = parseInlineOutput(
      stepCompletedFor(events, MAP_STEP_ID).body,
    );
    if (!Array.isArray(mapOutput)) {
      throw new Error(
        `map step output is not an array: ${JSON.stringify(mapOutput)}`,
      );
    }
    expect(mapOutput).toHaveLength(ITEM_COUNT);
    for (const itemOutput of mapOutput) {
      const reply = replyOf(itemOutput);
      expect(reply).toBe(EXPECTED_REPLY);
      expect(reply).not.toBe(mapAgent.id);
    }

    // One real inference call per agent invocation: the seed step plus one
    // per map item. An exact count proves two DISTINCT real invocations,
    // closing the "ran the same item twice" failure mode that a constant
    // reply alone could hide.
    expect(env.inference.requests.length).toBe(1 + ITEM_COUNT);
  });
});
