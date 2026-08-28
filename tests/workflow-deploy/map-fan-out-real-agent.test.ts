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
// invisible to CI. This test deploys a top-level map BY SOURCE-REF (bundle a
// source entry module into a hub asset, probe it, approve+freeze it against a
// real DB, deploy the source-ref frame) through the real hub + real sidecar
// subprocess + mock inference fixture and asserts each iteration's committed
// output is the agent's deterministic reply.
//
// The workflow is deliberately multi-step (a leading `seed` step plus the
// `fanout` map) so the deploy stages per-step assets at a per-step address. A
// regressed lookup throws (or materializes nothing) on the scoped id, so the
// run would terminate `RunFailed` or the reply would omit the expected
// content; the assertions below are the regression tripwires.
//
// Two cases share the same fixture shape:
//   - No tool: each iteration's reply is the empty-tool-set prefix, guarding
//     the inference-source resolver (a regressed lookup throws -> RunFailed).
//   - An inline tool: the per-item agent carries the inline `mail_send` tool,
//     so each iteration's reply lists the tool, guarding the tool-deploy-tree
//     base resolution (a regressed lookup reads the unstaged scoped address ->
//     empty tools -> the tool is absent from the reply).
// The grant scoped-id lookup keeps its unit coverage
// (`credentials-backed-authorize.test.ts`).

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";
import type { RepoId } from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import { mapFanOutEntry } from "./fixtures/map-fan-out";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_map-fan-out-real-agent-1";
const TOOL_DEPLOYMENT_ID = "run_map-fan-out-real-agent-tool-1";
const SEED_STEP_ID = "seed";
const MAP_STEP_ID = "fanout";
const ITEM_COUNT = 2;
const MAP_ITEM_AGENT_ID = "map-fan-out-item-agent";

// The mock inference server's reply for an empty tool set: it builds
// `I see these tools: <names>` from the tool names it was handed, so with
// no tools the reply is the stable prefix (trailing whitespace trimmed).
const EXPECTED_REPLY = "I see these tools:";

// The inline tool the item agent carries in the tool case; the mock echoes its
// exposed name into the reply.
const TOOL_NAME = MAIL_TOOL_NAME;

// The definition's own tenant, the caller principal that creates the
// definition assets, and the two `workflow`-kind assets the frozen definitions
// project over (one per deploy). The install/approve freeze and the anchor
// `workflow_run` insert both write against these, so they must exist in the
// real DB before each deploy runs.
const TENANT_ID = "tnt_map_fan_out";
const CALLER_PRINCIPAL_ID = "prn_map_fan_out";
const DEFINITION_ASSET_ID = "ast_map_fan_out_wf";
const TOOL_DEFINITION_ASSET_ID = "ast_map_fan_out_tool_wf";

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  h = await createTestDb();
  await h.db.insert(tenantTable).values({
    id: TENANT_ID,
    name: TENANT_ID,
    slug: TENANT_ID,
    domain: DEPLOYMENT_DOMAIN,
    parentId: null,
  });
  await seedPrincipal(h.db, {
    id: CALLER_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
  });
  for (const id of [DEFINITION_ASSET_ID, TOOL_DEFINITION_ASSET_ID]) {
    await seedAsset(h.db, {
      id,
      tenantId: TENANT_ID,
      kind: "workflow",
      name: id,
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });
  }

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
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

/** The reply of a map iteration's committed `StepCompleted` output. */
function iterationReply(
  events: readonly WorkflowRunEvent[],
  index: number,
): string {
  const scopedId = `${MAP_STEP_ID}[${String(index)}]`;
  return replyOf(parseInlineOutput(stepCompletedFor(events, scopedId).body));
}

/**
 * Deploy the multi-step `{ seed, fanout }` map workflow BY SOURCE-REF through
 * the real hub + sidecar, fire the mail trigger, wait for the run to complete,
 * and return the committed run events plus the per-item agent id and the number
 * of inference requests this run drove (a delta, since the mock's request log
 * is cumulative across the shared fixture). Optionally carries the inline tool
 * on the per-item agent.
 */
async function deployAndRunMap(opts: {
  anchorRunId: string;
  definitionAssetId: string;
  withTool?: boolean;
}): Promise<{
  events: readonly WorkflowRunEvent[];
  mapAgentId: string;
  inferenceRequestCount: number;
}> {
  const requestsBefore = env.inference.requests.length;
  const withTool = opts.withTool ?? false;

  const deploymentMailAddress = deriveRunAddress({
    runId: opts.anchorRunId,
    domain: DEPLOYMENT_DOMAIN,
  });

  const inferenceSource: InferenceSource = {
    id: "anthropic:mock-model",
    provider: "anthropic",
    baseURL: `http://localhost:${env.inference.server.port}`,
    apiKey: "sk-mock",
    model: "mock-model",
  };

  const config: HarnessConfig = {
    sessionId: SESSION_ID,
    agentId: `${opts.anchorRunId}`,
    tenantId: "tenant-1",
    principalId: "prin_integration-1",
    agentAddress: deploymentMailAddress,
    systemPrompt: "Fallback prompt (overridden per step by the definition)",
    tools: [],
    grants: [],
    sources: [inferenceSource],
    defaultSource: "anthropic:mock-model",
  };

  const operatorApprovals: ApprovalSet = new Set<string>([
    "inference.source:anthropic:mock-model",
    "director:@intx/agent/default",
    `mail.address:${deploymentMailAddress}`,
    `mail.send:${DEPLOYMENT_DOMAIN}`,
    ...(withTool ? [`tool:${TOOL_NAME}`] : []),
  ]);

  const entryModule = mapFanOutEntry({
    address: deploymentMailAddress,
    seedStepId: SEED_STEP_ID,
    mapStepId: MAP_STEP_ID,
    seedSystemPrompt: "You are the seed step agent.",
    itemSystemPrompt: "You are the map fan-out per-item agent.",
    itemCount: ITEM_COUNT,
    itemAgentId: MAP_ITEM_AGENT_ID,
    workflowId: `wf_${opts.anchorRunId}`,
    ...(withTool ? { withTool: true } : {}),
  });

  const handle = await deployWorkflowSourceForTest(env, {
    entryModule,
    db: h.db,
    tenantId: TENANT_ID,
    definitionAssetId: opts.definitionAssetId,
    anchorRunId: opts.anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: deploymentMailAddress,
    approvals: operatorApprovals,
    config,
    sources: {
      [SEED_STEP_ID]: [inferenceSource],
      [MAP_STEP_ID]: [inferenceSource],
    },
  });
  expect(handle.publicKey).toBeTruthy();

  const workflowRunRepoId: RepoId = handle.workflowRunRepoId;

  // The source-ref frame round-trips through the real sidecar subprocess, so
  // routability is asynchronous. Wait for it before firing the trigger.
  await waitFor(
    () => env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
    { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
  );

  await fireMailTrigger(env, deploymentMailAddress, {
    messageId: `<${opts.anchorRunId}@integration.interchange>`,
  });

  const runId = await waitForFirstRunId(env, workflowRunRepoId, {
    diagnostics: env.sidecarDiagnostics,
    timeoutMs: 20_000,
  });

  // Primary regression tripwire: a regressed scoped-id lookup throws, so the
  // run would terminate RunFailed. Reaching RunCompleted means every map
  // iteration resolved its base step's staged assets and ran.
  const terminal = await waitForWorkflowRunComplete(
    env,
    opts.anchorRunId,
    runId,
    {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 30_000,
    },
  );
  expect(terminal.type).toBe("RunCompleted");

  const events = await readWorkflowRunEvents(env, opts.anchorRunId, runId);
  return {
    events,
    mapAgentId: MAP_ITEM_AGENT_ID,
    inferenceRequestCount: env.inference.requests.length - requestsBefore,
  };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "map fan-out real-agent round-trip",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a top-level map runs a real agent per item and commits real output", async () => {
      const { events, mapAgentId, inferenceRequestCount } =
        await deployAndRunMap({
          anchorRunId: DEPLOYMENT_ID,
          definitionAssetId: DEFINITION_ASSET_ID,
        });

      // The leading step runs a real agent and completes before the map.
      const seedReply = replyOf(
        parseInlineOutput(stepCompletedFor(events, SEED_STEP_ID).body),
      );
      expect(seedReply).toBe(EXPECTED_REPLY);

      // Each iteration runs under a distinct scoped step id and commits the
      // real agent reply -- not the old placeholder `req.agent.id`.
      for (let i = 0; i < ITEM_COUNT; i += 1) {
        const reply = iterationReply(events, i);
        expect(reply).toBe(EXPECTED_REPLY);
        expect(reply).not.toBe(mapAgentId);
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
        expect(reply).not.toBe(mapAgentId);
      }

      // One real inference call per agent invocation: the seed step plus one
      // per map item. An exact count proves two DISTINCT real invocations,
      // closing the "ran the same item twice" failure mode that a constant
      // reply alone could hide.
      expect(inferenceRequestCount).toBe(1 + ITEM_COUNT);
    });

    test("each map iteration materializes an inline tool via base-step resolution", async () => {
      // The per-item agent carries the inline `mail_send` tool, staged in the
      // map step's deploy tree keyed by the BASE step id. Each iteration
      // resolves its scoped id `fanout[i]` back to the base to find that tree,
      // materializes the tool, and exposes it to inference -- the mock echoes
      // the exposed tool names into the reply. Without the tool-deploy-tree
      // base resolution the iteration reads the unstaged scoped address,
      // materializes no tools, and the reply omits the tool name.
      const { events, inferenceRequestCount } = await deployAndRunMap({
        anchorRunId: TOOL_DEPLOYMENT_ID,
        definitionAssetId: TOOL_DEFINITION_ASSET_ID,
        withTool: true,
      });

      for (let i = 0; i < ITEM_COUNT; i += 1) {
        expect(iterationReply(events, i)).toContain(TOOL_NAME);
      }

      expect(inferenceRequestCount).toBe(1 + ITEM_COUNT);
    });
  },
);
