// A childWorkflow child agent INVOKES a real tool, and the invocation lands in
// its turn (INTR-310).
//
// The sibling `child-workflow-roundtrip.test.ts` proves a child EXPOSES an
// inline tool (the mock lists its name). This proves the stronger property: the
// child's real agent actually CALLS the tool and feeds the result back into a
// follow-up turn -- the real `tool_use` -> execute -> `tool_result` -> reply
// round-trip, the same loop a top-level source step runs. The mock is
// configured to drive a `tool_use` on the first request that exposes the tool,
// so the child runs the inline `mail_send` tool for real and re-inferences with
// its result.
//
// The env runs exactly one workflow: a parent whose leading step is toolless
// and a child step that carries the tool. The parent step can produce no
// `tool_result` (it has no tools), so a `tool_result` in ANY captured inference
// request can only have originated in the child's tool execution.

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

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  type DeployFlowEnv,
  type InferenceRequest,
} from "../hub-agent/lib/deploy-flow-env";
import { childWorkflowEntry } from "./fixtures/child-workflow";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const PARENT_DEPLOYMENT_ID = "run_child-workflow-tool-invoke-parent-1";
const CHILD_DEPLOYMENT_ID = "run_child-workflow-tool-invoke-child-1";
const PARENT_WORKFLOW_ID = `wf_${PARENT_DEPLOYMENT_ID}`;
const CHILD_WORKFLOW_ID = `wf_${CHILD_DEPLOYMENT_ID}`;

const TENANT_ID = "tnt_child_workflow_tool_invoke";
const CALLER_PRINCIPAL_ID = "prn_child_workflow_tool_invoke";
const DEFINITION_ASSET_ID = "ast_child_workflow_tool_invoke_wf";

// Whether an inference request's history carries a `tool_result` block -- the
// proof that the agent executed a tool and re-inferenced with its output.
function requestHasToolResult(req: InferenceRequest): boolean {
  return (req.messages ?? []).some(
    (message) =>
      Array.isArray(message.content) &&
      message.content.some((block) => block.type === "tool_result"),
  );
}

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
  await seedAsset(h.db, {
    id: DEFINITION_ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "child-workflow-tool-invoke-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  // Drive the inline `mail_send` tool on the first request that exposes it, on
  // every run. The "fs" variant writes a file under the step workdir with no
  // env requirement, so the call runs for real in the child.
  env = await startDeployFlowEnv({
    inferenceToolCall: {
      toolName: MAIL_TOOL_NAME,
      input: { to: "child tool ran", body: "child-invoked.txt" },
    },
    inferenceToolCallEachRun: true,
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "a childWorkflow child invokes a real tool",
  () => {
    test("the child's agent calls its inline tool and the result lands in a turn", async () => {
      const parentMailAddress = deriveRunAddress({
        runId: PARENT_DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });
      const childMailAddress = deriveRunAddress({
        runId: CHILD_DEPLOYMENT_ID,
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
        agentId: `${PARENT_DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_integration-1",
        agentAddress: parentMailAddress,
        systemPrompt: "Fallback prompt (overridden per step).",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${parentMailAddress}`,
        `mail.address:${childMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
        // The child step declares the inline tool, so the deploy walk folds its
        // grant into the parent step; the operator must approve it.
        `tool:${MAIL_TOOL_NAME}`,
      ]);

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: childWorkflowEntry({
          workflowId: PARENT_WORKFLOW_ID,
          address: parentMailAddress,
          steps: [
            {
              stepId: "step1",
              agentId: "agent-tool-invoke-parent-step1",
              systemPrompt:
                "You are the tool-invoke parent's first step agent.",
            },
          ],
          spawns: [
            {
              stepId: "spawn",
              after: ["step1"],
              child: {
                workflowId: CHILD_WORKFLOW_ID,
                address: childMailAddress,
                steps: [
                  {
                    stepId: "childStep",
                    agentId: "agent-tool-invoke-child-step",
                    systemPrompt: "You are the tool-invoke child's step agent.",
                    tool: "fs",
                  },
                ],
              },
            },
          ],
        }),
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: PARENT_DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: parentMailAddress,
        approvals: operatorApprovals,
        config,
        sources: { step1: [inferenceSource], spawn: [inferenceSource] },
      });
      expect(handle.publicKey).toBeTruthy();

      await waitFor(
        () => env.hub.router.getRoutableAddresses().includes(parentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, parentMailAddress, {
        messageId: "<child-workflow-tool-invoke-1@integration.interchange>",
      });

      const parentRunId = await waitForFirstRunId(
        env,
        handle.workflowRunRepoId,
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 20_000 },
      );

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
      const spawnedEvent = parentEvents.find((e) => e.type === "ChildSpawned");
      if (spawnedEvent === undefined) throw new Error("unreachable");
      const childRunId = spawnedEvent.body["childRunId"];
      if (typeof childRunId !== "string") {
        throw new Error(
          `ChildSpawned missing string childRunId; got ${typeof childRunId}`,
        );
      }

      // Wait for the child step to complete: it completes only after the tool
      // call round-trip lands (tool_use -> execute -> tool_result -> reply).
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            PARENT_DEPLOYMENT_ID,
            childRunId,
          );
          return events.some(
            (e) =>
              e.type === "StepCompleted" && e.body["stepId"] === "childStep",
          );
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
      );

      // The child's agent executed the tool and re-inferenced with its result:
      // a captured request carries a tool_result. The parent step is toolless,
      // so this can only be the child's tool invocation landing in a turn.
      const invoked = env.inference.requests.some(requestHasToolResult);
      expect(invoked).toBe(true);
    }, 180_000);
  },
);
