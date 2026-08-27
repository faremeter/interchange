// Parent -> child workflow integration test.
//
// Deploys a parent workflow with a `childWorkflow` primitive BY SOURCE-REF
// (bundle a source entry module into a hub asset, probe it, approve+freeze it
// against a real DB, deploy the source-ref frame) against the real hub +
// sidecar subprocess + mock inference fixture, fires the parent's mail trigger,
// and asserts the canonical parent/child event chain materializes in the
// deployment's workflow-run repo. Child events land under
// `runs/<childRunId>/events/` in the same workflow-run repo as the parent's
// `runs/<parentRunId>/events/` -- the sub-namespace shape the in-process
// `runChild` recursion produces.
//
// The child workflow is embedded inline in the parent (an owned import), so
// only the parent is deployed. The deploy step lifts the inline child to an
// internal ref (`<parentWorkflowId>__<stepId>`); at child boot the host lifts
// the same inline child into an in-memory map, and the terminal spawn adapter
// (`createInMemorySpawnChild`) resolves the ref from that map with no on-disk
// read. The in-process `runChild` (`createSidecarRunChild`) builds a
// per-childRunId `WorkflowRuntimeEnv` and drives the child's `runtimeRun` to
// terminal status, settling the parent's spawn step with the child's terminal
// status.

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
  type DeployFlowEnv,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import {
  childWorkflowEntry,
  type ChildWorkflowFixtureParams,
} from "./fixtures/child-workflow";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";

// A toolless child agent's reply from the mock inference provider: the mock
// renders "I see these tools:" plus the (empty) tool-name list.
const EXPECTED_CHILD_REPLY = "I see these tools:";

/**
 * Extract a child (or grandchild) step's real agent reply from its
 * `StepCompleted` event body. A small `{ reply, turn }` output inlines as
 * `inline:<json>`, so the reply is recovered by parsing the JSON after the
 * `inline:` prefix -- the same shape a top-level or onTrigger-body step
 * produces, proving the child ran a real agent rather than a fabricated stub.
 */
function readStepReply(event: WorkflowRunEvent | undefined): string {
  if (event === undefined) {
    throw new Error("unreachable: missing StepCompleted event");
  }
  const output = event.body["output"];
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

const DEPLOYMENT_DOMAIN = "integration.interchange";
const PARENT_DEPLOYMENT_ID = "run_child-workflow-parent-1";
const CHILD_DEPLOYMENT_ID = "run_child-workflow-child-1";
const CHILD_WORKFLOW_ID = `wf_${CHILD_DEPLOYMENT_ID}`;
const PARENT_WORKFLOW_ID = `wf_${PARENT_DEPLOYMENT_ID}`;

// Grandchild-depth deployment ids. Each rung embeds the next inline: the
// parent embeds the child, the child embeds the grandchild. Only the parent
// is deployed; each rung lifts its own inline child to an in-memory map when
// it runs. The sub-namespace scoping under `runs/<runId>/` should isolate
// every rung in the parent's workflow-run repo without leakage.
const NESTED_PARENT_DEPLOYMENT_ID = "run_child-workflow-nested-parent-1";
const NESTED_CHILD_DEPLOYMENT_ID = "run_child-workflow-nested-child-1";
const NESTED_GRANDCHILD_DEPLOYMENT_ID =
  "run_child-workflow-nested-grandchild-1";
const NESTED_PARENT_WORKFLOW_ID = `wf_${NESTED_PARENT_DEPLOYMENT_ID}`;
const NESTED_CHILD_WORKFLOW_ID = `wf_${NESTED_CHILD_DEPLOYMENT_ID}`;
const NESTED_GRANDCHILD_WORKFLOW_ID = `wf_${NESTED_GRANDCHILD_DEPLOYMENT_ID}`;

// Siblings-fanout deployment ids. The parent carries 5 `childWorkflow`
// primitives in `stepOrder`, each embedding one of 5 distinct child
// definitions inline; each is lifted to its own `<parentId>__spawnN` ref.
const SIBLINGS_PARENT_DEPLOYMENT_ID = "run_child-workflow-siblings-parent-1";
const SIBLINGS_CHILD_COUNT = 5;
const SIBLINGS_CHILD_DEPLOYMENT_IDS: readonly string[] = Array.from(
  { length: SIBLINGS_CHILD_COUNT },
  (_unused, i) => `run_child-workflow-sibling-${(i + 1).toString()}`,
);
const SIBLINGS_PARENT_WORKFLOW_ID = `wf_${SIBLINGS_PARENT_DEPLOYMENT_ID}`;
const SIBLINGS_CHILD_WORKFLOW_IDS: readonly string[] =
  SIBLINGS_CHILD_DEPLOYMENT_IDS.map((id) => `wf_${id}`);

// Tool-bearing-child deployment ids. The parent's child step carries the
// inline `mail_send` tool, so the child runs a real TOOL-BEARING agent.
const TOOL_PARENT_DEPLOYMENT_ID = "run_child-workflow-tool-parent-1";
const TOOL_CHILD_DEPLOYMENT_ID = "run_child-workflow-tool-child-1";
const TOOL_PARENT_WORKFLOW_ID = `wf_${TOOL_PARENT_DEPLOYMENT_ID}`;
const TOOL_CHILD_WORKFLOW_ID = `wf_${TOOL_CHILD_DEPLOYMENT_ID}`;

const TENANT_ID = "tnt_child_workflow";
const CALLER_PRINCIPAL_ID = "prn_child_workflow";
const SINGLE_DEFINITION_ASSET_ID = "ast_child_workflow_single_wf";
const NESTED_DEFINITION_ASSET_ID = "ast_child_workflow_nested_wf";
const SIBLINGS_DEFINITION_ASSET_ID = "ast_child_workflow_siblings_wf";
const TOOL_DEFINITION_ASSET_ID = "ast_child_workflow_tool_wf";

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
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
  for (const id of [
    SINGLE_DEFINITION_ASSET_ID,
    NESTED_DEFINITION_ASSET_ID,
    SIBLINGS_DEFINITION_ASSET_ID,
    TOOL_DEFINITION_ASSET_ID,
  ]) {
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

function makeConfig(anchorRunId: string, address: string): HarnessConfig {
  return {
    sessionId: SESSION_ID,
    agentId: `${anchorRunId}`,
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
        baseURL: `http://localhost:${env.inference.server.port}`,
        apiKey: "sk-mock",
        model: "mock-model",
      },
    ],
    defaultSource: "anthropic:mock-model",
  };
}

/**
 * Deploy a parent workflow (with inline children) BY SOURCE-REF and wait for
 * its address to become routable. `topLevelStepIds` covers the parent's own
 * `stepOrder` so the deploy frame's per-step source map is complete (inline
 * child steps ride the closure, not the top-level source map).
 */
async function deployParent(opts: {
  anchorRunId: string;
  definitionAssetId: string;
  fixture: ChildWorkflowFixtureParams;
  topLevelStepIds: readonly string[];
  approvals: ApprovalSet;
}): Promise<{ workflowRunRepoId: RepoId; address: string }> {
  const address = deriveRunAddress({
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
  const sources: Record<string, InferenceSource[]> = {};
  for (const stepId of opts.topLevelStepIds) {
    sources[stepId] = [inferenceSource];
  }

  const handle = await deployWorkflowSourceForTest(env, {
    entryModule: childWorkflowEntry(opts.fixture),
    db: h.db,
    tenantId: TENANT_ID,
    definitionAssetId: opts.definitionAssetId,
    anchorRunId: opts.anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: address,
    approvals: opts.approvals,
    config: makeConfig(opts.anchorRunId, address),
    sources,
  });
  expect(handle.publicKey).toBeTruthy();

  await waitFor(() => env.hub.router.getRoutableAddresses().includes(address), {
    timeoutMs: 20_000,
    diagnostics: env.sidecarDiagnostics,
  });
  return { workflowRunRepoId: handle.workflowRunRepoId, address };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "parent -> child workflow round-trip",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("parent run spawns child, child run lands under runs/<childRunId>/", async () => {
      const parentMailAddress = deriveRunAddress({
        runId: PARENT_DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });
      const childMailAddress = deriveRunAddress({
        runId: CHILD_DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });
      // The deploy step lifts the inline child to an internal ref
      // (`<parentWorkflowId>__<stepId>`); ChildSpawned carries that ref.
      const CHILD_BODY_REF = `${PARENT_WORKFLOW_ID}__spawn`;

      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${parentMailAddress}`,
        `mail.address:${childMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
      ]);

      const { workflowRunRepoId: parentWorkflowRunRepoId } = await deployParent(
        {
          anchorRunId: PARENT_DEPLOYMENT_ID,
          definitionAssetId: SINGLE_DEFINITION_ASSET_ID,
          topLevelStepIds: ["step1", "spawn", "step2"],
          approvals: operatorApprovals,
          fixture: {
            workflowId: PARENT_WORKFLOW_ID,
            address: parentMailAddress,
            steps: [
              {
                stepId: "step1",
                agentId: "agent-parent-step1",
                systemPrompt: "You are the parent workflow's first step agent.",
              },
              {
                stepId: "step2",
                agentId: "agent-parent-step2",
                systemPrompt:
                  "You are the parent workflow's second step agent.",
                after: ["spawn"],
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
                      agentId: "agent-child-step",
                      systemPrompt: "You are the child workflow's step agent.",
                    },
                  ],
                },
              },
            ],
          },
        },
      );

      await fireMailTrigger(env, parentMailAddress, {
        messageId: "<child-workflow-roundtrip-1@integration.interchange>",
      });

      const parentRunId = await waitForFirstRunId(
        env,
        parentWorkflowRunRepoId,
        {
          diagnostics: env.sidecarDiagnostics,
          timeoutMs: 20_000,
        },
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
          `ChildSpawned event is missing a string childRunId field; got ${typeof childRunId}`,
        );
      }
      expect(spawnedEvent.body["childDefinitionRef"]).toBe(CHILD_BODY_REF);

      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            PARENT_DEPLOYMENT_ID,
            parentRunId,
          );
          return events.some((e) => e.type === "RunCompleted");
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
      );

      const finalParentEvents = await readWorkflowRunEvents(
        env,
        PARENT_DEPLOYMENT_ID,
        parentRunId,
      );
      const parentTypes = finalParentEvents.map((e) => e.type);
      const stepCompletedIdx = (stepId: string): number =>
        parentTypes.findIndex(
          (t, i) =>
            t === "StepCompleted" &&
            finalParentEvents[i]?.body["stepId"] === stepId,
        );
      const runStartedIdx = parentTypes.indexOf("RunStarted");
      const step1CompletedIdx = stepCompletedIdx("step1");
      const spawnStartedIdx = parentTypes.findIndex(
        (t, i) =>
          t === "StepStarted" &&
          finalParentEvents[i]?.body["stepId"] === "spawn",
      );
      const childSpawnedIdx = parentTypes.indexOf("ChildSpawned");
      const childCompletedIdx = parentTypes.indexOf("ChildCompleted");
      const spawnCompletedIdx = stepCompletedIdx("spawn");
      const step2CompletedIdx = stepCompletedIdx("step2");
      const runCompletedIdx = parentTypes.indexOf("RunCompleted");

      // The parent's first step completes, the childWorkflow spawn runs a real
      // child agent and settles ChildCompleted(completed), the spawn step
      // completes, the post-spawn step2 runs, and the run completes.
      expect(runStartedIdx).toBeGreaterThanOrEqual(0);
      expect(step1CompletedIdx).toBeGreaterThan(runStartedIdx);
      expect(spawnStartedIdx).toBeGreaterThan(step1CompletedIdx);
      expect(childSpawnedIdx).toBeGreaterThan(spawnStartedIdx);
      expect(childCompletedIdx).toBeGreaterThan(childSpawnedIdx);
      expect(spawnCompletedIdx).toBeGreaterThan(childCompletedIdx);
      expect(step2CompletedIdx).toBeGreaterThan(spawnCompletedIdx);
      expect(runCompletedIdx).toBeGreaterThan(step2CompletedIdx);

      const childCompletedBody = finalParentEvents[childCompletedIdx]?.body;
      if (childCompletedBody === undefined) throw new Error("unreachable");
      expect(childCompletedBody["childRunId"]).toBe(childRunId);
      expect(childCompletedBody["terminalStatus"]).toBe("completed");

      // Parent's namespace must contain only the parent's per-step entries.
      // A regression that leaked the child's step events into the parent's
      // run log would surface a stepId outside the parent's stepOrder.
      const parentStepIds = new Set(["step1", "spawn", "step2"]);
      for (const event of finalParentEvents) {
        if (
          event.type !== "StepStarted" &&
          event.type !== "StepCompleted" &&
          event.type !== "StepFailed"
        ) {
          continue;
        }
        const stepId = event.body["stepId"];
        expect(parentStepIds.has(String(stepId))).toBe(true);
      }

      // Child run lives under runs/<childRunId>/events/ in the same
      // workflow-run repo. Its step runs a REAL agent: a StepCompleted with a
      // reply distinct from the agent id, not a fabricated stub failure.
      const childEvents = await readWorkflowRunEvents(
        env,
        PARENT_DEPLOYMENT_ID,
        childRunId,
      );
      expect(childEvents.length).toBeGreaterThan(0);
      const childTypes = childEvents.map((e) => e.type);
      const childRunStartedIdx = childTypes.indexOf("RunStarted");
      const childStepCompletedIdx = childTypes.findIndex(
        (t, i) =>
          t === "StepCompleted" &&
          childEvents[i]?.body["stepId"] === "childStep",
      );
      const childRunCompletedIdx = childTypes.indexOf("RunCompleted");

      expect(childRunStartedIdx).toBeGreaterThanOrEqual(0);
      expect(childStepCompletedIdx).toBeGreaterThan(childRunStartedIdx);
      expect(childRunCompletedIdx).toBeGreaterThan(childStepCompletedIdx);

      const childReply = readStepReply(childEvents[childStepCompletedIdx]);
      expect(childReply).toBe(EXPECTED_CHILD_REPLY);
      expect(childReply).not.toBe("agent-child-step");

      const childRunStartedBody = childEvents[childRunStartedIdx]?.body;
      if (childRunStartedBody === undefined) throw new Error("unreachable");
      expect(childRunStartedBody["runId"]).toBe(childRunId);
    }, 180_000);

    // A tool-bearing child. The child step's agent carries the inline
    // `mail_send` tool, so the child runs a REAL tool-bearing agent through the
    // source-tools arm -- not a toolless one. The mock inference echoes the
    // exposed tool names into the reply, so the child's `StepCompleted` reply
    // listing the tool name is the proof the child materialized its tool from
    // the shared closure. A regression that ran the child toolless (or failed
    // to materialize the source tool) would omit the name from the reply.
    test("a childWorkflow child runs a real tool-bearing agent", async () => {
      const parentMailAddress = deriveRunAddress({
        runId: TOOL_PARENT_DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });
      const childMailAddress = deriveRunAddress({
        runId: TOOL_CHILD_DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${parentMailAddress}`,
        `mail.address:${childMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
        // The child step declares the inline tool, so the deploy walk folds its
        // `tool:` grant into the parent step; the operator must approve it.
        `tool:${MAIL_TOOL_NAME}`,
      ]);

      const { workflowRunRepoId: parentWorkflowRunRepoId } = await deployParent(
        {
          anchorRunId: TOOL_PARENT_DEPLOYMENT_ID,
          definitionAssetId: TOOL_DEFINITION_ASSET_ID,
          topLevelStepIds: ["step1", "spawn"],
          approvals: operatorApprovals,
          fixture: {
            workflowId: TOOL_PARENT_WORKFLOW_ID,
            address: parentMailAddress,
            steps: [
              {
                stepId: "step1",
                agentId: "agent-tool-parent-step1",
                systemPrompt: "You are the tool parent's first step agent.",
              },
            ],
            spawns: [
              {
                stepId: "spawn",
                after: ["step1"],
                child: {
                  workflowId: TOOL_CHILD_WORKFLOW_ID,
                  address: childMailAddress,
                  steps: [
                    {
                      stepId: "childStep",
                      agentId: "agent-tool-child-step",
                      systemPrompt: "You are the tool child's step agent.",
                      tool: "fs",
                    },
                  ],
                },
              },
            ],
          },
        },
      );

      await fireMailTrigger(env, parentMailAddress, {
        messageId: "<child-workflow-tool-1@integration.interchange>",
      });

      const parentRunId = await waitForFirstRunId(
        env,
        parentWorkflowRunRepoId,
        {
          diagnostics: env.sidecarDiagnostics,
          timeoutMs: 20_000,
        },
      );

      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            TOOL_PARENT_DEPLOYMENT_ID,
            parentRunId,
          );
          return events.some((e) => e.type === "ChildSpawned");
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 20_000 },
      );

      const parentEvents = await readWorkflowRunEvents(
        env,
        TOOL_PARENT_DEPLOYMENT_ID,
        parentRunId,
      );
      const spawnedEvent = parentEvents.find((e) => e.type === "ChildSpawned");
      if (spawnedEvent === undefined) throw new Error("unreachable");
      const childRunId = spawnedEvent.body["childRunId"];
      if (typeof childRunId !== "string") {
        throw new Error(
          `ChildSpawned is missing a string childRunId; got ${typeof childRunId}`,
        );
      }

      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            TOOL_PARENT_DEPLOYMENT_ID,
            childRunId,
          );
          return events.some(
            (e) =>
              e.type === "StepCompleted" && e.body["stepId"] === "childStep",
          );
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
      );

      const childEvents = await readWorkflowRunEvents(
        env,
        TOOL_PARENT_DEPLOYMENT_ID,
        childRunId,
      );
      const childStepCompleted = childEvents.find(
        (e) => e.type === "StepCompleted" && e.body["stepId"] === "childStep",
      );
      // The reply lists the materialized tool name: the child ran a real
      // tool-bearing agent. A toolless run would carry the bare
      // "I see these tools:" prefix with no tool name.
      const childReply = readStepReply(childStepCompleted);
      expect(childReply).toContain(MAIL_TOOL_NAME);
    }, 180_000);

    // Grandchild-depth recursion. The sidecar's `createSidecarRunChild`
    // wires the child env's `spawnChild` via `createInMemorySpawnChild`
    // against the same recursive `runChild`, so an in-process grandchild
    // spawn resolves the grandchild's inline child definition from the
    // parent rung's in-memory closure map and drives a per-grandchildRunId
    // `runtimeRun` exactly the way the child's own spawn does. Sub-namespace
    // scoping continues to hold at every depth because each rung's runtime
    // env keys substrate operations on its own `runId`.
    test("parent -> child -> grandchild recursion at depth 2", async () => {
      const parentMailAddress = deriveRunAddress({
        runId: NESTED_PARENT_DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });
      const childMailAddress = deriveRunAddress({
        runId: NESTED_CHILD_DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });
      const grandchildMailAddress = deriveRunAddress({
        runId: NESTED_GRANDCHILD_DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });
      // Refs the deploy step assigns as it lifts each inline child. The parent's
      // spawnChild body is `<parentId>__spawnChild`; that body's own inline
      // grandchild is lifted a second time when the child rung runs, giving
      // `<parentId>__spawnChild__spawnGrandchild`.
      const CHILD_BODY_REF = `${NESTED_PARENT_WORKFLOW_ID}__spawnChild`;
      const GRANDCHILD_BODY_REF = `${CHILD_BODY_REF}__spawnGrandchild`;

      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${parentMailAddress}`,
        `mail.address:${childMailAddress}`,
        `mail.address:${grandchildMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
      ]);

      const { workflowRunRepoId: parentWorkflowRunRepoId } = await deployParent(
        {
          anchorRunId: NESTED_PARENT_DEPLOYMENT_ID,
          definitionAssetId: NESTED_DEFINITION_ASSET_ID,
          topLevelStepIds: ["parentStep", "spawnChild"],
          approvals: operatorApprovals,
          fixture: {
            workflowId: NESTED_PARENT_WORKFLOW_ID,
            address: parentMailAddress,
            steps: [
              {
                stepId: "parentStep",
                agentId: "agent-nested-parent-step",
                systemPrompt:
                  "You are the nested parent workflow's step agent.",
              },
            ],
            spawns: [
              {
                stepId: "spawnChild",
                after: ["parentStep"],
                child: {
                  workflowId: NESTED_CHILD_WORKFLOW_ID,
                  address: childMailAddress,
                  steps: [
                    {
                      stepId: "childStep",
                      agentId: "agent-nested-child-step",
                      systemPrompt:
                        "You are the nested child workflow's step agent.",
                    },
                  ],
                  spawns: [
                    {
                      stepId: "spawnGrandchild",
                      after: ["childStep"],
                      child: {
                        workflowId: NESTED_GRANDCHILD_WORKFLOW_ID,
                        address: grandchildMailAddress,
                        steps: [
                          {
                            stepId: "grandchildStep",
                            agentId: "agent-grandchild-step",
                            systemPrompt:
                              "You are the grandchild workflow's step agent.",
                          },
                        ],
                      },
                    },
                  ],
                },
              },
            ],
          },
        },
      );

      await fireMailTrigger(env, parentMailAddress, {
        messageId: "<nested-1@integration.interchange>",
      });

      const parentRunId = await waitForFirstRunId(
        env,
        parentWorkflowRunRepoId,
        {
          diagnostics: env.sidecarDiagnostics,
          timeoutMs: 30_000,
        },
      );

      // Wait for the parent run to terminate. By then the child and
      // grandchild runs must have terminated too (the parent's
      // ChildCompleted only commits after the child returns terminal,
      // and the child's ChildCompleted likewise gates on the
      // grandchild). Every rung runs a real agent at its leaf step, so
      // each rung completes and the parent settles completed.
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            NESTED_PARENT_DEPLOYMENT_ID,
            parentRunId,
          );
          return events.some((e) => e.type === "RunCompleted");
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 60_000 },
      );

      const parentEvents = await readWorkflowRunEvents(
        env,
        NESTED_PARENT_DEPLOYMENT_ID,
        parentRunId,
      );
      const parentSpawned = parentEvents.find((e) => e.type === "ChildSpawned");
      if (parentSpawned === undefined) {
        throw new Error("nested test: parent run has no ChildSpawned event");
      }
      const childRunId = parentSpawned.body["childRunId"];
      if (typeof childRunId !== "string") {
        throw new Error(
          `nested test: ChildSpawned is missing string childRunId; got ${typeof childRunId}`,
        );
      }
      expect(parentSpawned.body["childDefinitionRef"]).toBe(CHILD_BODY_REF);

      const parentChildCompleted = parentEvents.find(
        (e) => e.type === "ChildCompleted",
      );
      if (parentChildCompleted === undefined) {
        throw new Error("nested test: parent has no ChildCompleted event");
      }
      expect(parentChildCompleted.body["childRunId"]).toBe(childRunId);
      expect(parentChildCompleted.body["terminalStatus"]).toBe("completed");

      // Child run lives under `runs/<childRunId>/events/` in the same
      // workflow-run repo (sub-namespace scoping). The child must have
      // its own ChildSpawned referencing the grandchild's runId and a
      // matching ChildCompleted.
      const childEvents = await readWorkflowRunEvents(
        env,
        NESTED_PARENT_DEPLOYMENT_ID,
        childRunId,
      );
      expect(childEvents.length).toBeGreaterThan(0);
      const childSpawned = childEvents.find((e) => e.type === "ChildSpawned");
      if (childSpawned === undefined) {
        throw new Error(
          `nested test: child run ${childRunId} has no ChildSpawned event`,
        );
      }
      const grandchildRunId = childSpawned.body["childRunId"];
      if (typeof grandchildRunId !== "string") {
        throw new Error(
          `nested test: child's ChildSpawned is missing string childRunId; got ${typeof grandchildRunId}`,
        );
      }
      expect(childSpawned.body["childDefinitionRef"]).toBe(GRANDCHILD_BODY_REF);

      const childChildCompleted = childEvents.find(
        (e) => e.type === "ChildCompleted",
      );
      if (childChildCompleted === undefined) {
        throw new Error("nested test: child has no ChildCompleted event");
      }
      expect(childChildCompleted.body["childRunId"]).toBe(grandchildRunId);
      expect(childChildCompleted.body["terminalStatus"]).toBe("completed");

      // Grandchild run lives under `runs/<grandchildRunId>/events/` in
      // the same parent workflow-run repo. The sub-namespace scoping
      // collapses cross-rung run logs into one repo without overwrite. The
      // grandchild is the recursion leaf: its step runs a REAL agent, so the
      // run completes with real output at depth 2.
      const grandchildEvents = await readWorkflowRunEvents(
        env,
        NESTED_PARENT_DEPLOYMENT_ID,
        grandchildRunId,
      );
      expect(grandchildEvents.length).toBeGreaterThan(0);
      const grandchildTypes = grandchildEvents.map((e) => e.type);
      const grandchildRunStartedIdx = grandchildTypes.indexOf("RunStarted");
      const grandchildStepCompletedIdx =
        grandchildTypes.indexOf("StepCompleted");
      const grandchildRunCompletedIdx = grandchildTypes.indexOf("RunCompleted");
      expect(grandchildRunStartedIdx).toBeGreaterThanOrEqual(0);
      expect(grandchildStepCompletedIdx).toBeGreaterThan(
        grandchildRunStartedIdx,
      );
      expect(grandchildRunCompletedIdx).toBeGreaterThan(
        grandchildStepCompletedIdx,
      );
      expect(readStepReply(grandchildEvents[grandchildStepCompletedIdx])).toBe(
        EXPECTED_CHILD_REPLY,
      );

      const grandchildRunStartedBody =
        grandchildEvents[grandchildRunStartedIdx]?.body;
      if (grandchildRunStartedBody === undefined)
        throw new Error("unreachable");
      expect(grandchildRunStartedBody["runId"]).toBe(grandchildRunId);

      // Cross-rung distinctness: every runId is unique. The
      // sub-namespace scoping would silently collide if the runtime
      // re-used a runId across rungs.
      expect(new Set([parentRunId, childRunId, grandchildRunId]).size).toBe(3);
    }, 180_000);

    test(`parent -> ${String(SIBLINGS_CHILD_COUNT)} siblings via stepOrder`, async () => {
      const parentMailAddress = deriveRunAddress({
        runId: SIBLINGS_PARENT_DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });
      const siblingMailAddresses = SIBLINGS_CHILD_DEPLOYMENT_IDS.map(
        (anchorRunId) =>
          deriveRunAddress({ runId: anchorRunId, domain: DEPLOYMENT_DOMAIN }),
      );
      // Each inline sibling is lifted to `<parentId>__spawnN`; ChildSpawned
      // carries that ref, one per sibling step in stepOrder.
      const SIBLING_BODY_REFS: readonly string[] = Array.from(
        { length: SIBLINGS_CHILD_COUNT },
        (_unused, i) =>
          `${SIBLINGS_PARENT_WORKFLOW_ID}__spawn${(i + 1).toString()}`,
      );

      const spawns = SIBLINGS_CHILD_DEPLOYMENT_IDS.map((id, i) => {
        const address = siblingMailAddresses[i];
        const wfId = SIBLINGS_CHILD_WORKFLOW_IDS[i];
        if (address === undefined || wfId === undefined) {
          throw new Error("unreachable");
        }
        return {
          stepId: `spawn${(i + 1).toString()}`,
          after: ["parentStep"] as const,
          child: {
            workflowId: wfId,
            address,
            steps: [
              {
                stepId: `${id}Step`,
                agentId: `agent-${id}-step`,
                systemPrompt: `You are the ${id} workflow's step agent.`,
              },
            ],
          },
        };
      });

      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${parentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
        ...siblingMailAddresses.map((a) => `mail.address:${a}`),
      ]);

      const { workflowRunRepoId: parentWorkflowRunRepoId } = await deployParent(
        {
          anchorRunId: SIBLINGS_PARENT_DEPLOYMENT_ID,
          definitionAssetId: SIBLINGS_DEFINITION_ASSET_ID,
          topLevelStepIds: ["parentStep", ...spawns.map((s) => s.stepId)],
          approvals: operatorApprovals,
          fixture: {
            workflowId: SIBLINGS_PARENT_WORKFLOW_ID,
            address: parentMailAddress,
            steps: [
              {
                stepId: "parentStep",
                agentId: "agent-siblings-parent-step",
                systemPrompt: "You are the siblings parent step agent.",
              },
            ],
            spawns,
          },
        },
      );

      await fireMailTrigger(env, parentMailAddress, {
        messageId: "<siblings-1@integration.interchange>",
      });

      const parentRunId = await waitForFirstRunId(
        env,
        parentWorkflowRunRepoId,
        {
          diagnostics: env.sidecarDiagnostics,
          timeoutMs: 30_000,
        },
      );

      // Wait for the parent run to terminate. Every sibling must terminate
      // before the parent's terminal lands because the runtime's spawn step
      // does not settle until the child's terminal status is committed.
      // Each sibling runs a real agent at its leaf step, so it completes and
      // the parent settles completed.
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            SIBLINGS_PARENT_DEPLOYMENT_ID,
            parentRunId,
          );
          return events.some((e) => e.type === "RunCompleted");
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 90_000 },
      );

      const parentEvents = await readWorkflowRunEvents(
        env,
        SIBLINGS_PARENT_DEPLOYMENT_ID,
        parentRunId,
      );
      const spawnedEvents = parentEvents.filter(
        (e) => e.type === "ChildSpawned",
      );
      const completedEvents = parentEvents.filter(
        (e) => e.type === "ChildCompleted",
      );
      expect(spawnedEvents.length).toBe(SIBLINGS_CHILD_COUNT);
      expect(completedEvents.length).toBe(SIBLINGS_CHILD_COUNT);

      // Each ChildSpawned must reference a distinct definitionRef and a
      // distinct childRunId; every ChildCompleted must reference one of
      // those runIds with terminalStatus completed (a real agent leaf).
      const spawnedRefs = new Set<string>();
      const spawnedRunIds = new Set<string>();
      for (const ev of spawnedEvents) {
        const ref = ev.body["childDefinitionRef"];
        const runId = ev.body["childRunId"];
        if (typeof ref !== "string") {
          throw new Error(
            `siblings test: ChildSpawned missing string childDefinitionRef`,
          );
        }
        if (typeof runId !== "string") {
          throw new Error(
            `siblings test: ChildSpawned missing string childRunId`,
          );
        }
        spawnedRefs.add(ref);
        spawnedRunIds.add(runId);
      }
      expect(spawnedRefs.size).toBe(SIBLINGS_CHILD_COUNT);
      expect(spawnedRunIds.size).toBe(SIBLINGS_CHILD_COUNT);
      expect([...spawnedRefs].sort()).toEqual([...SIBLING_BODY_REFS].sort());

      const completedRunIds = new Set<string>();
      for (const ev of completedEvents) {
        const runId = ev.body["childRunId"];
        const status = ev.body["terminalStatus"];
        if (typeof runId !== "string") {
          throw new Error(
            `siblings test: ChildCompleted missing string childRunId`,
          );
        }
        expect(status).toBe("completed");
        completedRunIds.add(runId);
      }
      expect(completedRunIds).toEqual(spawnedRunIds);

      // Each sibling run materialised under a distinct
      // `runs/<childRunId>/` sub-namespace and ran a REAL agent at its step,
      // completing with real output.
      for (const childRunId of spawnedRunIds) {
        const childEvents = await readWorkflowRunEvents(
          env,
          SIBLINGS_PARENT_DEPLOYMENT_ID,
          childRunId,
        );
        expect(childEvents.length).toBeGreaterThan(0);
        const types = childEvents.map((e) => e.type);
        const runStartedIdx = types.indexOf("RunStarted");
        const stepCompletedIdx = types.indexOf("StepCompleted");
        const runCompletedIdx = types.indexOf("RunCompleted");
        expect(runStartedIdx).toBeGreaterThanOrEqual(0);
        expect(stepCompletedIdx).toBeGreaterThan(runStartedIdx);
        expect(runCompletedIdx).toBeGreaterThan(stepCompletedIdx);
        expect(readStepReply(childEvents[stepCompletedIdx])).toBe(
          EXPECTED_CHILD_REPLY,
        );
      }
    }, 180_000);
  },
);
