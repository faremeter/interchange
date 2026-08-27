// onTrigger-body -> childWorkflow real-execution gate (INTR-310).
//
// The runtime counterpart to the capability-walk grant coverage for the same
// nesting (walk test "collects a childWorkflow's grants nested inside an
// onTrigger body"): the walk proves the operator APPROVES the nested child's
// grants; this proves the nested child EXECUTES for real. It covers the exact
// cross-type shape -- a childWorkflow spawn buried inside an onTrigger section
// body -- whose per-step assets the deploy layer must stage transitively.
//
// A workflow whose single top-level step is an `onTrigger` section is deployed
// BY SOURCE-REF (bundle a source entry module into a hub asset, probe it,
// approve+freeze it against a real DB, deploy the source-ref frame) against the
// real hub + sidecar subprocess + mock inference fixture. The section's inline
// body is a `defineWorkflow` whose only step is a `childWorkflow` spawn of a
// trivial one-agent child. Firing the section's mail trigger spawns the body as
// a suspendable child; the body's `childWorkflow` step spawns a nested child
// whose per-step agent runs a REAL agent through the sidecar's `childInvokeStep`.
// The terminal assertion is that the nested child reaches the runtime and
// produces a real `{ reply, turn }` output, distinct from the agent id.
//
// This proves the full chain deploys and runs: the onTrigger-body -> childWorkflow
// shape freezes, the walk approves it, the section spawns the body, the body
// spawns the nested child, and the nested child's step runs a real agent whose
// inference source was staged under the transitive `<bodyRef>__<spawnStep>` ref.

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
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  type DeployFlowEnv,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import { onTriggerChildWorkflowEntry } from "./fixtures/on-trigger-childworkflow-body";

// A toolless child agent's reply from the mock inference provider.
const EXPECTED_CHILD_REPLY = "I see these tools:";

/**
 * Extract a nested child step's real agent reply from its `StepCompleted`
 * event body (a small `{ reply, turn }` output inlines as `inline:<json>`).
 * Mirrors the same-named helper in `child-workflow-roundtrip.test.ts`; a real
 * reply here proves the childWorkflow inside an onTrigger body ran a real agent.
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
const DEPLOYMENT_ID = "run_on-trigger-childworkflow-1";
const WORKFLOW_ID = `wf_${DEPLOYMENT_ID}`;
const CHILD_WORKFLOW_ID = `wf_child_${DEPLOYMENT_ID}`;
const SECTION_ID = "section";
const SPAWN_STEP_ID = "spawn";
const CHILD_STEP_ID = "childStep";
const CHILD_AGENT_ID = "agent-nested-child";

// The body child run the section spawns for the first event. The runtime keys
// it on `<sectionId>__<eventIndex>`.
const BODY_CHILD_RUN_ID = `${SECTION_ID}__0`;

// The ref the deploy assigns to the body's inline childWorkflow. The section's
// body is lifted to `<workflowId>__<sectionId>` and becomes that body's id; the
// body's own inline childWorkflow is then lifted to `<bodyRef>__<spawnStepId>`
// when the body rung runs.
const NESTED_CHILD_BODY_REF = `${WORKFLOW_ID}__${SECTION_ID}__${SPAWN_STEP_ID}`;

// The definition's own tenant, the caller principal that creates the definition
// asset, and the `workflow`-kind asset the frozen definition projects over. The
// install/approve freeze and the anchor `workflow_run` insert both write against
// these, so they must exist in the real DB before the deploy runs.
const TENANT_ID = "tnt_on_trigger_childworkflow";
const CALLER_PRINCIPAL_ID = "prn_on_trigger_childworkflow";
const DEFINITION_ASSET_ID = "ast_on_trigger_childworkflow_wf";

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
  await seedAsset(h.db, {
    id: DEFINITION_ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "on-trigger-childworkflow-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
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

describe.skipIf(!harnessDbEnvAvailable())(
  "onTrigger body -> childWorkflow round-trip",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a section body's childWorkflow spawns a nested child that fails loud (INTR-310)", async () => {
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        apiKey: "sk-mock",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `${DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: `prin_${DEPLOYMENT_ID}`,
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
      ]);

      const entryModule = onTriggerChildWorkflowEntry({
        address: deploymentMailAddress,
        sectionId: SECTION_ID,
        spawnStepId: SPAWN_STEP_ID,
        workflowId: WORKFLOW_ID,
        childWorkflowId: CHILD_WORKFLOW_ID,
        childStepId: CHILD_STEP_ID,
        childAgentId: CHILD_AGENT_ID,
        childSystemPrompt: "You are the nested child workflow's step agent.",
      });

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule,
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: operatorApprovals,
        config,
        sources: { [SECTION_ID]: [inferenceSource] },
      });
      expect(handle.publicKey).toBeTruthy();

      const workflowRunRepoId: RepoId = handle.workflowRunRepoId;

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: `<${DEPLOYMENT_ID}@integration.interchange>`,
        content: "trigger the section body",
      });

      // The section container run (the long-lived, non-body run) appears first.
      const containerRunId = await (async () => {
        await waitFor(
          async () =>
            (await findContainerRunId(workflowRunRepoId)) !== undefined,
          { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
        );
        const id = await findContainerRunId(workflowRunRepoId);
        if (id === undefined) throw new Error("no container run");
        return id;
      })();

      // The body child (`section__0`) runs the childWorkflow spawn step, which
      // emits ChildSpawned into the body child's own run log. Wait for it.
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID,
            BODY_CHILD_RUN_ID,
          );
          return events.some((e) => e.type === "ChildSpawned");
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 60_000 },
      );

      const bodyEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        BODY_CHILD_RUN_ID,
      );
      const bodySpawned = bodyEvents.find((e) => e.type === "ChildSpawned");
      if (bodySpawned === undefined) throw new Error("unreachable");
      const nestedChildRunId = bodySpawned.body["childRunId"];
      if (typeof nestedChildRunId !== "string") {
        throw new Error(
          `body ChildSpawned is missing a string childRunId; got ${typeof nestedChildRunId}`,
        );
      }
      // The body's childWorkflow was lifted to `<bodyRef>__<spawnStepId>`.
      expect(bodySpawned.body["childDefinitionRef"]).toBe(
        NESTED_CHILD_BODY_REF,
      );

      // Wait for the nested child run to terminate. Its leaf step runs a REAL
      // agent (INTR-310), so it settles completed with real output.
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID,
            nestedChildRunId,
          );
          return events.some((e) => e.type === "RunCompleted");
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 60_000 },
      );

      // ---- The reachability proof: read the nested child's own run log ----
      const nestedEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        nestedChildRunId,
      );
      expect(nestedEvents.length).toBeGreaterThan(0);
      const nestedTypes = nestedEvents.map((e) => e.type);
      const nestedRunStartedIdx = nestedTypes.indexOf("RunStarted");
      const nestedStepStartedIdx = nestedTypes.indexOf("StepStarted");
      const nestedStepCompletedIdx = nestedTypes.indexOf("StepCompleted");
      const nestedRunCompletedIdx = nestedTypes.indexOf("RunCompleted");

      expect(nestedRunStartedIdx).toBeGreaterThanOrEqual(0);
      expect(nestedStepStartedIdx).toBeGreaterThan(nestedRunStartedIdx);
      expect(nestedStepCompletedIdx).toBeGreaterThan(nestedStepStartedIdx);
      expect(nestedRunCompletedIdx).toBeGreaterThan(nestedStepCompletedIdx);

      // A real agent reply -- not a fabricated success, and the childWorkflow
      // inside the onTrigger body reached the real invoker.
      expect(readStepReply(nestedEvents[nestedStepCompletedIdx])).toBe(
        EXPECTED_CHILD_REPLY,
      );

      const nestedRunStartedBody = nestedEvents[nestedRunStartedIdx]?.body;
      if (nestedRunStartedBody === undefined) throw new Error("unreachable");
      expect(nestedRunStartedBody["runId"]).toBe(nestedChildRunId);

      // The nested child's completion propagates up the spawn chain: the body
      // child's ChildCompleted settles completed, and the body's spawn step
      // completes.
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID,
            BODY_CHILD_RUN_ID,
          );
          return events.some(
            (e) =>
              e.type === "StepCompleted" && e.body["stepId"] === SPAWN_STEP_ID,
          );
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
      );

      const finalBodyEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        BODY_CHILD_RUN_ID,
      );
      const bodyChildCompleted = finalBodyEvents.find(
        (e) => e.type === "ChildCompleted",
      );
      if (bodyChildCompleted === undefined) {
        throw new Error("body child run has no ChildCompleted event");
      }
      expect(bodyChildCompleted.body["childRunId"]).toBe(nestedChildRunId);
      expect(bodyChildCompleted.body["terminalStatus"]).toBe("completed");

      const bodySpawnCompleted = finalBodyEvents.find(
        (e) => e.type === "StepCompleted" && e.body["stepId"] === SPAWN_STEP_ID,
      );
      if (bodySpawnCompleted === undefined) {
        throw new Error(
          "body child run has no StepCompleted for the spawn step",
        );
      }

      void containerRunId;
    }, 180_000);
  },
);
