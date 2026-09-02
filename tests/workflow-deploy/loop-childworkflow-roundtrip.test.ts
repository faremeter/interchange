// Deployed loop-body -> childWorkflow real-execution round-trip.
//
// The deployed counterpart to the runLocal test
// (packages/workflow/src/runtime/loop-child-workflow.test.ts) and the deploy
// unit tests (inert-ontrigger-bodies.test.ts): those prove a loop body's
// childWorkflow is LIFTED and RESOLVED; this proves the nested grandchild
// EXECUTES for real on the deployed path, and that its per-step inference source
// was staged under the transitive `<workflowId>__<loopStepId>__<spawnStepId>`
// ref (the deploy enumerator recurses into the loop body).
//
// A workflow whose single suspending/spawning top-level step is a `loop` is
// deployed BY SOURCE-REF against the real hub + sidecar subprocess + mock
// inference. The loop body's only step is a `childWorkflow` spawn of a trivial
// one-agent grandchild. Firing the mail trigger runs iteration 0 under the
// inherited env; the body's `childWorkflow` step spawns the grandchild, whose
// agent runs a REAL agent through the sidecar. The terminal assertion is that
// the grandchild reaches the runtime and produces a real `{ reply, turn }`
// output, and its completion propagates up the spawn chain.
//
// Harness justification: SPAWN-REAL. Mirrors on-trigger-childworkflow-roundtrip
// with a loop container instead of an onTrigger section.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import { loopBodyRunId } from "@intx/workflow";
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
import { loopChildWorkflowEntry } from "./fixtures/loop-childworkflow-body";

// A toolless grandchild agent's reply from the mock inference provider.
const EXPECTED_CHILD_REPLY = "I see these tools:";

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
const DEPLOYMENT_ID = "run_loop-childworkflow-1";
const WORKFLOW_ID = `wf_${DEPLOYMENT_ID}`;
const CHILD_WORKFLOW_ID = `wf_child_${DEPLOYMENT_ID}`;
const LOOP_STEP_ID = "rework";
const SPAWN_STEP_ID = "spawn";
const CHILD_STEP_ID = "childStep";
const CHILD_AGENT_ID = "agent-nested-child";

// The loop iteration body child run for iteration 0. The runtime keys it on
// `<runId>__<loopStepId>__<index>` (`loopBodyRunId`); the run's own id is
// `DEPLOYMENT_ID`.
const BODY_CHILD_RUN_ID = loopBodyRunId(DEPLOYMENT_ID, LOOP_STEP_ID, 0);

// The ref the deploy assigns to the loop body's inline childWorkflow: the body
// is `<workflowId>__<loopStepId>`, and its inline childWorkflow lifts to
// `<bodyRef>__<spawnStepId>`.
const NESTED_CHILD_BODY_REF = `${WORKFLOW_ID}__${LOOP_STEP_ID}__${SPAWN_STEP_ID}`;

const TENANT_ID = "tnt_loop_childworkflow";
const CALLER_PRINCIPAL_ID = "prn_loop_childworkflow";
const DEFINITION_ASSET_ID = "ast_loop_childworkflow_wf";

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
  // A file-scope beforeAll fires even when describe.skipIf skips the
  // suite bodies, so it needs its own guard or a missing DB env throws
  // here. See the two-shape rule in tests/lib/db-harness.ts.
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
    name: "loop-childworkflow-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

// The top-level container run is the one that is NOT a loop iteration child
// (children are `<loopStepId>__<n>`) and NOT the grandchild (a minted run id).
async function findContainerRunId(
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  const ids = await listRunIds(env, workflowRunRepoId);
  return ids.find((id) => !id.includes("__") && id !== CHILD_WORKFLOW_ID);
}

describe.skipIf(!harnessDbEnvAvailable())(
  "loop body -> childWorkflow round-trip",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a loop body's childWorkflow spawns a grandchild that runs a real agent", async () => {
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        credentialId: "sk-mock",
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

      const entryModule = loopChildWorkflowEntry({
        address: deploymentMailAddress,
        loopStepId: LOOP_STEP_ID,
        spawnStepId: SPAWN_STEP_ID,
        workflowId: WORKFLOW_ID,
        childWorkflowId: CHILD_WORKFLOW_ID,
        childStepId: CHILD_STEP_ID,
        childAgentId: CHILD_AGENT_ID,
        childSystemPrompt:
          "You are the nested grandchild workflow's step agent.",
      });

      // Omit `sources`: the deploy computes per-step pins and stages the
      // grandchild's inference source transitively via referencedDefinitions
      // (enumerateInertBodies recurses into the loop body).
      const handle = await deployWorkflowSourceForTest(env, {
        entryModule,
        loops: "./workflow.mjs",
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: operatorApprovals,
        config,
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
        content: "trigger the loop body",
      });

      await waitFor(
        async () => (await findContainerRunId(workflowRunRepoId)) !== undefined,
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
      );

      // The loop iteration body child (`rework__0`) runs the childWorkflow spawn
      // step, emitting ChildSpawned into its own run log.
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
      const grandchildRunId = bodySpawned.body["childRunId"];
      if (typeof grandchildRunId !== "string") {
        throw new Error(
          `body ChildSpawned is missing a string childRunId; got ${typeof grandchildRunId}`,
        );
      }
      // The loop body's childWorkflow was lifted to the transitive ref.
      expect(bodySpawned.body["childDefinitionRef"]).toBe(
        NESTED_CHILD_BODY_REF,
      );

      // Wait for the grandchild run to terminate; its leaf step runs a REAL
      // agent, so it settles completed with real output.
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID,
            grandchildRunId,
          );
          return events.some((e) => e.type === "RunCompleted");
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 60_000 },
      );

      // ---- The reachability proof: read the grandchild's own run log ----
      const nestedEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        grandchildRunId,
      );
      const nestedTypes = nestedEvents.map((e) => e.type);
      const nestedRunStartedIdx = nestedTypes.indexOf("RunStarted");
      const nestedStepStartedIdx = nestedTypes.indexOf("StepStarted");
      const nestedStepCompletedIdx = nestedTypes.indexOf("StepCompleted");
      const nestedRunCompletedIdx = nestedTypes.indexOf("RunCompleted");

      expect(nestedRunStartedIdx).toBeGreaterThanOrEqual(0);
      expect(nestedStepStartedIdx).toBeGreaterThan(nestedRunStartedIdx);
      expect(nestedStepCompletedIdx).toBeGreaterThan(nestedStepStartedIdx);
      expect(nestedRunCompletedIdx).toBeGreaterThan(nestedStepCompletedIdx);

      // A real agent reply -- not a fabricated success. The grandchild reached
      // the real invoker, and its inference source was staged under the
      // transitive loop-body ref.
      expect(readStepReply(nestedEvents[nestedStepCompletedIdx])).toBe(
        EXPECTED_CHILD_REPLY,
      );

      // The grandchild's completion propagates up: the body child's
      // ChildCompleted settles completed and the body's spawn step completes.
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
      expect(bodyChildCompleted.body["childRunId"]).toBe(grandchildRunId);
      expect(bodyChildCompleted.body["terminalStatus"]).toBe("completed");
    }, 180_000);
  },
);
