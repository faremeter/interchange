// An agent step inside an `onTrigger` section body INVOKES a real tool, and the
// invocation lands in its turn.
//
// The sibling `on-trigger-agent-body.test.ts` proves a body agent RUNS: a
// tool-less body step reaches real inference and commits the model reply. This
// proves the stronger property the operator is actually promised -- that a body
// agent can CALL a tool and feed the result back into a follow-up turn, the
// same `tool_use` -> execute -> `tool_result` -> reply loop a top-level step and
// a childWorkflow child both run.
//
// The failure this pins is SILENT. The body agent's tool survives the whole
// deploy path: the capability walk collects its `tool:<name>` grant, the gate
// rejects the deploy with `grants_not_approved` if the operator withholds it,
// and the frozen projection carries it. At run time the body step invoker
// builds its env with tool materialization skipped, so the provider is handed
// an EMPTY tool list. No step fails, no error is raised, and no log line is
// emitted at any level -- the operator's approval decision simply has no
// effect. A test asserting "no error occurred" would pass today and prove
// nothing, so the assertion here is POSITIVE: the tool's own return text must
// appear as a `tool_result` in the captured inference traffic.
//
// The env runs exactly one workflow, and an `onTrigger` container runs no agent
// of its own. The body agent is therefore the ONLY agent in this deployment, so
// a `tool_result` in ANY captured inference request can only have originated in
// the body's tool execution.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow-process child driving `runOnTrigger` with the production
// suspendable-child seam, and a real agent inside the body via the body-only
// invoker, driven against the mock inference fixture (real inference in CI is
// impractical). The mock is configured to drive a `tool_use` on the first
// request that exposes the tool, so a correctly wired body runs the inline
// `mail_send` tool for real and re-inferences with its result.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import {
  createApprovalSet,
  deriveRunAddress,
  type ApprovalSet,
} from "@intx/workflow-deploy";
import { loadFrozenGrantSnapshot } from "@intx/db";
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
  type InferenceRequest,
} from "../hub-agent/lib/deploy-flow-env";
import {
  deriveWireRunGrants,
  toolResultTexts,
} from "./nested-tool-invoke-helpers";
import { onTriggerToolInvokeEntry } from "./fixtures/on-trigger-tool-invoke-workflow";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_on-trigger-tool-invoke-1";
const SECTION_ID = "section";
const BODY_STEP_ID = "work";
const BODY_AGENT_ID = "agent-on-trigger-tool-invoke-body";
const BODY_CHILD_RUN_ID = `${SECTION_ID}__0`;

const TENANT_ID = "tnt_on_trigger_tool_invoke";
const CALLER_PRINCIPAL_ID = "prn_on_trigger_tool_invoke";
const DEFINITION_ASSET_ID = "ast_on_trigger_tool_invoke_wf";

/** The filename the scripted tool call writes under the body step's workdir. */
const TOOL_OUTPUT_FILE = "body-invoked.txt";
/** The "fs" variant's own return text for that call. */
const EXPECTED_TOOL_RESULT_TEXT = `wrote ${TOOL_OUTPUT_FILE}`;

/** The tool names the provider was handed on each captured request, in order. */
function describeToolLists(requests: readonly InferenceRequest[]): string {
  if (requests.length === 0) return "(no inference requests captured)";
  return requests
    .map(
      (req, index) =>
        `#${String(index)}: [${(req.tools ?? []).map((tool) => tool.name).join(", ")}]`,
    )
    .join(" ");
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
    name: "on-trigger-tool-invoke-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  // Drive the inline `mail_send` tool on the first request of every run that
  // exposes it. The "fs" variant writes a file under the step workdir with no
  // env requirement, so the call runs for real inside the body child.
  env = await startDeployFlowEnv({
    inferenceToolCall: {
      toolName: MAIL_TOOL_NAME,
      input: { to: "on-trigger body tool ran", body: TOOL_OUTPUT_FILE },
    },
    inferenceToolCallEachRun: true,
  });
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

const hasChildCompleted = (
  events: { type: string; body: Record<string, unknown> }[],
  childRunId: string,
): boolean =>
  events.some(
    (e) => e.type === "ChildCompleted" && e.body["childRunId"] === childRunId,
  );

describe.skipIf(!harnessDbEnvAvailable())(
  "an onTrigger body agent invokes a real tool",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("the body agent calls its inline tool and the result lands in a turn", async () => {
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

      // The body agent declares the inline tool, so the deploy walk folds its
      // grant into the section's approved surface; withholding `tool:<name>`
      // here makes the gate reject the deploy with `grants_not_approved`.
      const operatorApprovals: ApprovalSet = createApprovalSet([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
        `tool:${MAIL_TOOL_NAME}`,
      ]);

      const entryModule = onTriggerToolInvokeEntry({
        address: deploymentMailAddress,
        sectionId: SECTION_ID,
        stepId: BODY_STEP_ID,
        agentId: BODY_AGENT_ID,
        systemPrompt: "You are the onTrigger body agent; use your tool.",
        tool: "fs",
        workflowId: `wf_${DEPLOYMENT_ID}`,
        bodyWorkflowId: `wf_${DEPLOYMENT_ID}_body`,
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
        { diagnostics: env.sidecarDiagnostics },
      );

      // The body's tool grant reaches the run the way production delivers it:
      // the deploy walk folds the body step's `tool:` grant into the frozen
      // snapshot, and the trigger route projects that snapshot into the run's
      // grant rows. `fireMailTrigger` is the router-level helper, so unlike the
      // production route it does not materialize the rows itself -- deriving
      // them from the frozen snapshot here reproduces the delivery, and keeps
      // the grant the body authorizes against sourced from the probe walk
      // rather than a hand-authored constant. A source-ref deploy has no
      // tool-mark floor to fall back on (a source tool's runtime name is the
      // bare `definition.name` the walk already emitted a grant for), so this
      // IS the body's only authority.
      if (!handle.approved.approval.ok) {
        throw new Error("expected an approved definition");
      }
      const snapshot = await loadFrozenGrantSnapshot(
        h.db,
        handle.approved.approval.definitionId,
      );
      if (snapshot === null) {
        throw new Error("expected a frozen grant snapshot for the definition");
      }
      expect(
        snapshot.perStep.flatMap((s) =>
          s.grants.filter((g) => g.startsWith("tool:")),
        ),
      ).toContain(`tool:${MAIL_TOOL_NAME}`);

      // Fire the event; the section spawns the body child, whose tool-bearing
      // agent step runs through the spawned-child invoker.
      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: `<${DEPLOYMENT_ID}@integration.interchange>`,
        content: "trigger the tool-bearing agent body",
        grants: deriveWireRunGrants(snapshot),
      });

      const containerRunId = await (async () => {
        await waitFor(
          async () =>
            (await findContainerRunId(workflowRunRepoId)) !== undefined,
          { diagnostics: env.sidecarDiagnostics },
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
        { diagnostics: env.sidecarDiagnostics },
      );

      // The body step ran to completion. This is what makes the defect silent:
      // the step neither fails nor stalls when its tools are dropped, so the
      // assertions below are the only signal that anything went wrong.
      const bodyEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        BODY_CHILD_RUN_ID,
      );
      const bodyTypes = bodyEvents.map((e) => e.type);
      expect(bodyTypes).not.toContain("StepFailed");
      expect(
        bodyEvents.some(
          (e) =>
            e.type === "StepCompleted" && e.body["stepId"] === BODY_STEP_ID,
        ),
      ).toBe(true);

      // The proof: the body agent executed its tool and re-inferenced with the
      // result. The onTrigger container runs no agent of its own, so a
      // `tool_result` in any captured request can only be the body's tool
      // invocation landing in a turn, and the tool's own return text proves
      // the handler ran rather than faulting into an error result. The failure
      // message carries the tool lists the provider actually received, which is
      // where an empty body tool set shows itself.
      const observedResults = env.inference.requests.flatMap(toolResultTexts);
      const roundTrip = observedResults.includes(EXPECTED_TOOL_RESULT_TEXT)
        ? EXPECTED_TOOL_RESULT_TEXT
        : `tool_result ${JSON.stringify(EXPECTED_TOOL_RESULT_TEXT)} absent; tool_results seen: ${JSON.stringify(observedResults)}; tool lists the provider received: ${describeToolLists(env.inference.requests)}`;
      expect(roundTrip).toBe(EXPECTED_TOOL_RESULT_TEXT);

      // The body agent's own tool reached the provider. The round-trip above
      // cannot happen if the provider was handed no tools, so this names the
      // mechanism directly rather than only its consequence.
      expect(
        env.inference.requests.some((req) =>
          (req.tools ?? []).some((tool) => tool.name === MAIL_TOOL_NAME),
        ),
      ).toBe(true);
    }, 180_000);
  },
);
