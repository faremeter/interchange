// onTrigger body real-agent reachability gate (INTR-310).
//
// The proof that a deployed onTrigger section's body runs a REAL agent step,
// not the ChildStepNotImplementedError stub. An onTrigger body is a child run
// spawned per section event; body agent-step execution was stubbed until the
// cold, tool-less, body-only invoker landed. This drives that path end-to-end
// on the real deploy stack: the workflow is deployed BY SOURCE-REF (bundle a
// source entry module into a hub asset, probe it, approve+freeze it against a
// real DB, deploy the source-ref frame) with a section subscribed to the
// deployment mail address whose body is a single tool-less agent step. Firing
// the trigger spawns the body child, whose agent step runs through the body
// invoker against the mock inference fixture and commits the model's
// deterministic reply as its output.
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
// suspendable-child seam, and a real agent inside the body via the body-only
// invoker. Body agents are tool-less by the deploy guard, so no tool trees are
// staged; this exercises exactly the reachable body-agent path.

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
} from "../hub-agent/lib/deploy-flow-env";
import { onTriggerBodyEntry } from "./fixtures/on-trigger-body";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_on-trigger-agent-body-1";
const SECTION_ID = "section";
const BODY_STEP_ID = "work";
const BODY_AGENT_ID = "agent-body-work";
const BODY_CHILD_RUN_ID = `${SECTION_ID}__0`;

// The mock inference server's reply for a tool-less agent (no tool names).
const EXPECTED_REPLY = "I see these tools:";

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition
// projects over. The install/approve freeze and the anchor `workflow_run`
// insert both write against these, so they must exist in the real DB before
// the deploy runs.
const TENANT_ID = "tnt_on_trigger_agent_body";
const CALLER_PRINCIPAL_ID = "prn_on_trigger_agent_body";
const DEFINITION_ASSET_ID = "ast_on_trigger_agent_body_wf";

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
    name: "on-trigger-agent-body-wf",
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

const hasChildCompleted = (
  events: { type: string; body: Record<string, unknown> }[],
  childRunId: string,
): boolean =>
  events.some(
    (e) => e.type === "ChildCompleted" && e.body["childRunId"] === childRunId,
  );

// Whether a captured agent-event payload is a live inference event (its type is
// in the `inference.*` family the reactor emits during a step's `agent.send`).
function isLiveInferenceEvent(event: unknown): boolean {
  if (typeof event !== "object" || event === null || !("type" in event)) {
    return false;
  }
  const { type } = event;
  return typeof type === "string" && type.startsWith("inference.");
}

describe.skipIf(!harnessDbEnvAvailable())(
  "onTrigger body runs a real agent step through the body invoker",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a tool-less agent body runs for real and commits the model reply as its step output", async () => {
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

      const entryModule = onTriggerBodyEntry({
        address: deploymentMailAddress,
        sectionId: SECTION_ID,
        body: {
          variant: "agent",
          stepId: BODY_STEP_ID,
          systemPrompt: "You are the onTrigger body agent.",
          agentId: BODY_AGENT_ID,
        },
        workflowId: `wf_${DEPLOYMENT_ID}`,
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

      // Fire the event; the section spawns the body child, whose agent step runs
      // through the body invoker and completes -- no signal, no gate, just the
      // agent. Wait for the body child to complete on the container's log.
      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: `<${DEPLOYMENT_ID}@integration.interchange>`,
        content: "trigger the agent body",
      });

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
      expect(reply).not.toBe(BODY_AGENT_ID);

      // The body agent's inference call actually reached the mock provider, so the
      // reply is real model output rather than a synthesized constant.
      expect(env.inference.requests.length).toBeGreaterThan(0);

      // The onEvent plumb (INTR-310 follow-up): the body agent's LIVE inference
      // events reach the hub's agent-event stream. Before the body invoker's
      // onEvent was wired they were SILENTLY DROPPED -- durable per-run events
      // still landed under runs/<childRunId>/events/, but the live firehose saw
      // nothing from the body. The onTrigger container runs no agent of its own,
      // so an inference-typed event on this deployment's stream can only have
      // originated in the body. This is the regression guard: an omitted callback
      // and a wired-but-dead one are indistinguishable without it.
      await waitFor(
        () =>
          env.hub.agentEvents.some(
            (e) =>
              e.addr === deploymentMailAddress && isLiveInferenceEvent(e.event),
          ),
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 10_000 },
      );

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
  },
);

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
