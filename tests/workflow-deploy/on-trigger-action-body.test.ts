// A mail-triggered onTrigger body runs an action step on the stock sidecar.
//
// The body is an agent step, an action that reads that step's output and
// performs one declared effect, and a gate on the effect's result. Both gate
// branches are real steps. Firing the deployment's mail address spawns the
// body through the production suspendable-child seam. The body's own log is
// the proof: the agent, the action, and the taken branch complete, the other
// branch is skipped, and the container stays parked.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { sectionBodyRunId } from "@intx/workflow";

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
import {
  seedAsset,
  seedPrincipal,
  tenantSlugFromId,
} from "@intx/test-harness/seed";
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
import { onTriggerActionBodyEntry } from "./fixtures/on-trigger-action-body";
import { deriveWireRunGrants } from "./nested-tool-invoke-helpers";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_on-trigger-action-body-1";
const SECTION_ID = "section";
const EXPECTED_REPLY = "I see these tools:";

const TENANT_ID = "tnt_on_trigger_action_body";
const CALLER_PRINCIPAL_ID = "prn_on_trigger_action_body";
const DEFINITION_ASSET_ID = "ast_on_trigger_action_body_wf";

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  h = await createTestDb();
  await h.db.insert(tenantTable).values({
    id: TENANT_ID,
    name: TENANT_ID,
    slug: tenantSlugFromId(TENANT_ID),
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
    name: "on-trigger-action-body-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

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
    (event) =>
      event.type === "ChildCompleted" &&
      event.body["childRunId"] === childRunId,
  );

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

type RunEvent = { type: string; body: Record<string, unknown> };

function stepOutput(events: readonly RunEvent[], stepId: string): unknown {
  const completed = events.find(
    (event) =>
      event.type === "StepCompleted" && event.body["stepId"] === stepId,
  );
  if (completed === undefined) {
    throw new Error(`no StepCompleted event for step ${stepId}`);
  }
  const output = completed.body["output"];
  if (!isRecord(output)) {
    throw new Error(
      `step ${stepId} output is not an object: ${JSON.stringify(output)}`,
    );
  }
  const ref = output["ref"];
  if (typeof ref !== "string" || !ref.startsWith("inline:")) {
    throw new Error(
      `step ${stepId} output is not an inline ref: ${JSON.stringify(output)}`,
    );
  }
  return JSON.parse(ref.slice("inline:".length));
}

describe.skipIf(!harnessDbEnvAvailable())(
  "onTrigger body runs an action step through the body env",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a mail trigger runs the body's agent, action, and taken gate branch", async () => {
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
        agentId: DEPLOYMENT_ID,
        tenantId: "tenant-1",
        principalId: `prin_${DEPLOYMENT_ID}`,
        agentAddress: deploymentMailAddress,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      const operatorApprovals: ApprovalSet = createApprovalSet([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
        "effect:ship",
      ]);

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: onTriggerActionBodyEntry({
          address: deploymentMailAddress,
          workflowId: `wf_${DEPLOYMENT_ID}`,
        }),
        actions: "./workflow.mjs",
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
      // The body's effect is folded into the section step. The harness
      // delivers these rows itself; they are the same runtime grants the
      // mail materializer writes from this snapshot.
      expect(snapshot.perStep.flatMap((step) => step.grants)).toContain(
        "effect:ship",
      );

      const workflowRunRepoId: RepoId = handle.workflowRunRepoId;

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: `<${DEPLOYMENT_ID}@integration.interchange>`,
        content: "trigger the action body",
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

      const bodyRunId = sectionBodyRunId(containerRunId, SECTION_ID, 0);
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID,
            containerRunId,
          );
          return hasChildCompleted(events, bodyRunId);
        },
        { diagnostics: env.sidecarDiagnostics },
      );

      const bodyEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        bodyRunId,
      );
      expect(bodyEvents.map((event) => event.type)).not.toContain("StepFailed");

      const agentOutput = stepOutput(bodyEvents, "work");
      if (!isRecord(agentOutput) || typeof agentOutput["reply"] !== "string") {
        throw new Error(
          `agent output has no reply: ${JSON.stringify(agentOutput)}`,
        );
      }
      expect(agentOutput["reply"]).toBe(EXPECTED_REPLY);

      expect(stepOutput(bodyEvents, "ship")).toEqual({
        shipped: true,
        reply: EXPECTED_REPLY,
      });
      expect(stepOutput(bodyEvents, "choose")).toEqual({
        branch: "taken",
        value: true,
      });
      expect(stepOutput(bodyEvents, "taken")).toBe(null);
      expect(stepOutput(bodyEvents, "left")).toEqual({
        skipped: true,
        gateId: "choose",
        branch: "left",
      });

      expect(env.inference.requests.length).toBeGreaterThan(0);

      const containerEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        containerRunId,
      );
      const containerTypes = containerEvents.map((event) => event.type);
      expect(hasChildCompleted(containerEvents, bodyRunId)).toBe(true);
      expect(containerTypes).not.toContain("RunCompleted");
      expect(containerTypes).not.toContain("RunFailed");
    }, 120_000);
  },
);
