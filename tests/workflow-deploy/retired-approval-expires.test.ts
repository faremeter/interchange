// A run that stops waiting on an approval and goes on expires that approval
// on the Hub: an approver sees only the one the run asks for now, and a
// decision on the retired one is refused.
//
// A timed step asks for an approval and times out; its handler step asks for
// another. The steps are an onTrigger body, run once as is and once with the
// child killed while the first approval waits, so the resumed container
// retires it, and once more as the workflow's own steps, where the timed step
// retires its approval itself.
//
// Harness justification: SPAWN-REAL: real hub routes, real sidecar subprocess,
// real workflow-process child, mock inference. The mock hub applies packs
// without writing rows, so the test settles approvals from each pack's wait
// changes with the store function production's pack receive calls.

import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { eq } from "drizzle-orm";

import { approval, tenant as tenantTable } from "@intx/db/schema";
import { createApp, createRunGrantsLoader } from "@intx/hub-api";
import {
  applyApprovalWaitChanges,
  createAssetService,
} from "@intx/hub-sessions";
import { signalName } from "@intx/types";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedGrant,
  seedPrincipal,
  seedPrincipalKey,
  tenantSlugFromId,
} from "@intx/test-harness/seed";
import { sectionBodyRunId } from "@intx/workflow";
import { createApprovalSet, deriveRunAddress } from "@intx/workflow-deploy";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  killWorkflowHostChild,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";
import {
  createMockEventCollectors,
  createMockGetSession,
  createRegisterSignalCorrelation,
} from "./scope-always-helpers";

const DOMAIN = "integration.interchange";
const DEPLOYMENTS = {
  live: "run_retired-approval-live",
  restart: "run_retired-approval-restart",
  step: "run_retired-approval-step",
} as const;
const TENANT_ID = "tnt_retired_approval";
const USER_ID = "usr_retired_approval";
const PRINCIPAL_ID = "prn_retired_approval";
const ASSET_ID = "ast_retired_approval";
const STEP_TIMEOUT_MS = 8_000;
const SECTION = "section";
const MAIL_TOOL_MODULE = path.resolve(import.meta.dir, "fixtures/mail-tool.ts");

const STEPS = `{
  timed: step({
    agent: agent("timed-agent"),
    timeout: ${String(STEP_TIMEOUT_MS)},
    onFailure: "handler",
  }),
  handler: step({ agent: agent("handler-agent"), after: ["timed"] }),
}`;

// The two steps as an onTrigger body, or as the workflow's own steps.
const entry = (address: string, asBody: boolean): string => `
import { defineWorkflow, onTrigger, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
import { mailSendTool } from ${JSON.stringify(MAIL_TOOL_MODULE)};

const agent = (id) => defineAgent({
  id,
  systemPrompt: id,
  tools: [mailSendTool("ask")],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
});

export const workflow = ${
  asBody
    ? `defineWorkflow({
  id: "retired-approval-wf",
  steps: {
    ${SECTION}: onTrigger({
      on: { type: "mail", to: ${JSON.stringify(address)} },
      body: defineWorkflow({
        id: "retired-approval-body",
        trigger: { type: "manual" },
        steps: ${STEPS},
      }),
    }),
  },
})`
    : `defineWorkflow({
  id: "retired-approval-wf",
  trigger: { type: "mail", to: ${JSON.stringify(address)} },
  steps: ${STEPS},
})`
};
`;

let h: TestDb;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  h = await createTestDb();
  await h.db.insert(tenantTable).values({
    id: TENANT_ID,
    name: TENANT_ID,
    slug: tenantSlugFromId(TENANT_ID),
    domain: DOMAIN,
    parentId: null,
  });
  await seedPrincipal(h.db, {
    id: PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
    refId: USER_ID,
    status: "active",
  });
  await seedPrincipalKey(h.db, PRINCIPAL_ID);
  await seedAsset(h.db, {
    id: ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "retired-approval-wf",
    creatorPrincipalId: PRINCIPAL_ID,
  });
  for (const [id, resource, action] of [
    ["grant-manage-live", `workflow-run:${DEPLOYMENTS.live}`, "manage"],
    ["grant-manage-restart", `workflow-run:${DEPLOYMENTS.restart}`, "manage"],
    ["grant-manage-step", `workflow-run:${DEPLOYMENTS.step}`, "manage"],
    // The mail that starts the run needs a send grant of its own.
    ["grant-send", `mail.send:tenant:${TENANT_ID}`, "send"],
    ["grant-resolve", "approval:*", "resolve"],
  ] as const) {
    await seedGrant(h.db, {
      id,
      tenantId: TENANT_ID,
      resource,
      action,
      effect: "allow",
      origin: "system",
      principalId: PRINCIPAL_ID,
    });
  }
});

afterAll(async () => {
  if (h !== undefined) await h.close();
});

function startEnv(deploymentId: string): Promise<DeployFlowEnv> {
  return startDeployFlowEnv({
    // Each agent calls the ask-granted tool until its result is in history,
    // so each step parks on an approval.
    inferenceApprovalToolCall: {
      toolName: MAIL_TOOL_NAME,
      input: { to: "retired-approval", body: "retired-approval.txt" },
      resultPrefix: "done: ",
    },
    registerSignalCorrelation: createRegisterSignalCorrelation(h.db),
    settleApprovalWaits: (changes) =>
      h.db.transaction((tx) =>
        applyApprovalWaitChanges(tx, deploymentId, changes, new Date()),
      ),
    loadRunGrants: createRunGrantsLoader({ db: h.db }),
  });
}

describe.skipIf(!harnessDbEnvAvailable())(
  "an approval its run stopped waiting on",
  () => {
    test.each(["live", "restart", "step"] as const)(
      "expires, leaves the list of pending approvals, and refuses a decision (%s)",
      async (mode) => {
        const deploymentId = DEPLOYMENTS[mode];
        const env = await startEnv(deploymentId);
        try {
          const address = deriveRunAddress({
            runId: deploymentId,
            domain: DOMAIN,
          });
          const source: InferenceSource = {
            id: "anthropic:mock-model",
            provider: "anthropic",
            baseURL: `http://localhost:${String(env.inference.server.port)}`,
            credentialId: "sk-mock",
            model: "mock-model",
          };
          const config: HarnessConfig = {
            sessionId: SESSION_ID,
            agentAddress: address,
            sources: [source],
            defaultSource: "anthropic:mock-model",
          };
          const handle = await deployWorkflowSourceForTest(env, {
            entryModule: entry(address, mode !== "step"),
            db: h.db,
            tenantId: TENANT_ID,
            definitionAssetId: ASSET_ID,
            anchorRunId: deploymentId,
            sourceAuthorityPrincipalId: PRINCIPAL_ID,
            deploymentDomain: DOMAIN,
            agentAddress: address,
            approvals: createApprovalSet([
              "inference.source:anthropic:mock-model",
              "director:@intx/agent/default",
              `mail.address:${address}`,
              `mail.send:${DOMAIN}`,
              `tool:${MAIL_TOOL_NAME}`,
            ]),
            config,
          });
          const diagnostics = { diagnostics: env.sidecarDiagnostics };
          await waitFor(
            () => env.hub.router.getRoutableAddresses().includes(address),
            diagnostics,
          );

          const app = createApp({
            getSession: createMockGetSession(USER_ID, "retired"),
            authHandler: () => new Response("", { status: 404 }),
            db: h.db,
            sidecarRouter: env.hub.router,
            sessionService: env.hub.sessionService,
            eventCollectors: createMockEventCollectors("retired"),
            assetService: createAssetService({
              db: h.db,
              repoStore: env.hub.agentRepoStore.repoStore,
            }),
            repoStore: env.hub.agentRepoStore.repoStore,
            maxTarballBytes: 10_000_000,
          });
          const post = (route: string, body: unknown) =>
            app.request(`/api/tenants/${TENANT_ID}/${route}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify(body),
            });
          const approvals = () =>
            h.db
              .select()
              .from(approval)
              .where(eq(approval.anchorRunId, deploymentId))
              .orderBy(approval.createdAt);

          expect(
            (await post(`workflows/${deploymentId}/mail`, { content: "go" }))
              .status,
          ).toBe(202);
          const runId = await waitForFirstRunId(
            env,
            handle.workflowRunRepoId,
            diagnostics,
          );
          const bodyRunId = sectionBodyRunId(runId, SECTION, 0);
          const containerEvents = () =>
            readWorkflowRunEvents(env, deploymentId, runId);

          // ---- the timed step asks for the first approval ----
          await waitFor(
            async () => (await approvals()).length === 1,
            diagnostics,
          );
          const [first] = await approvals();
          if (first === undefined) throw new Error("unreachable");
          if (mode === "restart") killWorkflowHostChild(env);

          // ---- the step times out and its handler asks for the second ----
          await waitFor(
            async () => (await approvals()).length === 2,
            diagnostics,
          );
          const second = (await approvals()).find((row) => row.id !== first.id);
          if (second === undefined) throw new Error("unreachable");
          await waitFor(
            async () =>
              (await containerEvents()).some(
                (e) =>
                  e.type === "SignalAwaitAbandoned" &&
                  e.body["signalName"] === signalName(first.correlationId),
              ),
            diagnostics,
          );

          // ---- the Hub expires the first once that history reaches it ----
          await waitFor(
            async () =>
              (await approvals()).find((row) => row.id === first.id)?.status ===
              "expired",
            diagnostics,
          );
          const listed = await app.request(
            `/api/tenants/${TENANT_ID}/approvals`,
          );
          expect(listed.status).toBe(200);
          const page: unknown = await listed.json();
          expect(page).toMatchObject({
            data: [{ id: second.id, status: "pending" }],
          });

          // ---- a decision on the retired approval is refused ----
          const refused = await post(`approvals/${first.id}/approve`, {
            scope: "once",
          });
          expect(refused.status).toBe(409);
          expect(await refused.json()).toMatchObject({
            error: { code: "already_resolved" },
          });

          // ---- the approval the run waits on still completes the body ----
          expect(
            (await post(`approvals/${second.id}/approve`, { scope: "once" }))
              .status,
          ).toBe(200);
          await waitFor(
            async () =>
              (await containerEvents()).some((e) =>
                mode === "step"
                  ? e.type === "RunCompleted"
                  : e.type === "ChildCompleted" &&
                    e.body["childRunId"] === bodyRunId &&
                    e.body["terminalStatus"] === "completed",
              ),
            diagnostics,
          );
          expect((await approvals()).map((row) => row.status)).toEqual([
            "expired",
            "approved",
          ]);
        } finally {
          await env.teardown();
        }
      },
      240_000,
    );
  },
);
