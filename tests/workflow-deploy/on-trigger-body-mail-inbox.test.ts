// An agent step inside an `onTrigger` section body reads the deployment's
// inbound mail: the bytes of the trigger mail's attachment, and the inbox
// through a mail read tool.
//
// A section body is a spawned child run. Its step agent is built by the
// spawned-child step invoker, not the top-level one. The two properties
// below hold only when that invoker carries the inbound surfaces:
//   - With no mail part reader, a trigger mail whose attachment bytes must be
//     read fails the body step before inference ("no mail-part reader
//     wired").
//   - `mail_search` lists the trigger mail only when the body agent's
//     transport reaches the deployment inbox.
//
// The body agent pins the production `@intx/tools-mail` sidecar bundle. The
// scripted mock provider makes the first turn of each run call `mail_search`
// and then closes with a text turn. Each test deploys its own section so that
// a failure of one property does not hide the other.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow-process child driving `runOnTrigger` with the production
// suspendable-child seam, and a real agent inside the body through the
// spawned-child invoker, driven against the mock inference fixture.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { sectionBodyRunId } from "@intx/workflow";
import { base64Encode } from "@intx/types/base64";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress } from "@intx/workflow-deploy";
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
  type FireMailTriggerOpts,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import {
  deriveWireRunGrants,
  failureMessages,
  toolResultTexts,
} from "./nested-tool-invoke-helpers";
import { onTriggerBodyEntry } from "./fixtures/on-trigger-body";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const SECTION_ID = "section";
const BODY_STEP_ID = "work";
const BODY_AGENT_ID = "agent-on-trigger-body-mail-inbox";

const TENANT_ID = "tnt_on_trigger_body_mail_inbox";
const CALLER_PRINCIPAL_ID = "prn_on_trigger_body_mail_inbox";

const ATTACHMENT_DEPLOYMENT_ID = "run_on-trigger-body-mail-attachment-1";
const ATTACHMENT_ASSET_ID = "ast_on_trigger_body_mail_attachment_wf";
const ATTACHMENT_MESSAGE_ID =
  "<on-trigger-body-mail-attachment-1@integration.interchange>";
// Distinct bytes, so their base64 form in a captured request can only come
// from this attachment.
const ATTACHMENT_BYTES = new TextEncoder().encode(
  "on-trigger-body-attachment-bytes-5c81e7",
);

const SEARCH_DEPLOYMENT_ID = "run_on-trigger-body-mail-search-1";
const SEARCH_ASSET_ID = "ast_on_trigger_body_mail_search_wf";
const SEARCH_MESSAGE_ID =
  "<on-trigger-body-mail-search-1@integration.interchange>";

const CLOSING_TEXT = "BODY-MAIL-INBOX-DONE";

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
  for (const assetId of [ATTACHMENT_ASSET_ID, SEARCH_ASSET_ID]) {
    await seedAsset(h.db, {
      id: assetId,
      tenantId: TENANT_ID,
      kind: "workflow",
      name: assetId,
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });
  }

  // The script is chosen by how many tool results a request's history holds,
  // so each body run gets its own `mail_search` turn and then a closing turn.
  env = await startDeployFlowEnv({
    inferenceScriptedTurns: [
      { toolUse: { name: "mail_search", input: {} } },
      { text: CLOSING_TEXT },
    ],
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

/**
 * Deploy an onTrigger section whose body agent carries the mail tools, fire
 * one trigger mail at it, and return the body run's events once its agent
 * step has completed or failed.
 */
async function runMailBody(opts: {
  deploymentId: string;
  definitionAssetId: string;
  mail: FireMailTriggerOpts;
}): Promise<WorkflowRunEvent[]> {
  const deploymentMailAddress = deriveRunAddress({
    runId: opts.deploymentId,
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
    agentAddress: deploymentMailAddress,
    sources: [inferenceSource],
    defaultSource: "anthropic:mock-model",
  };

  const entryModule = onTriggerBodyEntry({
    address: deploymentMailAddress,
    sectionId: SECTION_ID,
    body: {
      variant: "agent",
      stepId: BODY_STEP_ID,
      systemPrompt: "You are the onTrigger body agent; read your mail.",
      agentId: BODY_AGENT_ID,
      mailTools: true,
    },
    workflowId: `wf_${opts.deploymentId}`,
    bodyWorkflowId: `wf_${opts.deploymentId}_body`,
  });

  const handle = await deployWorkflowSourceForTest(env, {
    entryModule,
    db: h.db,
    tenantId: TENANT_ID,
    definitionAssetId: opts.definitionAssetId,
    anchorRunId: opts.deploymentId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: deploymentMailAddress,
    approvals: "approve-probed",
    config,
    sources: { [SECTION_ID]: [inferenceSource] },
  });
  const workflowRunRepoId: RepoId = handle.workflowRunRepoId;

  await waitFor(
    () => env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
    { diagnostics: env.sidecarDiagnostics },
  );

  // The trigger carries the run grants the deploy walk approved, the body's
  // `tool:mail_search` among them.
  await fireMailTrigger(env, deploymentMailAddress, {
    ...opts.mail,
    grants: deriveWireRunGrants(handle.approved.grantSnapshot),
  });

  // The container is the one run whose id is not a minted section body id.
  const findContainerRunId = async (): Promise<string | undefined> => {
    const ids = await listRunIds(env, workflowRunRepoId);
    return ids.find((id) => !id.startsWith(`${SECTION_ID}__`));
  };
  await waitFor(async () => (await findContainerRunId()) !== undefined, {
    diagnostics: env.sidecarDiagnostics,
  });
  const containerRunId = await findContainerRunId();
  if (containerRunId === undefined) throw new Error("no container run");
  const bodyRunId = sectionBodyRunId(containerRunId, SECTION_ID, 0);

  const isStepSettled = (e: WorkflowRunEvent): boolean =>
    (e.type === "StepCompleted" || e.type === "StepFailed") &&
    e.body["stepId"] === BODY_STEP_ID;
  await waitFor(
    async () =>
      (await readWorkflowRunEvents(env, opts.deploymentId, bodyRunId)).some(
        isStepSettled,
      ),
    { diagnostics: env.sidecarDiagnostics },
  );
  return readWorkflowRunEvents(env, opts.deploymentId, bodyRunId);
}

describe.skipIf(!harnessDbEnvAvailable())(
  "an onTrigger body agent reads the deployment's inbound mail",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("the trigger mail's attachment bytes reach the body agent's inference request", async () => {
      const bodyEvents = await runMailBody({
        deploymentId: ATTACHMENT_DEPLOYMENT_ID,
        definitionAssetId: ATTACHMENT_ASSET_ID,
        mail: {
          messageId: ATTACHMENT_MESSAGE_ID,
          content: "The attached image is for the body agent.",
          attachments: [
            {
              name: "body.png",
              contentType: "image/png",
              data: ATTACHMENT_BYTES,
            },
          ],
        },
      });

      expect(failureMessages(bodyEvents)).toEqual([]);

      // The reactor turns an image attachment into a base64 image block, so
      // the encoded bytes in a request prove the reader resolved the part.
      const encoded = base64Encode(ATTACHMENT_BYTES);
      expect(
        env.inference.requests.some((req) =>
          JSON.stringify(req).includes(encoded),
        ),
      ).toBe(true);
    });

    test("the body agent's mail_search answers from the deployment inbox", async () => {
      const bodyEvents = await runMailBody({
        deploymentId: SEARCH_DEPLOYMENT_ID,
        definitionAssetId: SEARCH_ASSET_ID,
        mail: {
          messageId: SEARCH_MESSAGE_ID,
          content: "Search your inbox for this message.",
        },
      });

      expect(failureMessages(bodyEvents)).toEqual([]);

      // A successful search lists each match's Message-ID. A failed search
      // answers an error result that carries no match.
      const results = env.inference.requests.flatMap(toolResultTexts);
      const searchResult = results.find((text) =>
        text.includes(SEARCH_MESSAGE_ID),
      );
      if (searchResult === undefined) {
        throw new Error(
          `no tool_result listed ${SEARCH_MESSAGE_ID}; tool_results seen: ${JSON.stringify(results)}`,
        );
      }
    });
  },
);
