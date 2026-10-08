// An onTrigger body uses a credential whose parent grant was seeded at the
// trigger. The body's own grants file is the child cap's output, and Gate 2
// reads that snapshot. The probe fetches its pinned origin only when the cap
// kept `credential:{id}` / `use` for `toolConsumer(factory.id)`.
//
// The container is one section and runs no agent. The probe exists only inside
// the inline body, so a bearer on the origin is the body's authorization, not
// the container's. The container stays up after the body finishes; waiting for
// its `RunCompleted` would hang.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { sectionBodyRunId } from "@intx/workflow";

import { toolConsumer } from "@intx/authz";
import { createNoopCredentialCipher } from "@intx/crypto";
import { loadFrozenGrantSnapshot } from "@intx/db";
import { tenant as tenantTable } from "@intx/db/schema";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import type { WireGrantRule } from "@intx/types/grant-wire";
import {
  createApprovalSet,
  deriveRunAddress,
  type ApprovalSet,
} from "@intx/workflow-deploy";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedCredential,
  seedPrincipal,
  seedProvider,
  tenantSlugFromId,
} from "@intx/test-harness/seed";
import type { RepoId } from "@intx/hub-sessions";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import {
  CREDENTIAL_HANDLE,
  PROBE_DEFINITION_NAME,
} from "./fixtures/credential-tool-bundle";
import { CREDENTIAL_BINDING_PACKAGE } from "./fixtures/credential-tool-workflow";
import { onTriggerCredentialToolEntry } from "./fixtures/on-trigger-credential-tool-workflow";
import { deriveWireRunGrants } from "./nested-tool-invoke-helpers";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_on-trigger-credential-tool-1";
const SECTION_ID = "section";
const BODY_STEP_ID = "work";
const BODY_AGENT_ID = "agent-on-trigger-credential-body";

const TENANT_ID = "tnt_on_trigger_credential_tool";
const CALLER_PRINCIPAL_ID = "prn_on_trigger_credential_tool";
const PROVIDER_ID = "prv_on_trigger_credential_tool";
const PROVIDER_NAME = "on-trigger-credential-probe-provider";
const CREDENTIAL_ID = "cred_on_trigger_credential_tool";
const CREDENTIAL_NAME = "on-trigger-credential-probe-cred";
const DEFINITION_ASSET_ID = "ast_on_trigger_credential_tool";

const SECRET = "sk-on-trigger-probe-4c18";
const PROBE_PATH = "/whoami";
const PROBE_SENTINEL = "on-trigger-credential-probe.json";

// Source arm: Gate 2 and the cap both key the consumer on the factory id.
const CONSUMER = toolConsumer(CREDENTIAL_BINDING_PACKAGE);

const CREDENTIAL_USE_GRANT: WireGrantRule = {
  id: "grant-on-trigger-credential-probe-use",
  resource: `credential:${CREDENTIAL_ID}`,
  action: "use",
  effect: "allow",
  origin: "creator",
  conditions: { tool: CONSUMER },
  expiresAt: null,
  roleId: null,
  principalId: null,
};

let env: DeployFlowEnv;
let h: TestDb;
let origin: ReturnType<typeof Bun.serve>;
const originRequests: { path: string; authorization: string | null }[] = [];

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  origin = Bun.serve({
    port: 0,
    fetch(req) {
      const url = new URL(req.url);
      originRequests.push({
        path: url.pathname,
        authorization: req.headers.get("authorization"),
      });
      return new Response(
        JSON.stringify({ ok: true, seenPath: url.pathname }),
        { headers: { "content-type": "application/json" } },
      );
    },
  });

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
  await seedProvider(h.db, {
    id: PROVIDER_ID,
    tenantId: TENANT_ID,
    name: PROVIDER_NAME,
    plugin: "http",
    apiBaseUrl: origin.url.origin,
  });
  await seedCredential(h.db, {
    id: CREDENTIAL_ID,
    tenantId: TENANT_ID,
    providerId: PROVIDER_ID,
    name: CREDENTIAL_NAME,
    secret: SECRET,
  });
  await seedAsset(h.db, {
    id: DEFINITION_ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "on-trigger-credential-tool-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv({
    inferenceToolCall: {
      toolName: PROBE_DEFINITION_NAME,
      input: { path: PROBE_PATH, sentinel: PROBE_SENTINEL },
    },
    inferenceToolCallEachRun: true,
  });
});

afterAll(async () => {
  if (origin !== undefined) await origin.stop(true);
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

async function findContainerRunId(
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  const ids = await listRunIds(env, workflowRunRepoId);
  return ids.find((id) => !id.startsWith(`${SECTION_ID}__`));
}

describe.skipIf(!harnessDbEnvAvailable())(
  "an onTrigger body uses a seeded credential grant",
  () => {
    test("the body's probe authenticates to the pinned origin", async () => {
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
        `tool:${PROBE_DEFINITION_NAME}`,
        `credential:${CREDENTIAL_HANDLE}`,
      ]);
      const entryModule = onTriggerCredentialToolEntry({
        address: deploymentMailAddress,
        sectionId: SECTION_ID,
        stepId: BODY_STEP_ID,
        agentId: BODY_AGENT_ID,
        systemPrompt: "You are the onTrigger body agent; use your probe.",
        workflowId: `wf_${DEPLOYMENT_ID}`,
        bodyWorkflowId: `wf_${DEPLOYMENT_ID}_body`,
        binding: {
          handle: CREDENTIAL_HANDLE,
          provider: PROVIDER_NAME,
          name: CREDENTIAL_NAME,
        },
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
        credentialCipher: createNoopCredentialCipher(),
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
      expect(
        snapshot.perStep.flatMap((step) =>
          step.grants.filter((grant) => grant.startsWith("tool:")),
        ),
      ).toContain(`tool:${PROBE_DEFINITION_NAME}`);

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      const before = originRequests.length;
      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: `<${DEPLOYMENT_ID}@integration.interchange>`,
        content: "trigger the credential probe body",
        grants: [...deriveWireRunGrants(snapshot), CREDENTIAL_USE_GRANT],
      });

      const workflowRunRepoId: RepoId = handle.workflowRunRepoId;
      await waitFor(
        async () => (await findContainerRunId(workflowRunRepoId)) !== undefined,
        { diagnostics: env.sidecarDiagnostics },
      );
      const containerRunId = await findContainerRunId(workflowRunRepoId);
      if (containerRunId === undefined) throw new Error("no container run");

      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID,
            containerRunId,
          );
          return events.some(
            (event) =>
              event.type === "ChildCompleted" &&
              event.body["childRunId"] ===
                sectionBodyRunId(containerRunId, SECTION_ID, 0),
          );
        },
        { diagnostics: env.sidecarDiagnostics },
      );

      const fresh = originRequests.slice(before);
      const probeReq = fresh.find((request) => request.path === PROBE_PATH);
      if (probeReq?.authorization !== `Bearer ${SECRET}`) {
        const bodyEvents = await readWorkflowRunEvents(
          env,
          DEPLOYMENT_ID,
          sectionBodyRunId(containerRunId, SECTION_ID, 0),
        );
        throw new Error(
          `origin did not record the bearer: ${JSON.stringify(fresh)}; body events: ${JSON.stringify(bodyEvents.map((event) => event.type))}\n${env.sidecarDiagnostics()}`,
        );
      }
    }, 180_000);
  },
);
