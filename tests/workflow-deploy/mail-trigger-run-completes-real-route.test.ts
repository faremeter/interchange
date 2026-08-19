// Trigger-route runId contract: a real-route run reaches terminal SUCCESS.
//
// The supervisor keys a run on the deployment's mail address as the stable
// runId and reads that run's grants from `runs/<runId>/grants.json`; the
// sidecar's `onRunStart` barrier refuses to start a run whose grants file
// is absent at that path. So a producer that stages the run's grants under
// any OTHER id leaves the supervisor's path empty and the run fails closed.
//
// This test drives the PRODUCTION `POST /workflows/:runId/mail`
// route against a real migrated schema and a real sidecar subprocess, and
// asserts the dispatched run reaches `RunCompleted` -- not just that DB
// rows were committed. A completing single-step agent workflow (echo
// inference) is used precisely so the run CAN terminate successfully; the
// only thing that keeps it from completing is the grants file landing under
// a runId the supervisor never reads. Before the runId contract was
// unified (the route derived runId from the mail's Message-ID), the grants
// were staged under that per-message id, `onRunStart` found no grants at
// `runs/<deploymentMailAddress>/`, and the run failed closed -- this test
// fails there. The `mail-trigger-derived-grants-roundtrip` sibling proves
// the derivation + DB commit; this one proves the run actually runs.
//
// The deployment is stood up through the shared code-sourced front
// (`deployWorkflowSourceForTest`): it bundles the tool-less single-step
// fixture, installs/probes/gates/freezes the definition against the real DB
// (writing the frozen grant snapshot the trigger route materializes from),
// deploys it by source-ref to the real sidecar, and writes the anchor
// `workflow_run` row. The route then hydrates the run's grants from that
// frozen snapshot -- the static `workflow.json` definition path is gone.

import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { type } from "arktype";

import { createGrantStore } from "@intx/db";
import {
  tenant as tenantTable,
  workflowRun as workflowRunTable,
} from "@intx/db/schema";
import { createApp, type GetSession } from "@intx/hub-api";
import {
  createAssetService,
  type EventCollectorRegistry,
} from "@intx/hub-sessions";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedGrant, seedPrincipal } from "@intx/test-harness/seed";
import { deriveRunAddress } from "@intx/workflow-deploy";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepAgentEntry } from "./fixtures/single-step-agent";

// The tenant domain must equal the fixture's deploy domain so the route's
// derived address (`<anchorRunId>@<tenant.domain>`) matches the
// address the fixture deployed the sidecar workflow under; otherwise the
// route's sendRunGrants/routeMail target an unknown address (409).
const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_mail-trigger-run-completes-1";
const TENANT_ID = "tnt_mail_trigger_completes";
const CALLER_USER_ID = "usr_mail_trigger_completes_caller";
const CALLER_PRINCIPAL_ID = "prn_mail_trigger_completes_caller";
const DEFINITION_ASSET_ID = "ast_mail_trigger_completes_wf";
const STEP_ID = "step1";

const deploymentMailAddress = deriveRunAddress({
  runId: DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

// The trigger route's 202 body shape. Validated rather than cast so a
// route response drift surfaces at the boundary.
const TriggerResponse = type({
  runId: "string",
  address: "string",
  messageId: "string",
});

let env: DeployFlowEnv;
let h: TestDb;

function createMockGetSession(userId: string): GetSession {
  const now = new Date("2025-01-01");
  return async () => ({
    user: {
      id: userId,
      email: "caller@example.com",
      emailVerified: true,
      name: "Caller",
      createdAt: now,
      updatedAt: now,
    },
    session: {
      id: "session_mail_trigger_completes",
      userId,
      token: "tok_mail_trigger_completes",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function notImpl(name: string): never {
  throw new Error(`mail-trigger run-completes mock: ${name} not implemented`);
}

function createMockEventCollectors(): EventCollectorRegistry {
  return {
    create: () => notImpl("create"),
    dispatch: () => notImpl("dispatch"),
    abandon: () => notImpl("abandon"),
    has: () => false,
    getStatus: () => undefined,
    getAccumulatedText: () => undefined,
    getCurrentTurnId: () => undefined,
    getLastTurnId: () => undefined,
  };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a mail-triggered run started through the real route reaches RunCompleted",
  () => {
    let hasRun = false;

    beforeAll(async () => {
      h = await createTestDb();
      // Echo inference so the single agent step produces a reply and the
      // run terminates on its own once its grants barrier is satisfied.
      env = await startDeployFlowEnv({ inferenceEchoUserMessage: true });
    });

    afterAll(async () => {
      if (env !== undefined) await env.teardown();
      if (h !== undefined) await h.close();
    });

    beforeEach(async () => {
      await h.reset();
    });

    afterEach(async () => {
      await h.reset();
    });

    test("the run reaches RunCompleted, not RunFailed on a missing grants file", async () => {
      if (hasRun) {
        throw new Error(
          "this suite assumes a single test per shared subprocess env; " +
            "add a new scenario in its own file with its own env instead",
        );
      }
      hasRun = true;

      // Seed the tenancy the route resolves against: the tenant carries the
      // deploy domain so the derived address matches the sidecar deployment,
      // and the caller is an active user-principal holding the
      // `workflow-run:<id>/manage` grant the `/mail` route requires. The
      // caller also creates the `workflow`-kind definition asset the frozen
      // definition projects over; the trigger route reads that asset's
      // creator to resolve the run's creator grants.
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
        refId: CALLER_USER_ID,
        status: "active",
      });
      await seedAsset(h.db, {
        id: DEFINITION_ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "mail-trigger-completes-wf",
        creatorPrincipalId: CALLER_PRINCIPAL_ID,
      });
      await seedGrant(h.db, {
        id: "grant-caller-manage",
        tenantId: TENANT_ID,
        resource: `workflow-run:${DEPLOYMENT_ID}`,
        action: "manage",
        effect: "allow",
        origin: "system",
        principalId: CALLER_PRINCIPAL_ID,
      });

      // Deploy the same workflow to the real sidecar through the code-sourced
      // front. This freezes the definition + grant snapshot against the real
      // DB, writes the anchor `workflow_run` row, and makes the deployment's
      // mail address routable; it also registers the deployment with the
      // fixture so run events can be read for the completion wait.
      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        apiKey: "sk-mock",
        model: "mock-model",
      };
      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: DEPLOYMENT_ID,
        tenantId: "tenant-1",
        principalId: "prin_integration-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };
      const entryModule = singleStepAgentEntry({
        stepId: STEP_ID,
        systemPrompt: "You are the single-step run-completes agent.",
        address: deploymentMailAddress,
        agentId: `agent_${DEPLOYMENT_ID}`,
      });
      const handle = await deployWorkflowSourceForTest(env, {
        entryModule,
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: "approve-probed",
        config,
        sources: { [STEP_ID]: [inferenceSource] },
      });
      expect(handle.publicKey).toBeTruthy();

      // The source-ref frame round-trips through the real sidecar subprocess
      // (index the pack, check out the pinned subtree, register the address),
      // so routability is asynchronous. Wait for it before driving the route.
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      const grantStore = createGrantStore(h.db);
      const assetService = createAssetService({
        db: h.db,
        repoStore: env.hub.agentRepoStore.repoStore,
      });
      const triggerApp = createApp({
        getSession: createMockGetSession(CALLER_USER_ID),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        grantStore,
        sidecarRouter: env.hub.router,
        sessionService: env.hub.sessionService,
        eventCollectors: createMockEventCollectors(),
        assetService,
        repoStore: env.hub.agentRepoStore.repoStore,
        maxTarballBytes: 10_000_000,
      });

      const res = await triggerApp.request(
        `/api/tenants/${TENANT_ID}/workflows/${DEPLOYMENT_ID}/mail`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ content: "kick off the run" }),
        },
      );
      if (res.status !== 202) {
        const body: unknown = await res.json();
        throw new Error(
          `expected 202 from /mail, got ${String(res.status)}: ${JSON.stringify(body)}\n${env.sidecarDiagnostics()}`,
        );
      }
      const json = TriggerResponse.assert(await res.json());
      expect(json.address).toBe(deploymentMailAddress);

      // The load-bearing assertion: the run the route dispatched reaches
      // terminal SUCCESS. The runId is the deployment mail address, so that
      // is what the run events are keyed on. A RunFailed here (or a timeout)
      // is the signature of grants staged under the wrong runId.
      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        DEPLOYMENT_ID,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      if (terminal.type !== "RunCompleted") {
        throw new Error(
          `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(terminal.body)}\n${env.sidecarDiagnostics()}`,
        );
      }
      expect(terminal.type).toBe("RunCompleted");

      // The collapse: the trigger reconciles onto the deployment's single
      // self-anchored run rather than minting a second routable row. After the
      // first trigger exactly ONE workflow_run row exists, and it is the anchor
      // (id == anchorRunId == anchorRunId) -- the anchor IS the run.
      const runRows = await h.db.select().from(workflowRunTable);
      expect(runRows).toHaveLength(1);
      expect(runRows[0]?.id).toBe(DEPLOYMENT_ID);
      expect(runRows[0]?.anchorRunId).toBe(DEPLOYMENT_ID);
    });
  },
);
