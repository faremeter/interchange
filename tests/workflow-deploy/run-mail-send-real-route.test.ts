// Run-surface mail send routes through the workflow-native Trigger path.
//
// `POST /workflows/runs/:runId/mail` on the run surface fires the run through
// the SAME shared trigger the deployment Trigger route (`POST /workflows/:runId/
// mail`) uses. This test drives the PRODUCTION run-surface route against a real
// migrated schema and a real sidecar subprocess and asserts the send is
// accepted for dispatch (202) and reconciles onto the deployment's single
// self-anchored run rather than minting a second routable row.
//
// It covers the seam this surface adds over the shared core: the run-first
// pre-resolution (`findRoutableById`) and the `assetService`/dispatch deps
// threaded into `createRunRoutes`. The trigger core itself is proven by the
// deployment Trigger route's own real-route tests; this proves the run surface
// reaches it.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

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

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { createGrantStore } from "@intx/db";
import {
  tenant as tenantTable,
  workflowDefinition as workflowDefinitionTable,
  workflowRun as workflowRunTable,
} from "@intx/db/schema";
import { createSSHSignature, generateKeyPair } from "@intx/crypto";
import { createApp, type GetSession } from "@intx/hub-api";
import {
  DEFAULT_ASSET_REF,
  WORKFLOW_JSON_PATH,
  createAssetService,
  createRepoStore,
  workflowAuthorize,
  workflowKindHandler,
  workflowRunAuthorize,
  workflowRunKindHandler,
  type AuthorizeFn,
  type EventCollectorRegistry,
  type RepoId,
  type RepoStore,
  type SessionService,
  type WorkflowRunHubPrincipal,
} from "@intx/hub-sessions";
import type { HarnessConfig, KeyPair } from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedGrant, seedPrincipal } from "@intx/test-harness/seed";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveRunAddress,
  type ApprovalSet,
  type DeploySingleStepFn,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";

import {
  SESSION_ID,
  startDeployFlowEnv,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

// The tenant domain must equal the fixture's deploy domain so the route's
// derived address (`run_<deploymentId>@<tenant.domain>`) matches the address
// the fixture deployed the sidecar workflow under; otherwise the route's
// sendRunGrants/routeMail target an unknown address (409).
const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_run-mail-send-route-1";
const TENANT_ID = "tnt_run_mail_send_route";
const CALLER_USER_ID = "usr_run_mail_send_route_caller";
const CALLER_PRINCIPAL_ID = "prn_run_mail_send_route_caller";
const DEFINITION_ASSET_ID = "ast_run_mail_send_route_wf";
const STEP_ID = "step1";

const TOOL_PINS: readonly ToolPackagePin[] = [];

const deploymentMailAddress = deriveRunAddress({
  runId: DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

// A single agent step with no tools: the send only needs to reach the deployed
// sidecar and be accepted for dispatch, so no completion is awaited here.
const agent = defineAgent({
  id: `agent_${DEPLOYMENT_ID}`,
  systemPrompt: "You are the run-mail-send route agent.",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

const workflow: WorkflowDefinition = defineWorkflow({
  id: `wf_${DEPLOYMENT_ID}`,
  trigger: { type: "mail", to: deploymentMailAddress },
  steps: {
    [STEP_ID]: step({ agent }),
  },
});

// The run-surface send's 202 body shape (the shared trigger response). Validated
// rather than cast so a route response drift surfaces at the boundary.
const TriggerResponse = type({
  runId: "string",
  address: "string",
  messageId: "string",
});

let env: DeployFlowEnv;
let h: TestDb;
let signingKey: KeyPair;
const tempDirs: string[] = [];

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
      id: "session_run_mail_send_route",
      userId,
      token: "tok_run_mail_send_route",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function notImpl(name: string): never {
  throw new Error(`run-mail-send route mock: ${name} not implemented`);
}

function createMockSessionService(): SessionService {
  return {
    stageWorkflowStep: () => notImpl("stageWorkflowStep"),
    deployInstanceAtHead: () => notImpl("deployInstanceAtHead"),
    deployWorkflowDefinition: () => notImpl("deployWorkflowDefinition"),
    deploySingleStepAtHead: () => notImpl("deploySingleStepAtHead"),
    sendUserMessage: () => notImpl("sendUserMessage"),
    endSession: () => notImpl("endSession"),
  };
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

// A real RepoStore carrying both kinds the run mail-send trigger reads: the
// workflow asset's `workflow.json` and the deployment's workflow-run lifecycle.
async function createWorkflowRepoStore(): Promise<RepoStore> {
  const dataDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "run-mail-send-route-"),
  );
  tempDirs.push(dataDir);
  const signer = async (payload: string) =>
    createSSHSignature(payload, signingKey.privateKey, signingKey.publicKey);
  const authorize: AuthorizeFn = (principal, repoId, ref, act) => {
    if (repoId.kind === "workflow") {
      return workflowAuthorize(principal, repoId, ref, act);
    }
    if (repoId.kind === "workflow-run") {
      return workflowRunAuthorize(principal, repoId, ref, act);
    }
    return { allowed: false, reason: `no authorize for ${repoId.kind}` };
  };
  return createRepoStore({
    dataDir,
    signingKey,
    handlers: {
      workflow: workflowKindHandler,
      "workflow-run": workflowRunKindHandler,
    },
    authorize,
    signingCallback: () => signer,
  });
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a mail send through the run-surface route reaches the deployment's run",
  () => {
    let hasRun = false;

    beforeAll(async () => {
      signingKey = await generateKeyPair();
      h = await createTestDb();
      env = await startDeployFlowEnv();
    });

    afterAll(async () => {
      await env.teardown();
      await h.close();
      for (const d of tempDirs.splice(0)) {
        await fs.promises.rm(d, { recursive: true, force: true });
      }
    });

    beforeEach(async () => {
      await h.reset();
    });

    afterEach(async () => {
      await h.reset();
    });

    test("the run-surface send is accepted and reconciles onto the single anchored run", async () => {
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
      // `workflow-run:<id>/manage` grant the run mail-send route requires.
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
        name: "run-mail-send-route-wf",
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

      // Write the workflow's `workflow.json` into the hub-api asset the
      // trigger hydrates its grants from.
      const repoStore = await createWorkflowRepoStore();
      const assetService = createAssetService({ db: h.db, repoStore });
      await repoStore.initRepo({ kind: "workflow", id: DEFINITION_ASSET_ID });
      await assetService.populateAsset({
        assetId: DEFINITION_ASSET_ID,
        ref: DEFAULT_ASSET_REF,
        principal: { kind: "hub" },
        tree: {
          files: { [WORKFLOW_JSON_PATH]: JSON.stringify(workflow) },
          message: "seed workflow.json",
        },
      });

      // Deploy the same workflow to the real sidecar so its address is routable
      // and its workflow-run repo exists.
      await deployWorkflowToSidecar();

      // Seed the deployment's definition + anchor run after the asset and
      // sidecar deploy so the FK targets exist for the trigger's commit.
      await seedDeploymentRow();

      const grantStore = createGrantStore(h.db);
      const runApp = createApp({
        getSession: createMockGetSession(CALLER_USER_ID),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        grantStore,
        sidecarRouter: env.hub.router,
        sessionService: createMockSessionService(),
        eventCollectors: createMockEventCollectors(),
        assetService,
        repoStore,
        maxTarballBytes: 10_000_000,
      });

      const res = await runApp.request(
        `/api/tenants/${TENANT_ID}/workflows/runs/${DEPLOYMENT_ID}/mail`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ content: "kick off the run" }),
        },
      );
      if (res.status !== 202) {
        const body: unknown = await res.json();
        throw new Error(
          `expected 202 from run mail send, got ${String(res.status)}: ${JSON.stringify(body)}\n${env.sidecarDiagnostics()}`,
        );
      }
      const json = TriggerResponse.assert(await res.json());
      expect(json.runId).toBe(DEPLOYMENT_ID);
      expect(json.address).toBe(deploymentMailAddress);

      // The collapse: the trigger reconciles onto the deployment's single
      // self-anchored run rather than minting a second routable row. After the
      // first send exactly ONE workflow_run row exists, and it is the anchor
      // (id == anchorRunId == deploymentId) -- the anchor IS the run.
      const runRows = await h.db.select().from(workflowRunTable);
      expect(runRows).toHaveLength(1);
      expect(runRows[0]?.id).toBe(DEPLOYMENT_ID);
      expect(runRows[0]?.anchorRunId).toBe(DEPLOYMENT_ID);
    });

    // Seed the deployment's first-class definition and its anchor run at the
    // fixture's deploy address: the trigger reads the workflow asset and the
    // run's definition off the anchor.
    async function seedDeploymentRow(): Promise<void> {
      await h.db.insert(workflowDefinitionTable).values({
        id: `wfd_${DEPLOYMENT_ID}`,
        tenantId: TENANT_ID,
        name: DEPLOYMENT_ID,
        assetId: DEFINITION_ASSET_ID,
      });
      await h.db.insert(workflowRunTable).values({
        id: DEPLOYMENT_ID,
        tenantId: TENANT_ID,
        anchorRunId: DEPLOYMENT_ID,
        definitionId: `wfd_${DEPLOYMENT_ID}`,
        address: deploymentMailAddress,
        // The deploy-time birth state; the first mail send flips it running.
        status: "deployed",
      });
    }

    async function deployWorkflowToSidecar(): Promise<void> {
      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `${DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_integration-1",
        agentAddress: deploymentMailAddress,
        systemPrompt:
          "Fallback prompt (overridden per step by the orchestrator)",
        tools: [],
        grants: [],
        sources: [
          {
            id: "anthropic:mock-model",
            provider: "anthropic",
            baseURL: `http://localhost:${String(env.inference.server.port)}`,
            apiKey: "sk-mock",
            model: "mock-model",
          },
        ],
        defaultSource: "anthropic:mock-model",
      };
      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
      ]);
      const launchSession: LaunchSessionFn = async (p) => {
        await env.hub.sessionService.stageWorkflowStep({
          agentAddress: p.agentAddress,
          agentId: p.agentId,
          runId: p.instanceId,
          config: p.config,
          deployContent: toLaunchDeployContent(p.deployContent),
          ...(p.toolPackagePins !== undefined
            ? { toolPackagePins: p.toolPackagePins }
            : {}),
        });
      };
      const sendMultiStepDeploy: SendMultiStepDeployFn = async (params) =>
        env.hub.router.sendAgentDeploy(params.agentAddress, params.config, {
          definition: {
            id: params.definition.id,
            triggers: [...params.definition.triggers],
            stepOrder: [...params.definition.stepOrder],
            steps: params.definition.steps as Record<string, unknown>,
            ...(params.definition.state !== undefined
              ? { state: params.definition.state }
              : {}),
          },
          sources: params.sources,
        });
      const deploySingleStepAtHead: DeploySingleStepFn = (params) =>
        env.hub.sessionService.deploySingleStepAtHead(params);
      const workflowRepo: WorkflowRepoWriter = {
        async writeWorkflowRepo(args) {
          const repoId: RepoId = { kind: "workflow", id: args.workflowRepoId };
          const principal: WorkflowRunHubPrincipal = { kind: "hub" };
          const files: Record<string, string> = {};
          for (const [k, v] of args.files) files[k] = v;
          await env.hub.agentRepoStore.repoStore.writeTree(
            principal,
            repoId,
            DEFAULT_ASSET_REF,
            {
              files,
              message: `run-mail-send route test: ${args.workflowRepoId}`,
            },
          );
        },
      };
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry: createDefaultDirectorRegistry(),
        workflowRepo,
        launchSession,
        sendMultiStepDeploy,
        deploySingleStepAtHead,
      });
      const result = await orchestrator.deployWorkflow({
        workflow,
        config,
        deployContent: { systemPrompt: config.systemPrompt },
        operatorApprovals,
        deploymentId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        hubPublicKey: "00".repeat(32),
        ...(TOOL_PINS.length > 0 ? { toolPackagePins: TOOL_PINS } : {}),
      });
      if (!result.publicKey) {
        throw new Error(
          `deployWorkflow returned no publicKey\n${env.sidecarDiagnostics()}`,
        );
      }
    }
  },
);
