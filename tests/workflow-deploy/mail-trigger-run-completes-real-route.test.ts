// Trigger-route runId contract: a real-route run reaches terminal SUCCESS.
//
// The supervisor keys a run on the deployment's mail address as the stable
// runId and reads that run's grants from `runs/<runId>/grants.json`; the
// sidecar's `onRunStart` barrier refuses to start a run whose grants file
// is absent at that path. So a producer that stages the run's grants under
// any OTHER id leaves the supervisor's path empty and the run fails closed.
//
// This test drives the PRODUCTION `POST /workflows/:deploymentId/mail`
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
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
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
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

// The tenant domain must equal the fixture's deploy domain so the route's
// derived address (`ins_<deploymentId>@<tenant.domain>`) matches the
// address the fixture deployed the sidecar workflow under; otherwise the
// route's sendRunGrants/routeMail target an unknown address (409).
const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_mail-trigger-run-completes-1";
const TENANT_ID = "tnt_mail_trigger_completes";
const CALLER_USER_ID = "usr_mail_trigger_completes_caller";
const CALLER_PRINCIPAL_ID = "prn_mail_trigger_completes_caller";
const DEFINITION_ASSET_ID = "ast_mail_trigger_completes_wf";
const STEP_ID = "step1";

const TOOL_PINS: readonly ToolPackagePin[] = [];

const deploymentMailAddress = deriveRunAddress({
  runId: DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

// A single agent step with no tools: the run completes on the echo
// inference alone, so the ONLY thing that can stop it reaching
// RunCompleted is the grants barrier failing to find the run's grants.
const agent = defineAgent({
  id: `agent_${DEPLOYMENT_ID}`,
  systemPrompt: "You are the single-step run-completes agent.",
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

// The trigger route's 202 body shape. Validated rather than cast so a
// route response drift surfaces at the boundary.
const TriggerResponse = type({
  deploymentId: "string",
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

// A real RepoStore carrying both kinds the `/mail` route reads: the workflow
// asset's `workflow.json` and the deployment's workflow-run lifecycle.
async function createWorkflowRepoStore(): Promise<RepoStore> {
  const dataDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "mail-trigger-completes-"),
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
  "a mail-triggered run started through the real route reaches RunCompleted",
  () => {
    let hasRun = false;

    beforeAll(async () => {
      signingKey = await generateKeyPair();
      h = await createTestDb();
      // Echo inference so the single agent step produces a reply and the
      // run terminates on its own once its grants barrier is satisfied.
      env = await startDeployFlowEnv({ inferenceEchoUserMessage: true });
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
      // `workflow-run:<id>/manage` grant the `/mail` route requires.
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

      // Write the workflow's `workflow.json` into the hub-api asset the
      // route hydrates its grants from.
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

      // Deploy the same workflow to the real sidecar so its address is
      // routable and its workflow-run repo exists, then register it with the
      // fixture so run events can be read for the completion wait.
      await deployWorkflowToSidecar();

      // Seed the deployment's definition + anchor run after the asset and
      // sidecar deploy so the FK targets exist for the route's commit.
      await seedDeploymentRow();

      const grantStore = createGrantStore(h.db);
      const triggerApp = createApp({
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
      // (id == anchorRunId == deploymentId) -- the anchor IS the run.
      const runRows = await h.db.select().from(workflowRunTable);
      expect(runRows).toHaveLength(1);
      expect(runRows[0]?.id).toBe(DEPLOYMENT_ID);
      expect(runRows[0]?.anchorRunId).toBe(DEPLOYMENT_ID);
    });

    // Seed the deployment's first-class definition and its anchor run at the
    // fixture's deploy address: the trigger route reads the workflow asset
    // and the run's definition off the anchor.
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
        status: "running",
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
              message: `mail-trigger run-completes test: ${args.workflowRepoId}`,
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
      const workflowRunRepoId: RepoId = {
        kind: "workflow-run",
        id: deriveDeploymentId(deploymentMailAddress),
      };
      env.registerDeployment({
        deploymentId: DEPLOYMENT_ID,
        workflowDefinition: workflow,
        workflowRunRepoId,
        workflowRunRef: "refs/heads/main",
        mailAddress: deploymentMailAddress,
      });
    }
  },
);
