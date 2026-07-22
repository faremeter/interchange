// Trigger-route grant derivation + commit, under real constraints.
//
// INTR-339's headline: when a workflow run is triggered, the run's
// authorization grants are DERIVED from the deployment's definition and
// committed to Postgres before the run dispatches. This test drives that
// derivation through the PRODUCTION `POST /workflows/:deploymentId/mail`
// route against a real migrated schema and a real sidecar subprocess:
//
//   - The workflow's single ACTION step declares `effect:{requires:
//     ["fs:write"]}`. The deploy-time capability walk lifts that into a
//     `effect:fs:write` runtime grant; the trigger route materializes it
//     onto a fresh run principal and commits the principal + run + grant
//     rows in one transaction (`commitRunGrants`). The route declares NO
//     `grants` inline -- the `effect:fs:write` row exists only because the
//     WALK derived it, which is the property under test.
//   - `createApp` is wired with the real `h.db`, a real `assetService` +
//     `repoStore` (so the `/workflows` routes mount and `hydrateDefinition`
//     reads the deployment's `workflow.json`), a real DB-backed grant store,
//     and the fixture's real `env.hub.router`. The route's `sendRunGrants`
//     and `routeMail` therefore reach the SAME deployed sidecar the fixture
//     stood up, so a 202 means the run was genuinely accepted for dispatch.
//   - The committed rows insert under real foreign keys: `workflow_run`'s
//     `deployment_id` references `workflow_deployment.id`, and the run
//     principal is a real `principal` row. A broken derivation or a wrong
//     deployment id fails at the DB, not at a mock.
//
// SCOPE. This proves the route's grant DERIVATION + DB COMMIT under real
// constraints. It does NOT exercise the runtime CONSUME side: the sidecar
// workflow host does not execute action primitives (no `invokeAction` is
// wired on the production run env), so the dispatched run fails its action
// step with "this host does not support action primitives" and never runs
// the effect. That downstream failure is expected and irrelevant here --
// the derivation and commit both complete before the run dispatches, and
// those are what this test asserts. The action-effect authorization path
// (`EffectContext.perform` gating `effect:fs:write`) only runs under the
// in-process `runLocal` host and is covered there.

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
import { and, eq } from "drizzle-orm";
import { type } from "arktype";

import { createDefaultDirectorRegistry } from "@intx/agent";
import { createGrantStore } from "@intx/db";
import {
  grant as grantTable,
  principal as principalTable,
  tenant as tenantTable,
  workflowDeployment as workflowDeploymentTable,
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
  type AuthorizeFn,
  type EventCollectorRegistry,
  type RepoId,
  type RepoStore,
  type SessionService,
  type WorkflowRunHubPrincipal,
} from "@intx/hub-sessions";
import type { KeyPair } from "@intx/types/runtime";
import type { HarnessConfig } from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedGrant, seedPrincipal } from "@intx/test-harness/seed";
import {
  action,
  defineWorkflow,
  type WorkflowDefinition,
} from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveDeploymentAddress,
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
// derived address (`ins_<deploymentId>@<tenant.domain>`) matches the
// address the fixture deployed the sidecar workflow under; otherwise
// `sendRunGrants`/`routeMail` route to an unknown address and the route
// returns 409 instead of 202.
const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "mail-trigger-derived-grants-1";
const TENANT_ID = "tnt_mail_trigger_derived";
const CALLER_USER_ID = "usr_mail_trigger_caller";
const CALLER_PRINCIPAL_ID = "prn_mail_trigger_caller";
const DEFINITION_ASSET_ID = "ast_mail_trigger_wf";
const STEP_ID = "act";

const TOOL_PINS: readonly ToolPackagePin[] = [];

const deploymentMailAddress = deriveDeploymentAddress({
  deploymentId: DEPLOYMENT_ID,
  deploymentDomain: DEPLOYMENT_DOMAIN,
});

// The workflow the deployment carries: a single action step whose effect
// requires `fs:write`. The capability walk derives `effect:fs:write` from
// this; no `grants` are declared inline, so the derived row is the ONLY
// source of that grant. The handler ref is inert here (the run never
// executes the action on this host).
const workflow: WorkflowDefinition = defineWorkflow({
  id: `wf_${DEPLOYMENT_ID}`,
  trigger: { type: "mail", to: deploymentMailAddress },
  steps: {
    [STEP_ID]: action({
      handler: "writer",
      effect: { requires: ["fs:write"] },
    }),
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
      id: "session_mail_trigger",
      userId,
      token: "tok_mail_trigger",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function notImpl(name: string): never {
  throw new Error(`mail-trigger derived-grants mock: ${name} not implemented`);
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

// A real RepoStore carrying the workflow kind handler, so a workflow asset
// can be created and its `workflow.json` written and read back. The `/mail`
// route's `hydrateDefinition` reads the definition through this store.
async function createWorkflowRepoStore(): Promise<RepoStore> {
  const dataDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "mail-trigger-derived-"),
  );
  tempDirs.push(dataDir);
  const signer = async (payload: string) =>
    createSSHSignature(payload, signingKey.privateKey, signingKey.publicKey);
  const authorize: AuthorizeFn = (principal, repoId, ref, act) => {
    if (repoId.kind === "workflow") {
      return workflowAuthorize(principal, repoId, ref, act);
    }
    return { allowed: false, reason: `no authorize for ${repoId.kind}` };
  };
  return createRepoStore({
    dataDir,
    signingKey,
    handlers: { workflow: workflowKindHandler },
    authorize,
    signingCallback: () => signer,
  });
}

describe.skipIf(!harnessDbEnvAvailable())(
  "mail trigger derives and commits run grants under real constraints",
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

    test("commits the run principal, run row, and derived effect grant", async () => {
      // Single shared sidecar subprocess: guard against a second test
      // reusing the warm deploy state under a different DB reset.
      if (hasRun) {
        throw new Error(
          "this suite assumes a single test per shared subprocess env; " +
            "add a new scenario in its own file with its own env instead",
        );
      }
      hasRun = true;

      // Seed the tenancy the route resolves against. The tenant carries
      // the deploy domain so the derived address matches the sidecar
      // deployment; the caller is an active user-principal (for
      // resolveTenant) holding the `workflow-run:<id>/manage` grant the
      // `/mail` route's middleware requires.
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
        name: "mail-trigger-derived-wf",
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
      // Deploy the workflow asset's `workflow.json` through a real asset
      // service so the route can hydrate it.
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

      // Deploy the workflow to the real sidecar so its address is routable
      // and its workflow-run repo exists. The route's sendRunGrants +
      // routeMail reach this deployment.
      await deployWorkflowToSidecar();

      // Seed the deployment row after the asset + sidecar deploy so the
      // FK target exists for commitRunGrants.
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
          body: JSON.stringify({ content: "kick off" }),
        },
      );
      if (res.status !== 202) {
        const body: unknown = await res.json();
        throw new Error(
          `expected 202 from /mail, got ${String(res.status)}: ${JSON.stringify(body)}\n${env.sidecarDiagnostics()}`,
        );
      }
      const rawJson: unknown = await res.json();
      const json = TriggerResponse.assert(rawJson);
      expect(json.deploymentId).toBe(DEPLOYMENT_ID);
      expect(json.address).toBe(deploymentMailAddress);
      // The route mints runId = messageId verbatim.
      const runId = json.messageId;

      // ---- The run principal committed (kind workflow, refId = runId) ----
      const principals = await h.db
        .select()
        .from(principalTable)
        .where(
          and(
            eq(principalTable.tenantId, TENANT_ID),
            eq(principalTable.kind, "workflow"),
            eq(principalTable.refId, runId),
          ),
        );
      expect(principals).toHaveLength(1);
      const runPrincipal = principals[0];
      if (runPrincipal === undefined) throw new Error("unreachable");

      // ---- The run row committed, FK'd to the real deployment ----
      const runs = await h.db
        .select()
        .from(workflowRunTable)
        .where(eq(workflowRunTable.id, runId));
      expect(runs).toHaveLength(1);
      const runRow = runs[0];
      if (runRow === undefined) throw new Error("unreachable");
      expect(runRow.deploymentId).toBe(DEPLOYMENT_ID);
      expect(runRow.tenantId).toBe(TENANT_ID);
      expect(runRow.principalId).toBe(runPrincipal.id);

      // ---- The DERIVED effect grant committed on the run principal ----
      const grants = await h.db
        .select()
        .from(grantTable)
        .where(eq(grantTable.principalId, runPrincipal.id));
      const effectGrant = grants.find((g) => g.resource === "effect:fs:write");
      if (effectGrant === undefined) {
        throw new Error(
          `no effect:fs:write grant committed on the run principal; got ${JSON.stringify(
            grants.map((g) => g.resource),
          )}`,
        );
      }
      expect(effectGrant.action).toBe("invoke");
      expect(effectGrant.effect).toBe("allow");
      expect(effectGrant.origin).toBe("creator");
    });

    // Insert the deployment row directly so the address matches the
    // fixture's deploy address (seedWorkflowDeployment defaults to a
    // different domain).
    async function seedDeploymentRow(): Promise<void> {
      await h.db.insert(workflowDeploymentTable).values({
        id: DEPLOYMENT_ID,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        address: deploymentMailAddress,
        publicKey: null,
        status: "deployed",
      });
    }

    async function deployWorkflowToSidecar(): Promise<void> {
      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `ins_${DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_integration-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "Fallback",
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
        "effect:fs:write",
      ]);
      const launchSession: LaunchSessionFn = async (p) => {
        await env.hub.sessionService.stageWorkflowStep({
          agentAddress: p.agentAddress,
          agentId: p.agentId,
          instanceId: p.instanceId,
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
              message: `mail-trigger derived test: ${args.workflowRepoId}`,
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
