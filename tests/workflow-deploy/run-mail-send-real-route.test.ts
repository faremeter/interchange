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
//
// The deployment is stood up BY SOURCE-REF through `deployWorkflowSourceForTest`
// (bundle a source entry module into a hub asset, probe + approve + freeze it
// against the real DB, deploy the source-ref frame, insert the anchor
// `workflow_run` row). That is the single code-sourced deploy front; the run
// surface then triggers the resulting anchor run.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
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
  type SessionService,
  type SidecarRouter,
} from "@intx/hub-sessions";
import {
  parseHeaderSection,
  parseMimePart,
  parseMultipart,
  extractBoundary,
} from "@intx/mime";
import { verifyDetachedSignature } from "@intx/crypto";
import { base64Decode, hexDecode } from "@intx/types";
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
} from "@intx/test-harness/seed";
import { deriveRunAddress } from "@intx/workflow-deploy";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  startDeployFlowEnv,
  waitFor,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepAgentEntry } from "./fixtures/single-step-agent";

// The tenant domain must equal the fixture's deploy domain so the route's
// derived address (`run_<anchorRunId>@<tenant.domain>`) matches the address
// the fixture deployed the sidecar workflow under; otherwise the route's
// sendRunGrants/routeMail target an unknown address (409).
const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_run-mail-send-route-1";
const TENANT_ID = "tnt_run_mail_send_route";
const CALLER_USER_ID = "usr_run_mail_send_route_caller";
const CALLER_PRINCIPAL_ID = "prn_run_mail_send_route_caller";
const DEFINITION_ASSET_ID = "ast_run_mail_send_route_wf";
const STEP_ID = "step1";
const AGENT_ID = "agent-run-mail-send-route";

const deploymentMailAddress = deriveRunAddress({
  runId: DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
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
let callerPublicKey = "";

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

describe.skipIf(!harnessDbEnvAvailable())(
  "a mail send through the run-surface route reaches the deployment's run",
  () => {
    let hasRun = false;

    beforeAll(async () => {
      h = await createTestDb();
      // Seed the tenancy the route resolves against: the tenant carries the
      // deploy domain so the derived address matches the sidecar deployment,
      // the caller is an active user-principal holding the
      // `workflow-run:<id>/manage` grant the run mail-send route requires, and
      // the `workflow`-kind asset is the one the frozen definition projects
      // over (its `creatorPrincipalId` is where the trigger hydrates creator
      // grants from). The install/approve freeze and the anchor `workflow_run`
      // insert both write against these, so they exist before the deploy runs.
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
      // The caller signs the trigger mail with its durable hub principal key,
      // which production mints at principal creation; the direct row insert
      // above bypasses that, so mint it here or the trigger's sign() throws.
      // The returned public key anchors the outbound-signature assertion below.
      callerPublicKey = await seedPrincipalKey(h.db, CALLER_PRINCIPAL_ID);
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

      env = await startDeployFlowEnv();
    });

    afterAll(async () => {
      if (env !== undefined) await env.teardown();
      if (h !== undefined) await h.close();
    });

    test("the run-surface send is accepted and reconciles onto the single anchored run", async () => {
      if (hasRun) {
        throw new Error(
          "this suite assumes a single test per shared subprocess env; " +
            "add a new scenario in its own file with its own env instead",
        );
      }
      hasRun = true;

      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${env.inference.server.port}`,
        credentialId: "sk-mock",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `${DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_integration-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      // A single agent step with no tools: the send only needs to reach the
      // deployed sidecar and be accepted for dispatch, so no completion is
      // awaited here.
      const entryModule = singleStepAgentEntry({
        stepId: STEP_ID,
        systemPrompt: "You are the run-mail-send route agent.",
        address: deploymentMailAddress,
        agentId: AGENT_ID,
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
      // so routability is asynchronous. Wait for it before the run-surface send.
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
      // Capture the base64 the trigger hands to routeMail so the outbound
      // signature can be verified, while still delegating to the real router so
      // the send reaches the sidecar and the reconciliation assertions hold.
      let capturedMail: string | undefined;
      const capturingRouter: SidecarRouter = {
        ...env.hub.router,
        routeMail(agentAddress, rawMessage, messageId) {
          capturedMail = rawMessage;
          return env.hub.router.routeMail(agentAddress, rawMessage, messageId);
        },
      };
      const runApp = createApp({
        getSession: createMockGetSession(CALLER_USER_ID),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        grantStore,
        sidecarRouter: capturingRouter,
        sessionService: createMockSessionService(),
        eventCollectors: createMockEventCollectors(),
        assetService,
        repoStore: env.hub.agentRepoStore.repoStore,
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
      // (id == anchorRunId == DEPLOYMENT_ID) -- the anchor IS the run.
      const runRows = await h.db.select().from(workflowRunTable);
      expect(runRows).toHaveLength(1);
      expect(runRows[0]?.id).toBe(DEPLOYMENT_ID);
      expect(runRows[0]?.anchorRunId).toBe(DEPLOYMENT_ID);

      // The trigger signs hub-originated mail with the caller's durable hub
      // principal key: the outbound message's detached signature verifies
      // against the caller principal's public key, not a per-call throwaway
      // key, so a user sender resolves to the key that signed its mail.
      if (capturedMail === undefined) {
        throw new Error("routeMail was not called; no outbound mail captured");
      }
      const rawMail = base64Decode(capturedMail);
      const { headers, bodyOffset } = parseHeaderSection(rawMail);
      const contentType = headers.get("content-type");
      if (contentType === undefined) {
        throw new Error("captured mail has no content-type header");
      }
      const boundary = extractBoundary(contentType);
      if (!boundary) {
        throw new Error("captured mail has no multipart boundary");
      }
      const parts = parseMultipart(rawMail.slice(bodyOffset), boundary);
      expect(parts).toHaveLength(2);
      const signedPart = parts[0];
      const signaturePart = parts[1];
      if (signedPart === undefined || signaturePart === undefined) {
        throw new Error(
          "captured mail is not a two-part multipart/signed body",
        );
      }
      const parsedSignature = parseMimePart(signaturePart);
      const verified = await verifyDetachedSignature(
        signedPart,
        parsedSignature.body,
        hexDecode(callerPublicKey),
      );
      expect(verified).toBe(true);
    });
  },
);
