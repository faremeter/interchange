// The run-grants join, proven by removing the push it replaces.
//
// On the control socket a run's grants reach the sidecar because the hub writes
// them onto the same FIFO channel as the trigger mail, ahead of it. Over SMTP
// there is no shared channel, and losing that ordering is not a delay: the
// supervisor's pre-trigger barrier answers an absent grants file with a
// synthesized `RunFailed`, `RunFailed` is terminal, and a terminal run refuses
// every later trigger. One lost race permanently kills the deployment.
//
// This test SUPPRESSES the hub's proactive `sendRunGrants` entirely. The
// deployment therefore has exactly one way to obtain its authorization: notice
// it is missing when the mail arrives, ask the hub over the control socket
// (`run.grants.request`), and wait for the answer before letting the message
// reach the supervisor. If the join did not work, the run could not start at
// all -- so a terminal success here is attributable to nothing else.
//
// Suppression is applied by wrapping the `SidecarRouter` the HTTP trigger is
// handed. The hub's own request handler calls its internal send directly, so the
// wrapper silences the push without silencing the answer -- which is exactly the
// isolation this test needs.

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

import { createGrantStore, createPrincipalKeyStore } from "@intx/db";
import { tenant as tenantTable } from "@intx/db/schema";
import {
  createApp,
  createMailTriggeredRunGrantsMaterializer,
  type GetSession,
} from "@intx/hub-api";
import {
  createAssetService,
  type EventCollectorRegistry,
  type SidecarRouter,
} from "@intx/hub-sessions";
import { createSmtpRelay } from "@intx/mail-imap";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
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
import { deriveRunAddress } from "@intx/workflow-deploy";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepAgentEntry } from "../workflow-deploy/fixtures/single-step-agent";
import {
  MAIL_SERVER,
  mailServerReachable,
  provisionAddress,
  purgeInbox,
  testMailboxPassword,
  waitForMailboxLogin,
} from "./server";
import { testProvisionMailbox } from "./provisioner";

const DEPLOYMENT_DOMAIN = MAIL_SERVER.domain;
const DEPLOYMENT_ID = "run_grants-barrier-pull-only-1";
const TENANT_ID = "tnt_grants_barrier_pull";
const CALLER_USER_ID = "usr_grants_barrier_pull";
const CALLER_PRINCIPAL_ID = "prn_grants_barrier_pull";
const DEFINITION_ASSET_ID = "ast_grants_barrier_pull_wf";
const STEP_ID = "step1";

const deploymentMailAddress = deriveRunAddress({
  runId: DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});
const callerMailAddress = `${CALLER_USER_ID}@${DEPLOYMENT_DOMAIN}`;

const TriggerResponse = type({
  runId: "string",
  address: "string",
  messageId: "string",
});

const mailReachable = await mailServerReachable();
const canRun = harnessDbEnvAvailable() && mailReachable;

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
      id: "session_grants_barrier_pull",
      userId,
      token: "tok_grants_barrier_pull",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function notImpl(name: string): never {
  throw new Error(`grants-barrier pull-only mock: ${name} not implemented`);
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

describe.skipIf(!canRun)(
  "a run obtains its grants by asking, with no proactive push",
  () => {
    let hasRun = false;

    beforeAll(async () => {
      h = await createTestDb();
      // Only the CALLER's mailbox is pre-created: the relay authenticates as
      // it to submit, and nothing in the deploy path creates it. The
      // DEPLOYMENT's mailbox is deliberately NOT pre-created -- the hub
      // provisions it inside the deploy, with a credential derived per address
      // and delivered on the deploy frame, which is the path under test.
      await provisionAddress(CALLER_USER_ID);
      await purgeInbox(deploymentMailAddress);
      await purgeInbox(callerMailAddress);

      // The REAL materializer and sender-key resolver the production hub wires.
      // Both are what the sidecar's request is answered FROM: without them the
      // hub has nothing to send back, and the barrier would time out on every
      // message rather than on a genuine authorization refusal.
      const principalKeyStore = createPrincipalKeyStore({
        db: h.db,
        cipher: createTestCredentialCipher(),
      });
      env = await startDeployFlowEnv({
        inferenceEchoUserMessage: true,
        materializeMailTriggeredRunGrants:
          createMailTriggeredRunGrantsMaterializer({
            db: h.db,
            principalKeyStore,
            grantStore: createGrantStore(h.db),
          }),
        senderKeyResolution: { db: h.db, principalKeyStore },
        provisionMailbox: testProvisionMailbox(),
        sidecarEnv: {
          SIDECAR_MAIL_BACKEND: "imap",
          SIDECAR_IMAP_HOST: MAIL_SERVER.host,
          SIDECAR_IMAP_PORT: String(MAIL_SERVER.imapPort),
          SIDECAR_SMTP_PORT: String(MAIL_SERVER.smtpPort),
          // No shared mailbox password: the sidecar uses the per-deployment
          // credential the deploy frame carried, and holds no fallback.
        },
      });
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

    test("the run completes although the hub never pushed its grants", async () => {
      if (hasRun) {
        throw new Error(
          "this suite assumes a single test per shared subprocess env; " +
            "add a new scenario in its own file with its own env instead",
        );
      }
      hasRun = true;

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
        refId: CALLER_USER_ID,
        status: "active",
      });
      await seedPrincipalKey(h.db, CALLER_PRINCIPAL_ID);
      await seedAsset(h.db, {
        id: DEFINITION_ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "grants-barrier-pull-wf",
        creatorPrincipalId: CALLER_PRINCIPAL_ID,
      });
      await seedGrant(h.db, {
        id: "grant-pull-caller-manage",
        tenantId: TENANT_ID,
        resource: `workflow-run:${DEPLOYMENT_ID}`,
        action: "manage",
        effect: "allow",
        origin: "system",
        principalId: CALLER_PRINCIPAL_ID,
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
        systemPrompt: "You are the pull-only grants agent.",
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

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      // Suppress the push. Reporting `true` keeps the trigger on its happy path
      // -- a `false` would make it 409 "not routable" and never send the mail --
      // so the deployment is left genuinely unauthorized at the moment its mail
      // arrives, which is the condition under test.
      let suppressedPushes = 0;
      const pushSuppressingRouter: SidecarRouter = {
        ...env.hub.router,
        sendRunGrants: () => {
          suppressedPushes += 1;
          return true;
        },
      };

      const grantStore = createGrantStore(h.db);
      const assetService = createAssetService({
        db: h.db,
        repoStore: env.hub.agentRepoStore.repoStore,
      });
      const relay = createSmtpRelay({
        host: MAIL_SERVER.host,
        port: MAIL_SERVER.smtpPort,
        secure: false,
        ignoreTLS: true,
        auth: {
          user: callerMailAddress,
          pass: await testMailboxPassword(callerMailAddress),
        },
      });

      try {
        const triggerApp = createApp({
          getSession: createMockGetSession(CALLER_USER_ID),
          authHandler: () => new Response("", { status: 404 }),
          db: h.db,
          grantStore,
          sidecarRouter: pushSuppressingRouter,
          sessionService: env.hub.sessionService,
          eventCollectors: createMockEventCollectors(),
          assetService,
          repoStore: env.hub.agentRepoStore.repoStore,
          maxTarballBytes: 10_000_000,
          mailRelay: relay,
        });

        // The hub provisioned the deployment's mailbox inside the deploy above,
        // and that reloads the mail server -- which refuses logins while it
        // runs. The relay authenticates as the caller to submit, so firing
        // straight away races the reload and the submission fails to
        // authenticate.
        //
        // Waiting here is a TEST-SIDE settle for a PRODUCTION hazard: the hub's
        // own provisioning can transiently break its own relay's credentials.
        // The real answer is for the relay to retry a submission that fails to
        // authenticate, which is not built.
        await waitForMailboxLogin(callerMailAddress);

        const res = await triggerApp.request(
          `/api/tenants/${TENANT_ID}/workflows/${DEPLOYMENT_ID}/mail`,
          {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ content: "fire with no grants pushed" }),
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

        // The push really was suppressed, so the assertion below is about the
        // pull and not about a push that happened anyway.
        expect(suppressedPushes).toBe(1);

        // The run could only have started by the sidecar asking for its grants
        // and waiting for the answer. Without the join its mail would have
        // reached the supervisor unauthorized and terminally failed the run.
        // Assert the TYPE, not merely that a terminal event landed:
        // `waitForWorkflowRunComplete` returns on RunFailed and RunCancelled
        // too, so awaiting it alone would pass on the exact failure this test
        // exists to rule out.
        const terminal = await waitForWorkflowRunComplete(
          env,
          DEPLOYMENT_ID,
          DEPLOYMENT_ID,
          { diagnostics: env.sidecarDiagnostics },
        );
        if (terminal.type !== "RunCompleted") {
          throw new Error(
            `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(terminal.body)}\n${env.sidecarDiagnostics()}`,
          );
        }
        expect(terminal.type).toBe("RunCompleted");
      } finally {
        relay.close();
      }
    });
  },
);
