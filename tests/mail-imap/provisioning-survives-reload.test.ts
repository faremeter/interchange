// A live deployment keeps receiving mail across another deployment's
// provisioning.
//
// Creating a mailbox on Dovecot reloads it, and the reload SIGTERMs every live
// IMAP session on the server. Since the hub provisions a mailbox in the deploy
// path, that happens every time a deployment is created -- so a deployment that
// is already running loses its command and watch connections at that moment,
// through no fault of its own.
//
// `ImapFlow` cannot reopen a closed connection, so without recovery the victim
// is finished: its watch is gone, its commands fail, and it stops receiving
// mail silently and permanently. `createImapHubTransport` answers this by
// rebuilding the transport and re-running the backlog sweep, which is why a
// drop costs latency rather than mail.
//
// The sequence is: deploy, force the reload by provisioning an UNRELATED
// mailbox, confirm the deployment noticed and rebuilt, then fire its one
// trigger. The trigger is the assertion -- it can only be received over a
// connection established after the reload, because the original was terminated
// and `ImapFlow` cannot reopen it.
//
// The reload comes BEFORE the trigger rather than between two of them: a
// deployment owns exactly one top-level run, and once that run is terminal the
// deployment refuses every later trigger. There is no second firing to use.

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
} from "@intx/hub-sessions";
import { createSmtpRelay } from "@intx/mail-imap";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
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
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
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
  waitForOpenSession,
} from "./server";
import { testProvisionMailbox } from "./provisioner";

const DEPLOYMENT_DOMAIN = MAIL_SERVER.domain;
const DEPLOYMENT_ID = "run_provisioning-survives-1";
const TENANT_ID = "tnt_provisioning_survives";
const CALLER_USER_ID = "usr_provisioning_survives";
const CALLER_PRINCIPAL_ID = "prn_provisioning_survives";
const DEFINITION_ASSET_ID = "ast_provisioning_survives_wf";
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
      id: "session_provisioning_survives",
      userId,
      token: "tok_provisioning_survives",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function notImpl(name: string): never {
  throw new Error(`provisioning-survives mock: ${name} not implemented`);
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
  "a live deployment survives another deployment's mailbox provisioning",
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

    test("its one trigger still arrives after a reload drops its connections", async () => {
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
        name: "provisioning-survives-wf",
        creatorPrincipalId: CALLER_PRINCIPAL_ID,
      });
      await seedGrant(h.db, {
        id: "grant-provisioning-survives-manage",
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
      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: singleStepAgentEntry({
          stepId: STEP_ID,
          systemPrompt: "You are the reload-survival agent.",
          address: deploymentMailAddress,
          agentId: `agent_${DEPLOYMENT_ID}`,
        }),
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
          grantStore: createGrantStore(h.db),
          sidecarRouter: env.hub.router,
          sessionService: env.hub.sessionService,
          eventCollectors: createMockEventCollectors(),
          assetService: createAssetService({
            db: h.db,
            repoStore: env.hub.agentRepoStore.repoStore,
          }),
          repoStore: env.hub.agentRepoStore.repoStore,
          maxTarballBytes: 10_000_000,
          mailRelay: relay,
        });

        // The reload can only drop connections that exist, and the deploy
        // resolving does not mean the sidecar's login has finished -- a login
        // refused by the deploy's OWN provisioning reload is retried, so it can
        // still be in flight here. Forcing the second reload inside that window
        // drops nothing: the login absorbs both reloads, connects cleanly, and
        // the wait below then never sees a loss.
        await waitForOpenSession(deploymentMailAddress);

        // Force the reload by provisioning an UNRELATED mailbox -- the same
        // thing the hub now does inside every deploy. This is what terminates
        // the live deployment's command and watch connections.
        await provisionAddress(`run_reload-victim-${String(Date.now())}`);

        // Confirm the hazard actually fired and the recovery ran. Without this
        // the test could pass vacuously on a server that did not drop anything,
        // and would then prove nothing about reconnecting.
        await waitFor(
          () =>
            /lost its connection|reconnecting the mailbox/.test(
              env.sidecarDiagnostics(),
            ),
          {
            diagnostics: () =>
              `the sidecar never reported losing its mailbox connection, so the reload hazard did not reproduce and this test proves nothing\n${env.sidecarDiagnostics()}`,
          },
        );

        // Fire the deployment's one trigger. Receiving it requires a rebuilt
        // IMAP connection: the ingress observes the arrival over the watch
        // connection and reads the bytes over the command connection, and both
        // of the originals are gone.
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
            body: JSON.stringify({ content: "fired after the reload" }),
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

        // And the message was consumed off the server, which only the rebuilt
        // ingress does.
        expect(await countInbox(deploymentMailAddress)).toBe(0);
      } finally {
        relay.close();
      }
    });
  },
);

/** Messages currently in an address's INBOX, read through the server's own tool. */
async function countInbox(address: string): Promise<number> {
  const proc = Bun.spawn(
    [
      "docker",
      "exec",
      MAIL_SERVER.container,
      "doveadm",
      "search",
      "-u",
      address,
      "mailbox",
      "INBOX",
      "all",
    ],
    { stdout: "pipe", stderr: "pipe" },
  );
  await proc.exited;
  const stdout = await new Response(proc.stdout).text();
  return stdout.split("\n").filter((line) => line.trim() !== "").length;
}
