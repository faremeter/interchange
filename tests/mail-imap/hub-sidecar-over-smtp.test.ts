// The hub and a real sidecar subprocess moving workflow mail over SMTP and IMAP.
//
// Everything here is production code except the mail server and the inference
// backend: a real hub app, a real sidecar subprocess, a real deployed workflow,
// a real Postfix relay and a real Dovecot mailbox. Mail does NOT travel over the
// hub control socket in either direction.
//
//   OUTBOUND from the hub: the `POST /workflows/:runId/mail` trigger assembles
//   and signs the message exactly as it always does, then SUBMITS IT OVER SMTP
//   instead of pushing a `mail.inbound` frame. The run's grants still go over
//   the socket, and still first.
//
//   INBOUND at the sidecar: the sidecar runs with `SIDECAR_MAIL_BACKEND=imap`,
//   so its host transport is a real IMAP client. Its ingress observes the
//   arrival through IDLE, reads the verbatim bytes, runs the inbound-mail
//   admission gate, and hands them to the deployment's mail router -- the same
//   entry point the socket seam uses.
//
// The assertion is the one that cannot be faked: the run reaches RunCompleted.
// That requires the trigger mail to have travelled SMTP, been found over IMAP,
// passed admission, reached the supervisor, started the run under grants that
// arrived on a different transport, and driven the agent step to termination.

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
import { tenant as tenantTable } from "@intx/db/schema";
import { createApp, type GetSession } from "@intx/hub-api";
import {
  createAssetService,
  type EventCollectorRegistry,
} from "@intx/hub-sessions";
import { createSmtpRelay } from "@intx/mail-imap";
import { ImapFlow } from "imapflow";
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
import { createApprovalSet, deriveRunAddress } from "@intx/workflow-deploy";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { MAIL_TOOL_NAME } from "../workflow-deploy/fixtures/mail-tool";
import { singleStepMailToolEntry } from "../workflow-deploy/fixtures/single-step-mail-tool";
import {
  MAIL_SERVER,
  mailServerReachable,
  provisionAddress,
  purgeInbox,
  testMailboxPassword,
  waitForMailboxLogin,
} from "./server";
import { testProvisionMailbox } from "./provisioner";

// The tenant domain must be the domain the mail server hosts: the deployment's
// derived address is `<anchorRunId>@<tenant.domain>`, and that address has to be
// a real mailbox on the server for the sidecar to log into it.
const DEPLOYMENT_DOMAIN = MAIL_SERVER.domain;
const DEPLOYMENT_ID = "run_smtp-imap-hub-sidecar-1";
const TENANT_ID = "tnt_smtp_imap_hub_sidecar";
const CALLER_USER_ID = "usr_smtp_imap_caller";
const CALLER_PRINCIPAL_ID = "prn_smtp_imap_caller";
const DEFINITION_ASSET_ID = "ast_smtp_imap_wf";
const STEP_ID = "step1";

const deploymentMailAddress = deriveRunAddress({
  runId: DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

// The trigger's sender of record: the hub stamps `<principal.refId>@<domain>`
// as the From and signs with that principal's key. It needs a real mailbox too,
// because the relay authenticates as the sender it submits for.
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
      id: "session_smtp_imap",
      userId,
      token: "tok_smtp_imap",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function notImpl(name: string): never {
  throw new Error(`smtp-imap hub/sidecar mock: ${name} not implemented`);
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

/**
 * Read the deployment's real mailbox with an independent IMAP client, so what
 * the test asserts about the server's contents comes from the server rather
 * than from the transport under test.
 */
async function readInbox(address: string): Promise<
  {
    messageId: string | undefined;
    from: string | undefined;
    subject: string | undefined;
    flags: string[];
  }[]
> {
  const client = new ImapFlow({
    host: MAIL_SERVER.host,
    port: MAIL_SERVER.imapPort,
    secure: false,
    auth: { user: address, pass: await testMailboxPassword(address) },
    logger: false,
  });
  await client.connect();
  try {
    const lock = await client.getMailboxLock("INBOX");
    try {
      const out: {
        messageId: string | undefined;
        from: string | undefined;
        subject: string | undefined;
        flags: string[];
      }[] = [];
      for await (const message of client.fetch(
        "1:*",
        { uid: true, flags: true, envelope: true },
        { uid: true },
      )) {
        out.push({
          messageId: message.envelope?.messageId,
          from: message.envelope?.from?.[0]?.address,
          subject: message.envelope?.subject,
          flags: [...(message.flags ?? [])],
        });
      }
      return out;
    } finally {
      lock.release();
    }
  } finally {
    await client.logout();
  }
}

describe.skipIf(!canRun)(
  "a workflow run fires from mail delivered over real SMTP and IMAP",
  () => {
    let hasRun = false;

    beforeAll(async () => {
      h = await createTestDb();

      // Both mailboxes exist before anything connects. Provisioning reloads
      // Dovecot and drops live IMAP sessions, so doing it up front keeps that
      // disruption away from the sidecar's own connection. A production
      // provisioner cannot arrange that -- it creates an address while other
      // deployments are connected -- which is why the sidecar's ingress carries
      // a catch-up sweep rather than trusting IDLE alone.
      // Only the CALLER's mailbox is pre-created: the relay authenticates as
      // it to submit, and nothing in the deploy path creates it. The
      // DEPLOYMENT's mailbox is deliberately NOT pre-created -- the hub
      // provisions it inside the deploy, with a credential derived per address
      // and delivered on the deploy frame, which is the path under test.
      await provisionAddress(CALLER_USER_ID);
      // The addresses are fixed, so a previous run's mail is still in the
      // mailbox. Clear it before the sidecar connects, or its catch-up sweep
      // ingests that backlog into this run.
      await purgeInbox(deploymentMailAddress);
      await purgeInbox(callerMailAddress);

      env = await startDeployFlowEnv({
        // Drive the step agent to call its inline `mail_send`, which sends
        // through the supervisor-backed transport -- so the reply leaves over
        // SMTP. The recipient is the caller's mailbox, an address this sidecar
        // does NOT host: with the in-memory backend that would need the hub to
        // take over a "remote leg", and over SMTP there is no such concept.
        inferenceToolCall: {
          toolName: MAIL_TOOL_NAME,
          input: { to: callerMailAddress, body: "reply-receipt.json" },
        },
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

    test("the run reaches RunCompleted with no mail on the control socket", async () => {
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
      // The caller signs the trigger mail with its durable principal key. The
      // recipient verifies that signature locally against the key the hub
      // co-delivers on the run's grants barrier, so this is what makes the
      // message `clean` at the admission gate rather than `unknown`.
      await seedPrincipalKey(h.db, CALLER_PRINCIPAL_ID);
      await seedAsset(h.db, {
        id: DEFINITION_ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "smtp-imap-wf",
        creatorPrincipalId: CALLER_PRINCIPAL_ID,
      });
      await seedGrant(h.db, {
        id: "grant-smtp-imap-caller-manage",
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
      const entryModule = singleStepMailToolEntry({
        variant: "transport",
        stepId: STEP_ID,
        systemPrompt: "You are the SMTP/IMAP round-trip agent.",
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
        approvals: createApprovalSet([
          "inference.source:anthropic:mock-model",
          "director:@intx/agent/default",
          `mail.address:${deploymentMailAddress}`,
          `mail.send:${DEPLOYMENT_DOMAIN}`,
          `tool:${MAIL_TOOL_NAME}`,
        ]),
        config,
        sources: { [STEP_ID]: [inferenceSource] },
      });
      expect(handle.publicKey).toBeTruthy();

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      const grantStore = createGrantStore(h.db);
      const assetService = createAssetService({
        db: h.db,
        repoStore: env.hub.agentRepoStore.repoStore,
      });

      // The whole point: the trigger's mail leg is an SMTP submission. Supplying
      // `mailRelay` is the ONLY difference from the socket-path trigger test.
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
          sidecarRouter: env.hub.router,
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
            body: JSON.stringify({ content: "kick off the run over SMTP" }),
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

        // The run fired from a message the hub never put on the socket. Nothing
        // short of the full SMTP -> Dovecot -> IMAP IDLE -> admission ->
        // supervisor path reaching the agent produces a terminal success here.
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

        // The socket carried AUDIT only, never a routing request. The fixture
        // records a `mail.outbound` frame through `persistMail`, which the hub
        // reaches only for a `delivered: true` frame -- a routing request takes
        // the other branch entirely. So a recorded entry is by construction an
        // audit forward, and the trigger's own inbound leg produced none at all
        // because the relay path returns before `routeMail` is reached.
        //
        // That the list is NON-empty is the point: an earlier cut of this
        // backend fired no message-sent handler, so agent-sent mail reached no
        // hub audit record at all. This asserts that parity is back.
        const auditedReplies = env.hub.outboundMail.filter(
          (entry) => entry.senderAddress === deploymentMailAddress,
        );
        expect(auditedReplies.length).toBeGreaterThan(0);
        expect(auditedReplies[0]?.recipients).toContain(callerMailAddress);

        // Inbound evidence: the trigger message is GONE from the deployment's
        // real mailbox. The ingress expunges on acceptance, so a drained
        // mailbox plus a completed run means something read the message off
        // the server and consumed it -- which only the IMAP ingress does.
        const inbox = await readInbox(deploymentMailAddress);
        expect(inbox.some((entry) => entry.messageId === json.messageId)).toBe(
          false,
        );

        // The other direction: the agent's own reply left over SMTP and is in
        // the CALLER's mailbox. That address is not hosted by this sidecar, so
        // on the in-memory backend the send would have needed the hub to carry
        // a remote leg over the socket. Here the relay routed it.
        await waitFor(
          async () => {
            const callerInbox = await readInbox(callerMailAddress);
            return callerInbox.some(
              (entry) => entry.from === deploymentMailAddress,
            );
          },
          { diagnostics: env.sidecarDiagnostics },
        );
      } finally {
        relay.close();
      }
    });
  },
);
