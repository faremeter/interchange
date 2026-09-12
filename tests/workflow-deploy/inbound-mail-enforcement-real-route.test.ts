// Inbound-mail signature ENFORCEMENT, end to end against the real seam.
//
// The recipient-side `mail.inbound` seam verifies every inbound frame's
// signature against the key the hub co-delivered for its `authenticatedSender`,
// reduces the verdict to an admission outcome, and consults the recipient
// deployment's RESOLVED inbound-mail policy: it delivers an admitted outcome and
// DROPS everything else. The per-workflow authored `inboundMailPolicy` is the
// only switch on that decision -- there is no admit-all flag, and a deployment
// that authored no policy rejects every non-`clean` outcome.
//
// This test pins that switch through the production seam with two real sidecar
// deployments on one hub:
//
//   D1  authored NO policy (the secure default: reject-unless-verified).
//   D2  authored `{ missing: "admit" }`.
//
// and drives three scenarios sequentially (one shared subprocess env means one
// test):
//
//   (a) VALID  -> ADMITTED on D1. The production `POST /workflows/:id/mail`
//       route signs the trigger with the caller's durable principal key and
//       co-delivers that key on the run's grants barrier, so the recipient
//       verifies the signature against a resolved key and the visible From binds
//       to the caller: verdict `clean`, which every policy admits. The run
//       reaches `RunCompleted` and the trigger's messageId is consumed.
//   (b) UNSIGNED -> DROPPED on D1. A plain text/plain message (no
//       `multipart/signed` body) verifies `missing` ONCE a key is cached for the
//       sender -- so the scenario co-delivers a key on the grants barrier the way
//       the production dispatch does, making the outcome `missing` rather than the
//       cache-miss `unknown`. D1's default policy rejects `missing`, so the seam
//       drops it: the sidecar logs the reject, no `RunStarted` carries the
//       message, and the message is never consumed.
//   (c) SAME UNSIGNED shape -> ADMITTED on D2. Unsigned text/plain mail of the
//       same shape (its own messageId) routed at D2, whose `{ missing: "admit" }`
//       relaxes exactly that outcome. The seam admits it: the run starts carrying
//       the messageId, is consumed, and reaches `RunCompleted`. The identical
//       message D1 dropped, D2 admits -- the per-workflow policy is the switch.
//
// Scenarios (b)/(c) inject at the hub router's `sendRunGrants` +/`routeMail`
// seam (the same surface `mail-edge-cases` uses) to hand the seam arbitrary raw
// bytes with a chosen `authenticatedSender`; `buildMinimalMail` and
// `waitForConsumedFilename` mirror that file's local helpers verbatim.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { type } from "arktype";

import { createGrantStore } from "@intx/db";
import { tenant as tenantTable } from "@intx/db/schema";
import { createApp, type GetSession } from "@intx/hub-api";
import {
  createAssetService,
  type EventCollectorRegistry,
  type RepoId,
} from "@intx/hub-sessions";
import { base64Encode, deriveWorkflowRunId, hexEncode } from "@intx/types";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { generateKeyPair } from "@intx/crypto";
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
  readClaimCheckDir,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepAgentEntry } from "./fixtures/single-step-agent";

// The tenant domain must equal the fixture's deploy domain so the derived
// address (`<anchorRunId>@<tenant.domain>`) matches the address each fixture
// deployed the sidecar workflow under; otherwise the route/router target an
// unknown address.
const DEPLOYMENT_DOMAIN = "integration.interchange";

const TENANT_ID = "tnt_inbound_enforce";
const CALLER_USER_ID = "usr_inbound_enforce_caller";
const CALLER_PRINCIPAL_ID = "prn_inbound_enforce_caller";
const STEP_ID = "step1";

// D1: no authored policy (secure default). D2: admits `missing`.
const D1_DEPLOYMENT_ID = "run_inbound-enforce-default-1";
const D1_DEFINITION_ASSET_ID = "ast_inbound_enforce_default_wf";
const D2_DEPLOYMENT_ID = "run_inbound-enforce-admit-missing-1";
const D2_DEFINITION_ASSET_ID = "ast_inbound_enforce_admit_missing_wf";

const d1MailAddress = deriveRunAddress({
  runId: D1_DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});
const d2MailAddress = deriveRunAddress({
  runId: D2_DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

// The hub-verified sender the scenarios stamp on the direct-injected unsigned
// mail. Its key is co-delivered on the grants barrier so the recipient resolves
// a key for it and the unsigned message verifies `missing`, not the cache-miss
// `unknown`.
const EXTERNAL_SENDER = "external@integration.interchange";

// The trigger route's 202 body shape. Validated rather than cast so a route
// response drift surfaces at the boundary.
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
      id: "session_inbound_enforce",
      userId,
      token: "tok_inbound_enforce",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function notImpl(name: string): never {
  throw new Error(`inbound-mail enforcement mock: ${name} not implemented`);
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

function inferenceSourceForEnv(env: DeployFlowEnv): InferenceSource {
  return {
    id: "anthropic:mock-model",
    provider: "anthropic",
    baseURL: `http://localhost:${String(env.inference.server.port)}`,
    credentialId: "sk-mock",
    model: "mock-model",
  };
}

/**
 * Deploy a single-step echo workflow BY SOURCE-REF and wait for its mail
 * address to become routable. `inboundMailPolicy` is threaded into the deployed
 * `defineWorkflow` definition, so the sidecar resolves and registers it beside
 * the deployment's mail-router registration -- the recipient policy the enforce
 * seam reads back per message.
 */
async function deployEnforcementWorkflow(opts: {
  anchorRunId: string;
  definitionAssetId: string;
  mailAddress: string;
  inboundMailPolicy?: Parameters<
    typeof singleStepAgentEntry
  >[0]["inboundMailPolicy"];
}): Promise<{ workflowRunRepoId: RepoId }> {
  const inferenceSource = inferenceSourceForEnv(env);
  const config: HarnessConfig = {
    sessionId: SESSION_ID,
    agentId: opts.anchorRunId,
    tenantId: "tenant-1",
    principalId: "prin_integration-1",
    agentAddress: opts.mailAddress,
    systemPrompt: "Fallback prompt (overridden per step by the definition)",
    tools: [],
    grants: [],
    sources: [inferenceSource],
    defaultSource: "anthropic:mock-model",
  };
  const entryModule = singleStepAgentEntry({
    stepId: STEP_ID,
    systemPrompt: `Inbound-enforcement agent for ${opts.anchorRunId}.`,
    address: opts.mailAddress,
    agentId: `agent_${opts.anchorRunId}`,
    workflowId: `wf_${opts.anchorRunId}`,
    ...(opts.inboundMailPolicy !== undefined
      ? { inboundMailPolicy: opts.inboundMailPolicy }
      : {}),
  });
  const handle = await deployWorkflowSourceForTest(env, {
    entryModule,
    db: h.db,
    tenantId: TENANT_ID,
    definitionAssetId: opts.definitionAssetId,
    anchorRunId: opts.anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: opts.mailAddress,
    approvals: "approve-probed",
    config,
    sources: { [STEP_ID]: [inferenceSource] },
  });
  expect(handle.publicKey).toBeTruthy();

  await waitFor(
    () => env.hub.router.getRoutableAddresses().includes(opts.mailAddress),
    { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
  );

  return { workflowRunRepoId: handle.workflowRunRepoId };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "inbound-mail admission is enforced against the per-workflow policy",
  () => {
    let hasRun = false;

    beforeAll(async () => {
      h = await createTestDb();
      // Echo inference so an admitted single-step run produces a reply and
      // terminates on its own once its grants barrier is satisfied.
      env = await startDeployFlowEnv({ inferenceEchoUserMessage: true });
    });

    afterAll(async () => {
      if (env !== undefined) await env.teardown();
      if (h !== undefined) await h.close();
    });

    test("the default policy drops unsigned mail that an admit-missing policy admits", async () => {
      if (hasRun) {
        throw new Error(
          "this suite assumes a single test per shared subprocess env; " +
            "add a new scenario in its own file with its own env instead",
        );
      }
      hasRun = true;

      // Seed the tenancy both deployments resolve against: the tenant carries
      // the deploy domain so each derived address matches its sidecar
      // deployment, and the caller is an active user-principal holding a durable
      // hub key (which production mints at principal creation; the direct row
      // insert bypasses that, so mint it here or the trigger route's sign()
      // throws) plus the `workflow-run:<D1>/manage` grant the `/mail` route
      // requires. The caller also creates each `workflow`-kind definition asset
      // the frozen definitions project over; the trigger route reads that
      // asset's creator to resolve the run's creator grants.
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
      await seedPrincipalKey(h.db, CALLER_PRINCIPAL_ID);
      await seedAsset(h.db, {
        id: D1_DEFINITION_ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "inbound-enforce-default-wf",
        creatorPrincipalId: CALLER_PRINCIPAL_ID,
      });
      await seedAsset(h.db, {
        id: D2_DEFINITION_ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "inbound-enforce-admit-missing-wf",
        creatorPrincipalId: CALLER_PRINCIPAL_ID,
      });
      await seedGrant(h.db, {
        id: "grant-caller-manage-d1",
        tenantId: TENANT_ID,
        resource: `workflow-run:${D1_DEPLOYMENT_ID}`,
        action: "manage",
        effect: "allow",
        origin: "system",
        principalId: CALLER_PRINCIPAL_ID,
      });

      // D1: no authored policy. D2: admits `missing`.
      const d1 = await deployEnforcementWorkflow({
        anchorRunId: D1_DEPLOYMENT_ID,
        definitionAssetId: D1_DEFINITION_ASSET_ID,
        mailAddress: d1MailAddress,
      });
      const d2 = await deployEnforcementWorkflow({
        anchorRunId: D2_DEPLOYMENT_ID,
        definitionAssetId: D2_DEFINITION_ASSET_ID,
        mailAddress: d2MailAddress,
        inboundMailPolicy: { missing: "admit" },
      });

      const d1RunId = deriveWorkflowRunId(d1MailAddress);
      const d2RunId = deriveWorkflowRunId(d2MailAddress);

      // ----- (a) VALID -> ADMITTED on D1 (real trigger route) -----
      // The production `/mail` route signs with the caller's durable principal
      // key and co-delivers that key, so the recipient verdict is `clean`
      // (valid signature + From binding), which every policy admits.
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
        `/api/tenants/${TENANT_ID}/workflows/${D1_DEPLOYMENT_ID}/mail`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ content: "kick off the valid run" }),
        },
      );
      if (res.status !== 202) {
        const body: unknown = await res.json();
        throw new Error(
          `expected 202 from /mail, got ${String(res.status)}: ${JSON.stringify(body)}\n${env.sidecarDiagnostics()}`,
        );
      }
      const trigger = TriggerResponse.assert(await res.json());
      expect(trigger.address).toBe(d1MailAddress);

      // Admitted end to end: the run reaches terminal SUCCESS and the trigger's
      // messageId lands in the deployment's consumed dedup subtree.
      const validTerminal = await waitForWorkflowRunComplete(
        env,
        D1_DEPLOYMENT_ID,
        d1RunId,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      if (validTerminal.type !== "RunCompleted") {
        throw new Error(
          `expected RunCompleted for the valid D1 run, got ${validTerminal.type}: ${JSON.stringify(validTerminal.body)}\n${env.sidecarDiagnostics()}`,
        );
      }
      const validConsumed = await waitForConsumedFilename(
        env,
        d1.workflowRunRepoId,
        d1MailAddress,
        `${trigger.messageId}.json`,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      expect(validConsumed.map((e) => e.filename)).toContain(
        `${trigger.messageId}.json`,
      );

      // ----- (b) UNSIGNED -> DROPPED on D1 (default policy) -----
      // A key for EXTERNAL_SENDER is co-delivered on the grants barrier so the
      // unsigned message verifies `missing` (a resolvable sender), not the
      // cache-miss `unknown`. D1's default policy rejects `missing`.
      const senderKeyPair = await generateKeyPair();
      const senderIdentity = {
        address: EXTERNAL_SENDER,
        publicKey: hexEncode(senderKeyPair.publicKey),
      };
      // No outcome word ("missing"/"unknown") in the id, so the reject-log
      // grep below matches only the genuine verdict/outcome, not the id.
      const droppedMessageId = "<enforce-drop-1@integration.interchange>";
      const droppedRaw = buildMinimalMail({
        from: EXTERNAL_SENDER,
        to: d1MailAddress,
        includeMessageIdHeader: true,
        messageId: droppedMessageId,
        body: "unsigned enforcement body — dropped by the default policy",
      });

      const d1GrantsDelivered = env.hub.router.sendRunGrants(
        d1MailAddress,
        d1RunId,
        [],
        [senderIdentity],
      );
      expect(d1GrantsDelivered).toBe(true);

      // Snapshot the diagnostics buffer BEFORE routing so the reject-log wait
      // scopes its match to events that arrive AFTER this route call.
      const diagBeforeDrop = env.sidecarDiagnostics();
      const droppedDelivered = env.hub.router.routeMail(
        d1MailAddress,
        base64Encode(droppedRaw),
        EXTERNAL_SENDER,
        droppedMessageId,
      );
      expect(droppedDelivered).toBe(true);

      // POSITIVE signal: the enforce seam logs the reject and the verify logs a
      // `missing` verdict for this message. Because the dropped messageId
      // carries no outcome word, `/missing/` matches only the genuine `missing`
      // verdict/outcome -- it would NOT match had the outcome regressed to the
      // cache-miss `unknown` (which the default also rejects), so this
      // discriminates that the co-delivered key forced `missing`, not `unknown`.
      await waitFor(
        () => {
          const fresh = env.sidecarDiagnostics().slice(diagBeforeDrop.length);
          return /Rejecting inbound mail/.test(fresh) && /missing/.test(fresh);
        },
        { timeoutMs: 10_000, diagnostics: env.sidecarDiagnostics },
      );

      // ABSENCE signal: a dropped message is never dispatched, so no
      // `RunStarted` carries it and it is never consumed. Bound the settle so a
      // late materialization would still be caught.
      await assertConsumedFilenameAbsent(
        env,
        d1.workflowRunRepoId,
        d1MailAddress,
        `${droppedMessageId}.json`,
        { windowMs: 2_000, diagnostics: env.sidecarDiagnostics },
      );
      const d1Events = await readWorkflowRunEvents(
        env,
        D1_DEPLOYMENT_ID,
        d1RunId,
      );
      const d1ConsumedMessageIds = d1Events
        .filter((e) => e.type === "RunStarted")
        .map((e) => e.body["consumedMessageId"]);
      expect(d1ConsumedMessageIds).not.toContain(droppedMessageId);

      // ----- (c) SAME UNSIGNED shape -> ADMITTED on D2 (admits `missing`) -----
      const admittedMessageId =
        "<enforce-missing-admit-1@integration.interchange>";
      const admittedRaw = buildMinimalMail({
        from: EXTERNAL_SENDER,
        to: d2MailAddress,
        includeMessageIdHeader: true,
        messageId: admittedMessageId,
        body: "unsigned enforcement body — admitted by { missing: admit }",
      });

      const d2GrantsDelivered = env.hub.router.sendRunGrants(
        d2MailAddress,
        d2RunId,
        [],
        [senderIdentity],
      );
      expect(d2GrantsDelivered).toBe(true);
      const admittedDelivered = env.hub.router.routeMail(
        d2MailAddress,
        base64Encode(admittedRaw),
        EXTERNAL_SENDER,
        admittedMessageId,
      );
      expect(admittedDelivered).toBe(true);

      // Admitted: the run starts carrying the messageId, is consumed, and
      // reaches terminal completion -- the identical shape D1 dropped.
      const admittedTerminal = await waitForWorkflowRunComplete(
        env,
        D2_DEPLOYMENT_ID,
        d2RunId,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      if (admittedTerminal.type !== "RunCompleted") {
        throw new Error(
          `expected RunCompleted for the admitted D2 run, got ${admittedTerminal.type}: ${JSON.stringify(admittedTerminal.body)}\n${env.sidecarDiagnostics()}`,
        );
      }
      const d2Events = await readWorkflowRunEvents(
        env,
        D2_DEPLOYMENT_ID,
        d2RunId,
      );
      const admittedStarted = d2Events.find((e) => e.type === "RunStarted");
      if (admittedStarted === undefined) {
        throw new Error(
          `admitted D2 run ${d2RunId} has no RunStarted\n${env.sidecarDiagnostics()}`,
        );
      }
      expect(admittedStarted.body["consumedMessageId"]).toBe(admittedMessageId);
      const admittedConsumed = await waitForConsumedFilename(
        env,
        d2.workflowRunRepoId,
        d2MailAddress,
        `${admittedMessageId}.json`,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      expect(admittedConsumed.map((e) => e.filename)).toContain(
        `${admittedMessageId}.json`,
      );
    }, 120_000);
  },
);

/**
 * Construct a minimal RFC 2822-shaped, unsigned `text/plain` mail by hand. The
 * fixture's `fireMailTrigger` runs through `assembleMessage`, which would emit a
 * `multipart/signed` body; this helper emits a plain unsigned message so the
 * recipient's signature verify returns `missing`. Mirrors the local helper in
 * `mail-edge-cases.test.ts`.
 */
function buildMinimalMail(opts: {
  from: string;
  to: string;
  includeMessageIdHeader: boolean;
  messageId?: string;
  body: string;
}): Uint8Array {
  const lines: string[] = [];
  lines.push(`From: ${opts.from}`);
  lines.push(`To: ${opts.to}`);
  lines.push(`Date: ${new Date(0).toUTCString()}`);
  lines.push("Subject: enforcement-case");
  lines.push("MIME-Version: 1.0");
  lines.push("Content-Type: text/plain; charset=utf-8");
  if (opts.includeMessageIdHeader) {
    if (opts.messageId === undefined) {
      throw new Error(
        "buildMinimalMail: includeMessageIdHeader=true requires messageId",
      );
    }
    lines.push(`Message-Id: ${opts.messageId}`);
  }
  const headerSection = lines.join("\r\n");
  const full = `${headerSection}\r\n\r\n${opts.body}\r\n`;
  return new TextEncoder().encode(full);
}

/**
 * Poll the deployment's `consumed/` subtree until the expected filename is
 * present. The supervisor's `markConsumed` pack push lands strictly after the
 * run's terminal-event observation, so a test that observes terminal then reads
 * consumed/ in one shot can race the supervisor's pack pipeline. Mirrors the
 * local helper in `mail-edge-cases.test.ts`.
 */
async function waitForConsumedFilename(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
  address: string,
  expected: string,
  opts: { timeoutMs?: number; diagnostics?: () => string } = {},
): Promise<{ filename: string; bytes: Uint8Array }[]> {
  const { timeoutMs = 10_000, diagnostics } = opts;
  const start = Date.now();
  for (;;) {
    const entries = await readClaimCheckDir(
      env,
      workflowRunRepoId,
      address,
      "consumed",
    );
    if (entries.some((e) => e.filename === expected)) {
      return entries;
    }
    if (Date.now() - start > timeoutMs) {
      const diag = diagnostics?.();
      const ctx = diag ? `\n${diag}` : "";
      const observed = entries.map((e) => e.filename).join(", ") || "<empty>";
      throw new Error(
        `waitForConsumedFilename timed out after ${String(timeoutMs)}ms; expected ${expected}; observed ${observed}${ctx}`,
      );
    }
    await new Promise((r) => setTimeout(r, 50));
  }
}

/**
 * Assert a filename never appears in the deployment's `consumed/` subtree over a
 * bounded settle window. A dropped inbound message is never dispatched, so it is
 * never consumed; this is the ABSENCE counterpart to `waitForConsumedFilename`.
 */
async function assertConsumedFilenameAbsent(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
  address: string,
  forbidden: string,
  opts: { windowMs?: number; diagnostics?: () => string } = {},
): Promise<void> {
  const { windowMs = 2_000, diagnostics } = opts;
  const start = Date.now();
  for (;;) {
    const entries = await readClaimCheckDir(
      env,
      workflowRunRepoId,
      address,
      "consumed",
    );
    if (entries.some((e) => e.filename === forbidden)) {
      const diag = diagnostics?.();
      const ctx = diag ? `\n${diag}` : "";
      throw new Error(
        `expected ${forbidden} to never be consumed, but it appeared${ctx}`,
      );
    }
    if (Date.now() - start > windowMs) return;
    await new Promise((r) => setTimeout(r, 50));
  }
}
