// Mail-handling edge-case integration test.
//
// Coverage for mail-flow corner cases. The supervisor's
// `deriveMessageId` derivation is the load-bearing identity the
// FIFO inbox, dedup index, and run-id mint all depend on. The
// existing FIFO test pins the happy path on well-formed RFC 2822
// Message-Id headers; this file pins three documented-but-untested
// shapes:
//
// 1. Mail with NO `Message-Id:` header at all. The parser returns
//    `null` and `deriveMessageId` falls back to `sha256(rawMessage)`.
//    Two byte-identical mails therefore mint the same runId, which
//    triggers the substrate's claim-check dedup invariant
//    (`claim_check_already_consumed` after the first run terminates,
//    or `claim_check_already_processing` / `claim_check_already_inbox`
//    if the duplicate arrives sooner). The supervisor's
//    `onMailMessage` catches the error and logs it; the duplicate
//    is dropped silently on the floor.
//
// 2. Mail with a malformed `Message-Id:` header (here: no closing
//    angle bracket -- `Message-Id: <invalid`). The parser does NOT
//    validate the angle-bracket shape -- it returns the trimmed
//    suffix-after-colon verbatim. So the messageId is `<invalid` and
//    the run materialises with that runId. This is the documented
//    contract; if it changes, this test is the regression signal.
//
// 3. Two mails with the same `Message-Id` header. The substrate's
//    dedup catches the duplicate via the same path as case 1; the
//    first run materialises, the second is dropped at the
//    `enqueueInbox` boundary with one of the
//    `claim_check_already_*` errors. The supervisor's
//    `onMailMessage` `.catch` callback logs and continues; nothing
//    fires `trigger.fire` for the duplicate.
//
// The supervisor's mail-flow path:
//   onMailMessage -> deriveMessageId -> enqueueInbox -> dispatch loop
// The dispatch loop, when it forwards `trigger.fire` with
// `runId === messageId`, mints the run-id from the messageId. The
// workflow-process child commits `RunStarted.consumedMessageId` to the
// run's events log, so the test can correlate observed runs back to
// the bytes that triggered them.
//
// The fixture's existing `fireMailTrigger` uses `assembleMessage`,
// which validates Message-Id shape -- it would reject case 2's
// `<invalid` value before we ever exercise the supervisor's parser.
// This file constructs raw mail bytes by hand to bypass the
// fixture-side validator and exercise the supervisor's parser
// directly. The hub's `routeMail` takes a base64 string and the
// sidecar's hub-link decodes it back into the `Uint8Array` the
// supervisor's `onMailMessage` consumes; the wire transport
// preserves bytes verbatim.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import { base64Encode, hexEncode } from "@intx/types";
import type { HarnessConfig } from "@intx/types/runtime";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveDeploymentAddress,
  type ApprovalSet,
  type DeploySingleStepFn,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import type { RepoId, WorkflowRunHubPrincipal } from "@intx/hub-sessions";
import { DEFAULT_ASSET_REF } from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  dropHubLink,
  listRunIds,
  readClaimCheckDir,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const WORKFLOW_RUN_REF = "refs/heads/main";

const NO_HEADER_DEPLOYMENT_ID = "mail-edge-no-header-1";
const MALFORMED_DEPLOYMENT_ID = "mail-edge-malformed-1";
const DUPLICATE_DEPLOYMENT_ID = "mail-edge-duplicate-1";
const CONNECTED_WINDOW_DEPLOYMENT_ID = "mail-edge-connected-window-1";

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

describe("mail-handling edge cases", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("mail with no Message-Id header derives a sha256-of-bytes messageId; identical bytes collide", async () => {
    const ctx = await deployEdgeWorkflow(env, NO_HEADER_DEPLOYMENT_ID);

    // Construct two byte-identical raw mails with NO Message-Id header.
    // The supervisor's parser walks for a `message-id:` line
    // case-insensitively; without one, `parseMessageIdHeader` returns
    // null and `deriveMessageId` falls back to `sha256(rawMessage)`.
    // Under the stable-runId model the runId is the deployment address,
    // but the messageId (used for claim-check dedup) is still sha256.
    const raw = buildMinimalMail({
      from: "edge@integration.interchange",
      to: ctx.deploymentMailAddress,
      includeMessageIdHeader: false,
      body: "no-header edge case body",
    });

    const messageId = await sha256Hex(raw);
    const runId = ctx.deploymentMailAddress;

    // Fire the first mail; the supervisor should start a run with
    // runId === deployment address. Wait for `RunStarted` to land.
    await routeRaw(env, ctx.deploymentMailAddress, raw);
    await waitForWorkflowRunComplete(env, NO_HEADER_DEPLOYMENT_ID, runId, {
      timeoutMs: 30_000,
      diagnostics: env.sidecarDiagnostics,
    });

    // Verify the canonical chain materialised and the
    // `consumedMessageId` on `RunStarted` equals the sha256 messageId.
    const events = await readWorkflowRunEvents(
      env,
      NO_HEADER_DEPLOYMENT_ID,
      runId,
    );
    const types = events.map((e) => e.type);
    expect(types).toContain("RunStarted");
    expect(types).toContain("RunCompleted");
    const started = events.find((e) => e.type === "RunStarted");
    if (started === undefined) throw new Error("unreachable");
    expect(started.body["consumedMessageId"]).toBe(messageId);

    // Fire a second byte-identical mail. The supervisor's
    // `deriveMessageId` derives the same sha256 hash; the
    // claim-check substrate sees the messageId is already in
    // `consumed/` (or `processing/`, depending on timing), so
    // `enqueueInbox` returns an `already-present` outcome. The
    // supervisor acknowledges it (the bytes are durably on disk) but
    // dispatches no second run -- the duplicate is deduped.
    //
    // We pin the documented behavior: the duplicate must NOT
    // produce a second run. Wait for the supervisor's
    // `markConsumed` to land on the first run (which happens
    // strictly after the run's terminal event observation above),
    // then read consumed/ for a baseline.
    const consumedBefore = await waitForConsumedFilename(
      env,
      ctx.workflowRunRepoId,
      ctx.deploymentMailAddress,
      `${messageId}.json`,
      { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
    );
    const consumedMessageIds = consumedBefore
      .map((e) => /^(.+)\.json$/.exec(e.filename)?.[1])
      .filter((v): v is string => v !== undefined);
    expect(consumedMessageIds).toContain(messageId);

    // Snapshot the sidecar diagnostics buffer before firing the
    // duplicate so the log-substring wait below can scope its match
    // to events that arrive AFTER the second routeRaw call.
    const diagBeforeDuplicate = env.sidecarDiagnostics();
    await routeRaw(env, ctx.deploymentMailAddress, raw);

    // The duplicate must produce the supervisor's `already durably
    // present` log line carrying one of the already-present reasons.
    // This is the positive signal the test pins instead of sleeping a
    // fixed beat and re-reading the substrate.
    await waitFor(
      () => {
        const fresh = env
          .sidecarDiagnostics()
          .slice(diagBeforeDuplicate.length);
        return /already durably present/.test(fresh);
      },
      { timeoutMs: 10_000, diagnostics: env.sidecarDiagnostics },
    );

    const consumedAfter = await readClaimCheckDir(
      env,
      ctx.workflowRunRepoId,
      ctx.deploymentMailAddress,
      "consumed",
    );
    // Same set; the duplicate did not add a new consumed entry.
    expect(consumedAfter.length).toBe(consumedBefore.length);

    const runIdsAfter = await listRunIds(env, ctx.workflowRunRepoId);
    // Exactly one run for the deployment address; no synthetic second
    // run-id materialised.
    expect(runIdsAfter.filter((r) => r === runId).length).toBe(1);
  }, 60_000);

  test("mail with malformed Message-Id (no closing bracket) mints the raw value as messageId", async () => {
    const ctx = await deployEdgeWorkflow(env, MALFORMED_DEPLOYMENT_ID);

    // Construct a mail with a malformed Message-Id header. The
    // parser does NOT validate angle-bracket shape; it returns the
    // trimmed suffix after `Message-Id:`. So `<invalid` becomes
    // the messageId verbatim.  Under the stable-runId model the
    // runId is the deployment address, but the messageId is still
    // the parsed header value.
    const malformedMessageId = "<invalid";
    const raw = buildMinimalMail({
      from: "edge@integration.interchange",
      to: ctx.deploymentMailAddress,
      includeMessageIdHeader: true,
      messageId: malformedMessageId,
      body: "malformed message-id edge case body",
    });

    const runId = ctx.deploymentMailAddress;

    await routeRaw(env, ctx.deploymentMailAddress, raw);

    await waitForWorkflowRunComplete(env, MALFORMED_DEPLOYMENT_ID, runId, {
      timeoutMs: 30_000,
      diagnostics: env.sidecarDiagnostics,
    });

    const events = await readWorkflowRunEvents(
      env,
      MALFORMED_DEPLOYMENT_ID,
      runId,
    );
    const started = events.find((e) => e.type === "RunStarted");
    if (started === undefined) {
      throw new Error(`malformed edge: run ${runId} has no RunStarted`);
    }
    expect(started.body["consumedMessageId"]).toBe(malformedMessageId);
    const types = events.map((e) => e.type);
    expect(types).toContain("RunCompleted");
  }, 60_000);

  test("two mails with the same Message-Id: first runs, second is deduped on the substrate boundary", async () => {
    const ctx = await deployEdgeWorkflow(env, DUPLICATE_DEPLOYMENT_ID);

    const messageId = "<dup-edge-1@integration.interchange>";
    const raw1 = buildMinimalMail({
      from: "edge@integration.interchange",
      to: ctx.deploymentMailAddress,
      includeMessageIdHeader: true,
      messageId,
      body: "duplicate edge case body — first send",
    });
    // Byte-distinct second mail with the SAME Message-Id header. The
    // substrate's dedup is keyed on messageId, not on full-bytes hash,
    // so the second mail collides on the messageId index regardless
    // of body differences.
    const raw2 = buildMinimalMail({
      from: "edge@integration.interchange",
      to: ctx.deploymentMailAddress,
      includeMessageIdHeader: true,
      messageId,
      body: "duplicate edge case body — second send (different body)",
    });

    const runId = ctx.deploymentMailAddress;

    await routeRaw(env, ctx.deploymentMailAddress, raw1);
    await waitForWorkflowRunComplete(env, DUPLICATE_DEPLOYMENT_ID, runId, {
      timeoutMs: 30_000,
      diagnostics: env.sidecarDiagnostics,
    });

    // First run materialised under runs/<runId>/. The supervisor's
    // `markConsumed` lands strictly after the terminal observation
    // above; wait for the dedup entry to surface so the duplicate
    // collides on the consumed/ branch rather than the processing/
    // or inbox branch (the test pins documented behavior; we want a
    // stable error class to assert against).
    const consumedBefore = await waitForConsumedFilename(
      env,
      ctx.workflowRunRepoId,
      ctx.deploymentMailAddress,
      `${messageId}.json`,
      { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
    );
    const consumedNamesBefore = new Set(consumedBefore.map((e) => e.filename));
    expect(consumedNamesBefore).toContain(`${messageId}.json`);

    // Fire the duplicate. The supervisor's `enqueueInbox` returns an
    // `already-present` outcome (reason `consumed`); `onMailMessage`
    // acknowledges it without dispatching a run. The dedup index stays
    // at one entry; no second run materialises.
    const diagBeforeDuplicate = env.sidecarDiagnostics();
    await routeRaw(env, ctx.deploymentMailAddress, raw2);

    // Wait for the supervisor's `already durably present` log line
    // carrying the already-present reason; pinning on the positive log
    // signal beats sleeping a fixed beat and re-reading the substrate
    // hoping nothing changed.
    await waitFor(
      () => {
        const fresh = env
          .sidecarDiagnostics()
          .slice(diagBeforeDuplicate.length);
        return /already durably present/.test(fresh);
      },
      { timeoutMs: 10_000, diagnostics: env.sidecarDiagnostics },
    );

    const consumedAfter = await readClaimCheckDir(
      env,
      ctx.workflowRunRepoId,
      ctx.deploymentMailAddress,
      "consumed",
    );
    expect(consumedAfter.length).toBe(consumedBefore.length);

    const inboxAfter = await readClaimCheckDir(
      env,
      ctx.workflowRunRepoId,
      ctx.deploymentMailAddress,
      "inbox",
    );
    expect(inboxAfter).toEqual([]);
    const processingAfter = await readClaimCheckDir(
      env,
      ctx.workflowRunRepoId,
      ctx.deploymentMailAddress,
      "processing",
    );
    expect(processingAfter).toEqual([]);

    const runIds = await listRunIds(env, ctx.workflowRunRepoId);
    // Under the stable-runId model the runId is the deployment address.
    expect(runIds.filter((r) => r === runId).length).toBe(1);
  }, 60_000);

  test("mail into a connected window that drops before the ack survives reconnect and is processed exactly once", async () => {
    // The connected-window mail-loss gap, end to end against a real sidecar
    // subprocess over a real socket. A `mail.inbound` carrying a hub-minted
    // messageId is delivered over the LIVE link and the link is severed in the
    // same tick, before the sidecar's durable-write ack round-trips. The hub
    // retains the un-acked pending mail and redelivers it when the sidecar
    // reconnects; identical bytes and the same messageId make the delivery
    // effectively-once. The run reaching terminal completion is NO-LOSS (over a
    // real socket the severed link drops the in-flight frame, so ONLY the
    // reconnect redelivery gets the mail to the sidecar; without the retention
    // the message is lost and the run never starts). A single RunStarted and a
    // single consumed dedup entry are NO-DOUBLE-PROCESS.
    //
    // The run's grants are delivered and allowed to land on the sidecar's disk
    // BEFORE the connected-window drop: run.grants has no ack handshake, so it
    // is not part of what this test drops -- only the trigger mail is. The
    // grants persist on the sidecar across the reconnect.
    const ctx = await deployEdgeWorkflow(env, CONNECTED_WINDOW_DEPLOYMENT_ID);

    // Keep the deployment stably routable for a beat so the drop lands well
    // clear of the deploy window (a drop racing the key-ack would fail the
    // reconnect challenge for reasons unrelated to mail retention).
    const settleStart = Date.now();
    let stableSince = Date.now();
    while (Date.now() - stableSince < 1_000) {
      if (
        !env.hub.router
          .getRoutableAddresses()
          .includes(ctx.deploymentMailAddress)
      ) {
        stableSince = Date.now();
      }
      if (Date.now() - settleStart > 20_000) {
        throw new Error(
          `deployment never held routable for 1s\n${env.sidecarDiagnostics()}`,
        );
      }
      await new Promise((r) => setTimeout(r, 50));
    }

    const messageId = "<connected-window-1@integration.interchange>";
    const raw = buildMinimalMail({
      from: "edge@integration.interchange",
      to: ctx.deploymentMailAddress,
      includeMessageIdHeader: true,
      messageId,
      body: "connected-window edge case body",
    });
    const runId = ctx.deploymentMailAddress;

    // Deliver the run's grants and let them land durably on the sidecar before
    // the drop (there is no run.grants ack to wait on, so allow a generous
    // beat; the frame lands over localhost in well under this window).
    const grantsDelivered = env.hub.router.sendRunGrants(
      ctx.deploymentMailAddress,
      runId,
      [],
    );
    expect(grantsDelivered).toBe(true);
    await new Promise((r) => setTimeout(r, 2_000));

    // Deliver the trigger mail with its hub-minted messageId (the production
    // workflow-trigger route carries it; the hub tracks it for redelivery),
    // then sever the link in the same tick so the ack -- which fires only after
    // the async durable write -- never round-trips.
    const base64 = base64Encode(raw);
    const delivered = env.hub.router.routeMail(
      ctx.deploymentMailAddress,
      base64,
      messageId,
    );
    expect(delivered).toBe(true);
    dropHubLink(env);

    // The sidecar reconnects; the hub redelivers the retained pending mail.
    await waitForReconnect(env, ctx.deploymentMailAddress, {
      timeoutMs: 30_000,
    });

    // NO-LOSS: the run reaches terminal completion -- only the reconnect
    // redelivery could have gotten the dropped mail to the sidecar.
    const terminal = await waitForWorkflowRunComplete(
      env,
      CONNECTED_WINDOW_DEPLOYMENT_ID,
      runId,
      { timeoutMs: 60_000, diagnostics: env.sidecarDiagnostics },
    );
    expect(terminal.type).toBe("RunCompleted");

    // NO-DOUBLE-PROCESS: exactly one RunStarted (keyed to this messageId) and
    // exactly one consumed dedup entry -- a redelivery of a message already
    // written is deduped, not reprocessed.
    const events = await readWorkflowRunEvents(
      env,
      CONNECTED_WINDOW_DEPLOYMENT_ID,
      runId,
    );
    const runStarts = events.filter((e) => e.type === "RunStarted");
    expect(runStarts.length).toBe(1);
    expect(runStarts[0]?.body["consumedMessageId"]).toBe(messageId);

    const consumed = await waitForConsumedFilename(
      env,
      ctx.workflowRunRepoId,
      ctx.deploymentMailAddress,
      `${messageId}.json`,
      { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
    );
    expect(consumed.map((e) => e.filename)).toEqual([`${messageId}.json`]);
  }, 180_000);
});

/**
 * Deploy a trivial single-step multi-step workflow against the
 * fixture's orchestrator. Returns a context with the deployment's
 * derived mail address and the workflow-run repo id; the caller fires
 * raw bytes at the mail address via `routeRaw`.
 */
async function deployEdgeWorkflow(
  env: DeployFlowEnv,
  deploymentId: string,
): Promise<{
  deploymentMailAddress: string;
  workflowRunRepoId: RepoId;
}> {
  const stepAgent = defineAgent({
    id: `agent-${deploymentId}-step`,
    systemPrompt: `Edge-case agent for ${deploymentId}.`,
    tools: [],
    capabilities: [],
    inference: {
      sources: [{ provider: "anthropic", model: "mock-model" }],
    },
  });

  const deploymentMailAddress = deriveDeploymentAddress({
    deploymentId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
  });

  const workflow: WorkflowDefinition = defineWorkflow({
    id: `wf_${deploymentId}`,
    trigger: { type: "mail", to: deploymentMailAddress },
    steps: {
      edgeStep: step({ agent: stepAgent }),
    },
  });

  const config: HarnessConfig = {
    sessionId: SESSION_ID,
    agentId: `ins_${deploymentId}`,
    tenantId: "tenant-1",
    principalId: "prin_integration-1",
    agentAddress: deploymentMailAddress,
    systemPrompt: "Fallback prompt (overridden per step by orchestrator)",
    tools: [],
    grants: [],
    sources: [
      {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${env.inference.server.port}`,
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

  const launchSession: LaunchSessionFn = async (orchestratorParams) => {
    const deployContent = orchestratorParams.deployContent;
    await env.hub.sessionService.stageWorkflowStep({
      agentAddress: orchestratorParams.agentAddress,
      agentId: orchestratorParams.agentId,
      instanceId: orchestratorParams.instanceId,
      config: orchestratorParams.config,
      deployContent: toLaunchDeployContent(deployContent),
      ...(orchestratorParams.toolPackagePins !== undefined
        ? { toolPackagePins: orchestratorParams.toolPackagePins }
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
      for (const [k, v] of args.files) {
        files[k] = v;
      }
      await env.hub.agentRepoStore.repoStore.writeTree(
        principal,
        repoId,
        DEFAULT_ASSET_REF,
        {
          files,
          message: `mail-edge-cases test: write workflow repo ${args.workflowRepoId}`,
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
    deploymentId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    hubPublicKey: "00".repeat(32),
  });
  expect(result.publicKey).toBeTruthy();

  const workflowRunRepoId: RepoId = {
    kind: "workflow-run",
    id: deriveDeploymentId(deploymentMailAddress),
  };
  env.registerDeployment({
    deploymentId,
    workflowDefinition: workflow,
    workflowRunRepoId,
    workflowRunRef: WORKFLOW_RUN_REF,
    mailAddress: deploymentMailAddress,
  });

  if (!env.hub.router.getRoutableAddresses().includes(deploymentMailAddress)) {
    throw new Error(
      `mail-edge-cases: deployment ${deploymentId} did not register address ${deploymentMailAddress}`,
    );
  }

  return { deploymentMailAddress, workflowRunRepoId };
}

/**
 * Construct a minimal RFC 2822-shaped mail message by hand. The
 * fixture's `fireMailTrigger` runs through `assembleMessage`, which
 * validates the Message-Id shape and would reject the malformed-id
 * case before the bytes ever reach the supervisor's parser. This
 * helper emits raw bytes verbatim so the supervisor's
 * `parseMessageIdHeader` is the only validator on the path.
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
  lines.push("Subject: edge-case");
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
 * Route raw mail bytes through the hub-side mail bus. Mirrors the
 * encoding `routeMail` consumes (base64) so the bytes survive the
 * sidecar's hub-link decode unchanged.
 */
async function routeRaw(
  env: DeployFlowEnv,
  address: string,
  raw: Uint8Array,
): Promise<void> {
  // Under the stable-runId model the supervisor expects grants at
  // runs/<deploymentAddress>/grants.json, regardless of the message's
  // derived messageId.
  const runId = address;
  const grantsDelivered = env.hub.router.sendRunGrants(address, runId, []);
  if (!grantsDelivered) {
    throw new Error(
      `routeRaw: sendRunGrants returned false for ${address}; address is not routable on the hub`,
    );
  }
  const base64 = base64Encode(raw);
  const delivered = env.hub.router.routeMail(address, base64);
  if (!delivered) {
    throw new Error(
      `routeRaw: routeMail returned false for ${address}; address is not routable on the hub`,
    );
  }
}

async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new Uint8Array(bytes));
  return hexEncode(new Uint8Array(digest));
}

/**
 * List every `runs/<runId>/` subdirectory on the deployment's
 * workflow-run repo's main ref. Mirrors the FIFO test's helper.
 */

/**
 * Poll the deployment's `consumed/` subtree until the expected
 * filename is present. The supervisor's `markConsumed` pack push
 * lands strictly after the run's terminal-event observation, so a
 * test that observes terminal then reads consumed/ in one shot can
 * race the supervisor's pack pipeline.
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
