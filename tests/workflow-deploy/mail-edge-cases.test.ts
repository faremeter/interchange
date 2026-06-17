// Mail-handling edge-case integration test.
//
// Greybeard's pre-PR coverage gap on mail-flow corner cases. The
// supervisor's `deriveMessageId` derivation is the load-bearing identity
// the FIFO inbox, dedup index, and run-id mint all depend on. The
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
import type { HarnessConfig } from "@intx/types/runtime";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveDeploymentAddress,
  type ApprovalSet,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { deriveTrivialDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import type { RepoId, WorkflowRunHubPrincipal } from "@intx/hub-sessions";
import { DEFAULT_ASSET_REF } from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  listRunIds,
  readClaimCheckDir,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const WORKFLOW_RUN_REF = "refs/heads/main";

const NO_HEADER_DEPLOYMENT_ID = "mail-edge-no-header-1";
const MALFORMED_DEPLOYMENT_ID = "mail-edge-malformed-1";
const DUPLICATE_DEPLOYMENT_ID = "mail-edge-duplicate-1";

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

  test("mail with no Message-Id header derives a sha256-of-bytes runId; identical bytes collide", async () => {
    const ctx = await deployEdgeWorkflow(env, NO_HEADER_DEPLOYMENT_ID);

    // Construct two byte-identical raw mails with NO Message-Id header.
    // The supervisor's parser walks for a `message-id:` line
    // case-insensitively; without one, `parseMessageIdHeader` returns
    // null and `deriveMessageId` falls back to `sha256(rawMessage)`.
    const raw = buildMinimalMail({
      from: "edge@integration.interchange",
      to: ctx.deploymentMailAddress,
      includeMessageIdHeader: false,
      body: "no-header edge case body",
    });

    const expectedRunId = await sha256Hex(raw);

    // Fire the first mail; the supervisor should mint a run with
    // runId === sha256(raw). Wait for `RunStarted` to land.
    routeRaw(env, ctx.deploymentMailAddress, raw);
    await waitForWorkflowRunComplete(
      env,
      NO_HEADER_DEPLOYMENT_ID,
      expectedRunId,
      { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
    );

    // Verify the canonical chain materialised for the sha256 runId
    // and the `consumedMessageId` on `RunStarted` equals the sha256.
    const events = await readWorkflowRunEvents(
      env,
      NO_HEADER_DEPLOYMENT_ID,
      expectedRunId,
    );
    const types = events.map((e) => e.type);
    expect(types).toContain("RunStarted");
    expect(types).toContain("RunCompleted");
    const started = events.find((e) => e.type === "RunStarted");
    if (started === undefined) throw new Error("unreachable");
    expect(started.body["consumedMessageId"]).toBe(expectedRunId);

    // Fire a second byte-identical mail. The supervisor's
    // `deriveMessageId` derives the same sha256 hash; the
    // claim-check substrate sees the messageId is already in
    // `consumed/` (or `processing/`, depending on timing) and
    // `enqueueInbox` throws `claim_check_already_*`. The
    // supervisor's `onMailMessage` swallows the throw -- duplicate
    // is dropped on the floor.
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
      `${expectedRunId}.json`,
      { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
    );
    const consumedRunIds = consumedBefore
      .map((e) => /^(.+)\.json$/.exec(e.filename)?.[1])
      .filter((v): v is string => v !== undefined);
    expect(consumedRunIds).toContain(expectedRunId);

    // Snapshot the sidecar diagnostics buffer before firing the
    // duplicate so the log-substring wait below can scope its match
    // to events that arrive AFTER the second routeRaw call.
    const diagBeforeDuplicate = env.sidecarDiagnostics();
    routeRaw(env, ctx.deploymentMailAddress, raw);

    // The duplicate must produce the supervisor's `enqueueInbox
    // failed` log line carrying one of the `claim_check_already_*`
    // reasons. This is the positive signal the test pins instead of
    // sleeping a fixed beat and re-reading the substrate.
    await waitFor(
      () => {
        const fresh = env
          .sidecarDiagnostics()
          .slice(diagBeforeDuplicate.length);
        return /enqueueInbox failed:.*claim_check_already_/.test(fresh);
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
    // Exactly one run for the sha256 id; no synthetic second
    // run-id materialised.
    expect(runIdsAfter.filter((r) => r === expectedRunId).length).toBe(1);
  }, 60_000);

  test("mail with malformed Message-Id (no closing bracket) mints the raw value as runId", async () => {
    const ctx = await deployEdgeWorkflow(env, MALFORMED_DEPLOYMENT_ID);

    // Construct a mail with a malformed Message-Id header. The
    // parser does NOT validate angle-bracket shape; it returns the
    // trimmed suffix after `Message-Id:`. So `<invalid` becomes
    // the messageId verbatim.
    const malformedMessageId = "<invalid";
    const raw = buildMinimalMail({
      from: "edge@integration.interchange",
      to: ctx.deploymentMailAddress,
      includeMessageIdHeader: true,
      messageId: malformedMessageId,
      body: "malformed message-id edge case body",
    });

    routeRaw(env, ctx.deploymentMailAddress, raw);

    await waitForWorkflowRunComplete(
      env,
      MALFORMED_DEPLOYMENT_ID,
      malformedMessageId,
      { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
    );

    const events = await readWorkflowRunEvents(
      env,
      MALFORMED_DEPLOYMENT_ID,
      malformedMessageId,
    );
    const started = events.find((e) => e.type === "RunStarted");
    if (started === undefined) {
      throw new Error(
        `malformed edge: run ${malformedMessageId} has no RunStarted`,
      );
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

    routeRaw(env, ctx.deploymentMailAddress, raw1);
    await waitForWorkflowRunComplete(env, DUPLICATE_DEPLOYMENT_ID, messageId, {
      timeoutMs: 30_000,
      diagnostics: env.sidecarDiagnostics,
    });

    // First run materialised under runs/<messageId>/. The supervisor's
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

    // Fire the duplicate. The supervisor's `enqueueInbox` rejects
    // with `claim_check_already_consumed`; `onMailMessage` swallows
    // the rejection. The dedup index stays at one entry; no second
    // run materialises.
    const diagBeforeDuplicate = env.sidecarDiagnostics();
    routeRaw(env, ctx.deploymentMailAddress, raw2);

    // Wait for the supervisor's `enqueueInbox failed` log line that
    // carries the `claim_check_already_*` reason; pinning on the
    // positive log signal beats sleeping a fixed beat and re-reading
    // the substrate hoping nothing changed.
    await waitFor(
      () => {
        const fresh = env
          .sidecarDiagnostics()
          .slice(diagBeforeDuplicate.length);
        return /enqueueInbox failed:.*claim_check_already_/.test(fresh);
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
    expect(runIds.filter((r) => r === messageId).length).toBe(1);
  }, 60_000);
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
    await env.hub.sessionService.launchSession({
      agentAddress: orchestratorParams.agentAddress,
      agentId: orchestratorParams.agentId,
      instanceId: orchestratorParams.instanceId,
      config: orchestratorParams.config,
      deployContent: deployContent as Parameters<
        typeof env.hub.sessionService.launchSession
      >[0]["deployContent"],
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
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the wire validator carries WorkflowDefinition steps as Record<string, unknown>; the orchestrator emits the typed primitive union shape that satisfies the wire schema
        steps: params.definition.steps as Record<string, unknown>,
        ...(params.definition.state !== undefined
          ? { state: params.definition.state }
          : {}),
      },
      sources: params.sources,
    });

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
  if (result.kind !== "multi-step") {
    throw new Error(`expected multi-step deploy; got ${result.kind}`);
  }

  const workflowRunRepoId: RepoId = {
    kind: "workflow-run",
    id: deriveTrivialDeploymentId(deploymentMailAddress),
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
function routeRaw(env: DeployFlowEnv, address: string, raw: Uint8Array): void {
  const base64 = Buffer.from(raw).toString("base64");
  const delivered = env.hub.router.routeMail(address, base64);
  if (!delivered) {
    throw new Error(
      `routeRaw: routeMail returned false for ${address}; address is not routable on the hub`,
    );
  }
}

async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const crypto = await import("node:crypto");
  return crypto.createHash("sha256").update(bytes).digest("hex");
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
