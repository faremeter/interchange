// A run that resumes after a process death keeps its grants and its credential
// material.
//
// Each case deploys a workflow whose first step is a top-level `sleep`. The
// steps after the sleep need the run's authority:
//
//   - Mediated credential: the `action-credential-workflow` fixture with a
//     gate. Every root step runs after the sleep, so no handler resolves the
//     credential before the timer fires.
//   - Plain tool: the `single-step-mail-tool` fixture with a `nap`. The agent
//     step runs after the sleep and calls the `fs` tool, which needs the
//     run's `tool:<name>` grant.
//
// The cases:
//
//   1. The sidecar dies while the run sleeps, and a fresh sidecar starts on
//      the same data dir, once before the timer is due and once after. The
//      restarted sidecar's workflow child fires the timer, and the
//      credentialed handlers reach the origin with the seeded secret.
//   2. The same crash and restart with the plain tool. The tool result is the
//      tool's own output, not an authorization error.
//   3. Only the workflow child dies while the run sleeps, and the sidecar
//      respawns it. The respawned child's credentialed handlers succeed.
//
// Before the crash the run log has a `TimerSet` and no `TimerFired`, and no
// request has reached the origin or the inference server. After recovery the
// log has exactly one `TimerSet` and one `TimerFired`, so the resumed run
// re-adopted its timer and did not mint a second one.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow child, and a real process death.

import fs from "node:fs";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { actionConsumer } from "@intx/authz";
import { createNoopCredentialCipher } from "@intx/crypto";
import { tenant as tenantTable } from "@intx/db/schema";
import type { RepoId } from "@intx/hub-sessions";
import type { WireGrantRule } from "@intx/types/grant-wire";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { createApprovalSet, deriveRunAddress } from "@intx/workflow-deploy";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedCredential,
  seedPrincipal,
  seedProvider,
  tenantSlugFromId,
} from "@intx/test-harness/seed";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  killWorkflowHostChild,
  listWorkflowHostChildren,
  readWorkflowRunEvents,
  settleWorkflowRunPacks,
  startDeployFlowEnv,
  terminateSidecarSubprocess,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type StartDeployFlowEnvOpts,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import {
  CHILD_HANDLER,
  CHILD_PATH,
  LOOP_HANDLER,
  LOOP_PATH,
  TOP_HANDLER,
  TOP_PATH,
  actionCredentialWorkflowEntry,
} from "./fixtures/action-credential-workflow";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";
import { singleStepMailToolEntry } from "./fixtures/single-step-mail-tool";
import {
  deriveWireRunGrants,
  failureMessages,
  toolResultTexts,
} from "./nested-tool-invoke-helpers";
import {
  crashSidecar,
  restartSidecar,
  timerFireAt,
  waitUntilTimerDue,
  stepEventsOf,
  waitForTimerSet,
} from "./sleep-restart-helpers";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const TENANT_ID = "tnt_sleep_restart_authority";
const CALLER_PRINCIPAL_ID = "prn_sleep_restart_authority";
const PROVIDER_ID = "prv_sleep_restart_authority";
const PROVIDER_NAME = "sleep-restart-authority-provider";
const CREDENTIAL_ID = "cred_sleep_restart_authority";
const CREDENTIAL_NAME = "sleep-restart-authority-cred";
const SECRET = "sk-sleep-restart-delta-7c41";

const NAP_STEP_ID = "nap";
const AGENT_STEP_ID = "step1";

// Long enough that the test detects the park and kills the process before the
// timer fires (the run must be parked at the crash), while the persisted
// `fireAt` stays far inside the test budget. Park detection (a durable
// `TimerSet`) lands in well under a second on an idle machine and in 1-2s
// under suite-wide load, and the kill and restart fit in a few seconds, so 12s
// keeps a wide margin on both sides. The credential test also restarts once
// after `fireAt`, so the restored child finds its timer already due.
const SLEEP_DURATION_MS = 12_000;

// The tool writes a file named after its `body` arg and returns
// `wrote <file>`. An authorization refusal returns an error result instead.
const TOOL_FILENAME = "sleep-restart-tool-ran.txt";
const TOOL_CONTENT = "executed-after-restart";

// `fireMailTrigger` does not materialize run grants the way the production
// route does, so the test sends the snapshot's tool grant itself.
const RUN_TOOL_GRANT: WireGrantRule = {
  id: `run-grant:tool:${MAIL_TOOL_NAME}`,
  resource: `tool:${MAIL_TOOL_NAME}`,
  action: "invoke",
  effect: "allow",
  origin: "creator",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: null,
};

let h: TestDb | undefined;
let origin: ReturnType<typeof Bun.serve> | undefined;
const originRequests: { path: string; authorization: string | null }[] = [];

// Each case starts its own env, because a crash kills the env's sidecar. The
// cleanups run in reverse order, so a restarted sidecar stops before the env
// that registered it tears down.
const cleanups: (() => Promise<void>)[] = [];

beforeAll(async () => {
  // A file-scope beforeAll fires even when describe.skipIf skips the
  // suite bodies, so it needs its own guard or a missing DB env throws
  // here. See the two-shape rule in tests/lib/db-harness.ts.
  if (!harnessDbEnvAvailable()) return;
  origin = Bun.serve({
    port: 0,
    fetch(req) {
      const url = new URL(req.url);
      originRequests.push({
        path: url.pathname,
        authorization: req.headers.get("authorization"),
      });
      return Response.json({ ok: true, seenPath: url.pathname });
    },
  });

  h = await createTestDb();
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
  });
  await seedProvider(h.db, {
    id: PROVIDER_ID,
    tenantId: TENANT_ID,
    name: PROVIDER_NAME,
    plugin: "http",
    apiBaseUrl: origin.url.origin,
  });
  await seedCredential(h.db, {
    id: CREDENTIAL_ID,
    tenantId: TENANT_ID,
    providerId: PROVIDER_ID,
    name: CREDENTIAL_NAME,
    secret: SECRET,
  });
});

afterAll(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  if (origin !== undefined) await origin.stop(true);
  if (h !== undefined) await h.close();
});

function requireDb(): TestDb {
  if (h === undefined) throw new Error("test DB did not start");
  return h;
}

async function startEnv(
  opts: StartDeployFlowEnvOpts = {},
): Promise<DeployFlowEnv> {
  const env = await startDeployFlowEnv(opts);
  cleanups.push(() => env.teardown());
  return env;
}

function registerTempDir(dir: string): void {
  cleanups.push(() => fs.promises.rm(dir, { recursive: true, force: true }));
}

/** The run grant that lets `consumer` use the credential. */
function useGrant(consumer: string): WireGrantRule {
  return {
    id: `grant-use-${consumer}`,
    resource: `credential:${CREDENTIAL_ID}`,
    action: "use",
    effect: "allow",
    origin: "system",
    conditions: { tool: consumer },
    expiresAt: null,
    roleId: null,
    principalId: null,
  };
}

function mockSource(env: DeployFlowEnv): InferenceSource {
  return {
    id: "anthropic:mock-model",
    provider: "anthropic",
    baseURL: `http://localhost:${String(env.inference.server.port)}`,
    credentialId: "sk-mock",
    model: "mock-model",
  };
}

function harnessConfig(
  env: DeployFlowEnv,
  anchorRunId: string,
  mailAddress: string,
): HarnessConfig {
  return {
    sessionId: SESSION_ID,
    agentId: anchorRunId,
    tenantId: "tenant-1",
    principalId: `prin_${anchorRunId}`,
    agentAddress: mailAddress,
    systemPrompt: "Fallback prompt (overridden per step by the definition)",
    tools: [],
    grants: [],
    sources: [mockSource(env)],
    defaultSource: "anthropic:mock-model",
  };
}

type Deployed = {
  anchorRunId: string;
  mailAddress: string;
  workflowRunRepoId: RepoId;
};

async function seedDefinitionAsset(name: string): Promise<string> {
  const definitionAssetId = `ast_sleep_restart_${name}`;
  await seedAsset(requireDb().db, {
    id: definitionAssetId,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: definitionAssetId,
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });
  return definitionAssetId;
}

/**
 * Deploy the gated credential fixture and fire one run with the snapshot's
 * grants plus a use grant for each bound handler.
 */
async function startCredentialRun(
  env: DeployFlowEnv,
  name: string,
): Promise<Deployed & { runId: string }> {
  const anchorRunId = `run_sleep-restart-${name}`;
  const mailAddress = deriveRunAddress({
    runId: anchorRunId,
    domain: DEPLOYMENT_DOMAIN,
  });
  const handle = await deployWorkflowSourceForTest(env, {
    entryModule: actionCredentialWorkflowEntry({
      address: mailAddress,
      workflowId: `wf_sleep_restart_${name}`,
      childWorkflowId: `wf_sleep_restart_${name}_child`,
      binding: { provider: PROVIDER_NAME, name: CREDENTIAL_NAME },
      gate: { stepId: NAP_STEP_ID, durationMs: SLEEP_DURATION_MS },
    }),
    actions: "./workflow.mjs",
    loops: "./workflow.mjs",
    db: requireDb().db,
    tenantId: TENANT_ID,
    definitionAssetId: await seedDefinitionAsset(name),
    anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: mailAddress,
    approvals: "approve-probed",
    config: harnessConfig(env, anchorRunId, mailAddress),
    credentialCipher: createNoopCredentialCipher(),
  });
  await waitFor(
    () => env.hub.router.getRoutableAddresses().includes(mailAddress),
    { diagnostics: env.sidecarDiagnostics },
  );
  await fireMailTrigger(env, mailAddress, {
    messageId: `<sleep-restart-${name}@integration.interchange>`,
    grants: [
      ...deriveWireRunGrants(handle.approved.grantSnapshot),
      useGrant(actionConsumer(TOP_HANDLER)),
      useGrant(actionConsumer(CHILD_HANDLER)),
      useGrant(actionConsumer(LOOP_HANDLER)),
    ],
  });
  const runId = await waitForFirstRunId(env, handle.workflowRunRepoId, {
    diagnostics: env.sidecarDiagnostics,
  });
  return {
    anchorRunId,
    mailAddress,
    workflowRunRepoId: handle.workflowRunRepoId,
    runId,
  };
}

/** Assert the run is parked in the sleep, with no step after it started. */
async function expectParkedBeforeTimer(
  env: DeployFlowEnv,
  anchorRunId: string,
  runId: string,
  laterStepId: string,
): Promise<void> {
  const events = await readWorkflowRunEvents(env, anchorRunId, runId);
  expect(stepEventsOf(events, "TimerSet", NAP_STEP_ID)).toHaveLength(1);
  expect(events.filter((e) => e.type === "TimerFired")).toHaveLength(0);
  expect(stepEventsOf(events, "StepStarted", laterStepId)).toHaveLength(0);
  expect(stepEventsOf(events, "StepCompleted", laterStepId)).toHaveLength(0);
}

async function expectCompleted(
  env: DeployFlowEnv,
  anchorRunId: string,
  runId: string,
): Promise<WorkflowRunEvent[]> {
  const terminal = await waitForWorkflowRunComplete(env, anchorRunId, runId, {
    diagnostics: env.sidecarDiagnostics,
  });
  const events = await readWorkflowRunEvents(env, anchorRunId, runId);
  if (terminal.type !== "RunCompleted") {
    throw new Error(
      `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(failureMessages(events))}\n${env.sidecarDiagnostics()}`,
    );
  }
  // The resumed run re-adopted its timer rather than set a second one.
  expect(events.filter((e) => e.type === "TimerSet")).toHaveLength(1);
  expect(events.filter((e) => e.type === "TimerFired")).toHaveLength(1);
  return events;
}

function topHandlerOutput(events: readonly WorkflowRunEvent[]): unknown {
  const ref = stepEventsOf(events, "StepCompleted", TOP_HANDLER)[0]?.body[
    "output"
  ];
  if (
    typeof ref !== "object" ||
    ref === null ||
    !("ref" in ref) ||
    typeof ref.ref !== "string" ||
    !ref.ref.startsWith("inline:")
  ) {
    throw new Error(
      `no inline output for ${TOP_HANDLER}: ${JSON.stringify(failureMessages(events))}`,
    );
  }
  return JSON.parse(ref.ref.slice("inline:".length));
}

/** Assert every bound handler reached the origin with the seeded secret. */
function expectSecretReachedOrigin(
  events: readonly WorkflowRunEvent[],
  requests: readonly { path: string; authorization: string | null }[],
): void {
  expect(topHandlerOutput(events)).toEqual({
    status: 200,
    body: { ok: true, seenPath: TOP_PATH },
  });
  expect(requests).toHaveLength(3);
  for (const path of [TOP_PATH, CHILD_PATH, LOOP_PATH]) {
    expect(requests).toContainEqual({
      path,
      authorization: `Bearer ${SECRET}`,
    });
  }
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a run resumed after a process death keeps its authority",
  () => {
    test.each(["before", "after"] as const)(
      "a mediated credential works after a sidecar crash and a restart %s the timer is due",
      async (restart) => {
        const env = await startEnv();
        const before = originRequests.length;
        const run = await startCredentialRun(env, `sidecar-${restart}`);

        await waitForTimerSet(env, run.anchorRunId, run.runId, NAP_STEP_ID);
        await expectParkedBeforeTimer(
          env,
          run.anchorRunId,
          run.runId,
          TOP_HANDLER,
        );
        expect(originRequests.slice(before)).toEqual([]);

        const dataDir = await crashSidecar(env, run.mailAddress);
        expect(originRequests.slice(before)).toEqual([]);
        if (restart === "after") {
          await waitUntilTimerDue(env, run.anchorRunId, run.runId, NAP_STEP_ID);
        }

        const restarted = await restartSidecar(env, {
          address: run.mailAddress,
          dataDir,
          registerTempDir,
        });
        cleanups.push(() => terminateSidecarSubprocess(restarted));
        if (restart === "before") {
          // The restored child must find the timer not yet due.
          expect(Date.now()).toBeLessThan(
            await timerFireAt(env, run.anchorRunId, run.runId, NAP_STEP_ID),
          );
        }

        const events = await expectCompleted(env, run.anchorRunId, run.runId);
        expectSecretReachedOrigin(events, originRequests.slice(before));
      },
      180_000,
    );

    test("a plain tool keeps its grant after a sidecar crash and restart", async () => {
      const env = await startEnv({
        inferenceToolCall: {
          toolName: MAIL_TOOL_NAME,
          input: { to: TOOL_CONTENT, body: TOOL_FILENAME },
        },
      });
      const name = "tool";
      const anchorRunId = `run_sleep-restart-${name}`;
      const mailAddress = deriveRunAddress({
        runId: anchorRunId,
        domain: DEPLOYMENT_DOMAIN,
      });
      const source = mockSource(env);
      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: singleStepMailToolEntry({
          variant: "fs",
          stepId: AGENT_STEP_ID,
          systemPrompt: "You are the sleep-restart tool agent.",
          address: mailAddress,
          workflowId: `wf_sleep_restart_${name}`,
          napMs: SLEEP_DURATION_MS,
        }),
        db: requireDb().db,
        tenantId: TENANT_ID,
        definitionAssetId: await seedDefinitionAsset(name),
        anchorRunId,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: mailAddress,
        approvals: createApprovalSet([
          "inference.source:anthropic:mock-model",
          "director:@intx/agent/default",
          `mail.address:${mailAddress}`,
          `mail.send:${DEPLOYMENT_DOMAIN}`,
          `tool:${MAIL_TOOL_NAME}`,
        ]),
        config: harnessConfig(env, anchorRunId, mailAddress),
        // The deploy frame needs a source chain for every step, the sleep
        // included.
        sources: { [NAP_STEP_ID]: [source], [AGENT_STEP_ID]: [source] },
      });
      await waitFor(
        () => env.hub.router.getRoutableAddresses().includes(mailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );
      await fireMailTrigger(env, mailAddress, {
        messageId: `<sleep-restart-${name}@integration.interchange>`,
        grants: [RUN_TOOL_GRANT],
      });
      const runId = await waitForFirstRunId(env, handle.workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
      });

      await waitForTimerSet(env, anchorRunId, runId, NAP_STEP_ID);
      await expectParkedBeforeTimer(env, anchorRunId, runId, AGENT_STEP_ID);
      expect(env.inference.requests).toHaveLength(0);

      const dataDir = await crashSidecar(env, mailAddress);
      expect(env.inference.requests).toHaveLength(0);

      // The deployed config names the mock server's port, so the restarted
      // sidecar reaches the same mock with no extra env.
      const restarted = await restartSidecar(env, {
        address: mailAddress,
        dataDir,
        registerTempDir,
      });
      cleanups.push(() => terminateSidecarSubprocess(restarted));

      const events = await expectCompleted(env, anchorRunId, runId);
      expect(stepEventsOf(events, "StepCompleted", AGENT_STEP_ID)).toHaveLength(
        1,
      );

      // The tool's own return value is the only tool result. A refused call
      // also completes the run, but its result is an error text. The cold
      // multi-step path removes the run's workspace when the run ends, so
      // the result text, which the tool returns only after its write, is the
      // proof that the tool ran.
      const texts = env.inference.requests.flatMap(toolResultTexts);
      expect(texts.length).toBeGreaterThan(0);
      expect([...new Set(texts)]).toEqual([`wrote ${TOOL_FILENAME}`]);
    }, 180_000);

    test("a mediated credential works after the workflow child crashes and respawns", async () => {
      const env = await startEnv();
      const before = originRequests.length;
      const run = await startCredentialRun(env, "child");

      await waitForTimerSet(env, run.anchorRunId, run.runId, NAP_STEP_ID);
      await expectParkedBeforeTimer(
        env,
        run.anchorRunId,
        run.runId,
        TOP_HANDLER,
      );
      expect(originRequests.slice(before)).toEqual([]);

      await settleWorkflowRunPacks(env);
      const killedPids = killWorkflowHostChild(env);
      expect(env.sidecar.proc.exitCode).toBeNull();
      await waitFor(
        () =>
          listWorkflowHostChildren(env).some(
            (pid) => !killedPids.includes(pid),
          ),
        { diagnostics: env.sidecarDiagnostics },
      );

      // The killed child cannot send a request, so every request below came
      // from the respawned child.
      const events = await expectCompleted(env, run.anchorRunId, run.runId);
      expectSecretReachedOrigin(events, originRequests.slice(before));
    }, 180_000);
  },
);
