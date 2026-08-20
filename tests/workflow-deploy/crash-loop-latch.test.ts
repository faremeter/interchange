// Crash-loop guard acceptance-gate integration test.
//
// Proves the INTR-193 acceptance requirement: repeated unexpected child
// exits within the crash-loop window latch the deployment to the terminal
// `crash-looping` state instead of respawning forever, and the latch
// commits a `RunFailed` for the deployment's run.
//
// Shape: deploy a workflow, then SIGKILL the workflow-process child three
// times in quick succession. No mail is fired: the crash-loop guard counts
// child-process exits, not runs, so the deployment's anchor run (born
// `deployed`) is what the latch marks `RunFailed`. The 3rd unexpected exit
// trips the default `crashLoopMaxCount=3` within `crashLoopWindowMs=60s`;
// the supervisor stops respawning and tears down to `crash-looping`.
//
// Landing each kill on a RUNNING child (not one mid-`recycling`-handshake,
// which would fail the respawn to `stopped` instead of counting a crash)
// is sequenced off the supervisor's own stderr markers: a distinct
// per-crash `respawning after <Nms> backoff` line proves each crash was
// counted, and a killed child's replacement is confirmed alive by a fresh,
// stable pid before the next kill. A mistimed kill produces no such marker,
// so the `waitFor` times out and the test fails loud rather than silently
// mis-verifying.
//
// SCOPE: the crash driver is a real SIGKILL, not a self-crashing workflow
// body -- the declarative workflow DSL cannot express a body that crashes
// its own process, and a signal is one of the exit kinds the guard targets.
// Two acceptance sub-clauses are covered elsewhere, by design:
//   - The stable-run reset of the crash counter is a supervisor-internal
//     timer, unit-tested deterministically with an injected clock in
//     `packages/workflow-host/src/supervisor/crash-respawn.test.ts`; a 60s
//     wall-clock wait here would be inappropriate.
//   - "Redeploy resets the counter" is a fresh-supervisor tautology (a
//     redeploy mints a new supervisor whose counter is empty), not a guard
//     behavior worth integration complexity.
// The DB status flip (`workflow_run.status -> failed`) is NOT asserted
// here: this harness's hub is a stub that does not run the production
// `markTerminal` pack-receive path, so the observable latch surface is the
// committed `RunFailed` event, read through the real push pipeline.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess,
// real workflow-process child, mock inference. The crashes are genuine
// SIGKILLs; the latch rides the production crash-loop guard.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  killWorkflowHostChild,
  listWorkflowHostChildren,
  settleWorkflowRunPacks,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { twoStepEntry } from "./fixtures/two-step";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_crash-loop-latch-1";

const TENANT_ID = "tnt_crash_loop_latch";
const CALLER_PRINCIPAL_ID = "prn_crash_loop_latch";
const DEFINITION_ASSET_ID = "ast_crash_loop_latch_wf";

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
  h = await createTestDb();
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
  });
  await seedAsset(h.db, {
    id: DEFINITION_ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "crash-loop-latch-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

// eslint-disable-next-line no-control-regex -- match ANSI SGR escapes (CSI ... m)
const ANSI_ESCAPE_RE = /\x1b\[[0-9;]*m/g;

/**
 * Wait until the sidecar's stderr contains `needle`. ANSI color codes are
 * stripped first (the spawned sidecar logs in dev colour mode, so the
 * dev-formatter wraps every interpolated value in `\x1b[..m'value'\x1b[..m`);
 * `needle` should therefore carry the dev formatter's single quotes around
 * interpolated values (e.g. `after '1000'ms`).
 */
async function waitForSidecarLog(
  target: DeployFlowEnv,
  needle: string,
  opts: { timeoutMs?: number } = {},
): Promise<void> {
  const timeoutMs = opts.timeoutMs ?? 30_000;
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    // Join reconstructs lines split across raw decoded chunks, then strip
    // ANSI so the interpolated values are contiguous with their quotes.
    const text = target.sidecar.stderr.join("").replace(ANSI_ESCAPE_RE, "");
    if (text.includes(needle)) return;
    if (Date.now() > deadline) {
      throw new Error(
        `waitForSidecarLog: sidecar stderr never contained ${JSON.stringify(needle)} within ${String(timeoutMs)}ms\n${target.sidecarDiagnostics()}`,
      );
    }
    await new Promise((r) => setTimeout(r, 50));
  }
}

/**
 * Wait for a workflow-process child whose pid is not in `excludePids` and
 * that stays present for a stability window -- proof it survived the ready
 * handshake and reached the `running` phase (a child killed mid-handshake
 * vanishes before the window elapses, and the respawn then fails to
 * `stopped` with no stable replacement). Returns the stable pid.
 */
async function waitForStableRunningChild(
  target: DeployFlowEnv,
  excludePids: number[],
  opts: { timeoutMs?: number; stableMs?: number } = {},
): Promise<number> {
  const timeoutMs = opts.timeoutMs ?? 30_000;
  const stableMs = opts.stableMs ?? 2_000;
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const candidate = listWorkflowHostChildren(target).find(
      (pid) => !excludePids.includes(pid),
    );
    if (candidate !== undefined) {
      const stableUntil = Date.now() + stableMs;
      let stable = true;
      while (Date.now() < stableUntil) {
        await new Promise((r) => setTimeout(r, 100));
        if (!listWorkflowHostChildren(target).includes(candidate)) {
          stable = false;
          break;
        }
      }
      if (stable) return candidate;
    }
    if (Date.now() > deadline) {
      throw new Error(
        `waitForStableRunningChild: no stable new child (excluding ${excludePids.join(",")}) within ${String(timeoutMs)}ms\n${target.sidecarDiagnostics()}`,
      );
    }
    await new Promise((r) => setTimeout(r, 100));
  }
}

describe.skipIf(!harnessDbEnvAvailable())(
  "crash-loop guard latches a repeatedly-crashing deployment",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("three unexpected child exits within the window latch to crash-looping and commit RunFailed", async () => {
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        apiKey: "sk-mock",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `${DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_crash-loop-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
      ]);

      const entryModule = twoStepEntry({
        address: deploymentMailAddress,
        systemPrompt1: "You are the first crash-loop step agent.",
        systemPrompt2: "You are the second crash-loop step agent.",
        agentId1: "crash-loop-agent1",
        agentId2: "crash-loop-agent2",
        workflowId: `wf_${DEPLOYMENT_ID}`,
      });

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule,
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: operatorApprovals,
        config,
        sources: { step1: [inferenceSource], step2: [inferenceSource] },
      });
      expect(handle.publicKey).toBeTruthy();

      // Gate the first kill on a routable deployment (its child has passed
      // the ready handshake) plus a stable child pid.
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );
      await settleWorkflowRunPacks(env);
      const killed: number[] = [];
      await waitForStableRunningChild(env, killed);

      // Crash 1: under the threshold -> the supervisor logs a 1s backoff
      // and respawns. Waiting for the distinct backoff marker proves the
      // exit was counted (not mistaken for a planned kill).
      killed.push(...killWorkflowHostChild(env));
      await waitForSidecarLog(env, "respawning after '1000'ms backoff");
      await waitForStableRunningChild(env, killed);

      // Crash 2: still under the threshold -> a 2s backoff (distinct
      // marker) and another respawn.
      killed.push(...killWorkflowHostChild(env));
      await waitForSidecarLog(env, "respawning after '2000'ms backoff");
      await waitForStableRunningChild(env, killed);

      // Crash 3: reaches the threshold -> latch. No further respawn; the
      // deployment tears down to `crash-looping`.
      killed.push(...killWorkflowHostChild(env));
      await waitForSidecarLog(
        env,
        "crash-looped: '3' unexpected exits within '60000'ms",
      );

      // The latch committed a RunFailed for the deployment's anchor run,
      // observable through the real push pipeline. `waitForWorkflowRun
      // Complete` accepts any terminal event, so assert the type explicitly.
      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        DEPLOYMENT_ID,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      expect(terminal.type).toBe("RunFailed");

      // The latch tore down the deployment, not the sidecar.
      expect(env.sidecar.proc.exitCode).toBeNull();

      // No 4th respawn: wait comfortably past the would-be 4s backoff (never
      // scheduled -- the guard latched instead) and confirm no fresh child
      // appears under the sidecar.
      await new Promise((r) => setTimeout(r, 8_000));
      const survivors = listWorkflowHostChildren(env).filter(
        (pid) => !killed.includes(pid),
      );
      expect(survivors).toEqual([]);
    }, 180_000);
  },
);
