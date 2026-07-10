// Deploy-latency benchmark (NOT a CI test).
//
// Measures workflow DEPLOY wall-clock as a function of step count, so a
// regression in the serial multi-step deploy path can be observed. The
// multi-step deploy is serial: the orchestrator's `runMultiStepBranch`
// launches every step through the per-step `launchSession` loop (one hub
// round-trip per step) and then issues a single `sendMultiStepDeploy`
// hand-off, so the measured cost is expected to grow roughly linearly
// with step count (~n+1 round-trips). This bench quantifies the
// per-step slope (ms/step, OLS fit) and the per-count medians.
//
// The measured operation is `orchestrator.deployWorkflow(...)`, bracketed
// with `performance.now()` -- nothing else in the iteration is timed. The
// stack is the real deploy stack stood up by `startDeployFlowEnv` (real
// hub WebSocket server, real sidecar subprocess, mock echo inference so
// inference cost is ~0 and does not confound the deploy timing). Each
// measured deploy uses a fresh deploymentId so per-step `agent-state`
// repos and derived mail addresses never collide across iterations.
//
// For each step count the FIRST (cold) iteration is discarded: the first
// deploy on a fresh env pays one-time warm costs (sidecar link warm-up,
// repo-store directory materialization) the steady-state slope must
// exclude. The remaining iterations are the reported samples.
//
// Run:
//   bun run tests/workflow-deploy/deploy-latency.bench.ts \
//     [--iterations N] [--steps 1,3,5,10] [--out <dir>]
//
// Writes <out>/results.json and prints a per-step-count table to stdout.
// Not matched by `bun test` (it is a `.bench.ts`, not a `.test.ts`), so
// `make test` never runs it; it is type-checked by `make build` via this
// directory's tsconfig.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
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
import type { RepoId, WorkflowRunHubPrincipal } from "@intx/hub-sessions";
import { DEFAULT_ASSET_REF } from "@intx/hub-sessions";

import {
  SESSION_ID,
  startDeployFlowEnv,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";

type BenchOpts = {
  iterations: number;
  stepCounts: number[];
  outDir: string;
};

function parseArgs(argv: string[]): BenchOpts {
  let iterations = 6;
  let stepCounts = [1, 3, 5, 10];
  let outDir = path.resolve(
    import.meta.dir,
    "../../dispatch/workflow-launch-and-converge/deploy-latency",
  );
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--iterations") {
      const next = argv[i + 1];
      if (next === undefined) throw new Error("--iterations requires a value");
      const parsed = Number.parseInt(next, 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`--iterations must be a positive integer, got ${next}`);
      }
      iterations = parsed;
      i += 1;
    } else if (arg === "--steps") {
      const next = argv[i + 1];
      if (next === undefined) throw new Error("--steps requires a value");
      const parsed = next.split(",").map((s) => {
        const n = Number.parseInt(s.trim(), 10);
        if (!Number.isFinite(n) || n <= 0) {
          throw new Error(
            `--steps entries must be positive integers, got ${s}`,
          );
        }
        return n;
      });
      if (parsed.length === 0) throw new Error("--steps requires at least one");
      stepCounts = parsed;
      i += 1;
    } else if (arg === "--out") {
      const next = argv[i + 1];
      if (next === undefined) throw new Error("--out requires a value");
      outDir = path.resolve(next);
      i += 1;
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }
  return { iterations, stepCounts, outDir };
}

// --- percentile statistics -------------------------------------------------

type Stats = {
  n: number;
  min: number;
  p50: number;
  p95: number;
  p99: number;
  mean: number;
  max: number;
};

/**
 * Nearest-rank percentile on a copy sorted ascending. `p` in [0,100].
 * Nearest-rank (rather than interpolation) keeps the reported value an
 * actually-observed sample.
 */
function percentile(sortedAsc: number[], p: number): number {
  if (sortedAsc.length === 0) throw new Error("percentile of empty sample");
  const rank = Math.ceil((p / 100) * sortedAsc.length);
  const idx = Math.min(Math.max(rank, 1), sortedAsc.length) - 1;
  const value = sortedAsc[idx];
  if (value === undefined) throw new Error("percentile index out of range");
  return value;
}

function computeStats(samples: number[]): Stats {
  if (samples.length === 0) throw new Error("computeStats of empty sample");
  const sorted = [...samples].sort((a, b) => a - b);
  const sum = sorted.reduce((acc, v) => acc + v, 0);
  const min = sorted[0];
  const max = sorted[sorted.length - 1];
  if (min === undefined || max === undefined) {
    throw new Error("unreachable: non-empty sample has no min/max");
  }
  return {
    n: sorted.length,
    min,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    mean: sum / sorted.length,
    max,
  };
}

function fmt(ms: number): string {
  return ms.toFixed(3);
}

/**
 * Ordinary-least-squares fit of deploy-ms against step count over the
 * per-count medians. `slopeMsPerStep` is the marginal deploy cost of one
 * additional workflow step; `interceptMs` is the fixed deploy floor.
 */
type Trend = {
  slopeMsPerStep: number;
  interceptMs: number;
};

function computeTrend(points: { x: number; y: number }[]): Trend {
  const n = points.length;
  if (n === 0) throw new Error("computeTrend of empty sample");
  let sumX = 0;
  let sumY = 0;
  let sumXY = 0;
  let sumXX = 0;
  for (const { x, y } of points) {
    sumX += x;
    sumY += y;
    sumXY += x * y;
    sumXX += x * x;
  }
  const denom = n * sumXX - sumX * sumX;
  const slope = denom === 0 ? 0 : (n * sumXY - sumX * sumY) / denom;
  const intercept = (sumY - slope * sumX) / n;
  return { slopeMsPerStep: slope, interceptMs: intercept };
}

// --- one measured deploy ---------------------------------------------------

const OPERATOR_APPROVALS: ApprovalSet = new Set<string>([
  "inference.source:anthropic:mock-model",
  "director:@intx/agent/default",
]);

/**
 * Build an N-step workflow whose steps run strictly serially: step k
 * depends on step k-1 via `after`, so the orchestrator's multi-step
 * branch launches them in order (one per-step `launchSession` round-trip
 * each). Every step carries its own agent so each produces a per-step
 * `agent-state` deploy tree, matching the shape the serial deploy path
 * provisions in production. A single-step count yields the single-step
 * head deploy path; two or more yields the multi-step branch.
 */
function buildWorkflow(
  deploymentId: string,
  stepCount: number,
): { workflow: WorkflowDefinition; mailAddress: string } {
  const mailAddress = deriveDeploymentAddress({
    deploymentId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
  });

  const steps: Record<string, ReturnType<typeof step>> = {};
  for (let k = 0; k < stepCount; k += 1) {
    const stepId = `step${String(k)}`;
    const agent = defineAgent({
      id: `agent-${deploymentId}-${stepId}`,
      systemPrompt: `You are step ${String(k)} of the deploy-latency bench.`,
      tools: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });
    steps[stepId] =
      k === 0
        ? step({ agent })
        : step({ agent, after: [`step${String(k - 1)}`] });
  }

  const workflow = defineWorkflow({
    id: `wf_${deploymentId}`,
    trigger: { type: "mail", to: mailAddress },
    steps,
  });

  return { workflow, mailAddress };
}

/**
 * Compose a deploy orchestrator against the env's hub substrate and run
 * one `deployWorkflow` for the given N-step workflow, returning the
 * measured `deployWorkflow` wall-clock in milliseconds. The interval
 * brackets ONLY the `deployWorkflow` call.
 */
async function measureOneDeploy(
  env: DeployFlowEnv,
  deploymentId: string,
  stepCount: number,
): Promise<number> {
  const { workflow, mailAddress } = buildWorkflow(deploymentId, stepCount);

  const config: HarnessConfig = {
    sessionId: SESSION_ID,
    agentId: `ins_${deploymentId}`,
    tenantId: "tenant-1",
    principalId: "prin_integration-1",
    agentAddress: mailAddress,
    systemPrompt: "Fallback prompt (overridden per step by the orchestrator)",
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
    ...OPERATOR_APPROVALS,
    `mail.address:${mailAddress}`,
    `mail.send:${DEPLOYMENT_DOMAIN}`,
  ]);

  const launchSession: LaunchSessionFn = async (orchestratorParams) => {
    await env.hub.sessionService.stageWorkflowStep({
      agentAddress: orchestratorParams.agentAddress,
      agentId: orchestratorParams.agentId,
      instanceId: orchestratorParams.instanceId,
      config: orchestratorParams.config,
      deployContent: toLaunchDeployContent(orchestratorParams.deployContent),
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
        { files, message: `deploy-latency bench: write workflow repo` },
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

  const t0 = performance.now();
  const result = await orchestrator.deployWorkflow({
    workflow,
    config,
    deployContent: { systemPrompt: config.systemPrompt },
    operatorApprovals,
    deploymentId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    hubPublicKey: "00".repeat(32),
  });
  const elapsed = performance.now() - t0;

  if (!result.publicKey) {
    throw new Error(
      `deploy-latency bench: deploy of ${deploymentId} (${String(stepCount)} steps) did not return a public key`,
    );
  }
  return elapsed;
}

// --- main ------------------------------------------------------------------

type PerCount = {
  stepCount: number;
  samples: number[];
  stats: Stats;
};

function statsRow(label: string, s: Stats): string {
  return [
    label.padEnd(12),
    String(s.n).padStart(6),
    fmt(s.p50).padStart(10),
    fmt(s.p95).padStart(10),
    fmt(s.mean).padStart(10),
    fmt(s.min).padStart(10),
    fmt(s.max).padStart(10),
  ].join("  ");
}

async function main(): Promise<void> {
  const opts = parseArgs(process.argv.slice(2));
  fs.mkdirSync(opts.outDir, { recursive: true });

  const loadBefore = os.loadavg();

  // One env (one hub + one sidecar subprocess) drives every measured
  // deploy. A fresh env per iteration would fold sidecar spawn cost into
  // the samples; the deploy path under measurement does not spawn a
  // sidecar, so reusing the env keeps the measured interval to the deploy
  // itself. Each deploy gets a unique deploymentId so nothing collides.
  const env: DeployFlowEnv = await startDeployFlowEnv({
    inferenceEchoUserMessage: true,
  });

  const perCount: PerCount[] = [];
  try {
    for (const stepCount of opts.stepCounts) {
      const samples: number[] = [];
      // iterations + 1 deploys; the first (cold) sample is discarded.
      for (let i = 0; i < opts.iterations + 1; i += 1) {
        const deploymentId = `deploy-latency-s${String(stepCount)}-i${String(i)}`;
        const elapsed = await measureOneDeploy(env, deploymentId, stepCount);
        if (i > 0) samples.push(elapsed);
      }
      if (samples.length === 0) {
        throw new Error(
          `deploy-latency bench: zero steady-state samples for ${String(stepCount)} steps`,
        );
      }
      perCount.push({
        stepCount,
        samples,
        stats: computeStats(samples),
      });
    }
  } finally {
    await env.teardown();
  }

  const loadAfter = os.loadavg();

  // OLS slope of per-count median deploy time vs step count -- the
  // headline ms/step figure a regression watcher tracks.
  const medianPoints = perCount.map((c) => ({
    x: c.stepCount,
    y: c.stats.p50,
  }));
  const trend = computeTrend(medianPoints);

  const results = {
    generatedAt: new Date().toISOString(),
    iterationsPerCount: opts.iterations,
    stepCounts: opts.stepCounts,
    sampleNote:
      "first (cold) deploy per step count discarded; each measured deploy uses a fresh deploymentId on one shared hub+sidecar env",
    inference: "HTTP mock (echo mode); inference cost fixed/~0 (deploy path)",
    machine: {
      platform: `${os.type()} ${os.release()} ${os.arch()}`,
      cpus: os.cpus().length,
      loadavgBefore: loadBefore,
      loadavgAfter: loadAfter,
    },
    units: "milliseconds",
    perStepCount: perCount.map((c) => ({
      stepCount: c.stepCount,
      stats: c.stats,
      samples: c.samples,
    })),
    trend: {
      note: "OLS fit of per-count median deployWorkflow ms vs step count; slopeMsPerStep is the marginal cost of one additional serial step (~one extra hub round-trip)",
      slopeMsPerStep: trend.slopeMsPerStep,
      interceptMs: trend.interceptMs,
    },
  };
  fs.writeFileSync(
    path.join(opts.outDir, "results.json"),
    JSON.stringify(results, null, 2) + "\n",
  );

  const header = [
    "steps".padEnd(12),
    "n".padStart(6),
    "p50".padStart(10),
    "p95".padStart(10),
    "mean".padStart(10),
    "min".padStart(10),
    "max".padStart(10),
  ].join("  ");
  process.stdout.write(`\nWorkflow deploy latency vs step count (ms)\n`);
  process.stdout.write(
    `iterations/count=${String(opts.iterations)} (cold first deploy discarded)\n`,
  );
  process.stdout.write(
    `loadavg before=${loadBefore.map((v) => v.toFixed(2)).join(",")} after=${loadAfter.map((v) => v.toFixed(2)).join(",")}\n\n`,
  );
  process.stdout.write(header + "\n");
  for (const c of perCount) {
    process.stdout.write(
      statsRow(`${String(c.stepCount)}-step`, c.stats) + "\n",
    );
  }
  process.stdout.write(
    `\nper-step deploy cost (OLS slope over per-count medians):\n` +
      `  slope=${fmt(trend.slopeMsPerStep)} ms/step  intercept=${fmt(trend.interceptMs)} ms\n\n`,
  );
  process.stdout.write(`results.json written to ${opts.outDir}\n`);
}

if (import.meta.main) {
  await main();
}
