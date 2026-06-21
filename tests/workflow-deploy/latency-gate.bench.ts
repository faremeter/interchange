// Phase 4.7 LATENCY GATE benchmark (NOT a CI test).
//
// Measures the per-message round-trip latency of an interactive single
// agent on two paths, with the inference TEST PROVIDER so inference cost
// is fixed and near-zero -- the measured delta is the INFRASTRUCTURE the
// unified host adds over the in-process runtime, not inference.
//
//   BASELINE (in-process): a warm `@intx/agent` agent driven directly via
//     `agent.send(text)` -- the in-process mail->reactor->reply turn. No
//     IPC, no claim-check substrate, no dispatch loop. The same HTTP mock
//     inference server the unified path uses, in echo mode, so the
//     provider is byte-for-byte identical and inference is ~0.
//
//   UNIFIED (child): the real 4.6 spawn-real path -- hub routeMail ->
//     sidecar hub-link -> supervisor inbox claim-check write+read ->
//     dispatch loop -> trigger.fire IPC -> child reads bytes from the
//     processing entry -> WARM agent.send -> reply -> terminal.event IPC
//     back to the supervisor. Timed inside the supervisor (which runs in
//     the sidecar subprocess, so both ends of the IPC round-trip are
//     visible in one process) via the `onDispatchTiming` observability
//     hook, gated by `SIDECAR_LATENCY_BENCH_FILE`. The supervisor appends
//     `<runId> dispatch-start|reply-produced <perf.now ms>` lines that
//     this harness reads after the run.
//
// Boundary equivalence (documented; kept equivalent across paths):
//   - BASELINE measured interval: the wall-clock around one `agent.send`,
//     from just before the call to its resolution (the reply is produced
//     -- `agent.send` resolves on the director's `connector.reply`).
//   - UNIFIED measured interval: `dispatch-start` (the inbox entry is
//     dequeued for dispatch, claim-check read complete, `trigger.fire`
//     about to forward) to `reply-produced` (the child's terminal-event
//     frame for that run lands back at the supervisor). This brackets the
//     SAME warm `agent.send` the baseline times, PLUS exactly the infra
//     the unified path adds: claim-check read, IPC down, child bytes-read,
//     warm-cache acquire + step env, agent.send, terminal IPC up + commit.
//   So DELTA = UNIFIED - BASELINE isolates the unified infra tax.
//
// Both paths measure the SUSTAINED interactive case: back-to-back
// messages on ONE warm agent, message N+1 fired only after reply N (no
// pipelining), and the FIRST (cold) message is discarded so the agent
// build / tool materialization / LSP spawn cost is excluded -- the
// steady-state per-message round-trip is what the gate is about.
//
// Run:
//   bun run tests/workflow-deploy/latency-gate.bench.ts \
//     [--messages N] [--out <dir>]
//
// Writes <out>/raw-baseline.csv, <out>/raw-unified.csv, and
// <out>/results.json. Prints the percentile table to stdout. Not matched
// by `bun test` (it is a `.bench.ts`, not a `.test.ts`), so `make test`
// never runs it; it is type-checked by `make build` via this directory's
// tsconfig.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import { noopAuditStore, permissiveAuthorize } from "@intx/agent/testing";
import { createAgent, type Agent } from "@intx/agent";
import type { BaseEnv } from "@intx/agent";
import { createIsogitStore } from "@intx/storage-isogit";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
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
  fireMailTrigger,
  startDeployFlowEnv,
  startMockInference,
  type DeployFlowEnv,
  type MockInference,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "latency-gate-bench";
const STEP_ID = "step1";

// One steady-state message body. The echo provider reflects it; content
// is irrelevant to the infra timing as long as it is a small, fixed
// conversation body (so inference stays ~0 and constant across messages).
const BODY = "Latency-gate steady-state probe body 0xC0FFEE.";

type BenchOpts = {
  messages: number;
  outDir: string;
};

function parseArgs(argv: string[]): BenchOpts {
  let messages = 500;
  let outDir = path.resolve(
    import.meta.dir,
    "../../dispatch/workflow-launch-and-converge/p4-7-latency",
  );
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--messages") {
      const next = argv[i + 1];
      if (next === undefined) throw new Error("--messages requires a value");
      const parsed = Number.parseInt(next, 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`--messages must be a positive integer, got ${next}`);
      }
      messages = parsed;
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
  return { messages, outDir };
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
 * actually-observed sample, which is the honest choice for a latency
 * distribution a human will read.
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

type Trend = {
  firstSample: number;
  lastSample: number;
  /**
   * Ordinary-least-squares slope of round-trip-ms vs message index
   * (ms added per additional sustained message). A near-zero slope is a
   * stationary steady-state; a positive slope means the per-message cost
   * grows with accumulated history -- the percentiles then describe only
   * the measured window, not a stable distribution.
   */
  slopeMsPerMessage: number;
  interceptMs: number;
};

/** OLS linear fit of `samples` against their index 0..n-1. */
function computeTrend(samples: number[]): Trend {
  const n = samples.length;
  if (n === 0) throw new Error("computeTrend of empty sample");
  const first = samples[0];
  const last = samples[n - 1];
  if (first === undefined || last === undefined) {
    throw new Error("unreachable: non-empty sample has no first/last");
  }
  let sumX = 0;
  let sumY = 0;
  let sumXY = 0;
  let sumXX = 0;
  for (let i = 0; i < n; i += 1) {
    const y = samples[i];
    if (y === undefined) throw new Error("unreachable: sample hole");
    sumX += i;
    sumY += y;
    sumXY += i * y;
    sumXX += i * i;
  }
  const denom = n * sumXX - sumX * sumX;
  const slope = denom === 0 ? 0 : (n * sumXY - sumX * sumY) / denom;
  const intercept = (sumY - slope * sumX) / n;
  return {
    firstSample: first,
    lastSample: last,
    slopeMsPerMessage: slope,
    interceptMs: intercept,
  };
}

// --- baseline: warm in-process agent driven via agent.send ----------------

/**
 * Build ONE warm agent against the supplied HTTP mock inference URL and
 * drive `messages + 1` back-to-back sends, discarding the first (cold)
 * sample. Returns the steady-state per-message `agent.send` round-trip
 * samples in milliseconds.
 */
async function runBaseline(opts: {
  messages: number;
  inferenceURL: string;
}): Promise<number[]> {
  const workDir = fs.mkdtempSync(path.join(os.tmpdir(), "latency-baseline-"));
  const storeDir = path.join(workDir, "store");
  fs.mkdirSync(storeDir, { recursive: true });

  const source: InferenceSource = {
    id: "anthropic:mock-model",
    provider: "anthropic",
    baseURL: opts.inferenceURL,
    apiKey: "sk-mock",
    model: "mock-model",
  };

  // A real isogit ContextStore (the same store kind the unified step
  // agent uses for its conversation) so the baseline pays the same
  // per-turn context-commit cost the in-process runtime pays today.
  const storage = await createIsogitStore(storeDir);

  const def = defineAgent({
    id: "latency-baseline-agent",
    systemPrompt: "You are the latency-gate baseline agent.",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
  });

  const env: BaseEnv = {
    sources: [source],
    defaultSource: source.id,
    storage,
    workdir: storeDir,
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
  };

  const agent: Agent = await createAgent(def, env);
  const samples: number[] = [];
  try {
    // Cold warm-up send (discarded): the first send pays one-time
    // reactor/source/context warm costs the steady-state must exclude.
    await agent.send(BODY);

    for (let i = 0; i < opts.messages; i += 1) {
      const t0 = performance.now();
      await agent.send(BODY);
      const t1 = performance.now();
      samples.push(t1 - t0);
    }
  } finally {
    await agent.close();
    fs.rmSync(workDir, { recursive: true, force: true });
  }
  return samples;
}

// --- unified: real spawn-real child path, timed in the supervisor ---------

/**
 * Deploy a single warm agent on the unified child path, fire
 * `messages + 1` back-to-back mails (one warm agent, sustained), and
 * recover the supervisor's per-message dispatch round-trip from the
 * timing file the `onDispatchTiming` hook appends. The first run's
 * sample is discarded (cold agent build). Returns steady-state samples
 * in milliseconds.
 */
async function runUnified(opts: {
  messages: number;
  timingFile: string;
}): Promise<number[]> {
  const env: DeployFlowEnv = await startDeployFlowEnv({
    inferenceEchoUserMessage: true,
    sidecarEnv: { SIDECAR_LATENCY_BENCH_FILE: opts.timingFile },
  });

  try {
    const deploymentMailAddress = deriveDeploymentAddress({
      deploymentId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });

    const agent = defineAgent({
      id: "latency-unified-agent",
      systemPrompt: "You are the latency-gate unified agent.",
      tools: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });

    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${DEPLOYMENT_ID}`,
      trigger: { type: "mail", to: deploymentMailAddress },
      steps: { [STEP_ID]: step({ agent }) },
    });

    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `ins_${DEPLOYMENT_ID}`,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: deploymentMailAddress,
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
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${deploymentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

    const launchSession: LaunchSessionFn = async (orchestratorParams) => {
      await env.hub.sessionService.launchSession({
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
          { files, message: `latency-gate bench: write workflow repo` },
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
      deploymentId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      hubPublicKey: "00".repeat(32),
    });
    if (result.kind !== "multi-step") {
      throw new Error(
        `unified bench: expected multi-step deploy, got ${result.kind}`,
      );
    }

    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveTrivialDeploymentId(deploymentMailAddress),
    };
    env.registerDeployment({
      deploymentId: DEPLOYMENT_ID,
      workflowDefinition: workflow,
      workflowRunRepoId,
      workflowRunRef: "refs/heads/main",
      mailAddress: deploymentMailAddress,
    });

    // Confirm the deployment is routable before the first fire so a
    // first-run failure is attributable to the fire, not a half-wired
    // deploy.
    await waitForFirstRoutable(env, deploymentMailAddress);

    // Fire messages+1 mails back-to-back; pace strictly on the
    // supervisor's own `reply-produced` timing mark for the just-fired
    // message (NOT the hub-side workflow-run event poll, whose pack-push
    // pipeline lags as the repo grows and would conflate observation lag
    // with the measured interval). Exactly one message is in flight at a
    // time -- the sustained interactive case, no pipelining. The runId
    // the supervisor keys the timing file on is the inbound mail's
    // `Message-Id` verbatim, so the messageId we mint is the timing key.
    // The first message's sample is the cold agent build, discarded
    // below.
    const orderedRunIds: string[] = [];
    for (let i = 0; i < opts.messages + 1; i += 1) {
      const messageId = `<latency-${String(i)}@integration.interchange>`;
      await fireMailTrigger(env, deploymentMailAddress, {
        messageId,
        content: BODY,
      });
      // Wait until BOTH marks for this runId are present, then proceed to
      // the next fire. The per-message wait scales with the (growing)
      // round-trip, so the timeout is generous.
      await waitForRunReplyProduced(opts.timingFile, messageId, 120_000, () =>
        env.sidecarDiagnostics(),
      );
      orderedRunIds.push(messageId);
    }

    const byRun = parseTimingFile(opts.timingFile);
    const samples: number[] = [];
    // Discard the first (cold) run; steady-state is the 2nd..Nth.
    for (let i = 1; i < orderedRunIds.length; i += 1) {
      const runId = orderedRunIds[i];
      if (runId === undefined) continue;
      const pair = byRun.get(runId);
      if (pair === undefined) {
        throw new Error(
          `unified bench: no timing pair for runId ${runId}; the supervisor's onDispatchTiming hook did not emit both marks`,
        );
      }
      samples.push(pair.replyProduced - pair.dispatchStart);
    }
    if (samples.length === 0) {
      throw new Error("unified bench: zero steady-state samples collected");
    }
    return samples;
  } finally {
    await env.teardown();
  }
}

type TimingPair = { dispatchStart: number; replyProduced: number };

/** Parse `<runId> <marker> <atMs>` lines into per-run start/stop pairs. */
function parseTimingFile(file: string): Map<string, TimingPair> {
  const text = fs.readFileSync(file, "utf8");
  const starts = new Map<string, number>();
  const stops = new Map<string, number>();
  for (const line of text.split("\n")) {
    const trimmed = line.trim();
    if (trimmed === "") continue;
    const parts = trimmed.split(" ");
    // The supervisor's D2 attribution emits per-leg lines on this same
    // channel, shaped `<runId> leg <leg> <phase> <atMs> [counters...]`.
    // The 4.7 round-trip gate only consumes the `<runId> <marker> <atMs>`
    // round-trip lines, so any line whose second field is the `leg`
    // discriminator is skipped here rather than treated as malformed.
    if (parts[1] === "leg") continue;
    if (parts.length !== 3) {
      throw new Error(`malformed timing line: ${JSON.stringify(line)}`);
    }
    const [runId, marker, atRaw] = parts;
    if (runId === undefined || marker === undefined || atRaw === undefined) {
      throw new Error(`malformed timing line: ${JSON.stringify(line)}`);
    }
    const at = Number.parseFloat(atRaw);
    if (!Number.isFinite(at)) {
      throw new Error(`non-numeric timestamp in line: ${JSON.stringify(line)}`);
    }
    if (marker === "dispatch-start") starts.set(runId, at);
    else if (marker === "reply-produced") stops.set(runId, at);
    else throw new Error(`unknown marker in line: ${JSON.stringify(line)}`);
  }
  const out = new Map<string, TimingPair>();
  for (const [runId, dispatchStart] of starts) {
    const replyProduced = stops.get(runId);
    if (replyProduced === undefined) continue;
    out.set(runId, { dispatchStart, replyProduced });
  }
  return out;
}

/**
 * Wait until the deployment's mail address is routable on the hub, so the
 * first `fireMailTrigger` is not racing the deploy's address
 * registration. Bounded; throws with sidecar diagnostics on timeout.
 */
async function waitForFirstRoutable(
  env: DeployFlowEnv,
  address: string,
): Promise<void> {
  const start = Date.now();
  for (;;) {
    if (env.hub.router.getRoutableAddresses().includes(address)) return;
    if (Date.now() - start > 30_000) {
      throw new Error(
        `unified bench: deployment address ${address} never became routable\n${env.sidecarDiagnostics()}`,
      );
    }
    await new Promise((r) => setTimeout(r, 25));
  }
}

/**
 * Wait until the supervisor timing file holds the `reply-produced` mark
 * for `runId` (paired with its `dispatch-start`). This is the per-message
 * completion signal the bench paces on -- it comes straight from the
 * supervisor (one process, both IPC ends visible) and does not depend on
 * the laggy hub-side workflow-run pack-push pipeline. Throws on timeout
 * with sidecar diagnostics so a wedged run surfaces loudly.
 */
async function waitForRunReplyProduced(
  file: string,
  runId: string,
  timeoutMs: number,
  diagnostics: () => string,
): Promise<void> {
  const start = Date.now();
  for (;;) {
    if (fs.existsSync(file)) {
      const pair = parseTimingFile(file).get(runId);
      if (pair !== undefined) return;
    }
    if (Date.now() - start > timeoutMs) {
      throw new Error(
        `unified bench: timed out after ${String(timeoutMs)}ms waiting for reply-produced mark for runId ${runId}\n${diagnostics()}`,
      );
    }
    await new Promise((r) => setTimeout(r, 20));
  }
}

// --- main ------------------------------------------------------------------

function statsRow(label: string, s: Stats): string {
  return [
    label.padEnd(22),
    fmt(s.p50).padStart(10),
    fmt(s.p95).padStart(10),
    fmt(s.p99).padStart(10),
    fmt(s.mean).padStart(10),
    fmt(s.min).padStart(10),
    fmt(s.max).padStart(10),
  ].join("  ");
}

async function main(): Promise<void> {
  const opts = parseArgs(process.argv.slice(2));
  fs.mkdirSync(opts.outDir, { recursive: true });

  const loadBefore = os.loadavg();

  // Shared HTTP mock inference server (echo mode) for the BASELINE so the
  // provider is identical to the unified path's in-fixture mock. The
  // unified path starts its own mock inside the fixture; both are the
  // same `startMockInference` echo server, so inference is the same ~0
  // cost on both sides.
  const baselineInference: MockInference = startMockInference({
    echoUserMessage: true,
  });
  let baseline: number[];
  try {
    const port = baselineInference.server.port;
    if (port === undefined) {
      throw new Error("baseline mock inference server has no bound port");
    }
    baseline = await runBaseline({
      messages: opts.messages,
      inferenceURL: `http://localhost:${String(port)}`,
    });
  } finally {
    baselineInference.server.stop(true);
  }

  const timingFile = path.join(opts.outDir, "supervisor-timing.log");
  if (fs.existsSync(timingFile)) fs.rmSync(timingFile);
  const unified = await runUnified({
    messages: opts.messages,
    timingFile,
  });

  const loadAfter = os.loadavg();

  const baselineStats = computeStats(baseline);
  const unifiedStats = computeStats(unified);
  const baselineTrend = computeTrend(baseline);
  const unifiedTrend = computeTrend(unified);

  const delta: Stats = {
    n: Math.min(baselineStats.n, unifiedStats.n),
    min: unifiedStats.min - baselineStats.min,
    p50: unifiedStats.p50 - baselineStats.p50,
    p95: unifiedStats.p95 - baselineStats.p95,
    p99: unifiedStats.p99 - baselineStats.p99,
    mean: unifiedStats.mean - baselineStats.mean,
    max: unifiedStats.max - baselineStats.max,
  };

  // Raw samples (one ms value per line) so the numbers are auditable.
  fs.writeFileSync(
    path.join(opts.outDir, "raw-baseline.csv"),
    "round_trip_ms\n" + baseline.map((v) => v.toFixed(6)).join("\n") + "\n",
  );
  fs.writeFileSync(
    path.join(opts.outDir, "raw-unified.csv"),
    "round_trip_ms\n" + unified.map((v) => v.toFixed(6)).join("\n") + "\n",
  );

  const results = {
    generatedAt: new Date().toISOString(),
    messagesPerPath: opts.messages,
    sampleNote:
      "first (cold) message discarded on each path; samples are the steady-state 2nd..Nth back-to-back messages on one warm agent",
    inference: "HTTP mock (echo mode); inference cost fixed/~0 on both paths",
    machine: {
      platform: `${os.type()} ${os.release()} ${os.arch()}`,
      cpus: os.cpus().length,
      loadavgBefore: loadBefore,
      loadavgAfter: loadAfter,
    },
    units: "milliseconds",
    baseline: baselineStats,
    unified: unifiedStats,
    delta_unified_minus_baseline: delta,
    trend: {
      note: "OLS fit of per-message round-trip vs message index over the measured window; a positive unified slope means the per-message cost grows with accumulated history (not a stationary steady-state)",
      baseline: baselineTrend,
      unified: unifiedTrend,
    },
  };
  fs.writeFileSync(
    path.join(opts.outDir, "results.json"),
    JSON.stringify(results, null, 2) + "\n",
  );

  const header = [
    "path".padEnd(22),
    "p50".padStart(10),
    "p95".padStart(10),
    "p99".padStart(10),
    "mean".padStart(10),
    "min".padStart(10),
    "max".padStart(10),
  ].join("  ");
  process.stdout.write(`\nPer-message round-trip latency (ms)\n`);
  process.stdout.write(
    `messages/path=${String(opts.messages)} (cold first message discarded)\n`,
  );
  process.stdout.write(
    `loadavg before=${loadBefore.map((v) => v.toFixed(2)).join(",")} after=${loadAfter.map((v) => v.toFixed(2)).join(",")}\n\n`,
  );
  process.stdout.write(header + "\n");
  process.stdout.write(statsRow("baseline (in-process)", baselineStats) + "\n");
  process.stdout.write(statsRow("unified (child)", unifiedStats) + "\n");
  process.stdout.write(statsRow("DELTA (unified-base)", delta) + "\n\n");
  process.stdout.write(
    `growth (OLS slope, ms added per sustained message):\n` +
      `  baseline  first=${fmt(baselineTrend.firstSample)} last=${fmt(baselineTrend.lastSample)} slope=${fmt(baselineTrend.slopeMsPerMessage)}\n` +
      `  unified   first=${fmt(unifiedTrend.firstSample)} last=${fmt(unifiedTrend.lastSample)} slope=${fmt(unifiedTrend.slopeMsPerMessage)}\n\n`,
  );
  process.stdout.write(`results.json + raw CSVs written to ${opts.outDir}\n`);
}

await main();
